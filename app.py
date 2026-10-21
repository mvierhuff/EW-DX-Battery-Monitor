#!/usr/bin/env python3
import json, socket, time, sqlite3, threading, ipaddress, concurrent.futures, urllib.parse, sys, os, webbrowser
from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler
from pathlib import Path

NETWORK = "192.168.0.0/24"
BIND_IP = "0.0.0.0"
SSC_PORT = 45
HTTP_PORT = 8080
POLL_SECONDS = 5
SCAN_SECONDS = 60
BASE = Path(sys.executable).resolve().parent if getattr(sys, "frozen", False) else Path(__file__).resolve().parent
DB = BASE / "ewdx.db"
WEB = Path(getattr(sys, "_MEIPASS", BASE)) / "web"
SETTINGS = BASE / "settings.json"

def load_settings():
    global NETWORK, BIND_IP, HTTP_PORT
    try:
        d = json.loads(SETTINGS.read_text(encoding="utf-8"))
        NETWORK = str(ipaddress.ip_network(d.get("network", NETWORK), strict=False))
        BIND_IP = str(ipaddress.ip_address(d.get("bind_ip", BIND_IP)))
        HTTP_PORT = int(d.get("port", HTTP_PORT))
        if not 1024 <= HTTP_PORT <= 65535: raise ValueError("Port")
    except (OSError, ValueError, KeyError, TypeError): pass
    state["network"] = NETWORK


state = {"receivers": [], "updated": 0, "network": NETWORK, "poll_seconds": POLL_SECONDS}
state_lock = threading.Lock()
manual_new_run = set()


def req(ip, payload, timeout=.35):
    """Read-only SSCv1 request. All payloads used by this app request values with null."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.settimeout(timeout)
    try:
        s.sendto(json.dumps(payload, separators=(",", ":")).encode(), (ip, SSC_PORT))
        b, _ = s.recvfrom(65535)
        return json.loads(b.decode(errors="replace"))
    except Exception:
        return None
    finally:
        s.close()


def nested(o, *p):
    try:
        for k in p:
            o = o[k]
        return o
    except Exception:
        return None


def probe(ip):
    r = req(ip, {"mates": {"tx1": {"battery": {"gauge": None}}}}, .18)
    return ip if r and ("mates" in r or "osc" in r) else None


def discover():
    hosts = [str(x) for x in ipaddress.ip_network(NETWORK).hosts()]
    with concurrent.futures.ThreadPoolExecutor(max_workers=100) as ex:
        found = [x for x in ex.map(probe, hosts) if x]
    return sorted(found, key=lambda x: tuple(map(int, x.split("."))))


def dbcon():
    con = sqlite3.connect(DB, timeout=10)
    con.row_factory = sqlite3.Row
    return con


def initdb():
    con = dbcon()
    con.executescript("""
    CREATE TABLE IF NOT EXISTS profiles(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      label TEXT NOT NULL UNIQUE,
      tx_type TEXT,
      created_at REAL NOT NULL
    );
    CREATE TABLE IF NOT EXISTS channel_profiles(
      ip TEXT NOT NULL,
      channel INTEGER NOT NULL,
      profile_id INTEGER,
      PRIMARY KEY(ip, channel)
    );
    CREATE TABLE IF NOT EXISTS runs(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ip TEXT NOT NULL,
      channel INTEGER NOT NULL,
      profile_id INTEGER,
      name TEXT,
      tx_type TEXT,
      started_at REAL NOT NULL,
      ended_at REAL,
      end_reason TEXT,
      start_gauge INTEGER,
      end_gauge INTEGER,
      active_seconds REAL DEFAULT 0,
      lowbattery_at REAL
    );
    CREATE TABLE IF NOT EXISTS samples_v2(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ts REAL NOT NULL,
      ip TEXT NOT NULL,
      channel INTEGER NOT NULL,
      run_id INTEGER,
      profile_id INTEGER,
      name TEXT,
      tx_type TEXT,
      gauge INTEGER,
      online INTEGER NOT NULL,
      warnings TEXT,
      active_elapsed REAL DEFAULT 0,
      recovery INTEGER DEFAULT 0
    );
    CREATE INDEX IF NOT EXISTS idx_samples_v2_channel_ts ON samples_v2(ip,channel,ts);
    CREATE INDEX IF NOT EXISTS idx_samples_v2_run_ts ON samples_v2(run_id,ts);
    """)
    con.commit(); con.close()


def get_channel_profile(con, ip, ch):
    r = con.execute("""SELECT p.id,p.label,p.tx_type FROM channel_profiles cp
                       LEFT JOIN profiles p ON p.id=cp.profile_id
                       WHERE cp.ip=? AND cp.channel=?""", (ip, ch)).fetchone()
    return dict(r) if r and r["id"] else None


def channel(ip, ch):
    tx = f"tx{ch}"
    gr = req(ip, {"mates": {tx: {"battery": {"gauge": None}}}})
    gauge = nested(gr, "mates", tx, "battery", "gauge") if gr else None
    if gauge is None:
        return {"channel": ch, "online": False}

    nr = req(ip, {"mates": {tx: {"name": None}}})
    tr = req(ip, {"mates": {tx: {"type": None}}})
    wr = req(ip, {"mates": {tx: {"warnings": None}}})
    warnings = nested(wr, "mates", tx, "warnings")
    if warnings is None:
        warnings = []
    if not isinstance(warnings, list):
        warnings = [warnings]
    return {
        "channel": ch, "online": True,
        "name": nested(nr, "mates", tx, "name") or f"CH {ch}",
        "type": nested(tr, "mates", tx, "type") or "TX",
        "gauge": int(gauge), "warnings": warnings,
    }


def last_sample(con, ip, ch):
    return con.execute("SELECT * FROM samples_v2 WHERE ip=? AND channel=? ORDER BY ts DESC LIMIT 1", (ip, ch)).fetchone()


def open_run(con, ip, ch):
    return con.execute("SELECT * FROM runs WHERE ip=? AND channel=? AND ended_at IS NULL ORDER BY id DESC LIMIT 1", (ip, ch)).fetchone()


def start_run(con, now, ip, ch, c, profile_id=None):
    cur = con.execute("""INSERT INTO runs(ip,channel,profile_id,name,tx_type,started_at,start_gauge,active_seconds)
                         VALUES(?,?,?,?,?,?,?,0)""",
                      (ip, ch, profile_id, c.get("name"), c.get("type"), now, c.get("gauge")))
    return cur.lastrowid


def close_run(con, run_id, now, reason, end_gauge):
    con.execute("UPDATE runs SET ended_at=?, end_reason=?, end_gauge=? WHERE id=? AND ended_at IS NULL",
                (now, reason, end_gauge, run_id))


def looks_like_new_battery(prev_gauge, gauge, was_offline):
    if prev_gauge is None or gauge is None:
        return False
    jump = gauge - prev_gauge
    # Deliberately tolerant of alkaline recovery after a pause.
    if was_offline:
        return (gauge >= 85 and prev_gauge <= 60 and jump >= 25) or (gauge >= 70 and prev_gauge <= 25 and jump >= 35)
    return gauge >= 80 and jump >= 35


def warning_has_low(warnings):
    return "lowbattery" in json.dumps(warnings).lower()


def process_sample(con, now, ip, ch, c):
    profile = get_channel_profile(con, ip, ch)
    profile_id = profile["id"] if profile else None
    prev = last_sample(con, ip, ch)
    run = open_run(con, ip, ch)

    if not c.get("online"):
        active = float(run["active_seconds"]) if run else 0
        con.execute("""INSERT INTO samples_v2(ts,ip,channel,run_id,profile_id,online,active_elapsed)
                       VALUES(?,?,?,?,?,0,?)""", (now, ip, ch, run["id"] if run else None, profile_id, active))
        return None

    gauge = c.get("gauge")
    prev_gauge = prev["gauge"] if prev and prev["gauge"] is not None else None
    was_offline = bool(prev and not prev["online"])
    key = (ip, ch)
    force_new = key in manual_new_run
    if force_new:
        manual_new_run.discard(key)

    new_battery = force_new  # Nur manuelle Batteriewechsel, niemals automatische Erkennung
    if new_battery and run:
        close_run(con, run["id"], now, "battery_change_manual" if force_new else "battery_change_auto", prev_gauge)
        run = None

    if not run:
        run_id = start_run(con, now, ip, ch, c, profile_id)
        run = con.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone()

    active = float(run["active_seconds"] or 0)
    # Count time only when consecutive samples are online and the gap is reasonable.
    if prev and prev["online"] and prev["run_id"] == run["id"]:
        dt = now - prev["ts"]
        if 0 < dt <= POLL_SECONDS * 3:
            active += dt

    recovery = 0
    if was_offline and prev_gauge is not None and gauge > prev_gauge and not new_battery:
        recovery = 1

    low = warning_has_low(c.get("warnings", []))
    if low and run["lowbattery_at"] is None:
        con.execute("UPDATE runs SET lowbattery_at=? WHERE id=?", (now, run["id"]))

    con.execute("UPDATE runs SET active_seconds=?, end_gauge=?, profile_id=?, name=?, tx_type=? WHERE id=?",
                (active, gauge, profile_id, c.get("name"), c.get("type"), run["id"]))
    con.execute("""INSERT INTO samples_v2(ts,ip,channel,run_id,profile_id,name,tx_type,gauge,online,warnings,active_elapsed,recovery)
                   VALUES(?,?,?,?,?,?,?,?,1,?,?,?)""",
                (now, ip, ch, run["id"], profile_id, c.get("name"), c.get("type"), gauge,
                 json.dumps(c.get("warnings", []), ensure_ascii=False), active, recovery))
    return run["id"]


def quantile(vals, q):
    if not vals: return None
    vals = sorted(vals)
    pos = (len(vals)-1)*q
    lo = int(pos); hi = min(lo+1, len(vals)-1); f = pos-lo
    return vals[lo]*(1-f)+vals[hi]*f


def empirical_remaining(con, run_id, profile_id, tx_type, gauge, active_elapsed):
    """Empirical estimate to LowBattery when historical runs actually reached LowBattery."""
    if gauge is None: return None
    candidates = []
    scope = "type"
    if profile_id:
        rows = con.execute("SELECT * FROM runs WHERE profile_id=? AND lowbattery_at IS NOT NULL AND id<>?", (profile_id, run_id)).fetchall()
        if len(rows) >= 3:
            candidates = rows; scope = "device"
    if not candidates:
        rows = con.execute("SELECT * FROM runs WHERE tx_type=? AND lowbattery_at IS NOT NULL AND id<>?", (tx_type, run_id)).fetchall()
        candidates = rows

    remains = []
    for r in candidates:
        low_s = con.execute("SELECT active_elapsed FROM samples_v2 WHERE run_id=? AND ts>=? ORDER BY ts LIMIT 1",
                            (r["id"], r["lowbattery_at"])).fetchone()
        near = con.execute("""SELECT active_elapsed,gauge FROM samples_v2 WHERE run_id=? AND online=1 AND gauge IS NOT NULL
                              ORDER BY ABS(gauge-?) ASC, ts DESC LIMIT 1""", (r["id"], gauge)).fetchone()
        if low_s and near and abs(near["gauge"] - gauge) <= 4:
            rem = low_s["active_elapsed"] - near["active_elapsed"]
            if rem >= 0: remains.append(rem)
    if len(remains) < 3: return None
    return {
        "target": "lowbattery", "seconds": int(quantile(remains,.5)),
        "low": int(quantile(remains,.2)), "high": int(quantile(remains,.8)),
        "samples": len(remains), "scope": scope,
        "confidence": "high" if len(remains)>=15 else "medium" if len(remains)>=6 else "low"
    }


def slope_fallback(con, run_id, gauge):
    rows = con.execute("""SELECT active_elapsed,gauge FROM samples_v2 WHERE run_id=? AND online=1 AND gauge IS NOT NULL
                          ORDER BY id DESC LIMIT 240""", (run_id,)).fetchall()
    if len(rows) < 6 or gauge is None: return None
    newest = rows[0]
    # Find an earlier point at least 3 percentage points higher; active_elapsed excludes pauses.
    older = next((r for r in reversed(rows) if r["gauge"] >= gauge + 3), None)
    if not older: return None
    dg = older["gauge"] - newest["gauge"]
    dt = newest["active_elapsed"] - older["active_elapsed"]
    if dg <= 0 or dt <= 0: return None
    sec_per_pct = dt / dg
    return {"target":"zero_estimate", "seconds": int(gauge * sec_per_pct), "samples":0,
            "scope":"current_run", "confidence":"learning"}


def forecast(con, run_id, profile_id, tx_type, gauge, active_elapsed):
    e = empirical_remaining(con, run_id, profile_id, tx_type, gauge, active_elapsed)
    return e or slope_fallback(con, run_id, gauge)


def run_info(con, run_id):
    if not run_id: return None
    r = con.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone()
    if not r: return None
    recovery_count = con.execute("SELECT COUNT(*) n FROM samples_v2 WHERE run_id=? AND recovery=1", (run_id,)).fetchone()["n"]
    return {"id":r["id"], "started_at":r["started_at"], "active_seconds":int(r["active_seconds"] or 0),
            "start_gauge":r["start_gauge"], "recovery_count":recovery_count}


def loop():
    ips = []; lastscan = 0
    while True:
        try:
            if not ips or time.time() - lastscan > SCAN_SECONDS:
                ips = discover(); lastscan = time.time()
            out = []; con = dbcon(); now = time.time()
            for ip in ips:
                chans = []
                for ch in (1, 2):
                    c = channel(ip, ch)
                    run_id = process_sample(con, now, ip, ch, c)
                    profile = get_channel_profile(con, ip, ch)
                    if c.get("online") and run_id:
                        rr = con.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone()
                        c["run"] = run_info(con, run_id)
                        c["forecast"] = forecast(con, run_id, profile["id"] if profile else None,
                                                  c.get("type"), c.get("gauge"), rr["active_seconds"])
                    c["profile"] = profile
                    chans.append(c)
                out.append({"ip": ip, "channels": chans})
            con.commit(); con.close()
            with state_lock:
                state["receivers"] = out; state["updated"] = now
        except Exception as e:
            print("Monitor-Fehler:", e)
        time.sleep(POLL_SECONDS)


class H(SimpleHTTPRequestHandler):
    def send_json(self, obj, code=200):
        b = json.dumps(obj, ensure_ascii=False).encode()
        self.send_response(code); self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store"); self.end_headers(); self.wfile.write(b)

    def do_GET(self):
        if self.path == "/api/state":
            with state_lock: data = dict(state)
            return self.send_json(data)
        if self.path.startswith("/api/history"):
            q = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            run_id = int(q.get("run_id", ["0"])[0] or 0)
            con = dbcon()
            rows = con.execute("""SELECT ts,gauge,online,active_elapsed,recovery FROM samples_v2
                                  WHERE run_id=? ORDER BY ts""", (run_id,)).fetchall()
            con.close()
            return self.send_json([dict(r) for r in rows])
        if self.path == "/api/profiles":
            con=dbcon(); rows=con.execute("SELECT * FROM profiles ORDER BY label").fetchall(); con.close()
            return self.send_json([dict(r) for r in rows])
        return super().do_GET()

    def do_POST(self):
        n = int(self.headers.get("Content-Length", "0") or 0)
        try: data = json.loads(self.rfile.read(n) or b"{}")
        except Exception: return self.send_json({"ok":False,"error":"invalid json"},400)

        if self.path == "/api/battery-changed":
            ip = str(data.get("ip", "")); ch = int(data.get("channel", 0))
            if not ip or ch not in (1,2): return self.send_json({"ok":False},400)
            manual_new_run.add((ip,ch))
            return self.send_json({"ok":True})

        if self.path == "/api/profile":
            ip = str(data.get("ip", "")); ch = int(data.get("channel", 0)); label = str(data.get("label", "")).strip()
            tx_type = str(data.get("tx_type", "")).strip() or None
            if not ip or ch not in (1,2): return self.send_json({"ok":False},400)
            con=dbcon()
            if not label:
                con.execute("DELETE FROM channel_profiles WHERE ip=? AND channel=?",(ip,ch))
            else:
                con.execute("INSERT OR IGNORE INTO profiles(label,tx_type,created_at) VALUES(?,?,?)",(label,tx_type,time.time()))
                p=con.execute("SELECT id FROM profiles WHERE label=?",(label,)).fetchone()
                con.execute("INSERT OR REPLACE INTO channel_profiles(ip,channel,profile_id) VALUES(?,?,?)",(ip,ch,p["id"]))
                # Also attach the currently open run to the chosen physical profile.
                con.execute("UPDATE runs SET profile_id=? WHERE ip=? AND channel=? AND ended_at IS NULL",(p["id"],ip,ch))
                con.execute("UPDATE samples_v2 SET profile_id=? WHERE run_id IN (SELECT id FROM runs WHERE ip=? AND channel=? AND ended_at IS NULL)",
                            (p["id"],ip,ch))
            con.commit(); con.close()
            return self.send_json({"ok":True})
        return self.send_json({"ok":False,"error":"not found"},404)

    def log_message(self, *a): pass


def local_ips():
    ips = {"Alle Netzwerkadapter (0.0.0.0)": "0.0.0.0"}
    try:
        import psutil
        for nic, entries in psutil.net_if_addrs().items():
            for e in entries:
                if e.family == socket.AF_INET and not e.address.startswith("127."):
                    ips[f"{nic} – {e.address}"] = e.address
    except ImportError:
        pass
    return ips


def main():
    global NETWORK, BIND_IP, HTTP_PORT
    load_settings()
    initdb()
    threading.Thread(target=loop, daemon=True).start()
    os.chdir(WEB)
    import tkinter as tk
    from tkinter import ttk, messagebox
    root = tk.Tk()
    root.title("EW-DX Battery Monitor")
    root.geometry("540x420")
    root.resizable(False, False)
    frame = ttk.Frame(root, padding=20)
    frame.pack(fill="both", expand=True)
    ttk.Label(frame, text="EW-DX Battery Monitor", font=("Segoe UI", 18, "bold")).pack(anchor="w", pady=(0,12))
    status = tk.StringVar(value="Server startet …")
    ttk.Label(frame, textvariable=status).pack(anchor="w", pady=(0,15))
    ips = local_ips()
    ttk.Label(frame, text="Webinterface – Netzwerkadapter").pack(anchor="w")
    ip_choice = ttk.Combobox(frame, values=list(ips), state="readonly", width=65)
    ip_choice.set(next((k for k,v in ips.items() if v == BIND_IP), next(iter(ips))))
    ip_choice.pack(anchor="w", pady=(3,12))
    ttk.Label(frame, text="EW-DX Empfänger – Suchnetz (CIDR, z. B. 192.168.0.0/24)").pack(anchor="w")
    net_var = tk.StringVar(value=NETWORK)
    ttk.Entry(frame, textvariable=net_var, width=40).pack(anchor="w", pady=(3,12))
    ttk.Label(frame, text="Webserver-Port").pack(anchor="w")
    port_var = tk.StringVar(value=str(HTTP_PORT))
    ttk.Entry(frame, textvariable=port_var, width=12).pack(anchor="w", pady=(3,12))
    server = {"instance": None}
    def url():
        ip = BIND_IP
        if ip == "0.0.0.0":
            ip = "localhost"
        return f"http://{ip}:{HTTP_PORT}"
    def restart():
        global NETWORK, BIND_IP, HTTP_PORT
        try:
            port = int(port_var.get())
            if not 1024 <= port <= 65535: raise ValueError("Port muss zwischen 1024 und 65535 liegen")
            network = str(ipaddress.ip_network(net_var.get().strip(), strict=False))
            bind = ips[ip_choice.get()]
            old = server["instance"]
            # Bind first: if address/port invalid keep existing server alive.
            if old and (old.server_address[0], old.server_address[1]) == (bind, port):
                new_server = old
            else:
                new_server = ThreadingHTTPServer((bind, port), H)
            if old is not None and old is not new_server:
                old.shutdown(); old.server_close()
            if new_server is not old:
                server["instance"] = new_server
                threading.Thread(target=new_server.serve_forever, daemon=True).start()
            changed = network != NETWORK
            NETWORK, BIND_IP, HTTP_PORT = network, bind, port
            state["network"] = NETWORK
            SETTINGS.write_text(json.dumps({"network": NETWORK,"bind_ip": BIND_IP,"port": HTTP_PORT},indent=2),encoding="utf-8")
            status.set(f"● Server läuft: {url()}")
            if changed: status.set(status.get()+" | Suchnetz geändert (Neustart für sofortigen Scan)")
        except Exception as e:
            messagebox.showerror("Server konnte nicht gestartet werden", str(e))
    buttons = ttk.Frame(frame)
    buttons.pack(anchor="w", pady=12)
    ttk.Button(buttons,text="Open GUI",command=lambda: webbrowser.open(url())).pack(side="left",padx=(0,8))
    def copy_url():
        root.clipboard_clear(); root.clipboard_append(url()); root.update()
    ttk.Button(buttons,text="Copy URL",command=copy_url).pack(side="left",padx=(0,8))
    ttk.Button(buttons,text="Save & Restart Server",command=restart).pack(side="left")
    ttk.Label(frame,text="Das Dashboard ist auch von anderen Rechnern im LAN erreichbar.\nWindows-Firewall: eingehenden TCP-Port freigeben.",justify="left").pack(anchor="w",pady=12)
    def quit_app():
        if server["instance"]:
            server["instance"].shutdown(); server["instance"].server_close()
        root.destroy()
    root.protocol("WM_DELETE_WINDOW",quit_app)
    restart()
    root.mainloop()

if __name__ == "__main__":
    main()
