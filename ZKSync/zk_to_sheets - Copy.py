import os, time
from zk import ZK
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from collections import defaultdict
from datetime import datetime, timedelta


DEVICES = {
    "192.168.1.206": " 509 IN",    # Entry device
    "192.168.1.205": " 509 OUT",   # Exit device
     "192.168.1.207": " 609 IN",    # Entry device
    "192.168.1.208": " 609 OUT",   # Exit device
}
DEVICE_PORT = 4370

SHEET_ID   = "1Q5zQWb2WsLFeLhdOqYqppadwfasqBTBVl58xBTZm7gk"   # <-- set this
SA_JSON    = "service_account.json"
REGISTER_TAB = "DailyRegister"
RAWLOG_TAB   = "RawLogs"

# Shifts
DEFAULT_SHIFTS = {
    "Morning": {"start": "08:00", "expected": 7, "length": 8},
    "Evening": {"start": "16:00", "expected": 7, "length": 8},
    "Night":   {"start": "00:00", "expected": 7, "length": 8},
}
CUSTOM_SHIFTS = {
    "EH00009": {"name": "Zohaib", "start": "16:00", "expected": 9,  "length": 10},
    "EH00049": {"name": "Bilal",  "start": "18:00", "expected": 12, "length": 14},
    "EH00020": {"name": "Hamza",  "start": "21:00", "expected": 10, "length": 11},
}
# Attendance window start
WINDOW_START_HHMM = "08:00"
# ==========================================

HEADERS_REG = [
    "Date","Shift","UserID","UserName",
    "Time In","Time Out","Worked Hours",
    "Sitting Hours","Shift Length",
    "Overtime","Undertime","Late","Late Minutes",
    "Attendance","Punch Count","Outside Duration"
]
HEADERS_RAW = ["UserID","UserName","Punch Date","Punch Time","DeviceIP","Type"]

# ---------- helpers ----------
def authorize():
    scope = ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(SA_JSON, scope)
    client = gspread.authorize(creds)
    sh = client.open_by_key(SHEET_ID)
    return sh

def get_or_make(sh, tab, headers):
    try:
        ws = sh.worksheet(tab)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=tab, rows=40000, cols=len(headers))
        ws.append_row(headers)
        return ws
    # fix header if needed
    current = ws.row_values(1)
    if current != headers:
        ws.delete_rows(1)
        ws.insert_row(headers, 1)
    return ws

def fmt_time(dt):  return dt.strftime("%I:%M %p") if dt else ""
def fmt_date(dt):  return dt.strftime("%Y-%m-%d") if dt else ""
def minutes(a,b):  return int((b - a).total_seconds() // 60)
def hhmm(mins, blank=True):
    if mins is None: return ""
    if mins < 0: mins = 0
    h,m = divmod(mins,60)
    if mins==0 and blank: return ""
    return f"{h:02d}:{m:02d}"
def parse_hhmm(s): return datetime.strptime(s,"%H:%M").time()

# ------------------------------------------

def fetch_logs():
    """Fetch all logs from all devices"""
    logs = []
    users = {}
    for ip, dtype in DEVICES.items():
        zk = ZK(ip, port=DEVICE_PORT, timeout=10)
        conn = None
        try:
            conn = zk.connect(); conn.disable_device()
            for u in conn.get_users():
                users[u.user_id] = u.name
            for log in conn.get_attendance():
                logs.append((log.user_id, users.get(log.user_id,""), log.timestamp, ip, dtype))
        except Exception as e:
            print(f"Error {ip}: {e}")
        finally:
            if conn:
                conn.enable_device(); conn.disconnect()
    logs.sort(key=lambda x:x[2])
    return logs, users

def update_rawlogs(ws, logs):
    """Append new logs without duplicates"""
    existing = set()
    try:
        ids = ws.col_values(1)[1:]  # all UserIDs (skip header)
        times = ws.col_values(3)[1:] # Punch Date
        stamps = ws.col_values(4)[1:] # Punch Time
        existing = set(f"{u}|{d}|{t}" for u,d,t in zip(ids,times,stamps))
    except Exception:
        pass

    new_rows = []
    for uid, uname, ts, ip, dtype in logs:
        key = f"{uid}|{ts.date()}|{ts.strftime('%H:%M:%S')}"
        if key not in existing:
            new_rows.append([
                uid, uname, fmt_date(ts), ts.strftime("%H:%M:%S"), ip, dtype
            ])

    if new_rows:
        ws.append_rows(new_rows, value_input_option="RAW")
        print(f"RawLogs: added {len(new_rows)} new rows")
    else:
        print("RawLogs: no new rows")

def build_dailyregister(ws, logs, users):
    """Summarize logs into daily register (1 row per user per day)"""
    today = datetime.now().date()
    window_start = datetime.combine(today, parse_hhmm(WINDOW_START_HHMM))
    window_end = window_start + timedelta(days=1)

    by_user = defaultdict(list)
    for uid, uname, ts, ip, dtype in logs:
        if window_start <= ts < window_end:
            by_user[uid].append((ts, dtype, ip, uname))
    for uid in by_user:
        by_user[uid].sort(key=lambda x:x[0])

    rows = []
    for uid, uname in users.items():
        punches = by_user.get(uid, [])
        if punches:
            in_times  = [ts for ts,d,_,_ in punches if d=="IN"]
            out_times = [ts for ts,d,_,_ in punches if d=="OUT"]
            tin  = min(in_times) if in_times else None
            tout = max(out_times) if out_times else None

            # shift config
            h = (tin or punches[0][0]).hour
            if uid in CUSTOM_SHIFTS:
                cfg = CUSTOM_SHIFTS[uid]
                shift_name = cfg["name"]
            elif 8 <= h < 16:
                shift_name,cfg = "Morning", DEFAULT_SHIFTS["Morning"]
            elif 16 <= h < 24:
                shift_name,cfg = "Evening", DEFAULT_SHIFTS["Evening"]
            else:
                shift_name,cfg = "Night", DEFAULT_SHIFTS["Night"]

            sitting_min = cfg["expected"]*60
            length_min  = cfg["length"]*60

            # worked
            worked_mins = None
            if tin and tout and tout>=tin:
                worked_mins = minutes(tin,tout)

            # late
            late_flag, late_mins_val = "", None
            if tin:
                shift_start = datetime.combine(tin.date(), parse_hhmm(cfg["start"]))
                mins_late = minutes(shift_start, tin)
                late_flag = "Yes" if mins_late>15 else ""
                late_mins_val = max(mins_late,0)

            overtime  = hhmm(worked_mins-length_min) if worked_mins and worked_mins>length_min else ""
            undertime = hhmm(sitting_min-worked_mins) if worked_mins and worked_mins<sitting_min else hhmm(sitting_min)

            # outside
            outside_total=0
            for i in range(len(punches)-1):
                t1,d1,_,_ = punches[i]
                t2,d2,_,_ = punches[i+1]
                if d1=="OUT" and d2=="IN":
                    outside_total+=minutes(t1,t2)

            rows.append([
                fmt_date(tin) if tin else str(window_start.date()),
                shift_name, uid, uname,
                fmt_time(tin), fmt_time(tout),
                hhmm(worked_mins, False) if worked_mins else "",
                hhmm(sitting_min, False), hhmm(length_min, False),
                overtime, undertime,
                late_flag, hhmm(late_mins_val, False) if late_mins_val is not None else "",
                "Present", len(punches), hhmm(outside_total, False) if outside_total else "00:00"
            ])
        else:
            # absent
            if uid in CUSTOM_SHIFTS: cfg=CUSTOM_SHIFTS[uid]; shift_name=cfg["name"]
            else: cfg=DEFAULT_SHIFTS["Morning"]; shift_name="Morning"
            sitting_min=cfg["expected"]*60; length_min=cfg["length"]*60
            rows.append([
                str(window_start.date()), shift_name, uid, uname,
                "","","",
                hhmm(sitting_min, False), hhmm(length_min, False),
                " ", hhmm(sitting_min, False),
                "","","Absent",0,"00:00"
            ])

    # clear old rows for today
    all_vals = ws.get_all_values()
    to_delete = []
    for i,row in enumerate(all_vals[1:], start=2):
        if row and row[0]==str(window_start.date()):
            to_delete.append(i)
    for i in reversed(to_delete):
        ws.delete_rows(i)

    if rows:
        ws.append_rows(rows, value_input_option="RAW")
        print(f"DailyRegister: wrote {len(rows)} rows")
    else:
        print("DailyRegister: no rows")
        

# -------------------------------
def main():
    sh = authorize()
    ws_reg = get_or_make(sh, REGISTER_TAB, HEADERS_REG)
    ws_raw = get_or_make(sh, RAWLOG_TAB, HEADERS_RAW)

    logs, users = fetch_logs()
    update_rawlogs(ws_raw, logs)
    #build_dailyregister(ws_reg, logs, users)

if __name__ == "__main__":
    main()
