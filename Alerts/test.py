import logging
import json
import os
from datetime import datetime, timedelta, timezone

from pymongo import MongoClient
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

# ===================== CONFIG =====================
LOCAL_MONGO_URI = os.getenv("LOCAL_MONGO_URI", "mongodb://localhost:27017/")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
MAX_WORKERS = 20

# ===================== LOGGING =====================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [INFO] %(message)s"
)

# ===================== LOCAL DB (for creds + last_run + history) =====================
try:
    local_client = MongoClient(LOCAL_MONGO_URI)
except Exception as e:
    logging.error(f"❌ Failed to connect to LOCAL_MONGO_URI: {e}")
    raise

# ===================== UTIL =====================
def fmt_k(num):
    try:
        num = float(num)
        return f"{num/1000:.1f}k" if num >= 1000 else str(int(num))
    except:
        return "-"

def arrow(hr, prev):
    if hr > prev:
        return "↑"
    if hr < prev:
        return "↓"
    return "→"

def now_times():
    utc = datetime.now(timezone.utc)
    ist = utc.astimezone(timezone(timedelta(hours=5, minutes=30)))
    return utc, ist

# ===================== CHAT IDS =====================
def get_chat_ids():
    secret = os.getenv("CHAT_IDS_JSON")
    if secret:
        try:
            return json.loads(secret)
        except:
            logging.error("❌ CHAT_IDS_JSON secret invalid")
    if os.path.exists("chatids.json"):
        try:
            with open("chatids.json", "r") as f:
                return json.load(f)
        except:
            logging.error("❌ chatids.json invalid")
    logging.error("❌ No chat IDs found!")
    return {}

# ===================== DB HELPERS =====================
def get_client(uri):
    return MongoClient(uri, serverSelectionTimeoutMS=7000)

def get_db_and_collection(dtype, domain):
    clean = domain.split(".")[0].replace("/", "")
    dtype = dtype.lower()
    if dtype in ["programmatic", "prog", "porgrammatic"]:
        return f"{clean}_prod", "Target_P4_Opt"
    if "sub" in dtype:
        return "directclients_prod", "Target_P4_Opt"
    if "proxy" in dtype:
        return "prod_jobiak_ai", "job"
    return f"{clean}_prod", "Target_P4_Opt"

def get_remote_mongo_uri(db):
    rec = local_client["mongo_creds"]["creds"].find_one(
        {"domain": db},
        {"mongo_uri": 1}
    )
    return rec["mongo_uri"] if rec else None

# ===================== PROGRAMMATIC =====================
def count_programmatic(col, emp, start, end):
    q = {
        "gpost": 5,
        "job_status": {"$ne": 3},
        "gpost_date": {"$gte": start, "$lt": end},
        "employerId": emp if emp else {"$exists": True}
    }
    return col.count_documents(q)

def count_queue(col, emp):
    return col.count_documents({
        "gpost": 3,
        "job_status": {"$ne": 3},
        "employerId": emp if emp else {"$exists": True}
    })

# ===================== PROXY =====================
def proxy_hour(col, emp, start, end):
    pipeline = [
        {"$match": {
            "employerId": emp if emp else {"$exists": True},
            "datePosted": {"$gte": start, "$lt": end}
        }},
        {"$count": "count"}
    ]
    try:
        r = list(col.aggregate(pipeline, allowDiskUse=True, hint="employerId_1"))
    except:
        r = list(col.aggregate(pipeline, allowDiskUse=True))
    return r[0]["count"] if r else 0

def proxy_day(col, emp, start, end):
    pipeline = [
        {"$match": {
            "employerId": emp if emp else {"$exists": True},
            "datePosted": {"$gte": start, "$lt": end}
        }},
        {"$count": "count"}
    ]
    try:
        r = list(col.aggregate(pipeline, allowDiskUse=True, hint="employerId_1"))
    except:
        r = list(col.aggregate(pipeline, allowDiskUse=True))
    return r[0]["count"] if r else 0

# ===================== PROCESS DOMAIN =====================
def process_domain(domain, utc_now):
    name = domain["Domain"].strip().rstrip("/")
    dtype = domain.get("Domain Type", "").lower()
    emp = domain.get("EmployerId")
    quota = domain.get("Quota", 0)

    db, coll = get_db_and_collection(dtype, name)
    uri = get_remote_mongo_uri(db)
    if not uri:
        return None

    try:
        client = get_client(uri)
        col = client[db][coll]
        client.admin.command("ping")
    except:
        return None

    hr1_start = utc_now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
    hr1_end = hr1_start + timedelta(hours=1)
    hr2_start = hr1_start - timedelta(hours=1)
    hr2_end = hr1_start
    day_start = utc_now.replace(hour=0, minute=0, second=0, microsecond=0)

    if "proxy" in dtype:
        posted = proxy_day(col, emp, day_start, utc_now)
        hr = proxy_hour(col, emp, hr1_start, hr1_end)
        prev = proxy_hour(col, emp, hr2_start, hr2_end)
        queue_raw = 1
        queue_disp = "-"
    else:
        posted = count_programmatic(col, emp, day_start, utc_now)
        hr = count_programmatic(col, emp, hr1_start, hr1_end)
        prev = count_programmatic(col, emp, hr2_start, hr2_end)
        queue_raw = count_queue(col, emp)
        queue_disp = fmt_k(queue_raw)

    left = fmt_k(max(0, quota - posted))

    # Send alert if no posting in the last hour and quota left
    if hr == 0 and int(left.replace("k", "")) > 0:
        return None

    return {
        "Domain": name,
        "Posted": posted,
        "Hr": hr,
        "Prev": prev,
        "QueueRaw": queue_raw,
        "Queue": queue_disp,
        "Left": left
    }

# ===================== ALERT BUILDING =====================
def build_alerts(rows, last_rows, utc_now, ist_now):
    last_map = {x["Domain"]: x for x in last_rows} if last_rows else {}
    drops = []
    stopped = []
    no_postings = []

    for r in rows:
        old = last_map.get(r["Domain"])

        # Only append to drops if the difference is greater than 500
        if r["Hr"] < r["Prev"] and (r["Prev"] - r["Hr"]) >= 500:
            drops.append(r)

        if r["QueueRaw"] == 0 and r["Hr"] == 0:
            stopped.append(r)

        # Check if no postings were made in the last hour but quota is still left
        if r["Hr"] == 0 and int(r["Left"].replace("k", "")) > 0:
            no_postings.append(r)

    alerts = []

    def card(title, arr):
        msg = (
            f"⚠️ <b>{title}</b>\n"
            f"UTC {utc_now:%d %b %H:%M} | IST {ist_now:%H:%M}\n\n"
        )
        for x in arr:
            msg += (
                f"<b>{x['Domain']}</b>\n"
                f"• Posted: {fmt_k(x['Posted'])}\n"
                f"• Hr: {fmt_k(x['Hr'])} {arrow(x['Hr'], x['Prev'])} (prev {fmt_k(x['Prev'])})\n"
                f"• Queue: {x['Queue']}\n"
                f"• Left: {x['Left']}\n\n"
            )
        return msg

    if drops:
        alerts.append(card("POSTING DROPS", drops))
    if stopped:
        alerts.append(card("STOPPED — No Posts + No Queue", stopped))
    if no_postings:
        alerts.append(card("NO POSTS IN LAST HOUR", no_postings))

    return alerts

# ===================== SUMMARY =====================
def build_summary(rows, utc_now, ist_now, duration):
    msg = (
        f"📊 <b>Posting Summary</b>\n"
        f"UTC {utc_now:%d %b %H:%M} | IST {ist_now:%H:%M}\n\n"
    )

    for r in rows:
        queue_status = "No Queue" if r["QueueRaw"] == 0 else f"Queue {r['Queue']}"
        msg += (
            f"{r['Domain']}\n"
            f"Posted {fmt_k(r['Posted'])} | {queue_status}\n\n"
        )

    msg += f"⏱️ Duration: <b>{duration:.2f}s</b>"
    return msg

# ===================== TELEGRAM =====================
def send_telegram(msg, chatid):
    try:
        return requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": chatid, "text": msg, "parse_mode": "HTML"},
            timeout=15
        ).ok
    except:
        return False

# ===================== MAIN =====================
def main():
    start = datetime.now()
    utc_now, ist_now = now_times()

    domains = list(local_client["domain_postings"]["domains"].find({}, {"_id": 0}))
    last_rows = list(local_client["domain_postings"]["stats_last_run"].find({}, {"_id": 0}))

    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [ex.submit(process_domain, d, utc_now) for d in domains]
        for f in as_completed(futures):
            r = f.result()
            if r:
                results.append(r)

    if not results:
        return

    results.sort(key=lambda x: x["Posted"], reverse=True)
    duration = (datetime.now() - start).total_seconds()

    alerts = build_alerts(results, last_rows, utc_now, ist_now)
    summary = build_summary(results, utc_now, ist_now, duration)

    chat_ids = get_chat_ids()

    for cid in chat_ids.values():
        for a in alerts:
            send_telegram(a, cid)
        send_telegram(summary, cid)

    # Save last-run snapshot
    local_client["domain_postings"]["stats_last_run"].delete_many({})
    local_client["domain_postings"]["stats_last_run"].insert_many(results)

    # Save history
    local_client["domain_postings"]["stats_history"].insert_one({
        "time_utc": utc_now,
        "time_ist": ist_now,
        "duration": duration,
        "domains": results
    })

    logging.info(f"Run completed in {duration:.2f}s")

if __name__ == "__main__":
    main()
