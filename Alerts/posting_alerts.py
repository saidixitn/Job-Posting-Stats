import logging
import json
import os
from datetime import datetime, timedelta, timezone

from pymongo import MongoClient
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed


# ===================== CONFIG =====================
LOCAL_MONGO_URI = "mongodb://localhost:27017/"
TELEGRAM_BOT_TOKEN = "8384963131:AAHeirGrDv9bcOXOEFKOhzsAErCC7Wwt4go"
MAX_WORKERS = 20


# ===================== LOGGING =====================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [INFO] %(message)s"
)

local_client = MongoClient(LOCAL_MONGO_URI)


# ===================== UTIL =====================
def fmt_k(num):
    try:
        num = float(num)
        return f"{num/1000:.1f}k" if num >= 1000 else str(int(num))
    except:
        return "-"


def now_times():
    utc = datetime.now(timezone.utc)
    ist = utc.astimezone(timezone(timedelta(hours=5, minutes=30)))
    return utc, ist


# ===================== CHAT IDS =====================
def get_chat_ids():
    if os.path.exists("chatids.json"):
        try:
            with open("chatids.json", "r") as f:
                return json.load(f)
        except:
            logging.error("chatids.json invalid")

    secret = os.getenv("CHAT_IDS_JSON")
    if secret:
        try:
            return json.loads(secret)
        except:
            logging.error("CHAT_IDS_JSON secret invalid")

    return {}


# ===================== DB CLIENT =====================
def get_client(uri):
    return MongoClient(uri, serverSelectionTimeoutMS=7000)


# ===================== DB HELPERS =====================
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


# ===================== PROXY HOURLY FIX (FINAL) =====================
def proxy_hour(col, emp, start, end):
    pipeline = [
        {
            "$match": {
                "employerId": emp if emp else {"$exists": True},
                "datePosted": {"$gte": start, "$lt": end}
            }
        },
        {"$count": "count"}
    ]
    try:
        res = list(col.aggregate(pipeline, allowDiskUse=True, hint="employerId_1"))
    except:
        res = list(col.aggregate(pipeline, allowDiskUse=True))
    return res[0]["count"] if res else 0


def proxy_day(col, emp, day_start, day_end):
    pipeline = [
        {
            "$match": {
                "employerId": emp if emp else {"$exists": True},
                "datePosted": {"$gte": day_start, "$lt": day_end}
            }
        },
        {"$count": "count"}
    ]
    try:
        res = list(col.aggregate(pipeline, allowDiskUse=True, hint="employerId_1"))
    except:
        res = list(col.aggregate(pipeline, allowDiskUse=True))
    return res[0]["count"] if res else 0


# ===================== PROCESS DOMAIN =====================
def process_domain(domain, utc_now):
    name = domain["Domain"].strip().rstrip("/")
    dtype = domain.get("Domain Type", "").lower()
    emp = domain.get("EmployerId")
    quota = domain.get("Quota", 0)

    db, coll = get_db_and_collection(dtype, name)
    uri = get_remote_mongo_uri(db)

    if not uri:
        logging.error(f"No URI for DB {db}")
        return None

    try:
        client = get_client(uri)
        col = client[db][coll]
        client.admin.command("ping")
    except Exception as e:
        logging.error(f"Cannot connect to DB for {name}: {e}")
        return None

    # Proper aligned hour windows
    hr1_start = utc_now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
    hr1_end = hr1_start + timedelta(hours=1)

    hr2_start = hr1_start - timedelta(hours=1)
    hr2_end = hr1_start

    day_start = utc_now.replace(hour=0, minute=0, second=0, microsecond=0)

    if "proxy" in dtype:
        posted = proxy_day(col, emp, day_start, utc_now)
        hr = proxy_hour(col, emp, hr1_start, hr1_end)
        prev = proxy_hour(col, emp, hr2_start, hr2_end)
        queue = "-"
    else:
        posted = count_programmatic(col, emp, day_start, utc_now)
        hr = count_programmatic(col, emp, hr1_start, hr1_end)
        prev = count_programmatic(col, emp, hr2_start, hr2_end)
        queue = fmt_k(count_queue(col, emp))

    left = fmt_k(max(0, quota - posted))

    logging.info(f"{name}: Hr={hr} Prev={prev} Posted={posted}")

    return {
        "Domain": name,
        "Posted": posted,
        "Hr": hr,
        "Prev": prev,
        "Queue": queue,
        "Left": left
    }


# ===================== TREND =====================
def trend(hr, prev):
    if hr > prev:
        return "up"
    if hr < prev:
        return "down"
    return "steady"


# ===================== DROP ALERT =====================
def build_drop_alert(rows, utc_now, ist_now):
    drops = [r for r in rows if r["Hr"] < r["Prev"] or (r["Hr"] == 0 and r["Prev"] > 0)]

    if not drops:
        return None

    header = (
        "⚠️ <b>POSTING DROP ALERTS</b>\n"
        f"UTC {utc_now:%d %b %H:%M} | IST {ist_now:%H:%M}\n\n"
    )

    blocks = []
    for r in drops:
        blocks.append(
            f"<b>{r['Domain']}</b>\n"
            f"Post {fmt_k(r['Posted'])} | Hr {fmt_k(r['Hr'])} | {trend(r['Hr'], r['Prev'])}\n"
            f"Queue {r['Queue']} | Left {r['Left']}\n"
        )

    return header + "<pre>" + "\n".join(blocks) + "</pre>"


# ===================== SUMMARY =====================
def build_summary(rows, utc_now, ist_now, duration):
    header = (
        f"📊 <b>Posting Summary</b>\n"
        f"UTC {utc_now:%d %b %H:%M} | IST {ist_now:%H:%M}\n\n"
    )

    table = [
        "Domain                    | Posted | Queue",
        "-----------------------------------------------"
    ]

    for r in rows:
        table.append(
            f"{r['Domain'][:24]:<24} | "
            f"{fmt_k(r['Posted']):>6} | "
            f"{r['Queue']:>6}"
        )

    footer = f"\n⏱️ Duration: <b>{duration:.2f}s</b>"

    return header + "<pre>" + "\n".join(table) + "</pre>" + footer


# ===================== TELEGRAM =====================
def send_telegram(msg, chatid):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    return requests.post(
        url,
        json={"chat_id": chatid, "text": msg, "parse_mode": "HTML"},
        timeout=15
    ).ok


# ===================== MAIN =====================
def main():
    start = datetime.now()
    utc_now, ist_now = now_times()

    domains = list(local_client["domain_postings"]["domains"].find({}, {"_id": 0}))
    logging.info(f"Loaded {len(domains)} domains")

    results = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [ex.submit(process_domain, d, utc_now) for d in domains]
        for f in as_completed(futures):
            r = f.result()
            if r:
                results.append(r)

    if not results:
        logging.error("No domain results!")
        return

    results.sort(key=lambda x: x["Posted"], reverse=True)
    duration = (datetime.now() - start).total_seconds()

    drop_msg = build_drop_alert(results, utc_now, ist_now)
    summary_msg = build_summary(results, utc_now, ist_now, duration)

    chat_ids = get_chat_ids()

    for chatid in chat_ids.values():
        if drop_msg:
            send_telegram(drop_msg, chatid)
        send_telegram(summary_msg, chatid)

    logging.info(f"Run completed in {duration:.2f}s")


if __name__ == "__main__":
    main()
