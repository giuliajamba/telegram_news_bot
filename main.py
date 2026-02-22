import os
import re
import datetime as dt
from typing import Dict, List, Tuple

import requests
from fastapi import FastAPI, Request
from fastapi.responses import PlainTextResponse
from supabase import create_client, Client
from dateutil import tz

app = FastAPI()

TELEGRAM_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_ROLE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

ROME_TZ = tz.gettz("Europe/Rome")
sb: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)


# ---------- Telegram helpers ----------
def tg(method: str, payload: dict):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/{method}"
    r = requests.post(url, json=payload, timeout=25)
    r.raise_for_status()
    return r.json()

def send_message(chat_id: int, text: str, reply_markup: dict | None = None, disable_preview: bool = True):
    payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": disable_preview}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    return tg("sendMessage", payload)

def answer_callback(callback_query_id: str, text: str = ""):
    return tg("answerCallbackQuery", {"callback_query_id": callback_query_id, "text": text})


# ---------- Supabase helpers ----------
def ensure_user(chat_id: int):
    sb.table("users").upsert({"chat_id": chat_id}, on_conflict="chat_id").execute()

def add_feedback(chat_id: int, url: str, action: str):
    sb.table("feedback").insert({"chat_id": chat_id, "article_url": url, "action": action}).execute()

def bump_weight(chat_id: int, key_type: str, key: str, delta: float):
    existing = (
        sb.table("profile_weights")
        .select("weight")
        .eq("chat_id", chat_id)
        .eq("key_type", key_type)
        .eq("key", key)
        .execute()
    )
    if existing.data:
        w = float(existing.data[0]["weight"]) + delta
        (
            sb.table("profile_weights")
            .update({"weight": w, "updated_at": "now()"})
            .eq("chat_id", chat_id)
            .eq("key_type", key_type)
            .eq("key", key)
            .execute()
        )
    else:
        sb.table("profile_weights").insert(
            {"chat_id": chat_id, "key_type": key_type, "key": key, "weight": delta}
        ).execute()

def get_weights(chat_id: int) -> Dict[Tuple[str, str], float]:
    rows = sb.table("profile_weights").select("key_type,key,weight").eq("chat_id", chat_id).execute().data or []
    return {(r["key_type"], r["key"]): float(r["weight"]) for r in rows}


# ---------- News fetching (GDELT) ----------
TOPIC_SEEDS = {
    "appalti": ["appalti", "gara", "bando", "affidamento", "capitolato", "anac", "cig", "mepa"],
    "sanita": ["sanità", "azienda sanitaria", "ospedale", "asl", "ausl", "ssn", "lea"],
    "emilia-romagna": ["emilia-romagna", "bologna", "modena", "reggio emilia", "parma", "ravenna", "ferrara", "rimini"],
    "formula1": ["formula 1", "f1", "gran premio", "ferrari", "hamilton", "verstappen", "leclerc"],
}

def gdelt_search(query: str, max_records: int = 80) -> List[dict]:
    params = {
        "query": query,
        "mode": "ArtList",
        "format": "json",
        "maxrecords": str(max_records),
        "sort": "HybridRel",
        "lang": "italian",
    }
    try:
        r = requests.get("https://api.gdeltproject.org/api/v2/doc/doc", params=params, timeout=25)
        r.raise_for_status()
        data = r.json()
        arts = data.get("articles", [])
        return arts if isinstance(arts, list) else []
    except Exception as e:
        # Evita 500: se GDELT non risponde o risponde male, non crashare
        print(f"GDELT error: {e}")
        return []

def candidate_articles() -> List[dict]:
    queries = [
        "Italia (appalti OR gara OR bando OR affidamento OR ANAC)",
        "Italia (sanità OR azienda sanitaria OR ospedale OR ASL OR AUSL)",
        "Emilia-Romagna (sanità OR appalti OR gara OR bando)",
        "(Formula 1 OR F1) Italia"
    ]
    arts: List[dict] = []
    for q in queries:
        arts.extend(gdelt_search(q, max_records=80))
    seen = set()
    out = []
    for a in arts:
        url = a.get("url") or ""
        if not url or url in seen:
            continue
        seen.add(url)
        out.append(a)
    return out

def normalize_domain(url: str) -> str:
    m = re.match(r"https?://([^/]+)/", url)
    return (m.group(1).lower() if m else "unknown")

def extract_features(title: str, desc: str, url: str) -> Dict[str, List[str]]:
    text = f"{title} {desc}".lower()
    topics = []
    for t, seeds in TOPIC_SEEDS.items():
        if any(s in text for s in seeds):
            topics.append(t)
    domain = normalize_domain(url)
    terms = []
    for w in ["anac", "mepa", "affidamento", "gara", "bando", "ausl", "asl", "capitolato", "regione", "formula 1", "f1"]:
        if w in text:
            terms.append(w)
    return {"topics": topics, "domain": [domain], "terms": terms}

def score_article(weights: Dict[Tuple[str, str], float], feats: Dict[str, List[str]]) -> float:
    s = 0.0
    for t in feats["topics"]:
        s += weights.get(("topic", t), 0.0)
    for d in feats["domain"]:
        s += weights.get(("source", d), 0.0)
    for term in feats["terms"]:
        s += weights.get(("term", term), 0.0)
    return s + 0.2  # base score

def pick_digest(chat_id: int, n: int = 20) -> List[tuple]:
    arts = candidate_articles()
    weights = get_weights(chat_id)
    scored = []
    for a in arts:
        title = a.get("title") or ""
        desc = a.get("description") or ""
        url = a.get("url") or ""
        feats = extract_features(title, desc, url)
        s = score_article(weights, feats)
        scored.append((s, feats, a))

    scored.sort(key=lambda x: x[0], reverse=True)

    topic_count: Dict[str, int] = {}
    domain_count: Dict[str, int] = {}
    out = []
    for _, feats, a in scored:
        dom = feats["domain"][0] if feats["domain"] else "unknown"
        if domain_count.get(dom, 0) >= 4:
            continue
        ok = True
        for t in feats["topics"]:
            if topic_count.get(t, 0) >= 4:
                ok = False
                break
        if not ok:
            continue
        out.append((feats, a))
        domain_count[dom] = domain_count.get(dom, 0) + 1
        for t in feats["topics"]:
            topic_count[t] = topic_count.get(t, 0) + 1
        if len(out) >= n:
            break
    return out

def build_article_message(i: int, a: dict) -> Tuple[str, dict]:
    title = a.get("title") or "(senza titolo)"
    url = a.get("url") or ""
    src = normalize_domain(url)
    text = f"{i}. {title}\n{src}\n{url}"
    kb = {
        "inline_keyboard": [[
            {"text": "👍", "callback_data": f"like|{url}"},
            {"text": "👎", "callback_data": f"dislike|{url}"},
            {"text": "🔎", "callback_data": f"more|{url}"},
            {"text": "⭐", "callback_data": f"follow|{url}"},
            {"text": "🔕", "callback_data": f"less|{url}"},
        ]]
    }
    return text, kb


# ---------- Digest scheduling ----------
def rome_now() -> dt.datetime:
    return dt.datetime.now(tz=ROME_TZ)

def should_send(slot: str, now_local: dt.datetime) -> bool:
    target_hour = 9 if slot == "AM" else 21
    return now_local.hour == target_hour and now_local.minute < 15

def mark_sent(chat_id: int, slot: str, today: dt.date):
    col = "last_digest_am" if slot == "AM" else "last_digest_pm"
    sb.table("users").update({col: str(today)}).eq("chat_id", chat_id).execute()

def already_sent(chat_id: int, slot: str, today: dt.date) -> bool:
    col = "last_digest_am" if slot == "AM" else "last_digest_pm"
    row = sb.table("users").select(col).eq("chat_id", chat_id).execute().data
    if not row:
        return False
    v = row[0].get(col)
    return str(v) == str(today)

def send_digest(chat_id: int, slot: str):
    ensure_user(chat_id)
    now_local = rome_now()
    today = now_local.date()
    if already_sent(chat_id, slot, today):
        return

    header = "🗞️ Digest 09:00" if slot == "AM" else "🗞️ Digest 21:00"
    send_message(chat_id, f"{header} (personalizzato: usa 👍👎⭐🔕)\n20 notizie:")

    picked = pick_digest(chat_id, n=20)
    for idx, (_, art) in enumerate(picked, start=1):
        msg, kb = build_article_message(idx, art)
        send_message(chat_id, msg, reply_markup=kb)

    mark_sent(chat_id, slot, today)

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/tick")
async def tick(req: Request):
    body = await req.json()
    secret = body.get("secret")
    if secret != os.environ.get("TICK_SECRET"):
        return PlainTextResponse("forbidden", status_code=403)

    now_local = rome_now()
    users = sb.table("users").select("chat_id").execute().data or []
    for u in users:
        chat_id = int(u["chat_id"])
        if should_send("AM", now_local):
            send_digest(chat_id, "AM")
        if should_send("PM", now_local):
            send_digest(chat_id, "PM")
    return {"ran": True, "time": now_local.isoformat(), "users": len(users)}

@app.post("/telegram/webhook")
async def telegram_webhook(req: Request):
    try:
        update = await req.json()

        # ---- 1) Gestione messaggi (comandi /start /piu /meno /test) ----
        msg = update.get("message")
        if msg:
            chat_id = msg["chat"]["id"]
            text = (msg.get("text") or "").strip()
            ensure_user(chat_id)

            if text == "/start":
                send_message(
                    chat_id,
                    "Ciao! Ti mando 2 digest al giorno (09:00 e 21:00).\n"
                    "Usa i bottoni 👍👎⭐🔕 per farmi capire cosa ti interessa.\n"
                    "Comandi: /piu (altre 10), /meno (riduci rumore)."
                )

            elif text == "/piu":
                picked = pick_digest(chat_id, n=10)
                if not picked:
                    send_message(chat_id, "😕 Al momento non riesco a recuperare notizie. Riprova tra poco.")
                else:
                    send_message(chat_id, "➕ Altre 10 notizie:")
                    for idx, (_, art) in enumerate(picked, start=1):
                        msg2, kb = build_article_message(idx, art)
                        send_message(chat_id, msg2, reply_markup=kb)

            elif text == "/meno":
                send_message(chat_id, "Ok. Usa 🔕 sulle notizie che vuoi vedere meno: abbasso tema e fonte automaticamente.")

            elif text == "/test":
                send_message(chat_id, "🧪 Test digest (5 notizie):")
                picked = pick_digest(chat_id, n=5)
                if not picked:
                    send_message(chat_id, "😕 Al momento non riesco a recuperare notizie. Riprova tra poco.")
                else:
                    for idx, (_, art) in enumerate(picked, start=1):
                        msg2, kb = build_article_message(idx, art)
                        send_message(chat_id, msg2, reply_markup=kb)

            else:
                send_message(chat_id, "Ok 👍 Usa /piu per altre notizie o i bottoni sotto ogni articolo.")

        # ---- 2) Gestione click sui bottoni (callback_query) ----
        cq = update.get("callback_query")
        if cq:
            callback_id = cq["id"]
            chat_id = cq["message"]["chat"]["id"]
            data = cq.get("data") or ""

            try:
                action, url = data.split("|", 1)
            except ValueError:
                answer_callback(callback_id, "Ok")
                return {"ok": True}

            ensure_user(chat_id)
            add_feedback(chat_id, url, action.upper())

            domain = normalize_domain(url)
            if action == "like":
                bump_weight(chat_id, "source", domain, +0.4)
                answer_callback(callback_id, "Segnato 👍")
            elif action == "dislike":
                bump_weight(chat_id, "source", domain, -0.5)
                answer_callback(callback_id, "Segnato 👎")
            elif action == "follow":
                bump_weight(chat_id, "source", domain, +0.9)
                answer_callback(callback_id, "Ok ⭐ (più simili)")
            elif action == "less":
                bump_weight(chat_id, "source", domain, -1.0)
                answer_callback(callback_id, "Ok 🔕 (meno simili)")
            elif action == "more":
                answer_callback(callback_id, "Apri il link: se ti piace metti 👍")
            else:
                answer_callback(callback_id, "Ok")

        return {"ok": True}

    except Exception as e:
        print(f"Webhook error: {e}")
        return {"ok": True}
