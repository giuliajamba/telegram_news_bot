import os
import re
import time
import hashlib
import datetime as dt
import xml.etree.ElementTree as ET
from threading import Lock
from typing import Dict, List, Tuple
from urllib.parse import quote_plus

import requests
from fastapi import FastAPI, Request
from fastapi.responses import PlainTextResponse
from supabase import create_client, Client
from dateutil import tz

app = FastAPI()

# -------------------- ENV --------------------
TELEGRAM_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_ROLE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

ROME_TZ = tz.gettz("Europe/Rome")
sb: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

# ---- GDELT cache & rate-limit guard ----
_GDELT_CACHE: dict = {}  # key -> (expires_ts, articles_list)
_GDELT_CACHE_TTL_SEC = 15 * 60  # 15 minuti
_GDELT_COOLDOWN_UNTIL = 0.0
_GDELT_LOCK = Lock()


# -------------------- Telegram helpers --------------------
def tg(method: str, payload: dict):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/{method}"
    r = requests.post(url, json=payload, timeout=25)

    if not r.ok:
        # log utile (NON stampa token; attenzione a non incollare questi log in chat con token)
        try:
            print(f"Telegram error {r.status_code}: {r.text}")
        except Exception:
            pass
        r.raise_for_status()

    return r.json()


def send_message(
    chat_id: int,
    text: str,
    reply_markup: dict | None = None,
    disable_preview: bool = True,
):
    # Telegram limit: 4096 chars. Stiamo larghi.
    if text and len(text) > 3500:
        text = text[:3490] + "…"

    payload = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": disable_preview,
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup
    return tg("sendMessage", payload)


def answer_callback(callback_query_id: str, text: str = ""):
    return tg("answerCallbackQuery", {"callback_query_id": callback_query_id, "text": text})


# -------------------- Supabase helpers --------------------
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
    rows = (
        sb.table("profile_weights")
        .select("key_type,key,weight")
        .eq("chat_id", chat_id)
        .execute()
        .data
        or []
    )
    return {(r["key_type"], r["key"]): float(r["weight"]) for r in rows}


# ---- Callback map: cb_id -> url (evita BUTTON_DATA_INVALID) ----
def make_cb_id(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()[:12]


def store_cb(chat_id: int, cb_id: str, url: str):
    sb.table("callback_map").upsert(
        {"chat_id": chat_id, "cb_id": cb_id, "url": url},
        on_conflict="chat_id,cb_id",
    ).execute()


def load_cb_url(chat_id: int, cb_id: str) -> str | None:
    row = (
        sb.table("callback_map")
        .select("url")
        .eq("chat_id", chat_id)
        .eq("cb_id", cb_id)
        .limit(1)
        .execute()
        .data
    )
    if not row:
        return None
    return row[0].get("url")


# ---- Sent articles: evita ripetizioni tra /digest e /piu ----
def remember_sent(chat_id: int, url: str):
    if not url:
        return
    sb.table("sent_articles").upsert(
        {"chat_id": chat_id, "url": url},
        on_conflict="chat_id,url",
    ).execute()


def get_recent_sent(chat_id: int, limit: int = 600) -> set[str]:
    rows = (
        sb.table("sent_articles")
        .select("url")
        .eq("chat_id", chat_id)
        .order("sent_at", desc=True)
        .limit(limit)
        .execute()
        .data
        or []
    )
    return {r["url"] for r in rows if r.get("url")}


# -------------------- News fetching (GDELT + Google News RSS) --------------------
TOPIC_SEEDS = {
    "appalti": ["appalti", "gara", "bando", "affidamento", "capitolato", "anac", "cig", "mepa"],
    "sanita": ["sanità", "azienda sanitaria", "ospedale", "asl", "ausl", "ssn", "lea"],
    "emilia-romagna": ["emilia-romagna", "bologna", "modena", "reggio emilia", "parma", "ravenna", "ferrara", "rimini"],
    "formula1": ["formula 1", "f1", "gran premio", "ferrari", "hamilton", "verstappen", "leclerc"],
}


def gdelt_search(query: str, max_records: int = 80) -> List[dict]:
    """
    Ritorna una lista di dict {title,url,description,...} o [].
    Ha cache 15 min e cooldown 10 min dopo 429.
    """
    global _GDELT_COOLDOWN_UNTIL

    now = time.time()
    if now < _GDELT_COOLDOWN_UNTIL:
        return []

    base_params = {
        "query": query,
        "mode": "ArtList",
        "format": "json",
        "maxrecords": str(max_records),
        "sort": "HybridRel",
    }

    def cached_call(params: dict, cache_key: str) -> List[dict]:
        global _GDELT_COOLDOWN_UNTIL

        # cache
        with _GDELT_LOCK:
            item = _GDELT_CACHE.get(cache_key)
            if item:
                exp, data = item
                if time.time() < exp:
                    return data
                _GDELT_CACHE.pop(cache_key, None)

        # call
        try:
            r = requests.get(
                "https://api.gdeltproject.org/api/v2/doc/doc",
                params=params,
                timeout=20,
            )
            if r.status_code == 429:
                _GDELT_COOLDOWN_UNTIL = time.time() + 10 * 60
                print(f"GDELT rate-limited (429). Cooldown fino a {_GDELT_COOLDOWN_UNTIL}.")
                return []

            r.raise_for_status()
            data = r.json()
            arts = data.get("articles", [])
            arts = arts if isinstance(arts, list) else []

            with _GDELT_LOCK:
                _GDELT_CACHE[cache_key] = (time.time() + _GDELT_CACHE_TTL_SEC, arts)

            return arts

        except Exception as e:
            print(f"GDELT error: {e}")
            return []

    # tentativo italiano
    arts_it = cached_call({**base_params, "lang": "italian"}, f"it|{max_records}|{query}")
    if arts_it:
        return arts_it

    # fallback senza filtro lingua
    return cached_call(base_params, f"any|{max_records}|{query}")


def google_news_rss_search(query: str, max_items: int = 120) -> List[dict]:
    """
    Fallback gratuito: RSS di Google News.
    Restituisce lista di dict con chiavi minime: title, url, description.
    """
    q = quote_plus(query)
    rss_url = f"https://news.google.com/rss/search?q={q}&hl=it&gl=IT&ceid=IT:it"

    try:
        r = requests.get(rss_url, timeout=20)
        r.raise_for_status()
        root = ET.fromstring(r.text)

        items = root.findall(".//item")
        out = []
        for it in items[:max_items]:
            title = (it.findtext("title") or "").strip()
            link = (it.findtext("link") or "").strip()
            desc = (it.findtext("description") or "").strip()
            if link:
                out.append({"title": title, "url": link, "description": desc})
        return out

    except Exception as e:
        print(f"Google News RSS error: {e}")
        return []


def candidate_articles() -> List[dict]:
    # Query “cerchi concentrici”: specifiche -> ampie
    queries = [
        # --- Modena: AUSL / Azienda USL ---
        '"AUSL Modena" OR "Azienda USL di Modena" OR "Azienda Usl di Modena" OR "USL Modena"',
        '("AUSL Modena" OR "Azienda USL di Modena") (sanità OR ospedale OR servizi OR "liste d\'attesa")',

        # --- Modena: AOU / Policlinico / Baggiovara ---
        '"Azienda Ospedaliero-Universitaria di Modena" OR "AOU Modena" OR Policlinico OR Baggiovara OR "Ospedale di Baggiovara"',
        '("AOU Modena" OR Policlinico OR Baggiovara OR "Azienda Ospedaliero-Universitaria di Modena") (sanità OR pronto soccorso OR reparto OR intervento)',

        # --- Persone (Altini, Baldino) ---
        'Altini (sanità OR AUSL OR AOU OR "Emilia-Romagna")',
        'Baldino (sanità OR AUSL OR AOU OR "Emilia-Romagna")',

        # --- Regione Emilia-Romagna SOLO sanità ---
        '("Regione Emilia-Romagna" OR "Emilia-Romagna" OR RER OR ER) (sanità OR sanitario OR ospedali OR AUSL OR AOU OR "liste d\'attesa")',

        # --- Sanità generale (Italia + internazionale) ---
        '(sanità OR sanitario OR ospedale OR SSN OR "Servizio sanitario nazionale" OR "liste d\'attesa") Italia',
        '(healthcare OR hospital OR "public health" OR NHS OR "health system") (Europe OR EU OR Italia)',

        # --- Appalti / contratti pubblici ---
        '(appalto OR "gara d\'appalto" OR bando OR affidamento OR "contratti pubblici" OR "codice dei contratti" OR "codice degli appalti" OR ANAC OR MEPA OR Consip) (sanità OR ospedale OR AUSL OR ASL OR AOU)',
        '(appalto OR "gara d\'appalto" OR bando OR affidamento OR "contratti pubblici" OR "codice dei contratti" OR "codice degli appalti" OR ANAC OR MEPA OR Consip) Italia',

        # --- Referendum ---
        'referendum Italia OR "referendum abrogativo" OR "quesiti referendari"',

        # --- Formula 1 ---
        '("Formula 1" OR F1)',
    ]

    arts: List[dict] = []

    # 1) prova GDELT
    for q in queries:
        arts.extend(gdelt_search(q, max_records=40))

    # 2) fallback RSS se pochi risultati
    if len(arts) < 80:
        for q in queries:
            arts.extend(google_news_rss_search(q, max_items=120))

    # dedup per URL
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
    for w in [
        "anac", "mepa", "affidamento", "gara", "bando",
        "ausl", "asl", "capitolato", "regione", "formula 1", "f1"
    ]:
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
    """
    Ritorna lista [(feats, article_dict), ...] lunga al massimo n,
    escludendo URL già inviati all'utente.
    """
    arts = candidate_articles()
    weights = get_weights(chat_id)
    sent = get_recent_sent(chat_id, limit=600)

    scored = []
    for a in arts:
        title = a.get("title") or ""
        desc = a.get("description") or ""
        url = a.get("url") or ""

        # NO ripetizioni
        if not url or url in sent:
            continue

        feats = extract_features(title, desc, url)
        s = score_article(weights, feats)
        scored.append((s, feats, a))

    scored.sort(key=lambda x: x[0], reverse=True)

    topic_count: Dict[str, int] = {}
    domain_count: Dict[str, int] = {}
    out: List[tuple] = []

    # Selezione "varia" (limiti per dominio e topic)
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

    # Piano B: se per i limiti varietà rimangono pochi risultati,
    # riempiamo senza limiti, pur sempre senza ripetizioni.
    if len(out) < n:
        already = {x[1].get("url") for x in out}
        for _, feats, a in scored:
            url = a.get("url") or ""
            if not url or url in already:
                continue
            out.append((feats, a))
            already.add(url)
            if len(out) >= n:
                break

    return out


def build_article_message(chat_id: int, i: int, a: dict) -> Tuple[str, dict]:
    title = a.get("title") or "(senza titolo)"
    url = a.get("url") or ""
    src = normalize_domain(url)

    text = f"{i}. {title}\n{src}\n{url}"

    cb_id = make_cb_id(url)
    store_cb(chat_id, cb_id, url)

    kb = {
        "inline_keyboard": [[
            {"text": "👍", "callback_data": f"like|{cb_id}"},
            {"text": "👎", "callback_data": f"dislike|{cb_id}"},
            {"text": "🔎", "callback_data": f"more|{cb_id}"},
            {"text": "⭐", "callback_data": f"follow|{cb_id}"},
            {"text": "🔕", "callback_data": f"less|{cb_id}"},
        ]]
    }
    return text, kb


# -------------------- FastAPI endpoints --------------------
@app.get("/health")
def health():
    return {"ok": True}


@app.get("/")
def root():
    return {"ok": True}


@app.post("/telegram/webhook")
async def telegram_webhook(req: Request):
    try:
        update = await req.json()

        # ---------- 1) Messaggi (comandi) ----------
        msg = update.get("message")
        if msg:
            chat_id = msg["chat"]["id"]
            text = (msg.get("text") or "").strip()
            ensure_user(chat_id)

            if text == "/start":
                send_message(
                    chat_id,
                    "Ciao! 👋\n"
                    "Comandi principali:\n"
                    "• /digest → 20 notizie (su richiesta)\n"
                    "• /piu → altre 10\n\n"
                    "Usa i bottoni 👍👎⭐🔕 sotto gli articoli per personalizzare."
                )

            elif text == "/digest":
                send_message(chat_id, "🗞️ Digest su richiesta (20 notizie):")
                picked = pick_digest(chat_id, n=20)

                if not picked:
                    send_message(chat_id, "😕 Al momento non riesco a recuperare notizie nuove. Riprova tra poco.")
                else:
                    for idx, (_, art) in enumerate(picked, start=1):
                        msg2, kb = build_article_message(chat_id, idx, art)
                        send_message(chat_id, msg2, reply_markup=kb)
                        remember_sent(chat_id, art.get("url") or "")

            elif text == "/piu":
                picked = pick_digest(chat_id, n=10)

                if not picked:
                    send_message(chat_id, "😕 Niente di nuovo al momento. Riprova tra poco.")
                else:
                    send_message(chat_id, "➕ Altre 10 notizie (nuove):")
                    for idx, (_, art) in enumerate(picked, start=1):
                        msg2, kb = build_article_message(chat_id, idx, art)
                        send_message(chat_id, msg2, reply_markup=kb)
                        remember_sent(chat_id, art.get("url") or "")

            elif text == "/meno":
                send_message(chat_id, "Ok. Usa 🔕 sulle notizie che vuoi vedere meno: abbasso automaticamente tema/fonte.")

            elif text == "/test":
                send_message(chat_id, "🧪 Test digest (5 notizie):")
                picked = pick_digest(chat_id, n=5)

                if not picked:
                    send_message(chat_id, "😕 Niente di nuovo al momento. Riprova tra poco.")
                else:
                    for idx, (_, art) in enumerate(picked, start=1):
                        msg2, kb = build_article_message(chat_id, idx, art)
                        send_message(chat_id, msg2, reply_markup=kb)
                        remember_sent(chat_id, art.get("url") or "")

            else:
                send_message(chat_id, "Ok 👍 Prova /digest oppure /piu.")

        # ---------- 2) Callback (bottoni) ----------
        cq = update.get("callback_query")
        if cq:
            callback_id = cq["id"]
            chat_id = cq["message"]["chat"]["id"]
            data = cq.get("data") or ""

            try:
                action, cb_id = data.split("|", 1)
            except ValueError:
                answer_callback(callback_id, "Ok")
                return {"ok": True}

            url = load_cb_url(chat_id, cb_id)
            if not url:
                answer_callback(callback_id, "Link scaduto. Riprova con /digest.")
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
