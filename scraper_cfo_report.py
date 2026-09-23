#!/usr/bin/env python3
"""
CFO Weekly News Scraper — Luisa Spagnoli S.p.A.  (v2)
=======================================================

Rispetto alla v1:
  - Nuove categorie: "competitors" (comparable quotati: Moncler, Brunello
    Cucinelli, Kering, LVMH, Capri, Tapestry, Prada...), "commodities_fx"
    (materie prime — lana, cashmere, cotone — e cambi EUR/USD, EUR/GBP,
    cruciali per un'azienda che esporta e acquista fibre).
  - Nuove testate: Fashion United, Drapers, Just Style, The Fashion Law,
    MFFashion / Milano Finanza, Il Fatto Quotidiano Economia, Confindustria
    Moda / Sistema Moda Italia.
  - Relevance score (non più match binario): pesa di più i brand/competitor
    citati nel titolo, meno se solo nel summary; ordina per (data, score).
  - Filtro di recency: scarta articoli più vecchi di N giorni (default 10)
    per ridurre rumore, configurabile da CLI.
  - Fetch concorrente (ThreadPoolExecutor) con retry/backoff e User-Agent,
    per velocità e robustezza contro rate-limit di Google News.
  - Pulizia HTML dai summary, deduplica anche per titolo normalizzato
    (oltre che per URL), non solo per URL esatto.
  - Log strutturato invece di print sparsi; CLI configurabile.

Uso:
    pip install feedparser requests
    python3 scraper_cfo_report.py --days 10 --min-score 1 --output data.json
"""

import argparse
import concurrent.futures
import hashlib
import html
import json
import logging
import re
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import quote

import feedparser
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("cfo-scraper")

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)

# ---------------------------------------------------------------------
# Fonti curate, raggruppate per categoria (coerenti con i tab della dashboard)
# ---------------------------------------------------------------------
CATEGORY_SOURCES = {
    "fashion": [  # Moda Business, Trend & Supply Chain
        ("Business of Fashion", "businessoffashion.com"),
        ("WWD", "wwd.com"),
        ("Vogue Business", "voguebusiness.com"),
        ("Pambianco News", "pambianconews.com"),
        ("FashionNetwork Italia", "it.fashionnetwork.com"),
        ("Fashion United", "fashionunited.com"),
        ("Drapers", "drapersonline.com"),
        ("Just Style", "just-style.com"),
        ("The Fashion Law", "thefashionlaw.com"),
    ],
    "macro": [  # Finanza Globale & Macroeconomia
        ("Financial Times", "ft.com"),
        ("Bloomberg", "bloomberg.com"),
        ("Reuters", "reuters.com"),
        ("CNBC", "cnbc.com"),
    ],
    "tax": [  # Fisco, dazi, export — il riferimento italiano del CFO
        ("Il Sole 24 Ore", "ilsole24ore.com"),
        ("MF Fashion / Milano Finanza", "mffashion.com"),
        ("Confindustria Moda", "confindustriamoda.it"),
    ],
    "esg": [  # Sostenibilità, Compliance & Innovazione
        ("Sourcing Journal", "sourcingjournal.com"),
        ("Harvard Business Review", "hbr.org"),
    ],
    "competitors": [  # Comparable quotati: risultati trimestrali, guidance, M&A
        ("Reuters - Luxury/Fashion co.", "reuters.com"),
        ("Bloomberg - Luxury/Fashion co.", "bloomberg.com"),
        ("Business of Fashion - Competitors", "businessoffashion.com"),
        ("Fashion Network - Competitors", "it.fashionnetwork.com"),
    ],
    "commodities_fx": [  # Materie prime (lana, cashmere, cotone) e cambi
        ("Reuters - Commodities/FX", "reuters.com"),
        ("Just Style - Sourcing/Raw materials", "just-style.com"),
        ("Sourcing Journal - Raw materials", "sourcingjournal.com"),
    ],
}

CATEGORY_LABELS = {
    "fashion": "Fashion Business",
    "macro": "Macro & Mercati",
    "tax": "Finanza & Fisco",
    "esg": "ESG & Supply Chain",
    "competitors": "Competitor Quotati",
    "commodities_fx": "Materie Prime & Cambi",
}

# ---------------------------------------------------------------------
# Parole chiave per categoria: ogni categoria usa il proprio set, così una
# fonte generalista come Reuters/Bloomberg non torna sempre gli stessi
# articoli macro in ogni tab.
# ---------------------------------------------------------------------
THEME_KEYWORDS = [
    "quarterly results", "trimestrale", "semestrale", "M&A", "acquisizione",
    "fusione", "supply chain", "filiera", "luxury market", "mercato del lusso",
    "ESG", "export", "dazi", "tariffs", "dogane", "import", "guidance",
    "profit warning", "IPO", "buyback", "dividendo",
]

BRAND_KEYWORDS = [
    "Luisa Spagnoli", "Max Mara", "Liu Jo", "Elisabetta Franchi",
    "MarcCain", "Gerard Darel", "MFG",
]

COMPETITOR_KEYWORDS = [
    "Moncler", "Brunello Cucinelli", "Kering", "LVMH", "Capri Holdings",
    "Tapestry", "Prada", "Ferragamo", "Burberry", "Hugo Boss", "Zegna",
    "Herno", "Aspesi", "Piquadro", "Tod's",
]

COMMODITY_FX_KEYWORDS = [
    "cashmere price", "prezzo cashmere", "wool price", "prezzo lana",
    "cotton price", "prezzo cotone", "raw material cost", "costo materie prime",
    "EUR/USD", "euro dollaro", "EUR/GBP", "euro sterlina", "cambio valuta",
    "currency hedging", "textile inflation",
]

CATEGORY_KEYWORDS = {
    "fashion": THEME_KEYWORDS + BRAND_KEYWORDS,
    "macro": THEME_KEYWORDS,
    "tax": THEME_KEYWORDS + ["dazi", "dogane", "export", "fisco", "tasse"],
    "esg": ["ESG", "sostenibilità", "sustainability", "supply chain", "compliance"],
    "competitors": COMPETITOR_KEYWORDS + BRAND_KEYWORDS,
    "commodities_fx": COMMODITY_FX_KEYWORDS,
}

# Peso per lo scoring: un match sul brand/competitor vale più di un match
# tematico generico; un match nel titolo vale più di un match nel summary.
BRAND_WEIGHT = 3
COMPETITOR_WEIGHT = 2
THEME_WEIGHT = 1
TITLE_MULTIPLIER = 2
SUMMARY_MULTIPLIER = 1

HTML_TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")


def clean_text(raw: str) -> str:
    """Rimuove tag HTML ed entità dai summary di Google News RSS."""
    if not raw:
        return ""
    text = HTML_TAG_RE.sub(" ", raw)
    text = html.unescape(text)
    return WS_RE.sub(" ", text).strip()


def normalize_title(title: str) -> str:
    """Chiave di dedup 'morbida': minuscolo, senza punteggiatura/spazi doppi."""
    t = title.lower()
    t = re.sub(r"[^\w\s]", "", t)
    return WS_RE.sub(" ", t).strip()


def build_feed_url(domain: str, keywords: list) -> str:
    or_group = " OR ".join(f'"{k}"' if " " in k else k for k in keywords)
    query = f"site:{domain} ({or_group})"
    q = quote(query)
    return f"https://news.google.com/rss/search?q={q}&hl=it&gl=IT&ceid=IT:it"


def fetch_with_retry(url: str, retries: int = 3, backoff: float = 1.5) -> bytes:
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=10)
            resp.raise_for_status()
            return resp.content
        except requests.RequestException as e:
            last_err = e
            wait = backoff ** attempt
            log.warning("  tentativo %d/%d fallito (%s) — retry tra %.1fs",
                        attempt, retries, e, wait)
            time.sleep(wait)
    log.error("  fetch definitivamente fallito: %s", last_err)
    return b""


def score_entry(category: str, title: str, summary: str) -> int:
    """Calcola un punteggio di rilevanza per ordinare gli articoli."""
    title_l, summary_l = title.lower(), summary.lower()
    score = 0
    for kw in BRAND_KEYWORDS:
        kw_l = kw.lower()
        if kw_l in title_l:
            score += BRAND_WEIGHT * TITLE_MULTIPLIER
        elif kw_l in summary_l:
            score += BRAND_WEIGHT * SUMMARY_MULTIPLIER
    for kw in COMPETITOR_KEYWORDS:
        kw_l = kw.lower()
        if kw_l in title_l:
            score += COMPETITOR_WEIGHT * TITLE_MULTIPLIER
        elif kw_l in summary_l:
            score += COMPETITOR_WEIGHT * SUMMARY_MULTIPLIER
    for kw in CATEGORY_KEYWORDS.get(category, []):
        kw_l = kw.lower()
        if kw_l in title_l:
            score += THEME_WEIGHT * TITLE_MULTIPLIER
        elif kw_l in summary_l:
            score += THEME_WEIGHT * SUMMARY_MULTIPLIER
    return score


def fetch_source(category: str, source_name: str, domain: str, days_back: int):
    keywords = CATEGORY_KEYWORDS.get(category, THEME_KEYWORDS + BRAND_KEYWORDS)
    url = build_feed_url(domain, keywords)
    log.info("Scraping %-32s [%s]", source_name, category)

    raw = fetch_with_retry(url)
    feed = feedparser.parse(raw) if raw else feedparser.parse(url)

    cutoff = datetime.now(timezone.utc) - timedelta(days=days_back)
    results = []
    for entry in feed.entries:
        title = getattr(entry, "title", "").strip()
        link = getattr(entry, "link", "").strip()
        if not title or not link:
            continue

        try:
            dt = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
        except Exception:
            dt = None

        if dt is not None and dt < cutoff:
            continue  # troppo vecchio, scartato per ridurre rumore

        date_str = dt.strftime("%Y-%m-%d") if dt else ""
        summary = clean_text(getattr(entry, "summary", ""))[:300]
        score = score_entry(category, title, summary)
        uid = hashlib.md5(link.encode("utf-8")).hexdigest()[:10]

        results.append({
            "id": uid,
            "category": category,
            "categoryLabel": CATEGORY_LABELS[category],
            "source": source_name,
            "title": title,
            "url": link,
            "date": date_str,
            "summary": summary,
            "score": score,
        })
    return results


def parse_args():
    p = argparse.ArgumentParser(description="CFO Weekly News Scraper v2")
    p.add_argument("--days", type=int, default=10,
                   help="scarta articoli più vecchi di N giorni (default 10)")
    p.add_argument("--min-score", type=int, default=0,
                   help="scarta articoli con score < soglia (default 0 = nessun filtro)")
    p.add_argument("--output", type=str, default="data.json",
                   help="percorso file JSON di output (default data.json)")
    p.add_argument("--workers", type=int, default=8,
                   help="numero di thread per il fetch concorrente (default 8)")
    return p.parse_args()


def main():
    args = parse_args()

    jobs = [
        (category, source_name, domain)
        for category, sources in CATEGORY_SOURCES.items()
        for source_name, domain in sources
    ]

    all_articles = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {
            pool.submit(fetch_source, category, source_name, domain, args.days): source_name
            for category, source_name, domain in jobs
        }
        for future in concurrent.futures.as_completed(futures):
            source_name = futures[future]
            try:
                all_articles.extend(future.result())
            except Exception as e:
                log.error("Errore su %s: %s", source_name, e)

    # Dedup: per URL esatto e per titolo normalizzato (stesso articolo
    # ripreso/rilanciato da testate diverse o con tracking diverso)
    seen_urls, seen_titles = set(), set()
    deduped = []
    for art in all_articles:
        norm_title = normalize_title(art["title"])
        if art["url"] in seen_urls or norm_title in seen_titles:
            continue
        seen_urls.add(art["url"])
        seen_titles.add(norm_title)
        if art["score"] >= args.min_score:
            deduped.append(art)

    deduped.sort(key=lambda a: (a["date"], a["score"]), reverse=True)

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(deduped, f, ensure_ascii=False, indent=2)

    by_cat = {}
    for a in deduped:
        by_cat[a["categoryLabel"]] = by_cat.get(a["categoryLabel"], 0) + 1

    log.info("Salvate %d notizie uniche in %s", len(deduped), args.output)
    for label, count in sorted(by_cat.items(), key=lambda x: -x[1]):
        log.info("  %-24s %3d", label, count)


if __name__ == "__main__":
    main()
