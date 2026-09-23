#!/usr/bin/env python3
"""
CFO Weekly News Scraper — Luisa Spagnoli S.p.A.

Trova notizie reali (non generate) sugli ultimi giorni, suddivise in tre
categorie rilevanti per la Direzione Finanziaria, usando i feed RSS di
Google News (nessuna API key richiesta, nessun servizio terzo instabile
come rss2json). Salva tutto in data.json con link diretto alla fonte.

Uso:
    pip install feedparser
    python3 scraper_cfo_report.py

Poi carica data.json nella dashboard con il bottone "Carica dati scraper".

Perché non fetch diretto dal browser: una pagina pubblicata non può
chiamare rss2json.com, Il Sole 24 Ore o ANSA direttamente — il sandbox
del browser blocca le richieste verso domini esterni non autorizzati.
Questo script gira invece sul tuo computer/server e produce un file dati
che la dashboard può caricare in sicurezza.
"""

import feedparser
import json
import hashlib
from datetime import datetime, timezone
from urllib.parse import quote

CATEGORY_QUERIES = {
    "tax": [
        '"Transizione 5.0" credito d\'imposta',
        "fisco moda tessile abbigliamento",
    ],
    "fashion": [
        '"Luisa Spagnoli"',
        "retail moda abbigliamento femminile export Italia",
    ],
    "macro": [
        "cambio euro dollaro mercati valutari",
        "materie prime lana cashmere prezzi",
    ],
}

CATEGORY_LABELS = {
    "tax": "Finanza & Fisco",
    "fashion": "Fashion Business",
    "macro": "Macro & Mercati",
}


def build_feed_url(query: str) -> str:
    q = quote(query)
    return f"https://news.google.com/rss/search?q={q}&hl=it&gl=IT&ceid=IT:it"


def fetch_category(category: str, queries):
    results = []
    for query in queries:
        feed = feedparser.parse(build_feed_url(query))
        for entry in feed.entries:
            title = getattr(entry, "title", "").strip()
            link = getattr(entry, "link", "").strip()
            if not title or not link:
                continue
            source = ""
            if hasattr(entry, "source") and hasattr(entry.source, "title"):
                source = entry.source.title
            try:
                dt = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
                date_str = dt.strftime("%Y-%m-%d")
            except Exception:
                date_str = ""
            summary = getattr(entry, "summary", "")[:260]
            uid = hashlib.md5(link.encode("utf-8")).hexdigest()[:10]
            results.append({
                "id": uid,
                "category": category,
                "categoryLabel": CATEGORY_LABELS[category],
                "source": source or "Google News",
                "title": title,
                "url": link,
                "date": date_str,
                "summary": summary,
            })
    return results


def main():
    all_articles, seen = [], set()
    for cat, queries in CATEGORY_QUERIES.items():
        print(f"Scraping categoria {cat}...")
        for art in fetch_category(cat, queries):
            if art["url"] in seen:
                continue
            seen.add(art["url"])
            all_articles.append(art)

    all_articles.sort(key=lambda a: a["date"], reverse=True)

    with open("data.json", "w", encoding="utf-8") as f:
        json.dump(all_articles, f, ensure_ascii=False, indent=2)

    print(f"\nSalvate {len(all_articles)} notizie uniche in data.json")


if __name__ == "__main__":
    main()
