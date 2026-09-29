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
from bs4 import BeautifulSoup

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
    "openings": "Openings",
    "fashion_news": "Fashion News",
    "fashion": "Fashion Business",
    "macro": "Macro & Mercati",
    "tax": "Finanza & Fisco",
    "esg": "ESG & Supply Chain",
    "competitors": "Competitor Quotati",
    "commodities_fx": "Materie Prime & Cambi",
    "attualita": "Attualità",
}

ECONOMY_FINANCE_KEYWORDS = [
    "fisco", "tass", "credit", "impost", "bilanc", "inflaz", "bce", "fed", "tassi", "mercat", "bors", "azion",
    "trimestral", "semestral", "pil", "gdp", "export", "dazi", "cfo", "ipo", "buyback", "utili", "dividend", "ricav", "fatturat",
    "finanz", "fiscal", "banc", "bank", "scontrin", "pos", "m&a", "acquisiz", "opa", "quotaz", "titol", "spread",
    "rendiment", "dollar", "euro", "commerci", "valut", "monetar", "investiment", "fondi", "prezz", "costo", "costi",
    "debit", "defic", "earning", "stock", "share", "revenue", "profit", "yield", "macro", "rate", "central bank",
    "economi", "economia", "impres", "aziend", "dichiaraz", "versament", "accertament", "dogan", "sanzion", "societ",
    "mercato", "retail", "privatiz", "risparmi", "obbligaz", "manovra", "legge di bilancio", "f24", "btp", "s&p", "ocse",
    "oat", "bund", "stima", "stime", "cedol", "portafogli", "gestion", "motori", "risultat", "margin", "ebitda",
    "carburanti", "price cap", "petrol", "oil", "greggio", "oro", "gold", "riforma", "norma", "decreto", "decreti",
    "vende", "rilancia", "riorganizza", "investe", "supply chain", "pmi", "produzion"
]

ATTUALITA_KEYWORDS = [
    "onu", "licei", "scuola", "universit", "terra madre", "mostra", "teatro", "concerto", "musica", "spettacolo",
    "sport", "calcio", "partita", "serie a", "champions", "tennis", "olimpiad", "ricetta", "cucina", "chef",
    "ristorante", "vino", "meteo", "cronaca", "gossip", "vip", "matrimonio", "libr", "scrittor", "romanzo",
    "papa", "vaticano", "cinema", "film", "guerra", "zar", "putin", "diplomaz", "elezion", "oops! pagina non trovata"
]


def classify_category(category: str, title: str, summary: str, source: str, tags: list = None, url: str = "") -> str:
    """Classifica la categoria basandosi su tag nativi RSS, percorso URL, testo e fonte."""
    t = (title or "").lower()
    s = (summary or "").lower()
    src = (source or "").lower()
    u = (url or "").lower()
    text = f"{t} {s}"

    if "oops! pagina non trovata" in text:
        return "attualita"

    # Estrazione tag/categoria dal feed RSS
    tag_terms = []
    if tags:
        for tag in tags:
            if isinstance(tag, dict) and "term" in tag:
                tag_terms.append(str(tag["term"]).lower())
            elif isinstance(tag, str):
                tag_terms.append(tag.lower())
    tag_str = " ".join(tag_terms)

    # Identificazione sezioni dal Sole 24 Ore / domini di notizie
    is_sole24 = "sole 24" in src or "ilsole24ore" in u

    finance_tax_sections = ["finanza", "norme e tributi", "norme & tributi", "fisco", "tributi", "economia", "mercati", "borse"]
    attualita_sections = ["italia", "mondo", "cronaca", "politica", "esteri", "cultura", "spettacoli", "sport", "notizie", "attualita"]

    is_fin_section = any(sec in tag_str for s in finance_tax_sections for sec in [s]) or any(f"/art/{s}" in u or f"/{s}/" in u or f"ntplus{s}" in u for s in ["finanza", "norme-e-tributi", "economia", "fisco"])
    is_att_section = any(sec in tag_str for s in attualita_sections for sec in [s]) or any(f"/art/{s}" in u for s in ["italia", "mondo", "cultura", "spettacoli", "sport", "cronaca"])

    has_econ = any(k in text for k in ECONOMY_FINANCE_KEYWORDS)
    has_att = any(k in text for k in ATTUALITA_KEYWORDS)

    if is_sole24:
        if is_fin_section:
            return "tax"
        if is_att_section:
            return "tax" if has_econ else "attualita"
        # Fallback per Il Sole 24 Ore se la sezione non è esplicitata
        return "tax" if has_econ else "attualita"

    if category in ["tax", "macro"]:
        if is_att_section and not has_econ:
            return "attualita"
        if has_att and not has_econ:
            return "attualita"
        if not has_econ and ("rainews" in src or "kommersant" in src):
            return "attualita"

    return category

REGION_LABELS = {
    "mondo": "Mondo",
    "italia": "Italia",
    "europa": "Europa",
    "russia": "Russia",
    "usa": "USA",
}


def detect_region(source: str, title: str, summary: str, url: str) -> tuple[str, str]:
    """Determina la regione geografica dell'articolo (mondo, italia, europa, russia, usa)."""
    s = (source or "").lower()
    t = (title or "").lower()
    m = (summary or "").lower()
    u = (url or "").lower()
    text = f"{s} {t} {m}"

    # 1. Russia
    if ".ru" in u or "russia" in s or any(c in text for c in "абвгдеёжзийклмнопрстуфхцчшщъыьэюя"):
        return "russia", REGION_LABELS["russia"]
    if re.search(r"\b(russia|russian|mosca|rublo|kremlin|putin)\b", text):
        return "russia", REGION_LABELS["russia"]

    # 2. Italia
    if any(k in s for k in [
        "sole 24 ore", "pambianco", "milano finanza", "vogue italia", "elle italia",
        "gq italia", "vanity fair italia", "amica", "grazia italia", "artribune",
        "exibart", "rainews", "soldionline", "distribuzione moderna", "lofficiel italia",
        "manintown", "nss magazine", "mffashion", "sistema moda italia", "fashionnetwork italia"
    ]):
        return "italia", REGION_LABELS["italia"]
    if re.search(r"\b(italia|italian|italiana|italiani|italiano|milano|roma|fisco|agenzia delle entrate|banca d'italia|made in italy|luisa spagnoli)\b", text):
        return "italia", REGION_LABELS["italia"]

    # 3. USA
    if any(k in s for k in [
        "marketwatch", "seeking alpha", "wsj", "wall street journal", "fortune",
        "business insider", "forbes", "barron", "federal reserve", "vogue us",
        "gq magazine", "fashionista", "retail dive", "footwear news", "glossy", "coveteur"
    ]):
        return "usa", REGION_LABELS["usa"]
    if re.search(r"\b(usa|u\.s\.|united states|stati uniti|fed|federal reserve|wall street|new york|washington)\b", text):
        return "usa", REGION_LABELS["usa"]

    # 4. Europa
    if any(k in s for k in [
        "cinco días", "les echos", "manager magazin", "banca centrale europea",
        "ecb", "fashionunited francia", "fashionunited germania", "fashionunited spagna",
        "journal du textile", "textilwirtschaft", "vogue france", "gq uk", "vogue uk",
        "elle uk", "marie claire uk", "dazed", "i-d magazine"
    ]):
        return "europa", REGION_LABELS["europa"]
    if re.search(r"\b(europa|europe|european|ue|unione europea|bce|ecb|germania|germany|francia|france|spagna|spain|regno unito|uk|london|londra|parigi|paris|bruxelles|brussels)\b", text):
        return "europa", REGION_LABELS["europa"]

    # Default: Mondo
    return "mondo", REGION_LABELS["mondo"]

KNOWN_CITIES = [
    "Milano", "Roma", "Parigi", "Londra", "New York", "Mosca", "Perugia",
    "Firenze", "Torino", "Venezia", "Bologna", "Napoli", "Hong Kong", "Tokyo",
    "Shanghai", "Pechino", "Ginevra", "Zurigo", "Francoforte", "Madrid", "Barcellona"
]

def detect_city(title: str, summary: str, source: str) -> str:
    """Rileva se una città nota è menzionata nel titolo, nel summary o nella fonte."""
    text = f"{title or ''} {summary or ''} {source or ''}"
    for city in KNOWN_CITIES:
        pattern = rf"\b{re.escape(city)}\b"
        if re.search(pattern, text, re.IGNORECASE):
            return city
    return "Tutte"

PAMBIANCO_DIRECT_SECTIONS = [
    ("openings", "Openings", "https://www.pambianconews.com/opening/"),
    ("fashion_news", "Fashion News", "https://www.pambianconews.com/news-in-breve/"),
]

DIRECT_RSS_SOURCES = [
    # Finanza, Borsa & Mercati
    ("MarketWatch", "https://feeds.content.dowjones.io/public/rss/mw_topstories", "macro"),
    ("Investing.com", "https://www.investing.com/rss/news.rss", "macro"),
    ("Seeking Alpha", "https://seekingalpha.com/market_currents.xml", "macro"),
    ("The Wall Street Journal (Markets)", "https://feeds.a.dj.com/rss/RSSMarketsMain.xml", "macro"),
    ("Fortune Magazine", "https://fortune.com/feed/", "macro"),
    ("Business Insider", "https://www.businessinsider.com/rss", "macro"),
    ("Forbes (Money/Finance)", "https://www.forbes.com/money/feed/", "tax"),
    ("Barron's", "http://blogs.barrons.com/techtraderdaily/feed/", "macro"),
    ("Yahoo Finance", "https://finance.yahoo.com/news/rssindex", "macro"),
    ("The Economic Times", "https://economictimes.indiatimes.com/rssfeedstopstories.cms", "macro"),
    # Finanza Europea & Italiana
    ("Il Sole 24 Ore Finanza", "https://www.ilsole24ore.com/rss/finanza.xml", "tax"),
    ("Il Sole 24 Ore Norme & Tributi", "https://www.ilsole24ore.com/rss/norme-e-tributi.xml", "tax"),
    ("Il Sole 24 Ore Economia", "https://www.ilsole24ore.com/rss/economia.xml", "tax"),
    ("Il Sole 24 Ore Italia", "https://www.ilsole24ore.com/rss/italia.xml", "attualita"),
    ("Il Sole 24 Ore Mondo", "https://www.ilsole24ore.com/rss/mondo.xml", "attualita"),
    ("Milano Finanza", "https://www.milanofinanza.it/rss/rss_mercati.xml", "tax"),
    ("SoldiOnline", "https://www.soldionline.it/rss/notizie", "tax"),
    ("Cinco Días (Spagna)", "https://cincodias.elpais.com/rss/cincodias/portada.xml", "macro"),
    ("Les Echos (Francia)", "https://www.lesechos.fr/rss/rss_finance.xml", "tax"),
    ("Manager Magazin (Germania)", "https://www.manager-magazin.de/finanzen/index.rss", "macro"),
    # Cripto, Central Banking & Analisi
    ("CoinDesk", "https://www.coindesk.com/arc/outboundfeeds/rss/", "macro"),
    ("CoinTelegraph", "https://cointelegraph.com/rss", "macro"),
    ("Banca Centrale Europea (BCE Press)", "https://www.ecb.europa.eu/rss/press.html", "macro"),
    ("Federal Reserve News", "https://www.federalreserve.gov/feeds/press_all.xml", "macro"),
    ("IMF", "https://www.imf.org/en/News/rss", "macro"),
    ("Financial Post", "https://financialpost.com/feed", "macro"),
    ("Morningstar", "https://www.morningstar.com/rss/news.xml", "macro"),
    ("World Economic Forum", "https://www.weforum.org/agenda/feed/", "macro"),
    ("Economia & Finanza RaiNews", "https://www.rainews.it/rss/economia", "tax"),
    # Fashion, Luxury & Retail
    ("The Business of Fashion (BoF)", "https://www.businessoffashion.com/feed/", "fashion"),
    ("Vogue Business", "https://www.voguebusiness.com/feed", "fashion"),
    ("WWD", "https://wwd.com/feed/", "fashion"),
    ("FashionNetwork", "https://ww.fashionnetwork.com/rss/news", "fashion"),
    ("Fibre2Fashion", "https://feeds.feedburner.com/fibre2fashion/topnews", "fashion"),
    ("Retail Dive", "https://www.retaildive.com/feeds/news/", "fashion"),
    ("Drapers Online", "https://www.drapersonline.com/feed", "fashion"),
    ("FashionUnited", "https://fashionunited.uk/rss-news", "fashion"),
    ("Luxuo", "https://www.luxuo.com/feed", "fashion"),
    ("The Fashion Law", "https://www.thefashionlaw.com/feed/", "fashion"),
    ("Vogue US", "https://www.vogue.com/feed/rss", "fashion"),
    ("GQ Magazine", "https://www.gq.com/feed/rss", "fashion"),
    ("Elle Magazine", "https://www.elle.com/rss/all.xml/", "fashion"),
    ("Harper's Bazaar", "https://www.harpersbazaar.com/rss/all.xml/", "fashion"),
    ("Esquire", "https://www.esquire.com/rss/all.xml/", "fashion"),
    ("Fashionista", "https://fashionista.com/.rss/full/", "fashion"),
    ("Highsnobiety", "https://www.highsnobiety.com/feed/", "fashion"),
    ("Hypebeast", "https://hypebeast.com/feed", "fashion"),
    ("Marie Claire", "https://www.marieclaire.com/rss/all.xml/", "fashion"),
    ("Coveteur", "https://coveteur.com/feed", "fashion"),
    ("Sustainable Fashion Forum", "https://www.thesustainablefashionforum.com/feed", "esg"),
    ("Sourcing Journal", "https://sourcingjournal.com/feed/", "esg"),
    ("Robb Report", "https://robbreport.com/feed/", "fashion"),
    ("Pambianco News RSS", "https://www.pambianconews.com/feed/", "fashion"),
    ("Fashion Magazine", "https://www.fashionmagazine.it/rss", "fashion"),
    # Finanza, Borsa & Mercati Russi
    ("RBK Finance", "https://rssexport.rbc.ru/rbcnews/finance/30/full.rss", "macro"),
    ("Investing.com Russia", "https://ru.investing.com/rss/news.rss", "macro"),
    ("Kommersant Economia", "https://www.kommersant.ru/RSS/section-economics.xml", "macro"),
    ("Kommersant Finanza", "https://www.kommersant.ru/RSS/section-finance.xml", "tax"),
    ("Vedomosti Finanza", "https://www.vedomosti.ru/rss/issue/finance", "tax"),
    ("Vedomosti Mercati", "https://www.vedomosti.ru/rss/rubric/finance/markets", "macro"),
    ("Banca Centrale della Russia (CBR)", "https://www.cbr.ru/rss/RssNews", "macro"),
    ("Borsa di Mosca (MOEX)", "https://www.moex.com/export/news.aspx?cat=1", "macro"),
    ("Finam.ru Mercati", "https://www.finam.ru/analysis/conews/rsspoint/", "macro"),
    ("Finam.ru Notizie", "https://www.finam.ru/international/advanced/rsspoint/", "macro"),
    ("TASS Economia", "https://tass.ru/rss/v2.xml?sections=MjI%3D", "macro"),
    ("RIA Novosti Economia", "https://ria.ru/export/rss2/economy/index.xml", "macro"),
    ("Interfax Economia", "https://www.interfax.ru/rss.asp?sec=1440", "macro"),
    ("Gazeta.ru Business", "https://www.gazeta.ru/export/rss/business_more.xml", "macro"),
    ("Lenta.ru Economia", "https://lenta.ru/rss/news/economics", "macro"),
    ("Izvestia Economia", "https://iz.ru/xml/rss/ekonomika.xml", "macro"),
    ("Prime Business News", "https://1prime.ru/export/rss2/index.xml", "macro"),
    ("Frank Media", "https://frankmedia.ru/feed", "tax"),
    ("Banki.ru News", "https://www.banki.ru/xml/news.rss", "tax"),
    ("Banki.ru Analisi", "https://www.banki.ru/xml/daytheme.rss", "macro"),
    ("BCS Express", "https://bcs-express.ru/rss", "macro"),
    ("BFM.ru Economia", "https://www.bfm.ru/news/data/rss/economy.xml", "macro"),
    ("RBC Investments", "https://rssexport.rbc.ru/rbcnews/quote/30/full.rss", "macro"),
    ("Expert Magazine", "https://expert.ru/rss/economics/", "macro"),
    ("Finversia", "https://www.finversia.ru/rss/news", "macro"),
    # Moda, Lusso & Retail Russi
    ("Fashion-Fashion.ru", "https://fashion-fashion.ru/index.php?option=com_ninja&view=rss&format=feed", "fashion"),
    ("Profashion.ru", "https://profashion.ru/rss/", "fashion"),
    ("FashionUnited Russia", "https://fashionunited.ru/rss-news", "fashion"),
    ("Intermoda.ru", "https://www.intermoda.ru/rss.xml", "fashion"),
    ("Moda.ru", "https://www.moda.ru/rss.xml", "fashion"),
    ("Bein.ru", "https://www.be-in.ru/rss.xml", "fashion"),
    ("RBC Style", "https://rssexport.rbc.ru/rbcnews/style/30/full.rss", "fashion"),
    ("Kommersant Style", "https://www.kommersant.ru/RSS/section-style.xml", "fashion"),
    ("Vedomosti Lifestyle", "https://www.vedomosti.ru/rss/rubric/lifestyle", "fashion"),
    ("The Symbol Russia", "https://www.thesymbol.ru/rss/", "fashion"),
    ("Buro 24/7 Russia", "https://www.buro247.ru/rss/", "fashion"),
    ("Marie Claire Russia", "https://www.marieclaire.ru/rss/", "fashion"),
    ("Grazia Russia", "https://graziamagazine.ru/rss/", "fashion"),
    ("VoICE Russia", "https://www.thevoicefire.ru/rss/", "fashion"),
    ("RBC Retail", "https://rssexport.rbc.ru/rbcnews/retail/30/full.rss", "fashion"),
    ("Shopping Center Russia", "https://www.shoppingcenter.ru/rss.xml", "fashion"),
    ("Wday.ru", "https://www.wday.ru/rss/", "fashion"),
    ("Woman.ru", "https://www.woman.ru/rss/", "fashion"),
    ("Spletnik", "https://www.spletnik.ru/rss", "fashion"),
    ("Peopletalk Russia", "https://peopletalk.ru/feed/", "fashion"),
    ("ModaNews.ru", "https://modanews.ru/rss", "fashion"),
    ("Moda-Online", "https://www.moda-online.ru/rss/", "fashion"),
    ("Lenta.ru Style", "https://lenta.ru/rss/news/style", "fashion"),
    ("Gazeta.ru Style", "https://www.gazeta.ru/export/rss/style_more.xml", "fashion"),
    ("The Mood Magazine", "https://themoodmagazine.com/feed/", "fashion"),
    # Fashion & Magazine Europa
    ("FashionUnited Francia", "https://fashionunited.fr/rss-actualites-mode", "fashion"),
    ("FashionUnited Germania", "https://fashionunited.de/rss-modenachrichten", "fashion"),
    ("FashionUnited Spagna", "https://fashionunited.es/rss-noticias-moda", "fashion"),
    ("Journal du Textile", "https://www.journaldutextile.com/rss", "fashion"),
    ("TextilWirtschaft", "https://www.textilwirtschaft.de/rss/news.xml", "fashion"),
    ("Vogue France", "https://www.vogue.fr/feed/rss", "fashion"),
    ("GQ UK", "https://www.gq-magazine.co.uk/feed/rss", "fashion"),
    ("Vogue UK", "https://www.vogue.co.uk/feed/rss", "fashion"),
    ("Elle UK", "https://www.elle.com/uk/rss/all.xml/", "fashion"),
    ("Harper's Bazaar UK", "https://www.harpersbazaar.com/uk/rss/all.xml/", "fashion"),
    ("Marie Claire UK", "https://www.marieclaire.co.uk/feed", "fashion"),
    ("Dazed Digital", "https://www.dazeddigital.com/rss", "fashion"),
    ("i-D Magazine", "https://i-d.co/feed/", "fashion"),
    ("Wallpaper", "https://www.wallpaper.com/feeds/home", "fashion"),
    ("Self Service Magazine", "https://selfservicemagazine.com/feed/", "fashion"),
    ("Ecotextile News", "https://www.ecotextile.com/rss/", "esg"),
    ("Fashion Revolution Blog", "https://www.fashionrevolution.org/feed/", "esg"),
    ("Textile Today", "https://www.textiletoday.com.bd/feed/", "esg"),
    ("Just-Style RSS", "https://www.just-style.com/feed/", "esg"),
    ("Luxe Digital", "https://luxe.digital/feed/", "fashion"),
    # Fashion & Retail USA
    ("Glossy", "https://www.glossy.co/feed/", "fashion"),
    ("Footwear News", "https://footwearnews.com/feed/", "fashion"),
    ("Apparel News", "https://www.apparelnews.net/rss/", "fashion"),
    ("Forbes Retail", "https://www.forbes.com/retail/feed/", "fashion"),
    ("Business Insider Retail", "https://www.businessinsider.com/retail/rss", "fashion"),
    ("Complex Style", "https://www.complex.com/style/rss", "fashion"),
    ("Remake World", "https://remake.world/feed/", "esg"),
    ("Ecocult", "https://ecocult.com/feed/", "esg"),
    ("Good On You", "https://goodonyou.eco/feed/", "esg"),
    # Fashion & Retail Italia
    ("Pambianco Beauty", "https://beauty.pambianconews.com/feed/", "fashion"),
    ("Il Sole 24 Ore Moda", "https://www.ilsole24ore.com/rss/moda.xml", "fashion"),
    ("Milano Finanza Fashion", "https://www.milanofinanza.it/rss/rss_fashion.xml", "fashion"),
    ("FashionNetwork Italia RSS", "https://it.fashionnetwork.com/rss/news", "fashion"),
    ("FashionUnited Italia", "https://fashionunited.it/rss-notizie-moda", "fashion"),
    ("Sistema Moda Italia", "https://www.sistemamodaitalia.it/rss", "fashion"),
    ("Distribuzione Moderna", "https://www.distribuzionemoderna.it/rss", "fashion"),
    ("Vogue Italia", "https://www.vogue.it/feed/rss", "fashion"),
    ("GQ Italia", "https://www.gqitalia.it/feed/rss", "fashion"),
    ("Elle Italia", "https://www.elle.com/it/rss/all.xml/", "fashion"),
    ("Harper's Bazaar Italia", "https://www.harpersbazaar.com/it/rss/all.xml/", "fashion"),
    ("Marie Claire Italia", "https://www.marieclaire.it/rss/all.xml/", "fashion"),
    ("Vanity Fair Italia", "https://www.vanityfair.it/feed/rss", "fashion"),
    ("Io Donna Moda", "https://www.iodonna.it/moda/feed/", "fashion"),
    ("AMICA", "https://www.amica.it/feed/", "fashion"),
    ("Grazia Italia", "https://www.grazia.it/feed", "fashion"),
    ("MFFashion RSS", "https://www.mffashion.com/rss", "fashion"),
    ("L'Officiel Italia", "https://www.lofficielitalia.com/rss", "fashion"),
    ("MANINTOWN", "https://www.manintown.com/feed/", "fashion"),
    ("nss magazine", "https://www.nssmag.com/it/rss", "fashion"),
    ("Artribune Moda", "https://www.artribune.com/category/viaggi/moda/feed/", "fashion"),
    ("Exibart Moda", "https://www.exibart.com/moda/feed/", "fashion"),
]

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


def scrape_pambianco_section(category: str, source_name: str, url: str, days_back: int):
    log.info("Direct scraping Pambianco %-20s [%s]", source_name, category)
    raw = fetch_with_retry(url)
    if not raw:
        return []
    soup = BeautifulSoup(raw, "html.parser")
    cutoff = datetime.now(timezone.utc) - timedelta(days=days_back)
    results = []
    seen = set()

    for post in soup.find_all("article", class_=lambda c: c and "jeg_post" in c):
        title_el = post.find(class_="jeg_post_title")
        if not title_el:
            continue
        a_tag = title_el.find("a")
        if not a_tag:
            continue
        link = a_tag.get("href", "").strip()
        title = clean_text(a_tag.text)
        if not link or link in seen or not title:
            continue
        seen.add(link)

        date_str = ""
        dt = None
        date_el = post.find(class_="jeg_meta_date")
        if date_el:
            raw_d = date_el.text.strip()
            m = re.search(r"(\d{2})/(\d{2})/(\d{4})", raw_d)
            if m:
                date_str = f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
                try:
                    dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                except ValueError:
                    dt = None
        if not date_str:
            m = re.search(r"/(\d{4})/(\d{2})/(\d{2})/", link)
            if m:
                date_str = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
                try:
                    dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                except ValueError:
                    dt = None

        if dt is not None and dt < cutoff:
            continue

        excerpt_el = post.find(class_="jeg_post_excerpt")
        summary = clean_text(excerpt_el.text) if excerpt_el else ""
        if not summary:
            summary = title

        score = score_entry(category, title, summary)
        uid = hashlib.md5(link.encode("utf-8")).hexdigest()[:10]
        full_source = f"Pambianco News — {source_name}"
        region, region_label = detect_region(full_source, title, summary, link)
        city = detect_city(title, summary, full_source)
        final_cat = classify_category(category, title, summary, full_source, url=link)

        results.append({
            "id": uid,
            "category": final_cat,
            "categoryLabel": CATEGORY_LABELS.get(final_cat, final_cat),
            "region": region,
            "regionLabel": region_label,
            "city": city,
            "source": full_source,
            "title": title,
            "url": link,
            "date": date_str,
            "summary": summary,
            "score": score,
        })
    return results


def fetch_direct_rss_feed(source_name: str, url: str, category: str, days_back: int):
    log.info("Direct RSS fetch %-32s [%s]", source_name, category)
    raw = fetch_with_retry(url)
    feed = feedparser.parse(raw) if raw else feedparser.parse(url)

    cutoff = datetime.now(timezone.utc) - timedelta(days=days_back)
    results = []
    for entry in feed.entries:
        title = getattr(entry, "title", "").strip()
        link = getattr(entry, "link", "").strip()
        if not title or not link:
            continue

        dt = None
        if hasattr(entry, "published_parsed") and entry.published_parsed:
            try:
                dt = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
            except Exception:
                dt = None
        elif hasattr(entry, "updated_parsed") and entry.updated_parsed:
            try:
                dt = datetime(*entry.updated_parsed[:6], tzinfo=timezone.utc)
            except Exception:
                dt = None

        if dt is not None and dt < cutoff:
            continue

        date_str = dt.strftime("%Y-%m-%d") if dt else ""
        summary = clean_text(getattr(entry, "summary", getattr(entry, "description", "")))[:300]
        if not summary:
            summary = title

        score = score_entry(category, title, summary)
        uid = hashlib.md5(link.encode("utf-8")).hexdigest()[:10]
        region, region_label = detect_region(source_name, title, summary, link)
        city = detect_city(title, summary, source_name)
        tags = getattr(entry, "tags", [])
        final_cat = classify_category(category, title, summary, source_name, tags=tags, url=link)

        results.append({
            "id": uid,
            "category": final_cat,
            "categoryLabel": CATEGORY_LABELS.get(final_cat, final_cat),
            "region": region,
            "regionLabel": region_label,
            "city": city,
            "source": source_name,
            "title": title,
            "url": link,
            "date": date_str,
            "summary": summary,
            "score": score,
        })
    return results


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
        region, region_label = detect_region(source_name, title, summary, link)
        city = detect_city(title, summary, source_name)
        tags = getattr(entry, "tags", [])
        final_cat = classify_category(category, title, summary, source_name, tags=tags, url=link)

        results.append({
            "id": uid,
            "category": final_cat,
            "categoryLabel": CATEGORY_LABELS.get(final_cat, final_cat),
            "region": region,
            "regionLabel": region_label,
            "city": city,
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
        for cat_code, s_name, s_url in PAMBIANCO_DIRECT_SECTIONS:
            futures[pool.submit(scrape_pambianco_section, cat_code, s_name, s_url, args.days)] = f"Pambianco {s_name}"

        for s_name, s_url, cat_code in DIRECT_RSS_SOURCES:
            futures[pool.submit(fetch_direct_rss_feed, s_name, s_url, cat_code, args.days)] = s_name

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
