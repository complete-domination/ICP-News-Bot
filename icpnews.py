import os
import logging
import asyncio
import time
from typing import List, Optional, Tuple, Set
from urllib.parse import urlparse

import aiohttp
import discord
from discord.ext import tasks
from dotenv import load_dotenv

import feedparser
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime

# ---------------- Env / Config ----------------
load_dotenv()  # local dev; Railway uses Variables

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
CHANNEL_ID = int(os.getenv("CHANNEL_ID", "0"))

# Polling + limits
POLL_MINUTES = float(os.getenv("POLL_MINUTES", "3"))
MAX_POSTS_PER_POLL = int(os.getenv("MAX_POSTS_PER_POLL", "3"))

# Time skew to avoid missing just-before-start items
SKEW_MINUTES = int(os.getenv("SKEW_MINUTES", "10"))

# Keywords to match for ICP (word boundaries)
KEYWORDS = os.getenv(
    "KEYWORDS",
    r"\bICP\b|\bInternet Computer\b|\bDFINITY\b|\bDfinity\b|\bDominic Williams\b|\bckBTC\b|\bckETH\b",
)

# Extra feeds, comma-separated (optional)
FEEDS_EXTRA = [u.strip() for u in os.getenv("FEEDS_EXTRA", "").split(",") if u.strip()]

# Core crypto feeds (curated; you can add more via FEEDS_EXTRA)
DEFAULT_FEEDS = [
    # Big outlets
    "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "https://cointelegraph.com/rss",
    "https://www.theblock.co/rss.xml",
    "https://decrypt.co/feed",
    "https://cryptoslate.com/feed/",
    "https://beincrypto.com/feed/",
    "https://ambcrypto.com/feed/",
    "https://www.coinspeaker.com/feed/",
    "https://u.today/rss",
    "https://cryptobriefing.com/feed/",
    "https://bitcoinmagazine.com/.rss/full/",
    "https://www.fxstreet.com/crypto/news/rss",
    "https://coinjournal.net/news/feed/",
    "https://www.newsbtc.com/feed/",
]

FEEDS = DEFAULT_FEEDS + FEEDS_EXTRA

if not DISCORD_TOKEN or CHANNEL_ID == 0:
    raise SystemExit("Set DISCORD_TOKEN and CHANNEL_ID env vars.")

# ---------------- Globals ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("icp-news-bot")

INTENTS = discord.Intents.default()
client = discord.Client(intents=INTENTS)

START_TIME = datetime.now(timezone.utc) - timedelta(minutes=SKEW_MINUTES)
SEEN_LINKS: Set[str] = set()  # cross-site dedupe
SEEN_TITLES: Set[str] = set()  # belt & suspenders

# ---------------- Utils ----------------
def to_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def entry_datetime(entry) -> Optional[datetime]:
    """
    Try multiple places an RSS entry might store its time.
    Skip if no reliable date is found.
    """
    # feedparser may give 'published_parsed' or 'updated_parsed' as struct_time
    for attr in ("published_parsed", "updated_parsed"):
        st = entry.get(attr)
        if st:
            try:
                return to_utc(datetime(*st[:6], tzinfo=timezone.utc))
            except Exception:
                pass

    # Try RFC 2822 style strings (published / updated)
    for attr in ("published", "updated", "date"):
        s = entry.get(attr)
        if s:
            try:
                return to_utc(parsedate_to_datetime(s))
            except Exception:
                pass

    return None

def matches_icp(text: str) -> bool:
    import re
    return bool(re.search(KEYWORDS, text or "", flags=re.IGNORECASE))

def domain_of(url: str) -> str:
    try:
        return urlparse(url).hostname or "source"
    except Exception:
        return "source"

async def try_get_synopsis(session: aiohttp.ClientSession, url: str, fallback: str) -> str:
    """
    Quick synopsis:
      1) Use summary from feed if available (handled upstream).
      2) Otherwise try OG/Twitter/meta description from the page.
    Skip if the site blocks/returns non-200 quickly.
    """
    try:
        async with session.get(url, timeout=20) as resp:
            if resp.status != 200:
                return fallback
            html = await resp.text()
    except Exception:
        return fallback

    def _grab(content: str, needle: str):
        c_low = content.lower()
        i = c_low.find(needle)
        if i == -1:
            return None
        j = c_low.find('content="', i)
        if j == -1:
            return None
        j += len('content="')
        k = content.find('"', j)
        if k == -1:
            return None
        return content[j:k].strip()

    og = _grab(html, 'property="og:description"')
    tw = _grab(html, 'name="twitter:description"')
    md = _grab(html, 'name="description"')

    summary = og or tw or md or fallback
    return (summary[:297].rstrip() + "...") if len(summary) > 300 else summary

# ---------------- Feed fetch ----------------
async def fetch_feed(session: aiohttp.ClientSession, url: str) -> List[dict]:
    """
    Download + parse a feed URL, return list of entries (as dicts we normalize).
    """
    try:
        async with session.get(url, timeout=30) as resp:
            if resp.status != 200:
                log.warning("Feed %s HTTP %s", url, resp.status)
                return []
            content = await resp.read()
    except Exception as e:
        log.warning("Feed fetch error %s: %s", url, e)
        return []

    parsed = feedparser.parse(content)
    out = []
    for e in parsed.entries:
        dt = entry_datetime(e)
        if not dt:
            continue
        link = e.get("link") or ""
        title = e.get("title") or ""
        summary = e.get("summary") or e.get("description") or ""

        out.append(
            {
                "title": title.strip(),
                "link": link.strip(),
                "summary": summary.strip(),
                "published": dt,  # aware UTC
                "feed": url,
            }
        )
    return out

async def gather_latest_icp() -> List[dict]:
    """
    Pull feeds concurrently, filter to ICP matches, ignore old, and sort by time.
    """
    timeout = aiohttp.ClientTimeout(total=40)
    connector = aiohttp.TCPConnector(limit=12, ttl_dns_cache=300)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        results = await asyncio.gather(*(fetch_feed(session, f) for f in FEEDS))
        entries = [item for sub in results for item in sub]

        # Filter: time >= START_TIME and keyword match
        fresh = []
        for e in entries:
            if e["published"] < START_TIME:
                continue
            text_blob = f"{e['title']} {e['summary']}"
            if matches_icp(text_blob):
                fresh.append(e)

        # Sort newest first
        fresh.sort(key=lambda x: x["published"], reverse=True)

        # Enrich synopsis if summary missing/weak
        enriched = []
        for e in fresh[: MAX_POSTS_PER_POLL * 2]:  # cap enrichment effort
            s = e["summary"]
            if not s or len(s) < 30:
                s = await try_get_synopsis(session, e["link"], fallback=e["title"])
            if len(s) > 400:
                s = s[:397].rstrip() + "..."
            e["summary"] = s
            enriched.append(e)

        return enriched

# ---------------- Discord formatting ----------------
def build_discord_message(item: dict) -> Tuple[str, Optional[discord.Embed]]:
    t = item["title"]
    u = item["link"]
    ts = item["published"].strftime("%Y-%m-%d %H:%M:%S UTC")
    d = domain_of(u)

    text = f"**{t}**\n{u}\n\n**Synopsis:** {item['summary']}"
    embed = discord.Embed(
        title=f"{d} • {ts}",
        description="Most-recent ICP mentions across major crypto outlets.",
        url=u,
    )
    return text, embed

# ---------------- Bot loop ----------------
@tasks.loop(minutes=POLL_MINUTES, reconnect=True)
async def poll_and_post():
    await client.wait_until_ready()
    channel = client.get_channel(CHANNEL_ID)
    if channel is None:
        log.error("Channel %s not found or bot lacks access.", CHANNEL_ID)
        return

    items = await gather_latest_icp()
    if not items:
        return

    posted = 0
    for e in items:
        # Cross-site dedupe using link + title
        key = (e["link"] or "").strip()
        title_key = e["title"].strip().lower()

        if not key or key in SEEN_LINKS or title_key in SEEN_TITLES:
            continue

        text, embed = build_discord_message(e)
        try:
            await channel.send(text, embed=embed)
            SEEN_LINKS.add(key)
            SEEN_TITLES.add(title_key)
            posted += 1
            if posted >= MAX_POSTS_PER_POLL:
                break
        except Exception as ex:
            log.exception("Send failed: %s", ex)

@poll_and_post.before_loop
async def before_poll():
    await client.wait_until_ready()

@client.event
async def on_ready():
    log.info("Logged in as %s (%s)", client.user, client.user.id)
    if not poll_and_post.is_running():
        poll_and_post.start()

# Optional manual trigger in the target channel only
@client.event
async def on_message(message: discord.Message):
    if message.author.bot:
        return
    if message.channel.id != CHANNEL_ID:
        return
    if message.content.strip().lower() == "!icpnow":
        items = await gather_latest_icp()
        if not items:
            await message.channel.send("No fresh ICP headlines right now.")
            return
        sent = 0
        for e in items:
            key = (e["link"] or "").strip()
            title_key = e["title"].strip().lower()
            if key in SEEN_LINKS or title_key in SEEN_TITLES:
                continue
            text, embed = build_discord_message(e)
            await message.channel.send(text, embed=embed)
            SEEN_LINKS.add(key)
            SEEN_TITLES.add(title_key)
            sent += 1
            if sent >= MAX_POSTS_PER_POLL:
                break

def main():
    try:
        client.run(DISCORD_TOKEN, log_handler=None)
    except KeyboardInterrupt:
        log.info("Shutting down.")

if __name__ == "__main__":
    main()
