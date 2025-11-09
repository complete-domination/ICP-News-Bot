import os
import re
import html as ihtml
import asyncio
import logging
from typing import List, Dict, Optional, Tuple, Set

import aiohttp
import feedparser
import discord
from discord.ext import tasks
from dotenv import load_dotenv

# ---------------- Env / Config ----------------
load_dotenv()

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
CHANNEL_ID = int(os.getenv("CHANNEL_ID", "0"))
POLL_MINUTES = float(os.getenv("POLL_MINUTES", "2"))

# Force a post when the bot boots (helps confirm it works). Turn off later.
POST_ON_STARTUP = os.getenv("POST_ON_STARTUP", "false").lower() == "true"
POST_STARTUP_MAX = int(os.getenv("POST_STARTUP_MAX", "1"))

# Synopsis size and HTTP timeouts
SYNOPSIS_MAX_CHARS = int(os.getenv("SYNOPSIS_MAX_CHARS", "900"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "25"))

# Keywords for ICP news. You can override with KEYWORDS env (comma-separated).
DEFAULT_KEYWORDS = [
    "ICP", "Internet Computer", "Internet Computer Protocol",
    "Dfinity", "DFINITY", "Dominic Williams", "ic0.app", "ckBTC", "ckETH"
]
KEYWORDS = [k.strip() for k in os.getenv("KEYWORDS", ",".join(DEFAULT_KEYWORDS)).split(",") if k.strip()]

# Optional excludes to avoid false positives (comma-separated)
EXCLUDE_KEYWORDS = [k.strip() for k in os.getenv("EXCLUDE_KEYWORDS", "").split(",") if k.strip()]

# Feeds list (comma-separated). You can override with FEEDS env to add/remove.
DEFAULT_FEEDS = [
    # Major crypto outlets
    "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "https://cointelegraph.com/rss",
    "https://decrypt.co/feed",
    "https://cryptoslate.com/feed/",
    "https://beincrypto.com/feed/",
    "https://u.today/rss",
    "https://ambcrypto.com/feed/",
    "https://news.bitcoin.com/feed/",
    # ICP / DFINITY official & community
    "https://blog.dfinity.org/feed.xml",
    "https://medium.com/feed/dfinity",
    # (Optional community): uncomment if you want Reddit in the mix
    # "https://www.reddit.com/r/dfinity/.rss",
]

FEEDS = [f.strip() for f in os.getenv("FEEDS", ",".join(DEFAULT_FEEDS)).split(",") if f.strip()]

if not DISCORD_TOKEN or CHANNEL_ID == 0:
    raise SystemExit("Set DISCORD_TOKEN and CHANNEL_ID env vars.")

# ---------------- Logging ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("icp-news-bot")

# ---------------- Discord ----------------
INTENTS = discord.Intents.default()
client = discord.Client(intents=INTENTS)

# Track what we've posted this runtime (in-memory). Optionally persist later if desired.
SEEN_IDS: Set[str] = set()
HAS_POSTED_ON_STARTUP = False


# ---------------- Helpers ----------------
def _strip_tags(html: str) -> str:
    html = re.sub(r"(?is)<(script|style).*?>.*?</\1>", "", html)
    html = re.sub(r"(?i)<br\s*/?>", "\n", html)
    html = re.sub(r"(?i)</p>", "\n", html)
    text = re.sub(r"(?s)<.*?>", "", html)
    text = ihtml.unescape(text)
    text = re.sub(r"[ \t\r\f\v]+", " ", text)
    text = re.sub(r"\n\s*\n\s*", "\n\n", text).strip()
    return text


def _first_sentences(text: str, max_chars: int, max_sents: int = 5) -> str:
    parts = re.split(r"(?<=[.!?])\s+", text)
    out, total = [], 0
    for p in parts:
        p = p.strip()
        if not p:
            continue
        next_len = len(p) + (1 if out else 0)
        if total + next_len > max_chars:
            break
        out.append(p)
        total += next_len
        if len(out) >= max_sents:
            break
    joined = " ".join(out).strip()
    return joined or text[:max_chars].rstrip()


def _entry_id(entry: dict) -> str:
    return (
        entry.get("id")
        or entry.get("guid")
        or entry.get("link")
        or f"{entry.get('title','')}-{entry.get('published','')}"
        or os.urandom(8).hex()
    )


def _match_icp(text: str) -> bool:
    t = text.lower()
    if EXCLUDE_KEYWORDS and any(ex.lower() in t for ex in EXCLUDE_KEYWORDS):
        return False
    return any(k.lower() in t for k in KEYWORDS)


async def fetch_text(session: aiohttp.ClientSession, url: str) -> Optional[str]:
    if not url:
        return None
    try:
        async with session.get(url, timeout=REQUEST_TIMEOUT) as resp:
            if resp.status != 200:
                return None
            return await resp.text()
    except Exception:
        return None


async def get_long_synopsis(session: aiohttp.ClientSession, url: str, fallback: str, rss_summary: Optional[str]) -> str:
    """
    Build a longer synopsis by combining:
      1) RSS summary/description if provided
      2) <meta> description (og/twitter/description)
      3) first paragraphs from <article> or <p> tags
    """
    pieces: List[str] = []
    if rss_summary:
        pieces.append(_strip_tags(rss_summary).strip())

    html = await fetch_text(session, url)
    if html:
        def grab(content: str, needle: str) -> Optional[str]:
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

        og = grab(html, 'property="og:description"')
        tw = grab(html, 'name="twitter:description"')
        md = grab(html, 'name="description"')
        meta = og or tw or md
        if meta:
            pieces.append(meta.strip())

        # extract main text
        article_match = re.search(r"(?is)<article[^>]*>(.*?)</article>", html)
        body_html = article_match.group(1) if article_match else html
        paragraphs = re.findall(r"(?is)<p[^>]*>(.*?)</p>", body_html)
        body_text = _strip_tags("\n\n".join(paragraphs[:8]) if paragraphs else body_html)
        if body_text:
            pieces.append(_first_sentences(body_text, SYNOPSIS_MAX_CHARS * 2, max_sents=6))

    synopsis = " ".join([p for p in pieces if p]).strip() or fallback
    if len(synopsis) > SYNOPSIS_MAX_CHARS:
        synopsis = synopsis[:SYNOPSIS_MAX_CHARS].rstrip() + "…"
    return synopsis


async def parse_feed(session: aiohttp.ClientSession, feed_url: str) -> List[Dict]:
    out: List[Dict] = []
    try:
        async with session.get(feed_url, timeout=REQUEST_TIMEOUT) as resp:
            if resp.status != 200:
                log.warning("Feed HTTP %s: %s", resp.status, feed_url)
                return out
            content = await resp.read()
    except Exception as e:
        log.warning("Feed error %s: %s", feed_url, e)
        return out

    parsed = feedparser.parse(content)
    for entry in parsed.entries:
        title = entry.get("title", "")
        summary = entry.get("summary", "") or entry.get("description", "")
        link = entry.get("link", "")
        combined = " ".join([title, summary or "", link or ""])
        if not _match_icp(combined):
            continue

        item = {
            "id": _entry_id(entry),
            "title": title or "New article",
            "summary": summary,
            "link": link or "",
            "published": entry.get("published", "") or entry.get("updated", ""),
            "source": parsed.feed.get("title", "") if parsed.feed else "",
        }
        out.append(item)
    return out


async def poll_all_feeds(session: aiohttp.ClientSession) -> List[Dict]:
    tasks = [parse_feed(session, url) for url in FEEDS]
    results: List[List[Dict]] = await asyncio.gather(*tasks, return_exceptions=True)
    items: List[Dict] = []
    for res in results:
        if isinstance(res, list):
            items.extend(res)
    # Sort newest first if have published; otherwise keep as-is
    def _key(x: Dict):
        return x.get("published") or ""
    items.sort(key=_key, reverse=True)
    return items


async def build_message(item: Dict, session: aiohttp.ClientSession) -> Tuple[str, Optional[discord.Embed]]:
    title = item.get("title") or "New article"
    url = item.get("link") or ""
    source = item.get("source") or "Source"
    published = item.get("published") or ""
    rss_summary = item.get("summary")

    synopsis = await get_long_synopsis(session, url, fallback=title, rss_summary=rss_summary)

    text = f"**{title}**\n{url}\n\n**Synopsis:** {synopsis}"
    embed = discord.Embed(
        title=f"{source} • {published}",
        description=f"Keywords: {', '.join(KEYWORDS)}",
        url=url,
    )
    return text, embed


# ---------------- Poller ----------------
@tasks.loop(minutes=POLL_MINUTES, reconnect=True)
async def poll_and_post():
    global HAS_POSTED_ON_STARTUP

    await client.wait_until_ready()

    # Get or fetch channel (cache-safe)
    channel = client.get_channel(CHANNEL_ID)
    if channel is None:
        try:
            channel = await client.fetch_channel(CHANNEL_ID)
        except Exception as e:
            log.error("Cannot fetch channel %s: %s", CHANNEL_ID, e)
            return

    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT + 5)
    connector = aiohttp.TCPConnector(limit=15, ttl_dns_cache=300)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        items = await poll_all_feeds(session)
        if not items:
            log.info("No ICP matches this poll.")
            return

        posted = 0
        for item in items:
            iid = item.get("id", "")
            if iid in SEEN_IDS and not (POST_ON_STARTUP and not HAS_POSTED_ON_STARTUP):
                continue

            text, embed = await build_message(item, session)
            try:
                await channel.send(text, embed=embed)
                SEEN_IDS.add(iid)
                posted += 1
                log.info("Posted: %s | %s", item.get("source"), item.get("title", "")[:80])
            except Exception as e:
                log.exception("Failed sending message: %s", e)

            if POST_ON_STARTUP and not HAS_POSTED_ON_STARTUP:
                HAS_POSTED_ON_STARTUP = True
                if posted >= POST_STARTUP_MAX:
                    break

        if posted == 0:
            # If nothing new, at least log what top item was
            top = items[0]
            log.info("Top match (already posted): %s | %s", top.get("source"), top.get("title", "")[:80])


@poll_and_post.before_loop
async def before_poll():
    await client.wait_until_ready()


@client.event
async def on_ready():
    log.info("Logged in as %s (%s)", client.user, client.user.id)
    if not poll_and_post.is_running():
        poll_and_post.start()


@client.event
async def on_message(message: discord.Message):
    if message.author.bot:
        return
    if message.channel.id != CHANNEL_ID:
        return  # keep bot confined to one channel
    cmd = message.content.strip().lower()
    if cmd == "!newsnow":
        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT + 5)
        connector = aiohttp.TCPConnector(limit=15, ttl_dns_cache=300)
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            items = await poll_all_feeds(session)
            if not items:
                await message.channel.send("No ICP news found right now.")
                return
            # find first not-seen or fallback to newest
            chosen = None
            for it in items:
                if it.get("id") not in SEEN_IDS:
                    chosen = it
                    break
            if not chosen:
                chosen = items[0]
            text, embed = await build_message(chosen, session)
            await message.channel.send(text, embed=embed)
            SEEN_IDS.add(chosen.get("id",""))


def main():
    try:
        client.run(DISCORD_TOKEN, log_handler=None)
    except KeyboardInterrupt:
        log.info("Shutting down.")


if __name__ == "__main__":
    main()
