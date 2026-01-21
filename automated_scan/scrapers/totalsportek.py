import re
from functools import partial
from urllib.parse import urljoin, urlparse

from selectolax.parser import HTMLParser

from .utils import Cache, Time, get_logger, leagues, network

log = get_logger(__name__)

urls: dict[str, dict[str, str | float]] = {}

TAG = "TOTALSPRTK"

CACHE_FILE = Cache(f"{TAG.lower()}.json", exp=28_800)

MIRRORS = [
    {
        "base": "https://live.totalsportek777.com/",
        "hex_decode": True,
    },
    {
        "base": "https://live2.totalsportek777.com/",
        "hex_decode": False,
    },
]


def fix_txt(s: str) -> str:
    s = " ".join(s.split())

    return s.upper() if s.islower() else s


async def process_event(href: str, url_num: int) -> tuple[str | None, str | None]:
    valid_m3u8 = re.compile(r'var\s+(\w+)\s*=\s*"([^"]*)"', re.IGNORECASE)

    for x, mirror in enumerate(MIRRORS, start=1):
        base: str = mirror["base"]

        hex_decode: bool = mirror["hex_decode"]

        url = urljoin(base, href)

        if not (html_data := await network.request(url, log=log)):
            log.info(f"M{x} | URL {url_num}) Failed to load url.")

            return None, None

        soup = HTMLParser(html_data.content)

        iframe = soup.css_first("iframe")

        if not iframe or not (iframe_src := iframe.attributes.get("src")):
            log.warning(f"M{x} | URL {url_num}) No iframe element found.")
            continue

        if not (iframe_src_data := await network.request(iframe_src, log=log)):
            log.warning(f"M{x} | URL {url_num}) Failed to load iframe source.")
            continue

        if not (match := valid_m3u8.search(iframe_src_data.text)):
            log.warning(f"M{x} | URL {url_num}) No Clappr source found.")
            continue

        raw: str = match[2]

        try:
            m3u8_url = bytes.fromhex(raw).decode("utf-8") if hex_decode else raw
        except Exception as e:
            log.warning(f"M{x} | URL {url_num}) Decoding failed: {e}")
            continue

        if m3u8_url and iframe_src:
            log.info(f"M{x} | URL {url_num}) Captured M3U8")

            return m3u8_url, iframe_src

        log.warning(f"M{x} | URL {url_num}) No M3U8 found")

    return None, None


async def get_events(url: str, cached_keys: list[str]) -> list[dict[str, str]]:
    events = []

    if not (html_data := await network.request(url, log=log)):
        return events

    soup = HTMLParser(html_data.content)

    sport = "Live Event"

    for node in soup.css("a"):
        if not node.attributes.get("class"):
            continue

        if (parent := node.parent) and "my-1" in parent.attributes.get("class", ""):
            if span := node.css_first("span"):
                sport = span.text(strip=True)

        sport = fix_txt(sport)

        if not (teams := [t.text(strip=True) for t in node.css(".col-7 .col-12")]):
            continue

        if not (href := node.attributes.get("href")):
            continue

        href = urlparse(href).path if href.startswith("http") else href

        if not (time_node := node.css_first(".col-3 span")):
            continue

        if time_node.text(strip=True) != "MatchStarted":
            continue

        event_name = fix_txt(" vs ".join(teams))

        if f"[{sport}] {event_name} ({TAG})" in cached_keys:
            continue

        events.append(
            {
                "sport": sport,
                "event": event_name,
                "href": href,
            }
        )

    return events


async def scrape() -> None:
    cached_urls = CACHE_FILE.load()

    valid_urls = {k: v for k, v in cached_urls.items() if v["url"]}

    valid_count = cached_count = len(valid_urls)

    urls.update(valid_urls)

    log.info(f"Loaded {cached_count} event(s) from cache")

    if not (base_url := await network.get_base([mirr["base"] for mirr in MIRRORS])):
        log.warning("No working TotalSportek mirrors")

        CACHE_FILE.write(cached_urls)

        return

    events = await get_events(base_url, cached_urls.keys())

    log.info(f"Processing {len(events)} new URL(s)")

    if events:
        now = Time.clean(Time.now())

        for i, ev in enumerate(events, start=1):
            handler = partial(
                process_event,
                href=ev["href"],
                url_num=i,
            )

            url, iframe = await network.safe_process(
                handler,
                url_num=i,
                semaphore=network.HTTP_S,
                log=log,
            )

            sport, event, href = (
                ev["sport"],
                ev["event"],
                ev["href"],
            )

            key = f"[{sport}] {event} ({TAG})"

            tvg_id, logo = leagues.get_tvg_info(sport, event)

            entry = {
                "url": url,
                "logo": logo,
                "base": iframe,
                "timestamp": now.timestamp(),
                "id": tvg_id or "Live.Event.us",
                "href": href,
            }

            cached_urls[key] = entry

            if url:
                valid_count += 1

                urls[key] = entry

    if new_count := valid_count - cached_count:
        log.info(f"Collected and cached {new_count} new event(s)")

    else:
        log.info("No new events found")

    CACHE_FILE.write(cached_urls)
