import re

from .utils import Cache, Time, get_logger, leagues, network

log = get_logger(__name__)

urls: dict[str, dict[str, str | float]] = {}

TAG = "TVPASS"

CACHE_FILE = Cache(f"{TAG.lower()}.json", exp=86_400)

BASE_URL = "https://tvpass.org/playlist/m3u"


async def get_events() -> dict[str, dict[str, str | float]]:
    events = {}

    if not (r := await network.request(BASE_URL, log=log)):
        return events

    now = Time.clean(Time.now())

    data = r.text.splitlines()

    for i, line in enumerate(data, start=1):
        if line.startswith("#EXTINF"):
            tvg_id_match = re.search(r'tvg-id="([^"]*)"', line)

            tvg_name_match = re.search(r'tvg-name="([^"]*)"', line)

            group_title_match = re.search(r'group-title="([^"]*)"', line)

            tvg = tvg_id_match[1] if tvg_id_match else None

            if not tvg and (url := data[i]).endswith("/sd"):
                if tvg_name := tvg_name_match[1]:
                    sport = group_title_match[1].upper().strip()

                    event = "(".join(tvg_name.split("(")[:-1]).strip()

                    key = f"[{sport}] {event} ({TAG})"

                    channel = url.split("/")[-2]

                    tvg_id, logo = leagues.info(sport)

                    events[key] = {
                        "url": f"http://origin.thetvapp.to/hls/{channel}/mono.m3u8",
                        "logo": logo,
                        "id": tvg_id or "Live.Event.us",
                        "base": "https://tvpass.org",
                        "timestamp": now.timestamp(),
                    }

    return events


async def scrape() -> None:
    if cached := CACHE_FILE.load():
        urls.update(cached)

        log.info(f"Loaded {len(urls)} event(s) from cache")

        return

    log.info(f'Scraping from "{BASE_URL}"')

    events = await network.safe_process(
        get_events,
        url_num=1,
        semaphore=network.HTTP_S,
        log=log,
    )

    urls.update(events or {})

    CACHE_FILE.write(urls)

    log.info(f"Collected and cached {len(urls)} new event(s)")
