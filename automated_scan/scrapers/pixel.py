import json
from functools import partial

from playwright.async_api import BrowserContext, async_playwright

from .utils import Cache, Time, get_logger, leagues, network

log = get_logger(__name__)

urls: dict[str, dict[str, str | float]] = {}

TAG = "PIXEL"

CACHE_FILE = Cache(f"{TAG.lower()}.json", exp=19_800)

BASE_URL = "https://pixelsport.tv/backend/livetv/events"


async def get_api_data(context: BrowserContext) -> dict[str, list[dict, str, str]]:
    try:
        page = await context.new_page()

        await page.goto(
            BASE_URL,
            wait_until="domcontentloaded",
            timeout=10_000,
        )

        raw_json = await page.locator("pre").inner_text(timeout=5_000)
    except Exception as e:
        log.error(f'Failed to fetch "{BASE_URL}": {e}')

        return {}

    return json.loads(raw_json)


async def get_events(context: BrowserContext) -> dict[str, dict[str, str | float]]:
    now = Time.clean(Time.now())

    api_data = await get_api_data(context)

    events = {}

    for event in api_data.get("events", []):
        event_dt = Time.from_str(event["date"], timezone="UTC")

        if event_dt.date() != now.date():
            continue

        event_name = event["match_name"]

        channel_info: dict[str, str] = event["channel"]

        category: dict[str, str] = channel_info["TVCategory"]

        sport = category["name"]

        stream_urls = [(i, f"server{i}URL") for i in range(1, 4)]

        for z, stream_url in stream_urls:
            if (stream_link := channel_info.get(stream_url)) and stream_link != "null":
                key = f"[{sport}] {event_name} {z} ({TAG})"

                tvg_id, logo = leagues.get_tvg_info(sport, event_name)

                events[key] = {
                    "url": stream_link,
                    "logo": logo,
                    "base": "https://pixelsport.tv",
                    "timestamp": now.timestamp(),
                    "id": tvg_id or "Live.Event.us",
                }

    return events


async def scrape() -> None:
    if cached := CACHE_FILE.load():
        urls.update(cached)

        log.info(f"Loaded {len(urls)} event(s) from cache")

        return

    log.info(f'Scraping from "{BASE_URL}"')

    async with async_playwright() as p:
        browser, context = await network.browser(p)

        try:
            handler = partial(get_events, context=context)

            events = await network.safe_process(
                handler,
                url_num=1,
                semaphore=network.PW_S,
                log=log,
            )

        finally:
            await browser.close()

    urls.update(events or {})

    CACHE_FILE.write(urls)

    log.info(f"Collected and cached {len(urls)} new event(s)")
