from functools import partial

from playwright.async_api import async_playwright

from .utils import Cache, Time, get_logger, leagues, network

log = get_logger(__name__)

urls: dict[str, dict[str, str | float]] = {}

TAG = "PPV"

CACHE_FILE = Cache(f"{TAG.lower()}.json", exp=10_800)

API_FILE = Cache(f"{TAG.lower()}-api.json", exp=19_800)

MIRRORS = [
    "https://old.ppv.to/api/streams",
    "https://api.ppvs.su/api/streams",
    "https://api.ppv.to/api/streams",
]


async def get_events(url: str, cached_keys: list[str]) -> list[dict[str, str]]:
    now = Time.clean(Time.now())

    if not (api_data := API_FILE.load(per_entry=False)):
        log.info("Refreshing API cache")

        api_data = {"timestamp": now.timestamp()}

        if r := await network.request(url, log=log):
            api_data: dict = r.json()

        API_FILE.write(api_data)

    events = []

    start_dt = now.delta(minutes=-30)
    end_dt = now.delta(minutes=30)

    for stream_group in api_data.get("streams", []):
        sport = stream_group["category"]

        if sport == "24/7 Streams":
            continue

        for event in stream_group.get("streams", []):
            name = event.get("name")

            start_ts = event.get("starts_at")

            logo = event.get("poster")

            iframe = event.get("iframe")

            if not (name and start_ts and iframe):
                continue

            if f"[{sport}] {name} ({TAG})" in cached_keys:
                continue

            event_dt = Time.from_ts(start_ts)

            if not start_dt <= event_dt <= end_dt:
                continue

            events.append(
                {
                    "sport": sport,
                    "event": name,
                    "link": iframe,
                    "logo": logo,
                    "timestamp": event_dt.timestamp(),
                }
            )

    return events


async def scrape() -> None:
    cached_urls = CACHE_FILE.load()

    cached_count = len(cached_urls)

    urls.update(cached_urls)

    log.info(f"Loaded {cached_count} event(s) from cache")

    if not (base_url := await network.get_base(MIRRORS)):
        log.warning("No working PPV mirrors")

        CACHE_FILE.write(cached_urls)

        return

    log.info(f'Scraping from "{base_url}"')

    events = await get_events(base_url, cached_urls.keys())

    log.info(f"Processing {len(events)} new URL(s)")

    if events:
        async with async_playwright() as p:
            browser, context = await network.browser(p, browser="external")

            try:
                for i, ev in enumerate(events, start=1):
                    handler = partial(
                        network.process_event,
                        url=ev["link"],
                        url_num=i,
                        context=context,
                        timeout=6,
                        log=log,
                    )

                    url = await network.safe_process(
                        handler,
                        url_num=i,
                        semaphore=network.PW_S,
                        log=log,
                    )

                    if url:
                        sport, event, logo, ts, link = (
                            ev["sport"],
                            ev["event"],
                            ev["logo"],
                            ev["timestamp"],
                            ev["link"],
                        )

                        key = f"[{sport}] {event} ({TAG})"

                        tvg_id, pic = leagues.get_tvg_info(sport, event)

                        entry = {
                            "url": url,
                            "logo": logo or pic,
                            "base": link,
                            "timestamp": ts,
                            "id": tvg_id or "Live.Event.us",
                            "link": link,
                        }

                        urls[key] = cached_urls[key] = entry

            finally:
                await browser.close()

    if new_count := len(cached_urls) - cached_count:
        log.info(f"Collected and cached {new_count} new event(s)")

    else:
        log.info("No new events found")

    CACHE_FILE.write(cached_urls)
