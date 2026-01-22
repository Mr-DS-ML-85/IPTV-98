#!/usr/bin/env python3
import asyncio
import re
from pathlib import Path

from scrapers import (
    cdnlivetv,
    embedhd,
    fawa,
    istreameast,
    pawa,
    pixel,
    ppv,
    roxie,
    shark,
    sport9,
    streambtw,
    streamcenter,
    streamfree,
    streamhub,
    streamsgate,
    strmd,
    totalsportek,
    tvpass,
    watchfooty,
    webcast,
)
from scrapers.utils import get_logger, network

log = get_logger(__name__)

BASE_FILE = Path(__file__).parent / "base.m3u8"

EVENTS_FILE = Path(__file__).parent / "events.m3u8"

collect = Path("collect/")

COMBINED_FILE = collect  / "TV.m3u8"


def load_base() -> tuple[list[str], int]:
    log.info("Fetching base M3U8")

    data = BASE_FILE.read_text(encoding="utf-8")

    pattern = re.compile(r'tvg-chno="(\d+)"')

    last_chnl_num = max(map(int, pattern.findall(data)), default=0)

    return data.splitlines(), last_chnl_num


async def main() -> None:
    log.info(f"{'=' * 10} Scraper Started {'=' * 10}")

    base_m3u8, tvg_chno = load_base()

    tasks = [
        asyncio.create_task(cdnlivetv.scrape()),
        asyncio.create_task(embedhd.scrape()),
        asyncio.create_task(fawa.scrape()),
        asyncio.create_task(istreameast.scrape()),
        asyncio.create_task(pawa.scrape()),
        asyncio.create_task(pixel.scrape()),
        asyncio.create_task(ppv.scrape()),
        asyncio.create_task(roxie.scrape()),
        asyncio.create_task(shark.scrape()),
        asyncio.create_task(sport9.scrape()),
        # asyncio.create_task(streambtw.scrape()),
        asyncio.create_task(streamcenter.scrape()),
        asyncio.create_task(streamfree.scrape()),
        asyncio.create_task(streamhub.scrape()),
        asyncio.create_task(streamsgate.scrape()),
        asyncio.create_task(strmd.scrape()),
        asyncio.create_task(totalsportek.scrape()),
        asyncio.create_task(tvpass.scrape()),
        asyncio.create_task(webcast.scrape()),
    ]

    await asyncio.gather(*tasks)

    await watchfooty.scrape()

    additions = (
        cdnlivetv.urls
        | embedhd.urls
        | fawa.urls
        | istreameast.urls
        | pawa.urls
        | pixel.urls
        | ppv.urls
        | roxie.urls
        | shark.urls
        | sport9.urls
        | streambtw.urls
        | streamcenter.urls
        | streamfree.urls
        | streamhub.urls
        | streamsgate.urls
        | strmd.urls
        | totalsportek.urls
        | tvpass.urls
        | watchfooty.urls
        | webcast.urls
    )

    live_events: list[str] = []

    combined_channels: list[str] = []

    for i, (event, info) in enumerate(
        sorted(additions.items()),
        start=1,
    ):
        extinf_all = (
            f'#EXTINF:-1 tvg-chno="{tvg_chno + i}" tvg-id="{info["id"]}" '
            f'tvg-name="{event}" tvg-logo="{info["logo"]}" group-title="Live Events",{event}'
        )

        extinf_live = (
            f'#EXTINF:-1 tvg-chno="{i}" tvg-id="{info["id"]}" '
            f'tvg-name="{event}" tvg-logo="{info["logo"]}" group-title="Live Events",{event}'
        )

        vlc_block = [
            f'#EXTVLCOPT:http-referrer={info["base"]}',
            f'#EXTVLCOPT:http-origin={info["base"]}',
            f"#EXTVLCOPT:http-user-agent={network.UA}",
            info["url"],
        ]

        combined_channels.extend(["\n" + extinf_all, *vlc_block])

        live_events.extend(["\n" + extinf_live, *vlc_block])

    COMBINED_FILE.write_text(
        "\n".join(base_m3u8 + combined_channels),
        encoding="utf-8",
    )

    log.info(f"Base + Events saved to {COMBINED_FILE.resolve()}")

    EVENTS_FILE.write_text(
        '#EXTM3U url-tvg="https://raw.githubusercontent.com/doms9/iptv/refs/heads/default/EPG/TV.xml"\n'
        + "\n".join(live_events),
        encoding="utf-8",
    )

    log.info(f"Events saved to {EVENTS_FILE.resolve()}")

    for hndlr in log.handlers:
        hndlr.flush()
        hndlr.stream.write("\n")


if __name__ == "__main__":
    asyncio.run(main())

    try:
        asyncio.run(network.client.aclose())
    except Exception:
        pass
