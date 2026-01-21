import asyncio
import logging
import random
import re
from collections.abc import Awaitable, Callable
from functools import partial
from typing import TypeVar
from urllib.parse import urlencode, urljoin

import httpx
from playwright.async_api import Browser, BrowserContext, Playwright, Request

from .logger import get_logger

logger = get_logger(__name__)

T = TypeVar("T")


class Network:
    UA = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/134.0.0.0 Safari/537.36 Edg/134.0.0.0"
    )

    HTTP_S = asyncio.Semaphore(10)

    PW_S = asyncio.Semaphore(3)

    proxy_base = "https://stream.nvrmind.xyz"

    def __init__(self) -> None:
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(5.0),
            follow_redirects=True,
            headers={"User-Agent": Network.UA},
            http2=True,
        )

    @staticmethod
    def build_proxy_url(
        tag: str,
        path: str,
        query: dict | None = None,
    ) -> str:

        tag = tag.lower()

        return (
            f"{urljoin(network.proxy_base, f'{tag}/{path}')}?{urlencode(query)}"
            if query
            else urljoin(network.proxy_base, f"{tag}/{path}")
        )

    async def request(
        self,
        url: str,
        log: logging.Logger | None = None,
        **kwargs,
    ) -> httpx.Response | None:

        log = log or logger

        try:
            r = await self.client.get(url, **kwargs)

            r.raise_for_status()

            return r
        except (httpx.HTTPError, httpx.TimeoutException) as e:
            log.error(f'Failed to fetch "{url}": {e}')

            return ""

    async def get_base(self, mirrors: list[str]) -> str | None:
        random.shuffle(mirrors)

        for mirror in mirrors:
            if not (r := await self.request(mirror)):
                continue

            elif r.status_code != 200:
                continue

            return mirror

    @staticmethod
    async def safe_process(
        fn: Callable[[], Awaitable[T]],
        url_num: int,
        semaphore: asyncio.Semaphore,
        timeout: int | float = 10,
        log: logging.Logger | None = None,
    ) -> T | None:

        log = log or logger

        async with semaphore:
            task = asyncio.create_task(fn())

            try:
                return await asyncio.wait_for(task, timeout=timeout)

            except asyncio.TimeoutError:
                log.warning(
                    f"URL {url_num}) Timed out after {timeout}s, skipping event"
                )

                task.cancel()

                try:
                    await task
                except asyncio.CancelledError:
                    pass

                except Exception as e:
                    log.debug(f"URL {url_num}) Ignore exception after timeout: {e}")

                return
            except Exception as e:
                log.error(f"URL {url_num}) Unexpected error: {e}")

                return

    @staticmethod
    def capture_req(
        req: Request,
        captured: list[str],
        got_one: asyncio.Event,
    ) -> None:

        invalids = ["amazonaws", "knitcdn"]

        escaped = [re.escape(i) for i in invalids]

        pattern = re.compile(
            rf"^(?!.*({'|'.join(escaped)})).*\.m3u8",
            re.IGNORECASE,
        )

        if pattern.search(req.url):
            captured.append(req.url)
            got_one.set()

    async def process_event(
        self,
        url: str,
        url_num: int,
        context: BrowserContext,
        timeout: int | float = 10,
        log: logging.Logger | None = None,
    ) -> str | None:

        log = log or logger

        page = await context.new_page()

        captured: list[str] = []

        got_one = asyncio.Event()

        handler = partial(
            self.capture_req,
            captured=captured,
            got_one=got_one,
        )

        page.on("request", handler)

        try:
            await page.goto(
                url,
                wait_until="domcontentloaded",
                timeout=15_000,
            )

            wait_task = asyncio.create_task(got_one.wait())

            try:
                await asyncio.wait_for(wait_task, timeout=timeout)
            except asyncio.TimeoutError:
                log.warning(f"URL {url_num}) Timed out waiting for M3U8.")

                return

            finally:
                if not wait_task.done():
                    wait_task.cancel()

                    try:
                        await wait_task
                    except asyncio.CancelledError:
                        pass

            if captured:
                log.info(f"URL {url_num}) Captured M3U8")

                return captured[0]

            log.warning(f"URL {url_num}) No M3U8 captured after waiting.")

            return

        except Exception as e:
            log.warning(f"URL {url_num}) Exception while processing: {e}")

            return

        finally:
            page.remove_listener("request", handler)

            await page.close()

    @staticmethod
    async def browser(
        playwright: Playwright, browser: str = "internal"
    ) -> tuple[Browser, BrowserContext]:
        if browser == "external":
            brwsr = await playwright.chromium.connect_over_cdp("http://localhost:9222")

            context = brwsr.contexts[0]

        else:
            brwsr = await playwright.firefox.launch(headless=True)

            context = await brwsr.new_context(
                user_agent=Network.UA,
                ignore_https_errors=False,
                viewport={"width": 1366, "height": 768},
                device_scale_factor=1,
                locale="en-US",
                timezone_id="America/New_York",
                color_scheme="dark",
                permissions=["geolocation"],
                extra_http_headers={
                    "Accept-Language": "en-US,en;q=0.9",
                    "Upgrade-Insecure-Requests": "1",
                },
            )

            await context.add_init_script("""
            Object.defineProperty(navigator, "webdriver", { get: () => undefined });

            Object.defineProperty(navigator, "languages", {
            get: () => ["en-US", "en"],
            });

            Object.defineProperty(navigator, "plugins", {
            get: () => [1, 2, 3, 4],
            });

            const elementDescriptor = Object.getOwnPropertyDescriptor(
            HTMLElement.prototype,
            "offsetHeight"
            );

            Object.defineProperty(HTMLDivElement.prototype, "offsetHeight", {
            ...elementDescriptor,
            get: function () {
                if (this.id === "modernizr") {
                return 24;
                }
                return elementDescriptor.get.apply(this);
            },
            });

            Object.defineProperty(window.screen, "width", { get: () => 1366 });
            Object.defineProperty(window.screen, "height", { get: () => 768 });

            const getParameter = WebGLRenderingContext.prototype.getParameter;

            WebGLRenderingContext.prototype.getParameter = function (param) {
            if (param === 37445) return "Intel Inc."; //  UNMASKED_VENDOR_WEBGL
            if (param === 37446) return "Intel Iris OpenGL    Engine"; // UNMASKED_RENDERER_WEBGL
            return getParameter.apply(this, [param]);
            };

            const observer = new MutationObserver((mutations) => {
            mutations.forEach((mutation) => {
                mutation.addedNodes.forEach((node) => {
                if (node.tagName === "IFRAME" && node.hasAttribute("sandbox")) {
                    node.removeAttribute("sandbox");
                }
                });
            });
            });

            observer.observe(document.documentElement, { childList: true, subtree: true });

            """)

        return brwsr, context


network = Network()

__all__ = ["network"]
