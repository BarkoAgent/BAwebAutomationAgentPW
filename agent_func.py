import re
import os
import json
import time
import asyncio
import logging
import threading

import ba_ws_sdk.streaming as streaming
import ba_ws_sdk.file_system as file_system
from dotenv import load_dotenv as _load_dotenv
from playwright.async_api import async_playwright
_load_dotenv()

DEFAULT_TIMEOUT = int(os.getenv("DEFAULT_TIMEOUT", "3"))  # seconds; Playwright expects ms

test_timeout: dict[str, int] = {}
test_variables = {}
driver: dict[str, object] = {}
run_test_id = ""

# ─── OCR (EasyOCR) configuration ─────────────────────────────────────────────
# Languages the OCR reader recognises (comma-separated, e.g. "en,es").
OCR_LANGUAGES = [l.strip() for l in os.getenv("OCR_LANGUAGES", "en").split(",") if l.strip()] or ["en"]
# Minimum EasyOCR confidence (0-1) for a detection to be considered a match.
try:
    OCR_MIN_CONFIDENCE = float(os.getenv("OCR_MIN_CONFIDENCE", "0.3"))
except ValueError:
    OCR_MIN_CONFIDENCE = 0.3

_ocr_reader = None  # Lazily/eagerly constructed easyocr.Reader singleton


def _init_ocr_reader():
    """
    Construct the (heavy) EasyOCR reader once and cache it as a module-level
    singleton. Called eagerly at agent startup via a background warmup thread so
    the first OCR action does not pay the model-load cost, and again as a
    fallback from _get_ocr_reader() if warmup hasn't finished. Never raises —
    on failure it logs and leaves the reader as None.
    """
    global _ocr_reader
    if _ocr_reader is not None:
        return _ocr_reader
    try:
        import easyocr  # heavy import (pulls in torch); kept local on purpose
        logging.info(f"Loading EasyOCR reader (languages={OCR_LANGUAGES})...")
        _ocr_reader = easyocr.Reader(OCR_LANGUAGES, gpu=True)
        logging.info("EasyOCR reader loaded.")
    except Exception as e:
        logging.error(f"Failed to initialize EasyOCR reader: {e}")
        _ocr_reader = None
    return _ocr_reader


def _get_ocr_reader():
    """Return the cached EasyOCR reader, building it synchronously if needed."""
    if _ocr_reader is None:
        return _init_ocr_reader()
    return _ocr_reader


async def _ocr_screenshot_bytes(page) -> bytes:
    """
    Capture a viewport screenshot for OCR. Prefers scale="css" (one pixel per CSS
    pixel) when available; the caller (_ocr_detect) rescales coordinates from the
    actual image size to the CSS viewport regardless, so the fallback is safe too.
    """
    try:
        return await page.screenshot(scale="css")
    except TypeError:
        # Older Playwright builds don't support the `scale` kwarg.
        return await page.screenshot()


async def _ocr_detect(page):
    """
    Screenshot the viewport, run EasyOCR, and return every detection as a list of
    {"text", "confidence", "x", "y"} dicts where (x, y) is the box center in CSS /
    page coordinates.

    Retina / HiDPI safe: the screenshot may be captured at a device scale factor
    (e.g. 2x on Retina, or whenever `scale="css"` is unavailable), which makes the
    image larger than the CSS viewport. EasyOCR returns coordinates in image
    pixels, but page.mouse.click() expects CSS pixels — so we measure the ratio
    between the actual image size and the CSS viewport and rescale accordingly.
    """
    reader = _get_ocr_reader()
    if reader is None:
        raise RuntimeError(
            "EasyOCR is not available. Ensure 'easyocr' is installed in the agent environment."
        )
    png_bytes = await _ocr_screenshot_bytes(page)
    viewport = page.viewport_size or {}

    def _run():
        from io import BytesIO
        from PIL import Image  # provided transitively by easyocr (Pillow)

        with BytesIO(png_bytes) as buf:
            img_w, img_h = Image.open(buf).size

        # image pixels -> CSS pixels. 1.0 when scale="css" (image == viewport),
        # 0.5 on a 2x Retina/HiDPI capture (image is twice the viewport), etc.
        vp_w = viewport.get("width")
        vp_h = viewport.get("height")
        scale_x = (vp_w / img_w) if (vp_w and img_w) else 1.0
        scale_y = (vp_h / img_h) if (vp_h and img_h) else 1.0

        items = []
        for bbox, detected, conf in reader.readtext(png_bytes, canvas_size=1000):
            if conf < OCR_MIN_CONFIDENCE:
                continue
            xs = [float(p[0]) for p in bbox]
            ys = [float(p[1]) for p in bbox]
            items.append({
                "text": detected,
                "confidence": float(conf),
                "x": (sum(xs) / len(xs)) * scale_x,
                "y": (sum(ys) / len(ys)) * scale_y,
            })
        return items

    return await asyncio.to_thread(_run)


async def _ocr_find_matches(page, text: str, match: str):
    """
    Return OCR detections matching `text` (center coords in CSS / page pixels).
    `match` is 'contains' (default, case-insensitive substring) or 'exact'
    (case-insensitive full match).
    """
    target = (text or "").strip().lower()
    matches = []
    for item in await _ocr_detect(page):
        d = (item["text"] or "").strip().lower()
        hit = (target in d) if match != "exact" else (d == target)
        if hit:
            matches.append(item)
    return matches


async def _ocr_wait_for_match(page, text: str, match: str, _run_test_id: str):
    """
    Poll OCR until the text is found or the default timeout elapses. Returns the
    list of matches on success; raises TimeoutError if nothing matches in time
    (mirrors how the locator-based methods rely on wait_for_selector's timeout).
    """
    deadline = time.time() + test_timeout[_run_test_id]
    while True:
        matches = await _ocr_find_matches(page, text, match)
        if matches:
            return matches
        if time.time() >= deadline:
            raise TimeoutError(
                f"OCR did not find text '{text}' within {test_timeout[_run_test_id]}s"
            )
        await asyncio.sleep(0.5)


def _warmup_ocr_reader():
    """Start a daemon thread at import time so the reader loads at agent startup."""
    threading.Thread(target=_init_ocr_reader, daemon=True, name="ocr-warmup").start()


def set_default_timeout(timeout: str, _run_test_id='1'):
    """
    Sets the default timeout in seconds for Playwright actions (default is 3 seconds). This can be called from the agent's test plan to adjust timeouts dynamically.

    Args:
        timeout: Timeout in seconds (can be passed as a string from the test plan)
    """
    global DEFAULT_TIMEOUT
    try:
        if int(timeout) > 30:
            logging.warning(f"Specified timeout {timeout}s is quite high and may lead to long waits. Will default to 30s if value is invalid.")
            test_timeout[_run_test_id] = int(30)
        else:
            test_timeout[_run_test_id] = int(timeout)
        logging.info(f"Default timeout set to {test_timeout[_run_test_id]} seconds.")
    except ValueError:
        logging.error(f"Invalid timeout value: '{timeout}'. Must be an integer representing seconds.")

# Max characters get_page_html will return (across all frames); the rest is truncated.
GET_PAGE_HTML_MAX_CHARS = int(os.getenv("GET_PAGE_HTML_MAX_CHARS", "40000"))
# Per-frame char budget when aggregating multiple frames (defaults to the overall cap).
GET_PAGE_HTML_PER_FRAME_MAX_CHARS = int(
    os.getenv("GET_PAGE_HTML_PER_FRAME_MAX_CHARS", str(GET_PAGE_HTML_MAX_CHARS))
)
# Safety cap on how many frames get_page_html will read (Fiori can spawn many).
GET_PAGE_HTML_MAX_FRAMES = int(os.getenv("GET_PAGE_HTML_MAX_FRAMES", "12"))

_PRUNE_DOM_JS = r"""
() => {
  const ALLOWED = new Set(['id','name','class','type','role','href','src','alt',
    'title','value','placeholder','for','label','checked','selected','disabled',
    'readonly','required','tabindex','aria-label','aria-labelledby',
    'aria-describedby','aria-expanded','aria-checked','aria-selected',
    'aria-current','data-testid']);
  // Attributes whose values are useful as locators and must not be truncated.
  const NO_TRUNC = new Set(['id','name','class','for','aria-label','aria-labelledby']);
  const DROP = new Set(['SCRIPT','STYLE','SVG','NOSCRIPT','LINK','META','HEAD',
    'TEMPLATE','BASE','IFRAME','PATH','USE']);
  const VOID = new Set(['INPUT','IMG','BR','HR','AREA','COL','EMBED','SOURCE','TRACK','WBR']);
  const esc = (s) => s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');

  const isHidden = (el) => {
    if (el.hidden) return true;
    if (el.getAttribute && el.getAttribute('aria-hidden') === 'true') return true;
    const cs = window.getComputedStyle(el);
    if (cs && (cs.display === 'none' || cs.visibility === 'hidden' || cs.visibility === 'collapse')) return true;
    return false;
  };

  const attrs = (el) => {
    let out = '';
    for (const a of Array.from(el.attributes || [])) {
      const n = a.name.toLowerCase();
      if (!ALLOWED.has(n)) continue;
      let v = a.value || '';
      if ((n === 'src' || n === 'href') && v.startsWith('data:')) v = 'data:[omitted]';
      else if (!NO_TRUNC.has(n) && v.length > 200) v = v.slice(0, 200) + '…';
      out += ' ' + n + '="' + esc(v) + '"';
    }
    return out;
  };

  const walk = (el) => {
    if (DROP.has(el.tagName) || isHidden(el)) return '';
    const tag = el.tagName.toLowerCase();
    let html = '<' + tag + attrs(el);
    if (VOID.has(el.tagName)) return html + '/>';
    html += '>';
    for (const node of Array.from(el.childNodes)) {
      if (node.nodeType === 3) {                 // text node
        const t = node.textContent.replace(/\s+/g, ' ');
        if (t.trim()) html += esc(t);
      } else if (node.nodeType === 1) {          // element node
        html += walk(node);
      }
    }
    return html + '</' + tag + '>';
  };

  return walk(document.body || document.documentElement);
}
"""


async def _get_pruned_or_full_html(target) -> str:
    """
    Return the attribute-pruned DOM for a Page or Frame, falling back to full
    content on JS error. Both Page and Frame expose .evaluate() and .content().
    """
    try:
        html = await target.evaluate(_PRUNE_DOM_JS)
        if html:
            return html
    except Exception as e:
        # A navigation race will re-raise from .content() below and trigger the
        # caller's retry loop; a benign JS error just falls back to full HTML.
        logging.warning(f"[get_page_html] DOM prune failed ({e}); using full content")
    return await target.content()


async def _collect_frame_html(page, active_frame=None) -> str:
    """
    Frame-aware HTML reader. Returns the pruned/cleaned DOM of the *primary*
    frame first (the frame the agent switched into via change_frame_*, else the
    main frame), then appends the content of every other non-empty frame on the
    page. This keeps the DOM aligned with what is actually painted on screen — 
    the top document alone often only shows the launchpad shell ("My Home") while 
    the real app lives in a child frame.
    """
    primary = active_frame or page.main_frame

    parts = []
    primary_html = _clean_html(
        await _get_pruned_or_full_html(primary),
        max_chars=GET_PAGE_HTML_PER_FRAME_MAX_CHARS,
    )
    parts.append(primary_html)

    frames_read = 1
    for fr in page.frames:
        if frames_read >= GET_PAGE_HTML_MAX_FRAMES:
            parts.append("<!-- …additional frames omitted (GET_PAGE_HTML_MAX_FRAMES reached) -->")
            break
        if fr is primary:
            continue
        try:
            url = fr.url or ""
        except Exception:
            url = ""
        if not url or url == "about:blank":
            continue
        try:
            fr_html = _clean_html(
                await _get_pruned_or_full_html(fr),
                max_chars=GET_PAGE_HTML_PER_FRAME_MAX_CHARS,
            )
        except Exception as e:
            logging.warning(f"[get_page_html] failed reading frame {url}: {e}")
            continue
        # Skip frames with no meaningful content (empty bodies, spacers, etc.).
        if fr_html and len(fr_html) > 60:
            name = fr.name or ""
            parts.append(f"<!-- IFRAME name={name!r} url={url!r} -->\n{fr_html}")
            frames_read += 1

    combined = "\n".join(parts)
    if len(combined) > GET_PAGE_HTML_MAX_CHARS:
        combined = (
            combined[:GET_PAGE_HTML_MAX_CHARS]
            + "\n<!-- …truncated (multiple frames). Switch into the relevant frame "
              "with change_frame_by_locator to get its full DOM. -->"
        )
    return combined


def _clean_html(html_content, max_chars=None):
    for tag in ['script', 'style', 'svg', 'noscript']:
        html_content = re.sub(rf'<{tag}[^>]*>.*?</{tag}>', '', html_content, flags=re.DOTALL)
    html_content = re.sub(r'<!--.*?-->', '', html_content, flags=re.DOTALL)  # HTML comments
    html_content = re.sub(r'>\s+<', '><', html_content)                       # whitespace between tags
    html_content = re.sub(r'[ \t]{2,}', ' ', html_content)                    # runs of spaces/tabs
    html_content = html_content.strip()
    if max_chars and len(html_content) > max_chars:
        total = len(html_content)
        html_content = (
            html_content[:max_chars]
            + f"\n<!-- …truncated ({total} chars total). Narrow your target with a more "
              "specific locator, or use get_all_text_elements for on-screen text. -->"
        )
    return html_content

async def stop_all_drivers(**kwargs):
    global driver
    for run_id, d in list(driver.items()):
        try:
            streaming.stop_stream(run_id)
        except Exception as e:
            logging.warning(f"Error stopping stream for '{run_id}': {e}")
        for resource, method in [('context', 'close'), ('browser', 'close'), ('playwright', 'stop')]:
            obj = d.get(resource)
            if obj:
                try:
                    await getattr(obj, method)()
                except Exception as e:
                    logging.warning(f"Error closing {resource} for driver '{run_id}': {e}")
        logging.info(f"Driver '{run_id}' stopped.")
    driver.clear()
    logging.info("All drivers stopped and cleared.")

async def create_driver(_run_test_id='1', url=None):
    """
    Creates a Playwright browser context and initializes test_variables for this run id.
    """
    global driver, test_variables, test_timeout
    test_timeout[_run_test_id] = DEFAULT_TIMEOUT
    test_variables[_run_test_id] = {}
    playwright = await async_playwright().start()
    browser = await playwright.chromium.launch(headless=True)
    context = await browser.new_context(
        viewport={'width': 800, 'height': 800},
        accept_downloads=True,
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.4472.124 Safari/537.36",
    )
    page = await context.new_page()

    # ── Download handler ─────────────────────────────────────────────────
    # Capture run_id in closure so the callback knows which run it belongs to.
    captured_run_id = _run_test_id

    async def _handle_download(download):
        """Automatically save browser-initiated downloads to the agent file system."""
        suggested = download.suggested_filename
        try:
            safe_name = file_system.sanitize_filename(suggested)
        except ValueError:
            safe_name = f"download_{int(time.time())}"

        file_system.on_download_started(captured_run_id, safe_name)

        try:
            # Wait for Playwright to finish downloading to its temp location
            path = await download.path()
            if path is None:
                file_system.on_download_failed(
                    captured_run_id, safe_name,
                    "Download was cancelled or browser context closed",
                )
                return

            # Save to attachments directory
            dest = file_system.get_attachments_dir() / safe_name
            os.makedirs(str(file_system.get_attachments_dir()), exist_ok=True)
            await download.save_as(str(dest))

            file_system.on_download_complete(captured_run_id, safe_name, str(dest))
        except Exception as e:
            logging.error(f"[Download] Error handling download {safe_name}: {e}")
            file_system.on_download_failed(captured_run_id, safe_name, str(e))

    def _attach_download_handler(target_page):
        """Register the download handler on a page."""
        target_page.on("download", lambda dl: asyncio.ensure_future(_handle_download(dl)))

    _attach_download_handler(page)

    # Also register on any new pages the context opens (popups, new tabs)
    context.on("page", lambda new_page: _attach_download_handler(new_page))
    # ─────────────────────────────────────────────────────────────────────

    driver[_run_test_id] = {'playwright': playwright, 'browser': browser, 'context': context, 'page': page}

    main_url = url or os.getenv("MAIN_URL", "https://google.com")
    _stream_stop_after_raw = os.getenv("STREAM_STOP_AFTER_S", "600")
    try:
        _stream_stop_after = float(_stream_stop_after_raw)
    except ValueError:
        logging.warning(f"Invalid STREAM_STOP_AFTER_S value '{_stream_stop_after_raw}', using default 600s")
        _stream_stop_after = 600.0
    await streaming.astart_stream(driver[_run_test_id], run_id=_run_test_id, fps=0.5, jpeg_quality=70, stop_after=_stream_stop_after)
    await page.goto(main_url)
    return "driver created"

async def stop_driver(_run_test_id='1'):
    """
    Stops Playwright context for given run id.
    """
    global driver
    if _run_test_id in driver:
        await driver[_run_test_id]['context'].close()
        await driver[_run_test_id]['browser'].close()
        await driver[_run_test_id]['playwright'].stop()
        streaming.stop_stream(_run_test_id)
        file_system.clear_downloads(_run_test_id)
        return "success"
    return "no driver"

async def maximize_window(_run_test_id='1'):
    """
    Playwright does not support maximizing window directly, but you can set viewport size.
    """
    global driver
    await driver[_run_test_id]['page'].set_viewport_size({"width": 1280, "height": 800})
    return "success maximizing"

async def add_cookie(name, value, _run_test_id='1', use_vars='false'):
    """
    Adds cookie by name and value.
    """
    global driver, test_variables
    page = driver[_run_test_id]['page']
    context = driver[_run_test_id]['context']
    if use_vars == 'true' and _run_test_id in test_variables:
        name = test_variables[_run_test_id].get(name, name)
        value = test_variables[_run_test_id].get(value, value)
    await context.add_cookies([{'name': name, 'value': value, 'url': page.url}])
    return "Cookies added"

async def navigate_to_url(url: str, _run_test_id='1', use_vars='false') -> str:
    """
    Navigates the browser to a specific URL.
    """
    global driver, test_variables
    page = driver[_run_test_id]['page']
    if use_vars == 'true' and _run_test_id in test_variables:
        url = test_variables[_run_test_id].get(url, url)
    await page.goto(url)
    return url

async def send_keys(locator: str, value: str, _run_test_id='1', use_vars: str = 'false') -> str:
    """
    Playwright supports multiple locator strategies. Here are some common examples:
    CSS Selectors: The most common and powerful method for locating elements based on their HTML structure, classes, IDs, or attributes.
    'css=#my-id.my-class > button:visible'
    // Shorthand (prefix is optional for CSS):
    '.my-class'
    XPath Selectors: Used for targeting elements based on their hierarchical position, especially when unique attributes are missing.
    'xpath=//button[contains(text(), "Click me")]'
    ID Selectors: Directly target elements by their ID attribute.
    'id=my-unique-id'

    Types `value` into element specified by locator.
    """
    global driver, test_variables
    page = driver[_run_test_id].get('frame') or driver[_run_test_id]['page']
    if use_vars == 'true' and _run_test_id in test_variables:
        value = test_variables[_run_test_id].get(value, value)
    await page.wait_for_selector(locator, state="visible", timeout=test_timeout[_run_test_id] * 1000)
    await page.fill(locator, value)
    return value

async def exists(locator: str, _run_test_id='1') -> str:
    """
    Waits until element is visible (exists).
    """
    global driver
    page = driver[_run_test_id].get('frame') or driver[_run_test_id]['page']
    await page.wait_for_selector(locator, state="visible", timeout=test_timeout[_run_test_id] * 1000)
    return "exists"

async def exists_with_text(text: str, _run_test_id='1', use_vars: str = 'false') -> str:
    """
    Asserts that an element containing the given text exists.
    """
    global driver, test_variables
    page = driver[_run_test_id]['page']
    if use_vars == 'true' and _run_test_id in test_variables:
        text = test_variables[_run_test_id].get(text, text)
    locator = f"text={text}"
    await page.wait_for_selector(locator, timeout=test_timeout[_run_test_id] * 1000)
    return "exists (text)"

async def does_not_exist(locator: str, _run_test_id='1') -> str:
    """
    Waits until element does NOT exist.
    """
    global driver
    page = driver[_run_test_id]['page']
    await page.wait_for_selector(locator, state='detached', timeout=test_timeout[_run_test_id] * 1000)
    return "doesn't exists"

async def scroll_to_element(locator: str, _run_test_id='1') -> str:
    """
    Scrolls until the element is visible in the viewport.
    """
    global driver
    page = driver[_run_test_id].get('frame') or driver[_run_test_id]['page']
    await page.wait_for_selector(locator, timeout=test_timeout[_run_test_id] * 1000)
    await page.eval_on_selector(locator, "el => el.scrollIntoView({block: 'center', inline: 'nearest'})")
    return "scrolled"

async def click(locator: str, _run_test_id='1') -> str:
    """
    Clicks in the element defined by its locator.

    Playwright supports multiple locator strategies. Here are some common examples:
    CSS Selectors: The most common and powerful method for locating elements based on their HTML structure, classes, IDs, or attributes.
    'css=#my-id.my-class > button:visible'
    // Shorthand (prefix is optional for CSS):
    '.my-class'
    XPath Selectors: Used for targeting elements based on their hierarchical position, especially when unique attributes are missing.
    'xpath=//button[contains(text(), "Click me")]'
    ID Selectors: Directly target elements by their ID attribute.
    'id=my-unique-id'
    """
    global driver
    page = driver[_run_test_id].get('frame') or driver[_run_test_id]['page']
    await page.wait_for_selector(locator, state="visible", timeout=test_timeout[_run_test_id] * 1000)
    await page.click(locator)
    return "clicked successfully on the element"

async def double_click(locator: str, _run_test_id='1') -> str:
    """
    Double clicks on element.
    """
    global driver
    page = driver[_run_test_id].get('frame') or driver[_run_test_id]['page']
    await page.wait_for_selector(locator, state="visible", timeout=test_timeout[_run_test_id] * 1000)
    await page.dblclick(locator)
    return "double clicked"

async def right_click(locator: str, _run_test_id='1') -> str:
    """
    Right clicks on element.
    """
    global driver
    page = driver[_run_test_id].get('frame') or driver[_run_test_id]['page']
    await page.wait_for_selector(locator, state="visible", timeout=test_timeout[_run_test_id] * 1000)
    await page.click(locator, button='right')
    return "right clicked"

async def click_text_ocr(text: str, _run_test_id='1', match: str = 'contains', occurrence: str = '1', use_vars: str = 'false') -> str:
    """
    Clicks on visible TEXT located via OCR (EasyOCR) rather than a DOM locator.

    Use this as a fallback when text cannot be targeted with a normal CSS/XPath
    locator — e.g. text baked into a <canvas>, an image, a PDF/WebGL viewer, or a
    third-party widget with no stable selector. It screenshots the current page,
    runs OCR, finds the matching text and clicks the center of its bounding box.

    Notes:
      - Only text currently visible in the viewport can be found. Call
        scroll_to_element or scroll the page first if the text is off-screen.
      - The click is positional (page coordinates), so it works regardless of
        iframes/shadow DOM, but prefer a real locator with `click` when one exists.

    Waits up to the default timeout for the text to appear; raises if it does not.

    Args:
        text: The text to find and click.
        match: 'contains' (default, case-insensitive substring) or 'exact'.
        occurrence: 1-based index when multiple matches are found (default '1').
    """
    global driver, test_variables
    page = driver[_run_test_id]['page']
    if use_vars == 'true' and _run_test_id in test_variables:
        text = test_variables[_run_test_id].get(text, text)

    matches = await _ocr_wait_for_match(page, text, match, _run_test_id)

    try:
        idx = int(occurrence) - 1
    except (TypeError, ValueError):
        idx = 0
    if idx < 0 or idx >= len(matches):
        idx = 0
    m = matches[idx]

    await page.mouse.click(m["x"], m["y"])
    return "clicked successfully on the text via OCR"


async def exists_text_ocr(text: str, _run_test_id='1', match: str = 'contains', use_vars: str = 'false') -> str:
    """
    Asserts that visible TEXT is present on screen using OCR (EasyOCR).

    Like exists_with_text, but reads pixels instead of the DOM — use it to verify
    text rendered inside a <canvas>, image, or other non-DOM surface. Waits up to
    the default timeout for the text to appear; raises if it does not.

    Args:
        text: The text to look for.
        match: 'contains' (default, case-insensitive substring) or 'exact'.
    """
    global driver, test_variables
    page = driver[_run_test_id]['page']
    if use_vars == 'true' and _run_test_id in test_variables:
        text = test_variables[_run_test_id].get(text, text)

    await _ocr_wait_for_match(page, text, match, _run_test_id)
    return "exists (ocr text)"


async def get_all_text_elements(_run_test_id='1') -> str:
    """
    Returns ALL visible text on the current page detected via OCR (EasyOCR).

    Use this BEFORE get_page_html to understand what text is actually rendered on
    screen — it reads pixels, so it sees text inside <canvas>, images, and other
    non-DOM surfaces that HTML parsing would miss. Each entry includes the text,
    its OCR confidence, and the center coordinates (page pixels) of its bounding
    box, so a returned string can be fed directly to click_text_ocr / exists_text_ocr.

    Only text currently visible in the viewport is detected; scroll the page to
    reveal more if needed.

    Returns a JSON object: {"count": N, "elements": [{"text", "confidence", "x", "y"}, ...]}.
    """
    global driver
    page = driver[_run_test_id]['page']
    detections = await _ocr_detect(page)

    # Coordinates from _ocr_detect are already mapped to CSS / page pixels
    # (Retina / HiDPI safe); round them for a compact, click-ready result.
    elements = [
        {
            "text": d["text"],
            "confidence": round(d["confidence"], 2),
            "x": round(d["x"]),
            "y": round(d["y"]),
        }
        for d in detections
    ]
    return json.dumps({"count": len(elements), "elements": elements})


async def click_coordinates(x: str, y: str, _run_test_id='1') -> str:
    """
    Clicks at the given CSS pixel coordinates on the page.

    Use this when you know the exact (x, y) position from a screenshot —
    for example after a vision model or get_all_text_elements returns
    element positions. Coordinates must be in CSS pixels (the same space
    as click_text_ocr and get_all_text_elements), not physical/device pixels.

    Works regardless of iframes or shadow DOM — the click is positional.
    Prefer click(locator) when a stable DOM selector is available; use this
    for canvas elements, non-DOM widgets, or when acting on screenshot
    coordinates directly.

    Args:
        x: Horizontal CSS pixel position (numeric, e.g. '320').
        y: Vertical CSS pixel position (numeric, e.g. '240').
    """
    global driver
    page = driver[_run_test_id]['page']
    css_x = float(x)
    css_y = float(y)
    await page.mouse.move(css_x, css_y)
    await page.mouse.click(css_x, css_y)
    return f"clicked at ({css_x}, {css_y})"


async def scroll_by(dx: str, dy: str, _run_test_id='1') -> str:
    """
    Scrolls the page by the given number of CSS pixels using the mouse wheel.

    Positive dy scrolls DOWN; negative dy scrolls UP.
    Positive dx scrolls RIGHT; negative dx scrolls LEFT.

    Use scroll_to_element(locator) to scroll a specific element into view.
    Use this function when you need to scroll by a known pixel amount, e.g.
    to reveal content below the fold when acting from a screenshot.

    Args:
        dx: Horizontal scroll in CSS pixels (positive = right, negative = left).
        dy: Vertical scroll in CSS pixels (positive = down, negative = up).
    """
    global driver
    page = driver[_run_test_id]['page']
    await page.mouse.wheel(float(dx), float(dy))
    return f"scrolled by ({dx}, {dy})"


async def move_mouse(x: str, y: str, _run_test_id='1') -> str:
    """
    Moves the mouse pointer to the given CSS pixel coordinates WITHOUT clicking.

    Use this to:
    - Hover over an element to trigger a tooltip, dropdown, or hover state.
    - Position the mouse before calling scroll_by.
    - Drag preparation (move to source, then use mouse actions).

    Coordinates are in CSS pixels, matching the screenshot coordinate space.

    Args:
        x: Horizontal CSS pixel position.
        y: Vertical CSS pixel position.
    """
    global driver
    page = driver[_run_test_id]['page']
    await page.mouse.move(float(x), float(y))
    return f"mouse moved to ({x}, {y})"


async def run_javascript(script: str, _run_test_id='1') -> str:
    """
    Executes arbitrary JavaScript in the page and returns the result as a string.

    Use this for:
    - Reading values not accessible via DOM locators or OCR.
    - Triggering actions that cannot be performed via normal clicks/inputs.
    - Checking internal state (e.g. localStorage, sessionStorage, cookies).
    - Scrolling programmatically (e.g. window.scrollBy(0, 500)).

    The script runs in the context of the CURRENT frame (use change_frame_* first
    if you need to run JS inside an iframe). The result is JSON-serialised, so
    objects and arrays are returned as their JSON representation.

    CAUTION: Do not use this to bypass authentication or perform destructive
    operations unless the test explicitly requires it.

    Args:
        script: JavaScript expression or statement(s) to evaluate.
                Single expression: 'document.title'
                Multi-line: 'const a = 1; const b = 2; return a + b;'
                Wrap multi-line in an IIFE if needed:
                '(function(){ ... return result; })()'
    """
    global driver
    page = driver[_run_test_id].get('frame') or driver[_run_test_id]['page']
    result = await page.evaluate(script)
    return str(result)


async def type_keys(value: str, _run_test_id='1', clear: str = 'false', use_vars: str = 'false') -> str:
    """
    Types text into the CURRENTLY FOCUSED element via the keyboard — no locator
    needed. Use it right after click or click_text_ocr has focused a field. This
    is the way to fill inputs in screens whose controls
    have no stable DOM locator: click the field visually, then type.

    Args:
        value: The text to type.
        clear: 'true' to select-all (Control+A) and delete the existing content
               before typing — use when replacing a field's current value.
    """
    global driver, test_variables
    page = driver[_run_test_id]['page']
    if use_vars == 'true' and _run_test_id in test_variables:
        value = test_variables[_run_test_id].get(value, value)
    if clear == 'true':
        await page.keyboard.press('ControlOrMeta+A')
        await page.keyboard.press('Delete')
    await page.keyboard.type(value)
    return "typed text into the focused element"


async def press_key(key: str, _run_test_id='1') -> str:
    """
    Presses a single key or key chord, sent to the currently focused element.

    Useful for confirming/triggering value resolution and dismissing dialogs. 
    Examples: 'Enter' (e.g. resolve a Sold-to Party value),
    'Tab' (move to the next field), 'Escape' (dismiss a dialog), 'ArrowDown',
    'Control+A'. Key names follow Playwright's keyboard syntax.
    """
    global driver
    page = driver[_run_test_id]['page']
    await page.keyboard.press(key)
    return f"pressed {key}"


async def select_native_dropdown(locator: str, option: str, by: str = "label", _run_test_id='1') -> str:
    """
    Selects an option from a native <select> element.

    Args:
        locator: CSS/XPath selector for the <select> element.
        option: The value to select.
        by: How to match the option — "label" (visible text, default), "value" (option value attr), or "index" (0-based integer).
    """
    global driver
    page = driver[_run_test_id].get('frame') or driver[_run_test_id]['page']
    await page.wait_for_selector(locator, state="visible", timeout=test_timeout[_run_test_id] * 1000)

    if by == "value":
        await page.select_option(locator, value=option)
    elif by == "index":
        await page.select_option(locator, index=int(option))
    else:
        await page.select_option(locator, label=option)

    return "selected"

async def get_page_html(_run_test_id='1') -> str:
    """
    Returns condensed, frame-aware page HTML: the DOM is pruned to only
    locator-relevant attributes (id, name, class, type, role, aria-*, placeholder,
    href, text, …) with styles, scripts, SVG and framework noise removed, 
    and the result is capped at GET_PAGE_HTML_MAX_CHARS.

    Frame-aware: it returns the DOM of the frame you switched into with
    change_frame_* first, then appends the content of any child iframes.

    You can validate with get_all_text_elements for read on-screen text via OCR.
    """
    global driver
    page = driver[_run_test_id]['page']
    active_frame = driver[_run_test_id].get('frame')

    # page.content() can fail with "Unable to retrieve content because the page is
    # navigating and changing the content" if it's called mid-navigation. Retry a
    # few times, letting the page settle between attempts, before giving up.
    attempts = 4
    last_exc = None
    for attempt in range(1, attempts + 1):
        try:
            await page.wait_for_load_state('domcontentloaded', timeout=20000)
            return await _collect_frame_html(page, active_frame)
        except Exception as e:
            last_exc = e
            logging.warning(f"[get_page_html] attempt {attempt}/{attempts} failed: {e}")
            # Let the in-flight navigation finish, then retry.
            try:
                await page.wait_for_load_state('load', timeout=30000)
            except Exception:
                pass
            if attempt < attempts:
                await asyncio.sleep(0.5)

    raise last_exc

async def return_current_url(_run_test_id='1') -> str:
    """
    Returns current URL.
    """
    global driver
    page = driver[_run_test_id]['page']
    url = page.url
    return url

async def change_windows_tabs(_run_test_id='1') -> str:
    """
    Switches to another window/tab and returns cleaned HTML of the new active page.
    """
    global driver
    context = driver[_run_test_id]['context']
    pages = context.pages
    # Check for timeout or new page every 500ms, up to 10s total wait (since we don't know exactly when the new page will open)
    deadline = time.time() + test_timeout[_run_test_id]
    while time.time() < deadline:
        pages = context.pages
        if len(pages) > 1:
            break
        await asyncio.sleep(0.5)
    if len(pages) > 1:
        page = pages[-1]
        driver[_run_test_id]['page'] = page
        # The new tab has its own frames; drop any frame we were focused on.
        driver[_run_test_id].pop('frame', None)
        return await _collect_frame_html(page, None)
    return "no new tab"

async def change_frame_by_id(frame_name, _run_test_id='1') -> str:
    """
    Switches focus to the specified frame or iframe, by name.
    """
    global driver
    page = driver[_run_test_id]['page']
    frame = page.frame(name=frame_name)
    if frame:
        driver[_run_test_id]['frame'] = frame
        return "frame_changed"
    return "frame not found"

async def change_frame_by_locator(locator: str, _run_test_id='1') -> str:
    """
    Switches focus to the specified iframe by locator.
    """
    global driver
    page = driver[_run_test_id].get('frame') or driver[_run_test_id]['page']
    timeout_s = test_timeout[_run_test_id]
    deadline = time.time() + timeout_s
    logging.info(f"[change_frame_by_locator] locator={locator!r}, timeout={timeout_s}s, page_type={type(page).__name__}, is_frame={'frame' in driver[_run_test_id]}")
    attempt = 0
    while True:
        attempt += 1
        element_handle = await page.query_selector(locator)
        logging.info(f"[change_frame_by_locator] attempt={attempt}, element_handle={element_handle is not None}")
        if element_handle:
            tag = await element_handle.get_attribute("tagName") or await element_handle.evaluate("el => el.tagName")
            src = await element_handle.get_attribute("src")
            logging.info(f"[change_frame_by_locator] element tag={tag}, src={src!r}")
            try:
                frame = await asyncio.wait_for(
                    element_handle.content_frame(),
                    timeout=max(0.5, deadline - time.time()),
                )
            except asyncio.TimeoutError:
                logging.warning(f"[change_frame_by_locator] content_frame() timed out after attempt={attempt}")
                return "frame not found (content_frame timed out)"
            logging.info(f"[change_frame_by_locator] content_frame result: {frame}, frame_name={getattr(frame, 'name', None)}, frame_url={getattr(frame, 'url', None)}")
            if frame:
                driver[_run_test_id]['frame'] = frame
                logging.info(f"[change_frame_by_locator] SUCCESS - switched to frame url={frame.url}")
                return "frame_changed"
            else:
                logging.warning(f"[change_frame_by_locator] content_frame() returned None (iframe not loaded yet?)")
        remaining = deadline - time.time()
        logging.info(f"[change_frame_by_locator] remaining={remaining:.1f}s")
        if remaining <= 0:
            logging.warning(f"[change_frame_by_locator] TIMEOUT after {attempt} attempts")
            return "frame not found"
        await asyncio.sleep(0.3)

async def change_frame_to_original(_run_test_id='1') -> str:
    """
    Switches focus back to the main document.
    """
    global driver
    page = driver[_run_test_id]['page']
    driver[_run_test_id]['frame'] = page.main_frame
    return "frame_changed"

async def refresh_page(_run_test_id='1') -> str:
    """
    Refreshes the current page.
    """
    global driver
    page = driver[_run_test_id]['page']
    await page.reload()
    return "page refreshed"


async def wait_for_download(timeout: str = '30', _run_test_id='1') -> str:
    """
    Waits for a browser file download to complete. Call this AFTER clicking
    a download button or link. Returns the downloaded file name and size.

    Args:
        timeout: Maximum seconds to wait for the download to complete (default 30).
    """
    try:
        timeout_secs = float(timeout)
    except (TypeError, ValueError):
        timeout_secs = 30.0

    def _find_entry():
        """Check for a completed, failed, or pending download (most recent first)."""
        with file_system._download_lock:
            entries = file_system._pending_downloads.get(_run_test_id, [])
            for e in reversed(entries):
                if e["status"] == "complete":
                    return "complete", e
                if e["status"] == "failed":
                    return "failed", e
            for e in reversed(entries):
                if e["status"] == "pending":
                    return "pending", e
        return None, None

    # Check if a download already completed (fast path)
    status, entry = _find_entry()
    if status == "complete":
        return json.dumps({"status": "success", "file_name": entry["file_name"], "size_bytes": entry["size_bytes"]})
    if status == "failed":
        return json.dumps({"status": "error", "error": f"Download failed: {entry['error']}", "file_name": entry["file_name"]})

    # If no download event yet, wait briefly for it to fire
    # (small race between click() returning and the download event arriving)
    if status is None:
        await asyncio.sleep(0.5)
        status, entry = _find_entry()

    if status == "complete":
        return json.dumps({"status": "success", "file_name": entry["file_name"], "size_bytes": entry["size_bytes"]})
    if status == "failed":
        return json.dumps({"status": "error", "error": f"Download failed: {entry['error']}", "file_name": entry["file_name"]})
    if entry is None:
        return json.dumps({"status": "error", "error": "No download detected. Make sure you clicked a download link or button before calling wait_for_download."})

    # Attach an asyncio.Event so the completion handler can signal us
    event = asyncio.Event()
    with file_system._download_lock:
        entry["event"] = event

    # Double-check: status may have changed between our check and attaching the event
    if entry["status"] == "complete":
        return json.dumps({"status": "success", "file_name": entry["file_name"], "size_bytes": entry["size_bytes"]})
    if entry["status"] == "failed":
        return json.dumps({"status": "error", "error": f"Download failed: {entry['error']}", "file_name": entry["file_name"]})

    # Wait for the download to finish or timeout
    try:
        await asyncio.wait_for(event.wait(), timeout=timeout_secs)
    except asyncio.TimeoutError:
        return json.dumps({"status": "error", "error": f"Download timed out after {timeout_secs}s. The file may still be downloading.", "file_name": entry["file_name"]})

    if entry["status"] == "complete":
        return json.dumps({"status": "success", "file_name": entry["file_name"], "size_bytes": entry["size_bytes"]})
    return json.dumps({"status": "error", "error": f"Download failed: {entry.get('error', 'unknown')}", "file_name": entry["file_name"]})

async def wait_time(seconds: str, _run_test_id='1') -> str:
    """
    Waits for a specified number of seconds before proceeding.

    Args:
        seconds: The number of seconds to wait (can be a float, e.g. '2.5').
    """
    try:
        secs = float(seconds)
    except (TypeError, ValueError):
        secs = 1.0
    await asyncio.sleep(secs)
    return f"waited for {secs} seconds"

# ─── File Upload to Web Form (browser-specific) ─────────────────────────────

async def upload_file_to_form(locator: str, file_name: str, wait_for: str = '', timeout: int = 15000, _run_test_id='1') -> str:
    """
    Uploads an agent file to a web form's file input element using Playwright's set_input_files().
    Waits for the upload to actually complete before returning (network idle or custom selector).

    Args:
        locator: CSS/XPath selector for the file input element (must be input[type=file])
        file_name: name of the uploaded file to use
        wait_for: optional CSS/XPath selector to wait for after upload (e.g. a success message element).
                  If empty, waits for network idle instead.
        timeout: max milliseconds to wait for upload completion (default 15000 = 15s)
    """
    # Coerce timeout to int — remote calls may pass it as a string
    try:
        timeout = int(timeout)
    except (TypeError, ValueError):
        timeout = 15000

    try:
        safe_name = file_system.sanitize_filename(file_name)
    except ValueError as e:
        return f"error: {e}"

    attachments_dir = file_system.get_attachments_dir()
    file_path = (attachments_dir / safe_name).resolve()
    if not file_path.is_file():
        avail = [f.name for f in attachments_dir.resolve().iterdir() if f.is_file()] if attachments_dir.exists() else []
        return f"error: file '{safe_name}' not found at {file_path}. Available files: {avail}"

    global driver
    page = driver[_run_test_id]['page']
    abs_path = str(file_path)

    # set_input_files sets the file on the input element and dispatches
    # its own change/input events internally. Do NOT manually dispatch
    # additional change/input events — that causes upload widgets to
    # process the file twice, resulting in duplicate uploads.
    await page.set_input_files(locator, abs_path)

    # Wait for the upload to actually complete before returning.
    # This is critical for bulk execution where the next step runs immediately.
    wait_detail = ""
    try:
        if wait_for:
            # User specified a selector to wait for (e.g. success message, file list item)
            await page.wait_for_selector(wait_for, state="visible", timeout=timeout)
            wait_detail = f", waited for '{wait_for}' to appear"
        else:
            # Default: wait for network to go idle (covers HTTP upload completion)
            await page.wait_for_load_state("networkidle", timeout=timeout)
            wait_detail = ", waited for network idle"
    except Exception as e:
        # If wait times out, still report success for the file setting itself
        # but note that the upload may not have completed
        wait_detail = f", warning: wait timed out ({e}) - upload may still be in progress"

    return f"uploaded {safe_name} ({abs_path}) to {locator}{wait_detail}"


# ─── Eagerly warm up the OCR reader at agent startup ────────────────────────
# Loads the (heavy) EasyOCR model in the background so the first click_text_ocr /
# exists_text_ocr call is fast. Set OCR_WARMUP=false to skip (e.g. for tests).
if os.getenv("OCR_WARMUP", "true").lower() in ("1", "true", "yes"):
    _warmup_ocr_reader()