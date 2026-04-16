import re
import os
import json
import time
import asyncio
import logging

import ba_ws_sdk.streaming as streaming
import ba_ws_sdk.file_system as file_system
from dotenv import load_dotenv
from playwright.async_api import async_playwright
load_dotenv()

DEFAULT_TIMEOUT = int(os.getenv("DEFAULT_TIMEOUT", "10"))  # seconds; Playwright expects ms

test_variables = {}
driver: dict[str, object] = {}
run_test_id = ""


def clean_html(html_content):
    for tag in ['script', 'style', 'svg']:
        html_content = re.sub(rf'<{tag}[^>]*>.*?</{tag}>', '', html_content, flags=re.DOTALL)
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

async def create_driver(_run_test_id='1'):
    """
    Creates a Playwright browser context and initializes test_variables for this run id.
    """
    global driver, test_variables
    test_variables[_run_test_id] = {}
    playwright = await async_playwright().start()
    browser = await playwright.chromium.launch(headless=True)
    context = await browser.new_context(
        viewport={'width': 800, 'height': 800},
        accept_downloads=True,
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

    main_url = os.getenv("MAIN_URL", "https://beta.barkoagent.com")
    await streaming.astart_stream(driver[_run_test_id], run_id=_run_test_id, fps=0.5, jpeg_quality=70)
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
    await page.wait_for_selector(locator, state="visible", timeout=DEFAULT_TIMEOUT * 1000)
    await page.fill(locator, value)
    return "sent keys"

async def exists(locator: str, _run_test_id='1') -> str:
    """
    Waits until element is visible (exists).
    """
    global driver
    page = driver[_run_test_id].get('frame') or driver[_run_test_id]['page']
    await page.wait_for_selector(locator, state="visible", timeout=DEFAULT_TIMEOUT * 1000)
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
    await page.wait_for_selector(locator, timeout=DEFAULT_TIMEOUT * 1000)
    return "exists (text)"

async def does_not_exist(locator: str, _run_test_id='1') -> str:
    """
    Waits until element does NOT exist.
    """
    global driver
    page = driver[_run_test_id]['page']
    await page.wait_for_selector(locator, state='detached', timeout=DEFAULT_TIMEOUT * 1000)
    return "doesn't exists"

async def scroll_to_element(locator: str, _run_test_id='1') -> str:
    """
    Scrolls until the element is visible in the viewport.
    """
    global driver
    page = driver[_run_test_id].get('frame') or driver[_run_test_id]['page']
    await page.wait_for_selector(locator, timeout=DEFAULT_TIMEOUT * 1000)
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
    await page.wait_for_selector(locator, state="visible", timeout=DEFAULT_TIMEOUT * 1000)
    await page.click(locator)
    return "clicked successfully on the element"

async def double_click(locator: str, _run_test_id='1') -> str:
    """
    Double clicks on element.
    """
    global driver
    page = driver[_run_test_id].get('frame') or driver[_run_test_id]['page']
    await page.wait_for_selector(locator, state="visible", timeout=DEFAULT_TIMEOUT * 1000)
    await page.dblclick(locator)
    return "double clicked"

async def right_click(locator: str, _run_test_id='1') -> str:
    """
    Right clicks on element.
    """
    global driver
    page = driver[_run_test_id].get('frame') or driver[_run_test_id]['page']
    await page.wait_for_selector(locator, state="visible", timeout=DEFAULT_TIMEOUT * 1000)
    await page.click(locator, button='right')
    return "right clicked"

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
    await page.wait_for_selector(locator, state="visible", timeout=DEFAULT_TIMEOUT * 1000)

    if by == "value":
        await page.select_option(locator, value=option)
    elif by == "index":
        await page.select_option(locator, index=int(option))
    else:
        await page.select_option(locator, label=option)

    return "selected"

async def get_page_html(_run_test_id='1') -> str:
    """
    Returns cleaned page HTML. Recommended to use before doing any actions on the page to confirm the existance of elements.
    """
    global driver
    page = driver[_run_test_id]['page']
    try:
        await page.wait_for_load_state('domcontentloaded', timeout=20000)
        content = await page.content()
    except Exception:
        await page.wait_for_load_state('load', timeout=30000)
        content = await page.content()
    html_content = clean_html(content)
    return html_content

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
    if len(pages) > 1:
        page = pages[-1]
        driver[_run_test_id]['page'] = page
        content = await page.content()
        html_content = clean_html(content)
        return html_content
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
    page = driver[_run_test_id]['page']
    element_handle = await page.query_selector(locator)
    if element_handle:
        frame = await element_handle.content_frame()
        if frame:
            driver[_run_test_id]['frame'] = frame
            return "frame_changed"
    return "frame not found"

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