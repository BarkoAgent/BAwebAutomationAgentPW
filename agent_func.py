import re
import os
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import ba_ws_sdk.streaming as streaming
from playwright.async_api import async_playwright

test_variables = {}
driver: dict[str, object] = {}
run_test_id = ""

# ─── Attachments (flat directory: ./attachments/) ────────────────────────────
ATTACHMENTS_DIR = Path(__file__).resolve().parent / "attachments"

_attachments_cache = None  # list of {name, size_bytes, modified_iso} or None


def sanitize_filename(name: str) -> str:
    """
    Strip dangerous characters and path components from a filename.
    Returns a safe filename or raises ValueError if result is empty.
    """
    name = name.replace("..", "").replace("/", "").replace("\\", "")
    name = re.sub(r"[^a-zA-Z0-9\-_. ]", "", name)
    name = re.sub(r"\s+", " ", name).strip()
    if len(name) > 255:
        name = name[:255]
    name = name.lstrip(".")
    if not name:
        raise ValueError("Filename is empty after sanitization")
    return name


def _scan_attachments() -> list[dict]:
    """Scan ./attachments/ and return file metadata."""
    if not ATTACHMENTS_DIR.is_dir():
        return []
    files = []
    for entry in sorted(ATTACHMENTS_DIR.iterdir()):
        if entry.is_file() and not entry.name.startswith(".tmp_"):
            stat = entry.stat()
            files.append({
                "name": entry.name,
                "size_bytes": stat.st_size,
                "modified_iso": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
            })
    return files


def _invalidate_cache():
    """Remove cached metadata so next list call rescans."""
    global _attachments_cache
    _attachments_cache = None


def _get_attachments_metadata() -> list[dict]:
    """Get cached metadata or scan disk."""
    global _attachments_cache
    if _attachments_cache is None:
        _attachments_cache = _scan_attachments()
    return _attachments_cache


def _migrate_attachments_flat():
    """One-time migration: move files from ./attachments/{subdir}/ to ./attachments/ flat."""
    if not ATTACHMENTS_DIR.is_dir():
        return
    for entry in list(ATTACHMENTS_DIR.iterdir()):
        if entry.is_dir() and not entry.name.startswith("."):
            for f in entry.iterdir():
                if f.is_file():
                    dest = ATTACHMENTS_DIR / f.name
                    if not dest.exists():
                        f.rename(dest)
                        logging.info(f"[Migration] Moved {f} -> {dest}")
                    else:
                        logging.warning(f"[Migration] Skipped {f} (already exists at {dest})")
            # Remove empty subdir
            try:
                entry.rmdir()
            except OSError:
                pass

# Run migration on import
_migrate_attachments_flat()


def save_uploaded_file(file_name: str, file_bytes: bytes):
    """
    Save raw file bytes to ./attachments/{file_name}.
    Called by the WS binary envelope handler (not an agent function).
    """
    safe_name = sanitize_filename(file_name)
    ATTACHMENTS_DIR.mkdir(parents=True, exist_ok=True)
    dest = ATTACHMENTS_DIR / safe_name
    dest.write_bytes(file_bytes)
    _invalidate_cache()
    logging.info(f"[AgentFiles] Saved {len(file_bytes)} bytes -> {dest}")
    return safe_name

def clean_html(html_content):
    for tag in ['script', 'style', 'svg']:
        html_content = re.sub(rf'<{tag}[^>]*>.*?</{tag}>', '', html_content, flags=re.DOTALL)
    return html_content

async def stop_all_drivers():
    global driver
    for run_id, context in list(driver.items()):
        try:
            await context.close()
            print(f"✅ Driver '{run_id}' stopped.")
        except Exception as e:
            print(f"⚠️ Error stopping driver '{run_id}': {e}")
    driver.clear()
    print("🗑️ All drivers stopped and entries cleared.")

async def create_driver(_run_test_id='1'):
    """
    Creates a Playwright browser context and initializes test_variables for this run id.
    """
    global driver, test_variables
    test_variables[_run_test_id] = {}
    playwright = await async_playwright().start()
    browser = await playwright.chromium.launch(headless=True)
    context = await browser.new_context(
        viewport={'width': 800, 'height': 800}
    )
    page = await context.new_page()
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
    page = driver[_run_test_id]['page']
    if use_vars == 'true' and _run_test_id in test_variables:
        value = test_variables[_run_test_id].get(value, value)
    await page.fill(locator, value)
    return "sent keys"

async def exists(locator: str, _run_test_id='1') -> str:
    """
    Waits until element is visible (exists).
    """
    global driver
    page = driver[_run_test_id]['page']
    await page.wait_for_selector(locator, timeout=10000)
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
    await page.wait_for_selector(locator, timeout=10000)
    return "exists (text)"

async def does_not_exist(locator: str, _run_test_id='1') -> str:
    """
    Waits until element does NOT exist.
    """
    global driver
    page = driver[_run_test_id]['page']
    await page.wait_for_selector(locator, state='detached', timeout=10000)
    return "doesn't exists"

async def scroll_to_element(locator: str, _run_test_id='1') -> str:
    """
    Scrolls until the element is visible in the viewport.
    """
    global driver
    page = driver[_run_test_id]['page']
    await page.wait_for_selector(locator, timeout=1000)
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
    page = driver[_run_test_id]['page']
    await page.wait_for_selector(locator, timeout=1000)
    await page.click(locator)
    return "clicked successfully on the element"

async def double_click(locator: str, _run_test_id='1') -> str:
    """
    Double clicks on element.
    """
    global driver
    page = driver[_run_test_id]['page']
    await page.wait_for_selector(locator, timeout=1000)
    await page.dblclick(locator)
    return "double clicked"

async def right_click(locator: str, _run_test_id='1') -> str:
    """
    Right clicks on element.
    """
    global driver
    page = driver[_run_test_id]['page']
    await page.wait_for_selector(locator, timeout=1000)
    await page.click(locator, button='right')
    return "right clicked"

async def get_page_html(_run_test_id='1') -> str:
    """
    Returns cleaned page HTML. Recommended to use before doing any actions on the page to confirm the existance of elements.
    """
    global driver
    page = driver[_run_test_id]['page']
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


# ─── File Management Functions ──────────────────────────────────────────────

MAX_READ_BYTES = 1 * 1024 * 1024  # 1MB max per read
DEFAULT_READ_BYTES = 64 * 1024     # 64KB default


async def list_agent_files(_run_test_id='1') -> str:
    """
    Returns a JSON array of files uploaded to the agent.
    Each entry has: name, size_bytes, modified_iso.
    """
    files = _get_attachments_metadata()
    return json.dumps(files)


async def delete_agent_file(file_name: str, _run_test_id='1') -> str:
    """
    Deletes an uploaded file by name.
    """
    try:
        safe_name = sanitize_filename(file_name)
    except ValueError as e:
        return json.dumps({"status": "error", "error": str(e)})

    target = ATTACHMENTS_DIR / safe_name
    if not target.is_file():
        return json.dumps({"status": "error", "error": "file not found"})

    target.unlink()
    _invalidate_cache()
    return json.dumps({"status": "success", "deleted": safe_name})


async def read_agent_file(
    file_name: str,
    offset: str = '0',
    length: str = '',
    as_text: str = 'true',
    _run_test_id='1',
) -> str:
    """
    Reads an uploaded file by name. Supports partial reads via offset/length.
    Defaults to first 64KB if length is not specified. Max single read is 1MB.

    Args:
        file_name: name of the file to read
        offset: byte offset to start reading from (default 0)
        length: number of bytes to read (default 64KB, max 1MB)
        as_text: 'true' to decode as UTF-8, 'false' to return base64
    """
    try:
        safe_name = sanitize_filename(file_name)
    except ValueError as e:
        return json.dumps({"status": "error", "error": str(e)})

    target = ATTACHMENTS_DIR / safe_name
    if not target.is_file():
        return json.dumps({"status": "error", "error": "file not found"})

    total_size = target.stat().st_size
    byte_offset = int(offset)
    byte_length = int(length) if length else DEFAULT_READ_BYTES
    byte_length = min(byte_length, MAX_READ_BYTES)
    byte_offset = max(0, min(byte_offset, total_size))

    with open(target, "rb") as f:
        f.seek(byte_offset)
        data = f.read(byte_length)

    truncated = (byte_offset + len(data)) < total_size

    if as_text.lower() == 'true':
        try:
            content = data.decode("utf-8")
        except UnicodeDecodeError:
            import base64
            content = base64.b64encode(data).decode("ascii")
    else:
        import base64
        content = base64.b64encode(data).decode("ascii")

    return json.dumps({
        "content": content,
        "total_size": total_size,
        "offset": byte_offset,
        "length": len(data),
        "truncated": truncated,
    })


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
        safe_name = sanitize_filename(file_name)
    except ValueError as e:
        return f"error: {e}"

    file_path = (ATTACHMENTS_DIR / safe_name).resolve()
    if not file_path.is_file():
        avail = [f.name for f in ATTACHMENTS_DIR.resolve().iterdir() if f.is_file()] if ATTACHMENTS_DIR.exists() else []
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