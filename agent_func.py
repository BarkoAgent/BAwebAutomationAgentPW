import re
import os
import streaming
from playwright.async_api import async_playwright

test_variables = {}
driver: dict[str, object] = {}
run_test_id = ""

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
    import logging
    import time
    time_now = time.time()
    logging.info(f"about to take screenshot screenshot_{_run_test_id}_{time_now}.png")
    await page.screenshot()
    time_after = time.time()
    logging.info(f"screenshot taken screenshot_{_run_test_id}_{time_now}.png in {time_after - time_now:.2f}s")
    driver[_run_test_id] = {'playwright': playwright, 'browser': browser, 'context': context, 'page': page}
    main_url = os.getenv("MAIN_URL", "https://beta.barkoagent.com")
    await streaming.start_stream(driver[_run_test_id], run_id="1", fps=1.0, jpeg_quality=70)
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
    Returns cleaned page HTML.
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