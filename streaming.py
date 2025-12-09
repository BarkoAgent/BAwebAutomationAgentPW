# streaming_async.py
import asyncio
import time
import logging
from typing import Optional, Dict, Tuple
import numpy as np
import cv2

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')

# Globals (async context)
_STREAM_TASKS: Dict[str, asyncio.Task] = {}            # run_id -> asyncio.Task
_LATEST_FRAMES: Dict[str, Tuple[bytes, float]] = {}    # run_id -> (jpeg_bytes, timestamp)
_LOCK = asyncio.Lock()                                 # optional: guard concurrent dict access

def _png_to_jpeg_bytes(png_bytes: bytes, quality: int = 80) -> bytes:
    """
    Convert PNG bytes (Selenium/playwright/etc screenshot) to JPEG bytes.
    """
    nparr = np.frombuffer(png_bytes, dtype=np.uint8)
    img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
    if img is None:
        raise ValueError("Failed to decode image bytes")
    ok, enc = cv2.imencode('.jpg', img, [int(cv2.IMWRITE_JPEG_QUALITY), quality])
    if not ok:
        raise RuntimeError("Failed to encode JPEG")
    return enc.tobytes()

async def _stream_worker(
    run_id: str,
    driver,
    fps: float,
    jpeg_quality: int,
    stop_after: Optional[float] = None,
):
    """
    Async worker that captures screenshots from `driver` periodically.
    Expects `driver['page'].screenshot()` to be awaitable.
    Cancels itself when task is cancelled or stop_after exceeded.
    """
    interval = 1.0 / max(0.1, fps)
    logging.info(f"Stream worker started for run_id={run_id} fps={fps}")
    started_at = time.time()
    try:
        while True:
            # stop after timeout if requested
            if stop_after is not None and (time.time() - started_at) >= stop_after:
                logging.info(f"[{run_id}] stop_after reached ({stop_after}s). Exiting worker.")
                return

            # allow cancellation
            await asyncio.sleep(0)  # cooperative cancellation point

            screenshot_timeout = max(5.0, interval * 2)
            try:
                # Await screenshot with a timeout
                png_bytes = await asyncio.wait_for(driver['page'].screenshot(), timeout=screenshot_timeout)
            except asyncio.TimeoutError:
                logging.warning(f"[{run_id}] screenshot timed out after {screenshot_timeout}s")
                # avoid tight loop
                await asyncio.sleep(interval)
                continue
            except asyncio.CancelledError:
                # propagate cancellation cleanly
                logging.info(f"[{run_id}] worker cancelled during screenshot.")
                raise
            except Exception:
                logging.exception(f"[{run_id}] Failed to capture screenshot; stopping worker.")
                return

            # convert to jpeg (with fallback to original bytes on failure)
            try:
                jpeg_bytes = _png_to_jpeg_bytes(png_bytes, quality=jpeg_quality)
            except Exception:
                logging.exception(f"[{run_id}] PNG->JPEG conversion failed; storing PNG as fallback")
                jpeg_bytes = png_bytes

            # store latest frame (use lock if concurrent access expected)
            if _LOCK.locked():
                # unlikely but keep defensive pattern (acquire re-entrantly not possible),
                # so we do a simple try / finally with acquire to be consistent
                pass

            async with _LOCK:
                _LATEST_FRAMES[run_id] = (jpeg_bytes, time.time())

            # sleep for interval (cooperative; allows cancellation)
            try:
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                logging.info(f"[{run_id}] worker cancelled during sleep.")
                raise

    except asyncio.CancelledError:
        logging.info(f"[{run_id}] worker received cancellation.")
        raise
    except Exception:
        logging.exception(f"[{run_id}] Unexpected exception in stream worker for run_id={run_id}")
    finally:
        logging.info(f"Stream worker exiting for run_id={run_id}")

async def start_stream(
    driver,
    run_id: str = "1",
    fps: float = 1.0,
    jpeg_quality: int = 70,
    stop_after: Optional[float] = 180.0,
) -> None:
    """
    Start an async streaming task for `run_id`. No-op if already running.
    If an existing task is found and is still running, returns without starting a new one.
    """
    async with _LOCK:
        existing = _STREAM_TASKS.get(run_id)
        if existing and not existing.done():
            logging.info(f"Stream already running for run_id={run_id}; start_stream no-op.")
            return

        logging.info(f"Starting stream task for run_id={run_id} fps={fps} quality={jpeg_quality}")
        task = asyncio.create_task(_stream_worker(run_id, driver, fps, jpeg_quality, stop_after), name=f"stream-{run_id}")
        _STREAM_TASKS[run_id] = task

        # optional: attach done callback to clean up dicts when task finishes
        def _on_done(t: asyncio.Task, rid=run_id):
            logging.info(f"Stream task done for run_id={rid}. Cleaning up.")
            # schedule cleanup in loop
            async def _cleanup():
                async with _LOCK:
                    _STREAM_TASKS.pop(rid, None)
                    _LATEST_FRAMES.pop(rid, None)
            try:
                asyncio.create_task(_cleanup())
            except Exception:
                # fallback synchronous cleanup (best effort)
                try:
                    # this is safe: we are in callback executed in loop
                    asyncio.get_event_loop().create_task(_cleanup())
                except Exception:
                    logging.exception("Failed to schedule cleanup task")

        task.add_done_callback(_on_done)

async def stop_stream(run_id: str, cancel_timeout: float = 2.0) -> None:
    """
    Stop the stream task for `run_id` and cleanup.
    Attempts graceful cancellation and waits up to `cancel_timeout` seconds.
    """
    async with _LOCK:
        task = _STREAM_TASKS.get(run_id)

    if not task:
        logging.info(f"No active stream task for run_id={run_id}")
        # ensure any leftover frames removed
        async with _LOCK:
            _LATEST_FRAMES.pop(run_id, None)
            _STREAM_TASKS.pop(run_id, None)
        return

    if task.done():
        logging.info(f"Stream task already done for run_id={run_id}")
        async with _LOCK:
            _STREAM_TASKS.pop(run_id, None)
            _LATEST_FRAMES.pop(run_id, None)
        return

    logging.info(f"Stopping stream task for run_id={run_id}")
    task.cancel()
    try:
        await asyncio.wait_for(asyncio.shield(task), timeout=cancel_timeout)
    except asyncio.TimeoutError:
        logging.warning(f"Timeout while waiting for task cancellation for run_id={run_id}")
    except Exception:
        logging.exception(f"Exception while stopping task for run_id={run_id}")
    finally:
        async with _LOCK:
            _STREAM_TASKS.pop(run_id, None)
            _LATEST_FRAMES.pop(run_id, None)
        logging.info(f"Stopped stream for run_id={run_id}")

async def get_latest_frame(run_id: str) -> Optional[bytes]:
    """
    Return and remove the latest frame bytes for run_id (JPEG bytes preferred), or None.
    This is async to avoid accidental blocking; it acquires the async lock briefly.
    """
    async with _LOCK:
        item = _LATEST_FRAMES.pop(run_id, None)
    if item is None:
        return None
    return item[0]