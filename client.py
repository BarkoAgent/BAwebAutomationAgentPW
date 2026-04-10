#!/usr/bin/env python3
import asyncio
import logging
import os
import subprocess
import sys
from dotenv import load_dotenv
import agent_func
from ba_ws_sdk import main_connect_ws

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s'
)


def normalize_ws_env():
    """
    Make local websocket configuration work with the SDK's expected env names.
    """
    backend_ws_base = os.getenv("BACKEND_WS_BASE")

    if backend_ws_base:
        os.environ["DEFAULT_WS_BASE"] = backend_ws_base


def install_playwright_runtime():
    """
    Install Playwright browsers using the same interpreter running this process.
    """
    commands = [
        [sys.executable, "-m", "playwright", "install"],
        [sys.executable, "-m", "playwright", "install-deps"],
    ]

    for command in commands:
        try:
            subprocess.run(command, check=True)
        except FileNotFoundError:
            logging.exception("Unable to run Playwright setup command: %s", " ".join(command))
        except subprocess.CalledProcessError:
            logging.exception("Playwright setup command failed: %s", " ".join(command))


async def main():
    """
    Entry point: initializes WebSocket connection and handles optional streaming.
    Actual behavior depends on environment variables:

    - AGENT_CONNECTION_TYPE:
        'manager' -> multiplexed single socket (for Agent Manager)
        'direct'  -> dual sockets (direct-to-app)
    - ENABLE_STREAMING:
        'true'/'1' to enable frame streaming
    """
    normalize_ws_env()

    backend_ws_uri = os.getenv("BACKEND_WS_URI")
    if not backend_ws_uri:
        logging.error("BACKEND_WS_URI not set. Cannot start backend connection.")
        sys.exit(1)

    connection_type = os.getenv("AGENT_CONNECTION_TYPE", "manager").lower()
    enable_streaming = os.getenv("ENABLE_STREAMING", "true").lower() in ("1", "true", "yes")

    logging.info(f"Starting agent with connection type: {connection_type.upper()}")
    if enable_streaming:
        logging.info("Streaming is enabled via environment settings.")
    else:
        logging.info("Streaming is disabled.")

    install_playwright_runtime()

    try:
        await main_connect_ws(agent_func)
    except Exception as e:
        logging.exception(f"Agent encountered an error: {e}")
    finally:
        logging.info("Closing drivers...")
        await agent_func.stop_all_drivers()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Client stopped manually.")
