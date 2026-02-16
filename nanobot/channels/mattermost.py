"""Mattermost channel implementation using WebSocket and REST API."""

import asyncio
import json
import re
from pathlib import Path
from typing import Any

import httpx
import websockets
from loguru import logger

from nanobot.bus.events import OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.channels.base import BaseChannel
from nanobot.config.schema import MattermostConfig

MAX_UPLOAD_BYTES = 50 * 1024 * 1024  # 50MB Mattermost default


class MattermostChannel(BaseChannel):
    """
    Mattermost channel using WebSocket for events and REST API for sending.

    Uses httpx + websockets (already nanobot dependencies) for a fully async
    implementation, avoiding the sync-to-async bridging issues of mattermostdriver.
    """

    name = "mattermost"

    def __init__(self, config: MattermostConfig, bus: MessageBus):
        super().__init__(config, bus)
        self.config: MattermostConfig = config
        self._http: httpx.AsyncClient | None = None
        self._ws: websockets.WebSocketClientProtocol | None = None
        self._bot_user_id: str | None = None
        self._bot_username: str | None = None

    async def start(self) -> None:
        """Start the Mattermost WebSocket connection."""
        if not self.config.url or not self.config.token:
            logger.error("Mattermost URL and token must be configured")
            return

        base_url = self.config.url.rstrip("/")
        self._http = httpx.AsyncClient(
            base_url=f"{base_url}/api/v4",
            headers={"Authorization": f"Bearer {self.config.token}"},
            timeout=30.0,
        )

        # Authenticate and get bot user info
        try:
            resp = await self._http.get("/users/me")
            resp.raise_for_status()
            me = resp.json()
            self._bot_user_id = me["id"]
            self._bot_username = me.get("username", "")
            logger.info(f"Mattermost bot connected as @{self._bot_username}")
        except Exception as e:
            logger.error(f"Mattermost auth failed: {e}")
            await self._http.aclose()
            self._http = None
            return

        # Build WebSocket URL
        ws_scheme = "wss" if base_url.startswith("https") else "ws"
        # Strip http(s):// to get the host portion
        host = re.sub(r"^https?://", "", base_url)
        ws_url = f"{ws_scheme}://{host}/api/v4/websocket"

        self._running = True

        while self._running:
            try:
                logger.info(f"Connecting to Mattermost WebSocket at {ws_url}...")
                async with websockets.connect(ws_url) as ws:
                    self._ws = ws
                    # Authenticate the WebSocket connection
                    await ws.send(json.dumps({
                        "seq": 1,
                        "action": "authentication_challenge",
                        "data": {"token": self.config.token},
                    }))
                    await self._ws_loop()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"Mattermost WebSocket error: {e}")
                if self._running:
                    logger.info("Reconnecting to Mattermost in 5 seconds...")
                    await asyncio.sleep(5)

    async def stop(self) -> None:
        """Stop the Mattermost channel."""
        self._running = False
        if self._ws:
            await self._ws.close()
            self._ws = None
        if self._http:
            await self._http.aclose()
            self._http = None

    async def send(self, msg: OutboundMessage) -> None:
        """Send a message through Mattermost REST API, with file uploads."""
        if not self._http:
            logger.warning("Mattermost HTTP client not initialized")
            return

        mm_meta = msg.metadata.get("mattermost", {}) if msg.metadata else {}
        root_id = mm_meta.get("root_id")

        # Collect files to upload: explicit media paths + workspace paths found in text
        file_paths = self._collect_file_paths(msg)
        file_ids: list[str] = []

        for fp in file_paths:
            file_id = await self._upload_file(msg.chat_id, fp)
            if file_id:
                file_ids.append(file_id)

        payload: dict[str, Any] = {
            "channel_id": msg.chat_id,
            "message": msg.content,
        }
        if root_id:
            payload["root_id"] = root_id
        if file_ids:
            payload["file_ids"] = file_ids

        try:
            resp = await self._http.post("/posts", json=payload)
            resp.raise_for_status()
        except Exception as e:
            logger.error(f"Error sending Mattermost message: {e}")

    def _collect_file_paths(self, msg: OutboundMessage) -> list[Path]:
        """Find uploadable files from media list and workspace paths in message text."""
        paths: list[Path] = []
        seen: set[str] = set()

        # Explicit media paths from OutboundMessage.media
        for p in msg.media or []:
            resolved = Path(p).expanduser().resolve()
            if resolved.is_file() and str(resolved) not in seen:
                seen.add(str(resolved))
                paths.append(resolved)

        # Scan message text for absolute paths that exist as files in the workspace
        workspace = Path(self.config.workspace).expanduser().resolve() if self.config.workspace else None
        if workspace:
            for match in re.finditer(r'(?:^|\s|`)((?:/[\w.\-]+)+)', msg.content):
                candidate = Path(match.group(1)).expanduser().resolve()
                if (
                    candidate.is_file()
                    and str(candidate).startswith(str(workspace))
                    and candidate.stat().st_size <= MAX_UPLOAD_BYTES
                    and str(candidate) not in seen
                ):
                    seen.add(str(candidate))
                    paths.append(candidate)

        return paths

    async def _upload_file(self, channel_id: str, file_path: Path) -> str | None:
        """Upload a file to Mattermost. Returns file_id on success."""
        if not self._http:
            return None
        try:
            content = file_path.read_bytes()
            resp = await self._http.post(
                "/files",
                data={"channel_id": channel_id},
                files={"files": (file_path.name, content)},
            )
            resp.raise_for_status()
            file_infos = resp.json().get("file_infos", [])
            if file_infos:
                file_id = file_infos[0]["id"]
                logger.info(f"Uploaded {file_path.name} to Mattermost ({file_id})")
                return file_id
        except Exception as e:
            logger.error(f"Failed to upload {file_path.name}: {e}")
        return None

    async def _ws_loop(self) -> None:
        """Main WebSocket event loop."""
        if not self._ws:
            return

        async for raw in self._ws:
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                continue

            event = data.get("event")
            if event == "posted":
                await self._handle_posted(data)

    async def _handle_posted(self, data: dict[str, Any]) -> None:
        """Handle a 'posted' WebSocket event."""
        raw_post = data.get("data", {}).get("post")
        if not raw_post:
            return

        post = json.loads(raw_post) if isinstance(raw_post, str) else raw_post

        user_id = post.get("user_id", "")
        channel_id = post.get("channel_id", "")
        message = post.get("message", "")
        root_id = post.get("root_id") or post.get("id", "")

        # Ignore own messages
        if user_id == self._bot_user_id:
            return

        if not user_id or not channel_id:
            return

        if not self.is_allowed(user_id):
            logger.debug(f"Ignoring message from unauthorized Mattermost user: {user_id}")
            return

        # Determine if this is a DM or group channel
        channel_type = data.get("data", {}).get("channel_type", "")

        # In group channels, only respond if mentioned (unless group_policy is open)
        if channel_type in ("O", "P", "G"):  # Open, Private, Group
            if self.config.group_policy == "mention":
                if not self._is_bot_mentioned(message):
                    return
                message = self._strip_bot_mention(message)

        # Handle file attachments
        file_ids = post.get("file_ids") or []
        media: list[str] = []
        if file_ids and self._http:
            for file_id in file_ids:
                try:
                    resp = await self._http.get(f"/files/{file_id}/info")
                    resp.raise_for_status()
                    file_info = resp.json()
                    name = file_info.get("name", file_id)
                    media.append(f"[file: {name}]")
                except Exception as e:
                    logger.debug(f"Failed to get Mattermost file info: {e}")

        await self._handle_message(
            sender_id=user_id,
            chat_id=channel_id,
            content=message or "[empty message]",
            media=media,
            metadata={
                "mattermost": {
                    "post_id": post.get("id", ""),
                    "root_id": root_id,
                    "channel_type": channel_type,
                    "file_ids": file_ids,
                }
            },
        )

    def _is_bot_mentioned(self, text: str) -> bool:
        """Check if the bot is mentioned in the message."""
        if not self._bot_username:
            return False
        return f"@{self._bot_username}" in text

    def _strip_bot_mention(self, text: str) -> str:
        """Remove bot @mention from message text."""
        if not text or not self._bot_username:
            return text
        return re.sub(rf"@{re.escape(self._bot_username)}\s*", "", text).strip()
