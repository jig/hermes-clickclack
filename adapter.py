"""ClickClack gateway adapter — REST API + realtime WebSocket via aiohttp (no SDK).

ClickClack (https://github.com/openclaw/clickclack) is a self-hosted chat
server. A bot authenticates with a workspace-scoped ``ccb_...`` token; the
workspace is derived from the token.

Chat IDs are ClickClack's own IDs: ``chn_...`` for channels, ``dm_...`` for
direct conversations. Thread replies carry the root message ID as thread_id.

Environment variables:
    CLICKCLACK_URL                  Server URL (e.g. http://127.0.0.1:8080)
    CLICKCLACK_TOKEN                Bot token (ccb_...)
    CLICKCLACK_ALLOWED_USERS        Comma-separated user IDs (usr_...)
    CLICKCLACK_HOME_CHANNEL         Channel or DM ID for cron/notification delivery
    CLICKCLACK_REPLY_MODE           "thread" or "off" (default) for top-level channel posts
    CLICKCLACK_TOOL_PROGRESS        "fold" (default), "off" or "plain" for tool-progress bubbles
    CLICKCLACK_REQUIRE_MENTION      Require @mention in channels (default true)
    CLICKCLACK_FREE_RESPONSE_CHANNELS / CLICKCLACK_ALLOWED_CHANNELS
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import mimetypes
import os
import random
import re
import uuid
from pathlib import Path
from typing import Any, Optional
from urllib.parse import quote

from agent.i18n import t
from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import BasePlatformAdapter, SendResult, gateway_trust_env
from gateway.platforms.event import MessageEvent, MessageType
from gateway.platforms.helpers import MessageDeduplicator, cancel_task
from gateway.platforms._shared import (
    apply_yaml_bridge as _apply_yaml_bridge,
    env_is_connected as _env_is_connected,
    extra_or_secret as _extra_or_secret,
    get_scoped_secret as _get_scoped_secret,
    seed_extra_from_env as _seed_extra_from_env,
    send_error,
)

logger = logging.getLogger(__name__)

PLATFORM_NAME = "clickclack"

# The server accepts long Markdown bodies; 4000 keeps messages readable.
MAX_MESSAGE_LENGTH = 4000

_RECONNECT_BASE_DELAY, _RECONNECT_MAX_DELAY, _RECONNECT_JITTER = 2.0, 60.0, 0.2
_WS_CLOSE_RESYNC = 4001  # replay budget exhausted: drop the cursor and restart from the tail
_WS_CLOSE_POLICY = 1008  # credential revoked
_INBOUND_EVENTS = ("message.created", "thread.reply_created")
_MEDIA_MSG_TYPES = (("image/", MessageType.PHOTO), ("audio/", MessageType.VOICE))
_INBOUND_CACHE_EXT = {"image/": ".png", "audio/": ".ogg"}
_MENU_MAX_COMMANDS, _MENU_DESC_LIMIT = 100, 100
# Glyphs that open a Hermes tool-progress line besides the per-tool registry emojis.
_PROGRESS_EXTRA_GLYPHS = frozenset({"⚡", "💭", "🧠", "⏳", "⚙️", "🔧"})
# Keycap reactions offered as one-click clarify answers; choices past the ninth are typed.
_CLARIFY_EMOJIS = tuple(f"{n}\ufe0f\u20e3" for n in range(1, 10))
_CLARIFY_HINT = "React with the number, or reply with the number or the option text."
_CLARIFY_CARDS_MAX = 64

_Metadata = Optional[dict[str, Any]]


def _id_set(raw: Any) -> set[str]:
    items = raw if isinstance(raw, list) else str(raw or "").split(",")
    return {str(c).strip() for c in items if str(c).strip()}


def _is_dm(chat_id: str) -> bool:
    return chat_id.startswith("dm_")


def _url_and_token(config) -> tuple[str, str]:
    """(server URL, token): ``config`` first, CLICKCLACK_URL / CLICKCLACK_TOKEN env fallback."""
    extra = getattr(config, "extra", {}) or {}
    url = extra.get("url") or _get_scoped_secret("CLICKCLACK_URL", "")
    token = extra.get("token") or getattr(config, "token", None) or _get_scoped_secret("CLICKCLACK_TOKEN", "")
    return str(url).rstrip("/"), str(token).strip()


def _create_path(chat_id: str, thread_root: Optional[str]) -> str:
    """REST path that creates a message in a channel, a DM, or a thread under ``thread_root``."""
    if thread_root:
        return f"messages/{thread_root}/thread/replies"
    if _is_dm(chat_id):
        return f"dms/{chat_id}/messages"
    return f"channels/{chat_id}/messages"


def _message_id(data: dict[str, Any]) -> Optional[str]:
    msg = data.get("message") if isinstance(data, dict) else None
    return msg.get("id") if isinstance(msg, dict) else None


def _progress_glyphs() -> frozenset:
    """Emojis that start Hermes tool-progress lines: every registered tool's emoji plus fallbacks."""
    try:
        from tools.registry import registry
        names = registry.get_all_tool_names()
        return frozenset(registry.get_emoji(n, default="⚡") for n in names) | _PROGRESS_EXTRA_GLYPHS
    except Exception:
        return _PROGRESS_EXTRA_GLYPHS


def _is_tool_progress(content: str, glyphs: frozenset) -> bool:
    """True when every line looks like ``<tool emoji> <label>``, i.e. a gateway tool-progress bubble.

    The gateway sends progress through plain send()/edit_message() with no marker, so this is a
    content heuristic; a misclassified answer is still visible, only folded into the activity row."""
    lines = [ln for ln in content.splitlines() if ln.strip()]
    if not lines:
        return False
    for ln in lines:
        head, sep, _ = ln.strip().partition(" ")
        if not sep or head not in glyphs:
            return False
    return True


def check_clickclack_requirements() -> bool:
    try:
        import aiohttp  # noqa: F401
        return True
    except ImportError:
        logger.warning("ClickClack: aiohttp not installed")
        return False


def validate_clickclack_config(config: PlatformConfig) -> bool:
    url, token = _url_and_token(config)
    if not token:
        logger.debug("ClickClack: CLICKCLACK_TOKEN not set")
        return False
    if not url:
        logger.warning("ClickClack: CLICKCLACK_URL not set")
        return False
    return True


class ClickClackAdapter(BasePlatformAdapter):
    """Gateway adapter for a self-hosted ClickClack server."""

    splits_long_messages = True

    def __init__(self, config: PlatformConfig):
        super().__init__(config, Platform(PLATFORM_NAME))
        self._base_url, self._token = _url_and_token(config)
        self._bot_user_id = self._bot_handle = ""
        self._workspace_id = ""
        self._session: Any = None
        self._ws: Any = None
        self._ws_task: Optional[asyncio.Task] = None
        self._menu_task: Optional[asyncio.Task] = None
        self._closing = False
        # Last processed durable-event cursor; kept across reconnects within this process so gaps
        # are replayed, but not across restarts (a fresh start begins at the tail, no history replay).
        self._cursor: Optional[str] = None
        self._reply_mode = str(_extra_or_secret(config.extra, "reply_mode", "CLICKCLACK_REPLY_MODE", "off")).lower()
        # Tool-progress bubbles: "fold" posts them as agent_tool activity (collapsed under the answer's
        # turn in the web app), "off" drops them, "plain" sends them as ordinary messages.
        self._tool_progress = str(_extra_or_secret(
            config.extra, "tool_progress", "CLICKCLACK_TOOL_PROGRESS", "fold")).lower()
        self._progress_glyph_set: Optional[frozenset] = None
        self._turn_ids: dict[str, str] = {}  # chat_id → turn_id of the latest inbound message
        self._dedup = MessageDeduplicator()
        # Clarify prompts answerable by reaction: message_id → card (see send_clarify).
        self._clarify_cards: dict[str, dict[str, Any]] = {}
        self._reactions_warned = False

    # --- HTTP helpers ---

    def _auth_header(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._token}"}

    async def _api(self, method: str, path: str, payload: Optional[dict[str, Any]] = None,
                   params: Optional[dict[str, str]] = None, *, quiet: bool = False) -> dict[str, Any]:
        """{method} /api/{path} → parsed JSON, or {} on any error (logged; at debug when ``quiet``)."""
        import aiohttp
        if ".." in path:
            logger.error("ClickClack API path traversal blocked: %s", path)
            return {}
        log = logger.debug if quiet else logger.error
        url = f"{self._base_url}/api/{path.lstrip('/')}"
        kwargs: dict[str, Any] = {"headers": self._auth_header(), "timeout": aiohttp.ClientTimeout(total=30)}
        if payload is not None:
            kwargs["json"] = payload
        if params:
            kwargs["params"] = params
        try:
            async with self._session.request(method, url, **kwargs) as resp:
                if resp.status >= 400:
                    body = await resp.text()
                    log("ClickClack API %s %s → %s: %s", method, path, resp.status, body[:200])
                    return {}
                if resp.status == 204:
                    return {"ok": True}
                return await resp.json()
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            log("ClickClack API %s %s network error: %s", method, path, exc)
            return {}

    async def _upload(self, file_data: bytes, filename: str, content_type: str) -> Optional[str]:
        """Upload bytes to the workspace and return the ``upl_...`` ID, or None on failure."""
        import aiohttp
        form = aiohttp.FormData()
        form.add_field("file", file_data, filename=filename, content_type=content_type)
        try:
            async with self._session.post(
                    f"{self._base_url}/api/uploads", params={"workspace_id": self._workspace_id},
                    headers=self._auth_header(), data=form, timeout=aiohttp.ClientTimeout(total=120)) as resp:
                if resp.status >= 400:
                    logger.error("ClickClack upload → %s: %s", resp.status, (await resp.text())[:200])
                    return None
                data = await resp.json()
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            logger.error("ClickClack upload network error: %s", exc)
            return None
        upload = data.get("upload", data)
        return upload.get("id") if isinstance(upload, dict) else None

    async def _thread_root(self, chat_id: str, reply_to: Optional[str], metadata: _Metadata) -> Optional[str]:
        """Root message ID to reply under, or None for a top-level post.

        An inbound thread (metadata thread_id) always gets its reply in the thread; ``reply_to`` only
        opens a thread in reply_mode=thread. ``reply_to`` may itself be a reply, so it is resolved
        to its root (nested replies are rejected by the server)."""
        thread_id = isinstance(metadata, dict) and (metadata.get("thread_id") or metadata.get("root_id"))
        candidate = thread_id or (reply_to if self._reply_mode == "thread" else None)
        if not candidate:
            return None
        data = await self._api("GET", f"messages/{candidate}")
        msg = data.get("message") or {}
        return msg.get("thread_root_id") or str(candidate)

    async def _post(self, chat_id: str, body: str, thread_root: Optional[str],
                    upload_id: Optional[str] = None) -> dict[str, Any]:
        payload: dict[str, Any] = {"body": body}
        if upload_id and not thread_root:
            payload["upload_id"] = upload_id  # linked atomically with the create
        data = await self._api("POST", _create_path(chat_id, thread_root), payload)
        if upload_id and thread_root and (mid := _message_id(data)):
            await self._api("POST", f"messages/{mid}/attachments", {"upload_id": upload_id})
        return data

    # --- Required overrides ---

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        import aiohttp
        if not self._base_url or not self._token:
            logger.error("ClickClack: URL or token not configured")
            return False
        self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30), trust_env=gateway_trust_env())
        self._closing = False
        me = (await self._api("GET", "me")).get("user") or {}
        if not me.get("id"):
            logger.error("ClickClack: failed to authenticate — check CLICKCLACK_TOKEN and CLICKCLACK_URL")
            await self._session.close()
            return False
        self._bot_user_id, self._bot_handle = me["id"], me.get("handle", "")
        workspaces = (await self._api("GET", "workspaces")).get("workspaces") or []
        if len(workspaces) != 1:
            logger.error("ClickClack: bot token must be bound to exactly one workspace (got %d)", len(workspaces))
            await self._session.close()
            return False
        self._workspace_id = workspaces[0]["id"]
        logger.info("ClickClack: authenticated as @%s (%s) in %s on %s",
                    self._bot_handle, self._bot_user_id, self._workspace_id, self._base_url)
        self._ws_task = asyncio.create_task(self._ws_loop())
        self._menu_task = asyncio.create_task(self._register_command_menu())
        self._mark_connected()
        return True

    async def disconnect(self) -> None:
        self._closing = True
        await cancel_task(self._menu_task)
        await cancel_task(self._ws_task)
        if self._ws:
            await self._ws.close()
            self._ws = None
        if self._session and not self._session.closed:
            await self._session.close()
        self._mark_disconnected()
        logger.info("ClickClack: disconnected")

    async def send(self, chat_id: str, content: str, reply_to: Optional[str] = None,
                   metadata: _Metadata = None) -> SendResult:
        if not content:
            return SendResult(success=True)
        thread_root = await self._thread_root(chat_id, reply_to, metadata)
        if self._tool_progress != "plain" and self._is_progress(content):
            return await self._send_progress(chat_id, content, thread_root)
        result = SendResult(success=True)
        for chunk in self.truncate_message(content, MAX_MESSAGE_LENGTH):
            mid = _message_id(await self._post(chat_id, chunk, thread_root))
            result = SendResult(success=True, message_id=mid) if mid else SendResult(
                success=False, error="Failed to create message")
            if not result.success:
                break
        return result

    def _is_progress(self, content: str) -> bool:
        if self._progress_glyph_set is None:
            self._progress_glyph_set = _progress_glyphs()
        return _is_tool_progress(content, self._progress_glyph_set)

    async def _send_progress(self, chat_id: str, content: str, thread_root: Optional[str]) -> SendResult:
        """Post a tool-progress bubble as agent_tool activity. Thread replies cannot carry activity
        kinds, so progress inside a thread (or with tool_progress=off) is dropped."""
        if self._tool_progress == "off" or thread_root:
            return SendResult(success=True, message_id=None)
        turn_id = self._turn_ids.get(chat_id) or f"turn_{uuid.uuid4().hex}"
        data = await self._api("POST", _create_path(chat_id, None),
                               {"body": content[:MAX_MESSAGE_LENGTH], "kind": "agent_tool", "turn_id": turn_id})
        mid = _message_id(data)
        return SendResult(success=True, message_id=mid) if mid else SendResult(
            success=False, error="Failed to post tool progress")

    async def get_chat_info(self, chat_id: str) -> dict[str, Any]:
        if _is_dm(chat_id):
            conv = (await self._api("GET", f"dms/{chat_id}")).get("conversation") or {}
            names = [m.get("display_name") or m.get("handle") or m.get("id", "")
                     for m in conv.get("members") or [] if m.get("id") != self._bot_user_id]
            return {"name": ", ".join(names) or chat_id, "type": "dm" if len(names) <= 1 else "group"}
        chan = (await self._api("GET", f"channels/{chat_id}")).get("channel") or {}
        return {"name": chan.get("name") or chat_id,
                "type": "group" if chan.get("kind") == "private" else "channel"}

    # --- Optional overrides ---

    async def _ephemeral(self, chat_id: str, event_type: str) -> None:
        target = "direct_conversation_id" if _is_dm(chat_id) else "channel_id"
        await self._api("POST", "realtime/ephemeral",
                        {"workspace_id": self._workspace_id, target: chat_id, "type": event_type})

    async def send_typing(self, chat_id: str, metadata: _Metadata = None) -> None:
        await self._ephemeral(chat_id, "typing.started")

    async def stop_typing(self, chat_id: str) -> None:
        await self._ephemeral(chat_id, "typing.stopped")

    async def edit_message(self, chat_id: str, message_id: str, content: str, *, finalize: bool = False) -> SendResult:
        data = await self._api("PATCH", f"messages/{message_id}", {"body": content})
        mid = _message_id(data)
        return SendResult(success=True, message_id=mid) if mid else SendResult(success=False, error="Edit failed")

    async def delete_message(self, chat_id: str, message_id: str) -> bool:
        return bool(await self._api("DELETE", f"messages/{message_id}"))

    async def send_image_file(self, chat_id: str, image_path: str, caption: Optional[str] = None,
                              reply_to: Optional[str] = None, metadata: _Metadata = None) -> SendResult:
        return await self._send_local_file(chat_id, image_path, caption, reply_to, metadata=metadata)

    async def send_document(self, chat_id: str, file_path: str, caption: Optional[str] = None,
                            file_name: Optional[str] = None, reply_to: Optional[str] = None,
                            metadata: _Metadata = None) -> SendResult:
        return await self._send_local_file(chat_id, file_path, caption, reply_to, file_name, metadata)

    async def send_voice(self, chat_id: str, audio_path: str, caption: Optional[str] = None,
                         reply_to: Optional[str] = None, metadata: _Metadata = None, **kwargs: Any) -> SendResult:
        return await self._send_local_file(chat_id, audio_path, caption, reply_to, metadata=metadata)

    async def send_video(self, chat_id: str, video_path: str, caption: Optional[str] = None,
                         reply_to: Optional[str] = None, metadata: _Metadata = None) -> SendResult:
        return await self._send_local_file(chat_id, video_path, caption, reply_to, metadata=metadata)

    async def _send_local_file(self, chat_id: str, file_path: str, caption: Optional[str], reply_to: Optional[str],
                               file_name: Optional[str] = None, metadata: _Metadata = None) -> SendResult:
        p = Path(file_path)
        if not p.exists():
            logger.warning("ClickClack: local file not found, skipping: %s", file_path)
            return SendResult(success=True, message_id=None)
        fname = file_name or p.name
        data = await asyncio.to_thread(p.read_bytes)
        upload_id = await self._upload(data, fname, mimetypes.guess_type(fname)[0] or "application/octet-stream")
        if not upload_id:
            return SendResult(success=False, error="File upload failed")
        thread_root = await self._thread_root(chat_id, reply_to, metadata)
        # Empty bodies are rejected, so a bare file is captioned with its name.
        mid = _message_id(await self._post(chat_id, caption or fname, thread_root, upload_id))
        return SendResult(success=True, message_id=mid) if mid else SendResult(
            success=False, error="Failed to post file")

    # --- Clarify prompts ---

    async def send_clarify(self, chat_id: str, question: str, choices: Optional[list], clarify_id: str,
                           session_key: str, metadata: _Metadata = None) -> SendResult:
        """Single-select clarify: one numbered message with keycap reactions as one-click answers
        (ClickClack has no buttons). Typed numbers/labels still resolve through the gateway text
        intercept. Multi-select (a reaction cannot express a set) and open-ended prompts use the
        base-class text rendering."""
        from tools.clarify_gateway import get_pending, mark_awaiting_text
        entry = get_pending(clarify_id) if choices else None
        if entry is None or entry.multi_select:
            return await super().send_clarify(chat_id, question, choices, clarify_id, session_key, metadata)
        head = [t("gateway.clarify.question", question=question), "",
                *(t("gateway.clarify.option_line", index=i, choice=c) for i, c in enumerate(choices, start=1)), ""]
        body = "\n".join([*head, _CLARIFY_HINT])
        if len(body) > MAX_MESSAGE_LENGTH:  # reactions need a single anchor message
            return await super().send_clarify(chat_id, question, choices, clarify_id, session_key, metadata)
        mark_awaiting_text(clarify_id)
        thread_root = await self._thread_root(chat_id, None, metadata)
        mid = _message_id(await self._post(chat_id, body, thread_root))
        if not mid:
            return SendResult(success=False, error="Failed to create message")
        while len(self._clarify_cards) >= _CLARIFY_CARDS_MAX:
            self._clarify_cards.pop(next(iter(self._clarify_cards)))
        card = {"clarify_id": clarify_id, "entry": entry, "choices": list(choices), "chat_id": chat_id,
                "body": body, "emojis": _CLARIFY_EMOJIS[:len(choices)], "added": []}
        self._clarify_cards[mid] = card  # tracked before reacting so an early click counts
        for emoji in card["emojis"]:
            if await self._api("POST", f"messages/{mid}/reactions", {"emoji": emoji}, quiet=True):
                card["added"].append(emoji)
                continue
            if not self._reactions_warned:
                self._reactions_warned = True
                logger.warning("ClickClack: cannot add reactions to %s (token scope?); "
                               "clarify prompts fall back to typed answers", mid)
            if not card["added"]:  # text-only: drop the reaction hint
                self._clarify_cards.pop(mid, None)
                await self.edit_message(chat_id, mid, "\n".join([*head, t("gateway.clarify.hint_single")]))
            break
        return SendResult(success=True, message_id=mid)

    async def retire_clarify_card(self, clarify_id: str, notice: str) -> None:
        """Mark a clarify card that ended without a click (timeout, reset, superseded) and remove the
        bot's reactions. Best effort; never raises."""
        try:
            mid = next((m for m, c in self._clarify_cards.items() if c["clarify_id"] == clarify_id), None)
            card = self._clarify_cards.pop(mid, None) if mid else None
            if not card:
                return
            if notice:
                await self.edit_message(card["chat_id"], mid, f"{card['body']}\n\n{notice}")
            for emoji in card["added"]:
                await self._api("DELETE", f"messages/{mid}/reactions/{quote(emoji, safe='')}", quiet=True)
        except Exception:
            logger.debug("ClickClack: failed to retire clarify card %s", clarify_id, exc_info=True)

    async def _handle_reaction(self, payload: dict[str, Any]) -> None:
        """Resolve a tracked clarify card when an allowed user adds one of its keycap reactions."""
        mid, user_id = payload.get("message_id") or "", payload.get("user_id") or ""
        card = self._clarify_cards.get(mid)
        if not card or not user_id or user_id == self._bot_user_id:
            return
        emoji = str(payload.get("emoji") or "").replace("\ufe0f", "")
        idx = next((i for i, e in enumerate(card["emojis"]) if e.replace("\ufe0f", "") == emoji), None)
        if idx is None:
            return
        if not await self._user_allowed(user_id, card["chat_id"]):
            logger.debug("ClickClack: ignoring clarify reaction from unauthorized user %s", user_id)
            return
        from tools.clarify_gateway import get_pending, resolve_gateway_clarify
        self._clarify_cards.pop(mid, None)
        if get_pending(card["clarify_id"]) is card["entry"]:
            resolve_gateway_clarify(card["clarify_id"], card["choices"][idx])

    async def _user_allowed(self, user_id: str, chat_id: str) -> bool:
        """Whether ``user_id`` may talk to the bot: the gateway runner's own inbound authorization when
        reachable, else CLICKCLACK_/GATEWAY_ALLOWED_USERS and *_ALLOW_ALL_USERS. Fails closed."""
        try:
            from gateway.run import _gateway_runner_ref
            check = getattr(_gateway_runner_ref(), "_is_user_authorized", None)
        except Exception:
            check = None
        if callable(check):
            try:
                allowed = check(self.build_source(
                    chat_id=chat_id, chat_type="dm" if _is_dm(chat_id) else "channel", user_id=user_id))
                return bool(await allowed if inspect.isawaitable(allowed) else allowed)
            except Exception:
                logger.debug("ClickClack: gateway authorization check failed; using allow-list env", exc_info=True)
        if any(str(_get_scoped_secret(env, "")).strip().lower() in {"true", "1", "yes"}
               for env in ("CLICKCLACK_ALLOW_ALL_USERS", "GATEWAY_ALLOW_ALL_USERS")):
            return True
        allowed_ids = _id_set(_get_scoped_secret("CLICKCLACK_ALLOWED_USERS", "")) | _id_set(
            _get_scoped_secret("GATEWAY_ALLOWED_USERS", ""))
        return "*" in allowed_ids or user_id in allowed_ids

    # --- Command menu ---

    async def _register_command_menu(self) -> None:
        """Publish Hermes' gateway commands as this bot's ClickClack command menu (discovery only;
        ClickClack delivers unknown slash commands as plain messages, which is what Hermes parses)."""
        try:
            from hermes_cli.commands_platforms import telegram_bot_commands
            pairs = await asyncio.to_thread(telegram_bot_commands)
        except Exception:
            logger.debug("ClickClack: command menu unavailable", exc_info=True)
            return
        commands, seen = [], set()
        for name, desc in pairs:
            if name in seen or len(commands) >= _MENU_MAX_COMMANDS:
                continue
            seen.add(name)
            commands.append({"command": f"/{name}", "description": (str(desc or "").strip() or name)[:_MENU_DESC_LIMIT]})
        data = await self._api("PUT", "bots/self/commands", {"commands": commands})
        if data:
            logger.info("ClickClack: registered %d bot commands", len(commands))

    # --- Realtime ---

    async def _ws_loop(self) -> None:
        import aiohttp
        delay = _RECONNECT_BASE_DELAY
        while not self._closing:
            try:
                await self._ws_connect_and_listen()
                delay = _RECONNECT_BASE_DELAY
            except asyncio.CancelledError:
                return
            except Exception as exc:
                if self._closing:
                    return
                if isinstance(exc, aiohttp.WSServerHandshakeError) and exc.status in {401, 403}:
                    logger.error("ClickClack WS auth failed (HTTP %d) — stopping reconnect", exc.status)
                    self._set_fatal_error(
                        "clickclack_auth_error",
                        f"ClickClack WebSocket authentication rejected (HTTP {exc.status}). The bot token is "
                        "invalid, revoked, or lacks realtime:read — check CLICKCLACK_TOKEN.", retryable=False)
                    await self._notify_fatal_error()
                    return
                logger.warning("ClickClack WS error: %s — reconnecting in %.0fs", exc, delay)
            if self._closing:
                return
            await asyncio.sleep(delay + delay * _RECONNECT_JITTER * random.random())
            delay = min(delay * 2, _RECONNECT_MAX_DELAY)

    async def _tail_cursor(self) -> str:
        data = await self._api("GET", "realtime/events",
                               params={"workspace_id": self._workspace_id, "include_tail": "true", "limit": "1"})
        return data.get("tail_cursor") or ""

    async def _ws_connect_and_listen(self) -> None:
        import aiohttp
        if self._cursor is None:
            self._cursor = await self._tail_cursor()
        ws_url = re.sub(r"^http", "ws", self._base_url) + "/api/realtime/ws"
        params = {"workspace_id": self._workspace_id}
        if self._cursor:
            params["after_cursor"] = self._cursor
        logger.info("ClickClack: connecting to %s", ws_url)
        self._ws = await self._session.ws_connect(ws_url, params=params, headers=self._auth_header(), heartbeat=30.0)
        logger.info("ClickClack: WebSocket connected")
        async for raw in self._ws:
            if self._closing:
                return
            if raw.type in (aiohttp.WSMsgType.TEXT, aiohttp.WSMsgType.BINARY):
                try:
                    event = json.loads(raw.data)
                except (json.JSONDecodeError, TypeError):
                    continue
                try:
                    await self._handle_event(event)
                except Exception:
                    logger.exception("ClickClack: failed to handle event %s", event.get("id"))
                if event.get("cursor"):
                    self._cursor = event["cursor"]
            elif raw.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSE,
                              aiohttp.WSMsgType.CLOSING, aiohttp.WSMsgType.CLOSED):
                break
        code = self._ws.close_code
        logger.info("ClickClack: WebSocket closed (code %s)", code)
        if code == _WS_CLOSE_RESYNC:
            logger.warning("ClickClack: replay budget exhausted; resuming from the current tail")
            self._cursor = None
        elif code == _WS_CLOSE_POLICY:
            raise RuntimeError("credential revoked or session closed by server (1008)")

    def _gate_channel(self, channel_id: str, text: str, mentioned: list) -> Optional[str]:
        """Mention-gate a channel message; return the cleaned text, or None to ignore it."""
        extra = self.config.extra
        allowed = _id_set(_extra_or_secret(extra, "allowed_channels", "CLICKCLACK_ALLOWED_CHANNELS", blank_is_unset=False))
        if allowed and channel_id not in allowed:
            return None
        require_mention = str(_extra_or_secret(extra, "require_mention", "CLICKCLACK_REQUIRE_MENTION", "true",
                                               blank_is_unset=False)).lower() not in {"false", "0", "no"}
        free = _id_set(_extra_or_secret(extra, "free_response_channels", "CLICKCLACK_FREE_RESPONSE_CHANNELS",
                                        blank_is_unset=False))
        has_mention = self._bot_user_id in (mentioned or [])
        if require_mention and channel_id not in free and not has_mention:
            return None
        if self._bot_handle:
            text = re.sub(rf"@{re.escape(self._bot_handle)}\b", "", text, flags=re.IGNORECASE).strip()
        return text

    async def _download_attachments(self, attachments: list) -> tuple[list[str], list[str]]:
        """Download attachments now (their URLs need the bot token) → (cached paths, mime types)."""
        import aiohttp
        from gateway.platforms.base import (
            cache_audio_from_bytes_async, cache_document_from_bytes_async, cache_image_from_bytes_async)
        cache_fns = {"image/": cache_image_from_bytes_async, "audio/": cache_audio_from_bytes_async}
        paths, types = [], []
        for att in attachments or []:
            upload_id = att.get("id") or att.get("upload_id")
            if not upload_id or ".." in upload_id or "/" in upload_id:
                continue
            fname = att.get("filename") or f"file_{upload_id}"
            mime = att.get("content_type") or "application/octet-stream"
            try:
                async with self._session.get(f"{self._base_url}/api/uploads/{upload_id}", headers=self._auth_header(),
                                             timeout=aiohttp.ClientTimeout(total=60)) as resp:
                    if resp.status >= 400:
                        logger.warning("ClickClack: failed to download upload %s: HTTP %s", upload_id, resp.status)
                        continue
                    data = await resp.read()
                prefix = next((p for p in cache_fns if mime.startswith(p)), None)
                if prefix:
                    paths.append(await cache_fns[prefix](data, Path(fname).suffix or _INBOUND_CACHE_EXT[prefix]))
                else:
                    paths.append(await cache_document_from_bytes_async(data, fname))
                types.append(mime)
            except Exception as exc:
                logger.warning("ClickClack: error downloading upload %s: %s", upload_id, exc)
        return paths, types

    async def _handle_event(self, event: dict[str, Any]) -> None:
        if event.get("type") == "reaction.added":
            await self._handle_reaction(event.get("payload") or {})
            return
        if event.get("type") not in _INBOUND_EVENTS:
            return
        payload = event.get("payload") or {}
        message_id = payload.get("message_id", "")
        if not message_id or payload.get("author_id") == self._bot_user_id or self._dedup.is_duplicate(message_id):
            return
        msg = (await self._api("GET", f"messages/{message_id}")).get("message") or {}
        author = msg.get("author") or {}
        if (not msg or msg.get("author_id") == self._bot_user_id or msg.get("deleted_at")
                or msg.get("kind", "message") != "message"):
            return
        dm_id, channel_id = msg.get("direct_conversation_id"), msg.get("channel_id")
        chat_id = dm_id or channel_id
        if not chat_id:
            return
        text = msg.get("body", "")
        if not dm_id:  # DMs need no gating; channels are mention-gated
            gated = self._gate_channel(channel_id, text, event.get("mentioned_user_ids") or [])
            if gated is None:
                return
            text = gated
        self._turn_ids[chat_id] = f"turn_{uuid.uuid4().hex}"
        thread_id = msg.get("thread_root_id") if msg.get("parent_message_id") else None
        if not thread_id and self._reply_mode == "thread" and not dm_id:
            thread_id = message_id
        if text[:1].isspace() and text.lstrip().startswith("/"):
            text = text.lstrip()
        media_urls, media_types = await self._download_attachments(msg.get("attachments") or [])
        if text.startswith("/"):
            msg_type = MessageType.COMMAND
        elif media_types:
            msg_type = next((mt for prefix, mt in _MEDIA_MSG_TYPES if any(m.startswith(prefix) for m in media_types)),
                            MessageType.DOCUMENT)
        else:
            msg_type = MessageType.TEXT
        source = self.build_source(
            chat_id=chat_id, chat_type="dm" if dm_id else "channel",
            user_id=msg.get("author_id"), user_name=author.get("handle") or author.get("display_name") or None,
            thread_id=thread_id, message_id=message_id, is_bot=author.get("kind") == "bot")
        from gateway.platforms.base import resolve_channel_prompt
        await self.handle_message(MessageEvent(
            text=text, message_type=msg_type, source=source, raw_message=msg, message_id=message_id,
            media_urls=media_urls or None, media_types=media_types or None,
            channel_prompt=resolve_channel_prompt(self.config.extra, chat_id, None)))


# --- Standalone send (out-of-process cron delivery) ---

async def _standalone_send(pconfig, chat_id: str, message: str, *, thread_id: Optional[str] = None,
                           media_files: Optional[list] = None, force_document: bool = False) -> dict[str, Any]:
    """Send via the REST API without a live gateway adapter. ``force_document`` is signature parity only."""
    try:
        import aiohttp
    except ImportError:
        return send_error("aiohttp not installed. Run: pip install aiohttp")
    base_url, token = _url_and_token(pconfig)
    if not base_url or not token:
        return send_error("ClickClack standalone send: CLICKCLACK_URL and CLICKCLACK_TOKEN must both be set")
    headers = {"Authorization": f"Bearer {token}"}
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60)) as session:
            upload_ids: list[str] = []
            if media_files:
                async with session.get(f"{base_url}/api/workspaces", headers=headers) as resp:
                    workspaces = (await resp.json()).get("workspaces") or [] if resp.status == 200 else []
                if len(workspaces) != 1:
                    return send_error("ClickClack standalone send: cannot resolve the bot's workspace")
                for media in media_files:
                    file_path = media.get("path") if isinstance(media, dict) else media
                    if not file_path or not os.path.exists(file_path):
                        continue
                    form = aiohttp.FormData()
                    form.add_field("file", await asyncio.to_thread(Path(file_path).read_bytes),
                                   filename=os.path.basename(file_path))
                    async with session.post(f"{base_url}/api/uploads", params={"workspace_id": workspaces[0]["id"]},
                                            data=form, headers=headers) as resp:
                        if resp.status not in {200, 201}:
                            return send_error(f"ClickClack upload failed ({resp.status}): {(await resp.text())[:400]}")
                        data = await resp.json()
                        upload_ids.append(data.get("upload", data).get("id"))
            body = message or "(attachment)"
            payload: dict[str, Any] = {"body": body}
            if upload_ids and not thread_id:
                payload["upload_id"] = upload_ids.pop(0)
            async with session.post(f"{base_url}/api/{_create_path(chat_id, thread_id)}",
                                    headers=headers, json=payload) as resp:
                if resp.status not in {200, 201}:
                    return send_error(f"ClickClack API error ({resp.status}): {(await resp.text())[:400]}")
                mid = _message_id(await resp.json())
            for upload_id in upload_ids:  # extra files attach to the same message
                async with session.post(f"{base_url}/api/messages/{mid}/attachments", headers=headers,
                                        json={"upload_id": upload_id}) as resp:
                    if resp.status not in {200, 201}:
                        logger.warning("ClickClack standalone attach failed (%s)", resp.status)
            return {"success": True, "platform": PLATFORM_NAME, "chat_id": chat_id, "message_id": mid}
    except aiohttp.ClientError as exc:
        return send_error(f"ClickClack send failed (network): {exc}")
    except Exception as exc:
        return send_error(f"ClickClack send failed: {exc}")


# --- Config plumbing ---

_YAML_BRIDGE = (
    ("require_mention", "CLICKCLACK_REQUIRE_MENTION", "lower"),
    ("free_response_channels", "CLICKCLACK_FREE_RESPONSE_CHANNELS", "csv"),
    ("allowed_channels", "CLICKCLACK_ALLOWED_CHANNELS", "csv"),
    ("reply_mode", "CLICKCLACK_REPLY_MODE", "str"),
    ("tool_progress", "CLICKCLACK_TOOL_PROGRESS", "str"),
)


def _apply_yaml_config(yaml_cfg: dict, platform_cfg: dict) -> dict | None:
    return _apply_yaml_bridge(platform_cfg, _YAML_BRIDGE)


def _env_enablement() -> dict | None:
    url = _get_scoped_secret("CLICKCLACK_URL", "").strip()
    token = _get_scoped_secret("CLICKCLACK_TOKEN", "").strip()
    if not (url and token):
        return None
    return {"url": url, "token": token,
            **_seed_extra_from_env((("CLICKCLACK_REPLY_MODE", "reply_mode", None),),
                                   home_env="CLICKCLACK_HOME_CHANNEL")}


_is_connected = _env_is_connected("CLICKCLACK_URL", "CLICKCLACK_TOKEN")


def register(ctx) -> None:
    ctx.register_platform(
        name=PLATFORM_NAME, label="ClickClack", adapter_factory=ClickClackAdapter,
        check_fn=check_clickclack_requirements, validate_config=validate_clickclack_config,
        is_connected=_is_connected, required_env=["CLICKCLACK_URL", "CLICKCLACK_TOKEN"],
        install_hint="pip install aiohttp",
        env_enablement_fn=_env_enablement, apply_yaml_config_fn=_apply_yaml_config,
        allowed_users_env="CLICKCLACK_ALLOWED_USERS", allow_all_env="CLICKCLACK_ALLOW_ALL_USERS",
        cron_deliver_env_var="CLICKCLACK_HOME_CHANNEL",
        standalone_sender_fn=_standalone_send,
        max_message_length=MAX_MESSAGE_LENGTH,
        platform_hint=("You are chatting via ClickClack, a self-hosted team chat. "
                       "It renders standard Markdown, including code blocks and tables."),
        emoji="🦞", allow_update_command=True)
