"""Maubot plugin: Zabbix webhook receiver + alert management.

Accepts webhooks from Zabbix, posts formatted alerts into configured
Matrix rooms, and lets users acknowledge/close problems via reactions.

Architecture:
  - Each webhook endpoint is a unique token mapped to a Matrix room.
  - Zabbix media-type sends POST to <plugin_webapp_url>/webhook/<token>.
  - Alerts land in the mapped room with severity formatting.
  - React ✅ to acknowledge, ❌ to close the problem in Zabbix.
"""

from __future__ import annotations

import asyncio
import secrets
from datetime import datetime, timedelta
from typing import Type

from aiohttp import web
from mautrix.api import Method, Path
from mautrix.types import (
    EventID,
    EventType,
    Format,
    MessageType,
    ReactionEvent,
    RelatesTo,
    RoomID,
    TextMessageEventContent,
)
from mautrix.util import markdown
from mautrix.util.async_db import Connection, UpgradeTable
from mautrix.util.config import BaseProxyConfig, ConfigUpdateHelper
from maubot import MessageEvent, Plugin
from maubot.handlers import command, event, web as webhandler


# --- Reaction map ------------------------------------------------------------

ALERT_REACTIONS = {
    "✅": "acknowledge",
    "❌": "close",
    "🔇": "suppress",
    "🔔": "unsuppress",
}

# Normalize variation selectors / skin-tone modifiers.
import re as _re
_EMOJI_VARIATIONS = _re.compile("[︎️\U0001F3FB-\U0001F3FF]")


def _normalize_emoji(s: str) -> str:
    return _EMOJI_VARIATIONS.sub("", s).strip()


ALERT_REACTIONS_NORMALIZED = {
    _normalize_emoji(k): v for k, v in ALERT_REACTIONS.items()
}


# --- Zabbix severity helpers -------------------------------------------------

SEVERITY_MAP = {
    "0": "not_classified",
    "1": "information",
    "2": "warning",
    "3": "average",
    "4": "high",
    "5": "disaster",
    "not classified": "not_classified",
    "information": "information",
    "warning": "warning",
    "average": "average",
    "high": "high",
    "disaster": "disaster",
}


# --- DB schema ---------------------------------------------------------------

upgrade_table = UpgradeTable()


@upgrade_table.register(description="Initial schema")
async def upgrade_v1(conn: Connection) -> None:
    await conn.execute(
        """
        CREATE TABLE webhooks (
            token       TEXT PRIMARY KEY,
            room_id     TEXT NOT NULL,
            label       TEXT NOT NULL DEFAULT '',
            created_by  TEXT NOT NULL,
            created_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    await conn.execute(
        """
        CREATE TABLE alerts (
            matrix_event_id   TEXT PRIMARY KEY,
            room_id           TEXT NOT NULL,
            zabbix_event_id   TEXT NOT NULL,
            zabbix_host       TEXT,
            zabbix_problem    TEXT,
            severity          TEXT,
            received_at       TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )


@upgrade_table.register(description="Add room_settings table")
async def upgrade_v2(conn: Connection) -> None:
    await conn.execute(
        """
        CREATE TABLE room_settings (
            room_id          TEXT PRIMARY KEY,
            reactions_enabled INTEGER NOT NULL DEFAULT 1
        )
        """
    )


@upgrade_table.register(description="Add replies_enabled to room_settings")
async def upgrade_v3(conn: Connection) -> None:
    await conn.execute(
        "ALTER TABLE room_settings ADD COLUMN replies_enabled INTEGER NOT NULL DEFAULT 1"
    )


# --- Config ------------------------------------------------------------------


class Config(BaseProxyConfig):
    def do_update(self, helper: ConfigUpdateHelper) -> None:
        helper.copy("zabbix_url")
        helper.copy("zabbix_api_token")
        helper.copy("allowed_users")
        helper.copy("severity_icons")
        helper.copy("max_opdata_length")
        helper.copy("message_retention_days")
        helper.copy("debug")


# --- Plugin ------------------------------------------------------------------


class ZabbixPlugin(Plugin):

    @classmethod
    def get_config_class(cls) -> Type[BaseProxyConfig]:
        return Config

    @classmethod
    def get_db_upgrade_table(cls) -> UpgradeTable:
        return upgrade_table

    async def start(self) -> None:
        await super().start()
        self.config.load_and_update()
        self._cleanup_task = asyncio.create_task(self._periodic_cleanup())

    async def stop(self) -> None:
        if hasattr(self, "_cleanup_task") and self._cleanup_task:
            self._cleanup_task.cancel()
        await super().stop()

    async def _periodic_cleanup(self) -> None:
        """Periodically clean up old alerts from the DB and optionally redact
        old alert messages (and their threads) from Matrix rooms."""
        while True:
            await asyncio.sleep(3600)  # Run every hour
            try:
                retention_days = self.config.get("message_retention_days", 0)
                # If message retention is configured, redact old messages first.
                if retention_days and retention_days > 0:
                    cutoff = datetime.utcnow() - timedelta(days=retention_days)
                    rows = await self.database.fetch(
                        "SELECT matrix_event_id, room_id FROM alerts WHERE received_at < $1",
                        cutoff,
                    )
                    for row in rows:
                        await self._redact_thread(
                            RoomID(row["room_id"]), EventID(row["matrix_event_id"]),
                        )
                    if rows:
                        await self.database.execute(
                            "DELETE FROM alerts WHERE received_at < $1",
                            cutoff,
                        )
                else:
                    # Even without message retention, clean orphaned DB rows after 7 days.
                    cutoff = datetime.utcnow() - timedelta(days=7)
                    await self.database.execute(
                        "DELETE FROM alerts WHERE received_at < $1",
                        cutoff,
                    )
            except asyncio.CancelledError:
                return
            except Exception as e:
                self.log.debug("Periodic cleanup failed: %s", e)

    async def _redact_thread(self, room_id: RoomID, event_id: EventID) -> None:
        """Redact an alert message and all messages in its thread."""
        try:
            # Redact thread replies first.
            relations = await self.client.api.request(
                Method.GET,
                Path.v1.rooms[room_id].relations[event_id]["m.thread"],
            )
            for evt in relations.get("chunk", []):
                try:
                    await self.client.redact(room_id, EventID(evt["event_id"]))
                except Exception:
                    pass
            # Redact the alert message itself.
            await self.client.redact(room_id, event_id)
        except Exception as e:
            self.log.debug("Failed to redact thread for %s: %s", event_id, e)

    # --- Helpers -------------------------------------------------------------

    @staticmethod
    def _friendly_timestamp(raw: str) -> str:
        """Try to parse a Zabbix timestamp into a human-friendly format.
        Falls back to the original string if parsing fails."""
        if not raw:
            return ""
        for fmt in ("%Y.%m.%d %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S"):
            try:
                dt = datetime.strptime(raw.strip(), fmt)
                return dt.strftime("%b %-d, %Y at %-I:%M %p")
            except ValueError:
                continue
        return raw

    def _allowed(self, sender: str) -> bool:
        allowed = self.config["allowed_users"] or []
        return not allowed or sender in allowed

    def _severity_icon(self, severity: str) -> str:
        key = SEVERITY_MAP.get(severity.lower().strip(), "not_classified") if severity else "not_classified"
        icons = self.config["severity_icons"] or {}
        return icons.get(key, "❔")

    @staticmethod
    def _md_to_html(body: str) -> str:
        html = markdown.render(body).strip()
        if html.startswith("<p>") and html.endswith("</p>") and html.count("<p>") == 1:
            html = html[3:-4]
        return html

    async def _send_md(
        self, room_id: RoomID, body: str, thread_parent: str | None = None,
    ) -> str:
        content = TextMessageEventContent(
            msgtype=MessageType.TEXT,
            format=Format.HTML,
            body=body,
            formatted_body=self._md_to_html(body),
        )
        if thread_parent:
            content.relates_to = RelatesTo(
                rel_type="m.thread",
                event_id=EventID(thread_parent),
            )
        return await self.client.send_message(room_id, content)

    async def _reply_md(self, evt: MessageEvent, body: str) -> None:
        content = TextMessageEventContent(
            msgtype=MessageType.NOTICE,
            format=Format.HTML,
            body=body,
            formatted_body=self._md_to_html(body),
        )
        await evt.reply(content)

    # --- Commands ------------------------------------------------------------

    @command.new("zabbix", help="Zabbix alert management. Try `!zabbix help`.")
    async def zabbix_cmd(self, evt: MessageEvent) -> None:
        if not self._allowed(evt.sender):
            return
        await self._reply_md(
            evt,
            "**Zabbix Bot** — webhook-based alert routing\n\n"
            "**Webhook management**\n"
            "- `!zabbix webhook add [label]` — create a webhook for this room\n"
            "- `!zabbix webhook list` — list webhooks for this room\n"
            "- `!zabbix webhook listall` — list all webhooks (all rooms)\n"
            "- `!zabbix webhook remove <token>` — delete a webhook\n\n"
            "**Alert actions** (via reactions on alert messages)\n"
            "- ✅ — acknowledge the problem in Zabbix\n"
            "- ❌ — close the problem in Zabbix\n"
            "- 🔇 — suppress the problem indefinitely\n"
            "- 🔔 — unsuppress the problem\n\n"
            "**Room settings**\n"
            "- `!zabbix reactions enable` — enable reaction actions in this room\n"
            "- `!zabbix reactions disable` — disable reaction actions in this room\n"
            "- `!zabbix replies enable` — enable reply-to-alert messaging in this room\n"
            "- `!zabbix replies disable` — disable reply-to-alert messaging in this room\n\n"
            "**Setup:** configure `zabbix_url` and `zabbix_api_token` in the "
            "instance config for reaction-based actions to work."
        )

    @zabbix_cmd.subcommand("reactions", help="Enable or disable reaction actions in this room")
    @command.argument("toggle", required=True)
    async def reactions_cmd(self, evt: MessageEvent, toggle: str) -> None:
        if not self._allowed(evt.sender):
            return
        toggle = (toggle or "").strip().lower()
        if toggle not in ("enable", "disable"):
            await self._reply_md(evt, "Usage: `!zabbix reactions enable` or `!zabbix reactions disable`")
            return
        enabled = 1 if toggle == "enable" else 0
        await self.database.execute(
            "INSERT INTO room_settings (room_id, reactions_enabled) VALUES ($1, $2) "
            "ON CONFLICT (room_id) DO UPDATE SET reactions_enabled = $2",
            str(evt.room_id), enabled,
        )
        state = "enabled" if enabled else "disabled"
        await self._reply_md(evt, f"Reaction actions **{state}** for this room.")

    async def _reactions_enabled(self, room_id: str) -> bool:
        row = await self.database.fetchrow(
            "SELECT reactions_enabled FROM room_settings WHERE room_id = $1", room_id,
        )
        if not row:
            return True  # enabled by default
        return bool(row["reactions_enabled"])

    @zabbix_cmd.subcommand("replies", help="Enable or disable reply-to-alert messaging in this room")
    @command.argument("toggle", required=True)
    async def replies_cmd(self, evt: MessageEvent, toggle: str) -> None:
        if not self._allowed(evt.sender):
            return
        toggle = (toggle or "").strip().lower()
        if toggle not in ("enable", "disable"):
            await self._reply_md(evt, "Usage: `!zabbix replies enable` or `!zabbix replies disable`")
            return
        enabled = 1 if toggle == "enable" else 0
        await self.database.execute(
            "INSERT INTO room_settings (room_id, replies_enabled) VALUES ($1, $2) "
            "ON CONFLICT (room_id) DO UPDATE SET replies_enabled = $2",
            str(evt.room_id), enabled,
        )
        state = "enabled" if enabled else "disabled"
        await self._reply_md(evt, f"Reply-to-alert messaging **{state}** for this room.")

    async def _replies_enabled(self, room_id: str) -> bool:
        row = await self.database.fetchrow(
            "SELECT replies_enabled FROM room_settings WHERE room_id = $1", room_id,
        )
        if not row:
            return True  # enabled by default
        return bool(row["replies_enabled"])

    @zabbix_cmd.subcommand("webhook", help="Manage webhook endpoints")
    async def webhook_cmd(self, evt: MessageEvent) -> None:
        pass  # subcommand group

    @webhook_cmd.subcommand("add", help="Create a webhook endpoint for this room")
    @command.argument("label", required=False, pass_raw=True)
    async def webhook_add(self, evt: MessageEvent, label: str = "") -> None:
        if not self._allowed(evt.sender):
            return
        await evt.mark_read()

        token = secrets.token_urlsafe(24)
        label = (label or "").strip() or "default"

        await self.database.execute(
            "INSERT INTO webhooks (token, room_id, label, created_by) VALUES ($1, $2, $3, $4)",
            token, str(evt.room_id), label, evt.sender,
        )

        webhook_url = f"{str(self.webapp_url).rstrip('/')}/webhook/{token}"
        await self._reply_md(
            evt,
            f"**Webhook created**\n\n"
            f"- **Label:** `{label}`\n"
            f"- **Room:** `{evt.room_id}`\n"
            f"- **URL:** `{webhook_url}`\n\n"
            f"Configure this URL in Zabbix → Media types → Webhook.\n"
            f"Token: `{token}`"
        )

    @webhook_cmd.subcommand("list", help="List webhooks for this room")
    async def webhook_list(self, evt: MessageEvent) -> None:
        if not self._allowed(evt.sender):
            return
        rows = await self.database.fetch(
            "SELECT token, label, created_by FROM webhooks WHERE room_id = $1",
            str(evt.room_id),
        )
        if not rows:
            await self._reply_md(evt, "No webhooks configured for this room.")
            return

        lines = ["**Webhooks for this room:**\n"]
        for row in rows:
            url = f"{str(self.webapp_url).rstrip('/')}/webhook/{row['token']}"
            lines.append(
                f"- **{row['label']}** — `{row['token'][:8]}…` by `{row['created_by']}`\n"
                f"  URL: `{url}`"
            )
        await self._reply_md(evt, "\n".join(lines))

    @webhook_cmd.subcommand("listall", help="List all webhooks across all rooms")
    async def webhook_listall(self, evt: MessageEvent) -> None:
        if not self._allowed(evt.sender):
            return
        rows = await self.database.fetch(
            "SELECT token, room_id, label, created_by FROM webhooks ORDER BY created_at",
        )
        if not rows:
            await self._reply_md(evt, "No webhooks configured.")
            return

        lines = ["**All webhooks:**\n"]
        for row in rows:
            lines.append(
                f"- **{row['label']}** → `{row['room_id']}` — token `{row['token'][:8]}…` "
                f"by `{row['created_by']}`"
            )
        await self._reply_md(evt, "\n".join(lines))

    @webhook_cmd.subcommand("remove", help="Remove a webhook by token (prefix match)")
    @command.argument("token_prefix", required=True)
    async def webhook_remove(self, evt: MessageEvent, token_prefix: str) -> None:
        if not self._allowed(evt.sender):
            return
        token_prefix = (token_prefix or "").strip()
        if not token_prefix:
            await self._reply_md(evt, "Provide the webhook token (or prefix) to remove.")
            return

        # Support prefix match so users can paste the short form from `list`.
        row = await self.database.fetchrow(
            "SELECT token, label FROM webhooks WHERE token LIKE $1 || '%'",
            token_prefix,
        )
        if not row:
            await self._reply_md(evt, f"No webhook found matching `{token_prefix}`.")
            return

        await self.database.execute("DELETE FROM webhooks WHERE token = $1", row["token"])
        await self._reply_md(evt, f"Removed webhook **{row['label']}** (`{row['token'][:8]}…`).")

    # --- Webhook receiver ----------------------------------------------------

    @webhandler.post("/webhook/{token}")
    async def web_receive_alert(self, req: web.Request) -> web.Response:
        token = req.match_info.get("token", "")
        if not token:
            return web.Response(text="Missing token.", status=400)

        row = await self.database.fetchrow(
            "SELECT room_id FROM webhooks WHERE token = $1", token,
        )
        if not row:
            return web.Response(text="Unknown webhook token.", status=404)

        room_id = row["room_id"]

        # Parse the incoming payload. Zabbix webhook media types typically
        # send JSON with fields like: event_id, host, problem, severity, etc.
        try:
            data = await req.json()
        except Exception:
            return web.Response(text="Invalid JSON body.", status=400)

        if self.config.get("debug", False):
            self.log.debug("Webhook %s received payload: %s", token[:8], data)

        # Extract fields -- flexible to accommodate different Zabbix webhook
        # template configurations. Field names are case-insensitive for
        # robustness.
        normalized = {k.lower(): v for k, v in data.items()}

        event_id = str(normalized.get("event_id", normalized.get("eventid", "")))
        host = str(normalized.get("host", normalized.get("hostname", "unknown")))
        problem = str(normalized.get("problem", normalized.get("subject", normalized.get("message", "No description"))))
        severity = str(normalized.get("severity", normalized.get("trigger_severity", "0")))
        status = str(normalized.get("status", normalized.get("event_status", "PROBLEM")))
        url = str(normalized.get("url", normalized.get("trigger_url", "")))
        opdata = str(normalized.get("opdata", normalized.get("operational_data", "")))
        timestamp_raw = str(normalized.get("timestamp", normalized.get("event_date", "")))
        timestamp = self._friendly_timestamp(timestamp_raw)

        # Build the alert message
        icon = self._severity_icon(severity)
        sev_name = SEVERITY_MAP.get(severity.lower().strip(), severity) if severity else "unknown"

        # Build a clickable Zabbix event link. Prefer an explicit URL from
        # the webhook payload; fall back to auto-generating one from config.
        if url:
            event_link = url
        else:
            zabbix_url = (self.config["zabbix_url"] or "").rstrip("/")
            event_link = (
                f"{zabbix_url}/zabbix.php?action=problem.view&eventid={event_id}"
                if zabbix_url and event_id else ""
            )

        if status.upper() == "OK" or status.upper() == "RESOLVED":
            # Resolution message
            body = (
                f"### ✅ Resolved: {problem}\n\n"
                f"- **Host:** {host}\n"
                f"- **Severity:** {icon} {sev_name}\n"
            )
            if timestamp:
                body += f"- **Time:** {timestamp}\n"
            if event_link:
                body += f"- **Event:** [{event_id}]({event_link})\n"
            elif event_id:
                body += f"- **Event ID:** {event_id}\n"

            # Post as a thread reply on the original alert if we can find it.
            original_row = None
            if event_id:
                original_row = await self.database.fetchrow(
                    "SELECT matrix_event_id FROM alerts WHERE room_id = $1 AND zabbix_event_id = $2 "
                    "ORDER BY received_at ASC LIMIT 1",
                    room_id, event_id,
                )
            if original_row:
                alert_mx_id = original_row["matrix_event_id"]
                await self._send_md(
                    RoomID(room_id), body,
                    thread_parent=alert_mx_id,
                )
                # Remove the 🔴 reaction and add 🟢.
                await self._swap_reaction(
                    RoomID(room_id), EventID(alert_mx_id), "🔴", "🟢",
                )
                # Clean up resolved alerts from the database.
                await self.database.execute(
                    "DELETE FROM alerts WHERE room_id = $1 AND zabbix_event_id = $2",
                    room_id, event_id,
                )
                return web.Response(text="OK", status=200)
        else:
            # Problem message
            body = (
                f"### {icon} {problem}\n\n"
                f"- **Host:** {host}\n"
                f"- **Severity:** {icon} {sev_name}\n"
                f"- **Status:** {status}\n"
            )
            if opdata and not opdata.lower().startswith("not available"):
                max_opdata = self.config.get("max_opdata_length", 200)
                if max_opdata and len(opdata) > max_opdata:
                    opdata = opdata[:max_opdata].rstrip() + "\u2026"
                body += f"- **Operational data:** {opdata}\n"
            if timestamp:
                body += f"- **Time:** {timestamp}\n"
            if event_link:
                body += f"- **Event:** [{event_id}]({event_link})\n"
            elif event_id:
                body += f"- **Event ID:** {event_id}\n"
            if await self._reactions_enabled(room_id):
                body += "\nReact ✅ to acknowledge, ❌ to close, 🔇 to suppress, or 🔔 to unsuppress."

        matrix_event_id = await self._send_md(RoomID(room_id), body)

        # Track the alert for reaction handling (only for problem alerts).
        if event_id and status.upper() not in ("OK", "RESOLVED"):
            await self.database.execute(
                """
                INSERT INTO alerts (matrix_event_id, room_id, zabbix_event_id,
                                    zabbix_host, zabbix_problem, severity)
                VALUES ($1, $2, $3, $4, $5, $6)
                """,
                str(matrix_event_id), room_id, event_id, host, problem, sev_name,
            )
            # React with 🔴 to indicate an active problem.
            await self.client.react(RoomID(room_id), matrix_event_id, "🔴")

        return web.Response(text="OK", status=200)

    # --- Reaction handler (acknowledge / close) ------------------------------

    @event.on(EventType.REACTION)
    async def on_reaction(self, evt: ReactionEvent) -> None:
        rel = evt.content.relates_to
        if not rel or not rel.event_id:
            return

        emoji = _normalize_emoji(rel.key or "")
        action = ALERT_REACTIONS_NORMALIZED.get(emoji)
        if action is None:
            return

        if not await self._reactions_enabled(str(evt.room_id)):
            return

        if not self._allowed(evt.sender):
            return

        # Look up the alert this reaction is on.
        row = await self.database.fetchrow(
            "SELECT zabbix_event_id, zabbix_host, zabbix_problem, severity FROM alerts WHERE matrix_event_id = $1",
            str(rel.event_id),
        )
        if not row:
            return  # not one of our alert messages

        zabbix_url = (self.config["zabbix_url"] or "").rstrip("/")
        api_token = self.config["zabbix_api_token"] or ""
        if not zabbix_url or not api_token:
            self.log.warning(
                "Reaction on alert but zabbix_url/zabbix_api_token not configured."
            )
            return

        event_id = row["zabbix_event_id"]
        alert_event_id = str(rel.event_id)

        if action == "acknowledge":
            await self._zabbix_acknowledge(zabbix_url, api_token, event_id, evt.sender)
            confirm_id = await self._send_md(
                evt.room_id,
                f"✅ **Acknowledged** problem `{row['zabbix_problem']}` "
                f"(event {event_id}) — by `{evt.sender}`",
                thread_parent=alert_event_id,
            )
            await self._track_thread_msg(confirm_id, evt.room_id, row)
        elif action == "close":
            await self._zabbix_close(zabbix_url, api_token, event_id, evt.sender)
            confirm_id = await self._send_md(
                evt.room_id,
                f"❌ **Closed** problem `{row['zabbix_problem']}` "
                f"(event {event_id}) — by `{evt.sender}`",
                thread_parent=alert_event_id,
            )
            await self._track_thread_msg(confirm_id, evt.room_id, row)
        elif action == "suppress":
            await self._zabbix_suppress(zabbix_url, api_token, event_id, evt.sender)
            confirm_id = await self._send_md(
                evt.room_id,
                f"🔇 **Suppressed** problem `{row['zabbix_problem']}` "
                f"(event {event_id}) — by `{evt.sender}`",
                thread_parent=alert_event_id,
            )
            await self._track_thread_msg(confirm_id, evt.room_id, row)
        elif action == "unsuppress":
            await self._zabbix_unsuppress(zabbix_url, api_token, event_id, evt.sender)
            confirm_id = await self._send_md(
                evt.room_id,
                f"🔔 **Unsuppressed** problem `{row['zabbix_problem']}` "
                f"(event {event_id}) — by `{evt.sender}`",
                thread_parent=alert_event_id,
            )
            await self._track_thread_msg(confirm_id, evt.room_id, row)

    # --- Reply handler (add message to Zabbix event) -------------------------

    @event.on(EventType.ROOM_MESSAGE)
    async def on_message(self, evt: MessageEvent) -> None:
        # Ignore our own messages.
        if evt.sender == self.client.mxid:
            return

        # Only handle messages that are part of an alert thread.
        # Accept either: (a) proper m.thread messages, or (b) plain replies
        # to a message we have tracked (alert or confirmation in the thread).
        rel = getattr(evt.content, "relates_to", None)
        if not rel:
            return

        # Check for thread relation (rel_type = m.thread).
        thread_root_id = ""
        if getattr(rel, "rel_type", None) == "m.thread" and getattr(rel, "event_id", None):
            thread_root_id = str(rel.event_id)

        # Also get in_reply_to (plain replies or in-thread replies).
        in_reply_to = getattr(rel, "in_reply_to", None)
        reply_to_id = str(in_reply_to.event_id) if in_reply_to and in_reply_to.event_id else ""

        if not thread_root_id and not reply_to_id:
            return

        debug = self.config.get("debug", False)
        if debug:
            self.log.debug(
                "Reply detected from %s to event %s (thread_root=%s)",
                evt.sender, reply_to_id, thread_root_id,
            )

        if not await self._replies_enabled(str(evt.room_id)):
            return

        if not self._allowed(evt.sender):
            return

        # Try to resolve the alert: prefer thread root, fall back to reply target.
        row = None
        alert_event_id = ""
        if thread_root_id:
            row = await self.database.fetchrow(
                "SELECT zabbix_event_id, zabbix_host, zabbix_problem, severity FROM alerts WHERE matrix_event_id = $1",
                thread_root_id,
            )
            alert_event_id = thread_root_id
        if not row and reply_to_id:
            row = await self.database.fetchrow(
                "SELECT zabbix_event_id, zabbix_host, zabbix_problem, severity FROM alerts WHERE matrix_event_id = $1",
                reply_to_id,
            )
            alert_event_id = reply_to_id
        if not row:
            return

        zabbix_url = (self.config["zabbix_url"] or "").rstrip("/")
        api_token = self.config["zabbix_api_token"] or ""
        if not zabbix_url or not api_token:
            self.log.warning(
                "Reply on alert but zabbix_url/zabbix_api_token not configured."
            )
            return

        event_id = row["zabbix_event_id"]

        # Resolve the original alert (thread root) for this Zabbix event.
        # This ensures we always thread from the original alert, not a confirmation.
        original = await self.database.fetchrow(
            "SELECT matrix_event_id FROM alerts WHERE room_id = $1 AND zabbix_event_id = $2 "
            "ORDER BY received_at ASC LIMIT 1",
            str(evt.room_id), event_id,
        )
        thread_parent = original["matrix_event_id"] if original else alert_event_id

        message_text = evt.content.body or ""
        # Strip Matrix reply fallback (lines starting with "> ")
        lines = message_text.split("\n")
        cleaned = []
        past_fallback = False
        for line in lines:
            if not past_fallback and line.startswith("> "):
                continue
            if not past_fallback and line.strip() == "":
                past_fallback = True
                continue
            past_fallback = True
            cleaned.append(line)
        message_text = "\n".join(cleaned).strip() or message_text.strip()

        if not message_text:
            return

        await self._zabbix_add_message(
            zabbix_url, api_token, event_id, evt.sender, message_text,
        )
        await self.client.react(evt.room_id, evt.event_id, "🤖")

    async def _track_thread_msg(self, matrix_event_id, room_id, alert_row) -> None:
        """Store a thread message in the alerts table so replies to it are
        also recognized as belonging to the same Zabbix event."""
        if not matrix_event_id:
            return
        try:
            zabbix_event_id = alert_row["zabbix_event_id"]
            zabbix_problem = alert_row["zabbix_problem"]
            try:
                zabbix_host = alert_row["zabbix_host"] or ""
            except (KeyError, IndexError):
                zabbix_host = ""
            try:
                severity = alert_row["severity"] or ""
            except (KeyError, IndexError):
                severity = ""
            await self.database.execute(
                """
                INSERT INTO alerts (matrix_event_id, room_id, zabbix_event_id,
                                    zabbix_host, zabbix_problem, severity)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (matrix_event_id) DO NOTHING
                """,
                str(matrix_event_id), str(room_id),
                zabbix_event_id, zabbix_host, zabbix_problem, severity,
            )
        except Exception as e:
            self.log.debug("_track_thread_msg failed: %s", e)

    async def _swap_reaction(
        self, room_id: RoomID, event_id: EventID, old_emoji: str, new_emoji: str,
    ) -> None:
        """Remove the bot's own ``old_emoji`` reaction on ``event_id`` and add ``new_emoji``."""
        try:
            # Find the bot's reaction event so we can redact it.
            relations = await self.client.api.request(
                Method.GET,
                Path.v1.rooms[room_id].relations[event_id]["m.annotation"]["m.reaction"],
            )
            for evt in relations.get("chunk", []):
                if (
                    evt.get("sender") == self.client.mxid
                    and evt.get("content", {}).get("m.relates_to", {}).get("key") == old_emoji
                ):
                    await self.client.redact(room_id, EventID(evt["event_id"]))
                    break
        except Exception as e:
            self.log.debug("Failed to remove %s reaction: %s", old_emoji, e)
        try:
            await self.client.react(room_id, event_id, new_emoji)
        except Exception as e:
            self.log.debug("Failed to add %s reaction: %s", new_emoji, e)

    # --- Zabbix API calls ----------------------------------------------------

    async def _zabbix_add_message(
        self, zabbix_url: str, api_token: str, event_id: str, user: str, message: str,
    ) -> None:
        payload = {
            "jsonrpc": "2.0",
            "method": "event.acknowledge",
            "params": {
                "eventids": event_id,
                "action": 4,  # 4=add message only
                "message": f"[Matrix — {user}] {message}",
            },
            "id": 1,
        }
        debug = self.config.get("debug", False)
        if debug:
            self.log.debug("Zabbix add_message request for event %s: %s", event_id, payload)
        try:
            async with self.http.post(
                f"{zabbix_url}/api_jsonrpc.php",
                json=payload,
                headers={"Authorization": f"Bearer {api_token}"},
            ) as resp:
                data = await resp.json(content_type=None)
                if debug:
                    self.log.debug("Zabbix add_message response for event %s: %s", event_id, data)
                if "error" in data:
                    self.log.error("Zabbix add_message failed: %s", data["error"])
        except Exception:
            self.log.exception("Failed to add message to event %s in Zabbix", event_id)

    async def _zabbix_acknowledge(
        self, zabbix_url: str, api_token: str, event_id: str, user: str
    ) -> None:
        payload = {
            "jsonrpc": "2.0",
            "method": "event.acknowledge",
            "params": {
                "eventids": event_id,
                "action": 6,  # 2=acknowledge + 4=add message
                "message": f"Acknowledged via Matrix by {user}",
            },
            "id": 1,
        }
        debug = self.config.get("debug", False)
        if debug:
            self.log.debug("Zabbix acknowledge request for event %s: %s", event_id, payload)
        try:
            async with self.http.post(
                f"{zabbix_url}/api_jsonrpc.php",
                json=payload,
                headers={"Authorization": f"Bearer {api_token}"},
            ) as resp:
                data = await resp.json(content_type=None)
                if debug:
                    self.log.debug("Zabbix acknowledge response for event %s: %s", event_id, data)
                if "error" in data:
                    self.log.error("Zabbix acknowledge failed: %s", data["error"])
        except Exception:
            self.log.exception("Failed to acknowledge event %s in Zabbix", event_id)

    async def _zabbix_close(
        self, zabbix_url: str, api_token: str, event_id: str, user: str
    ) -> None:
        payload = {
            "jsonrpc": "2.0",
            "method": "event.acknowledge",
            "params": {
                "eventids": event_id,
                "action": 7,  # 1=close + 2=acknowledge + 4=add message
                "message": f"Closed via Matrix by {user}",
            },
            "id": 1,
        }
        debug = self.config.get("debug", False)
        if debug:
            self.log.debug("Zabbix close request for event %s: %s", event_id, payload)
        try:
            async with self.http.post(
                f"{zabbix_url}/api_jsonrpc.php",
                json=payload,
                headers={"Authorization": f"Bearer {api_token}"},
            ) as resp:
                data = await resp.json(content_type=None)
                if debug:
                    self.log.debug("Zabbix close response for event %s: %s", event_id, data)
                if "error" in data:
                    self.log.error("Zabbix close failed: %s", data["error"])
        except Exception:
            self.log.exception("Failed to close event %s in Zabbix", event_id)

    async def _zabbix_suppress(
        self, zabbix_url: str, api_token: str, event_id: str, user: str
    ) -> None:
        payload = {
            "jsonrpc": "2.0",
            "method": "event.acknowledge",
            "params": {
                "eventids": event_id,
                "action": 38,  # 2=acknowledge + 4=add message + 32=suppress
                "message": f"Suppressed via Matrix by {user}",
                "suppress_until": 0,  # indefinite
            },
            "id": 1,
        }
        debug = self.config.get("debug", False)
        if debug:
            self.log.debug("Zabbix suppress request for event %s: %s", event_id, payload)
        try:
            async with self.http.post(
                f"{zabbix_url}/api_jsonrpc.php",
                json=payload,
                headers={"Authorization": f"Bearer {api_token}"},
            ) as resp:
                data = await resp.json(content_type=None)
                if debug:
                    self.log.debug("Zabbix suppress response for event %s: %s", event_id, data)
                if "error" in data:
                    self.log.error("Zabbix suppress failed: %s", data["error"])
        except Exception:
            self.log.exception("Failed to suppress event %s in Zabbix", event_id)

    async def _zabbix_unsuppress(
        self, zabbix_url: str, api_token: str, event_id: str, user: str
    ) -> None:
        payload = {
            "jsonrpc": "2.0",
            "method": "event.acknowledge",
            "params": {
                "eventids": event_id,
                "action": 70,  # 2=acknowledge + 4=add message + 64=unsuppress
                "message": f"Unsuppressed via Matrix by {user}",
            },
            "id": 1,
        }
        debug = self.config.get("debug", False)
        if debug:
            self.log.debug("Zabbix unsuppress request for event %s: %s", event_id, payload)
        try:
            async with self.http.post(
                f"{zabbix_url}/api_jsonrpc.php",
                json=payload,
                headers={"Authorization": f"Bearer {api_token}"},
            ) as resp:
                data = await resp.json(content_type=None)
                if debug:
                    self.log.debug("Zabbix unsuppress response for event %s: %s", event_id, data)
                if "error" in data:
                    self.log.error("Zabbix unsuppress failed: %s", data["error"])
        except Exception:
            self.log.exception("Failed to unsuppress event %s in Zabbix", event_id)
