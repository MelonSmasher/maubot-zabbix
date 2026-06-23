# maubot-zabbix

A [maubot](https://github.com/maubot/maubot) plugin that receives Zabbix webhooks and posts formatted alerts into Matrix rooms. Users can acknowledge or close problems directly via emoji reactions.

## Features

- **Multi-endpoint webhooks** — generate unique webhook URLs per room/team so different Zabbix alert actions can route to different channels
- **Formatted alerts** — severity icons, host, problem description, operational data, timestamps
- **Threaded resolution notifications** — resolution messages posted as thread replies on the original alert
- **Status indicators** — 🔴 reaction on active problems, swapped to 🟢 on resolution
- **Reaction-based actions** — react ✅ to acknowledge, ❌ to close, 🔇 to suppress, or 🔔 to unsuppress a problem in Zabbix
- **Reply-to-thread forwarding** — user messages in an alert's thread are forwarded to Zabbix as event messages (confirmed with a 🤖 reaction)
- **Per-room toggles** — enable/disable reactions and reply forwarding independently per room
- **Webhook management via bot commands** — no config file editing needed to add/remove endpoints
- **Automatic cleanup** — resolved alerts are purged from the database; orphaned entries older than 7 days are cleaned up hourly

## Setup

1. Upload the `.mbp` to your maubot instance and create a plugin instance.
2. Configure the instance:
   - `zabbix_url` — your Zabbix frontend URL (e.g. `https://zabbix.example.com`)
   - `zabbix_api_token` — a Zabbix API token with problem acknowledge/close permissions
   - `allowed_users` — (optional) restrict who can manage webhooks
3. In a Matrix room, run `!zabbix webhook add <label>` to generate a webhook URL.
4. In Zabbix, create a **Media type → Webhook** that POSTs JSON to the generated URL.

## Commands

| Command | Description |
|---------|-------------|
| `!zabbix` | Show help |
| `!zabbix webhook add [label]` | Create a webhook endpoint for the current room |
| `!zabbix webhook list` | List webhooks for the current room |
| `!zabbix webhook listall` | List all webhooks across all rooms |
| `!zabbix webhook remove <token>` | Delete a webhook (prefix match supported) |
| `!zabbix reactions enable/disable` | Toggle reaction-based actions for the current room |
| `!zabbix replies enable/disable` | Toggle reply forwarding to Zabbix for the current room |

## Zabbix Webhook Configuration

Create a Media type in Zabbix (Administration → Media types → Create) of type **Webhook** with the following parameters sent as the JSON body:

```json
{
  "event_id": "{EVENT.ID}",
  "host": "{HOST.NAME}",
  "problem": "{ALERT.SUBJECT}",
  "severity": "{TRIGGER.SEVERITY}",
  "status": "{EVENT.VALUE}",
  "opdata": "{EVENT.OPDATA}",
  "timestamp": "{EVENT.DATE} {EVENT.TIME}",
  "url": "{TRIGGER.URL}"
}
```

Set the webhook script to POST this JSON to the URL provided by `!zabbix webhook add`.

> **Note:** The `status` field should resolve to `PROBLEM` or `OK`/`RESOLVED` so the bot can distinguish new alerts from resolutions. Use `{EVENT.VALUE}` (returns `1` for problem, `0` for OK) or set up separate actions for problem/recovery with explicit status strings.

## Reactions

On any alert message posted by the bot (when reactions are enabled):

| Reaction | Action |
|----------|--------|
| ✅ | Acknowledge the problem in Zabbix (adds a message noting who acknowledged) |
| ❌ | Close the problem in Zabbix (requires "Allow manual close" on the trigger) |
| 🔇 | Suppress the problem indefinitely (hides from dashboards/notifications) |
| 🔔 | Unsuppress a previously suppressed problem |

Bot confirmations for reactions are posted as thread replies on the original alert.

## Reply Forwarding

When replies are enabled, any message a user posts **in an alert's thread** is forwarded to the corresponding Zabbix event as a message. The bot confirms successful delivery by reacting to the user's message with 🤖.

Plain replies outside of a thread are ignored.

## Configuration Reference

```yaml
# Zabbix server URL (e.g. https://zabbix.example.com).
# Required for acknowledging alerts via reactions.
zabbix_url: ""

# Zabbix API token (created in Zabbix → Administration → API tokens).
# Needs permissions: Read/Write to Problems (for acknowledge/close).
zabbix_api_token: ""

# Optional ACL: list of Matrix user IDs allowed to manage webhooks.
# Empty list = anyone in a room with the bot can use admin commands.
allowed_users: []

# Severity → emoji mapping shown in alert cards.
severity_icons:
  not_classified: "❔"
  information: "ℹ️"
  warning: "⚠️"
  average: "🔶"
  high: "🚨"
  disaster: "🔥"

# Maximum length (in characters) for the operational data field.
# If the value exceeds this limit it will be truncated with "…".
# Set to 0 to disable truncation.
max_opdata_length: 200

# Enable debug logging of incoming webhook payloads.
# When true, every POST body received is logged.
debug: false
```

## License

MIT
