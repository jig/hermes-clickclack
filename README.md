# hermes-clickclack

[Hermes Agent](https://github.com/NousResearch/hermes-agent) platform plugin for
[ClickClack](https://github.com/openclaw/clickclack), the self-hosted team chat.

It talks to ClickClack with a bot token over the REST API and the realtime
WebSocket, using only `aiohttp` (already a Hermes dependency).

## Features

- DMs, channels (mention-gated by default) and threads.
- Slash commands: ClickClack delivers unknown `/commands` as plain messages,
  so `/new`, `/reset`, `/help`… work as typed. Hermes' gateway commands are
  also published as the bot's command menu.
- Inbound attachments (downloaded with the bot token) and outbound files.
- Tool-progress bubbles folded into ClickClack's collapsible agent activity.
- Clarify prompts (the agent's multiple-choice questions) answerable with one
  click on a number reaction, or by typing the number or the option.
- Typing indicator, message edits and deletes.
- Cron / `hermes send` delivery to a home channel, also out of process.
- Missed events are replayed from the last cursor after a reconnect. A fresh
  gateway start begins at the tail, so old history is never replayed.

## Install

```sh
git clone git@github.com:jig/hermes-clickclack.git ~/git/src/github.com/jig/hermes-clickclack
ln -s ~/git/src/github.com/jig/hermes-clickclack ~/.hermes/plugins/clickclack
hermes plugins enable clickclack-platform
```

Create the bot on the ClickClack host (the token is printed once):

```sh
clickclack admin bot create --data <data-dir> \
  --workspace wsp_... --created-by usr_<owner> \
  --name lebobot --handle lebobot \
  --scopes bot:write,agent_activity:write --plain
```

Then add to `~/.hermes/.env` and restart the gateway
(`systemctl --user restart hermes-gateway`):

```sh
CLICKCLACK_URL=http://127.0.0.1:8067
CLICKCLACK_TOKEN=ccb_...
CLICKCLACK_ALLOWED_USERS=usr_...,usr_...
CLICKCLACK_HOME_CHANNEL=chn_...        # optional, cron delivery target
```

## Configuration

| Env var | Default | Meaning |
|---|---|---|
| `CLICKCLACK_URL` | — | Server URL. |
| `CLICKCLACK_TOKEN` | — | Bot token (`ccb_...`); it also selects the workspace. |
| `CLICKCLACK_ALLOWED_USERS` | — | User IDs allowed to talk to the bot. |
| `CLICKCLACK_ALLOW_ALL_USERS` | `false` | Let any workspace member use the bot. |
| `CLICKCLACK_HOME_CHANNEL` | — | `chn_...` or `dm_...` for cron/notifications. |
| `CLICKCLACK_REPLY_MODE` | `off` | `thread` answers top-level channel posts in a thread. Replies to a thread always stay in it. |
| `CLICKCLACK_TOOL_PROGRESS` | `fold` | Tool-progress bubbles (`📚 Reading skill…`): `fold` posts them as agent activity, collapsed in the web app; `off` drops them; `plain` sends them as normal messages. Inside threads they are always dropped. |
| `CLICKCLACK_REQUIRE_MENTION` | `true` | Channels need an `@bot` mention. DMs never do. |
| `CLICKCLACK_FREE_RESPONSE_CHANNELS` | — | Channels where no mention is needed. |
| `CLICKCLACK_ALLOWED_CHANNELS` | — | Whitelist of channels the bot answers in. |

The same keys (lowercase, without prefix) can go under `clickclack:` in
`config.yaml`; env wins.

## Clarify prompts

ClickClack has no buttons, so a single-choice clarify is posted as one
numbered message and the bot reacts to it with 1️⃣…9️⃣. Clicking a reaction
answers it; typing the number or the option text also works. Only users
allowed to talk to the bot count (the gateway's own check when available,
else `CLICKCLACK_ALLOWED_USERS` / `CLICKCLACK_ALLOW_ALL_USERS`); the bot's own
reactions are ignored. Options past the ninth, multi-select prompts and
open-ended questions are answered by typing. If the bot cannot add reactions
(e.g. its token lacks the scope), the prompt degrades to text only. When a
prompt expires unanswered, its message is marked and the reactions removed.

## Chat IDs

Hermes chat IDs are ClickClack IDs: `chn_...` for channels and `dm_...` for
direct conversations. Thread replies use the root message ID as thread ID.

## Tests

Unit tests need no network. Run them with a Python that has Hermes'
dependencies plus `pytest`; the Hermes checkout is put on `sys.path`
(`HERMES_AGENT_DIR` overrides the default `~/.hermes/hermes-agent`):

```sh
python -m pytest tests -q
```

## License

MIT, see [LICENSE](LICENSE).
