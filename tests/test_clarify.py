"""Clarify prompts answered by keycap reactions; no network (``_api`` is faked)."""

import asyncio
import logging
import sys
import types
import uuid

import pytest

from tools import clarify_gateway

BOT, ALICE, MALLORY = "usr_bot", "usr_alice", "usr_mallory"
KEYCAPS = [f"{n}\ufe0f\u20e3" for n in range(1, 10)]


class FakeAPI:
    """Records every call; the question post returns ``msg_q``."""

    def __init__(self, fail_reactions=False, raise_on=None):
        self.calls = []
        self.fail_reactions = fail_reactions
        self.raise_on = raise_on

    async def __call__(self, method, path, payload=None, params=None, *, quiet=False):
        self.calls.append((method, path, payload))
        if self.raise_on and method == self.raise_on:
            raise RuntimeError("boom")
        if path.endswith("/reactions"):
            return {} if self.fail_reactions else {"ok": True}
        if method == "POST":
            return {"message": {"id": "msg_q"}}
        if method in ("PATCH", "GET"):
            return {"message": {"id": path.rsplit("/", 1)[-1]}}
        return {"ok": True}

    def posts(self):
        return [(p, b) for m, p, b in self.calls if m == "POST" and not p.endswith("/reactions")]

    def reactions(self):
        return [b["emoji"] for m, p, b in self.calls if m == "POST" and p.endswith("/reactions")]


@pytest.fixture
def secrets(adapter_mod, monkeypatch):
    values = {"CLICKCLACK_ALLOWED_USERS": ALICE}
    monkeypatch.setattr(adapter_mod, "_get_scoped_secret", lambda name, default="": values.get(name, default))
    return values


@pytest.fixture
def runner(monkeypatch):
    """No live gateway runner unless a test installs one in ``holder``."""
    holder = {"runner": None}
    stub = types.ModuleType("gateway.run")
    stub._gateway_runner_ref = lambda: holder["runner"]
    monkeypatch.setitem(sys.modules, "gateway.run", stub)
    return holder


@pytest.fixture
def cc(adapter_mod, monkeypatch, secrets, runner):
    from gateway.config import Platform, PlatformConfig
    try:
        Platform(adapter_mod.PLATFORM_NAME)
    except ValueError:
        Platform._add_pseudo_member(adapter_mod.PLATFORM_NAME)
    adapter = adapter_mod.ClickClackAdapter(
        PlatformConfig(enabled=True, token="ccb_test", extra={"url": "http://clickclack.invalid"}))
    adapter._bot_user_id = BOT
    adapter.fake = FakeAPI()
    monkeypatch.setattr(adapter, "_api", adapter.fake)
    return adapter


@pytest.fixture
def clarify():
    """Register a pending clarify; yields a factory and clears the session afterwards."""
    session = f"test:{uuid.uuid4().hex}"

    def make(choices, multi_select=False):
        cid = f"clr_{uuid.uuid4().hex}"
        return clarify_gateway.register(cid, session, "Which one?", choices, multi_select=multi_select)

    yield make
    clarify_gateway.clear_session(session)


def reaction(user_id, emoji, message_id="msg_q"):
    return {"type": "reaction.added", "payload": {"message_id": message_id, "emoji": emoji,
                                                   "user_id": user_id, "count": 1}}


def send(cc, entry, chat_id="dm_1", metadata=None):
    return asyncio.run(cc.send_clarify(chat_id, entry.question, entry.choices, entry.clarify_id,
                                       entry.session_key, metadata))


def test_single_select_resolved_by_reaction(cc, clarify):
    entry = clarify(["staging", "prod", "both"])
    result = send(cc, entry)
    assert result.success and result.message_id == "msg_q"
    [(path, body)] = cc.fake.posts()
    assert path == "dms/dm_1/messages"
    for choice in entry.choices:
        assert choice in body["body"]
    assert "React with the number" in body["body"]
    assert cc.fake.reactions() == KEYCAPS[:3]
    assert entry.awaiting_text  # typed answers still go through the text intercept

    asyncio.run(cc._handle_event(reaction(ALICE, KEYCAPS[1])))
    assert entry.event.is_set() and entry.response == "prod"
    assert "msg_q" not in cc._clarify_cards


def test_reaction_without_variation_selector_matches(cc, clarify):
    entry = clarify(["a", "b"])
    send(cc, entry)
    asyncio.run(cc._handle_event(reaction(ALICE, "2\u20e3")))
    assert entry.response == "b"


def test_own_reaction_ignored(cc, clarify):
    entry = clarify(["a", "b"])
    send(cc, entry)
    asyncio.run(cc._handle_event(reaction(BOT, KEYCAPS[0])))
    assert not entry.event.is_set()
    assert "msg_q" in cc._clarify_cards


def test_disallowed_and_unknown_users_ignored(cc, clarify):
    entry = clarify(["a", "b"])
    send(cc, entry)
    asyncio.run(cc._handle_event(reaction(MALLORY, KEYCAPS[0])))
    asyncio.run(cc._handle_event(reaction("", KEYCAPS[0])))
    asyncio.run(cc._handle_event(reaction(ALICE, "👍")))  # not one of the card's keycaps
    asyncio.run(cc._handle_event(reaction(ALICE, KEYCAPS[0], message_id="msg_other")))
    assert not entry.event.is_set()


def test_allow_all_users(cc, clarify, secrets):
    secrets["CLICKCLACK_ALLOW_ALL_USERS"] = "true"
    entry = clarify(["a", "b"])
    send(cc, entry)
    asyncio.run(cc._handle_event(reaction(MALLORY, KEYCAPS[0])))
    assert entry.response == "a"


def test_gateway_runner_authorization_wins(cc, clarify, runner):
    seen = []

    def deny(source):
        seen.append(source.user_id)
        return False

    runner["runner"] = types.SimpleNamespace(_is_user_authorized=deny)
    entry = clarify(["a", "b"])
    send(cc, entry)
    asyncio.run(cc._handle_event(reaction(ALICE, KEYCAPS[0])))  # in the env allow-list, but denied
    assert seen == [ALICE] and not entry.event.is_set()


def test_multi_select_falls_back_to_text(cc, clarify):
    entry = clarify(["a", "b", "c"], multi_select=True)
    result = send(cc, entry)
    assert result.success
    assert cc.fake.reactions() == []
    assert not cc._clarify_cards
    [(_, body)] = cc.fake.posts()
    assert "React with the number" not in body["body"]


def test_open_ended_is_plain_question(cc, clarify):
    entry = clarify(None)
    send(cc, entry)
    assert cc.fake.reactions() == []
    [(_, body)] = cc.fake.posts()
    assert "Which one?" in body["body"]


def test_more_than_nine_choices(cc, clarify):
    choices = [f"opt{n}" for n in range(1, 13)]
    entry = clarify(choices)
    send(cc, entry)
    assert cc.fake.reactions() == KEYCAPS
    [(_, body)] = cc.fake.posts()
    assert "opt12" in body["body"]
    asyncio.run(cc._handle_event(reaction(ALICE, KEYCAPS[8])))
    assert entry.response == "opt9"


def test_reaction_failure_falls_back_to_text(cc, clarify, caplog):
    cc.fake.fail_reactions = True
    caplog.set_level(logging.WARNING)
    first, second = clarify(["a", "b"]), clarify(["c", "d"])
    assert send(cc, first).success
    assert send(cc, second).success
    assert cc.fake.reactions() == [KEYCAPS[0], KEYCAPS[0]]  # stops after the first failure
    edits = [b["body"] for m, p, b in cc.fake.calls if m == "PATCH"]
    assert len(edits) == 2 and all("React with the number" not in e for e in edits)
    assert not cc._clarify_cards and first.awaiting_text
    warnings = [r for r in caplog.records if "cannot add reactions" in r.getMessage()]
    assert len(warnings) == 1
    asyncio.run(cc._handle_event(reaction(ALICE, KEYCAPS[0])))
    assert not first.event.is_set()


def test_question_posted_in_thread(cc, clarify):
    entry = clarify(["a", "b"])
    send(cc, entry, chat_id="chn_1", metadata={"thread_id": "msg_root"})
    [(path, _)] = cc.fake.posts()
    assert path == "messages/msg_root/thread/replies"


def test_retire_edits_and_removes_reactions(cc, clarify):
    entry = clarify(["a", "b"])
    send(cc, entry)
    asyncio.run(cc.retire_clarify_card(entry.clarify_id, "(expired)"))
    [patch] = [(p, b) for m, p, b in cc.fake.calls if m == "PATCH"]
    assert patch[0] == "messages/msg_q" and patch[1]["body"].endswith("(expired)")
    deletes = [p for m, p, b in cc.fake.calls if m == "DELETE"]
    assert deletes == ["messages/msg_q/reactions/1%EF%B8%8F%E2%83%A3",
                       "messages/msg_q/reactions/2%EF%B8%8F%E2%83%A3"]
    assert not cc._clarify_cards
    asyncio.run(cc.retire_clarify_card(entry.clarify_id, "(expired)"))  # unknown id: no-op


def test_retire_never_raises(cc, clarify):
    entry = clarify(["a", "b"])
    send(cc, entry)
    cc.fake.raise_on = "PATCH"
    asyncio.run(cc.retire_clarify_card(entry.clarify_id, "(expired)"))


def test_message_events_still_dispatched(cc, monkeypatch):
    seen = []

    async def handle_message(event):
        seen.append(event.text)

    async def api(method, path, payload=None, params=None, *, quiet=False):
        return {"message": {"id": "msg_in", "author_id": ALICE, "direct_conversation_id": "dm_1",
                            "body": "hello", "author": {"handle": "alice"}}}

    monkeypatch.setattr(cc, "_api", api)
    monkeypatch.setattr(cc, "handle_message", handle_message)
    asyncio.run(cc._handle_event({"type": "message.created",
                                  "payload": {"message_id": "msg_in", "author_id": ALICE}}))
    assert seen == ["hello"]
