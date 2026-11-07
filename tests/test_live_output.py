"""Live background-process output (CLICKCLACK_LIVE_OUTPUT); no network, the registry is faked."""

import asyncio
import sys
import time
import types

import pytest


class FakeRegistry:
    def __init__(self):
        self._running = {}
        self.fail = False

    def count_running(self):
        if self.fail:
            raise RuntimeError("registry exploded")
        return len(self._running)


def proc(pid, command="make test", chat_id="dm_1", thread_id="", platform="clickclack", output="",
         started_ago=5.0):
    return types.SimpleNamespace(
        id=pid, command=command, started_at=time.time() - started_ago, exited=False, exited_at=0.0,
        exit_code=None, completion_reason="exited", output_buffer=output, watcher_platform=platform,
        watcher_chat_id=chat_id, watcher_thread_id=thread_id)


def finish(session, code, reason="exited"):
    session.exited, session.exit_code, session.completion_reason = True, code, reason
    session.exited_at = session.started_at + 65


class FakeAPI:
    def __init__(self):
        self.calls = []
        self.next_id = 0
        self.fail_posts = False

    async def __call__(self, method, path, payload=None, params=None, *, quiet=False):
        self.calls.append((method, path, payload))
        if method == "POST":
            if self.fail_posts:
                return {}
            self.next_id += 1
            return {"message": {"id": f"msg_{self.next_id}"}}
        if method == "GET":  # thread root lookup: the id is already a root
            return {"message": {"id": path.rsplit("/", 1)[-1]}}
        return {"message": {"id": path.rsplit("/", 1)[-1]}}

    def posts(self):
        return [(p, b["body"]) for m, p, b in self.calls if m == "POST"]

    def edits(self):
        return [(p, b["body"]) for m, p, b in self.calls if m == "PATCH"]


@pytest.fixture
def registry(monkeypatch):
    import tools  # the real package, so the fake submodule resolves under it
    reg = FakeRegistry()
    mod = types.ModuleType("tools.process_registry")
    mod.process_registry = reg
    monkeypatch.setitem(sys.modules, "tools.process_registry", mod)
    monkeypatch.setattr(tools, "process_registry", mod, raising=False)
    return reg


def make_adapter(adapter_mod, monkeypatch, live="on"):
    from gateway.config import Platform, PlatformConfig
    try:
        Platform(adapter_mod.PLATFORM_NAME)
    except ValueError:
        Platform._add_pseudo_member(adapter_mod.PLATFORM_NAME)
    monkeypatch.delenv("CLICKCLACK_LIVE_OUTPUT", raising=False)
    extra = {"url": "http://clickclack.invalid"}
    if live is not None:
        extra["live_output"] = live
    adapter = adapter_mod.ClickClackAdapter(PlatformConfig(enabled=True, token="ccb_test", extra=extra))
    adapter._bot_user_id = "usr_bot"
    adapter.fake = FakeAPI()
    monkeypatch.setattr(adapter, "_api", adapter.fake)
    adapter._turn_ids = {"dm_1": "turn_a", "chn_1": "turn_b"}  # chats this adapter has served
    return adapter


@pytest.fixture
def cc(adapter_mod, monkeypatch, registry):
    return make_adapter(adapter_mod, monkeypatch)


def tick(cc, now):
    asyncio.run(cc._live_tick(now))


# --- feature flag / task lifecycle ---

async def _noop():
    return None


def _connect_disconnect(cc, monkeypatch):
    async def api(method, path, payload=None, params=None, *, quiet=False):
        if path == "me":
            return {"user": {"id": "usr_bot", "handle": "bot"}}
        if path == "workspaces":
            return {"workspaces": [{"id": "wsp_1"}]}
        return {}

    monkeypatch.setattr(cc, "_api", api)
    monkeypatch.setattr(cc, "_ws_loop", _noop)
    monkeypatch.setattr(cc, "_register_command_menu", _noop)
    seen = {}

    async def run():
        assert await cc.connect()
        seen["task"] = cc._live_task
        await asyncio.sleep(0)
        await cc.disconnect()

    asyncio.run(run())
    return seen["task"]


@pytest.mark.parametrize("live", [None, "off", "false", ""])
def test_flag_off_starts_no_task(adapter_mod, monkeypatch, registry, live):
    cc = make_adapter(adapter_mod, monkeypatch, live=live)
    assert cc._live_output is False
    assert _connect_disconnect(cc, monkeypatch) is None
    assert cc._live_task is None


def test_flag_from_env(adapter_mod, monkeypatch, registry):
    cc = make_adapter(adapter_mod, monkeypatch, live=None)
    monkeypatch.setenv("CLICKCLACK_LIVE_OUTPUT", "on")
    cc2 = adapter_mod.ClickClackAdapter(cc.config)
    assert cc2._live_output is True


def test_flag_on_task_started_and_cancelled(cc, monkeypatch):
    task = _connect_disconnect(cc, monkeypatch)
    assert task is not None and task.done()
    assert cc._live_task is None


# --- lifecycle ---

def test_lifecycle_exit_zero(cc, registry):
    s = proc("proc_a", output="compiling\n")
    registry._running[s.id] = s
    tick(cc, 0.0)
    [(path, body)] = cc.fake.posts()
    assert path == "dms/dm_1/messages"
    assert body.startswith("⏳ `make test` · ") and "```\ncompiling\n```" in body

    s.output_buffer += "linking\n"
    tick(cc, 3.5)
    s.output_buffer += "testing\n"
    tick(cc, 7.0)
    edits = cc.fake.edits()
    assert [p for p, _ in edits] == ["messages/msg_1", "messages/msg_1"]
    assert edits[0][1].endswith("compiling\nlinking\n```")
    assert edits[1][1].endswith("linking\ntesting\n```")

    finish(s, 0)
    del registry._running[s.id]
    tick(cc, 10.5)
    final = cc.fake.edits()[-1][1]
    assert final.startswith("✅ `make test` · done in 1m 5s (exit 0)")
    assert "testing" in final
    assert not cc._live_procs
    tick(cc, 20.0)  # forgotten: nothing more
    assert len(cc.fake.edits()) == 3 and len(cc.fake.posts()) == 1


def test_lifecycle_nonzero_exit(cc, registry):
    s = proc("proc_b", command="pytest", output="FAILED test_x\n")
    registry._running[s.id] = s
    tick(cc, 0.0)
    finish(s, 2)
    del registry._running[s.id]
    tick(cc, 1.0)  # final edit waits for the edit interval too
    assert not cc.fake.edits()
    tick(cc, 3.0)
    [(_, final)] = cc.fake.edits()
    assert final.startswith("❌ `pytest` · exit 2 after 1m 5s") and "FAILED test_x" in final


def test_killed_reason_shown(cc, registry):
    s = proc("proc_k")
    registry._running[s.id] = s
    tick(cc, 0.0)
    assert "_(no output yet)_" in cc.fake.posts()[0][1]
    finish(s, -15, reason="killed")
    del registry._running[s.id]
    tick(cc, 5.0)
    assert cc.fake.edits()[-1][1].startswith("❌ `make test` · exit -15 after 1m 5s (killed)")


def test_posted_in_thread(cc, registry):
    s = proc("proc_t", chat_id="chn_1", thread_id="msg_root")
    registry._running[s.id] = s
    tick(cc, 0.0)
    [(path, _)] = cc.fake.posts()
    assert path == "messages/msg_root/thread/replies"


# --- throttling ---

def test_throttle_and_unchanged_content(cc, registry):
    s = proc("proc_c", output="a\n")
    registry._running[s.id] = s
    tick(cc, 0.0)
    s.output_buffer += "b\n"
    tick(cc, 1.0)
    tick(cc, 2.9)
    assert not cc.fake.edits()  # changed, but within 3 s of the post
    tick(cc, 3.0)
    assert len(cc.fake.edits()) == 1
    tick(cc, 10.0)
    tick(cc, 20.0)
    assert len(cc.fake.edits()) == 1  # nothing new: no edit even though elapsed advanced


def test_edit_cap_keeps_final(cc, registry, adapter_mod, monkeypatch):
    monkeypatch.setattr(adapter_mod, "_LIVE_MAX_EDITS", 3)
    s = proc("proc_cap")
    registry._running[s.id] = s
    tick(cc, 0.0)
    for i in range(1, 10):
        s.output_buffer += f"line {i}\n"
        tick(cc, i * 3.0)
    assert len(cc.fake.edits()) == 2  # cap - 1 live edits
    finish(s, 0)
    del registry._running[s.id]
    tick(cc, 100.0)
    edits = cc.fake.edits()
    assert len(edits) == 3 and edits[-1][1].startswith("✅") and "line 9" in edits[-1][1]


# --- rendering ---

def test_ansi_and_control_stripped(adapter_mod):
    raw = ("\x1b[1;32mPASS\x1b[0m test_a\n"
           "\x1b]0;window title\x07plain\n"
           "progress 10%\rprogress 50%\rprogress 100%\n"
           "bell\x07 and \x1b[2Kcleared\r\n")
    assert adapter_mod._live_tail(raw) == "PASS test_a\nplain\nprogress 100%\nbell and cleared"


def test_tail_truncation(adapter_mod):
    raw = "".join(f"line {i}\n" for i in range(100))
    tail = adapter_mod._live_tail(raw)
    assert tail.splitlines() == [f"line {i}" for i in range(88, 100)]

    long_line = "x" * 5000
    tail = adapter_mod._live_tail(long_line + "\n")
    assert len(tail) == adapter_mod._LIVE_LINE_CHARS and tail.endswith("…")

    wide = "".join("y" * 190 + f"{i:02d}\n" for i in range(12))
    tail = adapter_mod._live_tail(wide)
    assert len(tail) <= adapter_mod._LIVE_TAIL_CHARS
    assert tail.splitlines()[-1].endswith("11")


def test_body_within_limits_and_fence_safe(cc, registry, adapter_mod):
    s = proc("proc_f", command="bash -c '" + "z" * 200 + "'", output="has ``` fence\n" + "w" * 9000)
    registry._running[s.id] = s
    tick(cc, 0.0)
    [(_, body)] = cc.fake.posts()
    assert len(body) <= adapter_mod.MAX_MESSAGE_LENGTH
    head = body.splitlines()[0]
    cmd = head.split("`")[1]
    assert len(cmd) == adapter_mod._LIVE_CMD_CHARS and cmd.endswith("…")


def test_fence_longer_than_backtick_runs(adapter_mod):
    body = adapter_mod._live_body("h", "a ```` b", "x")
    assert body.splitlines()[1] == "`````" and body.endswith("\n`````")


# --- ownership ---

def test_unowned_processes_ignored(cc, registry):
    others = [
        proc("proc_tg", platform="telegram", chat_id="12345"),
        proc("proc_unseen", chat_id="dm_someone_else"),  # ClickClack, but not a chat this adapter served
        proc("proc_nochat", chat_id=""),
        proc("proc_silent", platform="", chat_id="", started_ago=60.0),  # no routing ever stamped
    ]
    for s in others:
        registry._running[s.id] = s
    tick(cc, 0.0)
    tick(cc, 5.0)
    assert not cc.fake.calls
    assert cc._live_ignored == {s.id for s in others}


def test_routing_stamped_after_spawn_is_picked_up(cc, registry):
    s = proc("proc_late", platform="", chat_id="", started_ago=0.5)
    registry._running[s.id] = s
    tick(cc, 0.0)
    assert not cc.fake.calls and s.id not in cc._live_ignored
    s.watcher_platform, s.watcher_chat_id = "clickclack", "dm_1"
    tick(cc, 2.0)
    assert len(cc.fake.posts()) == 1


def test_ignored_ids_pruned_when_gone(cc, registry):
    s = proc("proc_tg", platform="telegram")
    registry._running[s.id] = s
    tick(cc, 0.0)
    assert cc._live_ignored == {"proc_tg"}
    del registry._running[s.id]
    tick(cc, 2.0)
    assert not cc._live_ignored


# --- robustness ---

def test_registry_errors_swallowed(cc, registry, monkeypatch):
    registry.fail = True
    sleeps = []

    async def fake_sleep(seconds):
        sleeps.append(seconds)
        if len(sleeps) >= 3:
            cc._closing = True

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    asyncio.run(cc._live_loop())  # returns normally despite every tick raising
    assert sleeps == [2.0, 2.0, 2.0]


def test_update_errors_swallowed(cc, registry, monkeypatch):
    s = proc("proc_e", output="x\n")
    registry._running[s.id] = s
    tick(cc, 0.0)

    async def boom(*a, **kw):
        raise RuntimeError("edit failed")

    monkeypatch.setattr(cc, "edit_message", boom)
    s.output_buffer += "y\n"
    tick(cc, 4.0)  # must not raise


def test_missing_registry_module_swallowed(cc, monkeypatch):
    monkeypatch.setitem(sys.modules, "tools.process_registry", None)  # import raises ImportError
    calls = []

    async def fake_sleep(seconds):
        calls.append(seconds)
        cc._closing = True

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    asyncio.run(cc._live_loop())
    assert calls == [2.0]


def test_failed_posts_give_up(cc, registry):
    cc.fake.fail_posts = True
    s = proc("proc_p")
    registry._running[s.id] = s
    for i in range(6):
        tick(cc, i * 2.0)
    assert len(cc.fake.posts()) == 3
    assert s.id in cc._live_ignored and not cc._live_procs


def test_tracked_cap(cc, registry, adapter_mod):
    for i in range(7):
        s = proc(f"proc_{i}")
        registry._running[s.id] = s
    tick(cc, 0.0)
    assert len(cc._live_procs) == adapter_mod._LIVE_MAX_TRACKED
    assert len(cc.fake.posts()) == adapter_mod._LIVE_MAX_TRACKED
    # a slot frees up once the finished process is finalized; a waiting one is adopted next tick
    first = registry._running.pop("proc_0")
    finish(first, 0)
    tick(cc, 4.0)
    assert "proc_0" not in cc._live_procs and len(cc.fake.posts()) == adapter_mod._LIVE_MAX_TRACKED
    tick(cc, 6.0)
    assert len(cc._live_procs) == adapter_mod._LIVE_MAX_TRACKED
    assert len(cc.fake.posts()) == adapter_mod._LIVE_MAX_TRACKED + 1
