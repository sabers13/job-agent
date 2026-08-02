"""The suite owns its network, the same way `conftest` owns its environment.

`tests/conftest.py` opens with "Offline and deterministic. No live network, no DB
container, no Playwright, no LLM." Until this module existed that sentence was a
docstring. `tests/test_suite_hermeticity.py` made three of the four executable —
environment variables, the database URL, the settings flags — and left the network
uncovered, because the leak CP1-7 exposed was a database and nobody generalised.

The cost of that gap, measured: `/search_stepstone` issued a real request to
`https://www.stepstone.de/en/` on every gate run, from two different tests, and both
passed either way. See CP-1-REVIEW.md §CP1-8.

Three design choices here are load-bearing.

**A refusal is recorded, not only raised, and the record is what fails the test.**
The route that leaked wraps its whole body in `except Exception: raise
HTTPException(500)` (`app/fastapi_run.py:397`), and 78 such handlers exist in this
repo (backlog A3). A guard that only raises is therefore absorbed by the code it is
meant to police, re-emitted as a 500, and — for the assertion that produced CP1-8 —
accepted. So every refusal is appended to a record that `conftest` checks at the end of
each test phase, where no `except` clause in `app/` can reach it. The same record is
what catches a connection attempted on a worker thread, which raises where nobody is
looking — and that is not hypothetical: asyncio resolves DNS in an executor thread, so
the CP1-8 leak is caught by the record rather than by the raise.

**`NetworkAccessAttempted` derives from `Exception`, not `BaseException`.** Deriving
from `BaseException` does defeat `except Exception:` — and it was tried here first.
Measured: it also escapes the anyio blocking portal that `TestClient` runs the ASGI
app in, killing the event-loop thread, so the client's `__exit__` then raises
`RuntimeError: This portal is not running`. Each leak became one red test plus one
teardown error naming anyio rather than the network. The record above makes the
stronger base class unnecessary, so the noisier option is not worth its cost.

**Loopback is allowed; anything else is refused.** What this closes is *egress* — a
gate whose result depends on whether the machine can reach the internet. Connections
to 127.0.0.1 / ::1 and AF_UNIX sockets pass through, for two reasons: a local database
is already pinned by `test_the_database_the_app_reaches_is_sqlite`, which asserts on
the engine `check_db` captured rather than on the socket it opens; and `-m external`
must stay runnable, where Playwright talks to its browser over a loopback port. The
boundary is recorded here so the next reader knows it was chosen, not overlooked.
"""

from __future__ import annotations

import ipaddress
import socket
import traceback
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[1]


class NetworkAccessAttempted(Exception):
    """The suite tried to open an outbound connection.

    If you are reading this in a traceback, a test reached the network. Stub the
    adapter the handler actually calls, and bind the stub with `raising=True` so it
    fails loudly when the symbol moves — `raising=False` is what let CP1-8 rot.
    """


@dataclass(frozen=True)
class Attempt:
    """One refused connection, with the first-party frames that led to it.

    `origin` is captured at refusal time on purpose. When a broad `except Exception:`
    swallows the raise, the traceback is gone by the time teardown reports the leak,
    and "something opened a socket" is not an actionable failure message.
    """

    target: str
    origin: str

    def __str__(self) -> str:
        return f"{self.target}\n{self.origin}" if self.origin else self.target


# `""` and `None` mean "this host" to the socket layer.
_LOCAL_NAMES = frozenset(
    {"", "localhost", "localhost.localdomain", "ip6-localhost", "ip6-loopback"}
)

_attempts: list[Attempt] = []


def attempts() -> tuple[Attempt, ...]:
    """Every outbound target refused since the last `reset()`, in order."""
    return tuple(_attempts)


def reset() -> None:
    """Clear the record. Called per phase, so a failure names that phase's own leak."""
    _attempts.clear()


def check_and_reset(phase: str) -> None:
    """Raise if anything was refused during `phase`. Always clears the record.

    Called by `conftest`'s per-phase hooks rather than by a fixture, so that a
    swallowed refusal fails the **call** phase. A teardown failure would leave the
    test counted as passed, and `pytest_passed` is the one `HIGHER_IS_BETTER` key in
    `ci/baseline.json` — a suite that leaks must move that number, not sit beside it.
    """
    leaked = attempts()
    reset()
    if leaked:
        detail = "\n".join(f"  {attempt}" for attempt in leaked)
        raise NetworkAccessAttempted(
            f"{len(leaked)} outbound connection(s) attempted during {phase} and the "
            f"refusal was swallowed -- the suite is offline:\n{detail}\n"
            f"Stub the adapter the handler calls, with raising=True."
        )


@contextmanager
def refuses_connection(match: str) -> Generator[None]:
    """Assert the guard refuses the enclosed block, and clear the record afterwards.

    For `tests/test_suite_hermeticity.py`, which trips the guard on purpose. The record
    has to be cleared *inside the test*: `conftest`'s check runs at the end of the call
    phase, before any fixture finalizer, so a fixture cannot clean up in time and the
    deliberate trip would be reported as a leak.
    """
    with pytest.raises(NetworkAccessAttempted, match=match):
        yield
    reset()


def _hostname(host: object) -> str:
    """Render a host as text. `getaddrinfo` is routinely handed bytes."""
    return host.decode("utf-8", "replace") if isinstance(host, bytes) else str(host)


def _is_local(host: object) -> bool:
    """True for loopback and for the socket layer's "this host" spellings.

    Fails closed: anything unrecognised — a hostname, an unparseable literal — is
    treated as remote, because a name is only resolved by leaving the machine.
    """
    if host is None:
        return True
    text = _hostname(host)
    if text.lower() in _LOCAL_NAMES:
        return True
    try:
        # Strip an IPv6 zone id (`fe80::1%eth0`); `ip_address` rejects it.
        return ipaddress.ip_address(text.split("%", 1)[0]).is_loopback
    except ValueError:
        return False


def _describe(address: object) -> str:
    if isinstance(address, tuple) and len(address) >= 2:
        return f"{_hostname(address[0])}:{address[1]}"
    return _hostname(address)


def _origin() -> str:
    """The repo's own frames from the current stack, innermost last.

    Third-party frames are dropped: `httpx` and `anyio` are always in the stack and
    never the answer. What is wanted is the `app/` or `tests/` line that started it.
    """
    frames = [
        f
        for f in traceback.extract_stack()
        if f.filename.startswith(str(ROOT)) and ".venv" not in f.filename and f.filename != __file__
    ]
    return "\n".join(
        f"    {Path(f.filename).relative_to(ROOT)}:{f.lineno} in {f.name}: {(f.line or '').strip()}"
        for f in frames[-6:]
    )


def _refuse(what: str, target: str) -> None:
    _attempts.append(Attempt(target=target, origin=_origin()))
    raise NetworkAccessAttempted(
        f"{what} to {target} from the offline suite. tests/conftest.py: 'No live "
        f"network, no DB container, no Playwright, no LLM.' Stub the adapter the "
        f"handler calls -- see tests/net_guard.py and CP-1-REVIEW.md §CP1-8."
    )


def _check_address(family: int, address: object) -> None:
    if family not in (socket.AF_INET, socket.AF_INET6):
        return  # AF_UNIX and friends never leave the machine.
    host = address[0] if isinstance(address, tuple) and address else address
    if _is_local(host):
        return
    _refuse("outbound connection", _describe(address))


def install(monkeypatch: pytest.MonkeyPatch) -> None:
    """Refuse outbound connections for the lifetime of `monkeypatch`.

    Both `connect` and `connect_ex` are wrapped: `create_connection`, `httpx` and
    asyncio's `sock_connect` all bottom out in one of the two. `getaddrinfo` is
    wrapped as well — a DNS lookup for a remote name has already left the machine
    before any `connect` happens, and refusing it names the host rather than an IP.
    """
    real_connect = socket.socket.connect
    real_connect_ex = socket.socket.connect_ex
    real_getaddrinfo = socket.getaddrinfo

    def guarded_connect(self: socket.socket, address: Any) -> Any:
        _check_address(self.family, address)
        return real_connect(self, address)

    def guarded_connect_ex(self: socket.socket, address: Any) -> Any:
        _check_address(self.family, address)
        return real_connect_ex(self, address)

    def guarded_getaddrinfo(host: Any, port: Any, *args: Any, **kwargs: Any) -> Any:
        if not _is_local(host):
            _refuse("DNS resolution", _hostname(host))
        return real_getaddrinfo(host, port, *args, **kwargs)

    monkeypatch.setattr(socket.socket, "connect", guarded_connect)
    monkeypatch.setattr(socket.socket, "connect_ex", guarded_connect_ex)
    monkeypatch.setattr(socket, "getaddrinfo", guarded_getaddrinfo)
