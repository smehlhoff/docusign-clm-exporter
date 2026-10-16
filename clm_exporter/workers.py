"""Bounded, connection-affine network workers; all event handlers run in the owner.

Only the coordinator submits tasks (at most one per slot). Workers may send small
acknowledged events, never payload bytes or database calls. Shutdown answers or
cancels every pending event before joining, including producers on a full queue.
"""
import logging
import queue
import threading
from dataclasses import dataclass, field

from .errors import FatalError
from .remote import Remote

LOG = logging.getLogger("clm_exporter")


class Cancelled(Exception):
    pass


@dataclass
class Event:
    slot: int
    kind: str
    value: object = None
    error: BaseException | None = None
    reply: queue.Queue = field(default_factory=lambda: queue.Queue(maxsize=1))


class Workers:
    def __init__(self, cfg, state, factory=Remote):
        self.cfg, self.state = cfg, state
        self.events = queue.Queue(maxsize=max(2, cfg.workers * 2))
        self.cancelled = threading.Event()
        self.slots = []
        self.reconnections = 0
        self.connection_lock = threading.Lock()
        # Key loading (including an encrypted-key prompt) belongs to the caller.
        prototype = Remote(cfg) if factory is Remote else None
        self.factory = ((lambda: Remote(cfg, key=prototype.key)) if prototype else
                        (lambda: factory(cfg, None)))

    def _put(self, event):
        while not self.cancelled.is_set():
            try:
                self.events.put(event, timeout=0.1)
                return
            except queue.Full:
                pass
        raise Cancelled()

    def _emit(self, slot, kind, value=None):
        event = Event(slot, kind, value)
        self._put(event)
        while not self.cancelled.is_set():
            try:
                result = event.reply.get(timeout=0.1)
                if isinstance(result, BaseException):
                    raise result
                return result
            except queue.Empty:
                pass
        raise Cancelled()

    def _connect(self, slot, remote, exc=None):
        # Admission and recovery budget are centralized; serialize establishments
        # so permanent authentication failures cannot fan out across the pool.
        while not self.connection_lock.acquire(timeout=0.1):
            if self.cancelled.is_set():
                raise Cancelled()
        try:
            if self.cancelled.is_set():
                raise Cancelled()
            if exc is not None:
                self._emit(slot, "reconnect", exc)
                remote.close()
            try:
                fingerprint = remote.connect()
                self._emit(slot, "connected", fingerprint)
            except FatalError as failure:
                # The terminal result is delivered before shutdown cancels peers.
                self._emit(slot, "fatal_connection", failure)
                raise
        finally:
            self.connection_lock.release()

    def _worker(self, slot, inbox):
        remote = None
        initialized = False
        try:
            remote = self.factory()
            if isinstance(remote, Remote):
                remote.retry = lambda exc: self._emit(slot, "reconnect", exc)
            self._connect(slot, remote)
            initialized = True
            pool = self

            class Session:
                def __getattr__(self, name):
                    return getattr(remote, name)

                def reconnect(self, exc):
                    check = getattr(remote, "is_connection_error", None)
                    broken = check(exc) if check else isinstance(exc, (ConnectionError, EOFError, TimeoutError))
                    if broken:
                        pool._connect(slot, remote, exc)

            session = Session()
            while not self.cancelled.is_set():
                try:
                    task = inbox.get(timeout=0.1)
                except queue.Empty:
                    continue
                try:
                    result = task(session, lambda kind, value=None: self._emit(slot, kind, value))
                    self._put(Event(slot, "done", result))
                except Cancelled:
                    break
                except BaseException as exc:  # noqa: BLE001 - Deliver worker failures to the owner.
                    self._put(Event(slot, "done", error=exc))
        except Cancelled:
            pass
        except BaseException as exc:  # noqa: BLE001 - Includes interruption during connection setup.
            # A slot that could not initialize has exited; never present its
            # failure as a retryable task error and queue work to a dead thread.
            if not initialized and isinstance(exc, Exception) and not isinstance(exc, FatalError):
                exc = FatalError(f"Cannot initialize SFTP worker: {exc}")
            try:
                self._put(Event(slot, "done", error=exc))
            except Cancelled:
                pass
        finally:
            if remote is not None:
                try:
                    remote.close()
                except Exception:
                    LOG.warning("Worker connection close failed", exc_info=True)

    def submit(self, slot, task):
        if self.cancelled.is_set():
            raise Cancelled()
        if slot == len(self.slots):
            if slot >= self.cfg.workers:
                raise RuntimeError("Worker ceiling exceeded")
            inbox = queue.Queue(maxsize=1)
            thread = threading.Thread(target=self._worker, args=(slot, inbox), name=f"clm-sftp-{slot}")
            try:
                thread.start()
            except RuntimeError as exc:
                raise FatalError(f"Cannot start SFTP worker {slot + 1}: {exc}") from exc
            self.slots.append((inbox, thread))
        self.slots[slot][0].put_nowait(task)

    def receive(self, handler):
        """Apply nonterminal events; return one terminal task result."""
        while True:
            event = self._wait_event()
            if event.kind == "done":
                return event
            try:
                if event.kind == "connected":
                    if event.value:
                        with self.state.transaction():
                            self.state.db.execute("UPDATE source SET host_key_fingerprint=? WHERE id=1", (event.value,))
                    result = None
                elif event.kind == "reconnect":
                    self.reconnections += 1
                    if self.reconnections > self.cfg.max_attempts:
                        raise FatalError("Run aborted: SFTP reconnection budget exhausted")
                    LOG.info("SFTP reconnection %d/%d", self.reconnections, self.cfg.max_attempts)
                    result = None
                elif event.kind == "fatal_connection":
                    # Acknowledge only this worker; the establishment lock stays
                    # held until the coordinator has stopped new admissions.
                    raise event.value
                else:
                    result = handler(event)
                event.reply.put_nowait(result)
            except BaseException:
                # Stop admission before acknowledging the failing worker: its
                # acknowledgement releases the connection establishment lock.
                self.cancelled.set()
                event.reply.put_nowait(Cancelled())
                raise

    def _wait_event(self):
        while True:
            try:
                return self.events.get(timeout=0.1)
            except queue.Empty:
                if self.cancelled.is_set():
                    raise Cancelled()

    def close(self):
        self.cancelled.set()
        # Timed transport operations bound network shutdown. Queue producers and
        # acknowledgement waiters check cancellation independently of the owner.
        while any(thread.is_alive() for _, thread in self.slots):
            try:
                event = self.events.get(timeout=0.05)
                if event.kind != "done":
                    event.reply.put_nowait(Cancelled())
            except queue.Empty:
                pass
            for _, thread in self.slots:
                thread.join(timeout=0.01)
