"""Deterministic concurrency, capacity, recovery and connection-ownership tests."""
import errno
import io
import os
import queue
import sqlite3
import tempfile
import threading
import unittest
from contextlib import redirect_stderr
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from clm_exporter.cli import execute
from clm_exporter.config import Config
from clm_exporter.errors import CapacityStop, FatalError
from clm_exporter.local import Reservations, prepare_layout, regular_open
from clm_exporter.state import State
from clm_exporter.workers import Cancelled, Workers

from .fakes import FakeRemote


class Sessions:
    """Independent clients over one fixture, with ownership and ceiling audits."""
    def __init__(self):
        self.source = FakeRemote()
        self.lock = threading.Lock()
        self.active = self.peak = self.connections = 0
        self.clients = []
        self.listing = self.reading = None
        self.connect_error = None

    def factory(self, cfg, state):
        assert state is None
        owner = self

        class Client(FakeRemote):
            def __init__(self):
                super().__init__()
                self.entries = owner.source.entries
                self.denied = owner.source.denied
                self.reported_modes = owner.source.reported_modes
                self.list_missing_attrs = owner.source.list_missing_attrs
                self.connected = False
                self.thread = threading.get_ident()
                owner.clients.append(self)

            def check(self):
                assert self.thread == threading.get_ident(), "Client crossed worker threads"

            def connect(self):
                self.check()
                with owner.lock:
                    owner.connections += 1
                    owner.active += 1
                    owner.peak = max(owner.peak, owner.active)
                    self.connected = True
                if owner.connect_error:
                    raise owner.connect_error
                return "SHA256:fixture"

            def close(self):
                self.check()
                with owner.lock:
                    if self.connected:
                        owner.active -= 1
                        self.connected = False

            def lstat(self, relative):
                self.check()
                if owner.source.stat_hook:
                    owner.source.stat_hook(relative)
                return super().lstat(relative)

            def iterdir(self, relative):
                self.check()
                if owner.listing:
                    yield from owner.listing(self, relative)
                else:
                    yield from super().iterdir(relative)

            def open(self, relative):
                self.check()
                if owner.source.open_hook:
                    owner.source.open_hook(relative)
                self.read_hook = owner.reading
                with owner.lock:
                    owner.source.opens.append(relative)
                return super().open(relative)

        return Client()


class ConcurrentTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.cfg = Config("test", "user", self.root / "key", self.root / "hosts", "/",
                          self.root / "export", workers=2, min_free_bytes=0,
                          max_staged_bytes=1000, chunk_bytes=2)
        self.sessions = Sessions()
        self.invoke("init")

    def invoke(self, command="download", **kwargs):
        owner = threading.get_ident()
        transaction = State.transaction

        def checked(state):
            self.assertEqual(threading.get_ident(), owner, "SQLite accessed by worker")
            return transaction(state)

        with patch.object(State, "transaction", checked), redirect_stderr(io.StringIO()):
            result = execute(self.cfg, command, remote_factory=self.sessions.factory,
                             sleep=lambda _: None, progress=False, **kwargs)
        self.assertEqual(self.sessions.active, 0)
        self.assertLessEqual(self.sessions.peak, self.cfg.workers)
        self.assertFalse(any(t.name.startswith("clm-sftp-") for t in threading.enumerate()))
        return result

    def rows(self, sql):
        with sqlite3.connect(self.cfg.database) as db:
            return db.execute(sql).fetchall()

    def test_barriers_prove_overlapping_listings_and_payloads(self):
        for directory in ("a", "b"):
            self.sessions.source.add(directory + "/x.pdf", b"abcd")
            self.sessions.source.add(directory + "/empty", kind="directory")
        listing = threading.Barrier(2, timeout=5)
        reading = threading.Barrier(2, timeout=5)

        def list_directory(client, relative):
            if relative in ("a", "b"):
                listing.wait()
            yield from FakeRemote.iterdir(client, relative)

        def read(relative, offset):
            if offset == 0:
                reading.wait()

        self.sessions.listing, self.sessions.reading = list_directory, read
        code, summary = self.invoke()
        self.assertEqual(code, 0)
        self.assertEqual(summary["new_successes"], 2)
        self.assertEqual(summary["directories_discovered"], 4)
        self.assertEqual(self.sessions.peak, 2)
        self.assertEqual(self.sessions.connections, 2)  # reused between phases
        self.assertEqual(self.rows("SELECT host_key_fingerprint FROM source"), [("SHA256:fixture",)])

    def test_partial_listing_retry_retains_observations_and_aliases(self):
        for name in ("A.pdf", "a.pdf", "a~2.pdf", "é.pdf", "é.pdf"):
            self.sessions.source.add("parent/" + name)
        attempts = 0

        def listing(client, relative):
            nonlocal attempts
            entries = list(FakeRemote.iterdir(client, relative))
            if relative == "parent":
                attempts += 1
                if attempts == 1:
                    yield entries[0]
                    raise ConnectionError("partial")
                entries.reverse()
            yield from entries

        self.sessions.listing = listing
        code, summary = self.invoke("inventory")
        self.assertEqual(code, 0)
        self.assertEqual(summary["scan_status"], "complete")
        self.assertEqual(len(self.rows("SELECT * FROM observations")), 6)
        before = self.rows("SELECT remote,local FROM paths ORDER BY remote")
        self.assertEqual(self.invoke("inventory")[0], 0)
        self.assertEqual(before, self.rows("SELECT remote,local FROM paths ORDER BY remote"))

    def test_successful_listing_retry_clears_transient_entry_failure(self):
        self.sessions.source.add("a.pdf")
        self.sessions.source.list_missing_attrs.add("a.pdf")
        attempts = 0

        def listing(client, directory):
            nonlocal attempts
            attempts += 1
            yield from FakeRemote.iterdir(client, directory)
            if attempts == 1:
                raise ConnectionError("listing interrupted after metadata lookup")

        def stat(relative):
            if relative == "a.pdf" and attempts == 1:
                raise TimeoutError("transient metadata failure")

        self.sessions.listing = listing
        self.sessions.source.stat_hook = stat
        code, summary = self.invoke()
        self.assertEqual(code, 0)
        self.assertEqual(summary["scan_status"], "complete")
        self.assertTrue(summary["local_download_complete"])
        self.assertEqual(summary["new_successes"], 1)
        self.assertEqual(self.rows("SELECT * FROM errors WHERE resolved_at IS NULL"), [])

    def test_queue_scheduling_scales_without_rescanning_finished_work(self):
        # Count SQLite VM work instead of wall time, which depends on the host.
        # Exercise real scans/downloads, including upgrading an existing database.
        initialize = State.__init__
        for command in ("inventory", "download"):
            with self.subTest(command=command):
                self.cfg = replace(self.cfg, working_directory=self.root / command)
                self.sessions = Sessions()
                self.invoke("init")
                with sqlite3.connect(self.cfg.database) as db:
                    db.execute("DROP INDEX IF EXISTS scans_pending")
                    db.execute("DROP INDEX IF EXISTS paths_schedule")
                for number in range(500):
                    if command == "inventory":
                        self.sessions.source.add(f"dir-{number:05}", kind="directory")
                    else:
                        self.sessions.source.add(f"{number:05}.pdf", b"")
                steps = 0

                def tick():
                    nonlocal steps
                    steps += 1000
                    return 0

                def measure(state, *args, **kwargs):
                    initialize(state, *args, **kwargs)
                    state.db.set_progress_handler(tick, 1000)

                with patch.object(State, "__init__", measure):
                    code, summary = self.invoke(command)
                self.assertEqual(code, 0)
                if command == "inventory":
                    self.assertEqual(summary["directories_discovered"], 500)
                else:
                    self.assertEqual(summary["new_successes"], 500)
                budget = 500_000 if command == "inventory" else 1_200_000
                self.assertLess(steps, budget, f"{command} rescans its scheduling queue")

    def test_capacity_waits_for_old_version_promotion(self):
        self.sessions.source.add("a.pdf", b"1234")
        self.assertEqual(self.invoke()[0], 0)
        self.cfg = replace(self.cfg, max_staged_bytes=8)
        self.sessions.source.add("a.pdf", b"5678", mtime=2)
        self.sessions.source.add("b.pdf", b"abcd")
        code, summary = self.invoke()
        self.assertEqual(code, 0)
        self.assertEqual(summary["new_successes"], 1)
        self.assertEqual(summary["changed_or_repeated_successes"], 1)
        self.assertEqual((self.cfg.export / "a.pdf").read_bytes(), b"5678")
        self.assertEqual((self.cfg.export / "b.pdf").read_bytes(), b"abcd")

    def test_capacity_defers_without_oversubscription(self):
        self.cfg = replace(self.cfg, max_staged_bytes=6)
        for name in ("a.pdf", "b.pdf"):
            self.sessions.source.add(name, b"1234")
        code, summary = self.invoke()
        self.assertEqual(code, 0)
        self.assertEqual(summary["stop_reason"], "batch_capacity")
        self.assertEqual(summary["new_successes"], 1)
        self.assertEqual(len(self.sessions.source.opens), 1)
        self.assertEqual(sum(p.stat().st_size for p in self.cfg.export.iterdir()), 4)

    def test_interrupt_preserves_ready_payload_and_other_jobs_recover(self):
        for name in ("a.pdf", "b.pdf"):
            self.sessions.source.add(name, b"1234")

        def interrupt(event, job):
            if event == "ready":
                raise KeyboardInterrupt

        code, summary = self.invoke(checkpoint=interrupt)
        self.assertEqual(code, 130)
        self.assertIn(("ready",), self.rows("SELECT state FROM transfers"))
        self.assertTrue(Path(summary["reports"]["backup"]).exists())
        self.assertEqual(self.invoke()[0], 0)
        self.assertEqual(self.rows("SELECT state FROM transfers"), [("downloaded",), ("downloaded",)])

    def test_chunk_interrupt_with_multiple_writers_is_recoverable(self):
        barrier = threading.Barrier(2, timeout=5)
        for name in ("a.pdf", "b.pdf"):
            self.sessions.source.add(name, b"1234")
        self.sessions.reading = lambda path, offset: barrier.wait() if offset == 0 else None

        def interrupt(event, job):
            if event == "chunk":
                raise KeyboardInterrupt

        self.assertEqual(self.invoke(checkpoint=interrupt)[0], 130)
        self.sessions.reading = None
        self.assertEqual(self.invoke()[0], 0)
        self.assertEqual(self.rows("SELECT state FROM transfers"), [("downloaded",), ("downloaded",)])

    def test_shared_reconnection_budget_and_fatal_auth(self):
        for name in ("a.pdf", "b.pdf", "c.pdf", "d.pdf"):
            self.sessions.source.add(name)
        self.sessions.source.open_hook = lambda _: (_ for _ in ()).throw(ConnectionError("outage"))
        code, _ = self.invoke()
        self.assertEqual(code, 1)
        self.assertLessEqual(self.sessions.connections, self.cfg.workers + self.cfg.max_attempts)
        self.sessions.source.open_hook = None
        self.sessions.connect_error = FatalError("untrusted host")
        before = self.sessions.connections
        self.assertEqual(self.invoke()[0], 1)
        self.assertEqual(self.sessions.connections - before, 1)

    def test_failed_worker_initialization_is_fatal_and_reported(self):
        self.sessions.connect_error = OSError("session initialization failed")
        code, summary = self.invoke("inventory")
        self.assertEqual(code, 1)
        self.assertEqual(summary["stop_reason"], "fatal_error")
        self.assertTrue(Path(summary["reports"]["backup"]).exists())

    def test_fatal_connection_cancels_waiting_workers_before_cleanup(self):
        state = State(self.cfg)
        self.addCleanup(state.close)
        self.sessions.connect_error = FatalError("authentication rejected")
        pool = Workers(self.cfg, state, self.sessions.factory)
        self.addCleanup(pool.close)
        pool.submit(0, lambda remote, emit: None)
        pool.submit(1, lambda remote, emit: None)
        with self.assertRaisesRegex(FatalError, "authentication rejected"):
            pool.receive(lambda event: None)
        # Simulate the coordinator logging the error before its finally block
        # closes the pool. The establishment lock must not admit the next peer.
        pool.slots[0][1].join(timeout=2)
        pool.slots[1][1].join(timeout=2)
        self.assertEqual(self.sessions.connections, 1)
        self.assertTrue(pool.cancelled.is_set())
        self.assertFalse(any(thread.is_alive() for _, thread in pool.slots))

    def test_permanent_failure_zero_bytes_and_force(self):
        self.sessions.source.add("a.pdf", b"")
        self.sessions.source.add("b.pdf", b"123")
        self.sessions.source.open_hook = lambda p: (_ for _ in ()).throw(PermissionError("denied")) if p == "b.pdf" else None
        code, summary = self.invoke()
        self.assertEqual(code, 2)
        self.assertEqual(summary["new_successes"], 1)
        self.sessions.source.open_hook = None
        code, summary = self.invoke(force_path="a.pdf")
        self.assertEqual(code, 0)
        self.assertEqual(summary["new_successes"], 1)
        self.assertEqual(summary["forced_successes"], 1)

    def test_reversed_directory_completion_keeps_identical_aliases(self):
        from clm_exporter import discovery
        for directory in ("A", "a", "a-1", "é", "é"):
            for name in ("A.pdf", "a.pdf", "a-1.pdf", "é.pdf", "é.pdf"):
                self.sessions.source.add(directory + "/" + name)
        original = discovery.enumerate_directory
        mappings = []
        for slow, fast in (("A", "a"), ("a", "A")):
            self.cfg = replace(self.cfg, working_directory=self.root / slow / "export")
            self.invoke("init")
            finished = threading.Event()
            def enumerate_in_order(remote, directory, attempt, emit, slow=slow, fast=fast, finished=finished):
                if directory == slow:
                    self.assertTrue(finished.wait(5))
                original(remote, directory, attempt, emit)
                if directory == fast:
                    finished.set()
            with patch.object(discovery, "enumerate_directory", enumerate_in_order):
                self.assertEqual(self.invoke("inventory")[0], 0)
            mappings.append(self.rows("SELECT remote,local FROM paths ORDER BY remote"))
        self.assertEqual(*mappings)

    def test_failed_subtree_remains_unknown_and_valid_files_download(self):
        self.sessions.source.add("hidden/old.pdf")
        self.assertEqual(self.invoke("inventory")[0], 0)
        self.sessions.source.denied.add("hidden")
        self.sessions.source.add("visible/new.pdf")
        code, summary = self.invoke()
        self.assertEqual(code, 2)
        self.assertEqual(summary["scan_status"], "incomplete")
        self.assertEqual(summary["new_successes"], 1)
        self.assertEqual(self.rows("SELECT remote FROM errors WHERE operation='availability'"), [])

    def test_refresh_grows_beyond_cap_and_other_file_still_downloads(self):
        self.cfg = replace(self.cfg, max_staged_bytes=5)
        self.sessions.source.add("a.pdf", b"12")
        self.sessions.source.add("b.pdf", b"34")
        def grow(relative):
            if relative == "a.pdf":
                self.sessions.source.add(relative, b"123456", mtime=2)
        self.sessions.source.stat_hook = grow
        code, summary = self.invoke()
        self.assertEqual(code, 2)
        self.assertEqual(summary["new_successes"], 1)
        self.assertEqual(self.sessions.source.opens, ["b.pdf"])
        self.assertEqual((self.cfg.export / "b.pdf").read_bytes(), b"34")

    def test_interrupt_after_metadata_refresh_preserves_committed_replacement(self):
        from clm_exporter import state

        connect = sqlite3.connect

        class InterruptedCommit(sqlite3.Connection):
            interrupt_commit = False

            def execute(self, sql, *args):
                result = super().execute(sql, *args)
                if sql.startswith("INSERT INTO transfers") and args[0][1] == 4:
                    self.interrupt_commit = True
                if sql == "COMMIT" and self.interrupt_commit:
                    self.interrupt_commit = False
                    raise KeyboardInterrupt
                return result

        self.sessions.source.add("a.pdf", b"old")

        def grow(relative):
            if relative == "a.pdf":
                self.sessions.source.add(relative, b"long", mtime=2)

        self.sessions.source.stat_hook = grow
        with patch.object(state.sqlite3, "connect",
                          lambda *args, **kwargs: connect(*args, **kwargs, factory=InterruptedCommit)):
            code, summary = self.invoke()

        self.assertEqual((code, summary["stop_reason"]), (130, "interrupted"))
        self.assertEqual(self.rows("SELECT state,size,mtime FROM transfers ORDER BY id"),
                         [("superseded", 3, "1"), ("pending", 4, "2")])
        self.assertEqual(self.rows("SELECT size,mtime FROM observations"), [(4, "2")])
        self.assertEqual(self.sessions.source.opens, [])
        self.assertTrue(Path(summary["reports"]["backup"]).exists())
        self.assertEqual(self.invoke()[0], 0)
        self.assertEqual((self.cfg.export / "a.pdf").read_bytes(), b"long")

    def test_forced_repeat_survives_refresh_and_reversion_to_downloaded_metadata(self):
        self.sessions.source.add("a.pdf", b"old")
        self.assertEqual(self.invoke()[0], 0)
        self.cfg = replace(self.cfg, max_staged_bytes=3)

        def grow(relative):
            if relative == "a.pdf":
                self.sessions.source.add(relative, b"new", mtime=2)

        self.sessions.source.stat_hook = grow
        code, summary = self.invoke(force_path="a.pdf")
        self.assertEqual((code, summary["stop_reason"]), (0, "batch_capacity"))
        self.assertEqual(self.rows("SELECT forced,size,mtime FROM transfers WHERE state='pending'"),
                         [(1, 3, "2")])
        self.assertEqual(self.sessions.source.opens, ["a.pdf"])
        self.assertEqual((self.cfg.export / "a.pdf").read_bytes(), b"old")

        # Keep the listing at version 2, then revert during the transfer's stat.
        def revert(relative):
            if relative == "a.pdf":
                self.sessions.source.add(relative, b"old", mtime=1)

        self.sessions.source.stat_hook = revert
        (self.cfg.export / "a.pdf").unlink()
        code, summary = self.invoke()
        self.assertEqual(code, 0)
        self.assertEqual(summary["forced_successes"], 1)
        self.assertEqual(self.sessions.source.opens, ["a.pdf", "a.pdf"])
        self.assertEqual(self.rows("SELECT size,mtime FROM observations ORDER BY rowid DESC LIMIT 1"),
                         [(3, "1")])
        self.assertEqual((self.cfg.export / "a.pdf").read_bytes(), b"old")

    def test_reverted_metadata_skips_without_opening_and_disappearing_source_fails(self):
        self.sessions.source.add("a.pdf", b"old", mtime=1)
        self.assertEqual(self.invoke()[0], 0)
        self.sessions.source.add("a.pdf", b"new", mtime=2)
        self.sessions.source.add("b.pdf")
        def revert(relative):
            if relative == "a.pdf":
                self.sessions.source.add(relative, b"old", mtime=1)
        self.sessions.source.stat_hook = revert
        def vanish(relative):
            if relative == "b.pdf":
                del self.sessions.source.entries[relative]
        self.sessions.source.open_hook = vanish
        opens = len(self.sessions.source.opens)
        code, summary = self.invoke()
        self.assertEqual(code, 2)
        self.assertEqual(summary["new_successes"], 0)
        self.assertEqual(self.sessions.source.opens[opens:], ["b.pdf"])
        self.assertEqual((self.cfg.export / "a.pdf").read_bytes(), b"old")

    def test_untracked_and_modified_targets_are_preserved(self):
        self.sessions.source.add("old.pdf", b"original")
        self.assertEqual(self.invoke()[0], 0)
        self.sessions.source.add("old.pdf", b"replacement", mtime=2)
        self.sessions.source.add("new.pdf")
        (self.cfg.export / "old.pdf").write_bytes(b"modified")
        (self.cfg.export / "new.pdf").write_bytes(b"untracked")
        code, summary = self.invoke()
        self.assertEqual(code, 2)
        self.assertEqual(summary["new_successes"], 0)
        self.assertEqual((self.cfg.export / "old.pdf").read_bytes(), b"modified")
        self.assertEqual((self.cfg.export / "new.pdf").read_bytes(), b"untracked")

    def test_crash_boundaries_recover_multiple_jobs(self):
        for boundary in ("downloading", "ready", "promoted"):
            with self.subTest(boundary=boundary):
                self.cfg = replace(self.cfg, working_directory=self.root / boundary)
                self.sessions = Sessions()
                self.invoke("init")
                for name in ("a.pdf", "b.pdf"):
                    self.sessions.source.add(name, b"1234")
                def crash(event, job, boundary=boundary):
                    if event == boundary:
                        raise SystemExit("simulated process death")
                with self.assertRaises(SystemExit):
                    self.invoke(checkpoint=crash)
                self.assertEqual(self.sessions.active, 0)
                self.assertEqual(self.invoke()[0], 0)
                self.assertEqual(self.rows("SELECT state FROM transfers"), [("downloaded",), ("downloaded",)])

    def test_disk_full_partial_payload_remains_retryable(self):
        from clm_exporter import parallel_transfer
        original = parallel_transfer.regular_open
        self.sessions.source.add("a.pdf", b"1234")
        self.sessions.source.add("b.pdf", b"5678")
        def full(path, flags=0, **kwargs):
            if flags:
                raise OSError(errno.ENOSPC, "disk full")
            return original(path, flags, **kwargs)
        with patch.object(parallel_transfer, "regular_open", full):
            code, summary = self.invoke()
        self.assertEqual(code, 0)
        self.assertEqual(summary["stop_reason"], "disk_reserve")
        self.assertEqual(summary["new_successes"], 0)
        self.assertEqual(self.invoke()[0], 0)

    def test_offline_commands_do_not_create_clients(self):
        self.assertEqual(self.invoke("status")[0], 0)
        self.assertEqual(self.sessions.connections, 0)

    def test_full_event_queue_shutdown_unblocks_producers(self):
        state = State(self.cfg)
        self.addCleanup(state.close)
        pool = Workers(self.cfg, state, self.sessions.factory)
        full = threading.Event()
        put = pool.events.put

        def observe_full_queue(*args, **kwargs):
            try:
                return put(*args, **kwargs)
            except queue.Full:
                full.set()
                raise

        # Confirm a producer actually blocks before cancelling. Otherwise a
        # fast close could pass without ever exercising a full queue.
        errors = []
        def produce(slot):
            try:
                pool._emit(slot, "test")
            except Cancelled as exc:
                errors.append(exc)
        producers = [threading.Thread(target=produce, args=(i,), daemon=True)
                     for i in range(pool.events.maxsize + 1)]
        with patch.object(pool.events, "put", side_effect=observe_full_queue):
            try:
                for thread in producers:
                    thread.start()
                self.assertTrue(full.wait(5), "Producers never filled the event queue")
            finally:
                pool.close()
                for thread in producers:
                    thread.join(timeout=2)
        for thread in producers:
            self.assertFalse(thread.is_alive())
        self.assertEqual(len(errors), len(producers))


class CapacityTests(unittest.TestCase):
    def test_simultaneous_reservations_cannot_spend_the_same_capacity(self):
        for limit, free, reason in ((6, 100, "batch_capacity"), (100, 8, "disk_reserve")):
            with self.subTest(reason=reason):
                self.check_simultaneous_reservations(limit, free, reason)

    def check_simultaneous_reservations(self, limit, free, reason):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            cfg = Config("test", "user", root / "key", root / "hosts", "/", root / "workdir",
                         max_staged_bytes=limit, min_free_bytes=2)
            prepare_layout(cfg)
            capacity = Reservations(cfg, lambda: free)
            barrier = threading.Barrier(2, timeout=5)
            outcomes = []
            def reserve(token):
                barrier.wait()
                try:
                    capacity.reserve(token, 4)
                    outcomes.append("accepted")
                except CapacityStop as exc:
                    outcomes.append(exc.reason)
            threads = [threading.Thread(target=reserve, args=(i,)) for i in range(2)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=5)
                self.assertFalse(thread.is_alive())
            self.assertCountEqual(outcomes, ["accepted", reason])
            self.assertEqual(capacity.reserved, 4)

    def test_equality_reservations_partial_write_and_release(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            cfg = Config("test", "user", root / "key", root / "hosts", "/", root / "workdir",
                         max_staged_bytes=10, min_free_bytes=5)
            prepare_layout(cfg)
            free = [15]
            capacity = Reservations(cfg, lambda: free[0])
            capacity.reserve(1, 4)
            capacity.reserve(2, 6)
            with self.assertRaises(CapacityStop):
                capacity.reserve(3, 1)
            with regular_open(cfg.work / "part", os.O_WRONLY | os.O_CREAT | os.O_EXCL, buffering=0) as output:
                capacity.write(1, output, b"abcd")
            free[0] -= 4
            self.assertEqual((capacity.staged, capacity.reserved), (4, 6))
            capacity.release(1)
            capacity.release(2)
            self.assertEqual(capacity.reserved, 0)

            class Partial:
                def __init__(self, output):
                    self.output = output
                def fileno(self):
                    return self.output.fileno()
                def write(self, chunk):
                    self.output.write(chunk[:1])
                    raise OSError(errno.ENOSPC, "full")
                def flush(self):
                    self.output.flush()

            capacity.reserve(3, 6)
            with (regular_open(cfg.work / "partial", os.O_WRONLY | os.O_CREAT | os.O_EXCL, buffering=0) as output,
                  self.assertRaises(OSError)):
                capacity.write(3, Partial(output), b"xyz")
            self.assertEqual((capacity.staged, capacity.reserved), (5, 5))
            capacity.release(3)
            capacity.removed(1)
            free[0] = 5
            with self.assertRaises(CapacityStop):
                capacity.reserve(4, 1)
