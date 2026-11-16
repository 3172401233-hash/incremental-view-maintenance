"""Incremental materialized view maintenance engine.

Sources accept batches of insert/update/delete operations tagged with an
idempotency key.  Each batch is applied inside a single SQLite transaction:
the delta propagates through the dependency graph of views, every affected
view state and the batch checkpoint commit atomically.  A crash or failure
rolls the whole batch back, so a retry can never double-apply or leave half
of the dependency chain updated.
"""

import sqlite3
import time

from .operators import IVMError


class IVMEngine:
    def __init__(self, path=":memory:", hooks=None):
        self.conn = sqlite3.connect(path)
        self.conn.isolation_level = None  # explicit transaction control
        self.hooks = hooks or {}
        self.sources = {}    # name -> {"key": [...], "columns": {col: type}}
        self.views = {}      # name -> View operator
        self.relations = {}  # name -> ordered output columns
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS __ivm_meta("
            "key TEXT PRIMARY KEY, value INTEGER)"
        )
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS __ivm_batches("
            "source TEXT NOT NULL, batch_id TEXT NOT NULL, "
            "seq INTEGER NOT NULL, applied_at REAL NOT NULL, "
            "PRIMARY KEY(source, batch_id))"
        )

    def close(self):
        self.conn.close()

    # ------------------------------------------------------------------
    # schema definition
    # ------------------------------------------------------------------

    def create_source(self, name, key, columns):
        """Declare a base fact table.  ``columns`` maps name -> SQL type."""
        if name in self.relations:
            raise IVMError("relation %r already defined" % name)
        cols = list(columns)
        missing = [k for k in key if k not in columns]
        if missing:
            raise IVMError("key columns not in schema: %r" % missing)
        ddl = 'CREATE TABLE IF NOT EXISTS "%s" (%s, "__cnt" INTEGER NOT NULL)' % (
            name,
            ", ".join('"%s" %s' % (c, t) for c, t in columns.items()),
        )
        self.conn.execute(ddl)
        self.conn.execute(
            'CREATE UNIQUE INDEX IF NOT EXISTS "__ivmkey_%s" ON "%s" (%s)'
            % (name, name, ", ".join('"%s"' % k for k in key))
        )
        self.sources[name] = {"key": list(key), "columns": dict(columns)}
        self.relations[name] = cols

    def add_view(self, name, view):
        """Define a derived view over sources or previously defined views."""
        if name in self.relations:
            raise IVMError("relation %r already defined" % name)
        for inp in view.inputs:
            if inp not in self.relations:
                raise IVMError("unknown input relation %r" % inp)
        cols = view.output_columns(self)
        self.conn.execute(
            'CREATE TABLE IF NOT EXISTS "%s" (%s, "__cnt" INTEGER NOT NULL)'
            % (name, ", ".join('"%s"' % c for c in cols))
        )
        view.setup(self, name)
        self.views[name] = view
        self.relations[name] = cols

    def _topo_order(self):
        order, done, visiting = [], set(), set()

        def visit(n):
            if n in done:
                return
            if n in visiting:
                raise IVMError("cycle in view definitions at %r" % n)
            visiting.add(n)
            for inp in self.views[n].inputs:
                if inp in self.views:
                    visit(inp)
            visiting.discard(n)
            done.add(n)
            order.append(n)

        for n in self.views:
            visit(n)
        return order

    # ------------------------------------------------------------------
    # state helpers (used by operators too)
    # ------------------------------------------------------------------

    def probe(self, relation, pairs):
        """Return ``(row_dict, count)`` for rows of ``relation`` matching
        the ``(column, value)`` equality pairs."""
        cols = self.relations[relation]
        where = " AND ".join('"%s" IS ?' % c for c, _ in pairs) or "1=1"
        cur = self.conn.execute(
            'SELECT * FROM "%s" WHERE %s' % (relation, where),
            [v for _, v in pairs],
        )
        return [
            ({c: r[i] for i, c in enumerate(cols)}, r[len(cols)])
            for r in cur.fetchall()
        ]

    def _apply_state(self, table, columns, row, sign):
        where = " AND ".join('"%s" IS ?' % c for c in columns)
        vals = [row[c] for c in columns]
        cur = self.conn.execute(
            'SELECT "__cnt" FROM "%s" WHERE %s' % (table, where), vals
        ).fetchone()
        new = (cur[0] if cur else 0) + sign
        if new < 0:
            raise IVMError("retraction underflow in relation %r" % table)
        if new == 0:
            if cur:
                self.conn.execute(
                    'DELETE FROM "%s" WHERE %s' % (table, where), vals
                )
        elif cur:
            self.conn.execute(
                'UPDATE "%s" SET "__cnt" = ? WHERE %s' % (table, where),
                [new] + vals,
            )
        else:
            self.conn.execute(
                'INSERT INTO "%s" VALUES (%s)'
                % (table, ", ".join("?" * (len(columns) + 1))),
                vals + [new],
            )

    # ------------------------------------------------------------------
    # batch application
    # ------------------------------------------------------------------

    def _consolidate(self, relation, deltas):
        """Merge ``(row, sign)`` entries for identical rows and drop zeros.

        Operators may emit complementary phantom pairs (e.g. a join whose
        inputs both changed); consolidating before touching state avoids
        transient negative multiplicities during sequential application.
        """
        cols = self.relations[relation]
        merged = {}
        order = []
        for row, sign in deltas:
            key = tuple(row[c] for c in cols)
            if key not in merged:
                merged[key] = [row, 0]
                order.append(key)
            merged[key][1] += sign
        return [
            (merged[k][0], merged[k][1]) for k in order if merged[k][1] != 0
        ]

    def _fetch_by_key(self, source, keyvals):
        key = self.sources[source]["key"]
        rows = self.probe(source, [(k, keyvals.get(k)) for k in key])
        return rows[0][0] if rows else None

    def _apply_source_op(self, source, op):
        """Apply one insert/update/delete to the source state; return deltas."""
        cols = self.relations[source]
        key = self.sources[source]["key"]
        kind = op["op"]
        if kind == "insert":
            row = {c: op["row"].get(c) for c in cols}
            if any(row[k] is None for k in key):
                raise IVMError("insert with NULL key into %r" % source)
            if self._fetch_by_key(source, row) is not None:
                raise IVMError("duplicate key in %r" % source)
            self._apply_state(source, cols, row, 1)
            return [(row, 1)]
        if kind == "delete":
            old = self._fetch_by_key(source, op["key"])
            if old is None:
                raise IVMError("delete of missing key in %r" % source)
            self._apply_state(source, cols, old, -1)
            return [(old, -1)]
        if kind == "update":
            row = {c: op["row"].get(c) for c in cols}
            old = self._fetch_by_key(source, row)
            if old is None:
                raise IVMError("update of missing key in %r" % source)
            self._apply_state(source, cols, old, -1)
            self._apply_state(source, cols, row, 1)
            return [(old, -1), (row, 1)]
        raise IVMError("unknown op %r" % kind)

    def _next_seq(self):
        row = self.conn.execute(
            "SELECT value FROM __ivm_meta WHERE key = 'seq'"
        ).fetchone()
        seq = (row[0] if row else 0) + 1
        self.conn.execute(
            "INSERT INTO __ivm_meta(key, value) VALUES('seq', ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (seq,),
        )
        return seq

    def apply_batch(self, source, batch_id, ops):
        """Apply a batch of ops to ``source`` and propagate to all views.

        Returns True if the batch was applied, False if a batch with the
        same ``(source, batch_id)`` was already applied (idempotent skip).
        The whole batch -- source state, every dependent view state and the
        checkpoint -- commits in one transaction; any failure rolls back
        completely so the batch can be retried safely.
        """
        if source not in self.sources:
            raise IVMError("unknown source %r" % source)
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            dup = self.conn.execute(
                "SELECT 1 FROM __ivm_batches WHERE source = ? AND batch_id = ?",
                (source, batch_id),
            ).fetchone()
            if dup:
                self.conn.execute("COMMIT")
                return False

            pending = {}
            deltas = []
            for op in ops:
                deltas.extend(self._apply_source_op(source, op))
            if deltas:
                pending[source] = deltas

            hook = self.hooks.get("after_view")
            for vname in self._topo_order():
                view = self.views[vname]
                ins = {i: pending.get(i, []) for i in view.inputs}
                if any(ins.values()):
                    out = view.compute_delta(self, vname, ins)
                    if out:
                        out = self._consolidate(vname, out)
                    if out:
                        for row, sign in out:
                            self._apply_state(
                                vname, self.relations[vname], row, sign
                            )
                        pending[vname] = out
                if hook:
                    hook(self, vname)

            seq = self._next_seq()
            self.conn.execute(
                "INSERT INTO __ivm_batches VALUES (?, ?, ?, ?)",
                (source, batch_id, seq, time.time()),
            )
            self.conn.execute("COMMIT")
            return True
        except BaseException:
            self.conn.execute("ROLLBACK")
            raise

    # ------------------------------------------------------------------
    # inspection
    # ------------------------------------------------------------------

    def rows(self, name):
        """Materialized rows of a relation as a list of dicts."""
        cols = self.relations[name]
        cur = self.conn.execute(
            'SELECT %s FROM "%s"'
            % (", ".join('"%s"' % c for c in cols), name)
        )
        return [dict(zip(cols, r)) for r in cur.fetchall()]

    def applied_batches(self, source=None):
        if source is None:
            cur = self.conn.execute(
                "SELECT source, batch_id, seq FROM __ivm_batches ORDER BY seq"
            )
        else:
            cur = self.conn.execute(
                "SELECT source, batch_id, seq FROM __ivm_batches "
                "WHERE source = ? ORDER BY seq",
                (source,),
            )
        return [
            {"source": s, "batch_id": b, "seq": q} for s, b, q in cur.fetchall()
        ]

    def last_seq(self):
        row = self.conn.execute(
            "SELECT value FROM __ivm_meta WHERE key = 'seq'"
        ).fetchone()
        return row[0] if row else 0
