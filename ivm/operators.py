"""Incremental view operators.

Every operator consumes deltas (lists of ``(row, sign)`` tuples where ``sign``
is a signed multiplicity change) and produces output deltas.  State lives in
SQLite tables managed by :class:`ivm.engine.IVMEngine`; operators only compute
delta transformations and maintain their own auxiliary support tables.
"""


class IVMError(Exception):
    """Raised for definition or maintenance errors."""


# ---------------------------------------------------------------------------
# Aggregate function descriptors
# ---------------------------------------------------------------------------

class AggFunc:
    def __init__(self, col=None):
        self.col = col


class Count(AggFunc):
    """COUNT(*) -- retract by decrementing the group counter."""

    def __init__(self):
        super().__init__(None)


class Sum(AggFunc):
    """SUM(col) -- retract by subtracting the retracted value."""


class Avg(AggFunc):
    """AVG(col) -- maintained incrementally as SUM/COUNT."""


class Min(AggFunc):
    """MIN(col) -- retract via a per-group value multiset (support table)."""


class Max(AggFunc):
    """MAX(col) -- retract via a per-group value multiset (support table)."""


# ---------------------------------------------------------------------------
# View operators
# ---------------------------------------------------------------------------

class View:
    """Base class for all view operators."""

    inputs = ()

    def output_columns(self, engine):
        raise NotImplementedError

    def setup(self, engine, name):
        """Create auxiliary state tables (default: none)."""

    def compute_delta(self, engine, name, input_deltas):
        """Return the output delta given ``{input_name: [(row, sign)]}``."""
        raise NotImplementedError


class Filter(View):
    """Selection: keeps rows for which ``predicate(row_dict)`` is true."""

    def __init__(self, input, predicate):
        self.input = input
        self.inputs = (input,)
        self.predicate = predicate

    def output_columns(self, engine):
        return list(engine.relations[self.input])

    def compute_delta(self, engine, name, input_deltas):
        return [
            (row, sign)
            for row, sign in input_deltas.get(self.input, [])
            if self.predicate(row)
        ]


class Project(View):
    """Projection / rename.

    ``mapping`` is either a list of columns to keep, or a dict
    ``{output_column: input_column}``.
    """

    def __init__(self, input, mapping):
        self.input = input
        self.inputs = (input,)
        if isinstance(mapping, dict):
            self.mapping = dict(mapping)
        else:
            self.mapping = {c: c for c in mapping}

    def output_columns(self, engine):
        return list(self.mapping)

    def compute_delta(self, engine, name, input_deltas):
        return [
            ({out: row[src] for out, src in self.mapping.items()}, sign)
            for row, sign in input_deltas.get(self.input, [])
        ]


class Union(View):
    """Bag union of two or more relations with identical schemas."""

    def __init__(self, inputs):
        if len(inputs) < 2:
            raise IVMError("Union needs at least two inputs")
        self.inputs = tuple(inputs)

    def output_columns(self, engine):
        cols = list(engine.relations[self.inputs[0]])
        for other in self.inputs[1:]:
            if list(engine.relations[other]) != cols:
                raise IVMError("Union inputs must have identical columns")
        return cols

    def compute_delta(self, engine, name, input_deltas):
        out = []
        for inp in self.inputs:
            out.extend(input_deltas.get(inp, []))
        return out


class Join(View):
    """Inner equi-join.

    ``on`` is a list of ``(left_col, right_col)`` equality pairs.  Output
    columns are the left columns plus right columns not present on the left.
    Both inputs may change in the same batch (diamond dependencies); the
    delta formula handles the overlap so rows are never double counted.
    """

    def __init__(self, left, right, on):
        if not on:
            raise IVMError("Join requires at least one equality pair")
        self.left = left
        self.right = right
        self.on = list(on)
        self.inputs = (left, right)

    def output_columns(self, engine):
        lcols = list(engine.relations[self.left])
        rcols = list(engine.relations[self.right])
        shared_keys = {l for l, r in self.on if l == r}
        for c in rcols:
            if c in lcols and c not in shared_keys:
                raise IVMError(
                    "Join column name collision on non-key column %r; "
                    "rename it with Project first" % c
                )
        return lcols + [c for c in rcols if c not in lcols]

    def _merge(self, engine, lrow, rrow):
        out = dict(lrow)
        for c in engine.relations[self.right]:
            if c not in out:
                out[c] = rrow[c]
        return out

    def compute_delta(self, engine, name, input_deltas):
        ldelta = input_deltas.get(self.left, [])
        rdelta = input_deltas.get(self.right, [])
        out = []
        # Left delta against the *new* right state (covers dL x dR).
        for row, sign in ldelta:
            pairs = [(rc, row[lc]) for lc, rc in self.on]
            for match, cnt in engine.probe(self.right, pairs):
                out.append((self._merge(engine, row, match), sign * cnt))
        # Right delta against the *old* left state = new left minus dL.
        # Retracted left rows no longer appear in the new state, so they
        # must be added back as probe candidates with a new-state count of 0.
        left_cols = engine.relations[self.left]
        ladj = {}
        for row, sign in ldelta:
            key = tuple(row[c] for c in left_cols)
            ladj[key] = ladj.get(key, 0) + sign
        for row, sign in rdelta:
            pairs = [(lc, row[rc]) for lc, rc in self.on]
            candidates = {}
            for match, cnt in engine.probe(self.left, pairs):
                key = tuple(match[c] for c in left_cols)
                candidates[key] = (match, cnt)
            for lrow, _lsign in ldelta:
                if all(lrow[lc] == row[rc] for lc, rc in self.on):
                    key = tuple(lrow[c] for c in left_cols)
                    if key not in candidates:
                        candidates[key] = (lrow, 0)
            for key, (lrow, new_cnt) in candidates.items():
                old_cnt = new_cnt - ladj.get(key, 0)
                if old_cnt:
                    out.append((self._merge(engine, lrow, row), sign * old_cnt))
        return out


class Aggregate(View):
    """GROUP BY with COUNT / SUM / AVG / MIN / MAX.

    Retraction strategies per function:

    - COUNT: decrement the group counter; the group disappears at zero.
    - SUM:   subtract the retracted values from the running sum.
    - AVG:   maintained as running SUM / COUNT.
    - MIN/MAX: a per-group multiset of values (support table) tracks how
      many details contribute each value; when the support count of the
      current extremum reaches zero the next extremum is recomputed from
      the remaining values, so deleting the last detail never leaves a
      stale MIN/MAX behind.
    """

    def __init__(self, input, group_by, aggs):
        self.input = input
        self.inputs = (input,)
        self.group_by = list(group_by)
        self.aggs = dict(aggs)
        for out, agg in self.aggs.items():
            if not isinstance(agg, AggFunc):
                raise IVMError("unknown aggregate for %r" % out)
            if not isinstance(agg, Count) and agg.col is None:
                raise IVMError("aggregate %r needs a column" % out)

    # -- schema ----------------------------------------------------------

    def output_columns(self, engine):
        return self.group_by + list(self.aggs)

    def _sum_outs(self):
        return [o for o, a in self.aggs.items() if isinstance(a, (Sum, Avg))]

    def _mm_outs(self):
        return [o for o, a in self.aggs.items() if isinstance(a, (Min, Max))]

    def setup(self, engine, name):
        gcols = ", ".join('"%s"' % c for c in self.group_by)
        prefix = (gcols + ", ") if gcols else ""
        sumcols = "".join(', "__sum_%s" NUMERIC' % o for o in self._sum_outs())
        engine.conn.execute(
            'CREATE TABLE IF NOT EXISTS "__ivmsup_%s" '
            "(%s__cnt INTEGER NOT NULL%s)" % (name, prefix, sumcols)
        )
        for out in self._mm_outs():
            engine.conn.execute(
                'CREATE TABLE IF NOT EXISTS "__ivmsup_%s_%s" '
                "(%s__val, __cnt INTEGER NOT NULL)" % (name, out, prefix)
            )

    # -- helpers ---------------------------------------------------------

    def _gwhere(self):
        if not self.group_by:
            return "1=1"
        return " AND ".join('"%s" IS ?' % c for c in self.group_by)

    # -- delta computation -----------------------------------------------

    def compute_delta(self, engine, name, input_deltas):
        deltas = input_deltas.get(self.input) or []
        if not deltas:
            return []
        conn = engine.conn
        out_cols = self.output_columns(engine)
        sum_outs = self._sum_outs()
        gw = self._gwhere()

        # 1. accumulate the batch's changes per group
        touched = {}
        for row, sign in deltas:
            gkey = tuple(row[c] for c in self.group_by)
            acc = touched.get(gkey)
            if acc is None:
                acc = touched[gkey] = {"count": 0, "sums": {}, "vals": {}}
            acc["count"] += sign
            for out, agg in self.aggs.items():
                if isinstance(agg, (Sum, Avg)):
                    acc["sums"][out] = acc["sums"].get(out, 0) + sign * row[agg.col]
                elif isinstance(agg, (Min, Max)):
                    vmap = acc["vals"].setdefault(out, {})
                    val = row[agg.col]
                    vmap[val] = vmap.get(val, 0) + sign

        out_delta = []
        for gkey, acc in touched.items():
            gvals = list(gkey)
            old_row = conn.execute(
                'SELECT * FROM "%s" WHERE %s' % (name, gw), gvals
            ).fetchone()
            old_out = (
                {c: old_row[i] for i, c in enumerate(out_cols)} if old_row else None
            )

            sup_select = '"__cnt"' + "".join(
                ', "__sum_%s"' % o for o in sum_outs
            )
            sup = conn.execute(
                'SELECT %s FROM "__ivmsup_%s" WHERE %s' % (sup_select, name, gw),
                gvals,
            ).fetchone()
            sup_cnt = sup[0] if sup else 0
            sup_sums = (
                {o: sup[1 + i] for i, o in enumerate(sum_outs)} if sup else {}
            )

            new_cnt = sup_cnt + acc["count"]
            if new_cnt < 0:
                raise IVMError("aggregate count underflow in view %r" % name)

            # 2. update MIN/MAX support multisets
            for out, vmap in acc["vals"].items():
                tbl = "__ivmsup_%s_%s" % (name, out)
                for val, d in vmap.items():
                    row = conn.execute(
                        'SELECT "__cnt" FROM "%s" WHERE %s AND "__val" IS ?'
                        % (tbl, gw),
                        gvals + [val],
                    ).fetchone()
                    n = (row[0] if row else 0) + d
                    if n < 0:
                        raise IVMError(
                            "MIN/MAX support underflow in view %r" % name
                        )
                    if n == 0:
                        if row:
                            conn.execute(
                                'DELETE FROM "%s" WHERE %s AND "__val" IS ?'
                                % (tbl, gw),
                                gvals + [val],
                            )
                    elif row:
                        conn.execute(
                            'UPDATE "%s" SET "__cnt" = ? WHERE %s '
                            'AND "__val" IS ?' % (tbl, gw),
                            [n] + gvals + [val],
                        )
                    else:
                        conn.execute(
                            'INSERT INTO "%s" VALUES (%s)'
                            % (tbl, ", ".join("?" * (len(gvals) + 2))),
                            gvals + [val, n],
                        )

            # 3. fold the group
            if new_cnt == 0:
                conn.execute(
                    'DELETE FROM "__ivmsup_%s" WHERE %s' % (name, gw), gvals
                )
                if old_out is not None:
                    out_delta.append((old_out, -1))
                continue

            new_sums = {
                o: (sup_sums.get(o) or 0) + acc["sums"].get(o, 0)
                for o in sum_outs
            }
            if sup:
                assignments = ", ".join(
                    '"__sum_%s" = ?' % o for o in sum_outs
                )
                sql = 'UPDATE "__ivmsup_%s" SET "__cnt" = ?' % name
                params = [new_cnt]
                if assignments:
                    sql += ", " + assignments
                    params += [new_sums[o] for o in sum_outs]
                sql += " WHERE " + gw
                conn.execute(sql, params + gvals)
            else:
                cols = ['"%s"' % c for c in self.group_by] + ['"__cnt"'] + [
                    '"__sum_%s"' % o for o in sum_outs
                ]
                params = gvals + [new_cnt] + [new_sums[o] for o in sum_outs]
                conn.execute(
                    'INSERT INTO "__ivmsup_%s" (%s) VALUES (%s)'
                    % (name, ", ".join(cols), ", ".join("?" * len(params))),
                    params,
                )

            new_out = {c: v for c, v in zip(self.group_by, gkey)}
            for out, agg in self.aggs.items():
                if isinstance(agg, Count):
                    new_out[out] = new_cnt
                elif isinstance(agg, Sum):
                    new_out[out] = new_sums[out]
                elif isinstance(agg, Avg):
                    new_out[out] = new_sums[out] / new_cnt
                else:
                    fn = "MIN" if isinstance(agg, Min) else "MAX"
                    new_out[out] = conn.execute(
                        'SELECT %s("__val") FROM "__ivmsup_%s_%s" '
                        'WHERE %s AND "__cnt" > 0' % (fn, name, out, gw),
                        gvals,
                    ).fetchone()[0]

            if old_out != new_out:
                if old_out is not None:
                    out_delta.append((old_out, -1))
                out_delta.append((new_out, 1))
        return out_delta
