import os
import random
import tempfile
import unittest

from ivm import (
    Aggregate,
    Avg,
    Count,
    Filter,
    IVMEngine,
    Join,
    Max,
    Min,
    Project,
    Sum,
)


def build_engine(path=":memory:", hooks=None):
    """Orders/refunds scenario: facts -> cleaned view -> per-user summary."""
    eng = IVMEngine(path, hooks=hooks)
    eng.create_source(
        "orders",
        key=["order_id"],
        columns={
            "order_id": "INTEGER",
            "user_id": "INTEGER",
            "amount": "REAL",
            "status": "TEXT",
        },
    )
    eng.create_source(
        "refunds",
        key=["refund_id"],
        columns={
            "refund_id": "INTEGER",
            "order_id": "INTEGER",
            "amount": "REAL",
        },
    )
    eng.add_view(
        "clean_orders", Filter("orders", lambda r: r["status"] != "test")
    )
    eng.add_view(
        "per_user",
        Aggregate(
            "clean_orders",
            group_by=["user_id"],
            aggs={
                "cnt": Count(),
                "total": Sum("amount"),
                "avg_amt": Avg("amount"),
                "min_amt": Min("amount"),
                "max_amt": Max("amount"),
            },
        ),
    )
    return eng


def seed_orders(eng, batch_id="b1"):
    return eng.apply_batch(
        "orders",
        batch_id,
        [
            {"op": "insert", "row": {"order_id": 1, "user_id": 10,
                                     "amount": 100.0, "status": "ok"}},
            {"op": "insert", "row": {"order_id": 2, "user_id": 10,
                                     "amount": 300.0, "status": "ok"}},
            {"op": "insert", "row": {"order_id": 3, "user_id": 20,
                                     "amount": 50.0, "status": "ok"}},
            {"op": "insert", "row": {"order_id": 4, "user_id": 10,
                                     "amount": 999.0, "status": "test"}},
        ],
    )


def summary_by_user(eng):
    return {r["user_id"]: r for r in eng.rows("per_user")}


class TestAggregateRetraction(unittest.TestCase):
    def setUp(self):
        self.eng = build_engine()
        seed_orders(self.eng)

    def tearDown(self):
        self.eng.close()

    def test_initial_aggregation(self):
        summ = summary_by_user(self.eng)
        self.assertEqual(set(summ), {10, 20})
        u10 = summ[10]
        self.assertEqual(u10["cnt"], 2)
        self.assertEqual(u10["total"], 400.0)
        self.assertEqual(u10["avg_amt"], 200.0)
        self.assertEqual(u10["min_amt"], 100.0)
        self.assertEqual(u10["max_amt"], 300.0)
        self.assertEqual(summ[20]["cnt"], 1)

    def test_update_retracts_and_reinserts(self):
        self.eng.apply_batch(
            "orders",
            "b2",
            [{"op": "update", "row": {"order_id": 2, "user_id": 10,
                                      "amount": 500.0, "status": "ok"}}],
        )
        u10 = summary_by_user(self.eng)[10]
        self.assertEqual(u10["cnt"], 2)
        self.assertEqual(u10["total"], 600.0)
        self.assertEqual(u10["min_amt"], 100.0)
        self.assertEqual(u10["max_amt"], 500.0)

    def test_min_max_step_down_on_delete(self):
        self.eng.apply_batch(
            "orders", "b2", [{"op": "delete", "key": {"order_id": 2}}]
        )
        u10 = summary_by_user(self.eng)[10]
        self.assertEqual(u10["cnt"], 1)
        self.assertEqual(u10["total"], 100.0)
        self.assertEqual(u10["min_amt"], 100.0)
        self.assertEqual(u10["max_amt"], 100.0)

    def test_deleting_last_detail_leaves_no_stale_min_max(self):
        self.eng.apply_batch(
            "orders",
            "b2",
            [
                {"op": "delete", "key": {"order_id": 1}},
                {"op": "delete", "key": {"order_id": 2}},
            ],
        )
        summ = summary_by_user(self.eng)
        self.assertNotIn(10, summ)  # group fully gone, no stale MIN/MAX row
        self.assertIn(20, summ)

    def test_delete_then_reinsert_recovers(self):
        self.eng.apply_batch(
            "orders",
            "b2",
            [
                {"op": "delete", "key": {"order_id": 1}},
                {"op": "delete", "key": {"order_id": 2}},
            ],
        )
        self.assertNotIn(10, summary_by_user(self.eng))
        self.eng.apply_batch(
            "orders",
            "b3",
            [{"op": "insert", "row": {"order_id": 5, "user_id": 10,
                                      "amount": 42.0, "status": "ok"}}],
        )
        u10 = summary_by_user(self.eng)[10]
        self.assertEqual(
            (u10["cnt"], u10["total"], u10["min_amt"], u10["max_amt"]),
            (1, 42.0, 42.0, 42.0),
        )

    def test_filtered_rows_never_reach_summary(self):
        # order 4 has status 'test' and must not appear anywhere downstream
        self.assertEqual(len(self.eng.rows("clean_orders")), 3)
        self.assertNotIn(999.0, [r["total"] for r in self.eng.rows("per_user")])


class TestSharedSubview(unittest.TestCase):
    def test_two_consumers_of_one_intermediate_view(self):
        eng = build_engine()
        eng.add_view(
            "per_status",
            Aggregate(
                "clean_orders",
                group_by=["status"],
                aggs={"cnt": Count(), "total": Sum("amount")},
            ),
        )
        seed_orders(eng)
        self.assertEqual(
            {r["status"]: (r["cnt"], r["total"])
             for r in eng.rows("per_status")},
            {"ok": (3, 450.0)},
        )
        self.assertEqual(
            {r["user_id"]: r["cnt"] for r in eng.rows("per_user")},
            {10: 2, 20: 1},
        )
        # one delete propagates to both consumers of the shared view
        eng.apply_batch("orders", "b2", [{"op": "delete", "key": {"order_id": 3}}])
        self.assertEqual(
            {r["status"]: (r["cnt"], r["total"])
             for r in eng.rows("per_status")},
            {"ok": (2, 400.0)},
        )
        self.assertNotIn(20, summary_by_user(eng))
        eng.close()


class TestMultiLevelDependency(unittest.TestCase):
    """orders -> clean -> join -> aggregate, refunds -> project -> join."""

    def setUp(self):
        self.eng = build_engine()
        self.eng.add_view(
            "refunds_renamed",
            Project(
                "refunds",
                {"refund_id": "refund_id", "order_id": "order_id",
                 "refund_amt": "amount"},
            ),
        )
        self.eng.add_view(
            "order_refunds",
            Join("clean_orders", "refunds_renamed",
                 on=[("order_id", "order_id")]),
        )
        self.eng.add_view(
            "net_per_user",
            Aggregate(
                "order_refunds",
                group_by=["user_id"],
                aggs={
                    "refunded_orders": Count(),
                    "gross": Sum("amount"),
                    "refunded": Sum("refund_amt"),
                },
            ),
        )
        seed_orders(self.eng)
        self.eng.apply_batch(
            "refunds",
            "r1",
            [
                {"op": "insert", "row": {"refund_id": 1, "order_id": 1,
                                         "amount": 30.0}},
                {"op": "insert", "row": {"refund_id": 2, "order_id": 2,
                                         "amount": 300.0}},
                # refund for a filtered-out ('test') order: no join match
                {"op": "insert", "row": {"refund_id": 3, "order_id": 4,
                                         "amount": 1.0}},
            ],
        )

    def tearDown(self):
        self.eng.close()

    def test_join_then_aggregate(self):
        net = {r["user_id"]: r for r in self.eng.rows("net_per_user")}
        self.assertEqual(set(net), {10})
        self.assertEqual(net[10]["refunded_orders"], 2)
        self.assertEqual(net[10]["gross"], 400.0)
        self.assertEqual(net[10]["refunded"], 330.0)

    def test_refund_delete_propagates_through_chain(self):
        self.eng.apply_batch(
            "refunds", "r2", [{"op": "delete", "key": {"refund_id": 2}}]
        )
        net = {r["user_id"]: r for r in self.eng.rows("net_per_user")}
        self.assertEqual(net[10]["refunded_orders"], 1)
        self.assertEqual(net[10]["gross"], 100.0)
        self.assertEqual(net[10]["refunded"], 30.0)

    def test_order_delete_propagates_through_chain(self):
        self.eng.apply_batch(
            "orders", "b2", [{"op": "delete", "key": {"order_id": 1}}]
        )
        net = {r["user_id"]: r for r in self.eng.rows("net_per_user")}
        self.assertEqual(net[10]["refunded_orders"], 1)
        self.assertEqual(net[10]["gross"], 300.0)
        self.assertEqual(net[10]["refunded"], 300.0)
        # and the direct summary lost the order too
        self.assertEqual(summary_by_user(self.eng)[10]["cnt"], 1)


class TestDiamondJoin(unittest.TestCase):
    """Both join inputs derive from one source: no double counting."""

    def test_both_inputs_change_in_one_batch(self):
        eng = IVMEngine()
        eng.create_source("t", key=["id"],
                          columns={"id": "INTEGER", "v": "INTEGER"})
        eng.add_view("f", Filter("t", lambda r: True))
        eng.add_view("p", Project("t", {"id": "id", "v2": "v"}))
        eng.add_view("j", Join("f", "p", on=[("id", "id")]))
        eng.apply_batch(
            "t", "b1",
            [{"op": "insert", "row": {"id": 1, "v": 10}}],
        )
        self.assertEqual(eng.rows("j"), [{"id": 1, "v": 10, "v2": 10}])
        eng.apply_batch(
            "t", "b2", [{"op": "update", "row": {"id": 1, "v": 20}}]
        )
        self.assertEqual(eng.rows("j"), [{"id": 1, "v": 20, "v2": 20}])
        eng.apply_batch("t", "b3", [{"op": "delete", "key": {"id": 1}}])
        self.assertEqual(eng.rows("j"), [])
        eng.close()


class TestIdempotency(unittest.TestCase):
    def test_duplicate_batch_is_skipped(self):
        eng = build_engine()
        self.assertTrue(seed_orders(eng, "b1"))
        before = eng.rows("per_user")
        self.assertFalse(seed_orders(eng, "b1"))  # same id, same ops
        self.assertEqual(eng.rows("per_user"), before)
        self.assertEqual(len(eng.applied_batches("orders")), 1)
        self.assertEqual(eng.last_seq(), 1)
        eng.close()

    def test_same_id_on_different_source_is_independent(self):
        eng = build_engine()
        self.assertTrue(seed_orders(eng, "b1"))
        self.assertTrue(
            eng.apply_batch(
                "refunds", "b1",
                [{"op": "insert", "row": {"refund_id": 1, "order_id": 1,
                                          "amount": 5.0}}],
            )
        )
        self.assertEqual(len(eng.applied_batches()), 2)
        eng.close()

    def test_failed_batch_can_be_retried_with_same_id(self):
        eng = build_engine()
        seed_orders(eng)
        with self.assertRaises(Exception):
            eng.apply_batch(
                "orders", "b2", [{"op": "delete", "key": {"order_id": 999}}]
            )
        # retry with a valid delete under the same id
        self.assertTrue(
            eng.apply_batch(
                "orders", "b2", [{"op": "delete", "key": {"order_id": 3}}]
            )
        )
        self.assertNotIn(20, summary_by_user(eng))
        eng.close()


class TestCrashRecovery(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "state.db")

    def tearDown(self):
        self.tmp.cleanup()

    def test_crash_mid_batch_rolls_back_and_resumes(self):
        def boom(engine, view_name):
            if view_name == "per_user":
                raise RuntimeError("simulated crash")

        eng = build_engine(self.path, hooks={"after_view": boom})
        with self.assertRaises(RuntimeError):
            seed_orders(eng)
        # nothing committed: no source rows, no views, no checkpoint
        self.assertEqual(eng.rows("orders"), [])
        self.assertEqual(eng.rows("per_user"), [])
        self.assertEqual(eng.applied_batches(), [])
        eng.close()

        # resume in a fresh process/engine on the same state file
        eng2 = build_engine(self.path)
        self.assertTrue(seed_orders(eng2))
        summ = summary_by_user(eng2)
        self.assertEqual(summ[10]["cnt"], 2)
        self.assertEqual(summ[10]["total"], 400.0)
        # retrying the same batch again is a no-op, never a double apply
        self.assertFalse(seed_orders(eng2))
        self.assertEqual(summary_by_user(eng2)[10]["cnt"], 2)
        self.assertEqual(eng2.last_seq(), 1)
        eng2.close()

    def test_state_survives_reopen(self):
        eng = build_engine(self.path)
        seed_orders(eng)
        eng.apply_batch(
            "orders", "b2", [{"op": "delete", "key": {"order_id": 2}}]
        )
        eng.close()

        eng2 = build_engine(self.path)
        u10 = summary_by_user(eng2)[10]
        self.assertEqual((u10["cnt"], u10["total"], u10["max_amt"]),
                         (1, 100.0, 100.0))
        self.assertFalse(seed_orders(eng2))  # checkpoint persisted
        self.assertEqual(eng2.last_seq(), 2)
        eng2.close()


class TestDifferentialAgainstRecompute(unittest.TestCase):
    """Random batches: incremental state must equal a full recompute."""

    def test_random_workload(self):
        rng = random.Random(20261116)
        eng = build_engine()
        live = {}  # order_id -> row
        for step in range(40):
            ops = []
            for _ in range(rng.randint(1, 6)):
                action = rng.choice(["insert", "update", "delete"])
                if action == "insert" or not live:
                    oid = rng.randint(1, 1000)
                    if oid in live:
                        continue
                    row = {
                        "order_id": oid,
                        "user_id": rng.randint(1, 5),
                        "amount": float(rng.randint(1, 500)),
                        "status": rng.choice(["ok", "ok", "test"]),
                    }
                    live[oid] = row
                    ops.append({"op": "insert", "row": row})
                elif action == "update":
                    oid = rng.choice(list(live))
                    row = dict(live[oid])
                    row["amount"] = float(rng.randint(1, 500))
                    row["status"] = rng.choice(["ok", "test"])
                    live[oid] = row
                    ops.append({"op": "update", "row": row})
                else:
                    oid = rng.choice(list(live))
                    ops.append({"op": "delete", "key": {"order_id": oid}})
                    del live[oid]
            if not ops:
                continue
            self.assertTrue(eng.apply_batch("orders", "step-%d" % step, ops))

            expected = {}
            for row in live.values():
                if row["status"] == "test":
                    continue
                agg = expected.setdefault(
                    row["user_id"],
                    {"cnt": 0, "total": 0.0, "min_amt": None, "max_amt": None},
                )
                agg["cnt"] += 1
                agg["total"] += row["amount"]
                agg["min_amt"] = (row["amount"] if agg["min_amt"] is None
                                  else min(agg["min_amt"], row["amount"]))
                agg["max_amt"] = (row["amount"] if agg["max_amt"] is None
                                  else max(agg["max_amt"], row["amount"]))
            actual = {
                r["user_id"]: {k: r[k]
                               for k in ("cnt", "total", "min_amt", "max_amt")}
                for r in eng.rows("per_user")
            }
            self.assertEqual(actual, expected, "mismatch at step %d" % step)
        eng.close()


if __name__ == "__main__":
    unittest.main()
