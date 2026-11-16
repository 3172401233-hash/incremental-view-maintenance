# incremental-view-maintenance

Python 增量物化视图维护库：上游事实表以**带幂等键的批次**（insert / update /
delete）进入，库沿依赖图把变化以增量（delta）方式传播到中间清洗视图和最终
汇总表，避免整表重算。视图状态和 checkpoint 全部持久化在 SQLite 中。

## 快速示例

```python
from ivm import IVMEngine, Filter, Aggregate, Count, Sum, Min, Max

eng = IVMEngine("state.db")
eng.create_source("orders", key=["order_id"], columns={
    "order_id": "INTEGER", "user_id": "INTEGER",
    "amount": "REAL", "status": "TEXT",
})
eng.add_view("clean_orders",
             Filter("orders", lambda r: r["status"] != "test"))
eng.add_view("per_user",
             Aggregate("clean_orders", group_by=["user_id"], aggs={
                 "cnt": Count(), "total": Sum("amount"),
                 "min_amt": Min("amount"), "max_amt": Max("amount"),
             }))

eng.apply_batch("orders", "batch-001", [
    {"op": "insert", "row": {"order_id": 1, "user_id": 10,
                             "amount": 100.0, "status": "ok"}},
    {"op": "update", "row": {"order_id": 1, "user_id": 10,
                             "amount": 150.0, "status": "ok"}},
    {"op": "delete", "key": {"order_id": 1}},
])
eng.rows("per_user")   # 物化汇总结果
```

## 工作原理

- **多重集（bag）语义**：每个关系（源表或视图）在 SQLite 中存为行 + 计数，
  变化以 `(row, ±count)` 的 delta 沿依赖图拓扑序传播。
- **幂等批次**：`apply_batch(source, batch_id, ops)` 先查 `__ivm_batches`，
  同 `(source, batch_id)` 的批次直接跳过并返回 `False`。
- **原子提交**：源表状态、所有下游视图状态、MIN/MAX 支撑表和 checkpoint
  在同一个 SQLite 事务里提交；任何异常整体回滚，失败重试不会重复扣减，
  也不会只提交依赖链的一半。崩溃后用同一状态文件重建引擎即可续跑。
- **checkpoint**：`__ivm_batches` 记录每个已应用批次的单调序号
  （`eng.last_seq()` / `eng.applied_batches()`）。

## 支持的 ETL 算子

| 算子 | 说明 |
| --- | --- |
| `Filter(input, predicate)` | 选择（WHERE），谓词为行字典上的 Python 函数 |
| `Project(input, mapping)` | 投影 / 列重命名 |
| `Join(left, right, on)` | 内连接（等值连接），支持菱形依赖（同一批次两侧同时变化，不会重复计数） |
| `Union(inputs)` | 多重集并集（输入 schema 必须一致） |
| `Aggregate(input, group_by, aggs)` | 分组聚合，见下方撤回策略 |

聚合函数的撤回（retraction）方式分别处理：

- **COUNT**：组计数器递减，减到 0 时整组删除。
- **SUM**：直接减去被撤回行的值。
- **AVG**：以 SUM / COUNT 增量维护。
- **MIN / MAX**：为每个组维护一个值的多重集支撑表
  （`__ivmsup_<view>_<col>`），撤回时递减对应值的支撑计数；当前最值的支撑
  归零后从剩余值中重算最值。**删除最后一条明细后整组移除，MIN/MAX 不会
  残留旧值。**

## 不可增量计算的表达式

以下表达式 / 语义不在本库的增量维护范围内，需要整表重算或改写：

- **非确定性表达式**：`random()`、`datetime('now')`、自增序列等——同一行
  重放结果不同，无法构造可撤回的 delta。
- **窗口函数**（`ROW_NUMBER`、`RANK`、`LAG/LEAD` 等）：单行变化可能引起
  任意多行的排名变化。
- **`COUNT(DISTINCT ...)` / `SUM(DISTINCT ...)`**：当前仅支持普通多重集
  聚合；DISTINCT 聚合需要额外的去重支撑结构，未实现。
- **外连接 / 反连接**（LEFT/RIGHT/FULL JOIN、NOT EXISTS）：撤回一侧可能
  触发另一侧行的补空/消失，当前只支持内连接。
- **`ORDER BY` / `LIMIT`（Top-K）**：属于展示层语义，不是可维护的关系
  状态；请在查询物化结果时自行排序截断。
- **相关子查询、递归 CTE**：无法用局部 delta 规则表达。
- **跨 join 两侧列的聚合表达式**（如 `MIN(a+b)` 中 `a`、`b` 分别来自
  join 两侧并被独立更新）：请先用 `Project` 物化出派生列再聚合。
- **用户自定义聚合函数**：只有 `Count/Sum/Avg/Min/Max` 有内置撤回实现。

## 运行测试

```bash
python -m unittest
```

测试覆盖：共享子视图（一个中间视图喂多个下游）、多级依赖
（orders → 清洗 → join → 聚合）、重复批次幂等跳过、删除最后明细后
MIN/MAX 不留旧值、删除后重新插入恢复、模拟崩溃回滚与断点续跑、
菱形 join 双侧同批变化，以及随机工作负载下与全量重算的差分对比。
