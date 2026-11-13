# 快照隔离事务服务（显式依赖图环检测）

从零实现的后端事务服务，Python 3 标准库 + SQLite（仅作持久化存储）。
事务读取开始时固定的旧快照，提交时通过写冲突检查 + 显式依赖图环检测，
阻止写偏差（write skew）与幻读环（phantom ring），即 SSI 风格的可串行化。

## 文件

| 文件 | 作用 |
| --- | --- |
| `transaction_service.py` | 服务本体：校验、快照、范围读、暂存、提交协议、环检测、持久化、CLI |
| `crash_driver.py` | 强退注入驱动，在提交协议的精确位置 `os._exit` / `SIGKILL` |
| `tests/test_acceptance.py` | 验收测试（184 项断言，15 个场景） |

仅依赖 Python 标准库（`sqlite3` 已内置）。运行环境 Python 3.11 / SQLite 3.40。

## 运行验收

```bash
python3 -I tests/test_acceptance.py
```

CLI 端到端（每行一个 JSON 请求，每行一个 JSON 应答）：

```bash
python3 -I transaction_service.py /path/to/svc.db <<'EOF'
{"op":"begin","tid":"X"}
{"op":"read_range","tid":"X","lo":"b","hi":"c"}
{"op":"stage","tid":"X","mapping":{"a":"1"}}
{"op":"commit","tid":"X"}
{"op":"snapshot"}
EOF
```

## 接口（`TransactionService`）

| 方法 | 语义 |
| --- | --- |
| `begin(tid)` | 开始事务，固定当前全局版本为快照 `s`；已存在的 id 返回原事务，绝不重置 |
| `read_range(tid, lo, hi)` | 读左闭右开 `[lo, hi)`，端点 `None` 为无穷；返回 `s` 时存在的行，按键升序；空读也登记依赖；暂存后禁止读 |
| `stage(tid, mapping)` | 一次性暂存写集，只能暂存一次；值为字符串、真实 `null`，或 `{"delete": true}` 墓碑；空写集合法 |
| `commit(tid)` | 提交协议，返回 `committed` / `write_conflict` / `cycle`；终态后重复调用回放原结局 |
| `abort(tid)` | 撤销打开的事务；幂等；终态不可逆 |
| `snapshot(version=None)` | 返回 `{"version", "rows"}`，行按 key 排序、只含存活行（墓碑与不存在的键不出现） |

事务 id 与键为非空 ASCII 字母数字串；`lo > hi`、未知 id、阶段违规等抛出 `ServiceError`。

## 提交协议（判定与发布在一个 SQLite 事务内原子完成）

1. **写冲突优先**：任一暂存键存在版本 `> s` 的成功写入（含墓碑、含无值变化的写），
   以 `write_conflict` 终结，不建图、不改数据、不动版本。
2. **建候选图**（节点 = 全部已提交事务 + 候选者）：
   - **ww 边**：同键相邻写者按提交次序（版本序）连边；
   - **rw 边**：对每个已登记读范围 `[lo, hi)` 内每个曾被成功写过的键：
     - 版本 `≤ s` 的最后写者 → 读者（墓碑也算写者）；
     - 版本 `> s` 的全部写者 ← 读者（反依赖，空读因此也能防幻影）；
     - 候选者暂存的键对任何覆盖它的读者同样产生 `读者 → 候选者` 反依赖边；
   - 跳过所有自边。
3. **环检测**：迭代式三色 DFS（避免深环递归限制），有环以 `cycle` 终结并返回
   任一完整环（事务 id 序列，首尾为同一 id）；无环则发布，版本加一（空提交也加一）。

失败与撤销：不入图、不改数据、不改版本；终态持久且不可反转。

## 持久化与崩溃恢复

SQLite 表（全部状态落盘，重启后打开事务可继续）：

- `meta`：全局版本（初始 0）；
- `txns`：事务 id、快照版本、状态、环、提交版本；
- `read_ranges`：每次范围读（含空读）；
- `staged_writes`：写集，终态后仍保留作为持久记录（真实 null 用 SQL NULL，墓碑用内部标记）；
- `committed_writes`：每次成功写入的完整历史 `(tid, version, key, value)`。

设置 `journal_mode=WAL`、`synchronous=FULL`；发布动作（版本递增、状态翻转、
历史写入）包在单个 `BEGIN IMMEDIATE ... COMMIT` 中。服务层用一把可重入锁
保证并发接口串行等价。

`crash_driver.py` 覆盖的强退点及恢复预期：

| 强退点 | 恢复后 |
| --- | --- |
| 暂存后、提交前 | 事务仍打开、写集保留，可重试提交 |
| 提交事务开启前（含真实 SIGKILL） | 版本/数据不变，事务可重试 |
| 提交事务内、COMMIT 前（含真实 SIGKILL） | SQLite 整体回滚，无部分写入，事务可重试 |
| COMMIT 返回后 | 写入与版本已持久，重复提交回放 `committed` |

## 验收覆盖

- 规范 X/Y 示例（X 先提交成功、Y 以环拒绝，环 `["X","Y","X"]`）；
- 8 事务长环（仅最后一个提交被拒，环含全部事务）；
- 空读幻影环、墓碑参与成环、真实 null 与墓碑的区分、删除不存在键；
- 只读事务提交（版本加一）及只读参与者在环上；
- 写冲突优先级高于环；无值变化仍算写并可致冲突；
- 终态不可反转、重复提交回放、重复 begin 返回原事务；
- 重启保留打开事务/写集/结局；五类强退（含两次真实 SIGKILL）后的数据、版本与重试结局；
- 240 并发事务串行等价、同键高争用下版本数恰等于提交数、同 id 并发 begin 一致；
- 全部参数与阶段校验。
