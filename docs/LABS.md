# SynthFlow 双语实验手册 / Bilingual Lab Workbook

[教学首页 / Index](README.md) · [概念导读 / Guide](TEACHING_GUIDE.md)

## 实验约定 / Lab conventions

**中文**：所有命令从仓库根目录运行，面向 macOS/Linux 的 Bash 或 Zsh。需要 Rust 1.93+ 和 Python 3；首次下载依赖需要网络，实验运行只使用 mock 或回环 HTTP 服务，不需要外部模型账号。实验 1–5 每次使用新的临时目录，避免与原有输出冲突。实验结束后可自行检查和清理打印出的目录。

**English**: Run all commands from the repository root using Bash or Zsh on macOS/Linux. Rust 1.93+ and Python 3 are required. Initial dependency downloads require a network connection, but the labs use mock providers or loopback HTTP servers and require no external model account. Labs 1–5 use fresh temporary directories to avoid collisions with existing output. Inspect and clean up the printed directories when finished.

**中文**：临时目录只复制采用 inline 输入的 `reliable_generation.yaml`。不能照搬此方法复制 JSONL 示例而不带源文件，因为相对源路径会随配置目录变化。

**English**: The temporary directories copy only `reliable_generation.yaml`, which uses inline input. Copying a JSONL example without its input file would break relative source paths.

```bash
cargo build --workspace
cargo run -q -p synthflow-cli -- --help
```

**预期 / Expected**：help 包含 validate、plan、run、resume 和 inspect。 / Help lists validate, plan, run, resume, and inspect.

## 实验 1：观察完整数据流 / Lab 1: Observe the complete data flow

**目标 / Goal**：把配置、处理结果、拒绝记录和提交状态对应起来。 / Relate configuration to accepted rows, rejected rows, and commit state.

```bash
LAB_DIR="$(mktemp -d)"
export LAB_DIR
cp examples/reliable_generation.yaml "$LAB_DIR/pipeline.yaml"
# Show the directory / 显示实验目录
printf '%s\n' "$LAB_DIR"
cargo run -q -p synthflow-cli -- validate "$LAB_DIR/pipeline.yaml"
cargo run -q -p synthflow-cli -- plan "$LAB_DIR/pipeline.yaml"
cargo run -q -p synthflow-cli -- run "$LAB_DIR/pipeline.yaml" > "$LAB_DIR/report.json"
python3 - <<'PY'
import json, os
from pathlib import Path
root = Path(os.environ['LAB_DIR'])
report = json.loads((root / 'report.json').read_text())
output = Path(report['output_path'])
accepted = [json.loads(line) for line in output.read_text().splitlines()]
rejected = [json.loads(line) for line in Path(report['dead_letter_path']).read_text().splitlines()]
assert report['status'] == 'completed'
assert len(accepted) == 2 and len(rejected) == 1
assert report['sink_state']['committed_source_position'] == 3
assert output.stat().st_size == report['sink_state']['accepted_bytes']
assert not Path(report['partial_path']).exists()
print('status:', report['status'])
print('accepted:', len(accepted), 'rejected:', len(rejected))
print('diagnostic:', rejected[0]['diagnostic'])
PY
```

**预期 / Expected**：读取 3 条、接受 2 条、拒绝 1 条；错误路径为 `/answer`。 / Three records read, two accepted, one rejected; the diagnostic path is `/answer`.

**解释 / Explanation**：第二条记录的 answer 是数字 42，Schema 要求字符串。允许拒绝数为 1，最终拒绝比例 1/3 小于 0.5，所以整个运行仍然成功。 / The second answer is the number 42, but the schema requires a string. The policy permits one rejection, and the final ratio 1/3 is below 0.5, so the run succeeds.

## 实验 2：同一条错误，不同失败策略 / Lab 2: One error, different failure policies

**目标 / Goal**：理解拒绝记录与运行失败的区别。 / Distinguish record rejection from run failure.

```bash
LAB_DIR="$(mktemp -d)"
export LAB_DIR
cp examples/reliable_generation.yaml "$LAB_DIR/pipeline.yaml"
# Failure is expected; capture its exit status explicitly / 此处预期失败，显式保存退出码
if cargo run -q -p synthflow-cli -- run --strict "$LAB_DIR/pipeline.yaml" > "$LAB_DIR/report.json"; then
    RUN_EXIT=0
else
    RUN_EXIT=$?
fi
printf 'exit=%s\n' "$RUN_EXIT"
python3 - <<'PY'
import json, os
from pathlib import Path
report = json.loads((Path(os.environ['LAB_DIR']) / 'report.json').read_text())
assert report['status'] == 'failed'
assert report['processed_records_total'] == 2
assert report['accepted_records_total'] == 1
assert report['rejected_records_total'] == 1
assert report['sink_state']['committed_source_position'] == 2
assert not Path(report['output_path']).exists()
assert Path(report['partial_path']).exists()
print(report['status'], report['errors'][0]['category'])
PY
```

**预期 / Expected**：退出码 1，状态 failed，保留 partial，正式文件不存在。 / Exit code 1, failed status, partial retained, no final output.

**解释 / Explanation**：strict 在第二条被拒绝并提交后停止。读取数可能包含已经进入并发窗口的后续记录，不能用读取数推断提交进度。 / Strict mode stops after the second record is rejected and committed. Read counts can include later records already admitted into the concurrency window and cannot be used as commit positions.

**加练 / Variation**：每次新建实验目录，把 `max_failed_records` 改为 0，或把 `max_failed_ratio` 改为 0.2，并去掉 `--strict`。前者在第二条后失败，后者在全部处理完后失败。 / In fresh directories, set `max_failed_records` to 0 or `max_failed_ratio` to 0.2 and omit `--strict`. The count limit fails after record two; the ratio limit fails after all records are processed.

## 实验 3：让配置错误可以定位 / Lab 3: Locate configuration errors

**目标 / Goal**：观察有字段路径的安全错误，而不回显错误值。 / Observe a safe error with a field path without echoing the invalid value.

```bash
LAB_DIR="$(mktemp -d)"
export LAB_DIR
python3 - <<'PY'
import json, os
from pathlib import Path
# JSON is valid YAML, so no Python YAML dependency is needed.
# JSON 是有效的 YAML 子集，无需安装 Python YAML 依赖。
config = {
    'version': 1, 'dataset': {'name': 'bad_config'},
    'source': {'type': 'inline', 'records': []},
    'providers': {'mock': {'type': 'mock', 'response': '{}'}},
    'generate': {'provider': 'mock', 'prompt': 'hello', 'output_schema': {'type': 'object'}},
    'output': {'format': 'jsonl', 'path': 'output/data.jsonl'},
    'errors': {'max_failed_records': 'PRIVATE_INVALID_VALUE'}
}
(Path(os.environ['LAB_DIR']) / 'pipeline.yaml').write_text(json.dumps(config))
PY
if cargo run -q -p synthflow-cli -- validate "$LAB_DIR/pipeline.yaml" > "$LAB_DIR/report.json"; then
    RUN_EXIT=0
else
    RUN_EXIT=$?
fi
printf 'exit=%s\n' "$RUN_EXIT"
python3 - <<'PY'
import json, os
from pathlib import Path
root = Path(os.environ['LAB_DIR'])
text = (root / 'report.json').read_text()
report = json.loads(text)
assert report['preflight'] is True
assert 'errors.max_failed_records' in text
assert 'PRIVATE_INVALID_VALUE' not in text
assert not (root / 'output' / 'data.jsonl').exists()
print(report['errors'][0]['message'])
PY
```

**解释 / Explanation**：max_failed_records 要求无符号整数或缺省值。预检失败尚未进入记录处理，因此没有正常运行的逐条统计。 / max_failed_records requires an unsigned integer or omission. Preflight failed before record processing, so there are no normal per-record run statistics.

**加练 / Variation**：把 version 改成 2，或把 generate.provider 改成不存在的名称，再比较错误。 / Change version to 2 or use a nonexistent generate.provider and compare the errors.

## 实验 4：修复一条 Schema 失败记录 / Lab 4: Repair a schema-invalid record

**目标 / Goal**：保持 Schema 不变，修正数据类型。 / Fix the data type while keeping the schema unchanged.

```bash
LAB_DIR="$(mktemp -d)"
export LAB_DIR
python3 - <<'PY'
import os
from pathlib import Path
text = Path('examples/reliable_generation.yaml').read_text()
assert 'answer: 42' in text
text = text.replace('answer: 42', "answer: 'Borrowing allows access without transferring ownership.'")
(Path(os.environ['LAB_DIR']) / 'pipeline.yaml').write_text(text)
PY
cargo run -q -p synthflow-cli -- run "$LAB_DIR/pipeline.yaml" > "$LAB_DIR/report.json"
python3 - <<'PY'
import json, os
from pathlib import Path
report = json.loads((Path(os.environ['LAB_DIR']) / 'report.json').read_text())
assert report['status'] == 'completed'
assert report['accepted_records_total'] == 3
assert report['rejected_records_total'] == 0
print('accepted=3 rejected=0')
PY
```

**讨论 / Discuss**：直接把 Schema 从 string 改成 number 也会改变行为，但那是在修改数据契约，而非修复契约下的数据。什么时候应该修改 Schema？ / Changing the schema from string to number also changes behavior, but changes the contract rather than repairing data under the existing contract. When should the schema change?

**参考 / Reference**：当业务明确需要数值答案，且下游消费者接受新契约时；不能只为消除报错而放宽约束。 / When the domain explicitly requires numeric answers and downstream consumers accept the new contract, not merely to suppress errors.

## 实验 5：验证禁止覆盖 / Lab 5: Verify no-clobber behavior

**目标 / Goal**：通过实际文件字节证明重复执行没有覆盖已有结果。 / Prove through file bytes that rerunning does not overwrite existing output.

```bash
LAB_DIR="$(mktemp -d)"
export LAB_DIR
cp examples/reliable_generation.yaml "$LAB_DIR/pipeline.yaml"
cargo run -q -p synthflow-cli -- run "$LAB_DIR/pipeline.yaml" > "$LAB_DIR/first.json"
cp "$LAB_DIR/output/reliable_demo.jsonl" "$LAB_DIR/before.jsonl"
if cargo run -q -p synthflow-cli -- run "$LAB_DIR/pipeline.yaml" > "$LAB_DIR/second.json"; then
    RUN_EXIT=0
else
    RUN_EXIT=$?
fi
printf 'second exit=%s\n' "$RUN_EXIT"
python3 - <<'PY'
import json, os
from pathlib import Path
root = Path(os.environ['LAB_DIR'])
assert (root / 'before.jsonl').read_bytes() == (root / 'output/reliable_demo.jsonl').read_bytes()
assert json.loads((root / 'second.json').read_text())['preflight'] is True
print('Existing output is unchanged / 已有输出未改变')
PY
```

**解释 / Explanation**：输出命名空间被已有文件占用，第二次运行在预检中失败。这不是续跑。成功输出文件即使被移走，留下的 manifest 或 dead-letter 仍会阻止同一命名空间的新运行。 / Existing artifacts occupy the output namespace, so the second run fails in preflight. This is not resume. Even if the final output is moved away, a remaining manifest or dead-letter file prevents a new run using that namespace.

## 实验 6：不用真实模型测试 HTTP / Lab 6: Test HTTP without a real model

**目标 / Goal**：用已有本地 HTTP fixture 验证重试和并发。 / Use the existing local HTTP fixtures to verify retry and concurrency behavior.

```bash
cargo test -p synthflow --test reliability http_retries_transient_errors_and_tracks_usage -- --exact
cargo test -p synthflow --test reliability permanent_http_errors_are_not_retried -- --exact
cargo test -p synthflow --test reliability timeouts_exhaust_bounded_retries -- --exact
cargo test -p synthflow --test reliability concurrent_calls_are_bounded_and_output_is_in_source_order -- --exact
cargo test -p synthflow --test reliability provider_semaphore_limits_callers_outside_the_engine -- --exact
```

**预期 / Expected**：每个命令各执行 1 个测试并通过。 / Each command runs exactly one passing test.

**阅读 / Read**：[reliability.rs](../crates/synthflow/tests/reliability.rs) 中的 `Reply`、`HttpState`、`handler` 和 `server`。429 → 503 → 200 的序列应产生 3 次请求、2 次重试。 / Inspect `Reply`, `HttpState`, `handler`, and `server` in the test file. A 429 → 503 → 200 sequence should produce three requests and two retries.

**思考 / Think**：为什么需要“绕过引擎直接调用 provider”的测试？ / Why test direct provider calls that bypass the engine?

**参考 / Reference**：只通过引擎调用时，即使 provider 的 semaphore 失效，引擎的窗口上限仍可能让测试通过。直接调用能独立验证 provider 的并发契约。 / When called only through the engine, its window limit can hide a broken provider semaphore. Direct calls independently test the provider's concurrency contract.

## 实验 7：中断与持久化前缀 / Lab 7: Interruption and durable prefixes

**目标 / Goal**：区分可处理的取消与无法处理的强制终止。 / Distinguish handled cancellation from unhandled forced termination.

```bash
# Unix-only SIGINT test / 仅 Unix 的 SIGINT 测试
cargo test -p synthflow-cli --test faults sigint_flushes_and_reports_cancelled_with_exit_130 -- --exact
cargo test -p synthflow-cli --test faults forced_termination_leaves_only_partial_and_last_durable_position -- --exact
cargo test -p synthflow --test reliability manifest_write_failure_preserves_data_and_returns_failure_report -- --exact
cargo test -p synthflow --test reliability publish_race_never_clobbers_another_writers_output -- --exact
```

**预期 / Expected**：测试提交第一条记录，让第二个 HTTP 请求保持在途，然后向子进程发信号。SIGINT 得到退出码 130 和 cancelled 清单；强制终止后清单仍为 running，但已经提交的位置和字节仍可检查。 / The tests commit the first record, leave the second HTTP request in flight, then signal the child process. SIGINT produces exit code 130 and a cancelled manifest. Forced termination leaves a running manifest whose last committed position and bytes can still be inspected.

**解释 / Explanation**：运行状态是持久化观测，不是进程存活探针。这些测试控制故障位置，不等同于真实断电测试。 / Run status is a persisted observation, not a process-liveness probe. These tests control failure locations; they are not power-loss tests.

**思考 / Think**：如果 partial 比 manifest.accepted_bytes 长，恢复程序能否直接把整个文件当作已提交结果？ / If partial is longer than manifest.accepted_bytes, can recovery treat the whole file as committed?

**参考 / Reference**：不能。超出部分可能处于未提交窗口；当前 `resume` 只接受 failed/cancelled 清单，并将 partial 和 dead-letter 截断到清单记录的已提交字节数。SIGKILL 后若清单仍为 running/publishing，不能直接运行 `resume`。 / No. Extra bytes can be uncommitted. Current `resume` accepts failed/cancelled manifests and truncates partial/dead-letter files to committed byte counts. A running/publishing manifest left by SIGKILL cannot be resumed directly.

## 实验 8：验证已实现的高级功能 / Lab 8: Verify implemented advanced features

**目标 / Goal**：用已有回归测试观察 CSV、重生成、限流、judge、去重、Parquet、恢复和 inspect 的行为。 / Use existing regression tests to observe CSV, regeneration, rate limiting, judging, deduplication, Parquet, resume, and inspect.

```bash
cargo test -p synthflow --test csv csv_source_handles_quotes_and_crlf_and_keeps_strings -- --exact
cargo test -p synthflow --test regeneration invalid_then_valid_output_is_repaired_and_accepted -- --exact
cargo test -p synthflow --test ratelimit every_retry_passes_the_request_rate_limit -- --exact
cargo test -p synthflow --test judge judge_scores_generated_records_and_appends_evidence -- --exact
cargo test -p synthflow --test dedup exact_duplicates_are_rejected_in_commit_order -- --exact
cargo test -p synthflow --test minhash near_duplicate_answers_are_deduplicated_end_to_end -- --exact
cargo test -p synthflow --test remediation parquet_resume_keeps_the_original_schema_constraints -- --exact
cargo test -p synthflow --test remediation concurrent_resumes_allow_exactly_one_writer -- --exact
cargo test -p synthflow --test inspect mixed_types_and_unsupported_extensions_are_reported -- --exact
```

**预期 / Expected**：各命令运行一个测试并通过。观察拒绝类别与已提交条数，并解释为什么 Parquet Schema 和去重状态必须跨 resume 保持一致。 / Each command runs one passing test. Inspect rejection categories and committed counts, then explain why the Parquet schema and deduplication state must survive resume unchanged.

**动手使用 inspect / Try inspect**：用独立临时目录生成数据，再查看统计。 / Generate data in a separate temporary directory, then inspect its statistics.

```bash
LAB_DIR="$(mktemp -d)"
cp examples/reliable_generation.yaml "$LAB_DIR/pipeline.yaml"
cargo run -q -p synthflow-cli -- run "$LAB_DIR/pipeline.yaml" > "$LAB_DIR/report.json"
cargo run -q -p synthflow-cli -- inspect "$LAB_DIR/output/reliable_demo.jsonl" --json
```

**讨论 / Discuss**：混合类型列中的非数值项能否参与数值均值的分母？ / Should non-numeric values in a mixed column contribute to the numeric mean's denominator?

**参考 / Reference**：不能。当前实现仅用数值项计算 min/max/mean；没有数值项时 mean 为 null。 / No. The implementation calculates min/max/mean from numeric values only; mean is null when there are no numeric values.

## 实验 9：设计并实现一个扩展 / Lab 9: Design and implement an extension

**中文**：任选一项。先提交设计说明，再修改代码。以下均为学生作业，不是当前已有功能。

**English**: Choose one assignment. Write a design note before changing code. These are student extensions, not existing features.

| 作业 / Assignment | 要回答的问题 / Design question | 验收要求 / Acceptance criteria |
| --- | --- | --- |
| 批量提交 / Batched commits | 吞吐提升会扩大多大的未提交窗口？ / How much does higher throughput enlarge the uncommitted window? | 正常结束 flush 尾批；故障测试证明清单不引用未同步数据 / Flush the final batch; fault tests prove manifests never refer to unsynchronized data |
| 崩溃状态恢复 / Crash-state recovery | 如何安全处理 SIGKILL 留下的 running/publishing 清单？ / How can a running/publishing manifest left by SIGKILL be recovered safely? | 校验锁与目录项，测试发布中断及连续/恢复输出一致 / Check locks and directory entries; test publication interruption and output equivalence |
| SQL 查询 / SQL querying | 如何在 JSONL/Parquet 上限制查询资源？ / How should queries over JSONL/Parquet be bounded? | 查询结果正确，错误可诊断，避免无界内存 / Correct results, diagnosable errors, bounded memory |

**中文**：验收材料应包含：问题、状态或数据模型、失败窗口、测试命令、已知限制。只有性能数字而没有正确性依据，不能证明扩展完成。

**English**: Submit the problem statement, state/data model, failure windows, test commands, and known limits. Performance numbers without correctness evidence do not establish completion.

## 完成检查 / Completion checklist

- 能解释实验 1 为何整体成功、实验 2 为何整体失败。 / Explain why lab 1 succeeds and lab 2 fails.
- 能定位一条记录的 ID、Schema 错误路径和提交位置。 / Locate a record ID, schema error path, and commit position.
- 能说明重试与重新生成、并发限制与速率限制的区别。 / Distinguish retry from regeneration and concurrency from rate limiting.
- 能说明 `resume` 的适用状态、检查点与锁，以及为什么 SIGKILL 后仍可能需要人工检查。 / Explain resume's allowed states, checkpoint and lock, and why SIGKILL can still require manual inspection.

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
```
