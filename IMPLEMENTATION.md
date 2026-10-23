# 实现记录

本文件按时间保留各轮交付记录，下方早期阶段中的限制不代表当前状态。当前能力与待办以 [README](README.md) 和 [能力状态](docs/STATUS.md) 为准；最初一轮范围遵循设计文档第 53 节的 Phase 0、1、2。

## Phase 0 — Workspace Bootstrap

- 文件：根 `Cargo.toml` / `Cargo.lock`、两个 crate 的 manifest、`lib.rs`、`main.rs`、`.gitignore`、`LICENSE`、CI 配置。
- 架构：核心库和 CLI 分离；clap 提供命令结构，Tokio 提供应用入口，tracing 记录运行事件。先采用模块化单库，避免过早拆成七个小 crate。
- 测试：CLI help、命令退出码；workspace 构建。
- 命令：`cargo build --workspace`、`cargo test --workspace`、`cargo run -p synthflow-cli -- --help`。
- 限制：此阶段只建立工程基础；后续 Phase 1 建立 DSL。

## Phase 1 — Pipeline Specification

- 文件：`spec.rs`、`error.rs`、`template.rs`、CLI validate/plan、`tests/pipeline.rs`、`tests/cli.rs`。
- 架构：serde 严格反序列化；MiniJinja 校验模板；jsonschema 编译结构契约；路径相对 YAML 解析；BLAKE3 配置 hash；未知字段和版本报错。
- 测试：版本、provider 引用、模板语法、Schema 语法/引用、保留输入字段、路径解析、配置 hash、OpenAI-compatible 基础参数。
- 命令：`cargo run -p synthflow-cli -- validate examples/simple_generation.yaml`、`cargo run -p synthflow-cli -- plan examples/simple_generation.yaml`。
- 限制：校验不会扫描整个 JSONL；读取过程中验证每条源记录。HTTP provider 配置可以验证但不能执行；只支持 JSONL sink。后续 Phase 2 打通离线执行链路。

## Phase 2 — Source + Prompt + Mock Generation

- 文件：`source.rs`、`provider.rs`、`record.rs`、`engine.rs`、示例 YAML/JSONL、端到端测试、README。
- 架构：顺序流式读取/生成/校验/写出，自然施加背压；模板和 Schema 在记录循环前编译；mock response 使用显式输入上下文；输入字段保留并追加 generated/meta；记录 ID 使用规范 JSON 和逻辑位置。
- 测试：JSONL 完整链路和重复运行结果一致性、inline 输入、无效 JSON/Schema/模板的分类统计、源错误时 flush 已接受前缀、输出防覆盖、空输入、Unicode/引号、嵌套 Schema、模板执行/输出上限与超大源行。
- 命令：`cargo run -p synthflow-cli -- run examples/simple_generation.yaml`、`cargo fmt --all -- --check`、`cargo clippy --workspace --all-targets --all-features -- -D warnings`、`cargo test --workspace`。
- 限制：不支持重试、dead-letter、run manifest、源指纹、checkpoint 或 resume。中断后的文件只是一份部分输出，不可直接作为续跑状态。统计是当前进程的基础计数，无 token/cost 指标；mock 不模拟真实模型质量。
- 下一阶段：Phase 3 HTTP provider，随后按设计顺序增加结构化输出策略、限流、重试、judge 和持久化状态。

## 本轮改进 — 可靠性与 Phase 3

以下条目取代前述阶段记录中的对应历史限制；Phase 0–2 记录保留作为第一轮交付说明。

1. **完成输出发布**：`run.rs` 管理 partial、dead-letter 和 manifest；create_new 保留命名空间，同步数据后用原子 manifest 替换记录提交位置，同目录硬链接发布最终文件且不覆盖已有目标。
2. **失败报告**：`error.rs` 定义安全结构化诊断；`engine.rs` 无论完成/失败/取消均保留报告，CLI 输出 JSON 和正确退出码；Schema 诊断包含 instance/schema 路径。
3. **失败阈值**：`ErrorPolicy` 支持最大拒绝数、最终拒绝比例和 strict；`run --strict` 覆盖配置，非空数据全部被拒绝始终失败。
4. **配置校验**：serde_path_to_error 提供字段路径；规范化文件路径；Schema 引用检测只遍历 Schema 位置，避免 `$ref` 普通属性误报；拒绝源与运行产物路径冲突。
5. **异步 provider**：`async_trait` + `Arc<dyn LlmProvider>`，支持注入；OpenAI-compatible reqwest 实现使用独立 semaphore、整体单次超时、有限次数的指数退避/抖动和 cancellation token；记录真实请求、重试和 token usage。CLI 第一信号取消，第二信号强制退出。
6. **恢复基础**：UUID run ID、源 BLAKE3 指纹、起止时间、运行状态、已提交记录数和字节数；源前后指纹不一致禁止发布。FuturesOrdered 保持有限并发和源顺序提交。尚未提供自动 resume。
7. **故障验证**：保留并更新原有测试；新增 reliability 测试和 CLI faults 测试，覆盖本地 HTTP、401/403 不重试、429/503 重试、超时、并发上限、取消、鉴权、进程 SIGINT/SIGKILL、拒绝策略、Schema 诊断、路径等价性、源变化、防覆盖及写入失败。

新增示例：`reliable_generation.yaml`、`openai_generation.yaml`。README 描述新文件状态、命令、配置、退出码、统计语义及取消/崩溃限制。

验证命令：

```bash
cargo build --workspace
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
cargo run -p synthflow-cli -- validate examples/openai_generation.yaml
cargo run -p synthflow-cli -- plan examples/openai_generation.yaml
cargo run -p synthflow-cli -- run examples/reliable_generation.yaml
```

约束：最终文件和清单不是跨文件事务；硬中断可能留下 `publishing` 状态，必须联合检查完成状态。每条记录同步确保清晰提交边界，但会牺牲本地吞吐量；同步文件 I/O 尚未拆到独立有界 worker。

## 第二轮交付 — Phase 4–13（结构化输出重生成之后的全阶段）

以下条目在 `main` 分支按阶段各成一个 commit；每个 commit 均通过 fmt / clippy -D warnings / 全量测试。

1. **Phase 4 结构化输出重生成**（`regenerate_on_invalid`，0–3 次修复重试，反馈只含规则路径；HTTP 附加在 user message，mock 经 `feedback` 变量注入；`_meta.attempt` 汇总全部调用）。
2. **Phase 5 RPM/TPM 限流**（60 秒滑动窗口，在发送前联合检查 RPM/TPM；每次重试计入，超额估算拒绝；实际 usage 修正原事件；等待配额时释放 semaphore；tokio paused-time 单测验证窗口行为）。
3. **Phase 6 Judge**（第二个 provider 渲染 judge prompt，解析数值分数并按 `min_score` 过滤；`judge_score`/`judge_output` 诊断；通过记录追加 `judge` 证据字段与元数据；mock judge 可模板化 `generated`）。
4. **Phase 7 精确去重**（`method: exact`：点路径字段提取 + 可选 lowercase 规范化 + BLAKE3 键；`duplicate`/`dedup_field` 拒绝类别；`restore()` 供 resume 重建状态）。
5. **Phase 8 MinHash 去重**（`method: minhash`：词 n-gram shingle、成对独立置换签名（默认 128 维）、LSH 分带候选查找、签名相同分量比例近似 Jaccard；阈值/维数/带数/shingle 全部校验）。
6. **CSV source**（RFC 4180 流式解析，表头命名、全字符串值、行宽校验、8 MiB 行限制；fingerprint 与 artifact 冲突防护与 JSONL 一致）。
7. **Phase 9 Parquet sink**（`output.format: parquet` + `batch_size`。关键设计：**partial 始终是 JSONL，默认逐条同步**，因此 resume 语义与 JSONL 完全一致；每条记录对照首条推断的 Arrow schema 校验，schema 外字段或类型冲突按记录拒绝（`sink_row`），绝不静默丢列；发布时在独立 `.synthflow-publish-*` 临时文件中转换，保存发布摘要后原子链接）。
8. **Phase 10/11 Checkpoint 消费 + Resume**（`synthflow resume`：校验配置 hash、源指纹与终态；partial/dead-letter 截断到提交字节前缀后追加；dedup 状态从前缀重建；跳过已提交源位置，确定性续跑不重复输出；新 manifest 记录 `resumed_from`；失败策略按全数据集计算）。
9. **Phase 12 指标**（按返回的逻辑 provider 调用统计耗时，包含失败、重生成与 judge；均值增量计算，p50/p95/p99 使用至多 4096 项蓄水池样本；引擎侧 prompt/completion token 总量含 judge 调用；可选 `pricing` 输出 `estimated_cost_usd`；每 500 条或 2 秒的 progress 日志事件）。
10. **Phase 13 Inspect**（`synthflow inspect <jsonl|parquet> [--json]`：行数、字节、列类型、非空/空值（缺失键计空值）、数值 min/max/mean、上限一百万的去重基数；Parquet 经 Arrow 批解码；SQL 查询未实现）。

新增示例：`judge_generation.yaml`、`csv_generation.yaml`（含 `topics.csv`）、`parquet_generation.yaml`。

验证命令：

```bash
cargo build --workspace
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
cargo run -p synthflow-cli -- run examples/judge_generation.yaml
cargo run -p synthflow-cli -- run examples/csv_generation.yaml
cargo run -p synthflow-cli -- run examples/parquet_generation.yaml
cargo run -p synthflow-cli -- inspect examples/output/rust_instruction.parquet
```

已知限制：DataFusion SQL 查询、分布式执行（设计文档第 47 节之后）、有界 source/sink blocking worker 文件 I/O 未实现。教学文档（docs/）已更新，覆盖 Phase 0–13 的主要行为与边界。


## 后续可靠性整改 / Reliability follow-up

- 联合 RPM/TPM 准入，超额预估拒绝，同一事件修正 usage 并保留原到期时间；同名 generator/judge 共享状态。 / Joint RPM/TPM admission, oversized-estimate rejection, usage correction at the original expiry, and shared state for the same generator/judge provider.
- CSV 使用 csv-core 的有界缓冲区，在解析时限制记录/表头大小和字段数，拒绝重复或空表头。 / Bounded csv-core buffers limit record/header bytes and field counts during parsing, rejecting duplicate or empty headers.
- 独占锁下恢复 running/publishing；最终文件通过发布前持久化的 digest 验证。 / Running/publishing recovery under an exclusive lock, verifying final output against a digest persisted before publication.
- 逐行重建 dedup，JSONL/CSV 保存字节偏移并 seek；旧清单兼容。 / Streaming dedup rebuild, persisted JSONL/CSV byte offsets and seek, with legacy checkpoint compatibility.
- 延迟按返回的逻辑调用采样，包含失败、重生成和 judge，新增 latency_calls_total。 / Latency samples cover returned logical calls, including failures, regeneration, and judging, with latency_calls_total.

SIGKILL 可能留下不会阻止恢复的孤立转换临时文件；缺少发布摘要的旧 publishing 清单若已有最终文件仍需人工检查。 / SIGKILL may leave orphan conversion files that do not block recovery; legacy publishing manifests with final output but no digest still require manual inspection.

## 教学与资源边界改进 / Teaching and resource follow-up

- 发布、摘要及发布恢复移入 blocking worker，转换和摘要响应取消，worker 退出前持有锁。 / Publication, hashing, and publication recovery use blocking workers with cooperative cancellation and lock retention.
- Provider HTTP 指标按名记录并汇总；返回 token 按名计费，支持 provider_pricing 覆盖默认价格，缺价格总量为 null。 / Named HTTP metrics and returned-token costs support per-provider price overrides; missing prices yield a null total.
- 去重索引默认 64 MiB 记账预算；inspect 使用固定摘要、共享预算和行/列限制。 / Dedup uses a default 64 MiB accounting budget; inspect uses fixed digests and shared state/row/column limits.
- MinHash 统一大小写和空白；output.commit_every 默认 1，正常完成提交尾批，故障回滚未提交尾部。 / MinHash normalizes case/whitespace consistently; commit_every defaults to 1, flushes final tails and rolls back uncommitted tails on failure.
- scripts/benchmark.py 测量吞吐、峰值 RSS 与 SIGKILL 恢复；scripts/check_labs.py 执行教材原文并验证测试非空。CI 覆盖两系统、stable 与 Rust 1.93。 / Scripts measure throughput/RSS/crash recovery and execute actual workbook blocks; CI covers two operating systems and stable/Rust 1.93.

限制 / Limits：预算非严格 RSS 沙箱；源读取和批次提交仍同步；配置变化需新运行；开始最终链接事务后不在事务中途取消。 / Budgets are not strict RSS sandboxes; source reads and checkpoint commits remain synchronous; configuration changes require a new run; the final linking transaction is not interrupted midway.


## 正确性与可用性完善（2026-10-22）

- 修复 exact lowercase 的对象键碰撞；支持中文字符 n-gram 与可重建 SQLite exact/MinHash 索引。
- HTTP 已返回 usage 在内容校验前记录，失败/重试计费；新增生成上限、采样参数、seed 和三种 response_format，TPM预留包含反馈/输出。
- Inspect 整数 extrema 保留 Number 类型，均值采用防溢出增量算法。
- 显式 Parquet 输出契约支持嵌套、可选/可空字段；运行/发布保持固定类型，拒绝截断和有损提升。
- 源与存储使用专用线程及容量1邮箱，独立心跳、SIGTERM收尾；小Tokio阻塞池不会被空闲worker耗尽。
- 检查点增量维护两份前缀摘要，先验证再截断；完整初始化意图消除空manifest窗口。
- 发布元信息、workspace package与隔离安装验证、二进制包工作流；多规模release性能矩阵。

兼容：新增默认配置字段在序列化时省略以保留旧配置hash；旧无摘要检查点不能识别所有内容篡改。Parquet恢复还会逐行检查固定Schema，拒绝包含旧版有损转换的已提交前缀，出错前不改动文件。明确schema/去重后端/预算等配置变更仍需要新运行。历史lowercase误拒绝数据不会自动补回。

验证：156项Rust测试、fmt/Clippy、Rust 1.93检查、workspace源码包构建及隔离安装通过；教材脚本10个区块通过。十万/百万条release实测与原始观测见[性能报告](docs/PERFORMANCE.md)。远端CI和公开发布需实际执行后另行确认。
