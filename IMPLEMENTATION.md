# 实现记录

范围遵循 `SYNTHFLOW_DESIGN.md` 第 53 节：本轮完成 Phase 0、1、2，不实现该节明确推迟的功能。

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
2. **Phase 5 RPM/TPM 限流**（60 秒滑动窗口，预留式记账保证并发不超预算；TPM 先按 prompt 长度估算、响应后用真实 usage 修正；限流等待先于 semaphore 与退避；tokio paused-time 单测验证准入时刻）。
3. **Phase 6 Judge**（第二个 provider 渲染 judge prompt，解析数值分数并按 `min_score` 过滤；`judge_score`/`judge_output` 诊断；通过记录追加 `judge` 证据字段与元数据；mock judge 可模板化 `generated`）。
4. **Phase 7 精确去重**（`method: exact`：点路径字段提取 + 可选 lowercase 规范化 + BLAKE3 键；`duplicate`/`dedup_field` 拒绝类别；`restore()` 供 resume 重建状态）。
5. **Phase 8 MinHash 去重**（`method: minhash`：词 n-gram shingle、成对独立置换签名（默认 128 维）、LSH 分带候选查找、签名相同分量比例近似 Jaccard；阈值/维数/带数/shingle 全部校验）。
6. **CSV source**（RFC 4180 流式解析，表头命名、全字符串值、行宽校验、8 MiB 行限制；fingerprint 与 artifact 冲突防护与 JSONL 一致）。
7. **Phase 9 Parquet sink**（`output.format: parquet` + `batch_size`。关键设计：**partial 始终是逐条同步的 JSONL**，因此 resume 语义与 JSONL 完全一致；每条记录对照首条推断的 Arrow schema 校验，schema 外字段或类型冲突按记录拒绝（`sink_row`），绝不静默丢列；发布时在 `.publishing` 临时文件中转换为 row group 后原子链接）。
8. **Phase 10/11 Checkpoint 消费 + Resume**（`synthflow resume`：校验配置 hash、源指纹与终态；partial/dead-letter 截断到提交字节前缀后追加；dedup 状态从前缀重建；跳过已提交源位置，确定性续跑不重复输出；新 manifest 记录 `resumed_from`；失败策略按全数据集计算）。
9. **Phase 12 指标**（每次 provider 调用的延迟计入 mean/p50/p95/p99；引擎侧 prompt/completion token 总量含 judge 调用；可选 `pricing` 输出 `estimated_cost_usd`；每 500 条或 2 秒的 progress 日志事件）。
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

已知限制：DataFusion SQL 查询、分布式执行（设计文档第 47 节之后）、有界 blocking worker 文件 I/O、可配置提交批次未实现。教学文档（docs/）仍基于 Phase 0–3，新阶段章节待补。
