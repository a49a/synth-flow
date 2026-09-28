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

约束：最终文件和清单不是跨文件事务；硬中断可能留下 `publishing` 状态，必须联合检查完成状态。每条记录同步确保清晰提交边界，但会牺牲本地吞吐量；同步文件 I/O 尚未拆到独立有界 worker。HTTP 不含 RPM/TPM 限流、模型输出重生成和定价。CSV/judge/dedup/Parquet、完整 checkpoint/resume 和 inspect 仍按设计文档后续阶段实现。
