# SynthFlow

Rust 本地优先的合成数据流水线引擎。已实现设计文档的 Phase 0–13:DSL、JSONL/CSV/inline 源、mock 与 OpenAI-compatible HTTP provider、结构化输出重生成、RPM/TPM 限流、judge 评判、精确与 MinHash 去重、JSONL/Parquet 输出、checkpoint resume、指标与 inspect。

```text
JSONL / CSV / Inline → Prompt → Mock / OpenAI-compatible HTTP → JSON Parse
  → Schema Validate → (Regenerate) → (Judge) → (Exact/MinHash Dedup) → JSONL / Parquet
```

## 教学文档 / Teaching materials

这是一个可用于学习 Rust 流式处理、异步并发和可靠性设计的教学项目。
This project is a teaching case for Rust streaming, async concurrency, and reliability.

请从 [中英文双语教学入口 / Bilingual teaching index](docs/README.md) 开始：包含 10 章源码导读、8 个实验、术语表和教师指南（撰写于 Phase 0–3；新阶段的教学章节待补充）。
Start with the bilingual teaching index for a ten-chapter walkthrough, eight labs, a glossary, and instructor notes (written for Phases 0–3; chapters for the newer phases are pending).

## 快速开始

需要 Rust 1.93 或更新版本。首次构建下载 Cargo 依赖；mock 示例和自动化测试不需要 API key 或外部模型服务。

```bash
cargo build --workspace
cargo run -p synthflow-cli -- validate examples/simple_generation.yaml
cargo run -p synthflow-cli -- plan examples/simple_generation.yaml
cargo run -p synthflow-cli -- run examples/simple_generation.yaml
```

示例读取三个 Rust 主题。其他示例：

```bash
cargo run -p synthflow-cli -- run examples/reliable_generation.yaml   # 失败策略
cargo run -p synthflow-cli -- run examples/judge_generation.yaml      # judge 评分
cargo run -p synthflow-cli -- run examples/csv_generation.yaml        # CSV 源
cargo run -p synthflow-cli -- run examples/parquet_generation.yaml    # Parquet 输出
cargo run -p synthflow-cli -- inspect examples/output/rust_instruction.jsonl
```

安装后可直接调用：

```bash
cargo install --path crates/synthflow-cli --locked
synthflow run examples/simple_generation.yaml
```

运行报告以 JSON 写入 stdout，日志写入 stderr；设置 `RUST_LOG=warn` 可以减少日志。运行中每 500 条记录或每 2 秒输出一次 progress 事件。`validate` 和 `plan` 成功时输出可读文本。

**已有正式文件不会被覆盖。** 输出、partial、manifest、dead-letter 任意一个已存在都会阻止新的 `run`。失败的运行用 `synthflow resume <pipeline>` 从已提交前缀续跑（见下文）；需要全新开始时请更换 `output.path`（自定义 dead-letter 也要一并更换），需要清理时自行归档/删除对应的整组文件。

## 输出与运行状态

假设 `output.path: output/data.jsonl`，运行会使用：

| 文件 | 作用 |
| --- | --- |
| `data.jsonl.partial` | 执行中的已接受记录；失败或取消时保留。**无论输出格式如何，partial 始终是 JSONL** |
| `data.jsonl`（或 `.parquet`） | 全流程成功后发布的正式数据；Parquet 在发布时由 partial 一次性转换生成 |
| `data.jsonl.rejected.jsonl` | 被拒绝记录的 ID、位置、错误类别、字段路径和尝试次数 |
| `data.jsonl.manifest.json` | run ID、状态、源指纹、配置 hash、时间、统计和提交位置；resume 的 checkpoint |

每条已处理记录按源顺序写入，随后同步 accepted/dead-letter 文件，再原子替换并同步 manifest。`sink_state` 包含已提交的源位置、accepted/rejected 字节数和记录数。这些字段表示持久化前缀；顶层统计表示处理情况，遇到磁盘错误时不应将它们直接当作已持久化的行数。

成功发布使用同目录硬链接：只会创建不存在的最终路径，不会覆盖并发创建的文件。Parquet 运行先在 `.publishing` 临时文件中完成转换再链接发布。当前面向支持硬链接和目录同步的本地文件系统（macOS/Linux）。完成后清理 partial 与临时文件；清理失败不影响完整输出。每条记录同步的策略优先保证正确性，可配置提交批次留待后续。

状态为 `running`、`publishing`、`completed`、`failed`、`cancelled`。数据文件和 manifest 不是跨文件事务：如果在发布窗口中被强制终止，可能同时存在完整数据和 `publishing` 清单。消费者应以 **正式文件存在且 manifest 为 completed** 为完成条件。

Ctrl+C 取消在途请求、保留已提交前缀并记录 `cancelled`，退出码为 130；第二次 Ctrl+C 强制退出。SIGKILL 无法执行清理，清单可能仍为 `running`；突然终止后 partial 尾部可能超出清单记录的提交字节数。

### Resume

```bash
synthflow resume examples/simple_generation.yaml
```

`resume` 从 manifest 记录的 checkpoint 续跑上次 `failed` 或 `cancelled` 的运行：

- 校验 pipeline 配置 hash、源文件指纹与 manifest 一致，manifest 状态必须是 `failed`/`cancelled`；`completed` 或正在运行的清单会拒绝。
- 输出路径对应的 `.lock` 文件提供跨进程独占锁，防止多个 run/resume 同时修改同一检查点。
- partial 与 dead-letter 截断到 manifest 记录的提交字节前缀后追加写入；崩溃遗留的尾部被丢弃。
- 已配置去重时，dedup 状态从已提交前缀的记录重建；Parquet 的首条已接受记录也用于重建原 Schema，保持恢复前后的验收规则一致。
- 已提交的源位置直接跳过；续跑后的正式输出恰好包含每条记录一次。
- 新 manifest 记录 `resumed_from`（前次 run ID 与已提交数量）；失败策略按全数据集（含继承前缀）计算。报告统计只统计本次运行新处理的记录。

## 配置

参见 [JSONL mock](examples/simple_generation.yaml)、[inline mock](examples/inline_generation.yaml)、[失败策略](examples/reliable_generation.yaml)、[judge](examples/judge_generation.yaml)、[CSV](examples/csv_generation.yaml)、[Parquet](examples/parquet_generation.yaml) 和 [HTTP provider](examples/openai_generation.yaml)。

相对路径均相对于 YAML 所在目录解析。配置 hash 使用规范化路径，统一相对/绝对路径及已存在目录的符号链接；它包含输出位置和错误策略。移动文件或更换输出路径会改变 hash，源内容另外由 fingerprint 标识（`resume` 要求两者都与 manifest 一致）。

### Provider

```yaml
providers:
  generator:
    type: openai_compatible
    base_url: http://127.0.0.1:8000/v1
    model: local-model
    # 只有需要鉴权时填写；值从进程环境读取，不进入清单或模板。
    # api_key_env: LOCAL_LLM_API_KEY
    concurrency: 4
    timeout_ms: 60000
    retry:
      max_attempts: 5
      initial_delay_ms: 500
      max_delay_ms: 30000
    rate_limit:
      requests_per_minute: 600
      tokens_per_minute: 200000
```

请求发往 `${base_url}/chat/completions`，发送 user message 和 `response_format: {type: json_object}`。不跟随 HTTP 重定向。服务器需要兼容此结构。

每个 provider 有独立 semaphore；执行器最多持有 `concurrency` 条在途记录，包括等待按序提交的结果。慢记录会对上游施加背压。重试使用有上限的指数退避和随机抖动，不在退避期间占用 semaphore。

- 重试：429、502、503、504、网络失败和超时。
- 不重试：400、401、403 等其他 HTTP 状态、无效响应结构、非法生成 JSON、Schema 不匹配。
- `timeout_ms` 覆盖每次 HTTP 请求和响应体读取；等待并发许可与重试退避不计入单次超时。
- `Retry-After` 支持秒数和 HTTP 日期；当前等待上限仍由 `max_delay_ms` 限制。
- `max_attempts` 包含首次请求；配置范围为 1–100，并发范围为 1–1024。
- `rate_limit` 是 60 秒滑动窗口：RPM 按每次 HTTP 尝试计，重试也占用配额；TPM 按每次请求前的 prompt 长度估算预留，响应返回真实 usage 后修正。每次尝试在取得 semaphore 前等待限流，被取消的请求只浪费自己的预留。
- `provider_usage` 记录实际请求数、重试数、已返回的 prompt/completion tokens 和已完成 HTTP 尝试的累计耗时。取消请求可能已产生服务端费用，但未返回 usage，不能据此推断实际账单。

Mock 使用 `type: mock` 和 `response` 模板，可选 `concurrency`，默认 1。上下文为 `record`、渲染后的 `prompt`、`prompt_hash`、`feedback`（重生成时的修复说明）和 `generated`（作为 judge 时的被评对象）；将字符串插入 JSON 时使用 `tojson`。mock 的生成内容和记录 ID 保持确定性，每次独立执行的 run ID 不同。

### 生成与重生成

```yaml
generate:
  provider: generator
  prompt: |
    Create one Rust interview question about {{ record.topic }}.
  output_schema: {type: object, ...}
  regenerate_on_invalid: 1   # 0–3，默认 0
```

模型输出非法 JSON 或 Schema 不匹配时，`regenerate_on_invalid` 次数内会携带修复反馈（只描述规则路径，不含原始输出）重试；最终仍失败才进入 dead-letter。`_meta.attempt` 统计全部生成调用。

### Judge

```yaml
judge:
  provider: reviewer
  prompt: |
    Rate this answer: {{ generated.answer }}
    Return JSON with a numeric score.
  score_field: score   # 默认 "score"
  min_score: 0.7
```

生成记录通过 Schema 后，judge provider 用 `record`、`generated`、`prompt` 上下文渲染第二个 prompt，解析数值分数字段：低于 `min_score` 记 `judge_score` 拒绝，无法解析记 `judge_output` 拒绝。通过的记录追加 `judge` 字段（provider、model、score）及 `_meta` 中的 judge 信息。

### 去重

```yaml
dedup:
  method: exact        # 或 minhash
  fields: [topic, generated.answer]   # 可选；默认整个 generated 对象
  normalize: lowercase               # 可选 none/lowercase
# 或
dedup:
  method: minhash
  field: generated.answer
  num_perm: 128        # 8–1024，必须被 bands 整除
  bands: 16
  threshold: 0.8       # 相似度阈值
  shingle_words: 3     # 1–10
```

exact 用规范化 JSON 的 BLAKE3 作为键；minhash 用词 n-gram shingle、128 维（可配）签名和 LSH 分带候选查找，以签名相同分量比例估计 Jaccard 相似度。重复记录以 `duplicate` 类别拒绝；配置的字段缺失记 `dedup_field`。去重按提交顺序在 judge 之后执行，state 可在 resume 时从已提交前缀重建。

### 失败策略

```yaml
errors:
  # 默认 output.path + .rejected.jsonl；不写完整输入或响应。
  dead_letter: output/rejected.jsonl
  max_failed_records: 10
  max_failed_ratio: 0.1
  strict: false
```

- `strict: true` 或 CLI `--strict`：首次记录被拒绝后停止，不发布正式输出。
- `max_failed_records`：允许的最大拒绝数；超过后停止。并发时可能已有其他请求在途，它们会被取消。
- `max_failed_ratio`：最终拒绝数 / 已处理数，**在源读取结束后检查**，避免第一条失败就让比例策略过早终止。
- 默认允许部分记录被拒绝，但非空数据集全部被拒绝时始终失败。空输入可以成功。
- 源解析错误和文件写入/同步错误是致命错误。resume 场景下失败策略按全数据集（含前次运行已提交的前缀）计算。
- 已建立运行后发生的失败仍返回 JSON 报告，CLI 退出 1；预检失败返回含 `preflight: true` 的错误 JSON。失败统计也尽力保存在 manifest；磁盘损坏时无法保证新状态可持久化，stdout 报告会携带相应错误。

拒绝诊断包含类别、阶段、尝试次数，Schema 错误另有 `instance_path` 和 `schema_path`。不记录原始输入、prompt、HTTP 错误响应体或密钥。源解析失败不能可靠生成记录 ID，因此记录为运行错误，而非伪造 dead-letter 行。

### 数据契约与资源限制

- 只接受 DSL `version: 1`，未知配置字段报错。配置错误尽可能提供字段路径和期望类型，不回显错误值。
- 输入必须为对象，`generated`、`judge`、`_meta` 是保留字段。JSONL 不接受空行，支持 CRLF 和无末尾换行。CSV 以表头命名（RFC 4180 引号/内嵌换行/CRLF），所有值为字符串；行宽与表头不一致视为源错误。
- 生成模板支持 `{{ record.topic }}` 和 `{{ topic }}`，`record` 始终表示完整输入。变量缺失导致拒绝；模板不提供环境变量、shell 或文件系统入口。
- 输出必须是纯 JSON 对象。Schema 支持嵌套对象、数组、基本类型、required、properties、enum 等。真正的 Schema 引用暂不支持；普通属性名或枚举数据中的 `$ref` 不会被误判。
- 输出保留原始字段，追加 `generated`（启用 judge 时还有 `judge`）和 `_meta`；元数据包含记录 ID、run ID、源位置和指纹、配置 hash、DSL 版本、provider/model、prompt hash、attempt、模板引擎版本以及 judge 信息。
- Parquet 输出以首条已接受记录推断 Arrow schema；后续记录出现 schema 之外的字段或类型冲突按记录拒绝（`sink_row`），不会静默丢列。发布前 partial 始终是逐条同步的 JSONL。
- YAML、JSONL 单行、模板输出及 HTTP 响应体各限制为 8 MiB，模板有 100,000 fuel 上限。inline 输入随 YAML 一次载入；JSONL/CSV 流式读取。模板中间值和 Schema 运算仍不是通用不可信代码的资源沙箱。

### 指标与成本

- 报告包含按记录累计成功生成调用耗时的摘要：`latency.mean_ms / p50_ms / p95_ms / p99_ms`。均值为增量计算；分位数来自最多 4096 个均匀蓄水池样本，超过该数量时为近似值。当前延迟口径不包含失败调用或 judge 调用。
- `prompt_tokens_total` / `completion_tokens_total` 为引擎视角的全部 provider 调用（含 judge）返回的 usage 之和；mock 不返回 usage。
- 可选 `pricing: {input_usd_per_mtok: 10.0, output_usd_per_mtok: 20.0}` 将其换算为 `estimated_cost_usd`（估算值，未含被取消请求的服务端费用）。
- 运行期间每 500 条或 2 秒记录一次 progress 日志事件（processed/accepted/rejected/rate）。

### Inspect

```bash
synthflow inspect output/data.jsonl          # 可读表格
synthflow inspect output/data.parquet --json # 完整 JSON 摘要
```

统计行数、字节数和每个顶层列的类型、非空/空值计数（缺失键计为空值）、数值列的 min/max/mean 以及上限一百万的去重基数。混合类型列只用数值项计算 min/max/mean；没有数值项时均值为 null。SQL 查询（DataFusion）暂未实现。

## 库与架构

`crates/synthflow` 提供配置、模板、Source、异步 `LlmProvider`、去重、引擎和运行文件管理；`crates/synthflow-cli` 提供命令行与信号处理。

- 异步入口：`run_async(&pipeline, CancellationToken)`；续跑用 `resume_async(&pipeline, CancellationToken)`。
- 可注入 provider：`run_with_provider` / `resume_with_provider(&pipeline, Arc<dyn LlmProvider>, CancellationToken)`。
- 同步兼容入口：`run(&pipeline)`；失败时返回错误。需要完整失败报告的调用者使用异步入口或读取 manifest。
- `Ok(report)` 只表示已经返回运行报告；调用者必须检查 `report.succeeded()`，不能把 `Ok` 等同于任务成功。

当前文件读取、写入和每条记录的同步操作仍在执行任务中同步进行；指纹扫描使用 blocking worker。HTTP 调用与等待是异步的。后续可将源和 sink 移到有界 blocking worker，以改善慢磁盘下的调度响应。

## 验证与后续

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
```

测试只使用临时文件及回环地址上的 HTTP server，覆盖重试/超时/鉴权/限流、拒绝策略、重生成、judge、精确与 MinHash 去重、CSV、Parquet schema 拒绝、resume 不重复输出、输出防覆盖、Schema 诊断、源变化、写入错误、取消和进程强制终止；Unix 上额外验证 SIGINT。

[IMPLEMENTATION.md](IMPLEMENTATION.md) 记录阶段交付。尚未实现：DataFusion SQL 查询、inspect 之外的分布式架构（设计文档第 47 节之后）、有界 blocking worker 的文件 I/O、可配置的提交批次与 throughput 优化。
