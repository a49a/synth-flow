# SynthFlow 教学文档 / Teaching Materials

**中文**：这套教材把 SynthFlow 当作一个可阅读、可运行、可故障注入的 Rust 工程案例。目标是理解流水线的行为与边界，而不仅是运行一条命令。每个章节采用中文与英文对照，命令和代码共用。

**English**: These materials use SynthFlow as a Rust case study that you can read, run, and test under failure. The goal is to understand pipeline behavior and its limits, beyond executing a command. Each section pairs Chinese and English explanations and shares commands and code.

## 学习入口 / Start here

| 文档 / Document | 内容 / Contents |
| --- | --- |
| [概念与源码导读 / Concepts and code walkthrough](TEACHING_GUIDE.md) | 数据流、Rust 边界、校验、恢复、去重与测试 / Data flow, Rust boundaries, validation, resume, deduplication, and testing |
| [实验手册 / Lab workbook](LABS.md) | 9 个实验，含预期结果与参考解释 / Nine labs with expected results and explanations |
| [术语与教师指南 / Glossary and instructor notes](GLOSSARY.md) | 中英术语、课堂安排、检查题与评分标准 / Bilingual terminology, lesson plans, review questions, and assessment |
| [Parquet 输出契约 / Parquet output contracts](PARQUET_SCHEMA.md) | 显式类型、空值、可选字段与检查点完整性 / Explicit types, nulls, optional fields, and checkpoint integrity |
| [性能测量 / Performance measurements](PERFORMANCE.md) | 可复现的吞吐、RSS 与恢复测试 / Reproducible throughput, RSS, and recovery checks |
| [项目使用说明 / Project usage](../README.md) | 当前 CLI、配置及限制 / Current CLI, configuration, and limitations |
| [设计目标 / Design goals](../SYNTHFLOW_DESIGN.md) | 长期蓝图，包含尚未实现的功能 / Long-term blueprint, including unimplemented features |

## 先修与学习目标 / Prerequisites and learning outcomes

**中文**：建议先掌握 Rust 的 struct、enum、Result、所有权和借用，以及 JSON/YAML、HTTP 和基本终端操作。需要 Rust 1.93+；实验手册还使用 Python 3 标准库读取结果。文件持久化实验以 macOS/Linux 本地文件系统为目标。

**English**: Familiarity with Rust structs, enums, Result, ownership, borrowing, JSON/YAML, HTTP, and terminal basics is recommended. Use Rust 1.93+; the workbook also uses the Python 3 standard library to inspect results. Persistence labs target local macOS/Linux filesystems.

完成学习后，你应能够 / After completing the material, you should be able to:

1. 沿源码追踪一条记录从输入到提交的过程。 / Trace a record from input to commit through the code.
2. 区分配置错误、记录拒绝、运行失败和取消。 / Distinguish configuration errors, record rejection, run failure, and cancellation.
3. 解释有界并发、按序提交和背压之间的关系。 / Explain bounded concurrency, ordered commits, and backpressure.
4. 用文件状态说明“处理完成”和“持久化完成”的区别。 / Explain the difference between processing and durable completion using artifact state.
5. 用本地测试验证 HTTP 故障，避免依赖真实模型的随机输出。 / Test HTTP failures locally without relying on nondeterministic model output.
6. 设计扩展功能时明确不变量、失败窗口与验证方法。 / State invariants, failure windows, and tests when designing extensions.

## 建议路线 / Suggested routes

**入门，约 90 分钟 / Introduction, about 90 minutes**：导读第 1–4 章 → 实验 1–3 → 解释为什么第二条记录被拒绝。 / Read chapters 1–4, complete labs 1–3, then explain why the second record is rejected.

**系统学习，约 4–6 小时 / Full study, about 4–6 hours**：完成全部导读、实验与检查题。 / Complete the guide, labs, and review questions.

**进阶课程作业 / Advanced assignment**：先完成实验 8 的现有功能验证，再从实验 9 选择扩展，写出验收条件并演示故障行为。 / Verify current features in lab 8, then select an extension from lab 9, define acceptance criteria, and demonstrate failure behavior.

## 文档与实现的边界 / Documentation versus implementation

**中文**：教材以当前仓库代码为准。已实现 JSONL/CSV/inline 输入、mock/HTTP provider、Schema 校验与重生成、judge、精确/MinHash 去重、JSONL/Parquet 输出、RPM/TPM 限流、显式 `resume`、指标和 `inspect`。`resume` 在独占锁下支持 failed/cancelled/running/publishing；已发布文件需要内容摘要验证，旧版缺少发布摘要或发生冲突时仍需人工检查。设计文档还包含未实现的长期目标。

**English**: These materials describe the current repository. It implements JSONL/CSV/inline sources, mock/HTTP providers, schema validation and regeneration, judging, exact/MinHash deduplication, JSONL/Parquet output, RPM/TPM limiting, explicit `resume`, metrics, and `inspect`. Under an exclusive lock, resume accepts failed/cancelled/running/publishing manifests. Existing final output requires digest verification; legacy manifests without a publication digest and conflicting files still need manual inspection. The design document also contains longer-term goals.

## 自动验证与性能 / Automated validation and performance

运行 `python3 scripts/check_labs.py` 会执行实验手册中的原始 Bash 代码块及 Python 断言，并检查命名测试确实执行了一个测试；最终全量检查由 CI 单独执行。Linux/macOS × stable/Rust 1.93 的 CI 验证构建、Clippy 和测试。性能测量方法见 [性能说明](PERFORMANCE.md)。

Run `python3 scripts/check_labs.py` to execute the workbook's original Bash blocks and Python assertions, including verification that each named test actually runs one test. CI runs the final workspace checks separately. Linux/macOS × stable/Rust 1.93 CI validates builds, Clippy, and tests. See [performance notes](PERFORMANCE.md) for measurements.

## 当前实现补充 / Current implementation additions

**中文**：源与存储操作通过有界专用 I/O 线程执行。精确与 MinHash 去重均可选择 `dedup_storage: disk`；中文近重复比较可选字符片段。HTTP 请求支持输出长度、采样和响应模式，错误响应返回的有效 usage 也计入统计与费用。进度每 2 秒独立更新，Unix SIGTERM 与 Ctrl+C 均可保存可恢复进度。

**English**: Source and storage operations use bounded dedicated I/O threads. Exact and MinHash deduplication can select `dedup_storage: disk`, while character shingles support near-duplicate comparison of unspaced Chinese text. HTTP requests expose output length, sampling, and response modes; valid usage on error responses contributes to usage and cost. Progress updates independently every two seconds, and both Unix SIGTERM and Ctrl+C preserve resumable progress.

**中文**：`output.schema` 为 Parquet 固定输出类型，自动补充 `_meta`；未配置时按首条接受记录推断，拒绝有损数值转换。检查点对 accepted/rejected 前缀分别维护增量摘要，恢复时先校验再修改文件。缺少前缀摘要的旧清单仍支持结构校验与恢复；这与已有最终输出必须匹配发布摘要是两种检查。完整的原子初始化清单允许安全补建尚未创建的空文件，不会删除未知数据。配置示例见 [显式 Parquet 示例](../examples/parquet_explicit_schema.yaml)。

**English**: `output.schema` fixes Parquet output types and adds `_meta` automatically. Without it, the first accepted row determines the schema; lossy numeric conversions are rejected. Checkpoints maintain incremental digests for accepted/rejected prefixes, verifying both before any resume mutation. Legacy manifests lacking prefix digests remain structurally verifiable and resumable; this is separate from requiring a publication digest for an existing final output. A complete atomic initialization manifest allows missing empty data files to be created safely without removing unknown content. See the [explicit Parquet example](../examples/parquet_explicit_schema.yaml).
