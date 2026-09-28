# SynthFlow 教学文档 / Teaching Materials

**中文**：这套教材把 SynthFlow 当作一个可阅读、可运行、可故障注入的 Rust 工程案例。目标是理解流水线的行为与边界，而不仅是运行一条命令。每个章节采用中文与英文对照，命令和代码共用。

**English**: These materials use SynthFlow as a Rust case study that you can read, run, and test under failure. The goal is to understand pipeline behavior and its limits, beyond executing a command. Each section pairs Chinese and English explanations and shares commands and code.

## 学习入口 / Start here

| 文档 / Document | 内容 / Contents |
| --- | --- |
| [概念与源码导读 / Concepts and code walkthrough](TEACHING_GUIDE.md) | 10 章：数据流、Rust 边界、校验、异步、持久化与测试 / Ten chapters on data flow, Rust boundaries, validation, async execution, persistence, and testing |
| [实验手册 / Lab workbook](LABS.md) | 8 个实验，含预期结果与参考解释 / Eight labs with expected results and explanations |
| [术语与教师指南 / Glossary and instructor notes](GLOSSARY.md) | 中英术语、课堂安排、检查题与评分标准 / Bilingual terminology, lesson plans, review questions, and assessment |
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

**进阶课程作业 / Advanced assignment**：从实验 8 选择一个扩展，先写验收条件，再修改代码并演示故障行为。 / Select an extension from lab 8, write acceptance criteria first, then implement it and demonstrate failure behavior.

## 文档与实现的边界 / Documentation versus implementation

**中文**：教材以当前仓库代码为准。已经实现 mock/HTTP provider、JSONL/inline 输入、Schema 校验、有界并发、重试、取消和持久化清单。CSV、judge、去重、Parquet、RPM/TPM 限流与自动 resume 尚未实现。设计文档中的完整架构图不是当前功能清单。

**English**: The teaching material describes the current repository. Mock/HTTP providers, JSONL/inline inputs, schema validation, bounded concurrency, retries, cancellation, and durable manifests are implemented. CSV, judging, deduplication, Parquet, RPM/TPM limiting, and automatic resume are not. The full architecture in the design document is not a list of currently available features.
