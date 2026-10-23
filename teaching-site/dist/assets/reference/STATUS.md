# 当前能力 / Current capability status

本表描述当前代码；`SYNTHFLOW_DESIGN.md` 保留历史蓝图，`IMPLEMENTATION.md` 保留阶段记录。
This table describes current code; the design and implementation notes retain their historical context.

| 范围 / Area | 当前支持 / Implemented | 边界与后续 / Limits and follow-up |
| --- | --- | --- |
| 配置与源 / DSL and source | version 1、严格字段验证、JSONL/CSV/inline、相对路径 | 单机本地文件；无远程流式源 |
| 生成 / Generation | mock、HTTP Chat Completions、重试、RPM/TPM、重生成、长度/采样/seed、三种响应格式 | 参数支持取决于服务端；TPM为估算准入 |
| 质量 / Quality | JSON Schema、单分数judge、exact/MinHash、词或Unicode字符窗口 | 多维judge表达式、语义向量去重尚未实现 |
| 去重存储 / Dedup storage | memory预算、临时SQLite exact/MinHash索引 | disk需要临时磁盘；跨运行索引复用尚未实现 |
| 输出 / Output | JSONL、Parquet显式或首条固定Schema、无损数值检查 | 不自动合并未知字段；不提供通用Schema演化 |
| 恢复 / Recovery | 按源序提交、双前缀摘要、独占锁、SIGINT/SIGTERM、发布摘要、初始化意图 | 旧无摘要清单只能检验结构/长度；配置变化需新运行 |
| 调度 / Scheduling | 有界source/storage专用worker、HTTP异步、独立进度心跳 | 每个管线两个I/O线程；单行/Arrow/模板内存不等于严格RSS沙箱 |
| 统计 / Statistics | 已返回HTTP usage计费、逻辑调用指标、整数精确min/max、防溢出mean | 无usage的真实费用未知；mean为浮点近似 |
| Phase 13 | Arrow/JSONL基础inspect | DataFusion SQL未实现，不能称为完整DataFusion阶段 |
| 交付 / Delivery | crate打包验证、隔离安装、Linux/macOS二进制包工作流 | 未在此改动中发布到crates.io或GitHub Releases |
| 性能 / Performance | release多规模、后端/并发矩阵、重复统计、可选强杀恢复 | 样本不构成通用性能承诺；见PERFORMANCE.md |

## 当前命令 / Current commands

```text
synthflow validate pipeline.yaml
synthflow plan pipeline.yaml
synthflow run pipeline.yaml [--strict]
synthflow resume pipeline.yaml [--strict]
synthflow inspect data.jsonl [--json]
```

设计文档里的 `run --resume`、自定义checkpoint路径、多维judge accept表达式属于旧蓝图，不能直接作为当前配置使用。
The design's `run --resume`, custom checkpoint paths, and multidimensional judge acceptance expressions are not current CLI/DSL syntax.

## 兼容性 / Compatibility

- 新配置默认值在哈希序列化时省略；旧默认配置可继续使用。配置内容变化仍由hash保护。
- `lowercase`现在保留对象键，只转换字符串值。旧版本误拒绝的数据需重跑；resume只处理未提交源位置。
- Parquet运行与发布使用相同固定Schema，整数截断或无法精确表示的float提升会被拒绝。恢复会对全部已提交行复核该约束；旧版不兼容前缀即使摘要匹配也会被拒绝，且不截断数据、不改写清单。
- 新checkpoint包含两份BLAKE3前缀摘要；旧无摘要清单恢复后，下一次提交开始保存摘要。
- 库的`ColumnStats.min/max`现在为`Option<serde_json::Number>`；调用者可按需选择整数或浮点表示。

Defaults remain hash-compatible. Historical incorrect dedup rejections require rerunning; resume does not regenerate committed positions. Compatible older checkpoints gain content digests on subsequent commits. Parquet resume rechecks every committed row against the fixed schema; incompatible historical prefixes are refused without modifying artifacts, even when their digests match. Library callers should note the exact-number extrema API.
