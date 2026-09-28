# 术语与教师指南 / Glossary and Instructor Notes

[教学首页 / Index](README.md) · [源码导读 / Guide](TEACHING_GUIDE.md) · [实验 / Labs](LABS.md)

## 双语术语 / Bilingual glossary

| 中文 | English | 本项目中的含义 / Meaning in this project |
| --- | --- | --- |
| 流水线 | Pipeline | 按约定阶段处理记录的过程 / A sequence of stages that processes records |
| 数据源 | Source | inline 数组、JSONL 或 CSV 文件 / Inline records, JSONL, or CSV files |
| 输出端 | Sink | 已接受/拒绝记录的提交及 JSONL/Parquet 发布 / Committing accepted/rejected records and publishing JSONL/Parquet |
| 数据契约 | Data contract | 字段与类型的明确约束 / Explicit field and type constraints |
| 提供者 | Provider | mock 或 HTTP 模型调用边界 / Boundary around mock or HTTP model calls |
| 结构化输出 | Structured output | 可以解析并接受 Schema 校验的内容 / Content that can be parsed and schema-validated |
| 在途任务 | In-flight work | 已接收但尚未按序处理完的记录任务 / Admitted record work not yet consumed in order |
| 有界并发 | Bounded concurrency | 限制同时在途的记录数量 / Limiting the number of in-flight records |
| 背压 | Backpressure | 下游变慢会限制上游继续接收数据 / Slower downstream work limits upstream admission |
| 队头阻塞 | Head-of-line blocking | 后续已完成结果等待前面的慢结果 / Later completed results wait for an earlier slow result |
| 信号量 | Semaphore | 控制同时进行的 HTTP 尝试数量 / Controlling simultaneous HTTP attempts |
| 速率限制 | Rate limiting | 60 秒滑动窗口限制 HTTP 请求数或 token 数；每次重试都计入 / A 60-second sliding window for HTTP requests or tokens; retries count individually |
| 重新生成 | Regeneration | Schema 拒绝后将修复反馈交给 provider 再生成 / Calling the provider again with repair feedback after schema rejection |
| 评判 | Judge | 第二次 provider 调用给生成内容评分并筛选 / A second provider call that scores and filters generated content |
| 去重 | Deduplication | 在 sink 接受后登记精确或 MinHash 键 / Registering exact or MinHash keys after sink acceptance |
| 恢复 | Resume | 校验失败/取消清单、源与配置后，从已提交前缀继续 / Continuing a failed/cancelled run from its committed prefix after source/config checks |
| 指数退避 | Exponential backoff | 重试间隔随尝试次数增加，带上限 / Increasing retry intervals with a cap |
| 抖动 | Jitter | 随机化等待时间以分散重试 / Randomized delay to spread retries |
| 取消令牌 | Cancellation token | 共享取消信号 / Shared cancellation signal |
| 拒绝记录文件 | Dead-letter file | 保存无法接受记录的身份与安全诊断 / Record identities and safe rejection diagnostics |
| 数据血缘 | Lineage | 从结果追溯输入、配置和 provider 的信息 / Information linking output to input, configuration, and provider |
| 源指纹 | Source fingerprint | 用于检测源变化的内容 hash / Content hash used to detect source changes |
| 运行清单 | Run manifest | 持久化的运行状态、统计和提交位置 / Persisted run state, statistics, and commit positions |
| 提交边界 | Commit boundary | 文件同步和清单描述的已提交前缀 / Committed prefix described by synchronized files and the manifest |
| 原子替换 | Atomic replacement | 文件名切换不暴露半份替换内容 / A name switch that does not expose a half-written replacement |
| 持久性 | Durability | 对已同步数据在故障后保留的要求 / Requirements for synchronized data to survive failures |
| 硬链接 | Hard link | 指向同一文件内容的另一个目录项 / Another directory entry for the same file contents |
| 幂等性 | Idempotency | 重复操作不产生额外业务影响；不能从重试自动推得 / Repeated operations do not add business effects; not implied by retries |
| 恰好一次 | Exactly once | 需要完整协议的处理/提交语义，当前不保证 / Processing/commit semantics requiring a full protocol; not guaranteed here |
| 故障注入 | Fault injection | 在受控位置制造错误验证行为 / Introducing controlled errors to verify behavior |

## 建议授课安排 / Suggested lesson plan

**中文**：下表是可调整的 4 次课安排，每次约 60–90 分钟。先让学生预测实验结果，再执行命令；讨论预测与实际差异，比只展示成功运行更能帮助理解。

**English**: The following adjustable plan has four sessions of approximately 60–90 minutes. Ask students to predict results before running commands. Discussing prediction errors teaches more than demonstrating only successful runs.

| 课次 / Session | 阅读 / Reading | 实践 / Practice | 课堂产出 / Deliverable |
| --- | --- | --- | --- |
| 1 | 导读 1–4 / Guide 1–4 | 实验 1、3、4 / Labs 1, 3, 4 | 画出记录的转换过程 / Draw a record's transformations |
| 2 | 导读 5–7 / Guide 5–7 | 实验 2、6 / Labs 2, 6 | 解释窗口、semaphore 与错误策略 / Explain the window, semaphore, and failure policy |
| 3 | 导读 8–9 / Guide 8–9 | 实验 5、7 / Labs 5, 7 | 标出崩溃窗口与有效前缀 / Mark crash windows and valid prefixes |
| 4 | 导读 10 / Guide 10 | 实验 8 / Lab 8 | 提交扩展设计并演示测试 / Present an extension design and tests |

## 检查题与参考答案 / Review questions and answers

### 1. 为什么 `Ok(report)` 不一定成功？ / Why can `Ok(report)` be unsuccessful?

**中文**：Result 表示是否成功返回运行报告；report.status 表示运行的业务结果。必须检查 `succeeded()`。预检失败通常返回 Err。

**English**: Result indicates whether a run report was returned; report.status indicates the business outcome. Check `succeeded()`. Preflight failures generally return Err.

### 2. 为什么不把全部输入放进 `Vec` 再并发调用？ / Why not load everything into a Vec and invoke concurrently?

**中文**：内存会随数据集规模增长，且创建全部 future 也可能保留大量输入/输出。当前 JSONL 源与有界窗口让工作集受并发度和记录大小约束。

**English**: Memory would grow with dataset size, and creating every future can retain large amounts of input/output. The JSONL source and bounded window constrain the working set by concurrency and record size.

### 3. record ID 能用来判断文本重复吗？ / Can record IDs detect duplicate text?

**中文**：不能直接用。ID 包含源位置，相同对象位于不同位置时 ID 不同；去重需要独立的字段选择、规范化和匹配策略。

**English**: Not directly. IDs include source position, so equal objects at different positions have different IDs. Deduplication needs separate field selection, normalization, and matching policies.

### 4. 为什么要在退避前释放 permit？ / Why release a permit before backoff?

**中文**：退避期间没有进行 HTTP 尝试，不应占用 provider 的活动请求容量。引擎窗口仍可限制逻辑记录数量，避免无限排队。

**English**: No HTTP attempt is active during backoff, so it should not consume active-request capacity. The engine window still bounds logical record work and prevents unlimited queuing.

### 5. 取消后 token 统计为 0，是否意味着没有费用？ / Does zero reported usage after cancellation imply zero cost?

**中文**：不是。服务端可能已处理请求，但 usage 没有返回；客户端取消不保证服务端停止。

**English**: No. The server may have processed the request without returning usage. Client cancellation does not guarantee server cancellation.

### 6. 为什么先同步数据，再替换 manifest？ / Why sync data before replacing the manifest?

**中文**：避免清单声明一个尚未持久化的数据前缀。相反，数据比旧清单多出一段时，旧清单仍可描述有效前缀，但多出的尾部必须由恢复协议处理。

**English**: To avoid declaring a prefix that is not durable. Extra data beyond an old manifest still leaves that manifest describing a valid prefix, but recovery must handle the extra tail.

### 7. 原子替换是否等于跨文件事务？ / Is atomic replacement a multi-file transaction?

**中文**：不是。某个目录项的原子替换不等于 accepted、rejected 和 manifest 的同时提交。发布中的异常状态需要明确检查和恢复。

**English**: No. Atomically replacing one directory entry does not commit accepted, rejected, and manifest files simultaneously. Interrupted publication needs explicit inspection and recovery.

### 8. 已保存偏移，resume 还需要检查什么？ / What must resume check beyond saved offsets?

**中文**：当前 `resume` 校验配置 hash 和源指纹，截断未提交尾部，恢复计数、去重状态及 Parquet Schema；输出命名空间使用跨进程锁，避免并发恢复。它只接受 failed/cancelled 清单，不自动处理 SIGKILL 留下的 running/publishing 状态。

**English**: Current `resume` checks the configuration hash and source fingerprint, truncates uncommitted tails, and restores counters, deduplication state, and the Parquet schema. A cross-process namespace lock prevents concurrent resume. It accepts failed/cancelled manifests only; running/publishing states left by SIGKILL are not recovered automatically.

## 常见误区 / Common misconceptions

| 误区 / Misconception | 更准确的说法 / More accurate statement |
| --- | --- |
| async 就不阻塞 / Async means nonblocking everywhere | 同步文件 I/O 和 sync_all 仍可阻塞任务线程 / Synchronous file I/O and sync_all can still block the task thread |
| Schema 合格就是答案正确 / Schema-valid means factually correct | 结构契约与语义质量是不同问题 / Structural contracts and semantic quality are separate |
| 400/401 不重试意味着整个任务立即退出 / No retry on 400/401 means immediate run exit | 当前先拒绝该记录，再由失败策略决定是否停止 / The record is rejected; failure policy decides whether the run stops |
| running 清单表示进程活着 / A running manifest proves a live process | 它只是最后持久化的状态 / It is only the last persisted state |
| provider.requests 等于已提交记录数 / provider.requests equals committed rows | 重试、取消和拒绝都会让两者不同 / Retries, cancellation, and rejection can make them differ |
| 通过本地测试就兼容所有模型 / Local tests prove universal model compatibility | 真实服务的能力、响应与配置还需要针对性验证 / Real services require targeted capability, response, and configuration checks |

## 评分建议 / Suggested assessment rubric

| 项目 / Criterion | 分值 / Points | 达标证据 / Evidence |
| --- | --- | --- |
| 数据流与 Rust 边界 / Data flow and Rust boundaries | 20 | 能从 CLI 追踪到 provider 与 commit / Trace from CLI through provider to commit |
| 实验复现 / Reproducibility | 25 | 提交实际命令、预期与观察，使用独立目录 / Actual commands, predictions, observations, isolated directories |
| 故障与持久化推理 / Failure and durability reasoning | 25 | 区分处理/提交/发布，指出崩溃窗口 / Distinguish processing, commit, publication, and crash windows |
| 扩展设计与测试 / Extension design and tests | 20 | 明确不变量、资源上限与故障验收 / Invariants, resource bounds, and failure acceptance criteria |
| 表述准确 / Accuracy | 10 | 不夸大幂等、exactly-once 或恢复保证 / No overstated idempotency, exactly-once, or recovery claims |

**中文**：不要因学生只得到“测试通过”而给满分。要求其解释测试为什么能发现目标错误，以及它没有覆盖的情况。教材中的实验使用测试夹具而非真实模型，是为了隔离引擎行为，不是证明模型效果。

**English**: Passing tests alone should not earn full credit. Ask students why a test can detect the target fault and what it does not cover. The labs use fixtures rather than real models to isolate engine behavior, not to demonstrate model quality.
