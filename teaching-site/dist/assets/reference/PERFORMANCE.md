# 性能基线 / Performance baseline

## 构建与快速复现 / Build and quick run

```bash
cargo build --release --workspace --locked
python3 scripts/benchmark.py --records 1000 --output /tmp/synthflow-quick.json
python3 scripts/benchmark.py --self-test
```

脚本默认使用 `target/release/synthflow`，保留 `--records 1000` 调用方式。默认只运行 JSONL、1,000 条、并发 1、提交间隔 1 和 128，各测一次；大矩阵和强制中断恢复都需要显式选择。运行进度写到 stderr，结果 JSON 写到 stdout；`--output` 还会在每个样本完成后原子更新结果文件。仅支持 macOS/Linux，使用 Python 标准库，无外部模型调用。

The default is a release binary and a small JSONL comparison, with commit intervals 1 and 128. Large matrices and crash recovery are opt-in. Progress goes to stderr, JSON to stdout; `--output` also saves results after each observation. Requires Python 3.9+ on macOS/Linux, with no third-party Python packages or model services.

源码与生成值均为确定的唯一短文本，默认每条文本 64 bytes。源文件逐条写入，JSONL 输出逐条核对源位置、ordinal、输入文本、生成文本，以及 judge 分数。每个 case 完成校验后删除临时产物，避免整个矩阵累积大量输出。源生成与结果验证不计入 run/resume 的耗时。

Source generation and JSONL validation stream one record at a time. Per-case temporary artifacts are removed after validation. Source preparation and output verification are excluded from timed run/resume commands. Unique short records exercise index growth; they do not represent a near-duplicate distribution or a real model workload.

## 可选矩阵 / Configurable matrix

```bash
# 首先用较少 case 测量 10 万和 100 万条。
python3 scripts/benchmark.py \
  --records 100000,1000000 --cases jsonl exact-disk \
  --commit-intervals 128,1024 --concurrency 8 --repeat 3 \
  --timeout 7200 --output /tmp/synthflow-scale.json

# 全部 7 个 case；默认内存预算不足的 case 会被记录为失败，其他 case 继续。
python3 scripts/benchmark.py \
  --records 100000,1000000 --cases all \
  --commit-intervals 128,1024 --concurrency 8 --repeat 3 \
  --timeout 7200 --allow-failures --output /tmp/synthflow-matrix.json

# 独立检查 SIGKILL 后的恢复；不与完整运行共用产物。
python3 scripts/benchmark.py \
  --records 10000 --cases jsonl exact-disk minhash-disk parquet \
  --commit-intervals 128 --concurrency 8 --repeat 3 \
  --recovery --recovery-after 256 --recovery-timeout 120 \
  --output /tmp/synthflow-recovery.json
```

`--records`、`--cases`、`--commit-intervals` 和 `--concurrency` 均支持逗号或空格分隔，组合为笛卡尔积。上面的全矩阵是 84 次完整运行，可能耗时较长。`--timeout` 控制每次 run、resume 或 inspect 的秒数；`--recovery-timeout` 控制等待可中断检查点的秒数。`--work-dir` 指定临时目录所在磁盘。`--text-bytes` 调整每条文本长度；它不改变记录数量。

Record counts, cases, commit intervals, and concurrency values form a Cartesian product. The full example performs 84 runs and can take substantial time. `--timeout` bounds each run/resume/inspect, while `--recovery-timeout` bounds checkpoint waiting. Use `--work-dir` to select the tested disk and `--text-bytes` to adjust record size.

| Case | 行为 / Workload |
| --- | --- |
| `jsonl` | JSONL 基线，无 judge/dedup / baseline without judge or dedup |
| `exact-memory` | 精确去重，内存索引 / exact dedup with memory index |
| `exact-disk` | 精确去重，SQLite 索引 / exact dedup with disk index |
| `minhash-memory` | 128 permutations、16 bands，内存索引 / MinHash memory index |
| `minhash-disk` | 同样的 MinHash 参数，SQLite 索引 / MinHash disk index |
| `parquet` | 包含最终 Parquet 转换和发布 / includes final conversion and publication |
| `judge` | 每条额外调用一次独立 mock judge / one separate mock judge call per record |

内存去重默认保留产品的 64 MiB accounting budget。当前记账规则下，精确去重每条唯一记录收取 128 bytes，默认 MinHash 每条收取 3,200 bytes；所以大规模 memory case 可能明确失败。这是容量结果，不应静默扩大内存来隐藏。可用 `--dedup-max-mib 512` 等显式改变预算；它不是 RSS 上限。磁盘模式采用有界 SQLite 页缓存，不按此预算积累索引。所有失败都会保留错误、耗时、峰值 RSS 和可用的检查点；默认矩阵末尾退出码为 1，`--allow-failures` 允许容量探索返回 0。

Memory cases retain the product's default 64 MiB accounting budget. Exact indexing charges 128 bytes per unique row; this MinHash configuration charges 3,200. Large memory cases can therefore fail explicitly. Override with `--dedup-max-mib` when intentionally testing a larger index; it is not an RSS limit. Failed cases remain in the JSON and the matrix continues. The final exit code is nonzero unless all cases pass or `--allow-failures` is set.

## 指标与解释 / Metrics and interpretation

每次测量由全新的 Python helper 启动 CLI，使用 `RUSAGE_CHILDREN` 读取该次 CLI 的峰值 RSS，避免历史子进程高水位串入后续样本。结果记录每次完整运行的 wall time、records/s、峰值 RSS、持久化 sink state、验证方式，以及可选的恢复检查点与恢复耗时。`summary` 按 case、规模、提交间隔和并发分组，提供成功/失败样本数及 median、p95、max；p95 使用线性插值，只有一次观测时等于该观测，不能据此判断尾延迟。

Each measurement uses a fresh helper process, isolating peak child RSS from previous samples. Summaries group samples by case, record count, commit interval, and concurrency, and report median, interpolated p95, and maximum. With one observation, p95 equals that observation and is not evidence about a latency distribution. Throughput measures an offline pipeline, not tokens/s or real provider latency.

JSON 中还包含系统/内核、CPU、逻辑核数、物理内存、Python/Rust/Cargo 版本、binary 大小和 SHA-256、修改时间、构建模式路径提示、Git revision/dirty 状态、Cargo.lock 与脚本的 SHA-256，以及临时目录所在路径。Rust/Cargo 版本是测量时的本机工具链；binary hash 才是被测文件标识，路径提示不能证明实际构建参数。结果之间只有在构建、数据、配置、磁盘与机器负载可比时才适合讨论回归。

The JSON records host/CPU/toolchain information, binary hash and timestamp, build-profile path hint, source revision/dirty state, lockfile and script hashes, and the temporary parent directory. Toolchain versions describe the measurement environment; the binary hash identifies the tested executable. A path hint does not attest compiler flags. These are observations, not CI performance thresholds.

Parquet 通过 CLI `inspect --json` 逐批解码，核对行数以及顶层 ordinal 的非空计数、最小值、最大值、均值和可用的 distinct 计数。JSON 会标明 `order_and_content: false`：这一检查不证明逐行顺序或所有字段完全一致。JSONL 的验证会标明 `true`，逐条检测重复、遗漏、重排和内容变化。发布摘要及持久化行数仍由运行报告检查。

Parquet validation uses the CLI's streaming decoder and checks row counts and ordinal aggregates. It is explicitly labeled `order_and_content: false`; it does not establish full row equality or ordering. JSONL validation checks every row's position and content and is labeled `true`.

## 本机 release 实测（2026-10-22） / Local release observations

Apple M1 Pro、32 GiB、macOS arm64、Rust 1.93.1；使用上面的 release 构建命令。短文本 64 bytes，并发 8，`commit_every: 1024`，每组重复两次。耗时与吞吐分别取各次观测的中位数，RSS 取两次最大值；样本数不足以推断尾延迟。完整运行包含源指纹、处理、持久化和最终发布，不包含测试脚本生成输入及核对输出的时间。

Apple M1 Pro, 32 GiB, macOS arm64, Rust 1.93.1; release build, 64-byte texts, concurrency 8, commit interval 1024, two observations per group. Times and throughput are separate medians; RSS is the observed maximum. These small samples do not characterize tail latency.

| Case | 记录 / Records | 完整运行 / Full run | 吞吐 / Rows/s | 峰值 RSS / Peak RSS |
| --- | ---: | ---: | ---: | ---: |
| JSONL | 100,000 | 8.040 s | 12,761.2 | 17.55 MiB |
| exact-disk | 100,000 | 10.471 s | 9,562.4 | 21.23 MiB |
| JSONL | 1,000,000 | 64.151 s | 15,588.3 | 17.44 MiB |
| exact-disk | 1,000,000 | 96.501 s | 10,370.1 | 21.44 MiB |

8 次全部完成，JSONL 逐行校验顺序和内容；每份百万条输出为 747,777,792 bytes。磁盘索引在这组测试中没有随记录数线性增加 RSS，但会增加磁盘空间和 I/O。这不代表默认逐条同步配置或真实 HTTP 模型调用的吞吐，也不是严格 RSS 上限。

All eight runs completed with per-row JSONL order/content verification. Each million-row output is 747,777,792 bytes. Disk indexing did not show linear RSS growth in this workload; it still consumes disk space and I/O. These measurements do not describe the default per-row commit setting or real HTTP model throughput, and are not a hard RSS bound.

原始观测与构建指纹：[scale JSON](benchmarks/2026-10-22-scale.json)。规模采样后补充了旧 Parquet checkpoint 类型预检；正常 JSONL/去重路径未改变，后续恢复测试使用重新构建的 release，并单独记录二进制指纹。

Raw observations and the tested binary fingerprint are in the linked JSON. A later rebuild added validation of legacy Parquet checkpoint types; the normal JSONL/dedup path was unchanged. Recovery results use that rebuilt executable and record its own fingerprint.

其他后端使用相同机器、64-byte文本、并发8、提交间隔1024，各测一次。原始观测：[matrix JSON](benchmarks/2026-10-22-matrix.json)、[capacity JSON](benchmarks/2026-10-22-capacity.json)。单次数据用于确认行为和容量，不用于稳定吞吐排名。

Other cases use the same host, text size, concurrency and commit interval, with one observation each. These samples check behavior/capacity rather than establish a stable throughput ranking.

| Case | 输入记录 / Input rows | 结果 / Result | 完整运行或失败耗时 / Time | 峰值 RSS / Peak RSS |
| --- | ---: | --- | ---: | ---: |
| exact-memory | 100,000 | 完成 / completed | 6.016 s | 25.97 MiB |
| minhash-memory | 100,000 | 预算拒绝，提交至20,480 / budget refusal | 1.642 s | 87.03 MiB |
| minhash-disk | 100,000 | 完成 / completed | 25.500 s | 21.98 MiB |
| parquet | 100,000 | 完成 / completed | 7.761 s | 34.77 MiB |
| judge | 100,000 | 完成 / completed | 6.991 s | 17.52 MiB |
| exact-memory | 1,000,000 | 预算拒绝，提交至524,288 / budget refusal | 32.820 s | 83.59 MiB |

两次预算拒绝均返回非零退出码并保留检查点，没有把超限记录当作重复记录丢弃。这不代表完整输入已经处理成功。64 MiB是索引记账预算，不是进程RSS限制；需处理更大唯一集合时，在开始新运行前选择disk或明确提高预算。更改后端/预算会改变配置hash，不能直接拿新配置续跑旧checkpoint。

Both budget refusals returned nonzero and retained checkpoints; they did not mislabel excess unique rows as duplicates. They are not completed runs. The 64 MiB accounting budget is not an RSS limit. Select disk storage or an explicit larger budget before starting a new run; changing either setting changes the configuration hash.

强杀恢复单独使用10,000条、并发8、提交间隔128，各测一次；四种case都在第256条已提交后SIGKILL，再由新进程恢复至10,000条。JSONL三种case逐行核对内容与顺序，Parquet核对行数与ordinal聚合。原始观测：[recovery JSON](benchmarks/2026-10-22-recovery.json)。

Separate recovery trials use 10,000 rows, concurrency 8 and commit interval 128, one observation per case. Each process was killed after durable position 256 and resumed to 10,000. JSONL cases verify every row; Parquet verifies counts and ordinal aggregates.

| Case | 恢复耗时 / Resume | 恢复峰值 RSS / Resume peak RSS |
| --- | ---: | ---: |
| JSONL | 1.911 s | 17.33 MiB |
| exact-disk | 2.188 s | 19.25 MiB |
| minhash-disk | 3.712 s | 21.86 MiB |
| parquet | 2.042 s | 26.11 MiB |

强杀会跳过临时SQLite索引的析构清理。本次恢复测试为整个脚本设置了独立`TMPDIR`，所有子进程退出后清理该目录；复现时也建议使用独立临时目录，避免遗留文件混入系统共享临时目录。

SIGKILL skips temporary SQLite cleanup. These recovery trials used a dedicated `TMPDIR`, removed after all children exited; use the same isolation when reproducing them.

## 历史样本 / Historical sample

2026-09-29，macOS Apple Silicon，debug，1,000 条短记录，无 judge/dedup；单次历史观测。以下不是本轮 release 矩阵的结果，不能作为 10 万／100 万条容量结论。

2026-09-29, macOS Apple Silicon, debug, 1,000 short records, no judge/dedup. These historical single observations are not results from the current release matrix and establish no 100k/1M capacity claim.

| 提交间隔 / Interval | 完整运行 / Full run | 吞吐 / Rows/s | 峰值 RSS / Peak RSS | 恢复位置 / Resume from | 续跑 / Resume |
| --- | --- | --- | --- | --- | --- |
| 1 | 25.282 s | 39.6 | 21.1 MiB | 1 | 24.818 s |
| 128 | 0.350 s | 2,860.6 | 21.0 MiB | 128 | 0.332 s |

该样本说明同步提交开销会影响短记录 mock 工作负载，不代表真实模型生成加速同等倍数。恢复时间包括源指纹、前缀验证、索引重建和剩余生成；不同恢复位置不能当作纯 seek 性能比较。

The sample illustrates synchronization overhead for short mock records. It does not imply an equivalent real-model speedup. Resume time includes fingerprinting, prefix verification, index rebuilding, and remaining generation; samples at different checkpoints do not isolate seek performance.

## 正确性边界 / Correctness boundary

默认提交间隔仍为 1。正常完成会提交尾批；可处理的失败/取消回滚到最后提交位置。`--recovery` 会记录发 SIGKILL 前观察的位置以及进程被杀后实际 manifest 的完整 `sink_state`，再独立测量 resume。若数据过少而未能在运行结束前中断，脚本明确失败；增加 records 或降低中断目标。SIGKILL 可能重做当前未提交批次，并发请求可能已计费。`sink_state` 才是持久化进度；本次尝试的 processed/accepted 计数可能包括回滚尾部。`output.batch_size` 只控制 Parquet 转换批次，与提交间隔独立。

The default commit interval remains 1. Normal completion commits the final partial batch; handled failure or cancellation rolls back to the last checkpoint. Recovery measurements record both the observed position before SIGKILL and the durable manifest after termination. If a run finishes too quickly to interrupt, the script fails explicitly. SIGKILL can replay the uncommitted batch, and concurrent model requests may already have incurred charges. Durable progress comes from `sink_state`, not attempt-level counters. Parquet conversion batch size is independent of the commit interval.
