/* Course content is a snapshot of the project documentation, 2026-10-23. */
(() => {
  const esc = value => String(value).replace(/[&<>"']/g, ch => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch]));
  const code = (text, name='terminal', lang='bash') => `<div class="code-block"><div class="code-title"><span><span class="code-dots"><i></i><i></i><i></i></span>${esc(name)}</span><button class="copy-button" data-copy>复制</button></div><pre data-language="${lang}"><code>${esc(text.trim())}</code></pre></div>`;
  const note = (title, text, warm=false) => `<aside class="callout${warm?' warm':''}"><strong>${title}</strong><p>${text}</p></aside>`;
  const table = (heads, rows) => `<div class="table-wrap"><table><thead><tr>${heads.map(h=>`<th>${h}</th>`).join('')}</tr></thead><tbody>${rows.map(r=>`<tr>${r.map(v=>`<td>${v}</td>`).join('')}</tr>`).join('')}</tbody></table></div>`;
  const concepts = items => `<div class="concept-grid">${items.map((x,i)=>`<div class="concept"><span>0${i+1} / ${x[0]}</span><h3>${x[1]}</h3><p>${x[2]}</p></div>`).join('')}</div>`;
  const reliable = `version: 1
dataset: {name: learning}
source:
  type: inline
  records:
    - {topic: ownership, answer: 'Each value has one owner.'}
    - {topic: borrowing, answer: 42}
    - {topic: traits, answer: 'Traits define shared behavior.'}
providers:
  generator:
    type: mock
    concurrency: 2
    response: '{"answer": {{ record.answer | tojson }}}'
generate:
  provider: generator
  prompt: 'Explain Rust {{ record.topic }}.'
  output_schema:
    type: object
    required: [answer]
    properties:
      answer: {type: string}
errors:
  max_failed_records: 1
  max_failed_ratio: 0.5
output:
  format: jsonl
  path: output/learning.jsonl`;
  const chapters = [
    {id:'start',number:'01',title:'跑通第一份数据',subtitle:'从零开始，看见一次完整运行',time:'15 min',level:'入门',description:'不需要 API key。用三条输入，理解配置、接受记录、拒绝诊断与运行报告之间的关系。',tags:'mock yaml 快速开始 安装 CLI validate plan run inspect',toc:['准备环境','运行第一个实验','读懂三份结果','动手改一行'],body:()=>`
      <h2 id="section-0">准备环境</h2>
      <p>SynthFlow 是一个 Rust 本地优先的合成数据流水线引擎。我们先用 <strong>mock provider</strong> 学习执行规则，让每次结果都能复现，再接入真实模型。</p>
      ${note('开始之前','准备 macOS / Linux、Rust 1.93+ 和 Python 3。从项目仓库根目录执行下面的命令；首次构建需要下载依赖，mock 实验无需外部模型服务。')}
      ${code('cargo build --workspace\ncargo run -q -p synthflow-cli -- --help','01 / 构建与查看命令')}
      <p>五个命令组成一条学习路径：<code>validate</code> 检查配置，<code>plan</code> 查看计划，<code>run</code> 开始运行，<code>resume</code> 续跑，<code>inspect</code> 查看数据。</p>
      <h2 id="section-1">运行第一个实验</h2>
      <p>每次创建新的临时目录，避免之前的产物占用输出路径。这个例子使用 inline 输入，因此可以独立复制配置。</p>
      ${code(`LAB_DIR="$(mktemp -d)"
cp examples/reliable_generation.yaml "$LAB_DIR/pipeline.yaml"
printf '%s\\n' "$LAB_DIR"
cargo run -q -p synthflow-cli -- validate "$LAB_DIR/pipeline.yaml"
cargo run -q -p synthflow-cli -- plan "$LAB_DIR/pipeline.yaml"
cargo run -q -p synthflow-cli -- run "$LAB_DIR/pipeline.yaml" > "$LAB_DIR/report.json"
cargo run -q -p synthflow-cli -- inspect "$LAB_DIR/output/reliable_demo.jsonl" --json`,'02 / 在独立目录运行')}
      <p>输入中第二条的 <code>answer: 42</code> 是有意保留的错误。mock 原样生成它，而 Schema 要求字符串。预期结果是<strong>接受 2 条、拒绝 1 条、运行完成</strong>。</p>
      ${code(reliable,'pipeline.yaml / 教学配置','yaml')}
      <h2 id="section-2">读懂三份结果</h2>
      ${table(['观察对象','你应该看到什么'],[['正式 JSONL','2 条记录，保留原输入，追加 <code>generated</code> 和 <code>_meta</code>。'],['rejected.jsonl','1 条诊断；字段路径是 <code>/answer</code>，不记录完整原始响应。'],['report.json / manifest','<code>status: completed</code>，<code>committed_source_position: 3</code>。']])}
      <p>为什么拒绝一条，运行仍然完成？配置允许 1 条拒绝，最终比例 1/3 也没有超过 0.5。<strong>单条记录的质量与整个运行的成功条件，是两层不同的规则。</strong></p>
      ${note('stdout 与 stderr','运行报告 JSON 写入 stdout，日志与进度写入 stderr。用 <code>&gt; report.json</code> 保存报告时，终端仍可能显示进度，这是正常行为。')}
      <h2 id="section-3">动手改一行</h2>
      <p>在新的实验目录里，把第二条的 <code>answer: 42</code> 改成字符串，然后重新运行。保持 Schema 不变，预期得到 3 条接受记录和 0 条拒绝记录。</p>
      <p>想在仓库外运行？先执行 <code>cargo install --path crates/synthflow-cli --locked</code>，再用 <code>synthflow run pipeline.yaml</code>。每次新运行使用新的输出命名空间。</p>`,quiz:{question:'第二条记录被 Schema 拒绝，整个运行一定失败吗？',options:['一定失败，Schema 错误会终止整个数据集。','取决于 strict、最大拒绝数和最终拒绝比例。','不会失败，拒绝记录永远不影响运行结果。'],answer:1,explanation:'记录拒绝会被计数并留下诊断；运行是否成功还取决于失败策略。非空数据集全部被拒绝时始终失败。'}},
    {id:'pipeline',number:'02',title:'拆开一条流水线',subtitle:'读懂架构、并发和背压',time:'20 min',level:'核心',description:'跟随一条记录穿过 Source、Template、Provider 和 Sink，理解为什么并发完成与按序提交能够同时成立。',tags:'architecture 架构 异步 tokio semaphore worker 背压 并发 顺序',toc:['从单条记录开始','在浏览器里演练','并发为何仍有序','按职责阅读源码'],body:()=>`
      <h2 id="section-0">从单条记录开始</h2>
      <p>模型调用位于流水线中段。输入先被读取，Prompt 被严格渲染；响应还要经过 JSON 解析、Schema 校验、可选 judge 和去重，才能进入输出。</p>
      ${concepts([['SOURCE','一条一条读','JSONL / CSV 使用流式读取；inline 数据随 YAML 一次加载。输入必须是对象。'],['PROVIDER','异步地生成','mock 模板与 HTTP provider 共享同一个调用边界，便于用确定性测试验证执行器。'],['QUALITY','规则先于落盘','结构约束、分数阈值与去重依次控制接受；错误以明确的诊断类别返回。'],['SINK','按序提交','源位置决定提交顺序。同步数据文件后，再原子替换 manifest。']])}
      <h2 id="section-1">在浏览器里演练</h2>
      <p>选择一条正常记录，或观察一条 Schema 错误如何被送到拒绝文件。这个演练只模拟阶段关系，<strong>不会调用模型或执行本机 CLI</strong>。</p>
      <div class="pipeline-demo" id="pipeline-demo"><div class="demo-toolbar"><label for="demo-scenario">选择输入情景</label><select id="demo-scenario"><option value="good">合法字符串</option><option value="invalid">answer 是数字</option><option value="duplicate">与已有记录重复</option></select><button class="button small" id="demo-run">运行演练 <span>▶</span></button><button class="button secondary small" id="demo-reset">重置</button></div><div class="demo-stages">${['Source','Prompt','Provider','Schema','Dedup','Commit'].map((s,i)=>`<div class="demo-stage" data-stage="${i}">${s}</div>`).join('')}</div><div class="demo-output"><pre class="demo-log" id="demo-log" aria-live="polite">$ synthflow run demo.yaml
等待开始…</pre><div class="demo-record"><span class="demo-label">CURRENT RECORD / 当前记录</span><pre id="demo-record">{"topic": "ownership",\n "answer": "Each value has one owner."}</pre></div></div><p class="demo-caption">教学模拟 · 所选情景均允许拒绝，演示默认 commit_every = 1 的提交语义</p></div>
      <h2 id="section-2">并发为何仍有序</h2>
      <p>假设记录 2 的 HTTP 请求先于记录 1 完成，它会在窗口中等待，而不是抢先提交。执行器最多持有配置并发数的在途记录，包含已经完成、但仍等待顺序的结果。</p>
      ${note('背压是一种保护','慢记录会占用窗口，阻止无限接收新记录。这限制内存积累，同时让输出位置稳定，恢复时可以重建一个明确的已提交前缀。')}
      <p>Source 与存储各有一个专用线程，容量为 1 的邮箱限制排队操作。同步文件 I/O 不占用 Tokio 异步工作线程，网络、心跳和取消仍能继续推进。Provider 还有独立 semaphore；重试退避期间不持有许可。</p>
      <h2 id="section-3">按职责阅读源码</h2>
      ${table(['文件','阅读问题'],[['<code>engine.rs</code>','何时接收下一条？如何等待按序提交？'],['<code>provider.rs</code>','一次物理 HTTP 请求与一次逻辑调用有什么区别？'],['<code>blocking.rs / storage_worker.rs</code>','谁持有输出锁？队列中的写入取消后怎么办？'],['<code>run.rs</code>','数据同步、manifest 更新和最终发布按什么顺序？']])}
      <p>从 CLI 的 <code>execute</code>，沿 <code>Pipeline::load → run_async → run_with_provider → process</code> 阅读。先区分“处理一条记录”和“调度整个运行”，再追踪错误如何回到引擎。</p>`,quiz:{question:'记录 2 已完成，记录 1 还在等待 HTTP 响应，记录 2 会怎样？',options:['立即落盘，因此输出顺序取决于网络。','在有界并发窗口里等待按源顺序提交。','重新调用模型，直到记录 1 完成。'],answer:1,explanation:'并发处理提高利用率，按源位置提交保持稳定的输出前缀。完成的后续记录仍占用窗口，从而对上游产生背压。'}},
    {id:'configuration',number:'03',title:'用 YAML 定义契约',subtitle:'把输入、生成与输出连起来',time:'20 min',level:'核心',description:'理解 DSL、严格模板和 Provider 参数，生成一份可以保存到本地的 mock 配置。',tags:'config 配置 YAML DSL 模板 tojson provider max_completion_tokens RPM TPM rate limit',toc:['六个配置块','配置生成器','接入 HTTP provider','错误发生在哪一层'],body:()=>`
      <h2 id="section-0">六个配置块</h2>
      ${table(['字段','职责'],[['<code>version / dataset</code>','只支持 DSL version 1；dataset.name 为数据集命名。'],['<code>source</code>','选择 inline、jsonl 或 csv。相对路径基于 YAML 所在目录。'],['<code>providers</code>','命名 mock / openai_compatible，配置并发、请求超时与限流。'],['<code>generate</code>','引用 provider、渲染 prompt，用 output_schema 校验生成对象。'],['<code>judge / dedup / errors</code>','可选质量规则与运行失败策略。'],['<code>output</code>','选择 JSONL / Parquet、输出位置、提交间隔和存储契约。']])}
      <p>未知字段会报错，变量缺失也不会被静默替换为空串。向 JSON 插入字符串应使用 <code>{{ record.topic | tojson }}</code>，保证引号、换行等字符被正确编码。模板不能读取环境变量、执行 shell 或访问文件系统。</p>
      <h2 id="section-1">配置生成器</h2>
      <p>选择输出与去重方式，得到一份完整的 inline mock 配置。页面只生成文本；下载后在本机使用 <code>synthflow validate</code> 检查，再执行运行。</p>
      <div class="builder" id="config-builder"><div class="builder-fields"><label for="builder-format">输出格式<select id="builder-format"><option value="jsonl">JSONL</option><option value="parquet">Parquet · 显式 Schema</option></select></label><label for="builder-dedup">去重索引<select id="builder-dedup"><option value="none">不去重</option><option value="memory">exact · memory</option><option value="disk">exact · disk</option></select></label><label for="builder-commit">提交间隔<input id="builder-commit" type="number" value="1" min="1" max="65536" step="1"></label></div>${code('','pipeline.yaml','yaml')}<div class="builder-actions"><span class="builder-status" id="builder-status">完整 mock 配置 · 不调用模型</span><button class="button small" id="download-config">下载 pipeline.yaml <span>↓</span></button></div></div>
      <h2 id="section-2">接入 HTTP provider</h2>
      <p>当 mock 流程通过后，将 provider 换为兼容 Chat Completions 的服务。下面是配置片段；实际请求发往 <code>base_url + /chat/completions</code>。</p>
      ${code(`providers:
  generator:
    type: openai_compatible
    base_url: http://127.0.0.1:8000/v1
    model: local-model
    # api_key_env: LOCAL_LLM_API_KEY
    concurrency: 4
    timeout_ms: 60000
    max_completion_tokens: 2048
    temperature: 0.7
    response_format: {type: json_object}
    retry: {max_attempts: 5, initial_delay_ms: 500, max_delay_ms: 30000}
    rate_limit: {requests_per_minute: 60, tokens_per_minute: 100000}`,'HTTP provider / 配置片段','yaml')}
      <p><code>response_format</code> 支持 json_object、text 和 json_schema。即使选择 text，本地流水线仍要求 JSON 对象。长度与采样参数由服务端模型决定是否支持；<code>seed</code> 不保证模型确定性。</p>
      ${note('RPM / TPM 是发送前的准入控制','每一次 HTTP 重试都重新通过 60 秒滑动窗口。TPM 预估包含消息、修复反馈、响应格式及输出预留；真实 usage 返回后修正预算。估算不能撤销已发送请求。')}
      <h2 id="section-3">错误发生在哪一层</h2>
      <p>字段类型错误、provider 引用不存在、路径冲突属于预检错误，尚未开始记录处理。模板缺变量、模型返回非法 JSON 属于记录拒绝。源解析与写入失败则是运行级致命错误。</p>
      <p>鉴权信息通过 <code>api_key_env</code> 指定的进程环境变量读取，不写进 YAML、模板或清单。未返回 usage 的请求，其服务端费用无法从本地报告得知。</p>`,quiz:{question:'将 pipeline.yaml 移到另一个目录后，source.path: topics.jsonl 会如何解析？',options:['始终相对于运行命令的当前工作目录。','始终相对于仓库根目录。','相对于 pipeline.yaml 所在目录。'],answer:2,explanation:'源、输出等相对路径以 YAML 目录为基准。移动配置文件可能改变规范化路径及配置 hash，恢复前必须与原清单保持一致。'}},
    {id:'quality',number:'04',title:'让数据通过质量关',subtitle:'Schema、重生成、judge 与去重',time:'25 min',level:'进阶',description:'从“模型返回了文本”走向“记录满足数据契约”，理解每一道质量门的不同职责。',tags:'quality schema judge 去重 dedup minhash 中文 characters regeneration retry 重试 失败策略',toc:['四道质量门','重试与重生成','中文近重复','决定何时停止'],body:()=>`
      <h2 id="section-0">四道质量门</h2>
      ${concepts([['SCHEMA','结构是否正确？','生成内容必须是 JSON 对象，字段的类型、required、enum 等约束由 generate.output_schema 定义。'],['REGENERATE','能否带反馈修复？','非法 JSON 或 Schema 不匹配时，可携带安全规则反馈再次生成；额外次数为 0–3。'],['JUDGE','内容是否达标？','结构合格后再次调用 provider，读取单一数值 score，低于阈值拒绝。'],['DEDUP','是否与已有结果重复？','judge 之后按源提交顺序执行 exact 或 MinHash，仅接受通过所有条件的记录。']])}
      <h2 id="section-1">重试与重生成</h2>
      ${table(['机制','触发原因','次数如何理解'],[['HTTP retry','429 / 502 / 503 / 504、网络错误、超时','max_attempts 包含第一次请求。'],['regenerate_on_invalid','生成 JSON 或 Schema 不合格','配置值是额外重生成次数，默认 0。']])}
      <p>401 鉴权错误不会因为重试变正确；Schema 不匹配也不会触发普通 HTTP retry。选择正确的修复层，才能避免反复发出同一个无效请求。</p>
      ${code(`generate:
  provider: generator
  prompt: 'Explain {{ record.topic }}. Return JSON only.'
  output_schema:
    type: object
    required: [answer]
    properties:
      answer: {type: string}
  regenerate_on_invalid: 1
judge:
  provider: reviewer
  prompt: 'Score {{ generated.answer }}. Return JSON with score.'
  min_score: 0.7`,'质量策略 / 配置片段（需另行定义 reviewer）','yaml')}
      <h2 id="section-2">中文近重复</h2>
      <p>exact 对规范化后的 JSON 计算 BLAKE3 摘要。<code>lowercase</code> 只转换字符串值，保留对象字段名，因此 <code>A</code> 与 <code>a</code> 不会因转换键名而丢失信息。</p>
      <p>MinHash 通过 shingle 集合、签名和 LSH 分带寻找候选，再用签名匹配比例估算 Jaccard 相似度。它是近似文本去重，不是语义向量相似度。连续中文可以选择 Unicode 字符窗口：</p>
      ${code(`dedup_storage: disk
dedup:
  method: minhash
  field: generated.answer
  tokenization: characters
  shingle_chars: 3
  num_perm: 128
  bands: 16
  threshold: 0.6`,'中文字符窗口 + SQLite 索引 / 配置片段','yaml')}
      <p>字符模式去除空白并转小写；窗口宽度为 1–32。阈值越高，要求签名越相近。应使用你的实际数据评估误拒绝与漏检，而不是把示例阈值当成通用答案。</p>
      <h2 id="section-3">决定何时停止</h2>
      ${table(['策略','检查时机'],[['strict 或 --strict','首次拒绝记录提交后停止。'],['max_failed_records','拒绝数超过允许值时停止。'],['max_failed_ratio','源处理结束后检查最终比例。']])}
      ${note('拒绝并不意味着可以任意丢弃','重复记为 duplicate，字段缺失记为 dedup_field。内存索引超预算会使运行失败并保留检查点，不会把无法登记的唯一记录假装成重复。')}
      <p>HTTP 响应中的 usage 在内容校验之前记录；失败、重试、拒答和截断响应已返回的用量也会参与费用估算。调用成功、记录接受和请求计费，是三个不同维度。</p>`,quiz:{question:'HTTP 200 的 content 不是合法 JSON，应优先由哪项机制修复？',options:['retry.max_attempts','generate.regenerate_on_invalid','提高 dedup.threshold'],answer:1,explanation:'这是生成内容无效。重生成会加入规则反馈；普通网络重试主要处理临时 HTTP、网络和超时错误。'}},
    {id:'recovery',number:'05',title:'在故障之后继续',subtitle:'检查点、提交前缀与安全发布',time:'25 min',level:'进阶',description:'分清“已经处理”和“已经持久化”，用一个可操作的演示理解提交间隔与恢复窗口。',tags:'recovery checkpoint resume 恢复 故障 manifest partial BLAKE3 SIGINT SIGTERM SIGKILL commit_every 锁',toc:['把运行状态落到文件','试着中断一次','恢复先验证再追加','完成不是只有一个文件'],body:()=>`
      <h2 id="section-0">把运行状态落到文件</h2>
      ${table(['文件 / 状态','含义'],[['<code>data.jsonl.partial</code>','已接受记录的中间文件。即使目标是 Parquet，partial 也始终为 JSONL。'],['<code>data.jsonl.rejected.jsonl</code>','拒绝记录的位置、类别、路径与尝试次数。'],['<code>data.jsonl.manifest.json</code>','源指纹、配置 hash、状态、已提交位置、字节数与两份前缀摘要。'],['<code>data.jsonl.lock</code>','跨进程独占锁；阻止两个写入者同时修改同一输出命名空间。']])}
      <p>一个提交先同步 accepted / rejected，再原子替换并同步 manifest。顶层 processed、accepted 等统计可能包含未持久化尾部；恢复位置应读取 <strong>sink_state.committed_source_position</strong>。</p>
      <h2 id="section-1">试着中断一次</h2>
      <p>假设已经处理 10 条，共有 12 条输入。调整提交间隔，再模拟在下一个提交前被 SIGKILL。这里用所有记录都被接受的简化情景说明边界。</p>
      <div class="checkpoint-lab" id="checkpoint-lab"><div class="checkpoint-controls"><label for="commit-range">commit_every = <strong id="commit-value">4</strong></label><input id="commit-range" type="range" min="1" max="6" value="4"><button class="button small" id="crash-toggle">模拟 SIGKILL <span>↯</span></button></div><div class="checkpoint-cells" id="checkpoint-cells"></div><div class="checkpoint-legend"><span>已提交</span><span>已处理 / 未提交</span></div><p class="checkpoint-explanation" id="checkpoint-explanation" aria-live="polite"></p></div>
      ${note('更多吞吐，意味着更大的重做窗口','默认 commit_every 为 1。提高间隔能减少同步次数，但硬中断可能重做当前批次，另有在途模型请求可能已计费。正常完成仍会提交不足一批的尾部。')}
      <h2 id="section-2">恢复先验证再追加</h2>
      <p><code>synthflow resume pipeline.yaml</code> 接管 failed、cancelled、running 或 publishing 清单，先校验配置 hash、源指纹、路径、独占锁及 accepted/rejected 两个已提交前缀。通过后才截断未提交尾部。</p>
      <p>新清单使用 BLAKE3 摘要检测同长度内容改写。旧清单没有摘要时，只能验证已有结构与长度；下一次提交开始加入摘要。Parquet 还会复核所有已提交行的固定类型，旧版有损前缀会被拒绝，原文件保持不变。</p>
      ${code('synthflow resume pipeline.yaml','恢复 / 需要同一份配置与源文件')}
      ${table(['信号','收尾与退出码'],[['SIGINT / Ctrl+C','取消在途请求，保留已提交前缀，cancelled，退出 130。'],['SIGTERM','执行相同的优雅收尾，退出 143。'],['第二次终止信号','强制退出。'],['SIGKILL','无法清理；manifest 可能仍是 running，尾部需要恢复时校验与截断。']])}
      <h2 id="section-3">完成不是只有一个文件</h2>
      <p>成功发布通过同目录硬链接创建正式文件，不覆盖已有路径。数据文件与 manifest 不是跨文件事务，发布窗口可能同时出现正式文件和 publishing 状态。</p>
      ${note('消费者的完成条件','正式文件存在，且 manifest.status 为 completed。恢复 publishing 时会验证最终文件的 publication_digest，并校验拒绝前缀。')}
      <p>更改配置、源内容、输出位置、去重后端或预算后，不能冒用旧检查点继续。需要新输出路径重新运行。已经 completed 的清单也不会重复生成。</p>`,quiz:{question:'processed=10，commit_every=4，manifest 已提交位置为 8。SIGKILL 后从哪里继续？',options:['从第 11 条开始，处理过的数据不会重做。','从第 9 条开始，先校验并截断未提交尾部。','从第 1 条开始，所有中间结果都不可用。'],answer:1,explanation:'持久化进度以 manifest 的 committed_source_position 为准。第 9、10 条虽已处理，但未提交，需要重做；还可能存在已经计费的在途调用。'}},
    {id:'data',number:'06',title:'设计可用的数据集',subtitle:'Parquet 契约、血缘与 inspect',time:'20 min',level:'进阶',description:'区分模型响应的结构校验和最终数据集的存储契约，避免空值、空数组与数字精度成为隐患。',tags:'data parquet arrow output.schema null nullable optional int64 uint64 float schema inspect 统计 metadata 血缘',toc:['两个 Schema 两种责任','先声明再写入','保留每条记录的来路','用 inspect 检查数据'],body:()=>`
      <h2 id="section-0">两个 Schema，两种责任</h2>
      <p><code>generate.output_schema</code> 校验模型生成的对象。<code>output.schema</code> 定义完整 Parquet 输出，包括原输入字段、generated，以及启用时的 judge。<strong>_meta 由系统自动加入，不要手动声明。</strong></p>
      ${note('首条推断的局限','未显式声明时，从首条通过检查的记录推断固定 Schema。首行的 null 或空数组不能预知后续类型，之后出现新字段或有损数值转换会被拒绝。')}
      <h2 id="section-1">先声明，再写入</h2>
      ${code(`output:
  format: parquet
  path: output/data.parquet
  schema:
    type: object
    required: [topic, generated]
    properties:
      topic: {type: string}
      count: {type: [integer, "null"]}
      tags:
        type: array
        items: {type: string}
      generated:
        type: object
        required: [answer]
        properties:
          answer: {type: string}`,'存储契约 / 配置片段','yaml')}
      <p><strong>optional 允许字段缺失，nullable 允许显式 null。</strong>required 字段即使允许 null，也必须存在。缺失的可选属性在 Parquet 中写作 null。</p>
      ${table(['类型','存储含义'],[['integer','Int64；配合 format: uint64 可用无符号 64 位。'],['number','Float64；不能精确表示的整数提升会被拒绝。'],['object / array','对象显式 properties，数组显式 items，可嵌套和可空。']])}
      <p>这套存储契约支持 JSON Schema 的有限子集。额外字段、任意类型联合、引用及不支持的约束会被拒绝。更丰富的生成质量规则放在 <code>generate.output_schema</code>。</p>
      <h2 id="section-2">保留每条记录的来路</h2>
      <p>输出保留原字段，再追加 generated、可选 judge 和 _meta。元数据连接记录 ID、源位置、源指纹、配置 hash、run ID、provider/model、prompt hash 与尝试次数。</p>
      ${concepts([['IDENTITY','记录身份','身份纳入源位置和记录值，忽略对象键的排列顺序。'],['LINEAGE','可追溯关系','恢复后 run ID 可以变化，但元数据仍能说明记录来自哪份源与哪次执行。']])}
      <h2 id="section-3">用 inspect 检查数据</h2>
      ${code('synthflow inspect output/data.jsonl\nsynthflow inspect output/data.parquet --json','顶层列摘要')}
      <p>inspect 汇总行数、字节数、顶层字段类型、非空/空值计数、数值 min/max/mean 和有界 distinct。混合列的数值统计只使用数值项；整数极值保留原始精度，均值是防溢出的浮点近似。</p>
      ${note('统计也有资源边界','单列 distinct 达到一百万或共享统计预算耗尽时返回 null，其他统计继续。Arrow 解码内存不等于统计状态预算。当前没有 DataFusion SQL 查询。')}
      <p><a class="text-link" href="assets/examples/parquet_explicit_schema.yaml" download>下载完整显式 Schema 示例 <span>↓</span></a></p>`,quiz:{question:'字段没有写进 required，但 type 只有 string。它可以是 null 吗？',options:['可以，optional 与 nullable 是同一回事。','不可以；可以缺失，但显式 null 仍需在 type 中声明。','只有首条记录可以是 null。'],answer:1,explanation:'optional 控制字段是否必须出现；nullable 控制出现时是否接受 null。二者是独立约束。'}},
    {id:'performance',number:'07',title:'有边界地跑到百万条',subtitle:'预算、性能测量与能力范围',time:'20 min',level:'工程实践',description:'用可复现的测试看待吞吐和内存，选择去重后端，并认识当前实现的适用范围。',tags:'performance 性能 百万 release benchmark memory disk sqlite 预算 RSS throughput 瓶颈',toc:['先选择存储后端','读一组真实测量','复现你的工作负载','当前能力的边界'],body:()=>`
      <h2 id="section-0">先选择存储后端</h2>
      <p>memory 索引随唯一记录数增长，默认记账预算 64 MiB。disk 使用可重建的临时 SQLite 索引，页缓存目标 2 MiB，逐条读取候选；正常退出清理，恢复时从已提交前缀重建。</p>
      ${table(['后端','适合的选择','需要理解的边界'],[['memory','较小唯一集合、较低磁盘开销','预算不是进程 RSS 上限，超限会明确失败。'],['disk','较大唯一集合、稳定控制索引内存','消耗临时磁盘与 I/O；不是跨运行共享索引。']])}
      <p>测试使用的 exact 每条唯一记录按 128 bytes 记账；128 维、16 bands 的 MinHash 按每条 3,200 bytes 记账。算法参数会影响容量，不能只看记录数量。</p>
      <h2 id="section-1">读一组真实测量</h2>
      <p>2026-10-22 本机实测：Apple M1 Pro、32 GiB、macOS arm64、Rust 1.93.1 release。每条 64-byte 唯一文本，并发 8，提交间隔 1024。每组两次；耗时取中位数，RSS 取最大值。</p>
      ${table(['Case','记录数','完整运行','峰值 RSS'],[['JSONL','100,000','8.040 s','17.55 MiB'],['exact-disk','100,000','10.471 s','21.23 MiB'],['JSONL','1,000,000','64.151 s','17.44 MiB'],['exact-disk','1,000,000','96.501 s','21.44 MiB']])}
      ${note('测量上下文也是结果的一部分','这是离线 mock 管线吞吐，使用批量提交；不代表真实模型延迟、默认逐条同步性能或严格内存保证。JSONL 逐行校验顺序与内容，源准备与脚本验证不计时。',true)}
      <p>默认预算下，百万条 exact-memory 在已提交 524,288 条后失败；十万条 MinHash-memory 在已提交 20,480 条后失败。两者均保留检查点，说明预算保护按预期工作，而非完整输入处理成功。</p>
      <h2 id="section-2">复现你的工作负载</h2>
      ${code(`cargo build --release --workspace --locked
python3 scripts/benchmark.py \\
  --records 100000,1000000 --cases jsonl exact-disk \\
  --commit-intervals 1024 --concurrency 8 --repeat 2 \\
  --output /tmp/synthflow-scale.json`,'规模测试 / 无外部模型调用')}
      <p>从小规模开始，固定数据、二进制、磁盘和配置，再增加样本。原始 JSON 记录构建指纹、环境、wall time、峰值 RSS、校验方法和失败信息。两次观测不足以可靠描述尾延迟。</p>
      <p>脚本还支持 Parquet、judge、MinHash、多提交间隔和 SIGKILL 恢复。Parquet 使用流式 inspect 聚合校验，<strong>不等同于逐行内容与顺序证明</strong>。强杀恢复测试建议使用独立 TMPDIR，结束后清理所属目录中的临时索引。</p>
      <h2 id="section-3">当前能力的边界</h2>
      ${table(['当前实现','尚未实现'],[['单机本地 JSONL / CSV / inline','分布式执行、远程流式源'],['单分数 judge、exact / MinHash','多维 judge 表达式、语义向量去重'],['Arrow / JSONL inspect','DataFusion SQL 查询'],['crate 与二进制包工作流','自动公开发布、跨运行增量数据集管理']])}
      <p>架构设计文档包含历史蓝图。遇到冲突时，以当前代码、<code>docs/STATUS.md</code> 和可执行测试为依据。</p>
      <p><a class="text-link" href="assets/reference/PERFORMANCE.md" download>下载完整性能报告与复现说明 <span>↓</span></a></p>`,quiz:{question:'将去重预算从 64 MiB 调到 512 MiB 后，可以直接用新配置 resume 原检查点吗？',options:['可以，预算只影响速度。','不可以；预算改变配置 hash，需要新输出路径重新运行。','只要手动改 manifest 的 hash 就可以。'],answer:1,explanation:'配置 hash 保护运行语义。后端、预算、提交间隔等变化都需要新运行，不能修改清单来绕过一致性检查。'}},
    {id:'labs',number:'08',title:'把理解变成实验',subtitle:'九个实验，一次工程闭环',time:'60–90 min',level:'动手实践',description:'运行、观察、提出假设，再用测试验证。把浏览器中的概念带回真实代码与本地终端。',tags:'labs 实验 教学 练习 测试 homework 扩展 参考答案',toc:['实验如何进行','九个动手任务','工程验证清单','完成之后'],body:()=>`
      <h2 id="section-0">实验如何进行</h2>
      <p>从仓库根目录开始，使用 Bash / Zsh。实验 1–5 每次创建独立临时目录；只复制 inline 示例，文件源示例必须连同数据源一起保留。先写出预期，再执行命令，最后解释结果。</p>
      <p>下面是精简任务卡。完整可执行命令与验证脚本收录在 <a class="text-link" href="assets/reference/LABS.md" download>实验手册 ↓</a>，建议下载后边做边记录。</p>
      <h2 id="section-1">九个动手任务</h2>
      ${[
        ['观察完整数据流','运行 reliable_generation.yaml，找出接受记录、拒绝诊断和 manifest 提交位置。','2 条接受、1 条拒绝、位置 3。第二条 answer=42 不满足 string 契约，但允许的失败比例使运行完成。'],
        ['同一条错误，不同失败策略','在新目录中加 --strict；再分别尝试 max_failed_records=0 与 max_failed_ratio=0.2。','strict 在第二条拒绝提交后停止；最大数限制在超限时停止，最终比例在源结束后检查。预期退出码为 1。'],
        ['让配置错误可以定位','将 max_failed_records 改为字符串，保存错误报告并检查字段路径。','属于预检错误；报告包含 errors.max_failed_records 的路径，不回显错误值，也不产生正式输出。'],
        ['修复一条 Schema 失败记录','保持 Schema 不变，把数字 42 改为字符串，用新输出目录重跑。','3 条接受、0 条拒绝。修复数据与放宽契约是两种不同的工程决策。'],
        ['验证禁止覆盖','成功后对同一命名空间再次 run，对比正式文件的前后字节。','第二次预检失败，已有文件完全不变。只移走正式文件也不足以重新占用仍有 manifest 的命名空间。'],
        ['不用真实模型测试 HTTP','运行 reliability 中的重试、超时和并发测试，阅读回环 HTTP fixture。','429→503→200 是 3 次物理请求、2 次重试。直接调用 provider 的测试用于独立验证 semaphore。'],
        ['中断与持久化前缀','运行 faults 中 SIGINT 与强杀测试，观察 manifest 的 cancelled / running 与后续 resume。','SIGINT 退出 130；SIGKILL 无法写收尾状态。恢复先验证前缀，再重做未提交部分。'],
        ['验证已实现的高级功能','选择 CSV、重生成、限流、judge、去重、Parquet、resume、inspect 中的回归测试，解释断言。','用局部测试验证特定契约。CSV 保留字符串类型，Parquet 保持固定 Schema，resume 保持按序前缀。'],
        ['设计并实现一个扩展','提出一个小扩展，先写输入/输出契约、资源预算与恢复语义，再实现并验证。','不要只添加一条 happy path。至少覆盖无效输入、失败/取消、边界规模与兼容性；多维 judge、语义去重等目前仅是扩展方向。']
      ].map((l,i)=>`<section class="lab-card"><header><span class="lab-number">LAB ${String(i+1).padStart(2,'0')}</span><h3>${l[0]}</h3></header><p>${l[1]}</p><details><summary>查看预期与解释</summary><p>${l[2]}</p></details></section>`).join('')}
      <h2 id="section-2">工程验证清单</h2>
      ${code(`cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
python3 scripts/check_labs.py`,'从行为验证到完整检查')}
      <p>先运行与你的改动相关的测试。只有新的改动、失败或未解决问题需要更多证据时，再扩大检查范围。README 中还有打包、隔离安装和 release 基线命令。</p>
      <h2 id="section-3">完成之后</h2>
      <p>你应该能够解释：一条记录为何被拒绝、何时意味着运行失败、哪一段数据已经持久化、恢复为何安全，以及性能结果在哪些条件下成立。</p>
      ${note('下一步不是再加一个模型调用','选择一个你能定义清楚边界的小问题。记录“预期是什么、故障时怎么办、如何证明”，再动手实现。')}
      <a class="button secondary" href="#resources">打开课程资料库 <span>↗</span></a>`,quiz:{question:'为“恢复不重复输出”增加测试时，最有说服力的验证是什么？',options:['只断言 resume 返回了 Ok。','只看日志里出现 completed。','检查状态、提交位置，并逐行验证最终输出的数量、顺序与内容。'],answer:2,explanation:'Ok 可能只表示返回了一份失败报告。测试必须验证具体持久化结果，才能覆盖重复、遗漏、乱序与错误恢复位置。'}}
  ];
  const resources = [
    ['TEACHING_GUIDE.md','源码导读','11 章，从问题建模到可靠执行。'],['LABS.md','双语实验手册','9 个实验，含完整命令与验证脚本。'],['GLOSSARY.md','工程术语表','为 semaphore、backpressure、lineage 建立共同语言。'],['PARQUET_SCHEMA.md','Parquet 输出契约','显式 Schema、可空字段和无损数值边界。'],['PERFORMANCE.md','性能与容量报告','测量条件、复现命令与各类后端观察。'],['STATUS.md','当前能力状态','区分当前实现、兼容规则与历史设计。'],['RELEASING.md','打包与交付指南','源码包、隔离安装与二进制发布工作流。'],['PROJECT_README.md','项目 README','命令、参数和架构的完整参考。']
  ];
  function makeConfig(format,dedup,commit){let result=`version: 1
dataset: {name: my_first_pipeline}
source:
  type: inline
  records:
    - {topic: ownership}
    - {topic: borrowing}
providers:
  generator:
    type: mock
    concurrency: 2
    response: '{"answer": {{ record.topic | tojson }}}'
generate:
  provider: generator
  prompt: 'Explain {{ record.topic }}.'
  output_schema:
    type: object
    required: [answer]
    properties:
      answer: {type: string}
`;if(dedup!=='none')result+=`dedup_storage: ${dedup}
dedup:
  method: exact
  fields: [generated.answer]
`;result+=`output:
  format: ${format}
  path: output/my_first_pipeline.${format}
  commit_every: ${commit}
`;if(format==='parquet')result+=`  schema:
    type: object
    required: [topic, generated]
    properties:
      topic: {type: string}
      generated:
        type: object
        required: [answer]
        properties:
          answer: {type: string}
`;return result;}
  window.SYNTHFLOW_COURSE={chapters,resources,code,note,table,esc,reliable,makeConfig};
})();
