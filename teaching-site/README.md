# SynthFlow 静态教学网站

中文工程课程，包含 8 个章节、9 个实验任务、配置生成器、流水线与检查点演练、章节自测、搜索和本机学习进度。

无需安装前端依赖或构建。全部网站文件位于 `dist/`，可以部署到任何静态托管服务。

```bash
python3 -m http.server 4173 --bind 127.0.0.1 --directory teaching-site/dist
```

从仓库根目录运行，再打开 `http://127.0.0.1:4173`。也可以直接打开 `dist/index.html`；本地文件模式下，浏览器可能限制剪贴板和进度存储。

- `dist/index.html`：布局与可访问性框架。
- `dist/assets/style.css`：响应式样式、打印模式与减少动态效果支持。
- `dist/assets/content.js`：课程正文、示例和自测。
- `dist/assets/app.js`：导航、搜索、进度、演练和配置下载。
- `dist/assets/examples/`、`examples.zip`：项目示例快照与离线包。
- `dist/assets/reference/`：项目教学文档与基准结果快照。

浏览器演练仅模拟项目语义，不调用模型、不运行 CLI、不操作本机项目文件。学习进度存于 localStorage，没有账户或数据上传。静态内容更新时应同步项目示例与参考文档。
