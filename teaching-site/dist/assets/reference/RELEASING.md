# 发布与安装验证 / Release and installation checks

这里只构建和验证产物；脚本不会推送tag、上传crates或创建公开Release。
These commands build and verify artifacts; they do not push tags, publish crates, or create public releases.

## 源码包 / Crate packages

```bash
cargo package --workspace --locked
```

`--workspace`按依赖顺序打包两个crate，并验证暂存源码的构建。CLI声明库依赖版本与本地path，支持Cargo的workspace暂存registry。检查本地未提交修改时可额外传`--allow-dirty`；正常发布前应使用干净、已审查的checkout。

Workspace packaging stages the local dependency before verifying the CLI. Use `--allow-dirty` only when intentionally checking uncommitted work.

## 隔离安装冒烟 / Isolated installation smoke test

```bash
python3 scripts/check_install.py
```

脚本把CLI从源码安装到临时prefix，运行版本检查、validate、离线mock生成和inspect，核对完成状态与行数，不改用户默认Cargo安装目录。
The script installs into a temporary prefix, exercises an offline pipeline, and checks the output without replacing the user's installed CLI.

## 二进制包 / Binary bundles

```bash
cargo build --release --workspace --locked
python3 scripts/package_binary.py
```

产物位于`dist/`，包含二进制、README、MIT许可证和SHA-256校验文件，包名记录版本/系统/CPU架构。只在对应系统和架构上运行。
Artifacts in `dist/` contain the executable, README, license, and SHA-256 checksum, named by version, OS, and architecture.

`.github/workflows/release.yml`可手动触发，也响应`v*`标签；Linux/macOS runner构建本机架构的release包并作为workflow artifact保存。普通CI另外执行fmt/clippy/test、教材脚本、workspace打包及隔离安装。远端workflow结果需要在实际运行后确认。

The release workflow runs manually or on version tags and saves Linux/macOS bundles as workflow artifacts. Remote runner success is established only by an actual workflow run.

若以后发布到crates.io，先发布库再发布CLI；两者版本和CLI依赖版本必须一致。当前流程不自动执行这一步。
For a future crates.io release, publish the library before the CLI, with matching package/dependency versions. Publication remains a separate operation.
