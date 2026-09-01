# Apache Kafka `LICENSE-binary` 维护工作流研究

> 研究日期：2026-09-01。范围：Apache Kafka 官方 `trunk` 源码、官方 Release Process、KAFKA-12622 及其落地 PR。本文记录 upstream 依据，并说明本仓库的对应落地边界。

## 简短结论

Apache Kafka 把 `LICENSE-binary` 当作由人审核和维护的 binary redistribution manifest，而不是从 Maven metadata 自动生成的法律结论。依赖新增、删除或升级时，维护者同步修改它；随后以最终 `releaseTarGz` tarball 为准，运行 `committer-tools/verify_license.py`，只自动核对“实际打包的第三方 JAR 集合”和 `LICENSE` 中可解析的 `name-version` 集合是否一致。许可文本、复杂 license 组合、NOTICE 归属内容仍需要人工判断。

`NOTICE-binary` 是另一份 attribution/NOTICE 文档，不是 `LICENSE-binary` 的替代品；两者在 binary package 中分别落为 `LICENSE` 和 `NOTICE`。SBOM 也不属于这个校验器的输入、输出或替代物；从所查的官方实现看，它是独立关注项，不能用 SBOM 代替发行包中的 `LICENSE`/`NOTICE`。

## 1. 官方文件与构建打包位置

| 对象 | Apache Kafka 官方位置 | 作用 |
|---|---|---|
| 人工维护的 manifest | [`LICENSE-binary`](https://github.com/apache/kafka/blob/trunk/LICENSE-binary) | 先是 Apache License 2.0 正文，后面按 license family 列出 binary 中的第三方依赖及版本；非 Apache 许可组通常用 `see: licenses/...` 指向随包的文本。 |
| 许可文本目录 | [`licenses/`](https://github.com/apache/kafka/tree/trunk/licenses) | 存放 manifest 引用的 license text；不是 JAR 清单本身。 |
| attribution 文档 | [`NOTICE-binary`](https://github.com/apache/kafka/blob/trunk/NOTICE-binary) | 收集项目/第三方 attribution、版权和需要随发行物保留的 NOTICE 内容，例如 Jersey、Lightbend、Hive 等。 |
| 生产包任务 | [`build.gradle#L1392-L1403`](https://github.com/apache/kafka/blob/trunk/build.gradle#L1392-L1403) | `releaseTarGz` 将 `LICENSE-binary` 重命名为包根的 `LICENSE`，将 `NOTICE-binary` 重命名为 `NOTICE`，复制 `licenses/`，并把 runtime/release-only/项目模块 JAR 放到 `libs/`。 |
| 依赖变更提示 | [`gradle/dependencies.gradle#L49-L50`](https://github.com/apache/kafka/blob/trunk/gradle/dependencies.gradle#L49-L50) | 官方代码注释要求添加、删除或升级依赖时同步更新 `LICENSE-binary`，并指向 KAFKA-12622 的验证步骤。 |

因此，检查的权威对象是最终 tarball 内的 `LICENSE` 与 `libs/`，不是仅看源码树里的文件。官方 Release Process 在 “Create Release Artifacts” 部分要求从项目根运行 `python3 ./committer-tools/verify_license.py`，并注明自 4.0.0 起该检查在 CI 执行：

- [Apache Kafka Release Process（官方 Confluence）](https://cwiki.apache.org/confluence/display/KAFKA/Release+Process)
- [Kafka `release/README.md`](https://github.com/apache/kafka/blob/trunk/release/README.md)

## 2. `verify_license.py` 的实际校验逻辑

官方脚本：[`committer-tools/verify_license.py`](https://github.com/apache/kafka/blob/trunk/committer-tools/verify_license.py)（源码行号以该链接当前 `trunk` 为准）。逻辑可以归纳为：

1. 默认执行 `./gradlew clean releaseTarGz`；`--skip-build` 时跳过构建。
2. 从 `core/build/distributions/` 选择最新的、匹配 `kafka_2.13-*.tgz` 且排除 docs 的 tarball。
3. 解压到临时目录，读取包根的 `LICENSE` 和 `libs/`。
4. 对 `libs/` 中的 `.jar` 去掉 `.jar` 后缀；用 `(kafka|connect|trogdor)` 的大小写不敏感正则排除项目自身 JAR，保留第三方 JAR。
5. 用正则只解析 `LICENSE` 中以 `-` 开头、形如 `artifact-version` 的条目：版本要求至少 `x.y`，可再有最多两段数字版本和一个 alpha/suffix。license 段落中的 `see: licenses/...` 不会被当作依赖名。
6. 比较两个集合：
   - `libs - license_deps`：实际随包但 manifest 缺失的 JAR，脚本提示应加入 `LICENSE-binary`；
   - `license_deps - libs`：manifest 有但包内没有的条目，脚本提示应删除；
   - 任一集合非空即退出码 1，否则通过。

对应源码锚点：

- [解析正则和构建/选包：`verify_license.py#L29-L59`](https://github.com/apache/kafka/blob/trunk/committer-tools/verify_license.py#L29-L59)
- [排除项目 JAR、解析 manifest：`#L67-L75`](https://github.com/apache/kafka/blob/trunk/committer-tools/verify_license.py#L67-L75)
- [集合比较和失败条件：`#L115-L146`](https://github.com/apache/kafka/blob/trunk/committer-tools/verify_license.py#L115-L146)

边界要点：它验证的是“JAR inventory 与 manifest 的一致性”，并不读取 Maven POM 来判定许可证、不验证 license URL、不判断多许可证组合是否合法，也不生成或解析 SBOM。它还不自动确认每个 `see:` 文件存在；这类内容仍落在人工 review 和其它 release 检查中。

## 3. 为什么保持人工维护

KAFKA-12622 的官方 Jira 描述给出了最直接的理由：

- [KAFKA-12622: Automate LICENSE file validation](https://issues.apache.org/jira/browse/KAFKA-12622)
- [Jira REST 原始描述](https://issues.apache.org/jira/rest/api/2/issue/KAFKA-12622?fields=summary,description,status,resolution,issuelinks,comment,updated)

Issue 记录指出，2.8.0 的正确 license 文件曾经人工构造，之后很可能再次过期；但自动“生成正确文件”被认为难以可靠完成，因为每个依赖可能改变 license，JAR 可能以不同方式携带 license 文件，POM 或 upstream repository 的链接格式不统一，且可能存在需要人工追踪的 broken URL。因此最终选择是：

- 人工决定每个组件应归入哪个 license family、使用哪些归属/文本说明；
- 自动化只做低风险、可机械判断的集合检查，报告 extra/missing；
- 把检查放入 release 流程（后来由 CI 执行），让依赖变化尽快暴露，而不是假设工具能替法律审阅。

该方案由 [PR #18931](https://github.com/apache/kafka/pull/18931) 落地（标题 `MINOR: Add verify_license tool`，2025-02-18 merge）。PR 说明明确说脚本比较 tarball 中的 libraries 与 `LICENSE` 双向集合，并为便于解析稍微调整 manifest 格式；合并提交 [5413063](https://github.com/apache/kafka/commit/5413063441c59bbc418a7983dad8b423cc1f56cd) 同时修改 `LICENSE-binary`、新增 verifier。

## 4. `NOTICE-binary` 的关系

Apache License 2.0 的 redistribution 条款要求：如果 Work 带有 NOTICE 文件，衍生发行物需要保留其中适用的 attribution notices；同时 NOTICE 内容是 informational，不修改 license 条款。Kafka 因而把两类信息分开：

- `LICENSE-binary`：按组件和 license 组织的许可清单，并指向 `licenses/` 中的文本；
- `NOTICE-binary`：版权、作者、项目来源和其它 attribution/NOTICE 内容。

`build.gradle` 的同一 `releaseTarGz` 任务分别复制两者，分别去掉 `-binary` 后缀；所以包内的 `LICENSE`、`NOTICE` 具有不同职责。`verify_license.py` 只打开包内 `LICENSE` 和 `libs/`，不校验 `NOTICE` 内容，故 NOTICE 的新增/变化不能仅靠该脚本视为完成。

参考：[Apache Kafka `NOTICE-binary`](https://github.com/apache/kafka/blob/trunk/NOTICE-binary)、[Apache License 2.0 §4(d)](https://www.apache.org/licenses/LICENSE-2.0#redistribution)。

## 5. SBOM 是否独立

结论是：独立。

依据不是把 `LICENSE-binary` 重新命名为 SBOM，而是官方实现的职责边界：

1. Kafka 官方 verifier 的输入是 release tarball 中的 JAR 文件名和 `LICENSE` 文本，输出只有 inventory mismatch 的报告/退出码；没有 SPDX/CycloneDX 生成、解析或校验代码（见 [`verify_license.py`](https://github.com/apache/kafka/blob/trunk/committer-tools/verify_license.py)）。
2. 官方仓库 `trunk` 的递归源码树可见 `LICENSE-binary`、`NOTICE-binary` 和 verifier，但没有以 SBOM 为该 workflow 输入/输出的文件或任务：[GitHub tree API](https://api.github.com/repos/apache/kafka/git/trees/trunk?recursive=1)。这只是对当前 `trunk` 的仓库观察，不推断 Apache 的全部供应链政策。
3. 因此 SBOM 可以另行由构建/发布系统针对**确切 artifact**生成并绑定 checksum；但它不能替代发行包必须携带的 `LICENSE`/`NOTICE`，也不能替代人工确认 license family、文本和 attribution。

## 6. 对本仓库的可执行理解

本仓库目前已对齐同样的分层意图：`gradle/dependencies.gradle` 引用 KAFKA-12622；`build.gradle` 的 `releaseTarGz` 把根目录 `LICENSE-binary` 作为包根 `LICENSE`，并把 `NOTICE-binary` 作为包根 `NOTICE`；`committer-tools/verify_license.py` 负责最终 tarball 的 JAR/manifest 检查。AutoMQ 只保留了上游脚本无法覆盖的本地适配：带 prefix 的 tarball、自有模块 artifact 识别，以及 dependency token 解析；维护入口和双向 inventory 校验保持与 Kafka current trunk 一致。

本文记录 upstream 依据；本仓库的落地实现位于根目录 `LICENSE-binary`、
`committer-tools/verify_license.py`、`build.gradle` 和发布 workflow 中。
