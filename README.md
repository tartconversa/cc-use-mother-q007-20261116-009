# RailWatch 铁路新闻证据台

RailWatch 服务铁路运营信息分析员。它保存已登记来源的新闻原文，提取可由原文引句核对的指标主张，并要求审核员以范围版本提交决议。若同一指标存在冲突，当前范围保持未解决且不能发布。发布后的证据快照包含来源链接、引文与稳定指纹，不受后续文章版本影响。

系统使用 SQLite 持久化和 FastAPI 接口。网络采集是可选运行路径；测试与演示使用本地合成数据，不依赖新闻网站在线状态。演示中的 example.org 文字与数值不是新闻报道事实。

## 本地验收

预装 requirements.txt 中锁定的依赖后，在项目根目录执行：

    python -m unittest discover -s tests -v
    python -m compileall -q railwatch
    python -m railwatch.demo

演示打印采集 ID、审核范围版本、跳变观测倍数、核查事项状态、快照摘要、数值与溯源条数；业务断言不成立时以非零状态退出。

## 接口与输入

设置 RAILWATCH_DB 为可写的 SQLite 路径，使用 ASGI 服务启动 railwatch.api:app。运行时可在 /docs 查看 OpenAPI。主要接口包括来源策略、文章回放与批量回放、审核队列与候选比较、审核决议、范围、发布快照、当前及历史报表、采集任务和质量诊断。

文章输入包括来源 ID、文章外部 ID、原文 URL、带时区的采集和发布时间、标题、正文、媒体类型和元数据。JSON 正文包含发布文本 text 和带引句的 claims 数组。主张按项目、类型、指标和期间归属，数值类需有受支持的单位；里程碑类需有状态。缺失引句或非法字段会隔离文章，不会进入报告。

审核员提交接受、拒绝或暂缓决议，同时提供审核人、理由和当前范围版本。旧版本决议失败。发布要求所有纳入范围都已选定且无冲突，既有快照在后续采集和审核变化后仍可复核。

## 量级跳变核查事项

高铁运量序列的相邻期间出现数量级跳变时，系统不靠一次性阈值判断放过或拦截，而是把检测结果落为可追踪的核查事项（anomaly review item）：

- 阈值按 `(项目, 指标)` 版本化登记（`PUT /anomaly/config`，带 CAS 版本）；未登记时使用保守默认倍数 3。检测在 claim 复核通过后自动运行，也可由 `POST /anomaly/scan` 手动重扫，重复扫描幂等。
- 每个事项锚定跳变前后两条 claim，冻结观测倍数、触发阈值与配置版本，并保留两条 claim 的引文、来源 URL、采集时间、内容指纹等完整上下文（`GET /anomaly/items/{id}`）。
- 复核动作包括 `confirm`（确认合理跳变）、`dismiss`（误报）、`request_evidence`（要求来源补证）与 `accept_evidence`（确认补证充分），均需审核人、理由和事项版本号。同序列更晚期间的补证文章到达后自动关联，事项进入 `evidence_received`，但自动关联不等于放行，仍须复核人确认。
- 状态机：`open → confirmed / dismissed / evidence_requested → evidence_received → supplemented`；锚点 claim 随来源更正被撤回或拒绝时，事项自动 `void`。
- 失效规则：阈值放宽后不再超阈的**待决**事项自动 `config_cleared` 并解除拦截；仍超阈的待决事项跟随新阈值；人工终态（confirmed/dismissed/supplemented）永不被配置变化覆盖，只在解释中标记 `config_stale` 供复核人判断是否需要重审。
- `open`、`evidence_requested`、`evidence_received` 状态的事项会阻止相应当前指标发布（错误码 `anomaly_unresolved`），质量视图 `/quality/readiness`、复核队列 `/anomaly/items`、当前报告 `/reports/current` 与发布快照共用同一份解释。快照冻结当时的异常列与阈值，之后的阈值重算、新文章和复核动作都不会改写历史快照及其指纹。

## 命令行与采集

使用 python -m railwatch --db local.sqlite3 source 加 JSON 来源策略登记来源；replay 命令回放本地清单；show scopes 查看当前审核范围；snapshot 配合发布名和项目键冻结证据。anomaly-config、anomaly-scan、anomaly-review 和 show anomalies 分别管理阈值配置、手动扫描、核查决议与复核队列。具体参数可用 python -m railwatch --help 查看。

采集任务仅面向已登记的来源主机。采集端限制响应大小和跳转，校验内容类型，支持条件请求；SQLite 租约防止多个工作进程同时领取同一任务。失败任务保留错误供重试。
