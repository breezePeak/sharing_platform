# 任务接口契约（PR-03/04/05）

本页是 JSON 任务接口的实施契约。PR-04 接入 PUSH，PR-05 接入单次 PULL 和验收回执，PR-06 支持引用监听配置执行单次 PULL。旧 XML 仍为暂定契约，默认关闭。PULL 验收回执使用 JSON 接口。

## 身份与配置

所有 `/api` 任务接口使用 `Authorization: Bearer <token>`。调用方身份由服务端 `platform.transfers.callers` 中的令牌绑定，忽略客户端自报的 callerId。每个调用方必须配置允许的 `endpoint-ids` 和固定 `notification-url`。令牌、连接凭据及通知地址均不来自任务请求。页面仍无需登录；API 使用独立令牌。

例如在部署配置中加入（实际令牌由环境密钥注入，不提交到仓库）：

```yaml
platform:
  transfers:
    workers: 2
    notification-attempts: 5
    callers:
      scheduler:
        token: ${SCHEDULER_API_TOKEN}
        endpoint-ids: ["已配置的 endpoint UUID"]
        notification-url: https://已授权接收端.example/notify
        xml-routes:
          weather: "已配置的 endpoint UUID"
```

`endpoint-ids` 与 `xml-routes` 由管理员维护。通知 URL 只允许 HTTP(S)，不跟随重定向。密钥应使用不同于数据库凭据的独立值。没有配置调用方时，API 返回 401。

支持在同一调用方下配置完整路由；无需新管理系统，修改受限外置 YAML 后按现有方式重启加载：

```yaml
platform:
  transfers:
    callers:
      scheduler:
        token: ${SCHEDULER_API_TOKEN}
        endpoint-ids: ["endpoint UUID"]
        notification-url: https://receiver.example/notify
        routes:
          weather:
            enabled: true
            version: "2026-11-16.1"
            endpoint-id: "endpoint UUID"
            source-root: source
            target-root: deliver
            notification-url: https://receiver.example/weather-notify
```

`source-root` 是 `platform.endpoints.local-roots` 的本地目录引用；`target-root` 是相对于 endpoint.writeRoot 的允许交付子树。请求 `sourceRoot` 必须匹配路由引用，`targetPath` 必须等于 `target-root` 或位于其下；不自动改写请求路径。XML `type` / JSON `sourceCode` 命中路由后校验 endpoint、启用状态、路径和版本；endpoint 仍须在调用方授权列表内。路由通知 URL 可省略并继承调用方 URL。禁用路由的新任务返回 400 `ROUTE_DISABLED`，配置不完整返回 `ROUTE_NOT_CONFIGURED`。

旧 `xml-routes: {type: endpointId}` 与直接指定 endpoint 的 JSON 请求继续可用，源根引用和目标范围沿用 endpoint 修订、通知继承调用方；新任务保存 `legacy-1` 快照。新旧路由同名时 `routes` 优先。完整快照含 callerId、type、启用状态、版本、实际源目录、目标范围、endpoint 修订及通知 URL，与任务同事务保存。重放先按原规范请求及原 endpoint 比较，之后路由改名、改目标/通知或停用均不能改变原任务；历史任务快照为空时继续使用原 endpoint 修订、源根和通知字段。

## 创建与重放

`POST /api/transfers/tasks`，`Content-Type: application/json`。请求上限 32 KiB，未知字段拒绝。`taskId` 是调用方范围内的外部业务号，`taskRecordId` 是平台内部号。根目录引用和 endpoint 均由服务端预配置；路径必须相对于受控根目录。

推送示例：

```json
{"taskId":"job-001","direction":"PUSH","endpointId":"<uuid>","sourceCode":"weather","sourceRoot":"configured-local-source-ref","targetPath":"batch/001","products":[{"productId":"product-1","productPath":"products/product-1","productName":"示例产品"}]}
```

PULL 请求字段见 [来源配置](#pull-来源配置与源清单)和[用监听配置执行单次拉取](#用监听配置执行单次拉取)。

新受理返回 HTTP 202：

```json
{"taskRecordId":"<uuid>","taskId":"job-001","accepted":true,"reused":false,"executionStatus":"QUEUED"}
```

同一调用方和任务号、相同规范化业务内容返回 HTTP 200，`reused=true`，保留原配置修订和任务号。方向、endpoint、来源代码、路径或产品范围变化返回 409 `TASK_ID_CONFLICT`。产品顺序和 JSON 属性顺序不影响指纹；产品显示名参与比较。XML `type` 对应 JSON `sourceCode`，跨格式重放时需提供相同来源代码。配置编辑后重放原请求仍按原快照比较。受理事务失败返回 500，不返回 `accepted=true`。

错误体为 `{"code":"..."}`。常见状态：400 `INVALID_TASK` / `ENDPOINT_DISABLED`，401 `AUTH_REQUIRED`，403 `ENDPOINT_FORBIDDEN`，404 `TASK_NOT_FOUND`，409 `TASK_ID_CONFLICT`，413 `REQUEST_TOO_LARGE`（请求体超过 32 KiB），503 `ENGINE_NOT_READY` / `NOTIFICATION_NOT_CONFIGURED`。

## 查询与文件分页

`GET /api/transfers/tasks/{taskRecordId}` 只返回当前调用方的任务、产品、执行状态、阶段、验收状态和各通知摘要。PUSH 清单冻结后另返回 `totalFiles`、`totalBytes`、`uploadedBytes`、`verifiedFiles`、`verifiedBytes`；上传和已验证字节分别统计，字节到 100% 仍须等待发布与通知。新增 `uploadTrafficBytes`（交给协议流的累计上传字节）、`readbackBytes`（业务文件实际回读字节，包含重复核验）、`retransmittedBytes`（逐文件累计上传减业务高水位之和，最小为 0）与 `routeVersion`。控制文件/探测流量不计业务进度。`uploadedBytes` 按逐文件高水位去重且不超过 `totalBytes`；`verifiedBytes` 只累计验证通过的唯一文件。流内每 8 MiB 或 1 秒及结束/异常刷新，不为每个缓冲区写库。`GET /api/transfers/tasks/{taskRecordId}/files?page=0&size=50` 限 `size=1..100`。PUSH 清单冻结前返回 `manifestFrozen=false`、`totalFilesKnown=false` 和空 `files`；冻结后返回 `totalFiles` 及逐文件产品 ID、相对路径、字节数、SHA-256 和验证状态。文件数不等于产品数。

PUSH 的 `fileCount` 可选；提供时必须等于 `products` 数量。每个产品对应受控源根目录下的非空产品目录。默认拒绝零字节文件及 `.tmp`、`.part`、`.crdownload` 后缀，可通过 `platform.transfers.allow-zero-byte-files` 和 `platform.transfers.temporary-suffixes` 调整。PUSH 连接需配置 `writeRoot`、`probeDirectory` 和 `localSourceRef`；预检在探测目录实际写入、回读、改名并清理。预检通过后的 PRECHECK 通知获关联 ACK 前不上传业务文件。

交付位置为 `targetPath/sourceCode/taskRecordId/productId/相对路径`（未提供 `sourceCode` 时使用安全的调用方 ID）。任务根目录的 `_manifest.xml` 记录固定目录和文件大小、SHA-256；最后发布的 `_READY` 为 UTF-8 文本，含 `taskId`、`manifestSha256`、`productCount`。接收方须按匹配的标记与清单消费，外部规则仍需联调。发布结果不明时状态为 `RECONCILING`，不发失败完成单；重入先回读标记和清单，再决定是否续发。确认发布后执行状态为 `SUCCEEDED`，阶段进入 `NOTIFYING`；COMPLETE ACK 与阶段 `COMPLETE` 同事务提交。已确认成功的 COMPLETE 通知列出全部原产品及相对交付路径；通知补发不重新传输。

## 通知与 ACK

测试处理器产生阶段结果时，结果和通知在同一事务保存。通知按配置 URL 发送 JSON，例如：

```json
{"notificationId":"<uuid>","taskRecordId":"<uuid>","taskId":"job-001","stage":"COMPLETE","result":"SUCCEEDED"}
```

接收端必须返回 HTTP 2xx 和 JSON `{"accepted":true,"notificationId":"<原编号>","taskRecordId":"<原内部任务号>"}`。仅 2xx 不算 ACK。响应丢失或关联不符按同一编号与原报文重发；默认最多 5 次，之后保留 `NEEDS_ATTENTION`。通知补发不重新调用传输处理器。`PRECHECK` 与 `COMPLETE` 为不同阶段，每阶段只允许一个有效结果。
失败结果包含可用的 `errorType` 和 `errorDetail`，不会把底层异常原文或凭据自动写入通知。

## 运行边界

任务到期后由固定大小线程池领取；线程池满时记录留在数据库。进程重启后，失去心跳超过 5 分钟的领取可恢复，旧领取身份不能覆盖新结果。任务和通知记录不因关闭入口而删除。当前实现不提供手动重置 `NEEDS_ATTENTION` 的操作；运维恢复入口随后续监控阶段补齐。

## PULL 来源配置与源清单

以下为本轮实施契约，真实业务端尚须联调确认。配置在受限的外置 Spring YAML 中，来源快照和配置摘要随任务持久化。`sourceId` 是稳定来源身份；新增任务不能自带主机、凭据、回调 URL、校验基准或本机绝对路径。调用方必须同时获得 endpoint 和来源的授权。

```yaml
platform:
  endpoints:
    local-roots:
      delivery: D:/sharing/delivery
  pulls:
    intake-enabled: true
    sources:
      weather:
        endpoint-id: <已配置连接的 UUID>
        source-path: incoming
        target-ref: delivery
        caller-ids: [scheduler]
        recipient-id: business
        access-base: smb://business-share/weather
        unit-type: PACKAGE_DIR
        scan-depth: 1
        file-pattern: '*'
        stable-observations: 3
        observation-seconds: 60
        validation: HASH
        manifest-name: _manifest.json
        ready-name: _READY
        acceptance-seconds: 86400
```

连接需设置 `readRoot` 和 `localTargetRef=delivery`；本机根目录应已存在且受操作系统权限保护。`scheduler` 和 `business` 在 `platform.transfers.callers` 配置独立令牌、允许的 endpoint 与固定通知 URL。交付通知发往 `business` 的固定 URL；尚未产生源版本的整单接入/执行失败使用任务调用方的通知 URL。

`sourcePath` 相对连接 `readRoot`，必须处于来源 `source-path` 或其子路径；`targetPath` 必须等于来源和连接的交付引用。`scan-depth=0`（默认）表示该路径本身是单元；`1..8` 表示在范围内第几层匹配单元，使用标准文件名 glob。PACKAGE_DIR 递归保留目录、空目录和文件，不解压压缩文件；FILE 直接复制普通文件。业务单元上限 10000，单元内文件及目录合计上限 100000、递归层级上限 32，超限明确失败。

未配置 `ready-name` 时按完整、间隔达到 `observation-seconds` 的相同观察累计稳定次数；部分扫描失败不会累计。默认 3 次/60 秒只是来源就绪约定，不能证明上游没有漏生产。配置完成标记时按该信号判断；同时有清单时，标记必须绑定清单的 SHA-256。BASIC 无清单时记录 `SIZE_MTIME_SET / WEAK`，无法发现所有同大小同修改时间覆盖。HASH 缺少基准保持 `WAITING_SOURCE`，不降级。

PACKAGE_DIR 的清单位于 `单元路径/manifest-name`；FILE 的清单为 `文件路径.manifest-name`，例如 `a.bin._manifest.json`。完成标记使用相同规则。清单为不超过 1 MiB 的 UTF-8 JSON：

```json
{"version":"upstream-v1","files":{"nested/a.bin":"<64 位 SHA-256 小写十六进制>"}}
```

`files` 必须精确覆盖业务文件；FILE 使用原文件名作为键，目录使用单元内相对路径。清单文件本身的 SHA-256 是强版本依据；`version` 保留上游版本说明。`_READY` 内容为该清单的 64 位 SHA-256。源控制文件不交给业务区；通知中的 `totalFiles/totalBytes` 只计算业务文件，`controlFiles/controlBytes` 单独记录。未配置清单时 BASIC 的 `expectedHash=null`，即使有本地 `actualHash` 也不声称 HASH 通过。

## PULL 查询、通知与验收

任务详情的 `deliveries` 逐单元返回 `transfer_id`、`source_version`、版本依据与强弱、技术状态、验收状态、原因、位置及超时提醒时间。文件分页返回 `transfer_id`、`relative_path`、`size_bytes`、`downloaded_bytes`、`expected_hash`、`actual_hash`、`verified`。待就绪单元使整单总量保持未知，但已冻结单元仍可分页查询。`downloadedBytes` 与 `verifiedBytes` 分开；按整文件持久化，不提供文件流内实时百分比。

交付唯一键包含 endpoint、规范源路径、单元规则、版本依据/值、稳定落地根/映射、接收方及校验等级。新 API taskId 命中相同交付时建立关联并复用，普通内部重试、通知补发和业务拒绝均不创建新一轮。不同接收方、落地目标或校验要求独立交付。范围首次完整扫描固定本次单元范围，后续只复查这些单元；空范围记录 `NO_NEW_DATA`，没有 transferId 或假验收通知。

### 用监听配置执行单次拉取

向 `POST /api/transfers/tasks` 提交以下请求。

```json
{
  "taskId": "pull-once-123",
  "direction": "PULL",
  "endpointId": "已授权的连接 ID",
  "listenerId": "监听 ID"
}
```

调用方须获同一连接和来源授权。省略 `sourcePath` 时使用监听范围。可提供更窄的相对路径，缩小扫描树并保留原单元层级和匹配规则。`sourceId` 和 `targetPath` 可省略，提供时须与监听配置一致。API 可引用已停用的监听，连接须保持启用。任务命中已登记的基线版本时，该单元跳过交付。没有其他单元时任务返回 `NO_NEW_DATA`。

任务查询返回 `triggerType`（`API`、`MANUAL`、`SCHEDULE`）、`listenerId` 和 `listenerRevision`。监听管理页按运行记录列出新建或复用的任务。文件匹配表达式用于发现单元。只要实际单元边界、规范路径、版本依据和交付范围一致，重叠的匹配规则共享同一交付。版本依据或单元边界无法安全对齐时，保存或受理返回范围冲突。

下载中源属性/集合变化会终止旧基准轮次、清零稳定观察并返回候选阶段。重新满足就绪条件后使用新 transferId；强清单版本未变化但观察基准已失效时，同样保留旧轮次历史后切换新轮次。该行为仅用于已记录的 `SOURCE_CHANGED`，不用于普通网络重试、通知补发或业务拒绝。

技术检查通过后，发布意图先落库，再优先同文件系统移动到 `pending/transferId/`；无法原子移动时校验复制结果，最后写 `.sharing-complete`。标记内容依次为 transferId、固定观察清单 SHA-256（每项一行）。**接收方必须忽略缺少有效标记的目录。** `access-base` 只允许有主机名的 HTTP(S)/SMB 映射，不接受 `C:/...`、`file://...` 充当业务访问位置；应用不实现该映射的读取服务，业务可达性须部署联调验证。

每个交付只生成一份固定 `PULL_RESULT` 通知；关联多个任务时所有任务都能查询该通知。成功内容示意：

```json
{"notificationId":"<uuid>","taskRecordId":"<首次任务 uuid>","taskId":"job-002","stage":"PULL_RESULT","result":"TECHNICALLY_PASSED","transferId":"<uuid>","sourceVersion":"<64 位摘要>","unitType":"PACKAGE_DIR","versionBasis":"MANIFEST_SHA256","versionStrength":"STRONG","totalFiles":1,"totalBytes":123,"controlFiles":2,"controlBytes":256,"validationLevel":"HASH","dataLocation":"smb://business-share/weather/pending/<transferId>/","completionMarker":".sharing-complete","files":[{"path":"nested/a.bin","size":123,"expectedHash":"<源基准>","actualHash":"<落盘摘要>"}]}
```

失败通知 `result=FAILED`，包含原因，不带有效 `dataLocation` 或 `validationLevel`，不启动业务验收。发布结果不明保持 `PUBLISHING / PUBLICATION_UNKNOWN` 并核对已保存意图；不能贸然发失败通知。通知 ACK 仍按上节回显原 notificationId 和 taskRecordId，丢 ACK 仅补原通知。

接收方使用自己的 Bearer 令牌发送 `POST /api/sharing/pull/confirmations`，请求同样限制 32 KiB、拒绝未知字段：

```json
{"transferId":"<uuid>","sourceVersion":"<通知中的完整摘要>","result":"ACCEPTED"}
```

失败回执用 `result=REJECTED`，且 `reason`（1..1000 字）与 `category`（1..100 字）必填，例如 `SOURCE_QUALITY` 或 `LOCAL_COPY_DAMAGED`；分类是接收方提供的业务说明，不自动触发重新下载。认证身份必须等于冻结的 `recipient-id`，并校验源版本、当前轮次及技术通过且已发布。通知 ACK 尚未到达也可验收。

成功响应为 HTTP 200：`{"received":true,"transferId":"...","outcome":"RECEIVED"}`。相同内容重复为 `DUPLICATE`；旧轮次为 `HISTORICAL`（只记历史）。错误版本、未发布交付或矛盾终态为 409 `CONFIRMATION_CONFLICT`，冲突记录先提交，原状态不覆盖；伪造接收方 403 `RECIPIENT_FORBIDDEN`，未知交付 404 `TRANSFER_NOT_FOUND`。同源不同版本仍可分别验收。

全部必需单元 ACCEPTED 才汇总任务 SUCCEEDED；尚有待就绪或待验收项时保持处理中，即使已有失败也在逐项结果保留；全部结束且有失败才为 FAILED。验收超时仍为 PENDING，并记录 `acceptance_reminded_at`，不自动成功、不无限重下业务坏数据。已发布内容和路径在验收后继续保留。

`PullProcessor.redeliver(transferId, repairReason)` 是受控运维服务方法，没有公开重跑端点；重新核对原源基准、原子切换当前轮次并保留历史关联，以新 transferId 交付。若源版本已变则拒绝该方法，改为正常新源版本任务。回退仅关闭 `platform.pulls.intake-enabled`，继续提供已经发出的查询、通知和回执。

## PR4 恢复、排序与会话限制

清单版本仍为 1，文件按 `(productId, relativePath)`、目录按完整相对路径使用 Java `String.compareTo`（UTF-16 字典序）；扫描、JPA 重载、源比对、清单序列化和文件分页使用同一顺序，不依赖 PostgreSQL 默认 collation。新任务另存清单原字节；历史记录重新按同一原规则生成并核对原 SHA-256，不更新基准摘要。

仅固定任务目录内、编号与当前任务匹配、有对应持久化发布意图、内容是预期控制文件完整字节或前缀的 manifest/READY 临时文件可清理重写。外来临时文件、无法确认归属的文件和额外业务文件仍阻止发布；正式成果不删除。READY 已存在时必须同时绑定任务、清单摘要、产品数并验证目标集合；结果不明继续 `RECONCILING`，不发失败完成单。

业务会话使用 endpoint 修订的 `timeoutSeconds` 作为无进展期限；上传与回读进展续期，超时关闭实际会话。探测使用原总时限。端点行锁串行化业务 claim 和探测创建，稳定 endpointId 下共享 `concurrency`；并存修订取较小限制。任务满额留库，无 worker 等名额；不同端点可并行。领取、阶段、进度、冻结和发布意图受当前 claim 约束，原结果/outbox 与 ACK/恢复事务保持一致。
