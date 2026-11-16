# PR-03 任务接口契约

本页是新 JSON 接口的实施契约。推送、拉取的真实处理器由 PR-04/05 接入；当前生产配置没有处理器，新任务返回 `503 {"code":"ENGINE_NOT_READY"}`，数据库不创建任务。测试处理器仅在测试类中装配。

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

## 创建与重放

`POST /api/transfers/tasks`，`Content-Type: application/json`。请求上限 32 KiB，未知字段拒绝。`taskId` 是调用方范围内的外部业务号，`taskRecordId` 是平台内部号。根目录引用和 endpoint 均由服务端预配置；路径必须相对于受控根目录。

推送示例：

```json
{"taskId":"job-001","direction":"PUSH","endpointId":"<uuid>","sourceCode":"weather","sourceRoot":"configured-local-source-ref","targetPath":"batch/001","products":[{"productId":"product-1","productPath":"products/product-1","productName":"示例产品"}]}
```

拉取示例：

```json
{"taskId":"job-002","direction":"PULL","endpointId":"<uuid>","sourcePath":"incoming/batch-2","targetPath":"configured-local-target-ref"}
```

新受理返回 HTTP 202：

```json
{"taskRecordId":"<uuid>","taskId":"job-001","accepted":true,"reused":false,"executionStatus":"QUEUED"}
```

同一调用方和任务号、相同规范化业务内容返回 HTTP 200，`reused=true`，保留原配置修订和任务号。方向、endpoint、来源代码、路径或产品范围变化返回 409 `TASK_ID_CONFLICT`。产品顺序和 JSON 属性顺序不影响指纹；产品显示名参与比较。XML `type` 对应 JSON `sourceCode`，跨格式重放时需提供相同来源代码。配置编辑后重放原请求仍按原快照比较。受理事务失败返回 500，不返回 `accepted=true`。

错误体为 `{"code":"..."}`。常见状态：400 `INVALID_TASK` / `ENDPOINT_DISABLED`，401 `AUTH_REQUIRED`，403 `ENDPOINT_FORBIDDEN`，404 `TASK_NOT_FOUND`，409 `TASK_ID_CONFLICT`，413 `REQUEST_TOO_LARGE`（请求体超过 32 KiB），503 `ENGINE_NOT_READY` / `NOTIFICATION_NOT_CONFIGURED`。

## 查询与文件分页

`GET /api/transfers/tasks/{taskRecordId}` 只返回当前调用方的任务、产品、执行状态、阶段、验收状态和各通知摘要。`GET /api/transfers/tasks/{taskRecordId}/files?page=0&size=50` 限 `size=1..100`。PR-03 尚未冻结文件清单，返回 `manifestFrozen=false`、`totalFilesKnown=false` 和空 `files`；这表示数量未知，不代表业务文件数为零。物理文件明细由 PR-04/05 接入。

## 通知与 ACK

测试处理器产生阶段结果时，结果和通知在同一事务保存。通知按配置 URL 发送 JSON，例如：

```json
{"notificationId":"<uuid>","taskRecordId":"<uuid>","taskId":"job-001","stage":"COMPLETE","result":"SUCCEEDED"}
```

接收端必须返回 HTTP 2xx 和 JSON `{"accepted":true,"notificationId":"<原编号>","taskRecordId":"<原内部任务号>"}`。仅 2xx 不算 ACK。响应丢失或关联不符按同一编号与原报文重发；默认最多 5 次，之后保留 `NEEDS_ATTENTION`。通知补发不重新调用传输处理器。`PRECHECK` 与 `COMPLETE` 为不同阶段，每阶段只允许一个有效结果。
失败结果包含可用的 `errorType` 和 `errorDetail`，不会把底层异常原文或凭据自动写入通知。

## 运行边界

任务到期后由固定大小线程池领取；线程池满时记录留在数据库。进程重启后，失去心跳超过 5 分钟的领取可恢复，旧领取身份不能覆盖新结果。任务和通知记录不因关闭入口而删除。当前实现不提供手动重置 `NEEDS_ATTENTION` 的操作；运维恢复入口随后续监控阶段补齐。
