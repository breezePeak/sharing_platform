# 暂定 XML 契约：待联调

这不是客户确认的 XML 样例。正式根节点、大小写、提交方式、doAsk/doComplete 目标及 ACK 仍需用对方真实样例确认并加入回归。生产默认 `platform.transfers.provisional-xml-enabled=false`，返回 503 `XML_CONTRACT_PENDING`。仅测试或受控联调时开启。

暂定入口：`POST /api/legacy/task-notices`，`Content-Type: application/xml`，使用与 JSON 接口相同的 Bearer 身份。`type` 通过管理员配置的 `xml-routes` 唯一解析 endpoint，不在 XML 中增加 endpointId。

```xml
<taskNotice>
  <taskId>job-001</taskId><type>weather</type>
  <sourceRoot>configured-local-source-ref</sourceRoot><targetPath>batch/001</targetPath>
  <fileCount>1</fileCount>
  <ProductList><product><productId>product-1</productId><productPath>products/product-1</productPath><productName>示例产品</productName></product></ProductList>
</taskNotice>
```

`product` 是产品包，`fileCount` 必须等于产品节点数，不能当作物理文件数。`doAsk` 的 `result=0/1` 分别表示预检查通过/拒绝，拒绝字段保留原拼写 `rejectrason`。`doComplete` 的 `status=0/1/2` 分别是成功/失败/作废删除；当前处理器仅产生 0 或 1。完成单保留原产品全集，且只输出一份 `ProductList`。暂定 ACK：

```xml
<ack><accepted>true</accepted><notificationId>原通知编号</notificationId><taskRecordId>原内部任务号</taskRecordId></ack>
```

禁用 DTD、外部实体、XInclude，并限制报文 32 KiB。XML 与 JSON 等价请求走相同业务规范化逻辑；同 callerId、taskId 且语义不同则 409。
