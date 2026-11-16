# 数据共享平台

工程位于 `sharing_platform/`。使用 **JDK 11 / Spring Boot 2.7.18、Spring Data JPA、直接连接 PostgreSQL、页面无需登录**，不引入 Spring Security。PR2 新增 FTP/SFTP 连接配置、版本化加密凭据和异步真实能力检测；PR3 新增任务接入骨架与通知队列；PR4 接入真实 PUSH；PR5 接入单次 PULL、版本去重和业务验收。周期监听尚未开放。

## 环境与数据库准备

需要完整 JDK 11，设置 `JAVA_HOME` 并确认 `java -version`、`javac -version` 均为 11。Maven Wrapper 固定 Maven 3.9.11，首次运行需要访问 Maven Central；不需要 Node/npm。Boot 2.7.18 是适配 JDK 11 的实施选择，已结束开源维护；任务书原 Java 21 / Boot 4 默认值被用户新要求替代。

应用直接连接 PostgreSQL，不使用 Docker。按当前本机部署条件固定 PostgreSQL **13.23**，CI 也直接启动同版本原生服务。**13 已结束官方支持；此版本用于本次本地兼容验收，正式投产前须升级到受支持版本并同步 CI 与兼容性验收。** 数据库类型来自用户 F17，版本是本轮实施决定，详见 [实施决策](doc/implementation-decisions.md)。

先启动 PostgreSQL，确保可从应用主机连接；默认建议使用 DBA 账户新建两个独立数据库。按本次用户要求，也可显式选择在本机应用库的随机测试 schema 中运行验收：

```text
createdb -h 127.0.0.1 -p 5432 -U postgres sharing_platform
createdb -h 127.0.0.1 -p 5432 -U postgres sharing_platform_test
```

若库已存在，先核对用途，不删除重建。生产由 DBA 创建独立应用账户，授予应用 schema 的迁移及读写权限，不能沿用本机超级用户。

## 配置与启动

数据库参数必填，无默认口令，也无需配置管理员账户。连接 URL 只包含主机、端口、库名及必要的 SSL 参数；用户名、密码单独设置，不能放进 URL。可通过环境变量或受限的外置 Spring 配置文件提供，不能提交到仓库。

PowerShell 7 示例（先将 JDK 11 配置为 `JAVA_HOME`）：

```powershell
cd sharing_platform
$env:DB_URL = 'jdbc:postgresql://127.0.0.1:5432/sharing_platform'
$env:DB_USERNAME = 'postgres'
$env:DB_PASSWORD = Read-Host '数据库密码' -MaskInput
$env:TEST_DB_URL = 'jdbc:postgresql://127.0.0.1:5432/sharing_platform_test'
$env:TEST_DB_USERNAME = 'postgres'
$env:TEST_DB_PASSWORD = Read-Host '独立测试库密码' -MaskInput
.\mvnw.cmd -B clean verify
java -jar target/sharing_platform-1.0-SNAPSHOT.jar --spring.profiles.active=local
```

本机开发可在仓库根目录执行 `. ./.env.local.ps1` 载入用户提供的数据库参数；该文件被 Git 忽略，不用于生产部署。

Unix 使用 `./mvnw -B clean verify`；环境变量含义相同。`package` 只执行单元/页面测试，完整验收必须使用 `verify`，其后运行真实数据库集成测试。数据库不可达、迁移冲突或缺少配置时启动失败，不回退到其他数据库。

| 配置 | 用途 |
| --- | --- |
| `local` | 本地开发；仅监听 127.0.0.1，模板不缓存 |
| `test` | 必须使用 `TEST_DB_*`；不能借用开发配置 |
| `prod` | 外置 `DB_*`，启用 Secure 会话 Cookie；通过 HTTPS 访问 |

默认端口 8080，可用 `SERVER_PORT` 调整。直接打开 [当前服务](http://127.0.0.1:8080/services)、[连接配置](http://127.0.0.1:8080/config/endpoints)、`/tasks`，无需登录。`/config` 转向连接列表。连接表单使用会话防伪令牌、字段白名单、参数校验和输出转义；任务 API 的鉴权、请求和响应见 [PR3 接口契约](doc/contracts/transfer-api.md)。健康接口只公开 UP/DOWN，不公开配置、凭据和异常堆栈。页面无需身份验证，部署时应限制网络访问范围。

前台运行按 `Ctrl+C` 停止。PR-01 不创建文件传输数据目录；持久数据存于 PostgreSQL 自身数据目录（由 DBA 管理），不写进项目。

## PR2 连接配置与探测

首次保存密码 / 私钥口令前，设置 `ENDPOINT_MASTER_KEY`：由安全随机数生成的 **32 字节 Base64** 密钥。无默认密钥，不自动生成并丢弃；缺失或错误时不能保存 / 解密秘密。密钥保存在受限的外置配置或环境中，与数据库分开备份。不要在命令行、仓库或聊天中填写真实密钥。更换密钥需先安排已有凭据的迁移，直接更换会使旧版本无法解密。

连接可以新增、编辑、启停；秘密字段留空保留原值，不回显。独立清除操作要求输入 `CLEAR`，创建无当前凭据的新版本，不删除历史版本或外置私钥文件。每次变更保留配置 / 凭据快照，历史任务以后应引用 `endpoint_revision.id`，不能执行时重读当前配置。过期检测结果明确标记。配置操作另记连接、版本、动作和时间，没有硬删除入口。

FTP 默认二进制、被动模式，可选择主动模式。必须确认管理员已隔离账号根目录；FTP 无法可靠证明所有链接属性，页面显示 `LIMITED_FTP_ISOLATED_ACCOUNT`。不支持可靠属性查询的服务器会报告路径类型无法验证，不能伪造通过。SFTP 必填从可信渠道取得的 `SHA256:…` 主机公钥指纹，拒绝未知 / 错误指纹，不继承本机 SSH 配置、代理或默认私钥。

SFTP 私钥仅填写文件名。管理员通过 `ENDPOINT_KEY_DIRECTORY` 指定受限绝对目录，使用操作系统权限限制应用读取；禁止链接和任意系统文件路径。保存时固定私钥文件摘要，连接前发现文件被替换则拒绝，须保存新配置版本。已验证 JDK 11 的 RSA PKCS#8 PEM 私钥；其他格式或算法需按真实服务器验证，不自动增加加密提供者。需要本地源 / 目标目录时，在受限外置 YAML 配置中定义引用，页面只填写引用名：

```yaml
platform:
  endpoints:
    local-roots:
      source: D:/sharing/source
      delivery: D:/sharing/delivery
```

目录须已存在且没有链接。远端读取测试对象是读取根目录下的非空普通文件；未指定时不声称读取已验证。写探测子目录必须明确指定且已存在；留空不会写入。平台只在其中新建随机独占 `.sharing-probe-<随机编号>` 目录，写入 4096 字节随机内容，比较大小并完整回读 SHA-256，验证改名后再非递归清理。无需服务端 shell 或远端哈希命令。只读可单独用于拉取；只有写入、完整性、改名及清理全部满足时才显示严格推送条件通过。SFTP 按服务器实际提供的 statvfs / space-available 扩展记录可用字节 `BYTES:…`；无扩展或查询失败显示 `UNKNOWN`，FTP 无标准容量接口时也显示 `UNKNOWN`，不推断“充足”。

检测从 POST 提交后异步执行，刷新列表查看阶段、时间、版本和残留路径。每连接遵守配置并发上限；全局最多 2 个检测、不排无限队列，忙时返回 429。单阶段超时 1—30 秒，总时限为其 6 倍；总超时后的迟到结果不能覆盖终态。服务重启将未完成检测标记为 `INTERRUPTED`，保留可能残留路径供人工核对，不自动删除归属不明的远端内容。检测不会代替后续任务的预检查。

FTP 总超时会关闭实际的数据 socket（包括读、写、列举及主动模式监听）和控制连接，持续有数据也不能绕过总时限；适配器不关闭调用方传入的流。`canRead` / 页面“读源检测”仅表示 `readRoot/readTestFile` 通过读取检测；写入探测文件回读成功只设置目标完整性结果。读源失败或未检测时可仍满足严格推送条件，但不能显示读取成功。

## PR4 推送

PUSH 使用现有 Bearer 调用方、endpoint 授权和通知配置。连接须设置可写远端根目录、已存在的探测目录及本地源根目录引用。调用方先提交产品目录；预检固定每个文件的大小和 SHA-256，实际探测目标读写能力，通知接收端确认 PRECHECK 后才传输业务文件。目标业务文件完整回读核验，`_manifest.xml` 验证后才写入并核对 `_READY`。完成通知保留全部原产品和相对交付路径。接口字段与示例见 [任务契约](doc/contracts/transfer-api.md)。

默认拒绝零字节文件以及 `.tmp`、`.part`、`.crdownload` 文件；可在外置配置中使用 `platform.transfers.allow-zero-byte-files` 和 `platform.transfers.temporary-suffixes` 调整。生产方下发任务须代表其固定版本已就绪；无上游预期清单时，平台只能证明所观察集合固定，无法证明上游未漏生成文件。接收方按有效 `_READY` 和匹配的清单消费，需在联调中确认。关闭新 PUSH 受理可设置 `platform.transfers.push-intake-enabled=false`，处理器继续完成在途任务及通知，已发布数据保留；旧版本不能识别新状态时不可直接降级执行器。

## PR5 单次拉取

PULL 使用统一任务入口，必须引用管理员配置的稳定 `sourceId`，显式选择 FILE 或 PACKAGE_DIR、扫描层级、BASIC/HASH、就绪规则、交付根和业务访问映射。来源配置及 HASH 清单格式见 [任务接口契约](doc/contracts/transfer-api.md#pull-来源配置与源清单)。一次调用只处理本次范围，不创建永久监听；缺少就绪条件时由原任务的 `nextRunAt` 重查。

数据在 `tmp/transferId/` 下载、校验后发布到 `pending/transferId/`；接收端只消费含有效 `.sharing-complete` 的目录。通知给出配置映射下的 `dataLocation`，需由部署方配置共享挂载或读取服务并验证业务平台确实可访问；应用不会自动暴露本机目录。下载完成后保持待验收，认证接收方调用 `POST /api/sharing/pull/confirmations` 后才决定最终结果；验收先于通知 ACK 也有效。原数据与已发布目录默认保留。

新增生产迁移 `V5__pull_deliveries.sql`；已执行 V1—V4 不改写。工作区并行的 PUSH 修复使用 V6，升级测试专用迁移顺延至 V7，不进入应用 JAR。关闭新拉取设置 `platform.pulls.intake-enabled=false`，保留处理器、查询、通知及回执；不要用 `platform.pulls.enabled=false` 回退在途交付。实现与验收证据见 [PR5 实施记录](doc/pr/PR-05_实施记录.md)。

PR4 审查修复后，连接的 `timeoutSeconds` 同时作为业务网络 I/O 无进展期限（1—30 秒）：上传/回读有字节进展便续期，持续传输不受固定总时长限制；过期直接关闭底层会话，进入原文件的有限重试。探测保留原总时限。`concurrency` 由稳定 endpointId 统一约束业务执行与探测，跨配置修订共享名额；并存修订采用较小限制。在途任务使用原配置，停用不切换地址或凭据；满额任务留库排队，不占 worker。不同端点可利用空闲 worker。

查询新增 `uploadTrafficBytes`、`readbackBytes`、`retransmittedBytes` 和 `routeVersion`；上传业务完成量按文件高水位计数，已验证量独立记录，网络流量含重试及重复核验。流内每 8 MiB 或 1 秒节流落库，文件结束/异常刷新。`SUCCEEDED` 表示技术发布已确认；完成通知未 ACK 时阶段 `NOTIFYING`，ACK 后为 `COMPLETE`，不因补通知重传文件。版本化 `callerId/type → endpoint` 路由沿用外置 YAML，见[接口契约](doc/contracts/transfer-api.md)。

V6 新增精确清单字节、manifest 发布意图、路由快照和流量列。保留当前工作区既有 V5（PULL）及 V1—V4 原迁移，不清空业务表。历史 V4 清单按 Java UTF-16 字符串顺序恢复并核对原摘要；不修改数据库 collation、不重新冻结源内容。已确认归属的 manifest/READY 临时文件在严格集合检查前核对、清理并重写，其他任务或内容不符的临时文件仍阻止发布。发布不明继续 `RECONCILING`，不提前发失败完成单。详细改动与本轮真实测试见[审查修复记录](doc/pr/PR-04_审查修复记录.md)。

## 测试、迁移与回退

纯单元/页面及真实 FTP/SFTP 适配器测试可执行 `./mvnw test`，不需要数据库。`./mvnw -B clean verify` 必须设置 `TEST_DB_URL`、`TEST_DB_USERNAME`、`TEST_DB_PASSWORD`；默认 URL 数据库名须以 `_test` 结尾，禁止 URL 内嵌凭据、查询参数和片段，拒绝与 `DB_URL` 相同的连接。经明确授权在本机应用库验收时，额外设置 `TEST_DB_ALLOW_LOCAL=true`，才允许回环地址上与 `DB_URL` 相同的非系统数据库。每次测试创建随机 `foundation_it_*` / `endpoint_it_*` / `task_it_*` / `push_it_*` schema，只删除本次随机 schema，不清空数据库或修改现有业务 schema。真实协议服务仅监听回环地址，使用临时目录和测试凭据。测试覆盖空库 V1→V6、已含固定推送记录的 V4→V6 增量迁移、JPA、重启保留、测试专用 V7 升级、页面防伪、双协议能力、任务幂等、领取、通知补发及故障回滚等。

首次启动由 Flyway 在应用 schema 建立迁移历史与 `platform_metadata`，通过 `PlatformMetadata` 实体和 `JpaRepository` 读写。Hibernate 使用 `ddl-auto=validate` 校验映射，关闭 Open Session in View；不自动创建或修改表。V1 只保存平台元数据，不预建业务表。后续升级新增 Flyway 迁移，不改写已执行 SQL。JPA 改造兼容已有 V1 表，不需要删表或重新建库。测试专用迁移不进入应用 JAR。失败迁移直接阻止启动，不执行 Flyway clean。

PR2 的生产 V2 新增 `endpoint`、`endpoint_revision`、`endpoint_probe`、`endpoint_operation` 及索引 / 外键；V1 未改写。原测试专用 V2 改名为 V3，不进入应用 JAR。回退 JAR 时保留全部新增数据及 Flyway 历史；旧版不会提供连接配置。Flyway 可能因旧代码找不到已应用 V2 而拒绝启动，应保留 V2 迁移资源发布回退包，不通过删表 / 删历史绕过校验。

CI 位于 [.github/workflows/ci.yml](.github/workflows/ci.yml)，使用原生隔离 PostgreSQL 13.23 执行同一 `clean verify`，没有 Docker 或客户凭据。迁移/数据备份由 DBA 负责；回退代码保留数据库与历史，PR-01 的 V1 为新增基础表，回到旧骨架无需删除它，但旧版本不具备这些页面和安全配置。发生破坏性升级时不能只回退 JAR，须先评估对应迁移。

PR-01 的实际命令与验收结果见 [实施记录](doc/pr/PR-01_实施记录.md)。未授权提交或创建远端 PR；当前修改留在工作区。

PR2 的实际测试与限制见 [PR2 实施记录](doc/pr/PR-02_实施记录.md)。

## 需求与开发计划

从 [开发 PR 总览](doc/pr/00_开发PR总览.md) 开始，再读 [公共实现约定](doc/pr/01_公共实现约定.md)。
首个任务使用 [PR-01 执行提示词](doc/pr/99_PR-01执行提示词.md)。

数据库选型已按用户新增指示记录为 PostgreSQL。开发实施约定调整为 local、数据库集成测试/CI、prod 统一 PostgreSQL，不保留 H2 运行/替代测试方案；具体数据库版本未由用户指定。

`doc/` 中事实需求依据、边界约定为 V1.2，推送和拉取流程为 V1.1；保留 SFTP、多监听、统一入口、回执与 F/I/D/Q 分类，不覆盖已有用户修订。
`doc/pr/` 中 8 份 PR 按编号逐个执行；实施情况以对应实施记录为准，不把计划编号视为 GitHub PR 编号。

代码仓库：`breezePeak/sharing_platform`；现有 Maven 工程：`sharing_platform/`。

本次生成日期：2026-11-13。
