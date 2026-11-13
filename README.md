# 数据共享平台

工程位于 `sharing_platform/`。按本轮用户更新，PR-01 使用 **JDK 11 / Spring Boot 2.7.18、Spring Data JPA、直接连接 PostgreSQL、页面无需登录**，不引入 Spring Security，提供服务端页面、迁移及健康检查。FTP/SFTP 连接配置、推送、拉取、监听和业务任务尚未开放，页面显示真实空状态。

## 环境与数据库准备

需要完整 JDK 11，设置 `JAVA_HOME` 并确认 `java -version`、`javac -version` 均为 11。Maven Wrapper 固定 Maven 3.9.11，首次运行需要访问 Maven Central；不需要 Node/npm。Boot 2.7.18 是适配 JDK 11 的实施选择，已结束开源维护；任务书原 Java 21 / Boot 4 默认值被用户新要求替代。

应用直接连接 PostgreSQL，不使用 Docker。按当前本机部署条件固定 PostgreSQL **13.23**，CI 也直接启动同版本原生服务。**13 已结束官方支持；此版本用于本次本地兼容验收，正式投产前须升级到受支持版本并同步 CI 与兼容性验收。** 数据库类型来自用户 F17，版本是本轮实施决定，详见 [实施决策](doc/implementation-decisions.md)。

先启动 PostgreSQL，确保可从应用主机连接；使用 DBA 账户新建两个独立数据库，不在默认 `postgres` 库或已有业务库中运行测试：

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
.\mvnw.cmd clean verify
java -jar target/sharing_platform-1.0-SNAPSHOT.jar --spring.profiles.active=local
```

Unix 使用 `sh ./mvnw clean verify`；环境变量含义相同。`package` 只执行单元/页面测试，完整验收必须使用 `verify`，其后运行真实数据库集成测试。数据库不可达、迁移冲突或缺少配置时启动失败，不回退到其他数据库。

| 配置 | 用途 |
| --- | --- |
| `local` | 本地开发；仅监听 127.0.0.1，模板不缓存 |
| `test` | 必须使用 `TEST_DB_*`；不能借用开发配置 |
| `prod` | 外置 `DB_*`，启用 Secure 会话 Cookie；通过 HTTPS 访问 |

默认端口 8080，可用 `SERVER_PORT` 调整。直接打开 [当前服务](http://127.0.0.1:8080/services)、`/config`、`/tasks`，无需登录，没有登录页或管理员账户。不依赖 Spring Security 或其测试包。当前只有只读页面，没有配置提交表单或业务 API：`POST /config` 返回 405，未实现的 `/api/**` 返回 404。未来开放写入表单和外部 API 时，在对应 PR 实施请求防伪和调用方校验。健康接口只公开 UP/DOWN，不公开配置、环境、组件、凭据和异常堆栈。页面无需身份验证，部署时应限制网络访问范围。

前台运行按 `Ctrl+C` 停止。PR-01 不创建文件传输数据目录；持久数据存于 PostgreSQL 自身数据目录（由 DBA 管理），不写进项目。

## 测试、迁移与回退

纯单元/页面测试可执行 `./mvnw test`，不需要数据库。`./mvnw clean verify` 必须设置 `TEST_DB_URL`、`TEST_DB_USERNAME`、`TEST_DB_PASSWORD`；URL 数据库名须以 `_test` 结尾，禁止 URL 内嵌凭据、查询参数和片段，拒绝与 `DB_URL` 相同的连接。每次测试创建随机 `foundation_it_*` schema，只删除本次随机 schema；不清空测试库。测试覆盖空库迁移、仓储读写、重复迁移、应用重启、测试专用 V2 升级、迁移校验失败及访问控制。

首次启动由 Flyway 在应用 schema 建立迁移历史与 `platform_metadata`，通过 `PlatformMetadata` 实体和 `JpaRepository` 读写。Hibernate 使用 `ddl-auto=validate` 校验映射，关闭 Open Session in View；不自动创建或修改表。V1 只保存平台元数据，不预建业务表。后续升级新增 Flyway 迁移，不改写已执行 SQL。JPA 改造兼容已有 V1 表，不需要删表或重新建库。测试用 V2 不进入应用 JAR。失败迁移直接阻止启动，不执行 Flyway clean。

CI 位于 [.github/workflows/ci.yml](.github/workflows/ci.yml)，使用原生隔离 PostgreSQL 13.23 执行同一 `clean verify`，没有 Docker 或客户凭据。迁移/数据备份由 DBA 负责；回退代码保留数据库与历史，PR-01 的 V1 为新增基础表，回到旧骨架无需删除它，但旧版本不具备这些页面和安全配置。发生破坏性升级时不能只回退 JAR，须先评估对应迁移。

PR-01 的实际命令与验收结果见 [实施记录](doc/pr/PR-01_实施记录.md)。未授权提交或创建远端 PR；当前修改留在工作区。

## 需求与开发计划

从 [开发 PR 总览](doc/pr/00_开发PR总览.md) 开始，再读 [公共实现约定](doc/pr/01_公共实现约定.md)。
首个任务使用 [PR-01 执行提示词](doc/pr/99_PR-01执行提示词.md)。

数据库选型已按用户新增指示记录为 PostgreSQL。开发实施约定调整为 local、数据库集成测试/CI、prod 统一 PostgreSQL，不保留 H2 运行/替代测试方案；具体数据库版本未由用户指定。

`doc/` 中事实需求依据、边界约定为 V1.2，推送和拉取流程为 V1.1；保留 SFTP、多监听、统一入口、回执与 F/I/D/Q 分类，不覆盖已有用户修订。
`doc/pr/` 中 8 份 PR 按编号逐个执行；实施情况以对应实施记录为准，不把计划编号视为 GitHub PR 编号。

代码仓库：`breezePeak/sharing_platform`；现有 Maven 工程：`sharing_platform/`。

本次生成日期：2026-11-13。
