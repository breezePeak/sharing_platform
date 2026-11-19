# PR-01 实施决策

日期：2026-11-13。数据库类型 PostgreSQL 来自用户 S6/F17；S7 明确 JDK 11、无需登录、直接连接 PostgreSQL 且不用 Docker；S8 明确使用 JPA，不使用 Spring Security 并更新 PR 任务书。其余技术选择为 P 类，不增补用户事实。任务书正文已同步这些用户更新。

| 项目 | 决定及核对依据 |
| --- | --- |
| 工程 | 复用 `sharing_platform/` 和 `com.siwei.App`，一个 JAR 同时提供 MVC 页面与后端，不创建第二工程或前端构建 |
| Java / Boot | 用户指定 JDK 11；Boot 4 要求 Java 17 以上，改为正式版 2.7.18。该版本已结束开源维护，属于兼容现有 JDK 的取舍；现场使用真实 Java 11 编译、测试及运行 |
| Starter | Boot 2.7 的 `web`、`thymeleaf`、`validation`、`data-jpa`、`actuator`、test；不引入 Spring Security 或其测试包，不使用 JdbcTemplate 作为持久化实现 |
| JDBC / Flyway / JUnit | JDBC 显式固定 42.7.14，避免回退到旧驱动；Flyway 8.5.13、JUnit Jupiter 5.8.2 由 Boot 2.7.18 BOM 管理。此 Flyway 版本的 PostgreSQL 支持内置于 flyway-core，不添加只适用于新版的独立 PostgreSQL 模块；通过真实库实测验证 |
| PostgreSQL | 用户要求直接连接本机，现场服务为 13.23。因此 local、数据库集成测试与 CI 固定主版本 13，CI 精确下载 13.23 源码并直接启动原生服务，无 Docker。13 已停止官方支持，正式投产前须选择受支持版本并同步本项、CI、版本断言及真实兼容性验证，不擅自升级现有服务 |
| 测试隔离 | 独立 `_test` 数据库、显式 `TEST_DB_*`、随机 schema；先校验后建表/清理，只清理本次随机 schema，拒绝缺参数和隐式回退 |
| 最小持久化 | Spring Data JPA 实体和 JpaRepository；V1 仅元数据表及 Flyway 历史；Hibernate ddl-auto=validate、open-in-view=false，表结构由 Flyway 管理；复用已有 V1 不改写 SQL；升级验证使用测试专用 V2 |
| 页面和 API 边界 | 按用户要求不引入 Spring Security，页面直接访问，无管理员参数。PR-01 没有写入表单或 API；配置 POST 返回 405，未实现 API 返回 404。真实写入/API 的请求防伪和调用方校验由对应 PR 实现，不在基础 PR 自建认证框架 |
| 错误和日志 | 响应不含堆栈/内部异常信息，统一中文错误页、参数错误信息、服务端生成 UUID traceId。请求日志不记录查询/表单/头/报文，异常日志仅类型；控制台对常见 credential 键脱敏且不自动输出堆栈 |
| 部署 | local 仅回环地址，prod 通过 HTTPS 并使用 Secure Cookie；数据库必需参数缺失、连接或迁移失败直接失败，不内置数据库；无登录的页面应通过网络边界控制可达范围 |
| 工具链 | 官方 Maven Wrapper 3.3.4 only-script + Maven 3.9.11，可执行 JAR；GitHub Actions 原生 PostgreSQL 执行完整 verify |

核对来源：[Boot 2.7.18 系统要求](https://docs.spring.io/spring-boot/docs/2.7.18/reference/html/getting-started.html)、[2.7.18 BOM](https://repo.maven.apache.org/maven2/org/springframework/boot/spring-boot-dependencies/2.7.18/spring-boot-dependencies-2.7.18.pom)、[Boot 2.7 Flyway 版本](https://github.com/spring-projects/spring-boot/wiki/Spring-Boot-2.7-Release-Notes)、[Maven Wrapper](https://maven.apache.org/tools/wrapper/)、[pgJDBC 连接](https://jdbc.postgresql.org/documentation/use/)、[PostgreSQL 支持周期](https://www.postgresql.org/support/versioning/)。现场命令及结果另记于 [PR-01 实施记录](pr/PR-01_实施记录.md)。

远端 HEAD 只读核对为 `5b1213e805daedc3e25221b31beb6cbea89cdc17`，与本地起点一致。现有用户文档改动保留，四份基线版本为 V1.2/V1.1/V1.1/V1.2；SFTP、多监听、统一入口和回执未被回退。未实施 PR-02 及后续业务。

JPA 实体及仓储遵循 [Spring Data JPA 2.7.18 官方文档](https://docs.spring.io/spring-data/jpa/docs/2.7.18/reference/html/)，不保留自写 JDBC 仓储或 Spring Security 包装层。


## 2026-11-16 PR4 审查修复补充

- PushProcessor 只编排流程，文件/目录使用复合键实体和 JpaRepository，PushStore 在短事务中锁任务并校验执行 claim；TransferJpa 保留必要原子原生语句，在更新前 flush、更新后 clear 防止上下文过期。生产代码不使用 JdbcTemplate。
- 稳定 endpointId 的数据库行锁协调任务领取与探测创建；RUNNING 记录占名额，满额不领取、不占线程；并存配置修订采用请求和已有执行的最小并发限制。成功/失败前先关闭会话，再释放状态；探测超时也先关闭，取消结果不会覆盖终态。
- 业务复用同一 FTP/SFTP 适配器和关闭路径，以修订中的 timeoutSeconds 限制网络无进展，连续传输可以长期运行；探测保留总时限。配置快照不增加一套超时配置。
- 清单 v1 按 Java UTF-16 字典序排序，新记录保存精确清单文本；旧记录核对原摘要，不改数据库默认 collation、不重冻基准。V6 只新增字段和保守迁移 manifest 意图；V1—V4 不改写，当前工作区既有 V5 保留。
- 仍使用外置 Spring YAML 提供 caller/type 完整路由，保留 xml-routes 与旧 JSON 兼容入口；受理时持久化路由版本、源/目标/通知/端点修订，重放优先比较旧快照。
- wrapper Git 模式为 100755，Unix/CI 使用 `./mvnw -B clean verify`，Windows 使用 `.\mvnw.cmd -B clean verify`。本轮实测与边界见 [PR4 审查修复记录](pr/PR-04_审查修复记录.md)。

## 2026-11-19 发布验证

- 控制台沿用同应用页面，生产运行使用外置 Basic 凭据和受限 HTTPS 入口，管理操作同时校验 CSRF。业务 API 保持独立的调用方授权。该部署要求基于 PR7 的运维操作权限，不代表客户已确认最终身份认证方案。
- V9 记录发布核对次数。单实例启动通过 schema 级 PostgreSQL 锁判断所有权，随后让旧领取重新可用，由原状态机继续处理固定快照、清单、通知和回执。
- 客户报文、真实地址、网络及消费责任仍需联调，`doc/contracts/confirmed/` 不加入模拟样例。逐项外部状态见 [PR8 实施记录](pr/PR-08_实施记录.md)。
