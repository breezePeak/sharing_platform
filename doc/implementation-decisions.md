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
