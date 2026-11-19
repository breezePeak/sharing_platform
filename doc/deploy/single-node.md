# 单机部署和恢复

## 构建与运行

运行时使用 Java 11。隔离验收环境使用 PostgreSQL 13.23、PostgreSQL JDBC 42.7.14、Flyway 8.5.13 和 Spring Boot 2.7.18。正式部署前确认受支持的运行时及数据库版本，并在该环境重新执行迁移与恢复验证。

在 `sharing_platform` 下构建 Spring Boot JAR。

```powershell
.\mvnw.cmd -B -DskipTests package
Get-FileHash -Algorithm SHA256 .\target\sharing_platform-1.0-SNAPSHOT.jar
```

复制配置模板到服务账户可读的受限路径。环境中的数据库密码、控制台凭据、业务令牌及 32 字节 Base64 主密钥由部署方注入，明文不进配置模板。连接密码保存在数据库加密修订中。私钥目录仅授予服务账户所需的读取权限。先创建连接并登记可信 SFTP 主机指纹，再把连接 ID 和实际目录写入外置来源及路由并重启加载。模板见 [外置配置示例](application-release.example.yml)。

```powershell
java -jar .\sharing_platform-1.0-SNAPSHOT.jar --spring.profiles.active=prod --spring.config.additional-location=file:D:/sharing/config/application-release.yml
```

默认只监听 `127.0.0.1:8080`。HTTPS 反向代理按来源限制管理路径 `/config`、`/tasks`、`/services`，对业务调用方只开放所需的 `/api` 路径。应用将日志写至标准输出和标准错误，由 Windows 服务管理器或进程监管器收集。启动后核对 `/actuator/health`、Flyway 当前版本、探测状态和待核对任务。同一 PostgreSQL 数据库 schema 只启动一个实例，启动锁需要连接池至少两个连接。正常停止使用服务停止命令或前台 Ctrl+C，等待应用退出后再启动替换实例。

FTP 普通协议使用受控内网和隔离账号，按对端设置开放控制与数据端口，优先被动模式。SFTP 仅登记经可信渠道取得的主机指纹。限制出站为批准的 FTP/SFTP 服务、PostgreSQL 和回调地址，限制回执入站来源。业务平台须验证 `dataLocation` 对接收账户可读，并按约定识别待验收目录的实际发布标记。

## 一致备份与升级

1. 在入口停止新任务和配置变更，暂停监听周期。等待可收敛的处理完成，记录仍在运行、待核对、待送达和待验收的 ID。然后正常停止应用。
2. 同一停机窗口备份逻辑数据库、外置配置、服务账户可访问的本地源、`tmp`、`pending` 及已发布数据。保留远端发布目录和标记清单。凭据修订需与备份独立存放的主密钥及外置私钥版本配套。用文件摘要和归档清单检查备份完整性。
3. 上新 JAR 前验证测试库增量迁移。V1 到 V9 由 Flyway 管理，启动只做增量迁移，失败时应用停止。先在隔离恢复库核对，再开启实际传输和通知。公共接口的任务、交付和通知 ID 会跨重启保留。

数据库归档可使用 `pg_dump -Fc -f sharing.dump <database>`。恢复演练使用全新的隔离数据库执行 `pg_restore --exit-on-error --no-owner -d <restore_database> sharing.dump`。命令用受限密码文件或进程凭据取得认证。演练库、协议地址和落地目录均与客户环境分开。

## 回退与发布核对

- 恢复备份时使用匹配的 JAR、数据库、任务数据目录、主密钥和配置。校验恢复版本及读写权限。若数据库快照早于外部成果，先用 `SHARING_DISPATCH_ENABLED=false` 启动只用于查看的恢复实例，同时关闭 PUSH/PULL 受理和对外入口。
- 核对推送的实际 `_READY`、清单和内容摘要，核对拉取目录的 `.sharing-complete`、冻结清单、台账 ID 和业务接收方记录。通知已确认的交付继续保持其原状态。证据不足时保留待核对并由业务方确认外部结果，确认之前不恢复写入。
- 正常启动会让中断领取立即可恢复。发布状态无法确认时进入有界退避，自动核对最多八次，超过上限后由控制台发起只读核对。停机保留临时目录、待验收目录、已发布成果和所有任务/通知状态。归属不明、待确认或已经交付的数据禁止自动清理。
- 若旧 JAR 与新迁移不兼容，按演练过的匹配快照恢复。回退应用前先核对外部成果，避免重复交付。不要逆向执行删除式迁移。

测试、放行条件和外部待确认项见 [PR8 实施记录](../pr/PR-08_实施记录.md)。
