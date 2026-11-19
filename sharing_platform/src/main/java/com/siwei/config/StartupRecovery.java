package com.siwei.config;

import com.siwei.endpoint.ProbeStore;
import com.siwei.transfer.PullStore;
import java.sql.Connection;
import java.sql.SQLException;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.sql.DataSource;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

@Component
public class StartupRecovery {
    private final DataSource dataSource;
    private final PullStore db;
    private final ProbeStore probes;
    private final TransactionTemplate transaction;
    private final boolean lockRequired;
    private final boolean dispatch;
    private Connection lock;
    private boolean acquired;

    public StartupRecovery(DataSource dataSource, PullStore db, ProbeStore probes,
                           PlatformTransactionManager manager, Environment environment) {
        this.dataSource = dataSource; this.db = db; this.probes = probes; this.transaction = new TransactionTemplate(manager);
        lockRequired = !environment.acceptsProfiles(Profiles.of("test"))
                || environment.getProperty("platform.recovery.test-instance-lock", Boolean.class, false);
        dispatch = environment.getProperty("platform.recovery.dispatch-enabled", Boolean.class, true);
    }

    @PostConstruct
    void recover() throws SQLException {
        try {
            if (lockRequired) {
                lock = dataSource.getConnection();
                try (var statement = lock.createStatement();
                     var result = statement.executeQuery("SELECT pg_try_advisory_lock(hashtext(current_schema()), 150080831)")) {
                    acquired = result.next() && result.getBoolean(1);
                    if (!acquired) throw new IllegalStateException("Another sharing platform instance owns this database schema");
                }
            }
            if (dispatch) transaction.executeWithoutResult(status -> {
                db.update("UPDATE transfer_task SET claimed_at=now()-interval '6 minutes' WHERE status='RUNNING' AND claim_id IS NOT NULL");
                db.update("UPDATE pull_transfer SET claimed_at=now()-interval '6 minutes' WHERE claim_id IS NOT NULL");
                db.update("UPDATE transfer_notification SET claimed_at=now()-interval '2 minutes' WHERE status='SENDING' AND claim_id IS NOT NULL");
                db.update("UPDATE pull_listener_run SET heartbeat_at=now()-interval '2 minutes' WHERE status='SCANNING'");
                probes.recoverInterrupted();
            });
        } catch (SQLException | RuntimeException error) {
            try { release(); } catch (SQLException cleanup) { error.addSuppressed(cleanup); }
            throw error;
        }
    }

    @PreDestroy
    void release() throws SQLException {
        if (lock == null) return;
        try (Connection connection = lock) {
            if (acquired) try (var statement = connection.createStatement()) {
                statement.execute("SELECT pg_advisory_unlock(hashtext(current_schema()), 150080831)");
            }
        } finally { lock = null; acquired = false; }
    }
}
