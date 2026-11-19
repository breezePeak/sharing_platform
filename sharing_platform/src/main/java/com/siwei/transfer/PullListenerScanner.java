package com.siwei.transfer;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.siwei.endpoint.Endpoint;
import com.siwei.endpoint.EndpointRevision;
import com.siwei.protocol.ControlledPaths;
import com.siwei.protocol.RemoteFileClientFactory;
import java.io.IOException;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import javax.annotation.PreDestroy;
import org.slf4j.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.server.ResponseStatusException;

@Component
@ConditionalOnProperty(name = "platform.pulls.enabled", havingValue = "true", matchIfMissing = true)
public class PullListenerScanner {
    private static final Logger log = LoggerFactory.getLogger(PullListenerScanner.class);
    private static final int LEASE_SECONDS = 90;
    private final PullStore db;
    private final PullListenerService listeners;
    private final PullSettings settings;
    private final PullProcessor pulls;
    private final TaskService tasks;
    private final RemoteFileClientFactory clients;
    private final TransactionTemplate tx;
    private final Clock clock;
    private final boolean scheduleEnabled;
    private final int perEndpoint;
    private final ThreadPoolExecutor executor;
    private final Set<String> active = ConcurrentHashMap.newKeySet();

    public PullListenerScanner(PullStore db, PullListenerService listeners, PullSettings settings, PullProcessor pulls,
            TaskService tasks, RemoteFileClientFactory clients, PlatformTransactionManager manager, Clock clock,
            @Value("${platform.pulls.listeners.schedule-enabled:true}") boolean scheduleEnabled,
            @Value("${platform.pulls.listeners.workers:4}") int workers,
            @Value("${platform.pulls.listeners.per-endpoint:1}") int perEndpoint) {
        this.db = db; this.listeners = listeners; this.settings = settings; this.pulls = pulls; this.tasks = tasks;
        this.clients = clients; this.tx = new TransactionTemplate(manager); this.clock = clock;
        this.scheduleEnabled = scheduleEnabled;
        int size = Math.max(1, Math.min(8, workers));
        this.perEndpoint = Math.max(1, Math.min(size > 1 ? size - 1 : 1, perEndpoint));
        this.executor = new ThreadPoolExecutor(size, size, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<>(size),
                runnable -> { Thread thread = new Thread(runnable, "pull-listener-scan"); thread.setDaemon(true); return thread; });
    }

    @Scheduled(fixedDelay = 1000)
    public synchronized void poll() {
        if (!settings.isIntakeEnabled()) return;
        Timestamp now = Timestamp.from(clock.instant());
        recover(now);
        db.update("WITH due AS (SELECT s.id,CASE WHEN l.manual_pending THEN 'MANUAL_QUEUED' ELSE 'PERIOD_COALESCED' END AS reason "
                + "FROM pull_listener_run s JOIN pull_listener l ON l.id=s.listener_id "
                + "WHERE s.status='SCANNING' AND s.coalesced_reason IS NULL AND "
                + "(l.manual_pending OR (? AND l.enabled AND l.next_run_at<=?)) FOR UPDATE OF s SKIP LOCKED) "
                + "UPDATE pull_listener_run s SET coalesced_reason=due.reason FROM due WHERE s.id=due.id", scheduleEnabled, now);
        int slots = executor.getMaximumPoolSize() - active.size();
        if (slots <= 0) return;
        List<Map<String, Object>> due = db.rows("SELECT l.id,r.endpoint_id,"
                + "row_number() OVER (PARTITION BY r.endpoint_id ORDER BY l.last_started_at NULLS FIRST,l.next_run_at,l.id) AS priority,"
                + "l.last_started_at,l.next_run_at FROM pull_listener l JOIN pull_listener_revision r ON r.listener_id=l.id AND l.revision=r.revision "
                + "JOIN endpoint e ON e.id=r.endpoint_id WHERE e.enabled AND "
                + "(l.manual_pending OR (? AND l.enabled AND l.next_run_at<=?)) "
                + "AND NOT EXISTS (SELECT 1 FROM pull_listener_run WHERE listener_id=l.id AND status='SCANNING') "
                + "ORDER BY priority,l.last_started_at NULLS FIRST,l.next_run_at,l.id LIMIT 100", scheduleEnabled, now);
        for (var row : due) {
            if (slots <= 0) break;
            Run run = tx.execute(status -> claim((String) row.get("id"), (String) row.get("endpoint_id")));
            if (run == null) continue;
            active.add(run.id);
            try { executor.execute(() -> scan(run)); slots--; }
            catch (RejectedExecutionException error) {
                active.remove(run.id);
                interrupt(run.id, run.listener, Timestamp.from(clock.instant()), true);
                break;
            }
        }
    }

    private Run claim(String id, String endpointId) {
        if (db.rows("SELECT id FROM endpoint WHERE id=? FOR UPDATE SKIP LOCKED", endpointId).isEmpty()) return null;
        Endpoint endpoint = listeners.lockEndpoint(endpointId);
        if (!endpoint.enabled) return null;
        EndpointRevision revision = listeners.current(endpoint);
        if (PullListenerService.number(db.one("SELECT count(*) AS n FROM pull_listener_run s "
                + "JOIN pull_listener_revision r ON r.listener_id=s.listener_id AND r.revision=s.revision "
                + "WHERE r.endpoint_id=? AND s.status='SCANNING'", endpointId), "n") >= Math.min(perEndpoint, revision.concurrency)) return null;
        Instant now = clock.instant();
        Timestamp stamp = Timestamp.from(now);
        var eligible = db.rows(PullListenerService.CURRENT + " WHERE l.id=? AND "
                + "(l.manual_pending OR (? AND l.enabled AND l.next_run_at<=?)) FOR UPDATE OF l", id, scheduleEnabled, stamp);
        if (eligible.isEmpty()) return null;
        var row = eligible.get(0);
        boolean manual = (Boolean) row.get("manual_pending");
        if (!db.rows("SELECT id FROM pull_listener_run WHERE listener_id=? AND status='SCANNING'", id).isEmpty()) return null;
        String runId = UUID.randomUUID().toString();
        int version = PullListenerService.number(row, "revision");
        String trigger = manual ? "MANUAL" : "SCHEDULE";
        db.update("INSERT INTO pull_listener_run(id,listener_id,revision,endpoint_revision_id,trigger_type,status,started_at,heartbeat_at) "
                + "VALUES (?,?,?,?,?,'SCANNING',?,?)", runId, id, version, revision.id, trigger, stamp, stamp);
        db.update("UPDATE pull_listener SET manual_pending=false,last_started_at=?,"
                + "next_run_at=CASE WHEN ? AND enabled AND next_run_at<=? THEN cast(? AS timestamptz) ELSE next_run_at END WHERE id=?",
                stamp, scheduleEnabled, stamp, Timestamp.from(now.plusSeconds(PullListenerService.number(row, "period_seconds"))), id);
        return new Run(runId, id, trigger, (String) row.get("source_path"), version, now, revision, row);
    }

    private void scan(Run run) {
        try {
            ObjectNode config = listeners.checkedSnapshot(run.snapshot, run.endpoint);
            String path = ControlledPaths.resolve(run.endpoint.readRoot, run.scope);
            Map<String, ObjectNode> observations = new LinkedHashMap<>();
            int entries = 0;
            try (var remote = clients.connect(run.endpoint)) {
                List<String> paths = pulls.discover(remote, path, config);
                if (paths == null) throw new PullProcessor.NotReady("SOURCE_MISSING");
                for (String unit : paths) {
                    ObjectNode observation = pulls.observe(remote, unit, config);
                    entries += observation.path("files").size() + observation.path("directories").size();
                    if (entries > 100000) throw new PullProcessor.InvalidSource("SCAN_TOO_LARGE");
                    observations.put(unit, observation);
                }
            }
            tx.executeWithoutResult(status -> publish(run, config, observations));
        } catch (Exception error) {
            String reason = "SCAN_ERROR";
            if (error instanceof ResponseStatusException) reason = ((ResponseStatusException) error).getReason();
            else if (error instanceof PullProcessor.InvalidSource || error instanceof PullProcessor.NotReady) reason = error.getMessage();
            else if (error instanceof IOException) reason = "SCAN_IO";
            var code = reason != null && reason.matches("[A-Z0-9_]{1,100}") ? reason : "SCAN_ERROR";
            if (code.equals("SCAN_ERROR")) log.warn("listener scan failed run={} type={}", run.id, error.getClass().getSimpleName(), error);
            db.update("UPDATE pull_listener_run SET status='FAILED',ended_at=?,error_type=? WHERE id=? AND status='SCANNING'",
                    Timestamp.from(clock.instant()), code, run.id);
        } finally { active.remove(run.id); }
    }

    private void publish(Run run, ObjectNode config, Map<String, ObjectNode> observations) {
        Endpoint endpoint = listeners.lockEndpoint(run.endpoint.endpointId);
        var current = listeners.require(PullListenerService.CURRENT + " WHERE l.id=? FOR UPDATE OF l", run.listener);
        if (db.rows("SELECT id FROM pull_listener_run WHERE id=? AND status='SCANNING' FOR UPDATE", run.id).isEmpty()) return;
        if (!endpoint.enabled) throw TaskService.error(org.springframework.http.HttpStatus.BAD_REQUEST, "ENDPOINT_DISABLED");
        if (PullListenerService.number(current, "revision") != run.version
                || ("SCHEDULE".equals(run.trigger) && !(Boolean) current.get("enabled")))
            throw TaskService.error(org.springframework.http.HttpStatus.CONFLICT, "SCAN_SUPERSEDED");
        boolean initialize = current.get("initialized_at") == null;
        boolean baseline = initialize && "BASELINE_ONLY".equals(current.get("initial_mode"));
        int versions = 0, waiting = 0, created = 0, reused = 0;
        String task = null;
        Set<String> reusedTasks = new HashSet<>();
        for (var entry : observations.entrySet()) {
            String path = entry.getKey();
            ObjectNode observation = entry.getValue();
            String reason = pulls.recordObservation(endpoint.id, path, config, observation, run.started);
            db.update("INSERT INTO pull_listener_candidate(listener_id,revision,candidate_key,last_run_id) VALUES (?,?,?,?) "
                    + "ON CONFLICT(listener_id,revision,candidate_key) DO UPDATE SET last_run_id=excluded.last_run_id",
                    run.listener, run.version, PullProcessor.candidateKey(endpoint.id, path, config), run.id);
            PullProcessor.Version version = PullProcessor.version(endpoint.id, path, config, observation);
            if (baseline) {
                versions += db.update("INSERT INTO pull_listener_baseline(listener_id,revision,delivery_key) VALUES (?,?,?) ON CONFLICT DO NOTHING",
                        run.listener, run.version, version.key);
                continue;
            }
            if (pulls.baselined(run.listener, version.key)) continue;
            if (reason != null) { waiting++; continue; }
            String existing = pulls.currentDelivery(endpoint.id, path, config, version);
            if (!pulls.needsTransfer(existing)) {
                reused++;
                reusedTasks.add((String) db.one("SELECT owner_task_id FROM pull_transfer WHERE id=?", existing).get("owner_task_id"));
                continue;
            }
            if (existing == null) versions++;
            if (task == null) task = tasks.acceptScanned(run.listener, run.version, run.trigger, run.scope, config, run.endpoint);
            db.update("INSERT INTO pull_task_unit(task_id,source_path) VALUES (?,?)", task, path);
            pulls.link(task, path, pulls.create(task, endpoint.id, path, config, observation, version, existing));
            created++;
        }
        // A complete inventory can reset missing candidates observed by this scope. A partial scan never reaches this transaction.
        db.update("UPDATE pull_candidate c SET stable_count=0,first_observed_at=NULL,last_seen_at=?,last_error='SOURCE_MISSING' "
                + "FROM pull_listener_candidate m WHERE m.listener_id=? AND m.revision=? AND m.candidate_key=c.candidate_key "
                + "AND m.last_run_id IS NOT NULL AND m.last_run_id<>? AND c.last_seen_at<=?",
                Timestamp.from(run.started), run.listener, run.version, run.id, Timestamp.from(run.started));
        db.update("UPDATE pull_listener_candidate SET last_run_id=NULL WHERE listener_id=? AND revision=? AND last_run_id<>?",
                run.listener, run.version, run.id);
        if (task != null) linkRun(run.id, task);
        for (String reusedTask : reusedTasks) linkRun(run.id, reusedTask);
        Timestamp now = Timestamp.from(clock.instant());
        if (initialize) db.update("UPDATE pull_listener SET initialized_at=?,initialized_revision=? WHERE id=?", now, run.version, run.listener);
        db.update("UPDATE pull_listener_run SET status=?,ended_at=?,candidate_count=?,waiting_count=?,new_version_count=?,"
                + "created_count=?,reused_count=?,created_task_count=?,reused_task_count=? WHERE id=?",
                baseline ? "BASELINED" : "SUCCEEDED", now, observations.size(), waiting, versions, created, reused,
                task == null ? 0 : 1, reusedTasks.size(), run.id);
    }

    private void linkRun(String run, String task) {
        db.update("INSERT INTO pull_listener_run_task(run_id,task_id) VALUES (?,?) ON CONFLICT DO NOTHING", run, task);
    }

    @Scheduled(fixedDelay = 15000)
    public void heartbeat() {
        for (String id : active) db.update("UPDATE pull_listener_run SET heartbeat_at=? WHERE id=? AND status='SCANNING'",
                Timestamp.from(clock.instant()), id);
    }

    private void recover(Timestamp now) {
        Timestamp expired = Timestamp.from(now.toInstant().minusSeconds(LEASE_SECONDS));
        for (var row : db.rows("SELECT id,listener_id FROM pull_listener_run WHERE status='SCANNING' AND heartbeat_at<=?", expired)) {
            String id = (String) row.get("id");
            if (!active.contains(id)) interrupt(id, (String) row.get("listener_id"), now, false);
        }
    }

    private void interrupt(String id, String listener, Timestamp now, boolean rejected) {
        tx.executeWithoutResult(status -> {
            listeners.require("SELECT id FROM pull_listener WHERE id=? FOR UPDATE", listener);
            var rows = db.rows("SELECT trigger_type FROM pull_listener_run WHERE id=? AND status='SCANNING' AND (?"
                    + " OR heartbeat_at<=?) FOR UPDATE", id, rejected, Timestamp.from(now.toInstant().minusSeconds(LEASE_SECONDS)));
            if (rows.isEmpty()) return;
            db.update("UPDATE pull_listener_run SET status='INTERRUPTED',ended_at=?,error_type=? WHERE id=?",
                    now, rejected ? "SCAN_POOL_BUSY" : "SCAN_LEASE_EXPIRED", id);
            db.update("UPDATE pull_listener SET manual_pending=manual_pending OR ?,next_run_at=LEAST(next_run_at,cast(? AS timestamptz)) WHERE id=?",
                    "MANUAL".equals(rows.get(0).get("trigger_type")), now, listener);
        });
    }

    @PreDestroy public void close() { executor.shutdownNow(); }

    private static final class Run {
        final String id, listener, trigger, scope;
        final int version;
        final Instant started;
        final EndpointRevision endpoint;
        final Map<String, Object> snapshot;

        Run(String id, String listener, String trigger, String scope, int version, Instant started,
            EndpointRevision endpoint, Map<String, Object> snapshot) {
            this.id = id; this.listener = listener; this.trigger = trigger; this.scope = scope; this.version = version;
            this.started = started; this.endpoint = endpoint; this.snapshot = snapshot;
        }
    }
}
