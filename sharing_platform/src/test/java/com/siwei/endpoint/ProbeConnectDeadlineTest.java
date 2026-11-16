package com.siwei.endpoint;

import com.siwei.StreamingFtpServer;
import com.siwei.protocol.RemoteFileClientFactory;
import java.time.Duration;
import java.util.Base64;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.web.server.ResponseStatusException;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class ProbeConnectDeadlineTest {
    @ParameterizedTest @EnumSource(value = StreamingFtpServer.Fault.class, names = {"WELCOME", "AUTH"})
    void totalDeadlineReleasesBothWorkersDuringHandshake(StreamingFtpServer.Fault fault) throws Exception {
        EndpointSettings settings = new EndpointSettings();
        settings.setMasterKey(Base64.getEncoder().encodeToString(new byte[32]));
        CredentialCipher cipher = new CredentialCipher(settings);
        EndpointRevision revision = new EndpointRevision();
        revision.id = UUID.randomUUID().toString(); revision.protocol = EndpointRevision.Protocol.FTP;
        revision.host = "127.0.0.1"; revision.username = "tester"; revision.auth = EndpointRevision.Auth.PASSWORD;
        revision.secret = cipher.encrypt("test-only", revision.id); revision.timeoutSeconds = 1;
        revision.isolatedFtpRoot = true; revision.passive = true;
        ProbeStore store = mock(ProbeStore.class);
        ConcurrentMap<String, String> persisted = new ConcurrentHashMap<>();
        when(store.create(anyString(), anyLong())).thenAnswer(invocation -> {
            EndpointProbe probe = new EndpointProbe(); probe.id = UUID.randomUUID().toString();
            probe.revisionId = revision.id; probe.status = "RUNNING";
            persisted.put(probe.id, "RUNNING"); return probe;
        });
        when(store.progress(anyString(), anyString(), anyString())).thenAnswer(invocation ->
                "RUNNING".equals(persisted.get(invocation.getArgument(0))));
        doAnswer(invocation -> { persisted.computeIfPresent(invocation.getArgument(0), (id, status) ->
                status.equals("RUNNING") ? invocation.getArgument(1) : status); return null; })
                .when(store).stop(anyString(), anyString());
        doAnswer(invocation -> { EndpointProbe result = invocation.getArgument(0);
            persisted.computeIfPresent(result.id, (id, status) -> status.equals("RUNNING") ? result.status : status);
            return null; }).when(store).finish(any());
        EndpointRevisionRepository revisions = mock(EndpointRevisionRepository.class);
        when(revisions.findById(revision.id)).thenReturn(Optional.of(revision));
        try (StreamingFtpServer server = new StreamingFtpServer(fault)) {
            revision.port = server.port();
            ProbeRunner runner = new ProbeRunner(store, revisions, new RemoteFileClientFactory(cipher, settings));
            try {
                runner.start("endpoint", 1); runner.start("endpoint", 1);
                StreamingFtpServer.await(() -> server.started.get() == 2, Duration.ofSeconds(3));
                assertThatThrownBy(() -> runner.start("endpoint", 1)).isInstanceOf(ResponseStatusException.class);
                StreamingFtpServer.await(() -> server.stopped.get() >= 2, Duration.ofSeconds(9));
                assertThat(persisted.values().stream().filter("TIMED_OUT"::equals).count()).isEqualTo(2);
                StreamingFtpServer.await(() -> {
                    try { runner.start("endpoint", 1); return true; }
                    catch (ResponseStatusException busy) { return false; }
                }, Duration.ofSeconds(3));
                StreamingFtpServer.await(() -> persisted.containsValue("COMPLETED"), Duration.ofSeconds(3));
                assertThat(server.connections.get()).isGreaterThanOrEqualTo(3);
                assertThat(persisted.values().stream().filter("TIMED_OUT"::equals).count()).isEqualTo(2);
                verify(store, atLeast(2)).stop(anyString(), eq("TIMED_OUT"));
            } finally { runner.close(); }
        }
    }
}
