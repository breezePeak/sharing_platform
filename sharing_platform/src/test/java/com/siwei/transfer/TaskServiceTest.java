package com.siwei.transfer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.siwei.endpoint.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.web.server.ResponseStatusException;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class TaskServiceTest {
    private final ObjectMapper json = new ObjectMapper();
    private final TransferJpa db = mock(TransferJpa.class);
    private final TransferTaskRepository tasks = mock(TransferTaskRepository.class);
    private final TransferProductRepository products = mock(TransferProductRepository.class);
    private final TransferNotificationRepository notifications = mock(TransferNotificationRepository.class);
    private final EndpointRepository endpoints = mock(EndpointRepository.class);
    private final EndpointRevisionRepository revisions = mock(EndpointRevisionRepository.class);
    private final TransferSettings settings = new TransferSettings();
    private final TaskService service;

    TaskServiceTest() {
        TransferSettings.Caller caller = new TransferSettings.Caller();
        caller.setToken("test-secret"); caller.setEndpointIds(Set.of("endpoint"));
        caller.setNotificationUrl("http://127.0.0.1:9000/ack");
        caller.setXmlRoutes(Map.of("weather", "endpoint"));
        settings.setCallers(Map.of("test", caller));
        service = new TaskService(json, db, tasks, products, notifications,
                mock(PlatformTransactionManager.class), settings, endpoints, revisions, new EndpointSettings(), List.of());
    }

    @Test void credentialsAreServerSideAndXxeIsRejected() {
        assertThat(new TransferSettings().isProvisionalXmlEnabled()).isFalse();
        assertThatThrownBy(() -> new com.siwei.web.TransferController(service, settings)
                .legacy("Bearer test-secret", new org.springframework.mock.web.MockHttpServletRequest()))
                .isInstanceOf(ResponseStatusException.class)
                .satisfies(e -> {
                    assertThat(((ResponseStatusException) e).getStatus()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
                    assertThat(((ResponseStatusException) e).getReason()).isEqualTo("XML_CONTRACT_PENDING");
                });
        assertThat(service.authenticate("Bearer test-secret")).isEqualTo("test");
        assertThatThrownBy(() -> service.authenticate("Bearer wrong"))
                .isInstanceOf(ResponseStatusException.class).satisfies(e -> assertThat(((ResponseStatusException) e).getStatus()).isEqualTo(HttpStatus.UNAUTHORIZED));
        String xxe = "<!DOCTYPE taskNotice [<!ENTITY xxe SYSTEM 'file:///etc/passwd'>]><taskNotice><taskId>&xxe;</taskId></taskNotice>";
        assertThatThrownBy(() -> service.parseXml(xxe)).isInstanceOf(ResponseStatusException.class);
        assertThatThrownBy(() -> service.parseJson("{" + "x".repeat(32769) + "}"))
                .isInstanceOf(ResponseStatusException.class);
    }

    @Test void noProductionProcessorCannotPersistAnUnrunnableTask() {
        String request = "{\"taskId\":\"a\",\"direction\":\"PUSH\",\"endpointId\":\"endpoint\",\"sourceRoot\":\"local\",\"targetPath\":\"a\",\"products\":[{\"productId\":\"p\",\"productPath\":\"p\"}]}";
        assertThatThrownBy(() -> service.accept("test", service.parseJson(request), request, "JSON"))
                .isInstanceOf(ResponseStatusException.class).satisfies(e -> assertThat(((ResponseStatusException) e).getStatus()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE));
        verifyNoInteractions(db);
    }

    @Test void injectedIdentifierAndUnknownFieldsAreRejectedBeforeDatabaseAccess() {
        String injected = "{\"taskId\":\"x'; DROP TABLE transfer_task;--\",\"direction\":\"PUSH\",\"endpointId\":\"endpoint\"}";
        String unknown = "{\"taskId\":\"ok\",\"direction\":\"PUSH\",\"endpointId\":\"endpoint\",\"callbackUrl\":\"http://evil\"}";
        assertThatThrownBy(() -> service.accept("test", service.parseJson(injected), injected, "JSON"))
                .isInstanceOf(ResponseStatusException.class);
        assertThatThrownBy(() -> service.accept("test", service.parseJson(unknown), unknown, "JSON"))
                .isInstanceOf(ResponseStatusException.class);
        verifyNoInteractions(db);
    }

    @Test void fileCountMustMatchProductCount() {
        String request = "{\"taskId\":\"count\",\"direction\":\"PUSH\",\"endpointId\":\"endpoint\",\"sourceRoot\":\"local\",\"targetPath\":\"a\",\"fileCount\":2,\"products\":[{\"productId\":\"p\",\"productPath\":\"p\"}]}";
        assertThatThrownBy(() -> service.accept("test", service.parseJson(request), request, "JSON"))
                .isInstanceOf(ResponseStatusException.class).satisfies(e -> assertThat(((ResponseStatusException) e).getStatus()).isEqualTo(HttpStatus.BAD_REQUEST));
    }

    @Test void stoppingNewPushIntakeKeepsExistingProcessorOutOfAcceptancePath() {
        settings.setPushIntakeEnabled(false);
        TaskProcessor processor = new TaskProcessor() {
            public String direction() { return "PUSH"; }
            public Result process(Task task) { return new Result("SUCCEEDED", "COMPLETE", null, null); }
        };
        TaskService stopped = new TaskService(json, db, tasks, products, notifications,
                mock(PlatformTransactionManager.class), settings, endpoints, revisions,
                new EndpointSettings(), List.of(processor));
        String request = "{\"taskId\":\"stopped\",\"direction\":\"PUSH\",\"endpointId\":\"endpoint\",\"sourceRoot\":\"local\",\"targetPath\":\"a\",\"products\":[{\"productId\":\"p\",\"productPath\":\"p\"}]}";
        assertThatThrownBy(() -> stopped.accept("test", stopped.parseJson(request), request, "JSON"))
                .isInstanceOf(ResponseStatusException.class).satisfies(e -> assertThat(((ResponseStatusException) e).getStatus()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE));
        verifyNoInteractions(db);
    }
}
