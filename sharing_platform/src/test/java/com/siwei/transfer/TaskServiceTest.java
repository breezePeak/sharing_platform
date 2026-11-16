package com.siwei.transfer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.siwei.endpoint.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.web.server.ResponseStatusException;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class TaskServiceTest {
    private final ObjectMapper json = new ObjectMapper();
    private final JdbcTemplate db = mock(JdbcTemplate.class);
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
        service = new TaskService(json, db, mock(PlatformTransactionManager.class), settings, endpoints, revisions, List.of());
    }

    @Test void credentialsAreServerSideAndXxeIsRejected() {
        assertThat(service.authenticate("Bearer test-secret")).isEqualTo("test");
        assertThatThrownBy(() -> service.authenticate("Bearer wrong"))
                .isInstanceOf(ResponseStatusException.class).satisfies(e -> assertThat(((ResponseStatusException) e).getStatus()).isEqualTo(HttpStatus.UNAUTHORIZED));
        String xxe = "<!DOCTYPE taskNotice [<!ENTITY xxe SYSTEM 'file:///etc/passwd'>]><taskNotice><taskId>&xxe;</taskId></taskNotice>";
        assertThatThrownBy(() -> service.parseXml(xxe)).isInstanceOf(ResponseStatusException.class);
        assertThatThrownBy(() -> service.parseJson("{" + "x".repeat(32769) + "}"))
                .isInstanceOf(ResponseStatusException.class);
    }

    @Test void noProductionProcessorCannotPersistAnUnrunnableTask() {
        when(db.queryForObject(anyString(), any(org.springframework.jdbc.core.RowMapper.class), any(Object[].class)))
                .thenThrow(new org.springframework.dao.EmptyResultDataAccessException(1));
        String request = "{\"taskId\":\"a\",\"direction\":\"PUSH\",\"endpointId\":\"endpoint\",\"sourceRoot\":\"local\",\"targetPath\":\"a\",\"products\":[{\"productId\":\"p\",\"productPath\":\"p\"}]}";
        assertThatThrownBy(() -> service.accept("test", service.parseJson(request), request, "JSON"))
                .isInstanceOf(ResponseStatusException.class).satisfies(e -> assertThat(((ResponseStatusException) e).getStatus()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE));
        verify(db, never()).update(anyString(), any(Object[].class));
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
}
