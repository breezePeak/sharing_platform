package com.siwei.web;

import java.util.Map;
import org.springframework.core.annotation.Order;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

@Order(org.springframework.core.Ordered.HIGHEST_PRECEDENCE)
@RestControllerAdvice(assignableTypes = {TransferController.class, PullConfirmationController.class})
public class TransferErrors {
    @ExceptionHandler(ResponseStatusException.class)
    ResponseEntity<Map<String, String>> known(ResponseStatusException error) {
        return ResponseEntity.status(error.getStatus()).body(Map.of("code", error.getReason() == null ? "REQUEST_REJECTED" : error.getReason()));
    }

    @ExceptionHandler({org.springframework.http.converter.HttpMessageNotReadableException.class,
            org.springframework.web.method.annotation.MethodArgumentTypeMismatchException.class})
    ResponseEntity<Map<String, String>> invalid() {
        return ResponseEntity.badRequest().body(Map.of("code", "INVALID_TASK"));
    }

    @ExceptionHandler(RuntimeException.class)
    ResponseEntity<Map<String, String>> unexpected() {
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("code", "INTERNAL_ERROR"));
    }
}
