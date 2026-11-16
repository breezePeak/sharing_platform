package com.siwei.web;

import com.siwei.transfer.*;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/api")
public class TransferController {
    private final TaskService service;
    private final TransferSettings settings;
    public TransferController(TaskService service, TransferSettings settings) { this.service = service; this.settings = settings; }

    @PostMapping(value = "/transfers/tasks", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<TaskService.Accepted> create(@RequestHeader(value = "Authorization", required = false) String authorization,
                                                         HttpServletRequest request) {
        String caller = service.authenticate(authorization);
        String body = body(request);
        TaskService.Accepted result = service.accept(caller, service.parseJson(body), body, "JSON");
        return ResponseEntity.status(result.reused ? HttpStatus.OK : HttpStatus.ACCEPTED).body(result);
    }

    @PostMapping(value = "/legacy/task-notices", consumes = {MediaType.APPLICATION_XML_VALUE, MediaType.TEXT_XML_VALUE})
    public ResponseEntity<TaskService.Accepted> legacy(@RequestHeader(value = "Authorization", required = false) String authorization,
                                                         HttpServletRequest request) {
        String caller = service.authenticate(authorization);
        if (!settings.isProvisionalXmlEnabled()) throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "XML_CONTRACT_PENDING");
        String body = body(request);
        TaskService.Accepted result = service.accept(caller, service.parseXml(body), body, "XML");
        return ResponseEntity.status(result.reused ? HttpStatus.OK : HttpStatus.ACCEPTED).body(result);
    }

    @GetMapping("/transfers/tasks/{id}")
    public Task get(@RequestHeader(value = "Authorization", required = false) String authorization, @PathVariable String id) {
        return service.get(service.authenticate(authorization), id);
    }

    @GetMapping("/transfers/tasks/{id}/files")
    public Map<String, Object> files(@RequestHeader(value = "Authorization", required = false) String authorization,
                                      @PathVariable String id, @RequestParam(defaultValue = "0") int page,
                                      @RequestParam(defaultValue = "50") int size) {
        return service.files(service.authenticate(authorization), id, page, size);
    }

    private static String body(HttpServletRequest request) {
        if (request.getContentLengthLong() > 32768) throw new ResponseStatusException(HttpStatus.PAYLOAD_TOO_LARGE, "REQUEST_TOO_LARGE");
        try {
            byte[] bytes = request.getInputStream().readNBytes(32769);
            if (bytes.length > 32768) throw new ResponseStatusException(HttpStatus.PAYLOAD_TOO_LARGE, "REQUEST_TOO_LARGE");
            return new String(bytes, StandardCharsets.UTF_8);
        } catch (IOException e) { throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "INVALID_TASK", e); }
    }
}
