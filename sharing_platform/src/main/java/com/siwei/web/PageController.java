package com.siwei.web;

import com.siwei.transfer.ConsoleQueryService;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

@Controller
public class PageController {
    private static final Set<String> STATES = Set.of("QUEUED", "RUNNING", "WAITING_FEEDBACK", "RECONCILING", "WAITING_SOURCE",
            "WAITING_ACCEPTANCE", "SUCCEEDED", "FAILED", "REJECTED", "NO_NEW_DATA");
    private final ConsoleQueryService console;
    private final ConsoleOperations operations;
    private final int refresh;

    public PageController(ConsoleQueryService console, ConsoleOperations operations,
                          @Value("${platform.console.refresh-millis:2000}") int refresh) {
        this.console = console; this.operations = operations;
        this.refresh = Math.max(1000, Math.min(refresh, 60000));
    }

    @ModelAttribute("csrfToken")
    Object csrf(HttpServletRequest request) {
        var session = request.getSession(false);
        return session == null ? null : session.getAttribute(EndpointCsrfFilter.TOKEN);
    }

    @ModelAttribute("operationNonce")
    String nonce() { return UUID.randomUUID().toString(); }

    @GetMapping("/")
    public String home() {
        return "redirect:/services";
    }

    @GetMapping("/services")
    public String services(Model model) {
        model.addAttribute("services", console.services());
        return "services";
    }

    @GetMapping("/config")
    public String config() {
        return "redirect:/config/endpoints";
    }

    @GetMapping("/config/routes")
    String routes(Model model) {
        model.addAllAttributes(console.configuration());
        return "config/routes";
    }

    @GetMapping("/tasks")
    public String tasks(@RequestParam(defaultValue = "") String direction, @RequestParam(defaultValue = "") String endpoint,
                        @RequestParam(defaultValue = "") String status,
                        @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate since,
                        @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate until,
                        @RequestParam(defaultValue = "0") int page, Model model) {
        if (page < 0 || endpoint.length() > 36 || !(direction.isEmpty() || Set.of("PUSH", "PULL").contains(direction))
                || !(status.isEmpty() || STATES.contains(status)) || (since != null && until != null && until.isBefore(since)))
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "INVALID_FILTER");
        model.addAttribute("services", console.services());
        model.addAttribute("states", new java.util.TreeSet<>(STATES));
        model.addAllAttributes(Map.of("direction", direction, "endpoint", endpoint, "status", status,
                "since", since == null ? "" : since.toString(), "until", until == null ? "" : until.toString()));
        var rows = console.tasks(direction, endpoint, status, since == null ? null : since.atStartOfDay().toInstant(ZoneOffset.UTC),
                until == null ? null : until.plusDays(1).atStartOfDay().toInstant(ZoneOffset.UTC), page);
        page(model, rows, page);
        return "tasks";
    }

    @GetMapping("/tasks/{id}")
    String task(@PathVariable String id, @RequestParam(defaultValue = "files") String view,
                @RequestParam(defaultValue = "0") int page, Model model) {
        if (page < 0 || !Set.of("files", "units", "timeline").contains(view))
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "INVALID_PAGE");
        var task = console.task(id);
        model.addAttribute("task", task);
        model.addAttribute("view", view);
        model.addAttribute("refresh", refresh);
        page(model, console.details(id, (String) task.get("direction"), view, page), page);
        return "task";
    }

    @GetMapping("/tasks/{id}/status")
    String snapshot(@PathVariable String id, Model model) {
        model.addAttribute("task", console.task(id));
        return "task :: summary";
    }

    @PostMapping("/tasks/{id}/notifications/{notice}/resend")
    String resend(@PathVariable String id, @PathVariable String notice, @RequestParam String nonce, HttpServletRequest request) {
        String before = console.notificationState(id, notice);
        operations.run((String) request.getAttribute(EndpointCsrfFilter.ACTOR), notice, "RESEND", nonce, before, () -> {
            if (!console.resend(id, notice)) throw new ResponseStatusException(HttpStatus.CONFLICT, "NOTIFICATION_NOT_RETRYABLE");
            return "PENDING";
        });
        return "redirect:/tasks/" + id + "?view=timeline";
    }

    @PostMapping("/tasks/{id}/recheck")
    String recheck(@PathVariable String id, @RequestParam String nonce, HttpServletRequest request) {
        String before = (String) console.task(id).get("status");
        operations.run((String) request.getAttribute(EndpointCsrfFilter.ACTOR), id, "RECHECK", nonce, before, () -> {
            if (!console.recheck(id)) throw new ResponseStatusException(HttpStatus.CONFLICT, "TASK_NOT_RECHECKABLE");
            return "RECHECK_QUEUED";
        });
        return "redirect:/tasks/" + id;
    }

    private static void page(Model model, List<Map<String, Object>> rows, int page) {
        model.addAttribute("rows", rows.subList(0, Math.min(rows.size(), ConsoleQueryService.PAGE_SIZE)));
        model.addAttribute("page", page);
        model.addAttribute("more", rows.size() > ConsoleQueryService.PAGE_SIZE);
    }
}
