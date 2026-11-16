package com.siwei.web;

import com.siwei.transfer.*;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.*;

@RestController
@ConditionalOnProperty(name = "platform.pulls.enabled", havingValue = "true", matchIfMissing = true)
public class PullConfirmationController {
    private final TaskService tasks;
    private final PullProcessor pulls;
    public PullConfirmationController(TaskService tasks, PullProcessor pulls) { this.tasks = tasks; this.pulls = pulls; }

    @PostMapping(value = "/api/sharing/pull/confirmations", consumes = "application/json")
    public Map<String, Object> confirm(@RequestHeader(value = "Authorization", required = false) String authorization,
                                       HttpServletRequest request) {
        String recipient = tasks.authenticate(authorization);
        return pulls.confirm(recipient, tasks.parseJson(TransferController.body(request)));
    }
}
