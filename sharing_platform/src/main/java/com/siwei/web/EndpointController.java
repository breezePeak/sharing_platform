package com.siwei.web;

import java.util.*;
import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;
import com.siwei.endpoint.*;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.WebDataBinder;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

@Controller
@RequestMapping("/config/endpoints")
public class EndpointController {
    private final EndpointService service;
    private final EndpointProbeRepository probes;
    private final ProbeRunner runner;
    public EndpointController(EndpointService service, EndpointProbeRepository probes, ProbeRunner runner) {
        this.service = service; this.probes = probes; this.runner = runner;
    }

    @InitBinder("form")
    void bind(WebDataBinder binder) {
        binder.initDirectFieldAccess();
        binder.setAllowedFields("revision", "name", "protocol", "host", "port", "username", "auth", "password", "keyFile",
                "fingerprint", "readRoot", "writeRoot", "readTestFile", "probeDirectory", "localSourceRef", "localTargetRef",
                "timeoutSeconds", "concurrency", "passive", "isolatedFtpRoot", "enabled");
    }
    @ModelAttribute("csrfToken")
    Object csrf(HttpServletRequest request) { return request.getSession().getAttribute(EndpointCsrfFilter.TOKEN); }

    @GetMapping
    String list(Model model) {
        List<Row> rows = new ArrayList<>();
        for (Endpoint e : service.list()) rows.add(new Row(e, service.current(e.id), probes.findFirstByEndpointIdOrderByStartedAtDesc(e.id).orElse(null)));
        model.addAttribute("rows", rows);
        return "config/endpoints";
    }
    @GetMapping("/new")
    String create(Model model) { model.addAttribute("form", new EndpointForm()); return "config/endpoint-edit"; }
    @GetMapping("/{id}")
    String edit(@PathVariable String id, Model model) {
        model.addAttribute("form", service.form(id)); model.addAttribute("endpointId", id);
        model.addAttribute("hasCredential", service.current(id).hasSecret());
        return "config/endpoint-edit";
    }
    @PostMapping({"", "/{id}"})
    String save(@PathVariable(required = false) String id, @Valid @ModelAttribute("form") EndpointForm form,
                BindingResult errors, Model model, javax.servlet.http.HttpServletResponse response) {
        if (Arrays.stream(errors.getSuppressedFields()).anyMatch(name -> !name.equals("_csrf"))) errors.reject("parameters");
        if (!errors.hasErrors()) {
            try { service.save(id, form); return "redirect:/config/endpoints"; }
            catch (IllegalArgumentException e) { model.addAttribute("validationMessage", e.getMessage()); }
        }
        form.password = "";
        model.addAttribute("endpointId", id);
        response.setStatus(400);
        return "config/endpoint-edit";
    }
    @PostMapping("/{id}/toggle")
    String toggle(@PathVariable String id, @RequestParam long revision) {
        service.toggle(id, revision); return "redirect:/config/endpoints";
    }
    @PostMapping("/{id}/clear-credential")
    String clear(@PathVariable String id, @RequestParam long revision, @RequestParam String confirmation) {
        if (!confirmation.equals("CLEAR")) throw new ResponseStatusException(HttpStatus.BAD_REQUEST);
        service.clearCredential(id, revision); return "redirect:/config/endpoints/" + id;
    }
    @PostMapping("/{id}/probe")
    String probe(@PathVariable String id, @RequestParam long revision) {
        runner.start(id, revision); return "redirect:/config/endpoints";
    }
    @ExceptionHandler(IllegalArgumentException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    String invalid(Model model) { model.addAttribute("safeMessage", "连接参数或当前状态不允许此操作，请刷新后检查。"); model.addAttribute("status", 400); return "error"; }

    public static class Row {
        public final String id, name, protocol, host;
        public final int port;
        public final long revision;
        public final boolean enabled, stale;
        public final EndpointProbe probe;
        Row(Endpoint e, EndpointRevision r, EndpointProbe p) {
            id = e.id; name = r.name; protocol = r.protocol.name(); host = r.host; port = r.port;
            revision = r.revision; enabled = e.enabled; probe = p; stale = p != null && !p.revisionId.equals(r.id);
        }
    }
}
