package com.siwei.web;

import com.siwei.transfer.PullListenerService;
import javax.servlet.http.HttpServletRequest;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.servlet.ModelAndView;

@Controller
@RequestMapping("/config/listeners")
public class PullListenerController {
    private final PullListenerService listeners;

    public PullListenerController(PullListenerService listeners) { this.listeners = listeners; }

    @ModelAttribute("csrfToken")
    Object csrf(HttpServletRequest request) { return request.getSession().getAttribute(EndpointCsrfFilter.TOKEN); }

    @GetMapping
    String list(Model model) {
        model.addAttribute("listeners", listeners.list());
        return "config/listeners";
    }

    @GetMapping({"/new", "/{id}"})
    String form(@PathVariable(required = false) String id, Model model) {
        model.addAttribute("listener", id == null ? null : listeners.form(id));
        model.addAttribute("sources", listeners.sourceIds());
        return "config/listener-edit";
    }

    @PostMapping({"", "/{id}"})
    String save(@PathVariable(required = false) String id, @RequestParam(defaultValue = "0") int editVersion,
                @RequestParam String name, @RequestParam String sourceId, @RequestParam String sourcePath,
                @RequestParam(defaultValue = "60") int period, @RequestParam String mode,
                @RequestParam(defaultValue = "false") boolean inherit) {
        listeners.save(id, editVersion, name, sourceId, sourcePath, period, mode, inherit);
        return "redirect:/config/listeners";
    }

    @PostMapping("/{id}/toggle")
    String toggle(@PathVariable String id, @RequestParam int editVersion) {
        listeners.toggle(id, editVersion);
        return "redirect:/config/listeners";
    }

    @PostMapping("/{id}/run")
    String run(@PathVariable String id) {
        listeners.manual(id);
        return "redirect:/config/listeners/" + id + "/runs";
    }

    @GetMapping("/{id}/runs")
    String history(@PathVariable String id, Model model) {
        model.addAttribute("listener", listeners.form(id));
        model.addAttribute("runs", listeners.history(id));
        model.addAttribute("tasks", listeners.apiTasks(id));
        return "config/listener-runs";
    }

    @GetMapping("/{id}/runs/{runId}/tasks")
    String runTasks(@PathVariable String id, @PathVariable String runId, Model model) {
        model.addAttribute("listener", listeners.form(id));
        model.addAttribute("tasks", listeners.runTasks(id, runId));
        return "config/listener-runs";
    }

    @ExceptionHandler(ResponseStatusException.class)
    ModelAndView invalid(ResponseStatusException error) {
        String message;
        switch (error.getReason() == null ? "" : error.getReason()) {
            case "OVERLAPPING_VERSION_RULES":
                message = "扫描范围和交付范围重叠，但单元边界或版本依据不同。请选择互不重叠的目录，或统一来源规则。";
                break;
            case "CONFIRM_SCOPE_CHANGE":
                message = "来源或范围已变化。请返回编辑页，确认保留历史并按新范围扫描后重新保存。";
                break;
            case "STALE_LISTENER":
                message = "监听配置已更新。请刷新页面后重试。";
                break;
            case "ENDPOINT_DISABLED":
                message = "连接已停用。启用连接后才可执行扫描。";
                break;
            case "ENGINE_NOT_READY":
                message = "当前部署已关闭新拉取任务受理。";
                break;
            default:
                message = "监听或来源配置无效。请检查来源、目录、扫描周期与初始化方式。";
        }
        ModelAndView view = new ModelAndView("error");
        view.setStatus(error.getStatus());
        view.addObject("status", error.getRawStatusCode());
        view.addObject("safeMessage", message);
        return view;
    }
}
