package com.siwei.web;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;

@Controller
public class PageController {
    @GetMapping("/")
    public String home() {
        return "redirect:/services";
    }

    @GetMapping("/services")
    public String services(Model model) {
        model.addAttribute("title", "当前服务");
        model.addAttribute("message", "尚未配置远端服务或拉取监听。连接配置与传输功能尚未开放。");
        return "overview";
    }

    @GetMapping("/config")
    public String config(Model model) {
        model.addAttribute("title", "平台配置");
        model.addAttribute("message", "远端连接与多目标监听的配置功能尚未开放。当前没有可编辑的业务配置。");
        return "overview";
    }

    @GetMapping("/tasks")
    public String tasks(Model model) {
        model.addAttribute("title", "任务记录");
        model.addAttribute("message", "暂无任务记录。推送、拉取和任务受理功能尚未开放。");
        return "overview";
    }
}
