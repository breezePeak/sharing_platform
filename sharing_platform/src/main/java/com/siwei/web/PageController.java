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
        model.addAttribute("message", "可前往平台配置维护 FTP/SFTP 连接并检测能力。服务运行汇总、监听与传输任务尚未开放。");
        return "overview";
    }

    @GetMapping("/config")
    public String config(Model model) {
        return "redirect:/config/endpoints";
    }

    @GetMapping("/tasks")
    public String tasks(Model model) {
        model.addAttribute("title", "任务记录");
        model.addAttribute("message", "暂无任务记录。推送、拉取和任务受理功能尚未开放。");
        return "overview";
    }
}
