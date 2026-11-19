package com.siwei;

import com.siwei.web.PageController;
import com.siwei.web.TraceFilter;
import com.siwei.web.WebErrors;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Controller;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(controllers = {PageController.class, PageTest.FailureController.class})
@Import({TraceFilter.class, WebErrors.class, PageTest.FailureController.class})
@org.springframework.test.context.ActiveProfiles("test")
class PageTest {
    @Autowired MockMvc mvc;
    @org.springframework.boot.test.mock.mockito.MockBean com.siwei.transfer.ConsoleQueryService console;
    @org.springframework.boot.test.mock.mockito.MockBean com.siwei.web.ConsoleOperations operations;

    @Test
    void pagesAreDirectlyAccessibleAndRenderHonestEmptyStates() throws Exception {
        for (String page : new String[] {"/services", "/tasks"}) {
            mvc.perform(get(page)).andExpect(status().isOk())
                    .andExpect(content().string(containsString(page.equals("/services") ? "暂无配置服务" : "当前筛选下暂无任务")))
                    .andExpect(content().string(containsString("/css/app.css")))
                    .andExpect(content().string(not(containsString("/login"))));
        }
        mvc.perform(get("/")).andExpect(redirectedUrl("/services"));
        mvc.perform(get("/config")).andExpect(redirectedUrl("/config/endpoints"));
        mvc.perform(get("/login")).andExpect(status().isNotFound());
    }

    @Test
    void traceIdsAreGeneratedByTheServerAndStylesAreLocal() throws Exception {
        mvc.perform(get("/services").header("X-Trace-Id", "untrusted"))
                .andExpect(header().exists("X-Trace-Id"))
                .andExpect(header().string("X-Trace-Id", not("untrusted")));
        mvc.perform(get("/css/app.css")).andExpect(status().isOk());
    }

    @Test
    void configurationPageHasNoWriteEndpointYet() throws Exception {
        var visit = mvc.perform(get("/config")).andReturn();
        var session = (org.springframework.mock.web.MockHttpSession) visit.getRequest().getSession(false);
        String csrf = (String) session.getAttribute(com.siwei.web.EndpointCsrfFilter.TOKEN);
        mvc.perform(post("/config").session(session).param("_csrf", csrf)).andExpect(status().isMethodNotAllowed());
    }

    @Test
    void businessApiIsClosedWithoutPretendingToAcceptTasks() throws Exception {
        mvc.perform(post("/api/transfers/tasks")).andExpect(status().isNotFound());
        mvc.perform(get("/api/transfers/tasks")).andExpect(status().isNotFound());
    }

    @Test
    void errorsHideSecretsAndInvalidParametersHaveUsefulMessage() throws Exception {
        mvc.perform(get("/test/failure")).andExpect(status().isInternalServerError())
                .andExpect(content().string(containsString("追踪编号")))
                .andExpect(content().string(not(containsString("database-password-canary"))))
                .andExpect(content().string(not(containsString("IllegalStateException"))));
        mvc.perform(get("/test/parameters").param("count", "invalid"))
                .andExpect(status().isBadRequest()).andExpect(content().string(containsString("请求参数无效")));
        mvc.perform(get("/test/parameters")).andExpect(status().isBadRequest());
    }

    @Controller
    static class FailureController {
        @GetMapping("/test/failure")
        String failure() {
            throw new IllegalStateException("database-password-canary");
        }

        @GetMapping("/test/parameters")
        String parameters(@RequestParam int count) {
            return "overview";
        }
    }
}
