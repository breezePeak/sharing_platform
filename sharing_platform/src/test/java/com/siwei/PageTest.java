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
class PageTest {
    @Autowired MockMvc mvc;

    @Test
    void pagesAreDirectlyAccessibleAndRenderHonestEmptyStates() throws Exception {
        for (String page : new String[] {"/services", "/config", "/tasks"}) {
            mvc.perform(get(page)).andExpect(status().isOk())
                    .andExpect(content().string(containsString("尚未开放")))
                    .andExpect(content().string(containsString("/css/app.css")))
                    .andExpect(content().string(not(containsString("/login"))));
        }
        mvc.perform(get("/")).andExpect(redirectedUrl("/services"));
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
        mvc.perform(post("/config")).andExpect(status().isMethodNotAllowed());
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
