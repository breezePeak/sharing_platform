package com.siwei.web;

import javax.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.validation.BindException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.servlet.ModelAndView;

@ControllerAdvice
public class WebErrors {
    private static final Logger log = LoggerFactory.getLogger(WebErrors.class);

    @ModelAttribute("traceId")
    public Object traceId(HttpServletRequest request) {
        return request.getAttribute("traceId");
    }

    @ExceptionHandler({BindException.class, MethodArgumentTypeMismatchException.class, MissingServletRequestParameterException.class})
    public ModelAndView invalidParameters(HttpServletRequest request) {
        return error(HttpStatus.BAD_REQUEST, "请求参数无效，请检查后重试。", request);
    }

    @ExceptionHandler(RuntimeException.class)
    public ModelAndView unexpected(RuntimeException exception, HttpServletRequest request) {
        if (exception instanceof org.springframework.web.server.ResponseStatusException) {
            throw exception;
        }
        log.error("request failed type={}", exception.getClass().getSimpleName());
        return error(HttpStatus.INTERNAL_SERVER_ERROR, "请求暂时无法处理，请联系管理员并提供追踪编号。", request);
    }

    private ModelAndView error(HttpStatus status, String message, HttpServletRequest request) {
        ModelAndView view = new ModelAndView("error");
        view.setStatus(status);
        view.addObject("status", status.value());
        view.addObject("safeMessage", message);
        view.addObject("traceId", request.getAttribute("traceId"));
        return view;
    }
}
