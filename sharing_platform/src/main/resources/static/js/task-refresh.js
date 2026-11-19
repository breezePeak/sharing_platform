"use strict";

const panel = document.querySelector("[data-status-url]");
if (panel) {
    const delay = Number(panel.dataset.refreshMs);
    const warning = panel.querySelector("[data-refresh-warning]");
    let timer;
    let request;

    function stop() {
        clearTimeout(timer);
        request?.abort();
        request = undefined;
    }

    async function update() {
        if (document.hidden) return;
        const operation = new AbortController();
        request = operation;
        const deadline = setTimeout(() => operation.abort(), 10000);
        try {
            const response = await fetch(panel.dataset.statusUrl, { cache: "no-store", signal: operation.signal });
            if (!response.ok) throw new Error("Status unavailable");
            const html = new DOMParser().parseFromString(await response.text(), "text/html");
            const summary = html.querySelector("[data-task-summary]");
            if (!summary) throw new Error("Status missing");
            if (request !== operation) return;
            panel.querySelector("[data-task-summary]").replaceWith(document.importNode(summary, true));
            warning.hidden = true;
        } catch {
            if (request === operation) warning.hidden = false;
        } finally {
            clearTimeout(deadline);
            if (request === operation) {
                request = undefined;
                if (!document.hidden) timer = setTimeout(update, delay);
            }
        }
    }

    document.addEventListener("visibilitychange", () => {
        stop();
        if (!document.hidden) update();
    });
    window.addEventListener("pagehide", stop);
    window.addEventListener("pageshow", event => {
        if (event.persisted && !document.hidden) update();
    });
    if (!document.hidden) timer = setTimeout(update, delay);
}
