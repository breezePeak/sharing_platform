const assert = require("node:assert/strict");
const { readFileSync } = require("node:fs");
const { runInNewContext } = require("node:vm");

const source = readFileSync(require("node:path").join(__dirname, "../../main/resources/static/js/task-refresh.js"), "utf8");
const events = {};
const queue = new Map();
const warning = { hidden: true };
const panel = {
    dataset: { statusUrl: "/tasks/example/status", refreshMs: "2000" },
    summary: "9007199254740993",
    querySelector(selector) {
        if (selector === "[data-refresh-warning]") return warning;
        return { replaceWith: value => { this.summary = value; } };
    }
};
let nextTimer = 0;
let pending;
const document = {
    hidden: false,
    querySelector: () => panel,
    addEventListener: (name, callback) => { events[name] = callback; },
    importNode: value => value
};
runInNewContext(source, {
    document, AbortController,
    window: { addEventListener: (name, callback) => { events[name] = callback; } },
    DOMParser: class { parseFromString(text) { return { querySelector: () => text }; } },
    setTimeout: (callback, delay) => { queue.set(++nextTimer, { callback, delay }); return nextTimer; },
    clearTimeout: id => queue.delete(id),
    fetch: (url, { signal }) => {
        assert.equal(url, "/tasks/example/status");
        return new Promise((resolve, reject) => {
            pending = { resolve, reject, signal };
            signal.addEventListener("abort", () => reject(new Error("Aborted")));
        });
    }
});

function tick() {
    const [id, entry] = [...queue].find(([, timer]) => timer.delay === 2000);
    queue.delete(id);
    return entry.callback();
}

(async () => {
    let refresh = tick();
    pending.reject(new Error("Offline"));
    await refresh;
    assert.equal(warning.hidden, false);
    assert.equal(panel.summary, "9007199254740993");
    refresh = tick();
    pending.resolve({ ok: true, text: async () => "9007199254740994" });
    await refresh;
    assert.equal(panel.summary, "9007199254740994");
    assert.equal(warning.hidden, true);
    refresh = tick();
    document.hidden = true;
    events.visibilitychange();
    await refresh;
    assert.equal(pending.signal.aborted, true);
    assert.equal(queue.size, 0);
    document.hidden = false;
    events.visibilitychange();
    assert.equal(pending.signal.aborted, false);
    events.pagehide();
    await new Promise(setImmediate);
    assert.equal(pending.signal.aborted, true);
    assert.equal(queue.size, 0);
    process.stdout.write("Offline, recovery, exact bytes, visibility and page exit passed.\n");
})().catch(error => { console.error(error); process.exitCode = 1; });
