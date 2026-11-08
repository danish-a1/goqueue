// goqueue dashboard: stat bar polling and batch job actions. No inline script (CSP).
(() => {
  "use strict";
  const base = document.body.dataset.base;
  const poll = Number(document.body.dataset.poll) || 3000;
  const fmt = (n) => Number(n).toLocaleString();
  const stats = document.querySelectorAll("[data-stat]");
  stats.forEach((el) => { el.textContent = fmt(el.textContent); });

  // Stat bar: poll /api/stats, paused while the tab is hidden. An expired session
  // answers with a redirect to the login page, so reload to land there.
  let timer = 0;
  const schedule = () => {
    clearTimeout(timer);
    if (!document.hidden) timer = setTimeout(refresh, poll);
  };
  async function refresh() {
    try {
      const res = await fetch(base + "/api/stats", { credentials: "same-origin", headers: { Accept: "application/json" } });
      if (res.redirected) { location.reload(); return; }
      if (res.ok) {
        const s = await res.json();
        stats.forEach((el) => { if (el.dataset.stat in s) el.textContent = fmt(s[el.dataset.stat]); });
      }
    } catch (_) { /* network blip: keep the last numbers */ }
    schedule();
  }
  document.addEventListener("visibilitychange", () => (document.hidden ? clearTimeout(timer) : refresh()));
  schedule();

  // Job tables: checkbox selection, Kill/Delete behind an in-page confirm bar.
  document.querySelectorAll(".jobs").forEach((box) => {
    const rows = () => [...box.querySelectorAll("tbody input[type=checkbox]")];
    const picked = () => rows().filter((c) => c.checked).map((c) => c.value);
    const buttons = box.querySelectorAll("[data-action]");
    const all = box.querySelector("[data-all]");
    const bar = box.querySelector(".confirm");
    const msg = box.querySelector(".msg");
    let queued = null;

    const sync = () => {
      const n = picked().length;
      buttons.forEach((b) => { b.disabled = n === 0; });
      all.checked = n > 0 && n === rows().length;
    };
    const closeBar = () => { bar.hidden = true; queued = null; };

    box.addEventListener("change", (e) => {
      if (e.target === all) rows().forEach((c) => { c.checked = all.checked; });
      sync();
    });
    buttons.forEach((b) => b.addEventListener("click", () => {
      const action = b.dataset.action, ids = picked();
      if (action === "kill" || action === "delete") {
        queued = { action, ids };
        bar.querySelector("span").textContent = b.textContent + " " + ids.length + " job(s)?";
        bar.hidden = false;
        return;
      }
      run(action, ids);
    }));
    bar.querySelector("[data-cancel]").addEventListener("click", closeBar);
    bar.querySelector("[data-confirm]").addEventListener("click", () => {
      const q = queued;
      closeBar();
      if (q) run(q.action, q.ids);
    });

    async function run(action, ids) {
      buttons.forEach((b) => { b.disabled = true; });
      msg.textContent = "Working...";
      try {
        const res = await fetch(base + "/api/actions", {
          method: "POST",
          credentials: "same-origin",
          headers: { "Content-Type": "application/json", Accept: "application/json" },
          body: JSON.stringify({ action, queue: box.dataset.queue, source: box.dataset.source, job_ids: ids }),
        });
        if (res.redirected) { location.reload(); return; }
        const out = await res.json().catch(() => ({}));
        if (!res.ok) { msg.textContent = out.error || "Failed (" + res.status + ")"; return; }
        const done = out.Done || [], gone = out.NotFound || [], skipped = out.Skipped || [];
        // Skipped jobs are still in Redis, so their rows stay.
        const remove = new Set(done.concat(gone));
        rows().forEach((c) => { if (remove.has(c.value)) c.closest("tr").remove(); });
        msg.textContent = action + ": " + done.length + " done" +
          (gone.length ? ", " + gone.length + " already gone" : "") +
          (skipped.length ? ", " + skipped.length + " skipped: job name unknown" : "");
        refresh();
      } catch (_) {
        msg.textContent = "Network error, nothing confirmed. Reload to check.";
      } finally {
        sync();
      }
    }
    sync();
  });
})();
