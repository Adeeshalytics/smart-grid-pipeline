// Capture the screenshots used in the report from the running stack.
//
//   node tools/screenshots.mjs            all shots
//   node tools/screenshots.mjs alerts     only shots whose name contains "alerts"
//
// Drives a headless Chrome over the DevTools protocol (no npm dependencies; needs Node 22+).
import { spawn } from "node:child_process";
import { mkdirSync, mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const CHROME = process.env.CHROME || "C:/Program Files/Google/Chrome/Application/chrome.exe";
const OUT = join(import.meta.dirname, "..", "docs", "img");
const PORT = 9777;
const GRAFANA = "http://localhost:3000";
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// name, url, viewport [w, h], wait ms, optional clip [x, y, w, h], optional JS to run before capture
const SHOTS = [
  { name: "grafana_grid_ops_1", url: `${GRAFANA}/d/grid-ops/grid-operations?kiosk&theme=light&refresh=1h`, size: [1500, 2010], wait: 14000, clip: [0, 0, 1500, 1010] },
  { name: "grafana_grid_ops_2", url: null, size: [1500, 2010], wait: 0, clip: [0, 1010, 1500, 1000] },
  { name: "grafana_pipeline_1", url: `${GRAFANA}/d/grid-pipeline/pipeline-health?kiosk&theme=light&refresh=1h`, size: [1500, 2050], wait: 16000, clip: [0, 0, 1500, 1000] },
  { name: "grafana_pipeline_2", url: null, size: [1500, 2050], wait: 0, clip: [0, 1000, 1500, 1050] },
  { name: "prometheus_alerts", url: "http://localhost:9090/alerts", size: [1500, 1110], wait: 5000 },
  { name: "daily_report", url: "http://localhost:8000/api/v1/reports/latest", size: [1400, 1750], wait: 2000 },
  { name: "api_docs", url: "http://localhost:8000/docs", size: [1400, 1150], wait: 4000 },
  { name: "lake_raw_partitions", url: "http://localhost:8888/buckets/grid-lake/raw/meter_readings/", size: [1400, 560], wait: 3000 },
  {
    name: "airflow_dag", url: "http://localhost:8080/login/", size: [1500, 900], wait: 6000,
    // Local development credentials from docker-compose.yml.
    js: `document.querySelector('#username').value='admin';document.querySelector('#password').value='admin';document.querySelector('input[type=submit]').click();`,
    then: { url: "http://localhost:8080/dags/grid_daily_billing/grid?tab=graph", wait: 12000 },
  },
];

const filter = process.argv[2];
mkdirSync(OUT, { recursive: true });
const chrome = spawn(CHROME, [
  "--headless=new", "--disable-gpu", "--hide-scrollbars", `--remote-debugging-port=${PORT}`,
  `--user-data-dir=${mkdtempSync(join(tmpdir(), "grid-shots-"))}`, "about:blank",
], { stdio: "ignore" });

try {
  let target;
  for (let i = 0; i < 40 && !target; i++) {
    await sleep(500);
    try {
      const pages = await (await fetch(`http://127.0.0.1:${PORT}/json`)).json();
      target = pages.find((p) => p.type === "page");
    } catch { /* Chrome still starting */ }
  }
  if (!target) throw new Error("Chrome did not start");

  const ws = new WebSocket(target.webSocketDebuggerUrl);
  await new Promise((resolve, reject) => { ws.onopen = resolve; ws.onerror = reject; });
  let nextId = 0;
  const pending = new Map();
  ws.onmessage = (msg) => {
    const data = JSON.parse(msg.data);
    if (data.id && pending.has(data.id)) { pending.get(data.id)(data); pending.delete(data.id); }
  };
  const send = (method, params = {}) => new Promise((resolve) => {
    const id = ++nextId;
    pending.set(id, resolve);
    ws.send(JSON.stringify({ id, method, params }));
  });

  await send("Page.enable");
  for (const shot of SHOTS) {
    if (filter && !shot.name.includes(filter)) continue;
    const [width, height] = shot.size;
    await send("Emulation.setDeviceMetricsOverride", { width, height, deviceScaleFactor: 1, mobile: false });
    if (shot.url) { await send("Page.navigate", { url: shot.url }); await sleep(shot.wait); }
    if (shot.js) { await send("Runtime.evaluate", { expression: shot.js }); await sleep(7000); }
    if (shot.then) { await send("Page.navigate", { url: shot.then.url }); await sleep(shot.then.wait); }
    const params = { format: "png" };
    if (shot.clip) {
      const [x, y, w, h] = shot.clip;
      params.clip = { x, y, width: w, height: h, scale: 1 };
    }
    const result = await send("Page.captureScreenshot", params);
    writeFileSync(join(OUT, `${shot.name}.png`), Buffer.from(result.result.data, "base64"));
    console.log("saved", `${shot.name}.png`);
  }
  ws.close();
} finally {
  chrome.kill();
}
