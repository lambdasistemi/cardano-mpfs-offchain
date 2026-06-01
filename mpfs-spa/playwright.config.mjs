import { defineConfig } from "@playwright/test";

const port = Number(process.env.PLAYWRIGHT_PORT || "4173");
const baseURL = process.env.PLAYWRIGHT_BASE_URL || `http://127.0.0.1:${port}/`;
const siteDir = process.env.MPFS_SPA_SITE_DIR || "../result";

const shellQuote = (value) => `'${String(value).replaceAll("'", "'\\''")}'`;

export default defineConfig({
  testDir: "./tests",
  timeout: 60_000,
  expect: {
    timeout: 15_000,
  },
  fullyParallel: false,
  workers: 1,
  retries: process.env.CI ? 1 : 0,
  reporter: process.env.CI ? "line" : "list",
  use: {
    baseURL,
    launchOptions: {
      args: ["--no-sandbox", "--disable-dev-shm-usage"],
    },
    screenshot: "only-on-failure",
    trace: "retain-on-failure",
  },
  webServer: process.env.PLAYWRIGHT_SKIP_WEBSERVER
    ? undefined
    : {
        command: `python -m http.server ${port} --bind 127.0.0.1 --directory ${shellQuote(siteDir)}`,
        port,
        timeout: 15_000,
        reuseExistingServer: true,
      },
  projects: [
    {
      name: "firefox",
      use: { browserName: "firefox" },
    },
  ],
});
