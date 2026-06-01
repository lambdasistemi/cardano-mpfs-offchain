// Thin wrapper over globalThis.runCageReactor, seeded by src/bootstrap.js.

export const runCageReactorImpl = (stdinText) => () => {
  if (typeof globalThis.runCageReactor !== "function") {
    return Promise.resolve({
      stdout: "",
      stderr: "runCageReactor is not installed",
      exitOk: false,
    });
  }

  return globalThis.runCageReactor(stdinText);
};

export const postJsonImpl = (url) => (bodyText) => () =>
  fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: bodyText,
  }).then(async (res) => {
    const text = await res.text();
    let json = null;
    try {
      json = text.length ? JSON.parse(text) : null;
    } catch (_e) {
      json = null;
    }
    return { ok: res.ok, status: res.status, json, text };
  });
