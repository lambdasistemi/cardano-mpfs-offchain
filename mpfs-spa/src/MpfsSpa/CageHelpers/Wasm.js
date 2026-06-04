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

// UTF-8 encode a user-typed string to a lowercase hex string, so the
// /facts/request/* fields (Hex on the wire) accept plain text like
// "start"/"amaru" instead of requiring the user to pre-hex-encode.
export const encodeUtf8Hex = (text) =>
  Array.from(new TextEncoder().encode(text))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");

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
