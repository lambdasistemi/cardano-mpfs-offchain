// Minimal fetch FFI.
//
// One entry point: issue a request and return a normalised response carrying
// the parsed JSON body (as an argonaut Json value — JSON.parse output is a
// valid Json representation), the HTTP status, the ok flag, and the raw text
// for error reporting. Decoding into typed records happens on the PureScript
// side. No MPFS protocol logic here — these are read endpoints plus /submit.

export const _fetchJson = (method) => (url) => (bodyOrNull) => () =>
  fetch(url, {
    method,
    headers:
      bodyOrNull == null ? {} : { "Content-Type": "application/json" },
    body: bodyOrNull == null ? undefined : bodyOrNull,
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
