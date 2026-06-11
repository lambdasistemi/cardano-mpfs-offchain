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
