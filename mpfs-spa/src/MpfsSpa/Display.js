export const _decodeUtf8Hex = (hex) => {
  const value = String(hex || "").trim();
  if (value.length === 0) return "";
  if (value.length % 2 !== 0 || /[^0-9a-fA-F]/.test(value)) return null;

  const bytes = new Uint8Array(value.length / 2);
  for (let i = 0; i < value.length; i += 2) {
    bytes[i / 2] = Number.parseInt(value.slice(i, i + 2), 16);
  }

  try {
    return new TextDecoder("utf-8", { fatal: true }).decode(bytes);
  } catch {
    return null;
  }
};

export const encodeUtf8Hex = (text) =>
  Array.from(new TextEncoder().encode(text))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");

export const currentTimeMillis = () => Date.now();
