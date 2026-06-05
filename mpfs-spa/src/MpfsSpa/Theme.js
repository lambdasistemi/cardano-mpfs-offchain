const storageKey = "mpfs-spa-theme-mode";

export const initialThemeMode = () => {
  try {
    const saved = window.localStorage.getItem(storageKey);
    if (saved === "light" || saved === "dark") return saved;
  } catch (_e) {
    // Fall through to OS preference.
  }

  try {
    return window.matchMedia("(prefers-color-scheme: dark)").matches
      ? "dark"
      : "light";
  } catch (_e) {
    return "light";
  }
};

export const storeThemeMode = (mode) => () => {
  try {
    window.localStorage.setItem(storageKey, mode === "dark" ? "dark" : "light");
  } catch (_e) {
    // Storage can be disabled; the in-memory React state still updates.
  }
};
