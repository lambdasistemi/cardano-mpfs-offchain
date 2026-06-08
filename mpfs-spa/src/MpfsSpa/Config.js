const intOverride = (name, fallback) => {
  if (typeof window === "undefined") return fallback;
  const parsed = Number.parseInt(window[name], 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
};

// Server base URL from the host page, defaulting to same-origin.
export const _baseUrl = () =>
  (typeof window !== "undefined" && window.MPFS_BASE_URL) || "";

export const _defaultProcessTime = intOverride(
  "MPFS_CAGE_DEFAULT_PROCESS_TIME",
  1800000,
);

export const _defaultRetractTime = intOverride(
  "MPFS_CAGE_DEFAULT_RETRACT_TIME",
  1800000,
);
