// Server base URL from the host page, defaulting to same-origin.
export const _baseUrl = () =>
  (typeof window !== "undefined" && window.MPFS_BASE_URL) || "";
