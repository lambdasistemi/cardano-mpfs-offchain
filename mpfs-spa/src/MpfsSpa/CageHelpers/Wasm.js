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
