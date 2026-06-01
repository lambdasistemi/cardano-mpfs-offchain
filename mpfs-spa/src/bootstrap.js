// Bootstrap the MPFS cage reactor for the browser bundle.
//
// esbuild embeds the WASM bytes via --loader:.wasm=binary. Each reactor call
// gets a fresh WASI instance while the compiled WebAssembly.Module is shared.

import { WASI, File, OpenFile, ConsoleStdout }
  from "@bjorn3/browser_wasi_shim";
import wasmBytes from "./assets/mpfs-cage-reactor.wasm";

const compiledModulePromise = WebAssembly.compile(wasmBytes);

globalThis.runCageReactor = async (stdinText) => {
  const stdin = new OpenFile(
    new File(new TextEncoder().encode(stdinText))
  );
  const stdoutLines = [];
  const stderrLines = [];
  const stdout = ConsoleStdout.lineBuffered((line) => stdoutLines.push(line));
  const stderr = ConsoleStdout.lineBuffered((line) => stderrLines.push(line));

  const wasi = new WASI([], [], [stdin, stdout, stderr]);
  const mod = await compiledModulePromise;
  const inst = await WebAssembly.instantiate(mod, {
    wasi_snapshot_preview1: wasi.wasiImport,
  });

  let exitOk = true;
  try {
    wasi.start(inst);
  } catch (err) {
    exitOk = false;
    stderrLines.push(String(err));
  }

  return {
    stdout: stdoutLines.join("\n"),
    stderr: stderrLines.join("\n"),
    exitOk,
  };
};
