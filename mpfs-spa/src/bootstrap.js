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
  const stdoutChunks = [];
  const stderrChunks = [];
  const stdout = new ConsoleStdout((chunk) => stdoutChunks.push(chunk));
  const stderr = new ConsoleStdout((chunk) => stderrChunks.push(chunk));

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
    stderrChunks.push(new TextEncoder().encode(String(err)));
  }

  return {
    stdout: decodeChunks(stdoutChunks),
    stderr: decodeChunks(stderrChunks),
    exitOk,
  };
};

function decodeChunks(chunks) {
  const size = chunks.reduce((total, chunk) => total + chunk.byteLength, 0);
  const bytes = new Uint8Array(size);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return new TextDecoder().decode(bytes);
}
