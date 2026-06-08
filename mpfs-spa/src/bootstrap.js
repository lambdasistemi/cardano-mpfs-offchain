// Bootstrap the MPFS cage reactor for the browser bundle. The wasm file is
// emitted as a separate hashed asset by esbuild, so browsers can stream and
// cache it independently from the JavaScript bundle.

import { WASI, File, OpenFile, ConsoleStdout }
  from "@bjorn3/browser_wasi_shim";
import reactorWasmAssetUrl from "./assets/mpfs-cage-reactor.wasm";

const reactorWasmUrl = new URL(
  reactorWasmAssetUrl,
  globalThis.document?.baseURI ?? globalThis.location?.href ?? "http://localhost/"
).toString();

let compiledModulePromise = null;

globalThis.runCageReactor = async (stdinText) => {
  const stdin = new OpenFile(
    new File(new TextEncoder().encode(stdinText))
  );
  const stdoutChunks = [];
  const stderrChunks = [];
  const stdout = new ConsoleStdout((chunk) => stdoutChunks.push(chunk));
  const stderr = new ConsoleStdout((chunk) => stderrChunks.push(chunk));

  const wasi = new WASI([], [], [stdin, stdout, stderr]);
  const inst = await instantiateReactor({
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

async function instantiateReactor(imports) {
  if (compiledModulePromise !== null) {
    const mod = await compiledModulePromise;
    return WebAssembly.instantiate(mod, imports);
  }

  if (WebAssembly.instantiateStreaming) {
    try {
      const result = await WebAssembly.instantiateStreaming(
        fetchReactorWasm(),
        imports
      );
      compiledModulePromise = Promise.resolve(result.module);
      return result.instance;
    } catch (_err) {
      compiledModulePromise = compileReactorWasm();
      const mod = await compiledModulePromise;
      return WebAssembly.instantiate(mod, imports);
    }
  }

  compiledModulePromise = compileReactorWasm();
  const mod = await compiledModulePromise;
  return WebAssembly.instantiate(mod, imports);
}

async function compileReactorWasm() {
  if (WebAssembly.compileStreaming) {
    try {
      return await WebAssembly.compileStreaming(fetchReactorWasm());
    } catch (_err) {
      // Fall back for servers that miss application/wasm on the response.
    }
  }

  const response = await fetchReactorWasm();
  return WebAssembly.compile(await response.arrayBuffer());
}

async function fetchReactorWasm() {
  const response = await fetch(reactorWasmUrl);
  if (!response.ok) {
    throw new Error(
      `failed to fetch reactor wasm: HTTP ${response.status}`
    );
  }
  return response;
}

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
