import { execFile } from "node:child_process";
import { spawn } from "node:child_process";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

export const preprodRequesterAddress =
  "addr_test1vz6zuvdm0gu3q54pk50wjfjwyt4mwj6uaelzdfh9extxgnqyycgv0";
export const preprodRequesterAddressHex =
  "60b42e31bb7a391052a1b51ee9264e22ebb74b5cee7e26a6e5c996644c";
export const preprodRequesterOwner =
  "b42e31bb7a391052a1b51ee9264e22ebb74b5cee7e26a6e5c996644c";

export const devnetGenesisAddressHex =
  "60f92331d882d35e05978c558352a66c61f476838e1e2fd1c4ae7fc0d6";

const devnetGenesisSkey = {
  type: "PaymentSigningKeyShelley_ed25519",
  description: "Devnet genesis UTxO signing key",
  cborHex:
    "58206532652d67656e657369732d7574786f2d6b65792d736565642d303030303031",
};

export async function signWitnessSet(txHex) {
  const normalized = normalizeHex(txHex);
  const tmp = await mkdtemp(path.join(os.tmpdir(), "mpfs-cip30-"));
  try {
    const skeyPath = path.join(tmp, "genesis.skey");
    const unsignedPath = path.join(tmp, "unsigned.tx");
    const signedPath = path.join(tmp, "signed.tx");

    await writeFile(skeyPath, `${JSON.stringify(devnetGenesisSkey, null, 2)}\n`);
    await writeFile(
      unsignedPath,
      `${JSON.stringify(
        {
          type: "Tx ConwayEra",
          description: "Ledger Cddl Format",
          cborHex: normalized,
        },
        null,
        2,
      )}\n`,
    );

    await execFileAsync(
      "cardano-cli",
      [
        "conway",
        "transaction",
        "sign",
        "--tx-file",
        unsignedPath,
        "--signing-key-file",
        skeyPath,
        "--testnet-magic",
        "42",
        "--out-file",
        signedPath,
      ],
      { maxBuffer: 4 * 1024 * 1024 },
    );

    const signed = JSON.parse(await readFile(signedPath, "utf8"));
    return extractWitnessSetHex(normalizeHex(signed.cborHex));
  } finally {
    await rm(tmp, { recursive: true, force: true });
  }
}

export async function signMoogWitnessSet(
  txHex,
  {
    walletPath = process.env.MPFS_SIGNER_WALLET,
    expectedAddress = preprodRequesterAddress,
    testnetMagic = process.env.MPFS_TESTNET_MAGIC || "1",
  } = {},
) {
  if (!walletPath) {
    throw new Error("MPFS_SIGNER_WALLET is required");
  }

  const normalized = normalizeHex(txHex);
  const wallet = await readMoogWallet(walletPath);
  const tmp = await mkdtemp(path.join(os.tmpdir(), "mpfs-cip30-preprod-"));
  try {
    const rootXskPath = path.join(tmp, "root.xsk");
    const paymentXskPath = path.join(tmp, "payment.xsk");
    const paymentSkeyPath = path.join(tmp, "payment.skey");
    const paymentVkeyPath = path.join(tmp, "payment.vkey");
    const paymentAddressPath = path.join(tmp, "payment.addr");
    const unsignedPath = path.join(tmp, "unsigned.tx");
    const signedPath = path.join(tmp, "signed.tx");

    const root = await runSecretTool("cardano-address", [
      "key",
      "from-recovery-phrase",
      "Shelley",
    ], {
      label: "derive root key",
      stdin: `${wallet.mnemonics}\n`,
    });
    await writeFile(rootXskPath, root.stdout);

    const payment = await runSecretTool("cardano-address", [
      "key",
      "child",
      "1852H/1815H/0H/0/0",
    ], {
      label: "derive payment key",
      stdin: root.stdout,
    });
    await writeFile(paymentXskPath, payment.stdout);

    await runPublicTool("cardano-cli", [
      "key",
      "convert-cardano-address-key",
      "--shelley-payment-key",
      "--signing-key-file",
      paymentXskPath,
      "--out-file",
      paymentSkeyPath,
    ], "convert payment key");

    await runPublicTool("cardano-cli", [
      "key",
      "verification-key",
      "--signing-key-file",
      paymentSkeyPath,
      "--verification-key-file",
      paymentVkeyPath,
    ], "derive payment verification key");

    await runPublicTool("cardano-cli", [
      "address",
      "build",
      "--payment-verification-key-file",
      paymentVkeyPath,
      "--testnet-magic",
      String(testnetMagic),
      "--out-file",
      paymentAddressPath,
    ], "derive payment address");

    const derivedAddress = (await readFile(paymentAddressPath, "utf8")).trim();
    if (expectedAddress && derivedAddress !== expectedAddress) {
      throw new Error(
        `derived payment address ${derivedAddress} does not match expected ${expectedAddress}`,
      );
    }

    await writeTxEnvelope(unsignedPath, normalized);
    await runPublicTool("cardano-cli", [
      "conway",
      "transaction",
      "sign",
      "--tx-file",
      unsignedPath,
      "--signing-key-file",
      paymentSkeyPath,
      "--testnet-magic",
      String(testnetMagic),
      "--out-file",
      signedPath,
    ], "sign transaction");

    const signed = JSON.parse(await readFile(signedPath, "utf8"));
    return extractWitnessSetHex(normalizeHex(signed.cborHex));
  } finally {
    await rm(tmp, { recursive: true, force: true });
  }
}

export function extractWitnessSetHex(txHex) {
  const bytes = Buffer.from(normalizeHex(txHex), "hex");
  const top = readHeader(bytes, 0);
  if (top.major !== 4 || top.length < 2) {
    throw new Error("expected a Conway transaction array");
  }

  let offset = top.offset;
  const body = readItem(bytes, offset);
  offset = body.end;
  const witnesses = readItem(bytes, offset);
  return bytes.subarray(witnesses.start, witnesses.end).toString("hex");
}

function normalizeHex(value) {
  const hex = String(value || "").replace(/\s+/g, "").toLowerCase();
  if (!hex || hex.length % 2 !== 0 || /[^0-9a-f]/.test(hex)) {
    throw new Error("expected even-length hex");
  }
  return hex;
}

async function readMoogWallet(walletPath) {
  const raw = await readFile(walletPath, "utf8");
  const wallet = JSON.parse(raw);
  if (typeof wallet.mnemonics !== "string" || wallet.mnemonics.trim() === "") {
    if (typeof wallet.encryptedMnemonics === "string") {
      throw new Error("encrypted moog wallets are not supported by this signer");
    }
    throw new Error("moog wallet JSON does not contain mnemonics");
  }
  return { mnemonics: wallet.mnemonics.trim().replace(/\s+/g, " ") };
}

async function writeTxEnvelope(filePath, cborHex) {
  await writeFile(
    filePath,
    `${JSON.stringify(
      {
        type: "Tx ConwayEra",
        description: "Ledger Cddl Format",
        cborHex,
      },
      null,
      2,
    )}\n`,
  );
}

async function runPublicTool(command, args, label) {
  try {
    await execFileAsync(command, args, { maxBuffer: 4 * 1024 * 1024 });
  } catch (error) {
    throw new Error(`${label} failed with exit code ${error.code ?? "unknown"}`);
  }
}

function runSecretTool(command, args, { label, stdin }) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      stdio: ["pipe", "pipe", "pipe"],
    });
    const stdout = [];
    const stderr = [];

    child.stdout.on("data", (chunk) => stdout.push(chunk));
    child.stderr.on("data", (chunk) => stderr.push(chunk));
    child.on("error", (error) => {
      reject(new Error(`${label} failed to start: ${error.message}`));
    });
    child.on("close", (code) => {
      if (code === 0) {
        resolve({
          stdout: Buffer.concat(stdout).toString("utf8"),
          stderr: Buffer.concat(stderr).toString("utf8"),
        });
      } else {
        reject(new Error(`${label} failed with exit code ${code}`));
      }
    });

    child.stdin.end(stdin);
  });
}

function readItem(bytes, start) {
  const header = readHeader(bytes, start);
  let offset = header.offset;

  switch (header.major) {
    case 0:
    case 1:
    case 7:
      return { start, end: offset };
    case 2:
    case 3:
      if (header.indefinite) {
        offset = skipIndefiniteChunks(bytes, offset);
      } else {
        offset += header.length;
      }
      break;
    case 4:
      if (header.indefinite) {
        while (bytes[offset] !== 0xff) {
          offset = readItem(bytes, offset).end;
        }
        offset += 1;
      } else {
        for (let i = 0; i < header.length; i += 1) {
          offset = readItem(bytes, offset).end;
        }
      }
      break;
    case 5:
      if (header.indefinite) {
        while (bytes[offset] !== 0xff) {
          offset = readItem(bytes, offset).end;
          offset = readItem(bytes, offset).end;
        }
        offset += 1;
      } else {
        for (let i = 0; i < header.length; i += 1) {
          offset = readItem(bytes, offset).end;
          offset = readItem(bytes, offset).end;
        }
      }
      break;
    case 6:
      offset = readItem(bytes, offset).end;
      break;
    default:
      throw new Error(`unsupported CBOR major type ${header.major}`);
  }

  if (offset > bytes.length) {
    throw new Error("truncated CBOR item");
  }
  return { start, end: offset };
}

function readHeader(bytes, offset) {
  if (offset >= bytes.length) {
    throw new Error("truncated CBOR header");
  }

  const first = bytes[offset];
  const major = first >> 5;
  const ai = first & 0x1f;
  offset += 1;

  if (ai < 24) {
    return { major, length: ai, offset, indefinite: false };
  }

  const widths = { 24: 1, 25: 2, 26: 4, 27: 8 };
  const width = widths[ai];
  if (width) {
    if (offset + width > bytes.length) {
      throw new Error("truncated CBOR scalar");
    }
    if (major === 0 || major === 1 || major === 6 || major === 7) {
      return { major, length: 0, offset: offset + width, indefinite: false };
    }
    return {
      major,
      length: readUInt(bytes, offset, width),
      offset: offset + width,
      indefinite: false,
    };
  }
  if (ai === 31 && (major === 2 || major === 3 || major === 4 || major === 5)) {
    return { major, length: 0, offset, indefinite: true };
  }
  throw new Error(`unsupported CBOR additional info ${ai}`);
}

function readUInt(bytes, offset, width) {
  if (offset + width > bytes.length) {
    throw new Error("truncated CBOR integer");
  }
  let value = 0;
  for (let i = 0; i < width; i += 1) {
    value = value * 256 + bytes[offset + i];
  }
  if (!Number.isSafeInteger(value)) {
    throw new Error("CBOR integer exceeds JavaScript safe range");
  }
  return value;
}

function skipIndefiniteChunks(bytes, offset) {
  while (bytes[offset] !== 0xff) {
    const chunk = readItem(bytes, offset);
    offset = chunk.end;
  }
  return offset + 1;
}
