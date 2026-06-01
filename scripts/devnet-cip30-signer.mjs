import { execFile } from "node:child_process";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

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
