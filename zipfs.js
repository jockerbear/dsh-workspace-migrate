// zipfs.js — minimal zero-dependency ZIP reader/writer for the migration plugin.
// Transport envelope only: the bundle format inside the archive is unchanged.
// Writer: store/deflate entries with UTF-8 names. Reader: central-directory
// driven, CRC-checked, and hardened against zip-slip (entry paths are forced
// under the destination root or refused).

import { promises as fsp } from 'node:fs'
import { join, resolve, sep, dirname } from 'node:path'
import { deflateRawSync, inflateRawSync } from 'node:zlib'

// ── CRC32 (table-driven, zip-compatible) ───────────────────────────────────

const CRC_TABLE = (() => {
  const table = new Uint32Array(256)
  for (let n = 0; n < 256; n += 1) {
    let c = n
    for (let k = 0; k < 8; k += 1) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1
    table[n] = c >>> 0
  }
  return table
})()

function crc32(buffer) {
  let c = 0xffffffff
  for (let i = 0; i < buffer.length; i += 1) c = CRC_TABLE[(c ^ buffer[i]) & 0xff] ^ (c >>> 8)
  return (c ^ 0xffffffff) >>> 0
}

// ── DOS time ───────────────────────────────────────────────────────────────

function dosDateTime(date = new Date()) {
  const time = (date.getHours() << 11) | (date.getMinutes() << 5) | (Math.floor(date.getSeconds() / 2) & 0x1f)
  const day = ((date.getFullYear() - 1980) << 9) | ((date.getMonth() + 1) << 5) | date.getDate()
  return { time, date: day }
}

// ── shared constants ───────────────────────────────────────────────────────

const SIG_LOCAL = 0x04034b50
const SIG_CENTRAL = 0x02014b50
const SIG_EOCD = 0x06054b50
const FLAG_UTF8 = 0x0800
const MAX_ENTRY_BYTES = 2 * 1024 * 1024 * 1024 // refuse >2GB entries (zip-bomb guard)

// Win32 device names are resolved before the filesystem sees them; an entry
// named e.g. "CON" would write to the console device instead of a file.
const WINDOWS_RESERVED_NAME = /^(con|prn|aux|nul|com[1-9]|lpt[1-9])$/i

// Payload that is already compressed gains nothing from deflate.
const STORED_EXT = new Set(['.zst', '.zip', '.png', '.jpg', '.jpeg', '.gif', '.webp', '.gz', '.br', '.woff', '.woff2'])

async function walkFiles(root, prefix = '') {
  const out = []
  const entries = await fsp.readdir(join(root, prefix), { withFileTypes: true })
  for (const entry of entries) {
    const rel = prefix ? `${prefix}/${entry.name}` : entry.name
    if (entry.isDirectory()) out.push(...(await walkFiles(root, rel)))
    else if (entry.isFile()) out.push(rel)
  }
  return out
}

function isStoredName(name) {
  const dot = name.lastIndexOf('.')
  return dot >= 0 && STORED_EXT.has(name.slice(dot).toLowerCase())
}

// ── writer ─────────────────────────────────────────────────────────────────

/**
 * Compress `srcDir` into `zipPath`. Returns { files, bytes } for the payload.
 * The caller owns atomic placement of the finished archive (tmp+rename).
 */
export async function zipDirectory(srcDir, zipPath) {
  const names = (await walkFiles(srcDir)).sort()
  const locals = []
  const centrals = []
  let offset = 0
  for (const name of names) {
    const content = await fsp.readFile(join(srcDir, ...name.split('/')))
    const crc = crc32(content)
    const useDeflate = !isStoredName(name)
    let payload = content
    let method = 0
    if (useDeflate) {
      const packed = deflateRawSync(content, { level: 6 })
      if (packed.length < content.length) {
        payload = packed
        method = 8
      }
    }
    const nameBuf = Buffer.from(name, 'utf8')
    const { time, date } = dosDateTime()
    const local = Buffer.alloc(30)
    local.writeUInt32LE(SIG_LOCAL, 0)
    local.writeUInt16LE(20, 4)
    local.writeUInt16LE(FLAG_UTF8, 6)
    local.writeUInt16LE(method, 8)
    local.writeUInt16LE(time, 10)
    local.writeUInt16LE(date, 12)
    local.writeUInt32LE(crc, 14)
    local.writeUInt32LE(payload.length, 18)
    local.writeUInt32LE(content.length, 22)
    local.writeUInt16LE(nameBuf.length, 26)
    local.writeUInt16LE(0, 28)
    locals.push(local, nameBuf, payload)

    const central = Buffer.alloc(46)
    central.writeUInt32LE(SIG_CENTRAL, 0)
    central.writeUInt16LE(20, 4)
    central.writeUInt16LE(20, 6)
    central.writeUInt16LE(FLAG_UTF8, 8)
    central.writeUInt16LE(method, 10)
    central.writeUInt16LE(time, 12)
    central.writeUInt16LE(date, 14)
    central.writeUInt32LE(crc, 16)
    central.writeUInt32LE(payload.length, 20)
    central.writeUInt32LE(content.length, 24)
    central.writeUInt16LE(nameBuf.length, 28)
    central.writeUInt16LE(0, 30)
    central.writeUInt16LE(0, 32)
    central.writeUInt16LE(0, 34)
    central.writeUInt32LE(0, 36)
    central.writeUInt32LE(offset, 42)
    centrals.push(central, nameBuf)
    offset += local.length + nameBuf.length + payload.length
  }
  const centralBuf = Buffer.concat(centrals)
  const eocd = Buffer.alloc(22)
  eocd.writeUInt32LE(SIG_EOCD, 0)
  eocd.writeUInt16LE(0, 4)
  eocd.writeUInt16LE(0, 6)
  eocd.writeUInt16LE(names.length, 8)
  eocd.writeUInt16LE(names.length, 10)
  eocd.writeUInt32LE(centralBuf.length, 12)
  eocd.writeUInt32LE(offset, 16)
  eocd.writeUInt16LE(0, 20)
  await fsp.writeFile(zipPath, Buffer.concat([...locals, centralBuf, eocd]))
  let bytes = 0
  for (const name of names) bytes += (await fsp.stat(join(srcDir, ...name.split('/')))).size
  return { files: names.length, bytes }
}

// ── reader ─────────────────────────────────────────────────────────────────

function readCentralDirectory(buffer) {
  if (buffer.length < 22) throw new Error('zip 结构损坏：文件过短')
  const scanFrom = Math.max(0, buffer.length - (22 + 0xffff))
  let eocdAt = -1
  for (let i = buffer.length - 22; i >= scanFrom; i -= 1) {
    if (buffer.readUInt32LE(i) === SIG_EOCD) {
      eocdAt = i
      break
    }
  }
  if (eocdAt < 0) throw new Error('zip 结构损坏：找不到中央目录结尾')
  const count = buffer.readUInt16LE(eocdAt + 10)
  const cdSize = buffer.readUInt32LE(eocdAt + 12)
  let cursor = buffer.readUInt32LE(eocdAt + 16)
  // zip64 sentinels mean the real values live in the zip64 record — this
  // reader has no zip64 support, so say so instead of misreading 0xffff as a
  // literal count (which surfaced as a confusing "central directory invalid").
  if (count === 0xffff || cdSize === 0xffffffff || cursor === 0xffffffff) {
    throw new Error('zip 包使用了 zip64 扩展（条目数或体积超过 4GB/65535），当前实现不支持；请改用 pack:false 的文件夹形态')
  }
  const entries = []
  const seenNames = new Set()
  for (let index = 0; index < count; index += 1) {
    if (cursor + 46 > buffer.length || buffer.readUInt32LE(cursor) !== SIG_CENTRAL) {
      throw new Error('zip 结构损坏：中央目录条目非法')
    }
    const method = buffer.readUInt16LE(cursor + 10)
    const crc = buffer.readUInt32LE(cursor + 16)
    const csize = buffer.readUInt32LE(cursor + 20)
    const usize = buffer.readUInt32LE(cursor + 24)
    const nameLen = buffer.readUInt16LE(cursor + 28)
    const extraLen = buffer.readUInt16LE(cursor + 30)
    const commentLen = buffer.readUInt16LE(cursor + 32)
    const localOffset = buffer.readUInt32LE(cursor + 42)
    if (cursor + 46 + nameLen + extraLen + commentLen > buffer.length) {
      throw new Error('zip 结构损坏：中央目录条目越界')
    }
    const name = buffer.toString('utf8', cursor + 46, cursor + 46 + nameLen)
    // Duplicate / case-colliding names are ambiguous: zipReadEntry takes the
    // first match while extraction lets the last one win, so different
    // readers can see different payloads for one archive. Refuse outright.
    const collisionKey = name.toLowerCase()
    if (seenNames.has(collisionKey)) throw new Error(`zip 条目名重复或大小写冲突：${name}`)
    seenNames.add(collisionKey)
    entries.push({ name, method, crc, csize, usize, localOffset })
    cursor += 46 + nameLen + extraLen + commentLen
  }
  return entries
}

function entryPayload(buffer, entry) {
  const at = entry.localOffset
  if (at + 30 > buffer.length || buffer.readUInt32LE(at) !== SIG_LOCAL) {
    throw new Error(`zip 结构损坏：条目头部非法（${entry.name}）`)
  }
  const nameLen = buffer.readUInt16LE(at + 26)
  const extraLen = buffer.readUInt16LE(at + 28)
  const start = at + 30 + nameLen + extraLen
  const payload = buffer.subarray(start, start + entry.csize)
  if (payload.length !== entry.csize) throw new Error(`zip 结构损坏：条目数据缺失（${entry.name}）`)
  if (entry.usize > MAX_ENTRY_BYTES) throw new Error(`zip 条目过大，拒绝解压：${entry.name}`)
  let content
  if (entry.method === 0) content = Buffer.from(payload)
  else if (entry.method === 8) content = inflateRawSync(payload)
  else throw new Error(`zip 条目压缩方式不支持（${entry.method}）：${entry.name}`)
  if (content.length !== entry.usize || crc32(content) !== entry.crc) {
    throw new Error(`zip 条目校验失败（内容损坏）：${entry.name}`)
  }
  return content
}

/**
 * Entry path must stay under destDir; refuse traversal/absolute names (zip-slip).
 * Names are also checked for portability: Windows silently maps reserved DOS
 * device names, strips trailing dots/spaces and rejects `< > : " | ? *`, so an
 * archive entry spelled that way could land on a different file (or a device)
 * than the sealed one. Refusing is fail-closed and keeps extraction identical
 * to the tree that was hashed.
 */
function safeEntryTarget(destDir, name) {
  const rel = String(name).replace(/\\/g, '/')
  if (!rel || rel.includes('\0')) throw new Error(`zip 条目路径非法：${name}`)
  if (rel.startsWith('/') || /^[A-Za-z]:/.test(rel)) throw new Error(`zip 条目路径非法：${name}`)
  const segments = rel.split('/')
  if (segments.some((segment) => segment === '' || segment === '.' || segment === '..')) {
    throw new Error(`zip 条目路径非法：${name}`)
  }
  for (const segment of segments) {
    if (/[<>:"|?*\x00-\x1f]/.test(segment)) throw new Error(`zip 条目路径非法（含 Windows 非法字符）：${name}`)
    if (/[. ]$/.test(segment)) throw new Error(`zip 条目路径非法（Windows 会去掉末尾的句点或空格）：${name}`)
    if (WINDOWS_RESERVED_NAME.test(segment.split('.')[0])) {
      throw new Error(`zip 条目路径非法（Windows 保留设备名）：${name}`)
    }
  }
  const root = resolve(destDir)
  const target = resolve(root, segments.join(sep))
  if (target !== root && !target.startsWith(root + sep)) throw new Error(`zip 条目越界：${name}`)
  return target
}

/** Extract `zipPath` under `destDir`. Returns { files, bytes }. Throws on any violation. */
export async function zipExtract(zipPath, destDir) {
  const buffer = await fsp.readFile(zipPath)
  const entries = readCentralDirectory(buffer)
  let files = 0
  let bytes = 0
  for (const entry of entries) {
    if (entry.name.endsWith('/')) continue // directory marker
    const target = safeEntryTarget(destDir, entry.name)
    const content = entryPayload(buffer, entry)
    await fsp.mkdir(dirname(target), { recursive: true })
    await fsp.writeFile(target, content)
    files += 1
    bytes += content.length
  }
  return { files, bytes }
}

/** Read one entry's raw content (e.g. manifest.json) without a full extract. */
export async function zipReadEntry(zipPath, wanted) {
  const buffer = await fsp.readFile(zipPath)
  const entries = readCentralDirectory(buffer)
  const entry = entries.find((candidate) => candidate.name === wanted)
  if (!entry) return null
  return entryPayload(buffer, entry)
}

/** Entry count without extracting (status surfaces). */
export async function zipCountEntries(zipPath) {
  const buffer = await fsp.readFile(zipPath)
  return readCentralDirectory(buffer).filter((entry) => !entry.name.endsWith('/')).length
}
