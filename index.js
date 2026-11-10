import { promises as fsp, createReadStream } from 'node:fs'
import { join, resolve, basename, dirname, sep } from 'node:path'
import { homedir, hostname, platform, tmpdir } from 'node:os'
import { randomUUID, createHash } from 'node:crypto'
import { fileURLToPath } from 'node:url'
import { zipDirectory, zipExtract, zipReadEntry, zipCountEntries } from './zipfs.js'

/**
 * Host half of the migration plugin: exports the three durable layers of a
 * Harness install (workspace registry, session logs, project files) into one
 * portable folder, and replays that folder on another machine through the
 * public `sessionPersistence` / `workspaceRegistry` services.
 */

const ROUTE_PREFIX = '/api/dsh-migrate'
const BUNDLE_SCHEMA = 1
const BUNDLE_KIND = 'dsh-migration-bundle'
const DEFAULT_EXCLUDES = ['node_modules', '.git', '.venv', 'venv', '__pycache__', '.cache', '.pnpm-store', 'dist', '.next']
const MAX_TASKS = 20

export const name = 'dsh-migrate'
export const inject = ['sessionPersistence', 'workspaceRegistry', 'webServer', 'tools']

export function apply(ctx, config = {}) {
  const excludes = new Set(
    (Array.isArray(config.excludeDirs) && config.excludeDirs.length ? config.excludeDirs : DEFAULT_EXCLUDES)
      .map((entry) => String(entry).toLowerCase()),
  )
  // Fork patch P3: profile-configured default export root, so both machines
  // agree on the sync-drive destination without hand-filling the UI field.
  // Falls back to ~/.dsh/migrations exactly as upstream did.
  const defaultOutDir =
    typeof config.defaultOutDir === 'string' && config.defaultOutDir.trim()
      ? resolve(config.defaultOutDir.trim())
      : null
  const tasks = new Map()
  // Rollback may only remove what THIS process created, so a stray or forged
  // request can never delete an unrelated session or project registration.
  const createdWorkspaces = new Set()
  const createdSessions = new Set()

  // ── small helpers ────────────────────────────────────────────────────────

  function dshHome() {
    const fromEnv = process.env.DSH_HOME
    if (fromEnv && fromEnv.trim().length > 0) return resolve(fromEnv)
    return join(homedir(), '.dsh')
  }

  async function pathExists(path) {
    try {
      await fsp.access(path)
      return true
    } catch {
      return false
    }
  }

  /**
   * Retry a filesystem op briefly: sync-drive clients and antivirus scanners
   * routinely hold just-written files for a moment (EBUSY/EPERM/EIO), which
   * must not fail a finished export. Non-transient errors still throw fast.
   */
  async function retryFs(operation, label, attempts = 4) {
    let lastError
    for (let attempt = 0; attempt < attempts; attempt += 1) {
      try {
        return await operation()
      } catch (error) {
        lastError = error
        const code = error && typeof error === 'object' ? error.code : null
        const transient = code === 'EBUSY' || code === 'EPERM' || code === 'EIO' || code === 'ENOTEMPTY' || code === 'EMFILE'
        if (!transient || attempt === attempts - 1) throw error
        await new Promise((resolveWait) => setTimeout(resolveWait, 200 * (attempt + 1)))
      }
    }
    throw lastError
  }

  // ── zip transport envelope (P5) ──────────────────────────────────────────
  // The bundle format is unchanged; a packed bundle simply travels as one
  // .zip on the sync drive instead of a tree of small files.

  function isZipPath(path) {
    return /\.zip$/i.test(String(path ?? '').trim())
  }

  /** Unique local staging dir (always on the local disk, never the sync drive). */
  function newStaging(prefix) {
    return join(tmpdir(), `dsh-migrate-${prefix}`, `${stamp()}-${randomUUID().slice(0, 8)}`)
  }

  async function dropStaging(staging) {
    try {
      await retryFs(() => fsp.rm(staging, { recursive: true, force: true }), 'staging')
    } catch {
      /* staging cleanup is best effort; TEMP is self-healing */
    }
  }

  /**
   * Run `fn(dir)` against a folder bundle as-is, or against a .zip envelope
   * extracted to a local sandbox that is cleaned afterwards. Zip entry paths
   * are contained by zipfs (zip-slip refused) before anything is written.
   */
  async function withBundleDir(bundlePath, fn) {
    const resolved = resolve(String(bundlePath))
    if (!isZipPath(resolved)) return fn(resolved)
    const staging = newStaging('unpack')
    await fsp.mkdir(staging, { recursive: true })
    try {
      await zipExtract(resolved, staging)
      return await fn(staging)
    } finally {
      await dropStaging(staging)
    }
  }

  /** Node's built-in Zstandard, when this runtime has it. */
  async function loadZstd() {
    try {
      const zlib = await import('node:zlib')
      if (typeof zlib.zstdCompressSync === 'function' && typeof zlib.zstdDecompressSync === 'function') return zlib
    } catch {
      /* older runtime: session files stay plain JSON */
    }
    return null
  }

  function safeName(id) {
    return String(id).replace(/[^A-Za-z0-9._-]+/g, '_')
  }

  // Fork audit fix: bundle-supplied ids and relative keys must never steer a
  // filesystem operation outside its base directory (SEC-01/03/05).
  function isSafePathSegment(value) {
    const text = String(value)
    return /^[A-Za-z0-9._-]{1,120}$/.test(text) && text !== '.' && text !== '..'
  }

  /** Validate a bundle-supplied relative path (nested allowed); reject traversal. */
  function safeRelPath(relative) {
    const rel = String(relative)
    if (!rel || rel.includes('\0')) throw new Error(`非法相对路径：${rel}`)
    if (/^[A-Za-z]:/.test(rel) || rel.startsWith('/') || rel.startsWith('\\')) throw new Error(`非法相对路径：${rel}`)
    const segments = rel.split(/[\\/]/)
    if (segments.some((segment) => segment === '' || segment === '.' || segment === '..')) {
      throw new Error(`非法相对路径：${rel}`)
    }
    return segments.join('/')
  }

  /** Join a bundle-supplied relative path under base and assert containment. */
  function safeJoinUnder(base, relative) {
    const root = resolve(base)
    const target = resolve(root, safeRelPath(relative))
    if (target !== root && !target.startsWith(root + sep)) throw new Error(`路径越界：${relative}`)
    return target
  }

  /** Existence probe for a bundle-supplied relative path; false when invalid. */
  async function safePathExists(base, relative) {
    try {
      return await pathExists(safeJoinUnder(base, relative))
    } catch {
      return false
    }
  }

  function stamp() {
    // Local wall-clock (what the operator sees) + seconds + random suffix, so
    // two exports in the same minute can never collide on one directory.
    const now = new Date()
    const pad = (value) => String(value).padStart(2, '0')
    const local = `${now.getFullYear()}${pad(now.getMonth() + 1)}${pad(now.getDate())}-${pad(now.getHours())}${pad(now.getMinutes())}${pad(now.getSeconds())}`
    return `${local}-${randomUUID().slice(0, 4)}`
  }

  function normalKey(path) {
    const text = String(path)
    return process.platform === 'win32' ? text.toLowerCase().replace(/\//g, '\\') : text
  }

  function trimSep(path) {
    return String(path).replace(/[\\/]+$/, '')
  }

  function mapPath(path, pairs) {
    if (!path) return path
    const key = normalKey(path)
    for (const pair of pairs) {
      const fromKey = normalKey(trimSep(pair.from))
      const toKey = trimSep(pair.to)
      if (key === fromKey) return toKey
      if (key.startsWith(fromKey + '\\') || key.startsWith(fromKey + '/')) {
        return toKey + String(path).slice(trimSep(pair.from).length)
      }
    }
    return path
  }

  async function dirSize(path) {
    let total = 0
    let stack = [path]
    while (stack.length > 0) {
      const current = stack.pop()
      let entries
      try {
        entries = await fsp.readdir(current, { withFileTypes: true })
      } catch {
        continue
      }
      for (const entry of entries) {
        const full = join(current, entry.name)
        if (entry.isDirectory()) stack.push(full)
        else if (entry.isFile()) {
          try {
            total += (await fsp.stat(full)).size
          } catch {
            /* unreadable entry does not fail the report */
          }
        }
      }
    }
    return total
  }

  /**
   * Snapshot a directory tree as `relative/path -> [size, mtimeMs]`, skipping
   * the rebuildable directories in `skipNames`. Keys use forward slashes so a
   * bundle written on Windows can be updated from the same relative spelling.
   */
  async function walkFiles(root, skipNames) {
    const result = new Map()
    const stack = ['']
    while (stack.length > 0) {
      const relative = stack.pop()
      const absolute = relative ? join(root, relative) : root
      let entries
      try {
        entries = await fsp.readdir(absolute, { withFileTypes: true })
      } catch {
        continue
      }
      for (const entry of entries) {
        const child = relative ? `${relative}/${entry.name}` : entry.name
        if (entry.isDirectory()) {
          if (skipNames.has(entry.name.toLowerCase())) continue
          stack.push(child)
          continue
        }
        if (!entry.isFile()) continue
        try {
          const info = await fsp.stat(join(root, child))
          result.set(child, [info.size, Math.round(info.mtimeMs)])
        } catch {
          /* an unreadable file is left out of the state and copied again next time */
        }
      }
    }
    return result
  }

  /** A filesystem-safe single path segment derived from a project title. */
  function safeSegment(value, fallback) {
    const cleaned = String(value ?? '')
      .replace(/[\\/:*?"<>|\x00-\x1f]/g, ' ')
      .replace(/\s+/g, ' ')
      .trim()
      .replace(/[. ]+$/g, '')
    return cleaned.length > 0 ? cleaned.slice(0, 80) : String(fallback ?? 'workspace')
  }

  /**
   * Normalize a user-supplied target path by its own style, so a POSIX root
   * such as `/home/evan` never keeps Windows separators pasted from a bundle
   * that came from Windows (and a `D:\...` root keeps backslashes on Windows).
   */
  function normalizeTargetPath(value) {
    const text = String(value ?? '').trim()
    if (text.startsWith('/')) return text.replace(/\\/g, '/')
    if (/^[A-Za-z]:[\\/]/.test(text)) return text.replace(/\//g, '\\')
    return text
  }

  function escapeRegExp(text) {
    return String(text).replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
  }

  /**
   * Rewrite absolute paths of the remapped roots inside one JSON value, in
   * place. This is what keeps a restored conversation usable: tool calls and
   * results still name the old machine's paths, and the model would chase
   * files that no longer exist there.
   */
  function rewritePaths(value, pairs, stats, depth = 0) {
    if (depth > 12) return value
    if (typeof value === 'string') {
      let text = value
      for (const pair of pairs) {
        if (pair.from === pair.to) continue
        const variants = [
          [pair.from, pair.to],
          [String(pair.from).replace(/\\/g, '/'), String(pair.to).replace(/\\/g, '/')],
        ]
        for (const [from, to] of variants) {
          if (!from || from.length < 4) continue
          const pattern = new RegExp(escapeRegExp(from), 'gi')
          text = text.replace(pattern, () => {
            stats.count += 1
            return to
          })
        }
      }
      return text
    }
    if (Array.isArray(value)) {
      for (let index = 0; index < value.length; index += 1) value[index] = rewritePaths(value[index], pairs, stats, depth + 1)
      return value
    }
    if (value && typeof value === 'object') {
      for (const key of Object.keys(value)) value[key] = rewritePaths(value[key], pairs, stats, depth + 1)
      return value
    }
    return value
  }

  function isUnderTargets(path, targets) {
    const key = normalKey(path)
    return targets.some((entry) => {
      const root = normalKey(entry.target)
      return key === root || key.startsWith(root + '\\') || key.startsWith(root + '/')
    })
  }

  /** Read the registry's global archive/pin sets from the durable unit file. */
  async function readRegistrySets(home) {
    try {
      const text = await fsp.readFile(join(home, 'storages', 'workspace.json'), 'utf8')
      const parsed = JSON.parse(text)
      const global = parsed.global ?? {}
      return {
        archived: Array.isArray(global.archivedSessionIds) ? global.archivedSessionIds.map(String) : [],
        pinned: Array.isArray(global.pinnedSessionIds) ? global.pinnedSessionIds.map(String) : [],
        order: Array.isArray(global.workspaceIds) ? global.workspaceIds.map(String) : [],
      }
    } catch {
      return { archived: [], pinned: [], order: [] }
    }
  }

  async function listBundles() {
    // Fork patch P3: scan the configured default export root too, so status
    // reflects bundles living on the sync drive, not only ~/.dsh/migrations.
    const roots = []
    if (defaultOutDir) roots.push(defaultOutDir)
    roots.push(join(dshHome(), 'migrations'))
    const bundles = []
    const seen = new Set()
    for (const root of roots) {
      const rootKey = normalKey(root)
      if (seen.has(rootKey)) continue
      seen.add(rootKey)
      let entries
      try {
        entries = await fsp.readdir(root, { withFileTypes: true })
      } catch {
        continue
      }
      for (const entry of entries) {
        // P5: packed bundles travel as one .zip; read its manifest from the
        // central directory without extracting.
        if (entry.isFile() && isZipPath(entry.name)) {
          const path = join(root, entry.name)
          if (seen.has(normalKey(path))) continue
          seen.add(normalKey(path))
          try {
            const text = await zipReadEntry(path, 'manifest.json')
            if (!text) continue
            const manifest = JSON.parse(text.toString('utf8'))
            if (manifest.kind !== BUNDLE_KIND) continue
            bundles.push({
              path,
              name: entry.name,
              packed: true,
              createdAt: manifest.createdAt ?? null,
              formatVersion: manifest.formatVersion ?? null,
              workspaces: Array.isArray(manifest.workspaces) ? manifest.workspaces.length : 0,
              sessions: Array.isArray(manifest.sessions) ? manifest.sessions.length : 0,
              sourceHost: manifest.source?.host ?? null,
              bytes: manifest.bytes ?? null,
            })
          } catch {
            continue
          }
          continue
        }
        if (!entry.isDirectory()) continue
        const path = join(root, entry.name)
        if (seen.has(normalKey(path))) continue
        seen.add(normalKey(path))
        try {
          const manifest = JSON.parse(await fsp.readFile(join(path, 'manifest.json'), 'utf8'))
          if (manifest.kind !== BUNDLE_KIND) continue
          bundles.push({
            path,
            name: entry.name,
            createdAt: manifest.createdAt ?? null,
            formatVersion: manifest.formatVersion ?? null,
            workspaces: Array.isArray(manifest.workspaces) ? manifest.workspaces.length : 0,
            sessions: Array.isArray(manifest.sessions) ? manifest.sessions.length : 0,
            sourceHost: manifest.source?.host ?? null,
            bytes: manifest.bytes ?? null,
          })
        } catch {
          // Fork audit fix (SEC-09): an interrupted export leaves a bundle
          // directory without a manifest (often with a .partial marker).
          // Surface it as incomplete instead of hiding it from the operator.
          if (await pathExists(join(path, '.partial'))) {
            bundles.push({ path, name: entry.name, incomplete: true, createdAt: null, sessions: 0, workspaces: 0 })
          }
          continue
        }
      }
    }
    bundles.sort((a, b) => String(b.createdAt ?? '').localeCompare(String(a.createdAt ?? '')))
    return bundles
  }

  // ── inventory ────────────────────────────────────────────────────────────

  async function inventory() {
    const home = dshHome()
    const workspaces = ctx.workspaceRegistry.list().map((workspace) => ({
      id: workspace.id,
      path: workspace.path,
      title: workspace.title,
      createdAt: workspace.createdAt,
      sessionIds: [...workspace.sessionIds].map(String),
    }))
    let sessions = []
    try {
      const snapshots = await ctx.sessionPersistence.list()
      sessions = snapshots.map((snapshot) => ({
        id: String(snapshot.header.id),
        cwd: snapshot.header.cwd ?? null,
        createdAt: snapshot.header.createdAt ?? null,
        parent: snapshot.header.parentSession ? String(snapshot.header.parentSession) : null,
        seeded: Boolean(snapshot.header.isSeeded),
        agentPreset: snapshot.header.agentPreset ?? null,
        bytes: snapshot.sizeBytes ?? null,
        format: snapshot.formatStatus ?? null,
      }))
    } catch (error) {
      sessions = []
    }
    const sets = await readRegistrySets(home)
    const attachmentsRoot = join(home, 'attachments')
    return {
      home,
      migrationsDir: join(home, 'migrations'),
      codeVersion: 10,
      workspaces,
      sessions,
      archived: sets.archived,
      pinned: sets.pinned,
      hasAttachments: await pathExists(attachmentsRoot),
      bundles: await listBundles(),
      excludes: [...excludes],
    }
  }

  async function readManifest(dir) {
    const manifest = JSON.parse(await fsp.readFile(join(dir, 'manifest.json'), 'utf8'))
    if (manifest.kind !== BUNDLE_KIND) throw new Error('这不是迁移包：manifest.kind 不匹配')
    if (Number(manifest.schema) > BUNDLE_SCHEMA) throw new Error(`迁移包结构版本 ${manifest.schema} 高于插件支持的 ${BUNDLE_SCHEMA}`)
    return manifest
  }

  async function preview(bundlePath) {
    if (!bundlePath) throw new Error('缺少迁移包路径')
    return withBundleDir(bundlePath, (dir) => previewInDir(dir))
  }

  async function previewInDir(dir) {
    const manifest = await readManifest(dir)
    let workspaces = []
    try {
      const doc = JSON.parse(await fsp.readFile(join(dir, 'workspaces.json'), 'utf8'))
      workspaces = (doc.workspaces ?? []).map((workspace) => ({
        id: workspace.id,
        path: workspace.path,
        title: workspace.title,
        sessions: Array.isArray(workspace.sessionIds) ? workspace.sessionIds.length : 0,
        hasFiles: true,
      }))
    } catch {
      workspaces = []
    }
    const sizes = {}
    for (const workspace of workspaces) {
      const files = join(dir, 'files', safeName(workspace.id))
      sizes[workspace.id] = (await pathExists(files)) ? await dirSize(files) : 0
    }
    return {
      path: dir,
      createdAt: manifest.createdAt ?? null,
      formatVersion: manifest.formatVersion ?? null,
      source: manifest.source ?? {},
      excludes: manifest.excludes ?? [],
      sessions: Array.isArray(manifest.sessions) ? manifest.sessions.length : 0,
      workspaces: workspaces.map((workspace) => ({ ...workspace, bytes: sizes[workspace.id] ?? 0 })),
      attachments: await pathExists(join(dir, 'attachments')),
      bytes: manifest.bytes ?? null,
    }
  }

  // ── task runner ──────────────────────────────────────────────────────────

  function startTask(kind) {
    const task = {
      id: randomUUID(),
      kind,
      phase: 'running',
      message: '准备中…',
      done: 0,
      total: 0,
      startedAt: Date.now(),
      finishedAt: null,
      result: null,
      error: null,
    }
    tasks.set(task.id, task)
    while (tasks.size > MAX_TASKS) {
      const oldest = tasks.keys().next().value
      if (oldest === undefined) break
      tasks.delete(oldest)
    }
    return task
  }

  function fail(task, error) {
    task.phase = 'failed'
    task.error = error instanceof Error ? error.message : String(error)
    task.message = task.error
    task.finishedAt = Date.now()
  }

  function succeed(task, result) {
    task.phase = 'done'
    task.result = result
    task.message = '完成'
    task.finishedAt = Date.now()
  }

  function taskView(id) {
    const task = tasks.get(String(id))
    if (!task) return { error: '任务不存在或已过期' }
    return task
  }

  // ── fork patch P2: bundle sealing (export receipt + sha256 sidecar) ─────

  /** Root-level governance metadata that must not be part of the payload hash. */
  function isSealExcluded(relative) {
    if (relative.includes('/')) return false
    return (
      relative === 'dir.sha256' ||
      relative === '.partial' ||
      (relative.startsWith('ACK-') && relative.endsWith('.json')) ||
      relative.startsWith('RETIRED')
    )
  }

  function sha256File(path) {
    return new Promise((resolveHash, reject) => {
      const hash = createHash('sha256')
      const stream = createReadStream(path)
      stream.on('error', reject)
      stream.on('data', (chunk) => hash.update(chunk))
      stream.on('end', () => resolveHash(hash.digest('hex')))
    })
  }

  /**
   * Fork audit fix (SEC-08): verify a bundle's `dir.sha256` payload hashes so
   * the import side can refuse a bundle that changed after it was sealed.
   * Same sha256sum semantics as sealBundle and migrate-govern.ps1.
   */
  async function verifySidecar(dir) {
    let text
    try {
      text = await fsp.readFile(join(dir, 'dir.sha256'), 'utf8')
    } catch {
      return { checked: false, match: false, differences: ['缺少 dir.sha256 边车文件'] }
    }
    const differences = []
    let checked = 0
    for (const line of text.split('\n')) {
      if (!line.trim()) continue
      const match = /^([0-9a-f]{64})  (.+)$/.exec(line)
      if (!match) {
        differences.push(`边车行格式非法：${line.slice(0, 80)}`)
        continue
      }
      const [, expected, relative] = match
      let target
      try {
        target = safeJoinUnder(dir, relative)
      } catch {
        differences.push(`边车路径越界：${relative}`)
        continue
      }
      checked += 1
      if (!(await pathExists(target))) {
        differences.push(`包内缺少文件：${relative}`)
        continue
      }
      const actual = await sha256File(target)
      if (actual !== expected) differences.push(`内容不匹配：${relative}`)
    }
    return { checked, match: differences.length === 0, differences: differences.slice(0, 20) }
  }

  /**
   * Fork patch P2: seal a finished bundle. The export receipt is written
   * first (so the sidecar covers it), then every payload file is hashed into
   * `dir.sha256` in sha256sum format — `<hex>  <rel/path>`, LF line endings,
   * ordinal path order — byte-compatible with `scripts/migrate-govern.ps1`,
   * which the target machine runs to verify the bundle before importing.
   * A seal failure never fails the export: the script can re-seal later.
   */
  async function sealBundle(dir, stats) {
    let version = null
    try {
      const pkg = JSON.parse(
        await fsp.readFile(join(dirname(fileURLToPath(import.meta.url)), 'package.json'), 'utf8'),
      )
      version = pkg.version ?? null
    } catch {
      /* an unknown plugin version does not fail the seal */
    }
    const receiptName = `RECEIPT-${safeName(hostname())}-${new Date().toISOString().replace(/[:.]/g, '-')}.json`
    const receipt = {
      kind: 'dsh-migrate-export-receipt',
      schema: 1,
      createdAt: new Date().toISOString(),
      host: hostname(),
      bundle: basename(dir),
      mode: stats.mode,
      stats: {
        sessions: stats.sessions,
        sessionsReused: stats.sessionsReused,
        workspaces: stats.workspaces,
        bytes: stats.bytes,
        filesCopied: stats.filesCopied,
        filesUnchanged: stats.filesUnchanged,
        filesRemoved: stats.filesRemoved,
        bytesCopied: stats.bytesCopied,
      },
      plugin: { name: 'dsh-workspace-migrate', version },
    }
    await retryFs(() => fsp.writeFile(join(dir, receiptName), `${JSON.stringify(receipt, null, 2)}\n`, 'utf8'), receiptName)

    const files = []
    const stack = ['']
    while (stack.length > 0) {
      const relative = stack.pop()
      const absolute = relative ? join(dir, relative) : dir
      let entries
      try {
        entries = await fsp.readdir(absolute, { withFileTypes: true })
      } catch {
        continue
      }
      for (const entry of entries) {
        const child = relative ? `${relative}/${entry.name}` : entry.name
        if (entry.isDirectory()) {
          stack.push(child)
          continue
        }
        if (!entry.isFile() || isSealExcluded(child)) continue
        files.push(child)
      }
    }
    files.sort()
    const lines = []
    for (const relative of files) {
      lines.push(`${await sha256File(join(dir, relative))}  ${relative}`)
    }
    const sidecar = lines.length > 0 ? `${lines.join('\n')}\n` : ''
    await retryFs(() => fsp.writeFile(join(dir, 'dir.sha256'), sidecar, 'utf8'), 'dir.sha256')
    return {
      receipt: receiptName,
      sidecar: 'dir.sha256',
      files: files.length,
      sidecarSha256: createHash('sha256').update(sidecar, 'utf8').digest('hex'),
    }
  }

  // ── export ───────────────────────────────────────────────────────────────

  async function runExport(task, options) {
    const home = dshHome()
    // Fork patch P3: explicit outDir wins, then config.defaultOutDir, then ~/.dsh/migrations.
    const outRoot =
      options.outDir && String(options.outDir).trim()
        ? resolve(String(options.outDir))
        : (defaultOutDir ?? join(home, 'migrations'))
    const updating = options.mode === 'update' && options.bundlePath && String(options.bundlePath).trim().length > 0
    // P5 transport envelope: pack=true (default) delivers ONE .zip on outDir
    // and works in a local staging dir; pack=false keeps the folder bundle.
    // An update keeps the target's current form (zip→zip, folder→folder).
    const pack = options.pack !== false
    const dryRun = options.dryRun === true
    let dir
    let staging = null
    let finalZip = null
    let previousState = null
    let bundleWorkspaceIds = null
    if (updating) {
      const target = resolve(String(options.bundlePath))
      const zipTarget = isZipPath(target)
      finalZip = zipTarget ? target : null
      if (zipTarget && dryRun) {
        // Dry runs write nothing anywhere: read bundle metadata straight out
        // of the zip instead of extracting to staging. Reuse counts for zip
        // envelopes are computed at execution time.
        dir = newStaging('stage') // planned name only; never created
        const manifestText = await zipReadEntry(target, 'manifest.json')
        if (!manifestText) throw new Error('这个迁移包没有 manifest.json')
        const manifest = JSON.parse(manifestText.toString('utf8'))
        if (manifest.kind !== BUNDLE_KIND) throw new Error('这不是迁移包：manifest.kind 不匹配')
        if (Number(manifest.schema) > BUNDLE_SCHEMA) {
          throw new Error(`迁移包结构版本 ${manifest.schema} 高于插件支持的 ${BUNDLE_SCHEMA}`)
        }
        const stateText = await zipReadEntry(target, 'state.json')
        if (!stateText) throw new Error('这个迁移包没有增量状态（可能是旧版本导出的），请先做一次完整导出')
        previousState = JSON.parse(stateText.toString('utf8'))
        const workspacesText = await zipReadEntry(target, 'workspaces.json')
        if (workspacesText) {
          bundleWorkspaceIds = (JSON.parse(workspacesText.toString('utf8')).workspaces ?? []).map((workspace) => String(workspace.id))
        }
      } else {
        if (zipTarget) {
          // P5: update a zip envelope via local staging, then repack at the end.
          staging = newStaging('stage')
          await fsp.mkdir(staging, { recursive: true })
          await zipExtract(target, staging)
          dir = staging
        } else {
          dir = target
        }
        await readManifest(dir)
        try {
          previousState = JSON.parse(await fsp.readFile(join(dir, 'state.json'), 'utf8'))
        } catch {
          throw new Error('这个迁移包没有增量状态（可能是旧版本导出的），请先做一次完整导出')
        }
        try {
          const doc = JSON.parse(await fsp.readFile(join(dir, 'workspaces.json'), 'utf8'))
          bundleWorkspaceIds = (doc.workspaces ?? []).map((workspace) => String(workspace.id))
        } catch {
          bundleWorkspaceIds = null
        }
      }
    } else if (pack) {
      staging = newStaging('stage')
      dir = staging
      finalZip = join(outRoot, `dsh-migration-${stamp()}.zip`)
    } else {
      dir = join(outRoot, `dsh-migration-${stamp()}`)
    }

    // Fork patch P1: a dry run must not touch the filesystem or the live
    // library — skip the flush, the mkdir, and every write below.
    if (!dryRun) {
      task.message = '刷新会话日志…'
      await ctx.sessionPersistence.flush()
    }

    const snapshots = await ctx.sessionPersistence.list()
    const snapshotById = new Map(snapshots.map((snapshot) => [String(snapshot.header.id), snapshot]))
    const allWorkspaces = ctx.workspaceRegistry.list()
    // An update keeps the bundle's own workspace set: re-selecting everything
    // would silently pull unrelated projects into a bundle meant for a subset.
    const selected =
      Array.isArray(options.workspaceIds) && options.workspaceIds.length > 0
        ? allWorkspaces.filter((workspace) => options.workspaceIds.includes(String(workspace.id)))
        : updating && bundleWorkspaceIds
          ? allWorkspaces.filter((workspace) => bundleWorkspaceIds.includes(String(workspace.id)))
          : allWorkspaces

    const sessionIds = new Set()
    for (const workspace of selected) for (const id of workspace.sessionIds) sessionIds.add(String(id))

    let grew = true
    while (grew) {
      grew = false
      for (const snapshot of snapshots) {
        const parent = snapshot.header.parentSession
        const id = String(snapshot.header.id)
        if (parent && sessionIds.has(String(parent)) && !sessionIds.has(id)) {
          sessionIds.add(id)
          grew = true
        }
      }
    }

    const ordered = [...sessionIds]
      .filter((id) => snapshotById.has(id))
      .sort((a, b) => {
        const left = snapshotById.get(a).header
        const right = snapshotById.get(b).header
        const leftChild = left.parentSession ? 1 : 0
        const rightChild = right.parentSession ? 1 : 0
        return leftChild - rightChild || (left.createdAt ?? 0) - (right.createdAt ?? 0)
      })

    // Fork audit fix (AUD-02): never drop sessions silently. Every snapshot
    // outside the export scope is reported with a reason.
    const orderedSet = new Set(ordered)
    const sessionsUnexported = snapshots
      .map((snapshot) => String(snapshot.header.id))
      .filter((id) => !orderedSet.has(id))
      .map((id) => {
        const parent = snapshotById.get(id)?.header?.parentSession
        return {
          id,
          reason: parent ? '父会话不在导出范围' : '未挂载到所选工作区且无父会话链',
        }
      })

    // Fork patch P1: dry-run stops here with a full plan; nothing below runs.
    if (dryRun) {
      let wouldReuse = 0
      if (previousState) {
        for (const id of ordered) {
          const previous = previousState.sessions?.[id]
          const snapshot = snapshotById.get(id)
          const sourceBytes = typeof snapshot?.sizeBytes === 'number' ? snapshot.sizeBytes : null
          const sourceEvents = typeof snapshot?.eventCount === 'number' ? snapshot.eventCount : null
          if (
            previous &&
            previous.meta &&
            previous.meta.file &&
            sourceBytes !== null &&
            previous.sourceBytes === sourceBytes &&
            previous.sourceEvents === sourceEvents &&
            (await safePathExists(join(dir, 'sessions'), previous.meta.file))
          ) {
            wouldReuse += 1
          }
        }
      }
      const planWarnings = []
      if (sessionsUnexported.length > 0) {
        planWarnings.push(`${sessionsUnexported.length} 个会话不在导出范围（详见 sessionsUnexported），确认后仍可继续`)
      }
      if (finalZip && updating) {
        planWarnings.push('zip 增量的复用文件数在执行时计算（dry-run 不解压）')
      }
      return {
        dryRun: true,
        path: finalZip ?? dir,
        packed: Boolean(finalZip),
        outRoot,
        mode: updating ? 'update' : 'full',
        sessions: ordered.length,
        sessionsWouldReuse: wouldReuse,
        sessionsUnexported,
        workspaces: selected.length,
        workspaceIds: selected.map((workspace) => String(workspace.id)),
        includeFiles: Boolean(options.includeFiles),
        includeAttachments: Boolean(options.includeAttachments),
        warnings: planWarnings,
      }
    }

    // Fork audit fix (SEC-09): mark the bundle incomplete until manifest.json
    // lands, so an interrupted export is visible instead of masquerading.
    await fsp.mkdir(dir, { recursive: true })
    await fsp.writeFile(join(dir, '.partial'), `${new Date().toISOString()}\n`, 'utf8')

    await fsp.mkdir(join(dir, 'sessions'), { recursive: true })
    task.total = ordered.length + (options.includeFiles ? selected.length : 0) + 2
    const manifestSessions = []
    const includedIds = new Set()
    const sessionState = {}
    let bytes = 0
    let sessionsReused = 0

    const zstd = await loadZstd()
    const sessionsFailed = []
    for (const id of ordered) {
      const snapshot = snapshotById.get(id)
      const previous = previousState?.sessions?.[id]
      // Reuse only when the source log itself is unchanged: compare the
      // backend's own size/event count, never the packaged artifact size.
      const sourceBytes = typeof snapshot?.sizeBytes === 'number' ? snapshot.sizeBytes : null
      const sourceEvents = typeof snapshot?.eventCount === 'number' ? snapshot.eventCount : null
      if (
        previous &&
        previous.meta &&
        previous.meta.file &&
        sourceBytes !== null &&
        previous.sourceBytes === sourceBytes &&
        previous.sourceEvents === sourceEvents &&
        (await safePathExists(join(dir, 'sessions'), previous.meta.file))
      ) {
        includedIds.add(id)
        manifestSessions.push(previous.meta)
        sessionState[id] = previous
        sessionsReused += 1
        task.done += 1
        task.message = `复用未变化的会话 ${sessionsReused} 个`
        continue
      }
      // Fork audit fix (AUD-01): a corrupt session log must never abort the
      // whole export mid-write. Skip it, record the reason, keep going.
      try {
        const handle = await ctx.sessionPersistence.open(id, 'read')
        try {
          const { events } = await handle.read()
          const document = {
            header: handle.header,
            inheritedEventCount: handle.inheritedEventCount,
            events,
          }
          const text = JSON.stringify(document)
          const packed = zstd ? zstd.zstdCompressSync(Buffer.from(text, 'utf8')) : null
          const file = packed ? `${safeName(id)}.json.zst` : `${safeName(id)}.json`
          await retryFs(() => fsp.writeFile(join(dir, 'sessions', file), packed ?? text), `sessions/${file}`)
          bytes += packed ? packed.length : Buffer.byteLength(text)
          includedIds.add(id)
          const meta = {
            id,
            file,
            compressed: Boolean(packed),
            createdAt: handle.header.createdAt ?? null,
            cwd: handle.header.cwd ?? null,
            parent: handle.header.parentSession ? String(handle.header.parentSession) : null,
            seeded: Boolean(handle.header.isSeeded),
            inherited: handle.inheritedEventCount ?? 0,
            events: Array.isArray(events) ? events.length : 0,
          }
          manifestSessions.push(meta)
          sessionState[id] = {
            meta,
            sourceBytes,
            sourceEvents,
            bytes: packed ? packed.length : Buffer.byteLength(text),
            events: Array.isArray(events) ? events.length : 0,
          }
        } finally {
          await handle.close()
        }
      } catch (error) {
        sessionsFailed.push({ id, reason: error instanceof Error ? error.message : String(error) })
        task.done += 1
        task.message = `跳过读取失败的会话 ${sessionsFailed.length} 个…`
        continue
      }
      task.done += 1
      task.message = `已导出会话 ${task.done}/${ordered.length}`
    }

    const sets = await readRegistrySets(home)
    const workspaceDoc = {
      version: 1,
      order: selected.map((workspace) => String(workspace.id)),
      workspaces: selected.map((workspace) => ({
        id: String(workspace.id),
        path: workspace.path,
        title: workspace.title,
        createdAt: workspace.createdAt ?? null,
        sessionIds: [...workspace.sessionIds].map(String).filter((id) => includedIds.has(id)),
      })),
      archived: sets.archived.filter((id) => includedIds.has(id)),
      pinned: sets.pinned.filter((id) => includedIds.has(id)),
    }
    await fsp.writeFile(join(dir, 'workspaces.json'), JSON.stringify(workspaceDoc, null, 2))

    const fileWarnings = []
    // Guard against packaging the bundle into itself: compare against the
    // FINAL destination (zip location or folder bundle), never the staging dir.
    const outKey = normalKey(finalZip ? dirname(finalZip) : dir)
    const filesState = {}
    let filesCopied = 0
    let filesUnchanged = 0
    let filesRemoved = 0
    let bytesCopied = 0
    for (const workspace of selected) {
      if (!options.includeFiles) break
      const source = workspace.path
      if (!(await pathExists(source))) {
        fileWarnings.push(`跳过不存在的项目目录：${workspace.title}（${source}）`)
        task.done += 1
        continue
      }
      const sourceKey = normalKey(source)
      if (outKey === sourceKey || outKey.startsWith(sourceKey + '\\') || outKey.startsWith(sourceKey + '/')) {
        fileWarnings.push(`导出目录位于项目「${workspace.title}」内部，已跳过它的文件复制，避免把导出包复制进自己`)
        task.done += 1
        continue
      }
      const dest = join(dir, 'files', safeName(workspace.id))
      const current = await walkFiles(source, excludes)
      if (updating) {
        const previousFiles = previousState.files?.[workspace.id] ?? {}
        for (const [relative, info] of current) {
          const before = previousFiles[relative]
          let target
          try {
            target = safeJoinUnder(dest, relative)
          } catch {
            continue
          }
          if (before && before[0] === info[0] && before[1] === info[1] && (await pathExists(target))) {
            // Fork audit fix (SEC-06): reuse only when the packaged copy still
            // matches its source bytes — never trust size/mtime alone.
            const sourceHash = await sha256File(join(source, relative))
            const targetHash = await sha256File(target)
            if (sourceHash === targetHash) {
              filesUnchanged += 1
              continue
            }
          }
          await fsp.mkdir(dirname(target), { recursive: true })
          await fsp.copyFile(join(source, relative), target)
          filesCopied += 1
          bytesCopied += info[0]
        }
        for (const relative of Object.keys(previousFiles)) {
          if (current.has(relative)) continue
          // Fork audit fix (SEC-01): deletion keys come from the bundle's
          // state.json — refuse anything that would escape the bundle.
          let target
          try {
            target = safeJoinUnder(dest, relative)
          } catch {
            fileWarnings.push(`增量清理跳过非法路径键：${relative}`)
            continue
          }
          await fsp.rm(target, { force: true })
          filesRemoved += 1
        }
      } else {
        await fsp.cp(source, dest, {
          recursive: true,
          force: true,
          filter: (entry) => {
            if (normalKey(entry) === normalKey(source)) return true
            return !excludes.has(basename(entry).toLowerCase())
          },
        })
        filesCopied += current.size
        for (const info of current.values()) bytesCopied += info[0]
      }
      filesState[workspace.id] = Object.fromEntries(current)
      task.done += 1
      task.message = updating
        ? `增量更新：${workspace.title}（变更 ${filesCopied}，未变 ${filesUnchanged}）`
        : `已复制项目文件：${workspace.title}`
    }
    if (updating) {
      for (const oldId of Object.keys(previousState.files ?? {})) {
        if (filesState[oldId]) continue
        // Fork audit fix (SEC-01): ids come from state.json — validate before rm.
        if (!isSafePathSegment(oldId)) {
          fileWarnings.push(`增量清理跳过非法项目 id：${oldId}`)
          continue
        }
        await fsp.rm(join(dir, 'files', safeName(oldId)), { recursive: true, force: true })
      }
    }

    let attachmentsBytes = 0
    const attachmentsRoot = join(home, 'attachments')
    if (options.includeAttachments && (await pathExists(attachmentsRoot))) {
      await fsp.cp(attachmentsRoot, join(dir, 'attachments'), { recursive: true, force: true })
      attachmentsBytes = await dirSize(join(dir, 'attachments'))
    }

    const filesBytes = await dirSize(join(dir, 'files'))
    // Ship this plugin itself so the target machine can install it without a
    // separate copy; the bundle is useless there without the same plugin.
    // Fork audit fix (ARCH-01/04): copy an explicit install-file whitelist —
    // a path-substring filter once wiped the copy on npm-style installs, and
    // a filter-based copy leaked docs/ and stray files into every bundle.
    try {
      const here = dirname(fileURLToPath(import.meta.url))
      const pluginDest = join(dir, 'plugin', 'dsh-migrate')
      const installFiles = ['package.json', 'cordis.patch.yml', 'index.js', 'zipfs.js', 'client.js', 'icon.svg', 'LICENSE', 'README.md']
      await fsp.mkdir(pluginDest, { recursive: true })
      for (const name of installFiles) {
        const sourceFile = join(here, name)
        if (await pathExists(sourceFile)) await fsp.copyFile(sourceFile, join(pluginDest, name))
      }
      const localeSource = join(here, 'locale')
      if (await pathExists(localeSource)) {
        await fsp.mkdir(join(pluginDest, 'locale'), { recursive: true })
        for (const entry of await fsp.readdir(localeSource, { withFileTypes: true })) {
          if (entry.isFile() && entry.name.endsWith('.json')) {
            await fsp.copyFile(join(localeSource, entry.name), join(pluginDest, 'locale', entry.name))
          }
        }
      }
      if (!(await pathExists(join(pluginDest, 'package.json')))) {
        fileWarnings.push('插件自携带副本不完整：缺少 package.json，目标机将无法从迁移包安装插件')
      }
    } catch (error) {
      fileWarnings.push(
        `插件自携带副本失败（目标机需另行安装 dsh-workspace-migrate）：${error instanceof Error ? error.message : String(error)}`,
      )
    }
    const totalBytes = bytes + filesBytes + attachmentsBytes
    const manifest = {
      schema: BUNDLE_SCHEMA,
      kind: BUNDLE_KIND,
      plugin: 'plugin/dsh-migrate',
      mode: updating ? 'update' : 'full',
      createdAt: new Date().toISOString(),
      source: {
        home,
        host: hostname(),
        platform: platform(),
        appVersion: process.env.DSH_CLIENT_VERSION ?? null,
      },
      formatVersion: pickFormatVersion(snapshots),
      options: {
        includeFiles: Boolean(options.includeFiles),
        includeAttachments: Boolean(options.includeAttachments),
      },
      excludes: [...excludes],
      bytes: totalBytes,
      sessions: manifestSessions,
      skippedSessions: sessionsFailed,
      workspaces: workspaceDoc.workspaces,
      archived: workspaceDoc.archived.length,
      pinned: workspaceDoc.pinned.length,
    }
    await retryFs(() => fsp.writeFile(join(dir, 'manifest.json.tmp'), JSON.stringify(manifest, null, 2)), 'manifest.json.tmp')
    await retryFs(
      () =>
        fsp.writeFile(
          join(dir, 'state.json.tmp'),
          JSON.stringify({ version: 1, createdAt: new Date().toISOString(), sessions: sessionState, files: filesState }),
        ),
      'state.json.tmp',
    )
    // Atomic-ish completion: state first, manifest last (its presence marks a
    // usable bundle), then lift the incomplete marker (SEC-09).
    await retryFs(() => fsp.rename(join(dir, 'state.json.tmp'), join(dir, 'state.json')), 'state.json')
    await retryFs(() => fsp.rename(join(dir, 'manifest.json.tmp'), join(dir, 'manifest.json')), 'manifest.json')
    // Marker cleanup must never fail a finished export: sync-drive clients
    // hold just-written files briefly (EBUSY on unlink is common), and the
    // marker is cosmetic — manifest.json is the real completion signal.
    try {
      await retryFs(() => fsp.rm(join(dir, '.partial'), { force: true }), '.partial')
    } catch (error) {
      fileWarnings.push(
        `导出已完成，但 .partial 标记清理被文件占用（同步盘/杀毒扫描常见，包内容完整）：${error instanceof Error ? error.message : String(error)}。可手动删除该文件，不影响导入。`,
      )
    }
    task.done += 1

    if (sessionsFailed.length > 0) {
      fileWarnings.push(
        `${sessionsFailed.length} 个会话读取失败已跳过（详见 sessionsSkipped）：${sessionsFailed.slice(0, 3).map((entry) => entry.id).join('、')}${sessionsFailed.length > 3 ? ' 等' : ''}`,
      )
    }
    if (sessionsUnexported.length > 0) {
      fileWarnings.push(
        `${sessionsUnexported.length} 个会话不在导出范围（详见 sessionsUnexported）：${sessionsUnexported.slice(0, 3).map((entry) => entry.id).join('、')}${sessionsUnexported.length > 3 ? ' 等' : ''}`,
      )
    }

    const result = {
      path: dir,
      mode: updating ? 'update' : 'full',
      sessions: manifestSessions.length,
      sessionsSkipped: sessionsFailed,
      sessionsUnexported,
      sessionsReused,
      workspaces: workspaceDoc.workspaces.length,
      bytes: totalBytes,
      sessionBytes: bytes,
      filesBytes,
      attachmentsBytes,
      filesCopied,
      filesUnchanged,
      filesRemoved,
      bytesCopied,
      warnings: fileWarnings,
    }
    // Fork patch P2: seal every finished export (full or update) so the
    // governance script and the importing machine can verify payload bytes.
    try {
      result.seal = { sealed: true, ...(await sealBundle(dir, result)) }
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error)
      result.seal = { sealed: false, error: message }
      fileWarnings.unshift(`⚠ 导出封存失败（dir.sha256/回执未写入，可用 migrate-govern.ps1 seal 补封）：${message}`)
    }

    // P5 transport envelope: compress the sealed bundle into ONE zip on the
    // destination root (or refresh the existing one on update), drop the
    // local staging tree. The sync drive sees a single archive + one-line
    // hash sidecar instead of a tree of small files.
    if (finalZip) {
      task.message = '压缩迁移包…'
      // P5 placement: compress on LOCAL staging first, then place exactly one
      // finished file on the destination (often a sync drive). The drive never
      // sees the intermediate tree or a .tmp litter, and a same-directory
      // rename (which some sync clients re-upload) is avoided entirely.
      const zipLocal = `${dir}.zip`
      const packedStats = await retryFs(() => zipDirectory(dir, zipLocal), 'zip')
      const zipSha = await sha256File(zipLocal)
      await retryFs(() => fsp.copyFile(zipLocal, finalZip), 'zip-place')
      const landedSha = await sha256File(finalZip)
      if (landedSha !== zipSha) {
        throw new Error(`压缩包落盘校验不符（${basename(finalZip)}），未写边车；请重试导出`)
      }
      await retryFs(() => fsp.writeFile(`${finalZip}.sha256`, `${zipSha}  ${basename(finalZip)}\n`, 'utf8'), 'zip-sidecar')
      result.path = finalZip
      result.packed = true
      result.zip = {
        name: basename(finalZip),
        sha256: zipSha,
        sidecar: `${basename(finalZip)}.sha256`,
        files: packedStats.files,
        bytes: packedStats.bytes,
      }
      try {
        await retryFs(() => fsp.rm(zipLocal, { force: true }), 'zip-local')
      } catch {
        /* TEMP is self-healing */
      }
      if (staging) await dropStaging(staging)
    } else {
      result.packed = false
    }
    return result
  }

  function pickFormatVersion(snapshots) {
    for (const snapshot of snapshots) {
      const version = snapshot.header?.version
      if (typeof version === 'number') return version
    }
    return null
  }

  // ── import ───────────────────────────────────────────────────────────────

  async function loadSessionDocument(dir, meta) {
    const candidates = []
    if (meta.file) {
      // Fork audit fix (SEC-05): the manifest is bundle-supplied — its file
      // name must stay inside sessions/ and never steer a path elsewhere.
      const name = String(meta.file)
      if (!isSafePathSegment(name)) throw new Error(`迁移包会话文件名非法：${name}`)
      candidates.push(name)
    }
    candidates.push(`${safeName(meta.id)}.json.zst`, `${safeName(meta.id)}.json`)
    for (const name of candidates) {
      const full = safeJoinUnder(join(dir, 'sessions'), name)
      let buffer
      try {
        buffer = await fsp.readFile(full)
      } catch (error) {
        if (error && error.code === 'ENOENT') continue
        throw error
      }
      if (name.endsWith('.zst')) {
        const zstd = await loadZstd()
        if (!zstd) throw new Error('本机运行时不支持 zstd，无法解压会话文件')
        return JSON.parse(zstd.zstdDecompressSync(buffer).toString('utf8'))
      }
      return JSON.parse(buffer.toString('utf8'))
    }
    throw new Error(`迁移包缺少会话文件：${meta.id}`)
  }

  async function removeSessionDirs(root, id) {
    // Fork audit fix (SEC-03): the id is bundle-supplied. A value like '..'
    // must never resolve a delete target outside the project directory.
    const safeId = String(id)
    if (!isSafePathSegment(safeId)) return 0
    let removed = 0
    let projects
    try {
      projects = await fsp.readdir(root, { withFileTypes: true })
    } catch {
      return removed
    }
    for (const project of projects) {
      if (!project.isDirectory()) continue
      const projectRoot = resolve(join(root, project.name))
      const target = resolve(join(projectRoot, safeId))
      if (target === projectRoot || !target.startsWith(projectRoot + sep)) continue
      if (await pathExists(target)) {
        await fsp.rm(target, { recursive: true, force: true })
        removed += 1
        try {
          const rest = await fsp.readdir(join(root, project.name))
          if (rest.length === 0) await fsp.rmdir(join(root, project.name))
        } catch {
          /* keep a non-empty project directory */
        }
      }
    }
    return removed
  }

  async function rollback(payload) {
    const workspaceIds = Array.isArray(payload.workspaceIds) ? payload.workspaceIds.map(String) : []
    const sessionIds = Array.isArray(payload.sessionIds) ? payload.sessionIds.map(String) : []
    const result = { workspaces: [], sessions: [] }
    for (const id of workspaceIds) {
      if (!createdWorkspaces.has(id)) {
        result.workspaces.push({ id, refused: '不是本次运行创建的项目，已拒绝删除' })
        continue
      }
      try {
        result.workspaces.push({ id, removed: await ctx.workspaceRegistry.delete(id) })
      } catch (error) {
        result.workspaces.push({ id, error: error instanceof Error ? error.message : String(error) })
      }
    }
    const root = join(dshHome(), 'sessions')
    for (const id of sessionIds) {
      if (!createdSessions.has(id)) {
        result.sessions.push({ id, refused: '不是本次运行导入的会话，已拒绝删除' })
        continue
      }
      try {
        result.sessions.push({ id, removed: await removeSessionDirs(root, id) })
      } catch (error) {
        result.sessions.push({ id, error: error instanceof Error ? error.message : String(error) })
      }
    }
    return result
  }

  async function cleanupWorkspaces(payload) {
    const only = Array.isArray(payload.workspaceIds) ? payload.workspaceIds.map(String) : null
    const removed = []
    for (const workspace of ctx.workspaceRegistry.list()) {
      const id = String(workspace.id)
      if (only && !only.includes(id)) continue
      // Only registrations that point at nothing and hold nothing are safe to drop.
      if (workspace.sessionIds.length > 0) continue
      if (await pathExists(workspace.path)) continue
      try {
        removed.push({ id, path: workspace.path, removed: await ctx.workspaceRegistry.delete(id) })
      } catch (error) {
        removed.push({ id, path: workspace.path, error: error instanceof Error ? error.message : String(error) })
      }
    }
    return { removed }
  }

  // ── attachment audit ─────────────────────────────────────────────────────

  /** Collect every attachment reference reachable from one event payload. */
  function collectAttachments(value, out, depth = 0) {
    if (depth > 12 || value === null || typeof value !== 'object') return
    if (Array.isArray(value)) {
      for (const item of value) collectAttachments(item, out, depth + 1)
      return
    }
    if (typeof value.attachmentId === 'string' && !out.has(value.attachmentId)) out.set(value.attachmentId, value)
    for (const key of Object.keys(value)) collectAttachments(value[key], out, depth + 1)
  }

  /** Whether this machine can still resolve one attachment reference. */
  function attachmentExists(ref) {
    const attachments = ctx.get('attachments')
    if (!attachments) return true
    try {
      if (typeof ref.mediaType === 'string' && typeof attachments.imageHostPath === 'function') {
        return typeof attachments.imageHostPath(ref) === 'string'
      }
      if (typeof attachments.fileHostPath === 'function') {
        return typeof attachments.fileHostPath(ref) === 'string'
      }
    } catch {
      return false
    }
    return true
  }

  function auditDocument(document) {
    const refs = new Map()
    for (const event of document.events ?? []) {
      if (event && event.data) collectAttachments(event.data, refs)
    }
    const missingIds = []
    for (const [id, ref] of refs) if (!attachmentExists(ref)) missingIds.push(id)
    return { refs: refs.size, missing: missingIds.length, missingIds: missingIds.slice(0, 20) }
  }

  async function auditBundle(bundlePath) {
    if (!bundlePath) throw new Error('缺少迁移包路径')
    return withBundleDir(bundlePath, (dir) => auditBundleInDir(dir))
  }

  async function auditBundleInDir(dir) {
    const manifest = await readManifest(dir)
    const problems = []
    let totalRefs = 0
    let totalMissing = 0
    let scanned = 0
    for (const meta of manifest.sessions ?? []) {
      let document
      try {
        document = await loadSessionDocument(dir, meta)
      } catch {
        continue
      }
      scanned += 1
      const result = auditDocument(document)
      totalRefs += result.refs
      totalMissing += result.missing
      if (result.missing > 0) {
        problems.push({
          id: String(meta.id),
          cwd: meta.cwd ?? null,
          refs: result.refs,
          missing: result.missing,
          missingIds: result.missingIds,
        })
      }
    }
    const attachmentsInBundle = await pathExists(join(dir, 'attachments'))
    // Fork audit fix (SEC-08): the pre-import check surface reports payload
    // integrity whenever the bundle carries a sealed sidecar.
    const integrity = await verifySidecar(dir)
    return {
      path: dir,
      sessions: (manifest.sessions ?? []).length,
      integrity,
      scanned,
      attachmentsInBundle,
      attachmentService: Boolean(ctx.get('attachments')),
      totalRefs,
      totalMissing,
      problems,
      advice:
        totalMissing === 0
          ? null
          : attachmentsInBundle
            ? '包内带了 attachments 目录：勾选「包含图片附件」会把它们装到本机；导入后仍提示缺失，说明该附件在旧机器上就已经不在了。'
            : '包内没有附件。把旧机器的 ~/.dsh/attachments 整个拷到新机器同名位置，或重新导出并勾选附件。',
    }
  }

  async function runImport(task, options) {
    if (!options.bundlePath) throw new Error('缺少迁移包路径')
    const sourcePath = resolve(String(options.bundlePath))
    return withBundleDir(sourcePath, (dir) => runImportInDir(task, options, dir, sourcePath))
  }

  async function runImportInDir(task, options, dir, sourcePath) {
    const manifest = await readManifest(dir)
    // Fork audit fix (SEC-08): when the bundle carries a sealed sidecar,
    // verify it before touching the live library. A mismatch is refused
    // unless the operator explicitly opts out via skipIntegrityCheck.
    const integrity = await verifySidecar(dir)
    if (integrity.checked && !integrity.match && options.skipIntegrityCheck !== true) {
      throw new Error(
        `迁移包完整性校验失败（${integrity.differences.length} 处差异：${integrity.differences.slice(0, 3).join('；')}）。已拒绝导入；如确认无碍，请显式传 skipIntegrityCheck 重试。`,
      )
    }
    const workspaceDoc = JSON.parse(await fsp.readFile(join(dir, 'workspaces.json'), 'utf8'))
    const rootDir =
      options.rootDir && String(options.rootDir).trim() ? resolve(normalizeTargetPath(options.rootDir)) : null
    const remapInput = options.remap && typeof options.remap === 'object' ? options.remap : {}

    // Resolve one target directory per bundled project: an explicit per-project
    // mapping wins, then <rootDir>/<project title>, then the original path.
    const targets = []
    const usedTargets = new Set()
    for (const workspace of workspaceDoc.workspaces ?? []) {
      const explicit = Object.prototype.hasOwnProperty.call(remapInput, workspace.path)
        ? String(remapInput[workspace.path] ?? '').trim()
        : ''
      let target
      if (explicit) {
        target = resolve(normalizeTargetPath(explicit))
      } else if (rootDir) {
        const base = safeSegment(workspace.title || basename(workspace.path), workspace.id)
        let candidate = join(rootDir, base)
        let suffix = 2
        while (usedTargets.has(normalKey(candidate))) {
          candidate = join(rootDir, `${base}-${suffix}`)
          suffix += 1
        }
        target = candidate
      } else {
        // Fork audit fix (SEC-02): the bundled workspace.path is data from the
        // bundle, not a decision by this machine's operator. Writing there by
        // default let a crafted bundle choose any destination. Require the
        // operator to name a new root (or explicit per-project targets) instead.
        throw new Error('未指定导入目标：请填写「新根目录」（rootDir）或为每个项目提供显式目标（remap），拒绝默认写入迁移包内记录的原始路径')
      }
      usedTargets.add(normalKey(target))
      targets.push({ source: workspace, target })
    }

    const pairs = []
    for (const entry of targets) {
      if (entry.source.path === entry.target) continue
      pairs.push({ from: entry.source.path, to: entry.target })
    }
    for (const [from, to] of Object.entries(remapInput)) {
      if (!from || !to || String(from).trim() === String(to).trim()) continue
      if (targets.some((entry) => normalKey(entry.source.path) === normalKey(from))) continue
      pairs.push({ from: String(from), to: String(to) })
    }
    pairs.sort((a, b) => normalKey(b.from).length - normalKey(a.from).length)

    const report = {
      path: sourcePath,
      rootDir,
      workspaces: [],
      sessionsImported: 0,
      importedSessions: [],
      sessionsSkipped: [],
      flattened: 0,
      ungrouped: 0,
      rewritten: 0,
      missingAttachments: 0,
      attachmentGaps: [],
      filesCopied: 0,
      fileSkips: [],
      attachments: false,
      warnings: [],
    }
    if (integrity.checked && !integrity.match) {
      report.warnings.push(
        `迁移包边车校验有差异（${integrity.differences.length} 处），已按 skipIntegrityCheck 显式放行：${integrity.differences.slice(0, 3).join('；')}`,
      )
    }
    const willInstallAttachments = Boolean(options.includeAttachments) && (await pathExists(join(dir, 'attachments')))

    const snapshots = await ctx.sessionPersistence.list()
    const localVersion = pickFormatVersion(snapshots)
    if (manifest.formatVersion && localVersion && localVersion < manifest.formatVersion) {
      report.warnings.push(
        `迁移包来自更新的格式 v${manifest.formatVersion}，本机是 v${localVersion}；会话可能无法导入，建议两台机器使用同一版本的 DSH。`,
      )
    }

    const preOrder = ctx.workspaceRegistry.list().map((workspace) => String(workspace.id))
    const preExisting = new Set(preOrder)
    const created = []
    task.total = (workspaceDoc.workspaces ?? []).length + (manifest.sessions ?? []).length + 2

    for (const entry of targets) {
      try {
        await fsp.mkdir(entry.target, { recursive: true })
        const entity = await ctx.workspaceRegistry.create(entry.target, entry.source.title)
        if (entry.source.title && entity.title !== entry.source.title && !preExisting.has(String(entity.id))) {
          try {
            await entity.setTitle(entry.source.title)
          } catch {
            /* rename is cosmetic; keep the created title */
          }
        }
        created.push({ source: entry.source, entity, target: entry.target })
        if (!preExisting.has(String(entity.id))) createdWorkspaces.add(String(entity.id))
        report.workspaces.push({ id: String(entity.id), path: entry.target, title: entity.title, from: entry.source.path })
      } catch (error) {
        report.warnings.push(
          `项目「${entry.source.title}」创建失败：${error instanceof Error ? error.message : String(error)}`,
        )
      }
      task.done += 1
      task.message = `已创建项目 ${created.length}/${targets.length}`
    }

    const tailAnchor = preOrder[0]
    for (let index = created.length - 1; index >= 0; index -= 1) {
      const anchor = created[index + 1]?.entity.id ?? tailAnchor
      try {
        await ctx.workspaceRegistry.insertBefore(created[index].entity.id, anchor)
      } catch {
        /* order is cosmetic */
      }
    }

    const existing = new Set(snapshots.map((snapshot) => String(snapshot.header.id)))
    const attached = new Set()

    for (const meta of manifest.sessions ?? []) {
      const id = String(meta.id)
      // Fork audit fix (SEC-03): ids steer filesystem paths on rollback —
      // anything outside the safe charset is refused up front.
      if (!isSafePathSegment(id)) {
        report.sessionsSkipped.push({ id, reason: '会话 id 含非法字符，已跳过' })
        task.done += 1
        continue
      }
      if (existing.has(id)) {
        report.sessionsSkipped.push({ id, reason: '本机已存在同名会话' })
        task.done += 1
        continue
      }
      try {
        const document = await loadSessionDocument(dir, meta)
        // Fork audit fix (SEC-04): the manifest id and the document header id
        // are both bundle-supplied; they must agree before anything is created
        // or registered for rollback.
        const headerId = document.header && document.header.id != null ? String(document.header.id) : ''
        if (headerId !== id) {
          report.sessionsSkipped.push({ id, reason: `会话文件内 id（${headerId || '缺失'}）与清单不一致，已跳过` })
          task.done += 1
          continue
        }
        if (!willInstallAttachments) {
          const audit = auditDocument(document)
          if (audit.missing > 0) {
            report.missingAttachments += 1
            report.attachmentGaps.push({ id, missing: audit.missing, total: audit.refs })
          }
        }
        let events = Array.isArray(document.events) ? document.events : []
        let header = {
          ...document.header,
          version: typeof localVersion === 'number' ? localVersion : document.header.version,
        }
        if (header.cwd) {
          header.cwd = mapPath(header.cwd, pairs)
          if (pairs.length > 0 && isUnderTargets(header.cwd, targets)) {
            try {
              await fsp.mkdir(header.cwd, { recursive: true })
            } catch {
              /* creating the working directory is best effort */
            }
          }
        }

        // Historical tool calls and results still name the old machine's paths.
        if (options.rewritePaths !== false && pairs.length > 0) {
          const stats = { count: 0 }
          for (const event of events) {
            if (event && event.data && typeof event.data === 'object') {
              event.data = rewritePaths(event.data, pairs, stats)
            }
          }
          report.rewritten += stats.count
        }

        if (header.isSeeded) {
          const inherited = Number(document.inheritedEventCount ?? 0)
          const parentId = header.parentSession ? String(header.parentSession) : null
          const parentMeta = parentId ? (manifest.sessions ?? []).find((entry) => String(entry.id) === parentId) : null
          if (!parentMeta || inherited <= 0) {
            report.sessionsSkipped.push({ id, reason: 'fork 会话缺少父会话种子，已跳过' })
            task.done += 1
            continue
          }
          const parentDocument = await loadSessionDocument(dir, parentMeta)
          const parentEvents = Array.isArray(parentDocument.events) ? parentDocument.events : []
          if (parentEvents.length < inherited) {
            report.sessionsSkipped.push({ id, reason: 'fork 会话的父会话事件不足，已跳过' })
            task.done += 1
            continue
          }
          events = [...parentEvents.slice(0, inherited), ...events]
          header = { ...header, isSeeded: false, parentSession: undefined, origin: undefined, delegationDepth: undefined }
          report.flattened += 1
        }

        const handle = await ctx.sessionPersistence.create(header)
        try {
          if (events.length > 0) await handle.append(events)
          await handle.flush()
        } finally {
          await handle.close()
        }
        existing.add(id)
        createdSessions.add(id)
        report.sessionsImported += 1
        report.importedSessions.push(id)

        const match = created.find((entry) => normalKey(header.cwd ?? '') === normalKey(entry.target))
        if (match && !attached.has(`${match.entity.id}:${id}`)) {
          try {
            await match.entity.attachSession(id)
            attached.add(`${match.entity.id}:${id}`)
          } catch (error) {
            report.warnings.push(`会话 ${id} 归属到项目失败：${error instanceof Error ? error.message : String(error)}`)
          }
        } else if (!match) {
          report.ungrouped += 1
        }
      } catch (error) {
        report.sessionsSkipped.push({ id, reason: error instanceof Error ? error.message : String(error) })
      }
      task.done += 1
      task.message = `已导入会话 ${report.sessionsImported}/${(manifest.sessions ?? []).length}`
    }

    for (const id of workspaceDoc.pinned ?? []) {
      if (!existing.has(String(id))) continue
      try {
        await ctx.workspaceRegistry.pinSession(String(id))
      } catch {
        /* pin is cosmetic */
      }
    }
    for (const id of workspaceDoc.archived ?? []) {
      if (!existing.has(String(id))) continue
      try {
        await ctx.workspaceRegistry.archiveSession(String(id))
      } catch {
        /* archive is cosmetic */
      }
    }

    if (options.includeFiles) {
      for (const entry of created) {
        const source = join(dir, 'files', safeName(entry.source.id))
        if (!(await pathExists(source))) continue
        try {
          const items = await fsp.readdir(entry.target)
          if (items.length > 0 && !options.overwriteFiles) {
            report.fileSkips.push({ path: entry.target, reason: '目标目录非空，已跳过文件复制' })
            continue
          }
          await fsp.cp(source, entry.target, { recursive: true, force: true })
          report.filesCopied += 1
        } catch (error) {
          report.warnings.push(`复制项目文件失败：${entry.target}（${error instanceof Error ? error.message : String(error)}）`)
        }
      }
    }

    if (options.includeAttachments && (await pathExists(join(dir, 'attachments')))) {
      try {
        await fsp.cp(join(dir, 'attachments'), join(dshHome(), 'attachments'), { recursive: true, force: true })
        report.attachments = true
      } catch (error) {
        report.warnings.push(`复制附件失败：${error instanceof Error ? error.message : String(error)}`)
      }
    }

    task.done += 1
    return report
  }

  // ── HTTP surface ─────────────────────────────────────────────────────────

  function sendJson(res, status, value) {
    const body = Buffer.from(JSON.stringify(value), 'utf8')
    res.writeHead(status, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' })
    res.end(body)
  }

  async function readJson(req) {
    const chunks = []
    let size = 0
    for await (const chunk of req) {
      size += chunk.length
      if (size > 1_000_000) throw new Error('请求体过大')
      chunks.push(chunk)
    }
    if (chunks.length === 0) return {}
    return JSON.parse(Buffer.concat(chunks).toString('utf8'))
  }

  /**
   * 身份认证。宿主提供 `connection` 服务时必须用它：
   *   ctx.connection.admit({ headers }) 会校验浏览器签名凭据（或 API token），
   *   返回 `{}` 或 `{ rejection: 401 | 403 }`。
   * 没有该服务（老宿主）时退回 Origin 检查，并额外要求 Origin 存在——
   * 缺 Origin 的请求只可能来自非浏览器客户端。
   */
  /**
   * 身份认证。只认宿主的 `connection` 服务：
   *   ctx.connection.requestRejection({ headers }) —— 宿主自带的 Host/Origin 围栏
   *   + 浏览器签名凭据（login-token cookie）校验，被拒时返回 401/403。
   * 没有这个服务就**直接拒绝**：Origin/Host 只是请求头，客户端可以随便写，
   * 不能当身份凭据用。迁移、回滚、清理这类会写盘的操作宁可不可用，也不可无认证。
   */
  function authenticate(req) {
    const connection = ctx.get('connection')
    if (!connection) return { status: 503, reason: 'unavailable' }
    try {
      if (typeof connection.requestRejection === 'function') {
        const rejection = connection.requestRejection({ headers: req.headers })
        return rejection ? { status: rejection } : {}
      }
      const admission = connection.admit({ headers: req.headers })
      if (admission && admission.rejection) return { status: admission.rejection }
      return {}
    } catch {
      return { status: 401 }
    }
  }

  async function handler(req, res) {
    const auth = authenticate(req)
    if (auth.status) {
      const message =
        auth.reason === 'unavailable'
          ? '宿主未提供 connection 认证服务，已拒绝处理（不做无身份校验的迁移）'
          : auth.status === 401
            ? '需要先在 Harness 中登录（缺少或无效的浏览器凭据）'
            : '拒绝跨站请求'
      sendJson(res, auth.status, { error: message })
      return
    }
    const url = new URL(req.url ?? '/', 'http://localhost')
    const route = url.pathname.slice(ROUTE_PREFIX.length) || '/'
    try {
      if (req.method === 'GET' && route === '/inventory') {
        sendJson(res, 200, await inventory())
        return
      }
      if (req.method === 'GET' && route === '/preview') {
        sendJson(res, 200, await preview(url.searchParams.get('path')))
        return
      }
      if (req.method === 'GET' && route === '/check') {
        sendJson(res, 200, await auditBundle(url.searchParams.get('path')))
        return
      }
      if (req.method === 'GET' && route === '/task') {
        sendJson(res, 200, taskView(url.searchParams.get('id')))
        return
      }
      if (req.method === 'POST' && route === '/export') {
        const body = await readJson(req)
        const task = startTask('export')
        runExport(task, body).then((result) => succeed(task, result)).catch((error) => fail(task, error))
        sendJson(res, 200, { taskId: task.id })
        return
      }
      if (req.method === 'POST' && route === '/import') {
        const body = await readJson(req)
        const task = startTask('import')
        runImport(task, body).then((result) => succeed(task, result)).catch((error) => fail(task, error))
        sendJson(res, 200, { taskId: task.id })
        return
      }
      if (req.method === 'POST' && route === '/rollback') {
        sendJson(res, 200, await rollback(await readJson(req)))
        return
      }
      if (req.method === 'POST' && route === '/cleanup') {
        sendJson(res, 200, await cleanupWorkspaces(await readJson(req)))
        return
      }
      sendJson(res, 404, { error: '未知接口' })
    } catch (error) {
      sendJson(res, 500, { error: error instanceof Error ? error.message : String(error) })
    }
  }

  ctx.effect(() => ctx.webServer.register({ kind: 'prefix', path: ROUTE_PREFIX, handler }))

  // ── fork patch P1: model tools ──────────────────────────────────────────
  // The HTTP routes require browser credentials, so an agent can never drive
  // them; these tools run in-process with the Host's own identity instead.
  // Export is gated: without apply=true it only returns a dry-run plan.

  const renderJson = (value) => [{ type: 'text', text: JSON.stringify(value, null, 2) }]

  ctx.effect(() =>
    ctx.tools.register({
      name: 'dsh_migrate_status',
      description:
        'Read-only migration status: DSH home, migration bundle roots, existing bundles, workspace/session counts, and configured default export directory. Never writes files.',
      parameters: { type: 'object', properties: {}, additionalProperties: false },
      output: {
        schema: {
          type: 'object',
          properties: {
            home: { type: 'string' },
            migrationsDir: { type: 'string' },
            defaultOutDir: {},
            codeVersion: { type: 'integer' },
            workspaces: { type: 'integer' },
            sessions: { type: 'integer' },
            hasAttachments: { type: 'boolean' },
            bundles: { type: 'array', items: { type: 'object' } },
            excludes: { type: 'array', items: { type: 'string' } },
          },
        },
        render: (_args, value) => renderJson(value),
      },
      async execute(_args, exec) {
        if (exec.signal.aborted) throw new Error('已取消')
        const inv = await inventory()
        return {
          home: inv.home,
          migrationsDir: inv.migrationsDir,
          defaultOutDir,
          codeVersion: inv.codeVersion,
          workspaces: inv.workspaces.length,
          sessions: inv.sessions.length,
          hasAttachments: inv.hasAttachments,
          bundles: inv.bundles,
          excludes: inv.excludes,
        }
      },
    }),
  )

  ctx.effect(() =>
    ctx.tools.register({
      name: 'dsh_migrate_export',
      description:
        'Export DSH workspaces and session logs into a portable migration bundle — by default a single .zip on outDir (sync-drive friendly), or a folder with pack=false. Dry-run by default: returns the plan without writing; pass apply=true to actually write. mode=update incrementally refreshes an existing bundlePath (folder or .zip). Import/rollback stay UI-only on purpose.',
      parameters: {
        type: 'object',
        properties: {
          apply: { type: 'boolean', description: 'Actually write the bundle. false (default) returns the plan only.' },
          pack: {
            type: 'boolean',
            description:
              'Pack the sealed bundle into ONE .zip on outDir and clean local staging (default true) — ideal for sync drives. pack:false writes the folder bundle. Updates keep the target form.',
          },
          outDir: {
            type: 'string',
            description: 'Directory that will contain the bundle. Defaults to config.defaultOutDir, then ~/.dsh/migrations.',
          },
          mode: { type: 'string', enum: ['full', 'update'], description: 'full creates a new bundle; update refreshes bundlePath in place.' },
          bundlePath: { type: 'string', description: 'Existing bundle (folder or .zip); required when mode is update.' },
          includeFiles: { type: 'boolean', description: 'Also copy project files into the bundle (default false).' },
          includeAttachments: { type: 'boolean', description: 'Also copy ~/.dsh/attachments into the bundle (default false).' },
          workspaceIds: { type: 'array', items: { type: 'string' }, description: 'Restrict the export to these workspace ids; default all.' },
        },
        additionalProperties: false,
      },
      output: {
        schema: {
          type: 'object',
          properties: {
            dryRun: { type: 'boolean' },
            applied: { type: 'boolean' },
            path: { type: 'string' },
            outRoot: { type: 'string' },
            mode: { type: 'string' },
            sessions: { type: 'integer' },
            sessionsSkipped: { type: 'array', items: { type: 'object' } },
            sessionsUnexported: { type: 'array', items: { type: 'object' } },
            sessionsReused: { type: 'integer' },
            sessionsWouldReuse: { type: 'integer' },
            workspaces: { type: 'integer' },
            bytes: { type: 'integer' },
            seal: { type: 'object' },
            packed: { type: 'boolean' },
            zip: { type: 'object' },
            warnings: { type: 'array', items: { type: 'string' } },
          },
        },
        render: (_args, value) => renderJson(value),
      },
      async execute(rawArgs, exec) {
        const args = rawArgs && typeof rawArgs === 'object' ? rawArgs : {}
        if (exec.signal.aborted) throw new Error('已取消')
        const mode = args.mode === 'update' ? 'update' : 'full'
        if (mode === 'update' && !(args.bundlePath && String(args.bundlePath).trim())) {
          throw new Error("mode='update' 需要 bundlePath（已有迁移包目录）")
        }
        const options = {
          dryRun: args.apply !== true,
          pack: args.pack !== false,
          outDir: typeof args.outDir === 'string' ? args.outDir : '',
          mode,
          bundlePath: typeof args.bundlePath === 'string' ? args.bundlePath : '',
          includeFiles: args.includeFiles === true,
          includeAttachments: args.includeAttachments === true,
          workspaceIds: Array.isArray(args.workspaceIds) ? args.workspaceIds.map(String) : undefined,
        }
        const task = startTask('export')
        try {
          const result = await runExport(task, options)
          succeed(task, result)
          return { applied: !options.dryRun, ...result }
        } catch (error) {
          fail(task, error)
          throw error
        }
      },
    }),
  )

  ctx.effect(() => () => {
    tasks.clear()
  })
}
