import express from 'express'
import { WebSocketServer } from 'ws'
import { Worker } from 'worker_threads'
import { createServer } from 'http'
import { fileURLToPath } from 'url'
import path from 'path'
import fs from 'fs/promises'

const __dirname = path.dirname(fileURLToPath(import.meta.url))
const DATA_FILE  = path.join(__dirname, 'data', 'socios.json')

const app    = express()
const server = createServer(app)
const wss    = new WebSocketServer({ server })

// ── CORS ───────────────────────────────────────────────────────────────────
// Necesario para que el browser de MortaGym (Hostinger) pueda llamar al agente
app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*')
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, DELETE, OPTIONS')
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type')
  if (req.method === 'OPTIONS') return res.sendStatus(200)
  next()
})

app.use(express.static(path.join(__dirname, 'public')))
app.use(express.json({ limit: '4mb' }))

let worker = null
let deviceConnected = false

// ── Sesiones de enrolamiento (para MortaGym) ───────────────────────────────
//   sessionId → { dni, nombre, status, step, templateBase64, len, error, createdAt }
//
//   status: 'waiting_finger' | 'lift_finger' | 'processing' | 'done' | 'error'
//   step:    0 (antes de 1ra captura) | 1 | 2 | 3 (las 3 completadas)

const enrollSessions = new Map()
let activeSessionId  = null       // solo una sesión a la vez (un lector)

// ── Helpers ────────────────────────────────────────────────────────────────

function broadcast(data) {
  const msg = JSON.stringify(data)
  wss.clients.forEach(c => { if (c.readyState === 1) c.send(msg) })
}

async function readSocios() {
  try { return JSON.parse(await fs.readFile(DATA_FILE, 'utf8')) } catch { return [] }
}
async function writeSocios(socios) {
  await fs.mkdir(path.dirname(DATA_FILE), { recursive: true })
  await fs.writeFile(DATA_FILE, JSON.stringify(socios, null, 2), 'utf8')
}

// Upsert local al terminar el enrolamiento desde MortaGym
async function upsertSocioLocal(dni, nombre, templateBase64) {
  try {
    const socios = await readSocios()
    const idx = socios.findIndex(s => String(s.dni) === String(dni))
    const now = new Date().toISOString()
    if (idx >= 0) {
      socios[idx] = { ...socios[idx], nombre, template_huella: templateBase64, actualizado_en: now }
    } else {
      socios.push({ id: socios.length + 1, nombre, dni: String(dni), template_huella: templateBase64, creado_en: now })
    }
    await writeSocios(socios)
    console.log(`[enroll] upsert local → ${nombre} (${dni})`)
  } catch (e) {
    console.error('[enroll] upsert local falló:', e.message)
  }
}

// ── Worker ─────────────────────────────────────────────────────────────────

function spawnWorker() {
  if (worker) { worker.terminate(); worker = null }
  worker = new Worker(new URL('./zk_worker.js', import.meta.url))

  worker.on('message', msg => {
    if (msg.type === 'connected')    deviceConnected = true
    if (msg.type === 'disconnected') deviceConnected = false

    // Mantener sesión de enrolamiento actualizada para el polling de MortaGym
    if (activeSessionId) {
      const s = enrollSessions.get(activeSessionId)
      if (s) {
        if (msg.type === 'enrollStep') {
          s.step   = msg.step
          // Si aún no terminaron las 3 capturas: esperar a que levante el dedo
          s.status = msg.step < 3 ? 'lift_finger' : 'processing'
          // Después de 2 s el worker sale de la pausa; sincronizamos el status
          if (msg.step < 3) setTimeout(() => {
            if (s.status === 'lift_finger') s.status = 'waiting_finger'
          }, 2100)
        }
        if (msg.type === 'enrollComplete') {
          s.status         = 'done'
          s.templateBase64 = msg.templateBase64
          s.len            = msg.len
          upsertSocioLocal(s.dni, s.nombre || `DNI ${s.dni}`, msg.templateBase64)
          activeSessionId = null
        }
        if (msg.type === 'enrollError') {
          s.status = 'error'
          s.error  = msg.msg || `DBMerge ret=${msg.ret}`
          activeSessionId = null
        }
      }
    }

    broadcast(msg)
  })

  worker.on('error', e => {
    console.error('[worker]', e.message)
    broadcast({ type: 'error', msg: e.message })
  })
  worker.on('exit', c => {
    worker = null; deviceConnected = false
    broadcast({ type: 'disconnected', msg: `Worker cerrado (${c})` })
  })
}

// ── Rutas — dispositivo ────────────────────────────────────────────────────

app.post('/api/connect', (_req, res) => {
  spawnWorker()
  worker.postMessage({ cmd: 'connect' })
  res.json({ ok: true })
})
app.post('/api/disconnect', (_req, res) => {
  if (worker) worker.postMessage({ cmd: 'disconnect' })
  res.json({ ok: true })
})
app.get('/api/status', (_req, res) => {
  res.json({ connected: deviceConnected, hasWorker: !!worker })
})

// ── Rutas — enrolamiento panel local (backward compat) ────────────────────
//  Importante: estas rutas específicas van ANTES de /api/enroll/:dni
//  para que Express no las confunda con el parámetro.

app.post('/api/enroll/start', (_req, res) => {
  if (!deviceConnected || !worker)
    return res.status(400).json({ error: 'Lector no conectado' })
  worker.postMessage({ cmd: 'startEnroll' })
  res.json({ ok: true })
})

app.post('/api/enroll/cancel', (_req, res) => {
  if (worker) worker.postMessage({ cmd: 'cancelEnroll' })
  if (activeSessionId) { enrollSessions.delete(activeSessionId); activeSessionId = null }
  res.json({ ok: true })
})

// ── Rutas — enrolamiento MortaGym ─────────────────────────────────────────

// Inicia un enrolamiento ligado a un DNI.
// Body (opcional): { nombre: "García Lucas" }
// Respuesta:       { ok, sessionId, status: "agent_ready" }
app.post('/api/enroll/:dni', (req, res) => {
  if (!deviceConnected || !worker)
    return res.status(400).json({ error: 'Lector no conectado. Verificá la PC del agente.' })

  // Rechazar si ya hay una sesión en curso
  if (activeSessionId) {
    const existing = enrollSessions.get(activeSessionId)
    if (existing && ['waiting_finger', 'lift_finger', 'processing'].includes(existing.status))
      return res.status(409).json({ error: 'Ya hay un registro en curso', sessionId: activeSessionId })
  }

  const { dni } = req.params
  const { nombre = '' } = req.body ?? {}
  const sessionId = `enr_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`

  enrollSessions.set(sessionId, {
    sessionId,
    dni,
    nombre,
    status: 'waiting_finger',
    step: 0,
    templateBase64: null,
    len: 0,
    error: null,
    createdAt: new Date().toISOString()
  })
  activeSessionId = sessionId

  worker.postMessage({ cmd: 'startEnroll' })

  console.log(`[enroll] sesión iniciada: ${sessionId} | DNI: ${dni} | nombre: ${nombre || '(sin nombre)'}`)
  res.json({ ok: true, sessionId, status: 'agent_ready' })
})

// Polling de estado. El React app llama cada ~500 ms.
app.get('/api/enroll/session/:sessionId', (req, res) => {
  const s = enrollSessions.get(req.params.sessionId)
  if (!s) return res.status(404).json({ error: 'Sesión no encontrada o expirada' })
  res.json(s)
})

// Cancela y limpia la sesión.
app.delete('/api/enroll/session/:sessionId', (req, res) => {
  const { sessionId } = req.params
  if (activeSessionId === sessionId) {
    if (worker) worker.postMessage({ cmd: 'cancelEnroll' })
    activeSessionId = null
  }
  enrollSessions.delete(sessionId)
  res.json({ ok: true })
})

// ── Rutas — socios ─────────────────────────────────────────────────────────

app.get('/api/socios', async (_req, res) => {
  res.json(await readSocios())
})
app.post('/api/socios', async (req, res) => {
  const { nombre, dni, template_huella } = req.body
  if (!nombre || !dni || !template_huella)
    return res.status(400).json({ error: 'Faltan campos: nombre, dni, template_huella' })
  const socios = await readSocios()
  const id = socios.length + 1
  const socio = { id, nombre, dni: String(dni), template_huella, creado_en: new Date().toISOString() }
  socios.push(socio); await writeSocios(socios)
  console.log(`[socios] +${nombre} (${dni}) → id ${id}`)
  res.json({ ok: true, socio })
})

// ── Rutas — verificación ───────────────────────────────────────────────────

app.post('/api/identify/start', async (_req, res) => {
  if (!deviceConnected || !worker)
    return res.status(400).json({ error: 'Lector no conectado' })
  const socios   = await readSocios()
  const templates = socios
    .filter(s => s.template_huella)
    .map(s => ({ id: s.id, nombre: s.nombre, dni: s.dni, templateBase64: s.template_huella }))
  if (!templates.length)
    return res.status(400).json({ error: 'No hay socios con huella registrada' })
  worker.postMessage({ cmd: 'startIdentify', templates })
  res.json({ ok: true, count: templates.length })
})
app.post('/api/identify/stop', (_req, res) => {
  if (worker) worker.postMessage({ cmd: 'stopIdentify' })
  res.json({ ok: true })
})

// ── WebSocket ──────────────────────────────────────────────────────────────

wss.on('connection', ws => {
  ws.send(JSON.stringify({ type: 'init', connected: deviceConnected }))
})

server.listen(3001, () => console.log('huella-agent → http://localhost:3001'))

// Agregá esto en index.js junto a las otras constantes de archivo
const FICHAS_FILE = path.join(__dirname, 'data', 'fichas.json')

// Helpers de fichas
async function readFichas() {
  try { return JSON.parse(await fs.readFile(FICHAS_FILE, 'utf8')) } catch { return [] }
}
async function writeFichas(fichas) {
  await fs.mkdir(path.dirname(FICHAS_FILE), { recursive: true })
  await fs.writeFile(FICHAS_FILE, JSON.stringify(fichas, null, 2), 'utf8')
}

// ── Rutas — fichas ─────────────────────────────────────────────────────────

app.get('/api/fichas', async (_req, res) => {
  res.json(await readFichas())
})

app.post('/api/fichas', async (req, res) => {
  const datos = req.body
  if (!datos.apellidoNombre || !datos.dni)
    return res.status(400).json({ error: 'Faltan campos obligatorios: apellidoNombre, dni' })

  const fichas = await readFichas()

  // Si ya existe el DNI, actualiza. Si no, crea.
  const idx = fichas.findIndex(f => String(f.dni) === String(datos.dni))
  const ficha = {
    ...datos,
    dni: String(datos.dni),
    guardado_en: new Date().toISOString()
  }

  if (idx >= 0) {
    fichas[idx] = { ...fichas[idx], ...ficha }
    console.log(`[fichas] actualizado: ${datos.apellidoNombre} (${datos.dni})`)
  } else {
    fichas.push(ficha)
    console.log(`[fichas] nuevo: ${datos.apellidoNombre} (${datos.dni}) | total: ${fichas.length}`)
  }

  await writeFichas(fichas)
  res.json({ ok: true, ficha })
})

app.delete('/api/fichas/:dni', async (req, res) => {
  const fichas = await readFichas()
  const nuevas = fichas.filter(f => String(f.dni) !== String(req.params.dni))
  await writeFichas(nuevas)
  res.json({ ok: true })
})