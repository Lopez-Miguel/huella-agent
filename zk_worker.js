import { parentPort } from 'worker_threads'
import koffi from 'koffi'
import path from 'path'
import { fileURLToPath } from 'url'

const __dirname = path.dirname(fileURLToPath(import.meta.url))
const DLL_PATH  = path.join(__dirname, 'sdk', 'libzkfp.dll')

const ZKFP_ERR_OK       = 0
const MAX_TEMPLATE_SIZE = 2048

// ── DLL ────────────────────────────────────────────────────────────────────

let lib
try { lib = koffi.load(DLL_PATH) } catch (e) {
  parentPort.postMessage({ type: 'error', msg: `No se pudo cargar libzkfp.dll: ${e.message}` })
  process.exit(1)
}

const ZKFPM_Init        = lib.func('int __stdcall ZKFPM_Init()')
const ZKFPM_Terminate   = lib.func('void __stdcall ZKFPM_Terminate()')
const ZKFPM_OpenDevice  = lib.func('void * __stdcall ZKFPM_OpenDevice(int index)')
const ZKFPM_CloseDevice = lib.func('void __stdcall ZKFPM_CloseDevice(void * hDevice)')
const ZKFPM_GetParameters = lib.func(
  'int __stdcall ZKFPM_GetParameters(void * hDevice, int paramCode, uint8_t * paramValue, uint32_t * paramLen)'
)
const ZKFPM_AcquireFingerprint = lib.func(
  'int __stdcall ZKFPM_AcquireFingerprint(void * hDevice, uint8_t * imgBuf, uint32_t imgBufSize, uint8_t * fpTemplate, uint32_t * templateLen)'
)
const ZKFPM_DBInit     = lib.func('void * __stdcall ZKFPM_DBInit()')
const ZKFPM_DBFree     = lib.func('void __stdcall ZKFPM_DBFree(void * hDBCache)')
const ZKFPM_DBClear    = lib.func('void __stdcall ZKFPM_DBClear(void * hDBCache)')
const ZKFPM_DBAdd      = lib.func(
  'int __stdcall ZKFPM_DBAdd(void * hDBCache, uint32_t tid, uint8_t * fpTemplate, uint32_t templateLen)'
)
const ZKFPM_DBMerge    = lib.func(
  'int __stdcall ZKFPM_DBMerge(void * hDBCache, uint8_t * t1, uint8_t * t2, uint8_t * t3, uint8_t * regTemplate, uint32_t * cbRegTemplate)'
)
const ZKFPM_DBIdentify = lib.func(
  'int __stdcall ZKFPM_DBIdentify(void * hDBCache, uint8_t * fpTemplate, uint32_t templateLen, uint32_t * tid, uint32_t * score)'
)

// ── Estado de cada device ──────────────────────────────────────────────────
// Ambos son iguales: cualquiera puede enrolar o identificar

function makeDevice(index) {
  return {
    index,
    handle:   null,
    dbCache:  null,
    imgBuf:   null,
    imgWidth: 0, imgHeight: 0,
    // Buffers privados para el loop unificado
    tmplBuf: Buffer.alloc(MAX_TEMPLATE_SIZE),
    tmplLen: Buffer.alloc(4),
    // Modo activo: 'idle' | 'enroll' | 'identify'
    mode: 'idle',
    paused: false,       // pausa entre capturas (levantá el dedo)
    // Enrolamiento
    enrollTemplates: [],
    // Identificación
    identifyMap: {},
  }
}

const devices = [makeDevice(0), makeDevice(1)]

// ── Loop unificado ─────────────────────────────────────────────────────────
// Un solo setTimeout que recorre ambos devices en secuencia.
// Como AcquireFingerprint retorna inmediato (-7) cuando no hay dedo,
// ambos devices se procesan sin bloquearse mutuamente.

let globalLoop = null

function startLoop() {
  if (globalLoop) return

  function tick() {
    for (const dev of devices) {
      if (!dev.handle || dev.mode === 'idle' || dev.paused) continue
      pollDevice(dev)
    }
    globalLoop = setTimeout(tick, 50)
  }

  tick()
}

function stopLoop() {
  if (globalLoop) { clearTimeout(globalLoop); globalLoop = null }
}

function pollDevice(dev) {
  dev.tmplLen.writeUInt32LE(MAX_TEMPLATE_SIZE, 0)

  const ret = ZKFPM_AcquireFingerprint(
    dev.handle, dev.imgBuf,
    dev.imgWidth * dev.imgHeight,
    dev.tmplBuf, dev.tmplLen
  )

  if (ret !== ZKFP_ERR_OK) return   // -7 = sin dedo, normal

  const len = dev.tmplLen.readUInt32LE(0)

  if (dev.mode === 'enroll')    processEnroll(dev, len)
  if (dev.mode === 'identify')  processIdentify(dev, len)
}

// ── Helpers ────────────────────────────────────────────────────────────────

function getParam(handle, code) {
  const buf = Buffer.alloc(4), len = Buffer.alloc(4)
  len.writeUInt32LE(4, 0)
  return ZKFPM_GetParameters(handle, code, buf, len) === ZKFP_ERR_OK ? buf.readUInt32LE(0) : null
}

const send = data => parentPort.postMessage(data)

function pause(dev, ms) {
  dev.paused = true
  setTimeout(() => { dev.paused = false }, ms)
}

// ── Comandos ───────────────────────────────────────────────────────────────

parentPort.on('message', ({ cmd, deviceIndex, templates }) => {
  switch (cmd) {
    case 'connect':       connect();                          break
    case 'disconnect':    disconnect();                       break
    // deviceIndex opcional (default 0). Así cualquier lector puede enrolar
    case 'startEnroll':   startEnroll(deviceIndex ?? 0);     break
    case 'cancelEnroll':  cancelEnroll(deviceIndex ?? 0);    break
    // Identificación: activa TODOS los lectores conectados simultáneamente
    case 'startIdentify': startIdentify(templates);          break
    case 'stopIdentify':  stopIdentify();                    break
  }
})

// ── Connect ────────────────────────────────────────────────────────────────

function connect() {
  const r = ZKFPM_Init()
  if (r !== ZKFP_ERR_OK) return send({ type: 'error', msg: `ZKFPM_Init falló: ${r}` })

  let opened = 0
  for (const dev of devices) {
    dev.handle = ZKFPM_OpenDevice(dev.index)
    if (!dev.handle) { send({ type: 'deviceMissing', index: dev.index }); continue }

    dev.dbCache   = ZKFPM_DBInit()
    dev.imgWidth  = getParam(dev.handle, 1) || 256
    dev.imgHeight = getParam(dev.handle, 2) || 360
    dev.imgBuf    = Buffer.alloc(dev.imgWidth * dev.imgHeight)
    opened++
    send({ type: 'deviceConnected', index: dev.index, width: dev.imgWidth, height: dev.imgHeight })
  }

  if (opened === 0) { ZKFPM_Terminate(); return send({ type: 'error', msg: 'No se encontró ningún lector.' }) }
  send({ type: 'connected', count: opened })
}

function disconnect() {
  stopLoop()
  for (const dev of devices) {
    dev.mode = 'idle'; dev.enrollTemplates = []; dev.identifyMap = {}
    if (dev.dbCache) { ZKFPM_DBFree(dev.dbCache); dev.dbCache = null }
    if (dev.handle)  { ZKFPM_CloseDevice(dev.handle); dev.handle = null }
  }
  ZKFPM_Terminate()
  send({ type: 'disconnected' })
}

// ── Enrolamiento ───────────────────────────────────────────────────────────

function startEnroll(idx) {
  const dev = devices[idx]
  if (!dev?.handle) return send({ type: 'error', msg: `Lector ${idx} no disponible` })

  dev.mode = 'enroll'; dev.paused = false; dev.enrollTemplates = []
  startLoop()
}

function cancelEnroll(idx) {
  const dev = devices[idx]
  if (!dev) return
  dev.mode = 'idle'; dev.enrollTemplates = []

  // Si ningún device necesita el loop, lo detenemos
  if (devices.every(d => d.mode === 'idle')) stopLoop()
}

function processEnroll(dev, len) {
  const tpl = Buffer.alloc(MAX_TEMPLATE_SIZE)
  dev.tmplBuf.copy(tpl, 0, 0, len)
  dev.enrollTemplates.push({ buf: tpl, len })

  const step = dev.enrollTemplates.length
  send({ type: 'enrollStep', step, deviceIndex: dev.index })
  pause(dev, 2000)

  if (step >= 3) {
    dev.mode = 'idle'
    doMerge(dev)
  }
}

function doMerge(dev) {
  const out = Buffer.alloc(MAX_TEMPLATE_SIZE), outLen = Buffer.alloc(4)
  outLen.writeUInt32LE(MAX_TEMPLATE_SIZE, 0)

  const ret = ZKFPM_DBMerge(
    dev.dbCache,
    dev.enrollTemplates[0].buf,
    dev.enrollTemplates[1].buf,
    dev.enrollTemplates[2].buf,
    out, outLen
  )
  dev.enrollTemplates = []

  if (ret === ZKFP_ERR_OK) {
    const len = outLen.readUInt32LE(0)
    send({ type: 'enrollComplete', templateBase64: out.slice(0, len).toString('base64'), len, deviceIndex: dev.index })
  } else {
    send({ type: 'enrollError', ret, msg: `DBMerge falló (${ret}). Intentá de nuevo.`, deviceIndex: dev.index })
  }

  if (devices.every(d => d.mode === 'idle')) stopLoop()
}

// ── Identificación ─────────────────────────────────────────────────────────
// Carga los templates en TODOS los lectores conectados
// y activa el loop unificado. Cualquiera que detecte una huella
// dispara el evento 'identified'.

function startIdentify(templates) {
  let ready = 0
  for (const dev of devices) {
    if (!dev.handle || !dev.dbCache) continue

    ZKFPM_DBClear(dev.dbCache)
    dev.identifyMap = {}

    for (const t of templates) {
      const buf = Buffer.from(t.templateBase64, 'base64')
      const ret = ZKFPM_DBAdd(dev.dbCache, t.id, buf, buf.length)
      if (ret === ZKFP_ERR_OK) dev.identifyMap[t.id] = { nombre: t.nombre, dni: t.dni }
      else console.error(`[worker] DBAdd dev=${dev.index} id=${t.id} ret=${ret}`)
    }

    dev.mode = 'identify'; dev.paused = false
    ready++
  }

  if (ready === 0) return send({ type: 'error', msg: 'Ningún lector disponible para identificación' })

  send({ type: 'identifyReady', count: Object.keys(devices.find(d => d.identifyMap).identifyMap).length, devices: ready })
  startLoop()
}

function stopIdentify() {
  for (const dev of devices) {
    if (dev.mode !== 'identify') continue
    dev.mode = 'idle'; dev.identifyMap = {}
    if (dev.dbCache) ZKFPM_DBClear(dev.dbCache)
  }
  if (devices.every(d => d.mode === 'idle')) stopLoop()
  send({ type: 'identifyStopped' })
}

function processIdentify(dev, len) {
  const tidBuf = Buffer.alloc(4), scoreBuf = Buffer.alloc(4)
  const ret = ZKFPM_DBIdentify(dev.dbCache, dev.tmplBuf, len, tidBuf, scoreBuf)

  pause(dev, 2000)

  if (ret === ZKFP_ERR_OK) {
    const tid   = tidBuf.readUInt32LE(0)
    const score = scoreBuf.readUInt32LE(0)
    const info  = dev.identifyMap[tid] || { nombre: 'Desconocido', dni: '-' }
    send({ type: 'identified', tid, score, nombre: info.nombre, dni: info.dni, deviceIndex: dev.index })
  } else {
    send({ type: 'notIdentified', deviceIndex: dev.index })
  }
}

// ── Cleanup ────────────────────────────────────────────────────────────────

process.on('exit', () => {
  stopLoop()
  for (const dev of devices) {
    if (dev.dbCache) ZKFPM_DBFree(dev.dbCache)
    if (dev.handle)  ZKFPM_CloseDevice(dev.handle)
  }
  ZKFPM_Terminate()
})