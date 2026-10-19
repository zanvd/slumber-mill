import { randomBytes, randomUUID } from 'node:crypto'
import { expect, test } from '@playwright/test'

const baseURL = process.env.SLUMBER_MILL_BASE_URL
const apiKey = process.env.SLUMBER_MILL_PROBE_API_KEY

// Checkly stops a multistep run after 240 s, so the test and the wait for `ready` end well before that.
const testTimeoutMs = 210_000
const readyTimeoutMs = 180_000
const pollIntervalMs = 2_000

const sampleRate = 48_000
const channels = 2
const seconds = 10
const amplitude = 0.3
const seed = 0x5eed

function mulberry32(state: number): () => number {
  return () => {
    state = (state + 0x6d2b79f5) | 0
    let t = Math.imul(state ^ (state >>> 15), 1 | state)
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }
}

// Stereo pink noise with an independent signal per channel, like a field recording, as a 16-bit PCM WAV.
// Pink rather than white: the 64k encoder's low-pass takes less of its loudness, so both renditions land on
// target without a corrective re-encode. The filter is Paul Kellet's economy approximation of -3 dB/octave.
function pinkNoiseWav(): Buffer {
  const frames = sampleRate * seconds
  const dataBytes = frames * channels * 2
  const wav = Buffer.alloc(44 + dataBytes)
  wav.write('RIFF', 0, 'ascii')
  wav.writeUInt32LE(36 + dataBytes, 4)
  wav.write('WAVE', 8, 'ascii')
  wav.write('fmt ', 12, 'ascii')
  wav.writeUInt32LE(16, 16)
  wav.writeUInt16LE(1, 20)
  wav.writeUInt16LE(channels, 22)
  wav.writeUInt32LE(sampleRate, 24)
  wav.writeUInt32LE(sampleRate * channels * 2, 28)
  wav.writeUInt16LE(channels * 2, 32)
  wav.writeUInt16LE(16, 34)
  wav.write('data', 36, 'ascii')
  wav.writeUInt32LE(dataBytes, 40)

  const random = mulberry32(seed)
  const state = Array.from({ length: channels }, () => [0, 0, 0])
  let offset = 44
  for (let i = 0; i < frames; i++) {
    for (let c = 0; c < channels; c++) {
      const white = random() * 2 - 1
      const b = state[c]
      b[0] = 0.99765 * b[0] + white * 0.099046
      b[1] = 0.963 * b[1] + white * 0.2965164
      b[2] = 0.57 * b[2] + white * 1.0526913
      const pink = (b[0] + b[1] + b[2] + white * 0.1848) * 0.25
      const sample = Math.max(-1, Math.min(1, pink * amplitude))
      wav.writeInt16LE(Math.round(sample * 32767), offset)
      offset += 2
    }
  }
  return wav
}

function uuidV7(): string {
  const bytes = randomBytes(16)
  bytes.writeUIntBE(Date.now(), 0, 6)
  bytes[6] = (bytes[6] & 0x0f) | 0x70
  bytes[8] = (bytes[8] & 0x3f) | 0x80
  const hex = bytes.toString('hex')
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`
}

test('ingest round trip', async ({ request }) => {
  test.setTimeout(testTimeoutMs)
  expect(baseURL, 'SLUMBER_MILL_BASE_URL is set').toBeTruthy()
  expect(apiKey, 'SLUMBER_MILL_PROBE_API_KEY is set').toBeTruthy()
  const auth = { Authorization: `Bearer ${apiKey}` }

  const id = await test.step('upload', async () => {
    const res = await request.post(`${baseURL}/v1/assets`, {
      headers: { ...auth, 'Idempotency-Key': randomUUID() },
      multipart: {
        title: `checkly probe ${new Date().toISOString()}`,
        file: { name: 'probe.wav', mimeType: 'audio/wav', buffer: pinkNoiseWav() },
      },
    })
    expect(res.status(), await res.text()).toBe(202)
    expect(res.headers()['location']).toBeTruthy()
    const asset = await res.json()
    expect(['queued', 'processing']).toContain(asset.status)
    return asset.id as string
  })

  await test.step('wait for ready', async () => {
    const deadline = Date.now() + readyTimeoutMs
    for (;;) {
      const res = await request.get(`${baseURL}/v1/assets/${id}`, { headers: auth })
      expect(res.status(), await res.text()).toBe(200)
      const asset = await res.json()
      if (asset.status === 'ready') {
        return
      }
      expect(asset.status, `asset ${id} failed: ${asset.error}`).not.toBe('failed')
      expect(Date.now(), `asset ${id} still ${asset.status} after ${readyTimeoutMs / 1000} s`).toBeLessThan(deadline)
      await new Promise((resolve) => setTimeout(resolve, pollIntervalMs))
    }
  })

  await test.step('download a range of aac_64k', async () => {
    const res = await request.get(`${baseURL}/v1/assets/${id}/renditions/aac_64k`, {
      headers: { ...auth, Range: 'bytes=0-1023' },
    })
    expect(res.status()).toBe(206)
    expect(res.headers()['etag']).toBeTruthy()
    expect((await res.body()).length).toBe(1024)
  })

  await test.step('unknown asset is 404', async () => {
    const res = await request.get(`${baseURL}/v1/assets/${uuidV7()}`, { headers: auth })
    expect(res.status()).toBe(404)
  })
})
