import http from 'node:http';
import { readFile, stat } from 'node:fs/promises';
import { createReadStream } from 'node:fs';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { renderJob, validateJob, inspectSupport, chooseOutputSize } from './render-engine.js';

const PORT = Number(process.env.PORT || 8080);
const OUTPUT_DIR = process.env.OUTPUT_DIR || '/data/outputs';
const MAX_BODY_BYTES = Number(process.env.MAX_BODY_BYTES || 8_000_000);
const WORKER_TOKEN = String(process.env.WORKER_TOKEN || '');
const PUBLIC_BASE_URL = String(process.env.PUBLIC_BASE_URL || '').replace(/\/$/, '');

function json(res, status, body) {
  const payload = Buffer.from(JSON.stringify(body, null, 2));
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'content-length': payload.length,
    'cache-control': 'no-store'
  });
  res.end(payload);
}

function authorized(req) {
  if (!WORKER_TOKEN) return true;
  return req.headers.authorization === `Bearer ${WORKER_TOKEN}`;
}

async function readJson(req) {
  let total = 0;
  const chunks = [];
  for await (const chunk of req) {
    total += chunk.length;
    if (total > MAX_BODY_BYTES) throw Object.assign(new Error('Request body too large.'), { status: 413 });
    chunks.push(chunk);
  }
  const raw = Buffer.concat(chunks).toString('utf8');
  return JSON.parse(raw || '{}');
}

async function ffmpegVersion() {
  return new Promise(resolve => {
    const child = spawn('ffmpeg', ['-version']);
    let out = '';
    child.stdout.on('data', d => { out += d.toString(); });
    child.on('error', () => resolve(null));
    child.on('close', code => resolve(code === 0 ? out.split('\n')[0] : null));
  });
}

async function serveOutput(req, res, pathname) {
  const name = path.basename(pathname.slice('/outputs/'.length));
  if (!/^[a-zA-Z0-9._-]+\.mp4$/.test(name)) return json(res, 400, { error: 'Invalid output name.' });
  const filePath = path.join(OUTPUT_DIR, name);
  try {
    const info = await stat(filePath);
    res.writeHead(200, {
      'content-type': 'video/mp4',
      'content-length': info.size,
      'accept-ranges': 'bytes',
      'cache-control': 'private, max-age=3600'
    });
    createReadStream(filePath).pipe(res);
  } catch {
    json(res, 404, { error: 'Output not found.' });
  }
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);

  if (req.method === 'GET' && url.pathname === '/health') {
    const version = await ffmpegVersion();
    return json(res, version ? 200 : 503, {
      ok: Boolean(version),
      service: 'olivia-render-worker',
      worker: 'V1',
      ffmpeg: version
    });
  }

  if (req.method === 'GET' && url.pathname.startsWith('/outputs/')) {
    return serveOutput(req, res, url.pathname);
  }

  if (!authorized(req)) return json(res, 401, { error: 'Unauthorized.' });

  if (req.method === 'POST' && url.pathname === '/plan') {
    try {
      const job = await readJson(req);
      const validation = validateJob(job);
      if (!validation.ok) return json(res, 400, { ok: false, errors: validation.errors });
      return json(res, 200, {
        ok: true,
        schema: job.schema,
        jobId: job.jobId,
        output: chooseOutputSize(job.manifest),
        support: inspectSupport(job.manifest)
      });
    } catch (error) {
      return json(res, error.status || 400, { error: error.message });
    }
  }

  if (req.method === 'POST' && url.pathname === '/render') {
    const started = Date.now();
    try {
      const job = await readJson(req);
      const result = await renderJob(job, { outputDir: OUTPUT_DIR });
      const filename = path.basename(result.outputPath);
      result.outputUrl = PUBLIC_BASE_URL ? `${PUBLIC_BASE_URL}/outputs/${filename}` : `/outputs/${filename}`;
      result.elapsedMs = Date.now() - started;
      delete result.outputPath;
      return json(res, 200, result);
    } catch (error) {
      console.error('[OLIVIA RENDER ERROR]', error);
      return json(res, error.status || 422, {
        schema: 'OLIVIA_RENDER_RESULT_V1',
        status: 'failed',
        error: error.message,
        code: error.code || 'RENDER_FAILED',
        details: error.details || null,
        ffmpeg: error.stderr ? String(error.stderr).slice(-12000) : null,
        elapsedMs: Date.now() - started
      });
    }
  }

  json(res, 404, { error: 'Not found.' });
});

server.listen(PORT, () => {
  console.log(`OLIVIA render worker listening on :${PORT}`);
});
