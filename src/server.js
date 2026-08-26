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

const renderJobs = new Map();

function publicOutputUrl(filename) {
  return PUBLIC_BASE_URL
    ? `${PUBLIC_BASE_URL}/outputs/${filename}`
    : `/outputs/${filename}`;
}

function publicDownloadUrl(filename) {
  return PUBLIC_BASE_URL
    ? `${PUBLIC_BASE_URL}/download/${filename}`
    : `/download/${filename}`;
}

function publicStatusUrl(jobId) {
  return PUBLIC_BASE_URL
    ? `${PUBLIC_BASE_URL}/render/status/${encodeURIComponent(jobId)}`
    : `/render/status/${encodeURIComponent(jobId)}`;
}

function safeJobId(job) {
  const raw = String(job && job.jobId || '').trim();
  return raw || `olivia-${Date.now().toString(36)}-${Math.random().toString(36).slice(2,10)}`;
}

function normaliseProgress(value, fallback = 0) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(0, Math.min(100, Math.round(n)));
}

function updateJobProgress(state, value, phase, detail = null) {
  if (!state) return;
  const next = normaliseProgress(value, state.progress || 0);
  if (next >= (state.progress || 0)) state.progress = next;
  if (phase) state.phase = String(phase);
  if (detail !== undefined) state.progressDetail = detail;
  state.updatedAt = new Date().toISOString();
}

async function runRenderInBackground(job) {
  const jobId = safeJobId(job);
  const state = renderJobs.get(jobId);
  if (!state) return;

  state.status = 'processing';
  state.startedAt = new Date().toISOString();
  updateJobProgress(state, 1, 'preparing');

  try {
    const started = Date.now();
    const result = await renderJob(job, {
      outputDir: OUTPUT_DIR,
      onProgress(progress, phase, detail) {
        updateJobProgress(state, progress, phase, detail);
      }
    });
    const filename = path.basename(result.outputPath);

    state.status = 'completed';
    updateJobProgress(state, 100, 'completed');
    state.completedAt = new Date().toISOString();
    state.elapsedMs = Date.now() - started;
    state.outputUrl = publicOutputUrl(filename);
    state.downloadUrl = publicDownloadUrl(filename);
    state.result = {
      ...result,
      outputUrl: state.outputUrl,
      downloadUrl: state.downloadUrl,
      elapsedMs: state.elapsedMs
    };
    delete state.result.outputPath;
  } catch (error) {
    console.error('[OLIVIA ASYNC RENDER ERROR]', error);
    state.status = 'failed';
    state.phase = 'failed';
    state.updatedAt = new Date().toISOString();
    state.completedAt = new Date().toISOString();
    state.error = error.message;
    state.code = error.code || 'RENDER_FAILED';
    state.details = error.details || null;
    state.ffmpeg = error.stderr ? String(error.stderr).slice(-12000) : null;
  }
}

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

async function serveDownload(req, res, pathname) {
  const name = path.basename(pathname.slice('/download/'.length));
  if (!/^[a-zA-Z0-9._-]+\.mp4$/.test(name)) return json(res, 400, { error: 'Invalid output name.' });
  const filePath = path.join(OUTPUT_DIR, name);
  try {
    const info = await stat(filePath);
    res.writeHead(200, {
      'content-type': 'video/mp4',
      'content-length': info.size,
      'content-disposition': `attachment; filename="${name}"`,
      'cache-control': 'private, no-store'
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
      worker: 'V119-TRUE-PROGRESS',
      ffmpeg: version
    });
  }

  if (req.method === 'GET' && url.pathname.startsWith('/outputs/')) {
    return serveOutput(req, res, url.pathname);
  }

  if (req.method === 'GET' && url.pathname.startsWith('/download/')) {
    return serveDownload(req, res, url.pathname);
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


  if (req.method === 'POST' && url.pathname === '/render/start') {
    try {
      const job = await readJson(req);
      const validation = validateJob(job);
      if (!validation.ok) {
        return json(res, 400, {
          schema: 'OLIVIA_RENDER_ACCEPTED_V1',
          status: 'rejected',
          errors: validation.errors
        });
      }

      const support = inspectSupport(job.manifest);
      if (support.blocking && support.blocking.length) {
        return json(res, 422, {
          schema: 'OLIVIA_RENDER_ACCEPTED_V1',
          status: 'rejected',
          errors: support.blocking,
          warnings: support.warnings || []
        });
      }

      const jobId = safeJobId(job);

      if (renderJobs.has(jobId)) {
        const existing = renderJobs.get(jobId);
        return json(res, 200, {
          schema: 'OLIVIA_RENDER_ACCEPTED_V1',
          jobId,
          status: existing.status,
          progress: existing.progress ?? 0,
          phase: existing.phase || existing.status,
          statusUrl: publicStatusUrl(jobId),
          duplicate: true
        });
      }

      renderJobs.set(jobId, {
        jobId,
        status: 'queued',
        progress: 0,
        phase: 'queued',
        progressDetail: null,
        updatedAt: new Date().toISOString(),
        queuedAt: new Date().toISOString(),
        startedAt: null,
        completedAt: null,
        outputUrl: null,
        downloadUrl: null,
        error: null,
        code: null,
        details: null,
        ffmpeg: null,
        elapsedMs: null,
        result: null
      });

      setImmediate(() => {
        runRenderInBackground({ ...job, jobId }).catch(error => {
          console.error('[OLIVIA BACKGROUND TASK ERROR]', error);
        });
      });

      return json(res, 202, {
        schema: 'OLIVIA_RENDER_ACCEPTED_V1',
        jobId,
        status: 'queued',
        progress: 0,
        phase: 'queued',
        statusUrl: publicStatusUrl(jobId)
      });
    } catch (error) {
      return json(res, error.status || 400, {
        schema: 'OLIVIA_RENDER_ACCEPTED_V1',
        status: 'rejected',
        error: error.message
      });
    }
  }

  if (req.method === 'GET' && url.pathname.startsWith('/render/status/')) {
    const jobId = decodeURIComponent(url.pathname.slice('/render/status/'.length));
    const state = renderJobs.get(jobId);

    if (!state) {
      return json(res, 404, {
        schema: 'OLIVIA_RENDER_STATUS_V1',
        jobId,
        status: 'not_found',
        error: 'Render job is not present in this worker process.'
      });
    }

    return json(res, 200, {
      schema: 'OLIVIA_RENDER_STATUS_V1',
      jobId,
      status: state.status,
      progress: state.progress ?? 0,
      phase: state.phase || state.status,
      progressDetail: state.progressDetail || null,
      updatedAt: state.updatedAt || null,
      queuedAt: state.queuedAt,
      startedAt: state.startedAt,
      completedAt: state.completedAt,
      outputUrl: state.outputUrl,
      downloadUrl: state.downloadUrl,
      error: state.error,
      code: state.code,
      details: state.details,
      elapsedMs: state.elapsedMs,
      result: state.result
    });
  }

  if (req.method === 'POST' && url.pathname === '/render') {
    const started = Date.now();
    try {
      const job = await readJson(req);
      const result = await renderJob(job, { outputDir: OUTPUT_DIR });
      const filename = path.basename(result.outputPath);
      result.outputUrl = publicOutputUrl(filename);
      result.downloadUrl = publicDownloadUrl(filename);
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
