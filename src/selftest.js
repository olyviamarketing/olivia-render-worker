import { mkdir, rm } from 'node:fs/promises';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { pathToFileURL } from 'node:url';
import { renderJob } from './render-engine.js';

function run(command, args) {
  return new Promise((resolve, reject) => {
    const p = spawn(command, args, { stdio: ['ignore', 'pipe', 'pipe'] });
    let err = '';
    p.stderr.on('data', d => { err += d.toString(); });
    p.on('error', reject);
    p.on('close', code => code === 0 ? resolve() : reject(new Error(err)));
  });
}

const root = '/tmp/olivia-worker-selftest';
await rm(root, { recursive: true, force: true });
await mkdir(path.join(root, 'source'), { recursive: true });
await mkdir(path.join(root, 'out'), { recursive: true });
const source = path.join(root, 'source', 'input.mp4');

await run('ffmpeg', [
  '-hide_banner', '-y',
  '-f', 'lavfi', '-i', 'testsrc2=size=640x360:rate=30:duration=6',
  '-f', 'lavfi', '-i', 'sine=frequency=440:sample_rate=48000:duration=6',
  '-c:v', 'libx264', '-pix_fmt', 'yuv420p', '-c:a', 'aac', '-shortest', source
]);

const job = {
  schema: 'OLIVIA_RENDER_JOB_V1',
  jobId: 'selftest',
  requestedAt: new Date().toISOString(),
  status: 'queued',
  manifest: {
    schema: 'OLIVIA_RENDER_MANIFEST_V1',
    source: { url: pathToFileURL(source).href, durationSeconds: 6, width: 640, height: 360 },
    output: { requestedAspectRatio: '16:9' },
    timeline: {
      durationSeconds: 4,
      clips: [
        {
          id: 'a', clipNumber: 1, order: 0,
          source: { inSeconds: 0, outSeconds: 2, durationSeconds: 2 },
          sequence: { startSeconds: 0, endSeconds: 2 },
          video: { fadeInSeconds: 0.2, fadeOutSeconds: 0, transitionOut: { type: 'dip-black', durationSeconds: 0.6 }, color: { brightness: 1, contrast: 1, saturation: 1, warmth: 0, tint: 0, hueDegrees: 0 } },
          audio: { muted: false, solo: false, audibleUnderSoloRule: true, offsetSeconds: 0, sourceInSeconds: 0, sourceOutSeconds: 2, volume: 1, volumeAutomation: { startLevel: 1, endLevel: 0.7, startPosition: 0, endPosition: 1 }, pan: 0, fadeInSeconds: 0.1, fadeOutSeconds: 0.1 }
        },
        {
          id: 'b', clipNumber: 2, order: 1,
          source: { inSeconds: 3, outSeconds: 5, durationSeconds: 2 },
          sequence: { startSeconds: 2, endSeconds: 4 },
          video: { fadeInSeconds: 0, fadeOutSeconds: 0.2, transitionOut: { type: 'none', durationSeconds: 0.8 }, color: { brightness: 1.05, contrast: 1.05, saturation: 1.1, warmth: 0, tint: 0, hueDegrees: 8 } },
          audio: { muted: false, solo: false, audibleUnderSoloRule: true, offsetSeconds: 0.2, sourceInSeconds: 3, sourceOutSeconds: 5, volume: 0.8, volumeAutomation: { startLevel: 1, endLevel: 1, startPosition: 0, endPosition: 1 }, pan: -0.25, fadeInSeconds: 0.1, fadeOutSeconds: 0.2 }
        }
      ]
    },
    overlays: []
  }
};

// fetch(file://) is not portable in Node, so temporarily replace downloader path by serving it through local HTTP.
const http = await import('node:http');
const fs = await import('node:fs');
const server = http.createServer((req, res) => {
  res.writeHead(200, { 'content-type': 'video/mp4' });
  fs.createReadStream(source).pipe(res);
});
await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
const port = server.address().port;
job.manifest.source.url = `http://127.0.0.1:${port}/input.mp4`;

try {
  const result = await renderJob(job, { outputDir: path.join(root, 'out'), tempRoot: path.join(root, 'tmp') });
  console.log(JSON.stringify({ ok: true, result }, null, 2));
} finally {
  await new Promise(resolve => server.close(resolve));
}
