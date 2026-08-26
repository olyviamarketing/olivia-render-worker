import { spawn } from 'node:child_process';
import { createWriteStream } from 'node:fs';
import { mkdir, writeFile, rm, copyFile } from 'node:fs/promises';
import { pipeline } from 'node:stream/promises';
import { Readable } from 'node:stream';
import path from 'node:path';

const EPS = 1e-6;

function n(value, fallback = 0) {
  const x = Number(value);
  return Number.isFinite(x) ? x : fallback;
}

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

function even(value) {
  const x = Math.max(2, Math.round(value));
  return x % 2 === 0 ? x : x - 1;
}

function safeId(value) {
  return String(value || 'job').replace(/[^a-zA-Z0-9._-]/g, '_').slice(0, 96);
}

function ffPath(value) {
  return String(value).replace(/\\/g, '/').replace(/'/g, "\\'").replace(/:/g, '\\:');
}

function escapeExprString(value) {
  return String(value).replace(/'/g, "\\'");
}

function parseFfmpegTimeSeconds(text) {
  const input = String(text || '');
  const re = /time=(\d{1,2}):(\d{2}):(\d{2}(?:\.\d+)?)/g;
  let match;
  let latest = null;
  while ((match = re.exec(input)) !== null) {
    latest = Number(match[1]) * 3600 + Number(match[2]) * 60 + Number(match[3]);
  }
  return Number.isFinite(latest) ? latest : null;
}

function emitProgress(onProgress, progress, phase, detail = null) {
  if (typeof onProgress !== 'function') return;
  const value = clamp(Math.round(n(progress, 0)), 0, 99);
  try {
    onProgress(value, phase, detail);
  } catch {
    // Progress reporting must never be allowed to fail a render.
  }
}

export function validateJob(job) {
  const errors = [];
  if (!job || typeof job !== 'object') errors.push('Request body must be an object.');
  if (job?.schema !== 'OLIVIA_RENDER_JOB_V1') errors.push('schema must be OLIVIA_RENDER_JOB_V1.');
  const m = job?.manifest;
  if (!m || m.schema !== 'OLIVIA_RENDER_MANIFEST_V1') errors.push('manifest.schema must be OLIVIA_RENDER_MANIFEST_V1.');
  if (!m?.source?.url) errors.push('manifest.source.url is required.');
  if (!Array.isArray(m?.timeline?.clips) || m.timeline.clips.length === 0) errors.push('At least one active clip is required.');
  return { ok: errors.length === 0, errors };
}

function normaliseRequestedAspectRatio(value) {
  const raw = String(value || '').trim();
  const compact = raw.toLowerCase().replace(/\s/g, '');

  if (compact.includes('9:16') || compact.includes('vertical')) return '9:16';
  if (compact.includes('16:9') || compact.includes('landscape')) return '16:9';
  if (compact.includes('1:1') || compact.includes('square')) return '1:1';

  return raw.replace(/\s/g, '');
}

export function chooseOutputSize(manifest) {
  const aspect = normaliseRequestedAspectRatio(manifest?.output?.requestedAspectRatio);
  const longEdge = Math.max(640, n(process.env.LOW_MEMORY_LONG_EDGE, 1280));

  if (aspect === '9:16') return { width: 720, height: 1280, aspect };
  if (aspect === '1:1') return { width: 720, height: 720, aspect };
  if (aspect === '16:9') return { width: 1280, height: 720, aspect };

  const sourceW = Math.max(2, n(manifest?.source?.width, 1280));
  const sourceH = Math.max(2, n(manifest?.source?.height, 720));
  const sourceLong = Math.max(sourceW, sourceH);
  const scale = Math.min(1, longEdge / sourceLong);

  return {
    width: even(sourceW * scale),
    height: even(sourceH * scale),
    aspect: aspect || 'source'
  };
}

export function inspectSupport(manifest) {
  const blocking = [];
  const warnings = [];
  const clips = manifest?.timeline?.clips || [];

  for (const clip of clips) {
    const transition = clip?.video?.transitionOut?.type || 'none';
    if (!['none', 'dip-black', 'dip-white'].includes(transition)) {
      blocking.push(`Clip ${clip.clipNumber ?? clip.id}: transition '${transition}' is not yet supported by Worker V1.`);
    }
    const color = clip?.video?.color || {};
    if (Math.abs(n(color.warmth)) > EPS || Math.abs(n(color.tint)) > EPS) {
      blocking.push(`Clip ${clip.clipNumber ?? clip.id}: warmth/tint require Worker V2 CSS-colour matching.`);
    }
  }

  const overlays = manifest?.overlays || [];
  for (const overlay of overlays) {
    if (String(overlay.text || '').includes('\n')) {
      warnings.push(`Overlay ${overlay.id || '?'} is multiline; V1 drawtext wrapping may differ from the browser preview.`);
    }
  }

  return { blocking, warnings };
}

async function runProcess(command, args, { cwd, onStderr, captureLimit = 2_000_000 } = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      cwd,
      stdio: ['ignore', 'pipe', 'pipe'],
      env: {
        ...process.env,
        OMP_NUM_THREADS: process.env.OMP_NUM_THREADS || '1'
      }
    });

    let stdout = '';
    let stderr = '';

    function appendLimited(current, addition) {
      const next = current + addition;
      return next.length > captureLimit
        ? next.slice(-captureLimit)
        : next;
    }

    child.stdout.on('data', d => {
      stdout = appendLimited(stdout, d.toString());
    });

    child.stderr.on('data', d => {
      const s = d.toString();
      stderr = appendLimited(stderr, s);
      if (onStderr) onStderr(s);
    });

    child.on('error', reject);
    child.on('close', code => {
      if (code === 0) resolve({ stdout, stderr });
      else reject(Object.assign(
        new Error(`${command} exited with code ${code}`),
        { stdout, stderr, code }
      ));
    });
  });
}

export async function probeSource(sourcePath) {
  const { stdout } = await runProcess('ffprobe', [
    '-v', 'error',
    '-show_entries', 'stream=index,codec_type,width,height:format=duration',
    '-of', 'json',
    sourcePath
  ]);
  const data = JSON.parse(stdout);
  return {
    duration: n(data?.format?.duration, 0),
    hasVideo: (data?.streams || []).some(s => s.codec_type === 'video'),
    hasAudio: (data?.streams || []).some(s => s.codec_type === 'audio'),
    width: n((data?.streams || []).find(s => s.codec_type === 'video')?.width, 0),
    height: n((data?.streams || []).find(s => s.codec_type === 'video')?.height, 0)
  };
}

export async function downloadSource(url, destination, maxBytes = 2_000_000_000, onProgress = null) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 120_000);
  try {
    const response = await fetch(url, { redirect: 'follow', signal: controller.signal });
    if (!response.ok || !response.body) throw new Error(`Source download failed: HTTP ${response.status}`);
    const length = n(response.headers.get('content-length'), 0);
    if (length > maxBytes) throw new Error(`Source is larger than MAX_SOURCE_BYTES (${maxBytes}).`);
    let received = 0;
    const reader = response.body.getReader();
    const stream = new Readable({
      async read() {
        try {
          const { done, value } = await reader.read();
          if (done) return this.push(null);
          received += value.byteLength;
          if (length > 0 && typeof onProgress === 'function') {
            onProgress(clamp(received / length, 0, 1));
          }
          if (received > maxBytes) {
            controller.abort();
            return this.destroy(new Error(`Source exceeded MAX_SOURCE_BYTES (${maxBytes}).`));
          }
          this.push(Buffer.from(value));
        } catch (error) {
          this.destroy(error);
        }
      }
    });
    await pipeline(stream, createWriteStream(destination));
    if (typeof onProgress === 'function') onProgress(1);
    return { bytes: received, contentType: response.headers.get('content-type') || '' };
  } finally {
    clearTimeout(timeout);
  }
}

function colorFilters(clip) {
  const c = clip?.video?.color || {};
  const brightness = clamp(n(c.brightness, 1), 0.5, 1.5);
  const contrast = clamp(n(c.contrast, 1), 0.5, 1.5);
  const saturation = clamp(n(c.saturation, 1), 0, 2);
  const hue = clamp(n(c.hueDegrees, 0), -180, 180);
  const filters = [];
  // CSS brightness is multiplicative; FFmpeg eq brightness is additive. This mapping
  // is intentionally conservative for V1 and is exact at neutral=1.
  filters.push(`eq=brightness=${(brightness - 1).toFixed(6)}:contrast=${contrast.toFixed(6)}:saturation=${saturation.toFixed(6)}`);
  if (Math.abs(hue) > EPS) filters.push(`hue=h=${hue.toFixed(4)}`);
  return filters;
}

function videoFadeFilters(clip, previousClip, duration) {
  const filters = [];
  const v = clip?.video || {};
  const fadeIn = clamp(n(v.fadeInSeconds), 0, duration);
  const fadeOut = clamp(n(v.fadeOutSeconds), 0, duration);
  if (fadeIn > EPS) filters.push(`fade=t=in:st=0:d=${fadeIn.toFixed(6)}:c=black`);
  if (fadeOut > EPS) filters.push(`fade=t=out:st=${Math.max(0, duration - fadeOut).toFixed(6)}:d=${fadeOut.toFixed(6)}:c=black`);

  const prevTransition = previousClip?.video?.transitionOut || { type: 'none', durationSeconds: 0 };
  if (prevTransition.type === 'dip-black' || prevTransition.type === 'dip-white') {
    const half = Math.max(0.1, n(prevTransition.durationSeconds, 0.8) / 2);
    const d = Math.min(duration, half);
    filters.push(`fade=t=in:st=0:d=${d.toFixed(6)}:c=${prevTransition.type === 'dip-white' ? 'white' : 'black'}`);
  }

  const ownTransition = v.transitionOut || { type: 'none', durationSeconds: 0 };
  if (ownTransition.type === 'dip-black' || ownTransition.type === 'dip-white') {
    const half = Math.max(0.1, n(ownTransition.durationSeconds, 0.8) / 2);
    const d = Math.min(duration, half);
    filters.push(`fade=t=out:st=${Math.max(0, duration - d).toFixed(6)}:d=${d.toFixed(6)}:c=${ownTransition.type === 'dip-white' ? 'white' : 'black'}`);
  }
  return filters;
}

function volumeExpression(audio, duration) {
  if (audio?.muted === true || audio?.audibleUnderSoloRule === false) return '0';
  const base = clamp(n(audio?.volume, 1), 0, 1);
  const va = audio?.volumeAutomation || {};
  const s = clamp(n(va.startPosition, 0), 0, 0.98) * duration;
  const e = clamp(n(va.endPosition, 1), 0.02, 1) * duration;
  const startLevel = clamp(n(va.startLevel, 1), 0, 1);
  const endLevel = clamp(n(va.endLevel, 1), 0, 1);
  if (Math.abs(startLevel - endLevel) < EPS) return (base * startLevel).toFixed(6);
  const span = Math.max(0.001, e - s);
  return `${base.toFixed(6)}*if(lt(t,${s.toFixed(6)}),${startLevel.toFixed(6)},if(gt(t,${e.toFixed(6)}),${endLevel.toFixed(6)},${startLevel.toFixed(6)}+(${endLevel.toFixed(6)}-${startLevel.toFixed(6)})*(t-${s.toFixed(6)})/${span.toFixed(6)}))`;
}

function audioFilters(clip, duration, hasAudio) {
  const audio = clip?.audio || {};
  if (!hasAudio) return { source: `anullsrc=r=48000:cl=stereo:d=${duration.toFixed(6)}`, lavfi: true, filters: [] };

  const offset = clamp(n(audio.offsetSeconds, 0), -10, 10);
  let sourceIn = Math.max(0, n(audio.sourceInSeconds, clip?.source?.inSeconds));
  let sourceOut = Math.max(sourceIn + 0.001, n(audio.sourceOutSeconds, clip?.source?.outSeconds));
  let delay = 0;
  if (offset > 0) {
    delay = Math.min(duration, offset);
  } else if (offset < 0) {
    sourceIn = Math.min(sourceOut - 0.001, sourceIn + (-offset));
  }

  const usableDuration = Math.max(0.001, Math.min(sourceOut - sourceIn, duration - delay));
  sourceOut = sourceIn + usableDuration;

  const filters = [
    `atrim=start=${sourceIn.toFixed(6)}:end=${sourceOut.toFixed(6)}`,
    'asetpts=PTS-STARTPTS',
    'aformat=sample_rates=48000:channel_layouts=stereo'
  ];
  if (delay > EPS) filters.push(`adelay=${Math.round(delay * 1000)}|${Math.round(delay * 1000)}`);
  filters.push(`apad=pad_dur=${Math.max(0, duration).toFixed(6)}`);
  filters.push(`atrim=duration=${duration.toFixed(6)}`);

  const fadeIn = clamp(n(audio.fadeInSeconds), 0, duration);
  const fadeOut = clamp(n(audio.fadeOutSeconds), 0, duration);
  if (fadeIn > EPS) filters.push(`afade=t=in:st=0:d=${fadeIn.toFixed(6)}`);
  if (fadeOut > EPS) filters.push(`afade=t=out:st=${Math.max(0, duration - fadeOut).toFixed(6)}:d=${fadeOut.toFixed(6)}`);

  const volumeExpr = volumeExpression(audio, duration);
  const initialVolume = clamp(n(audio?.volume, 1), 0, 1) * clamp(n(audio?.volumeAutomation?.startLevel, 1), 0, 1);
  filters.push(`volume='if(isnan(t),${initialVolume.toFixed(6)},${escapeExprString(volumeExpr)})':eval=frame`);

  const pan = clamp(n(audio.pan, 0), -1, 1);
  const leftGain = pan > 0 ? 1 - pan : 1;
  const rightGain = pan < 0 ? 1 + pan : 1;
  if (Math.abs(pan) > EPS) filters.push(`pan=stereo|c0=${leftGain.toFixed(6)}*c0|c1=${rightGain.toFixed(6)}*c1`);

  return { sourceIn, sourceOut, lavfi: false, filters };
}

function lowMemoryFfmpegBaseArgs() {
  return [
    '-hide_banner',
    '-y',
    '-threads', process.env.FFMPEG_THREADS || '1',
    '-filter_threads', process.env.FFMPEG_FILTER_THREADS || '1',
    '-filter_complex_threads', process.env.FFMPEG_FILTER_THREADS || '1'
  ];
}

function lowMemoryVideoEncoderArgs() {
  return [
    '-c:v', 'libx264',
    '-preset', process.env.FFMPEG_PRESET || 'ultrafast',
    '-crf', process.env.FFMPEG_CRF || '23',
    '-threads:v', process.env.FFMPEG_THREADS || '1',
    '-pix_fmt', 'yuv420p'
  ];
}

function lowMemoryAudioEncoderArgs() {
  return [
    '-c:a', 'aac',
    '-b:a', process.env.AUDIO_BITRATE || '128k',
    '-ar', '48000',
    '-ac', '2'
  ];
}

function videoFilterChain(clip, previousClip, duration, width, height, fps) {
  const inS = n(clip?.source?.inSeconds);
  const outS = n(clip?.source?.outSeconds);

  return [
    `trim=start=${inS.toFixed(6)}:end=${outS.toFixed(6)}`,
    'setpts=PTS-STARTPTS',
    `scale=${width}:${height}:force_original_aspect_ratio=increase`,
    `crop=${width}:${height}`,
    `fps=${fps}`,
    'setsar=1',
    ...colorFilters(clip),
    ...videoFadeFilters(clip, previousClip, duration),
    'format=yuv420p'
  ];
}

async function renderClipSegment({
  clip,
  previousClip,
  index,
  sourcePath,
  probe,
  workDir,
  width,
  height,
  fps,
  onProgress = null
}) {
  const inS = n(clip?.source?.inSeconds);
  const outS = n(clip?.source?.outSeconds);
  const duration = Math.max(0.001, outS - inS);
  const segmentPath = path.join(workDir, `segment-${String(index).padStart(3, '0')}.mp4`);

  const filters = [];
  const args = [
    ...lowMemoryFfmpegBaseArgs(),
    '-i', sourcePath
  ];

  filters.push(`[0:v]${videoFilterChain(clip, previousClip, duration, width, height, fps).join(',')}[vout]`);

  if (probe.hasAudio) {
    const af = audioFilters(clip, duration, true);
    filters.push(`[0:a]${af.filters.join(',')}[aout]`);
  } else {
    args.push(
      '-f', 'lavfi',
      '-t', duration.toFixed(6),
      '-i', 'anullsrc=r=48000:cl=stereo'
    );
    filters.push(`[1:a]atrim=duration=${duration.toFixed(6)},asetpts=PTS-STARTPTS[aout]`);
  }

  args.push(
    '-filter_complex', filters.join(';'),
    '-map', '[vout]',
    '-map', '[aout]',
    ...lowMemoryVideoEncoderArgs(),
    ...lowMemoryAudioEncoderArgs(),
    '-t', duration.toFixed(6),
    '-movflags', '+faststart',
    segmentPath
  );

  const stderrTail = [];
  let progressBuffer = '';
  let lastFraction = -1;
  await runProcess('ffmpeg', args, {
    cwd: workDir,
    captureLimit: 512_000,
    onStderr(chunk) {
      stderrTail.push(chunk);
      if (stderrTail.length > 12) stderrTail.shift();

      if (typeof onProgress === 'function') {
        progressBuffer = (progressBuffer + chunk).slice(-1600);
        const seconds = parseFfmpegTimeSeconds(progressBuffer);
        if (seconds !== null) {
          const fraction = clamp(seconds / duration, 0, 1);
          if (fraction >= lastFraction + 0.005 || fraction >= 0.999) {
            lastFraction = fraction;
            onProgress(fraction);
          }
        }
      }
    }
  });
  if (typeof onProgress === 'function') onProgress(1);

  return {
    segmentPath,
    duration,
    stderrTail: stderrTail.join('').slice(-8000)
  };
}

function concatFileLine(filePath) {
  return `file '${String(filePath).replace(/'/g, "'\\''")}'`;
}

async function concatSegments(segmentPaths, workDir) {
  const concatList = path.join(workDir, 'concat.txt');
  const concatPath = path.join(workDir, 'concatenated.mp4');

  await writeFile(
    concatList,
    segmentPaths.map(concatFileLine).join('\n') + '\n',
    'utf8'
  );

  await runProcess('ffmpeg', [
    ...lowMemoryFfmpegBaseArgs(),
    '-f', 'concat',
    '-safe', '0',
    '-i', concatList,
    '-c', 'copy',
    '-movflags', '+faststart',
    concatPath
  ], {
    cwd: workDir,
    captureLimit: 512_000
  });

  return concatPath;
}

async function applyFinalOverlays(basePath, outputPath, overlays, timelineDuration, workDir, height, onProgress = null) {
  if (!Array.isArray(overlays) || overlays.length === 0) {
    await copyFile(basePath, outputPath);
    if (typeof onProgress === 'function') onProgress(1);
    return [];
  }

  const filters = [];
  const overlayFiles = [];
  let current = '0:v';

  for (let i = 0; i < overlays.length; i++) {
    const overlay = overlays[i];
    const textPath = path.join(workDir, `overlay-${i}.txt`);
    await writeFile(textPath, String(overlay.text || ''), 'utf8');
    overlayFiles.push(textPath);

    const next = `vov${i}`;
    const x = `(w-text_w)*${(clamp(n(overlay.xPercent), 0, 100) / 100).toFixed(8)}`;
    const y = `(h-text_h)*${(clamp(n(overlay.yPercent), 0, 100) / 100).toFixed(8)}`;
    const fontSize = Math.max(8, n(overlay.fontSizePx, 16) * height / 640);
    const start = Math.max(0, n(overlay.startSeconds, 0));
    const end = Math.max(start, n(overlay.endSeconds, timelineDuration));

    filters.push(
      `[${current}]drawtext=` +
      `textfile='${ffPath(textPath)}':` +
      `fontcolor=white:` +
      `fontsize=${fontSize.toFixed(3)}:` +
      `x='${x}':y='${y}':` +
      `shadowcolor=black@0.75:shadowx=0:shadowy=2:` +
      `enable='between(t,${start.toFixed(6)},${end.toFixed(6)})'` +
      `[${next}]`
    );

    current = next;
  }

  let progressBuffer = '';
  let lastFraction = -1;
  const safeDuration = Math.max(0.001, n(timelineDuration, 0));

  await runProcess('ffmpeg', [
    ...lowMemoryFfmpegBaseArgs(),
    '-i', basePath,
    '-filter_complex', filters.join(';'),
    '-map', `[${current}]`,
    '-map', '0:a?',
    ...lowMemoryVideoEncoderArgs(),
    '-c:a', 'copy',
    '-movflags', '+faststart',
    outputPath
  ], {
    cwd: workDir,
    captureLimit: 512_000,
    onStderr(chunk) {
      if (typeof onProgress !== 'function') return;
      progressBuffer = (progressBuffer + chunk).slice(-1600);
      const seconds = parseFfmpegTimeSeconds(progressBuffer);
      if (seconds !== null) {
        const fraction = clamp(seconds / safeDuration, 0, 1);
        if (fraction >= lastFraction + 0.005 || fraction >= 0.999) {
          lastFraction = fraction;
          onProgress(fraction);
        }
      }
    }
  });

  if (typeof onProgress === 'function') onProgress(1);
  return overlayFiles;
}

export async function buildFfmpegPlan(job, workDir, sourcePath, probe) {
  const manifest = job.manifest;
  const support = inspectSupport(manifest);

  if (support.blocking.length) {
    const error = new Error('Manifest contains Worker V116B unsupported features.');
    error.code = 'UNSUPPORTED_FEATURE';
    error.details = support;
    throw error;
  }

  const output = chooseOutputSize(manifest);

  return {
    output: {
      ...output,
      fps: 30
    },
    support,
    mode: 'sequential-low-memory',
    clips: manifest.timeline.clips.length,
    overlays: (manifest.overlays || []).length
  };
}

export async function renderJob(job, options = {}) {
  const validation = validateJob(job);

  if (!validation.ok) {
    const error = new Error('Invalid OLIVIA render job.');
    error.code = 'INVALID_JOB';
    error.details = validation.errors;
    throw error;
  }

  const outputDir = options.outputDir || process.env.OUTPUT_DIR || '/data/outputs';
  const tempRoot = options.tempRoot || process.env.TEMP_DIR || '/tmp/olivia-render';

  await mkdir(outputDir, { recursive: true });
  await mkdir(tempRoot, { recursive: true });

  const jobId = safeId(job.jobId || `job-${Date.now()}`);
  const workDir = path.join(tempRoot, `${jobId}-${Math.random().toString(36).slice(2, 8)}`);
  await mkdir(workDir, { recursive: true });

  const sourcePath = path.join(workDir, 'source-media');
  const outputPath = path.join(outputDir, `${jobId}.mp4`);

  try {
    const onProgress = options.onProgress;
    emitProgress(onProgress, 1, 'preparing');

    const maxSourceBytes = n(process.env.MAX_SOURCE_BYTES, 1_000_000_000);
    emitProgress(onProgress, 2, 'downloading');
    await downloadSource(
      job.manifest.source.url,
      sourcePath,
      maxSourceBytes,
      fraction => emitProgress(onProgress, 2 + fraction * 6, 'downloading')
    );

    emitProgress(onProgress, 8, 'probing');
    const probe = await probeSource(sourcePath);
    if (!probe.hasVideo) {
      throw new Error('Downloaded source has no video stream.');
    }

    emitProgress(onProgress, 10, 'planning');
    const plan = await buildFfmpegPlan(job, workDir, sourcePath, probe);
    const { width, height, aspect, fps } = plan.output;
    const clips = job.manifest.timeline.clips;
    const segmentPaths = [];
    const ffmpegLogTail = [];

    const clipDurations = clips.map(clip =>
      Math.max(0.001, n(clip?.source?.outSeconds) - n(clip?.source?.inSeconds))
    );
    const totalClipDuration = Math.max(
      0.001,
      clipDurations.reduce((sum, duration) => sum + duration, 0)
    );
    let completedClipDuration = 0;

    emitProgress(onProgress, 12, 'rendering_clips', {
      clip: 0,
      totalClips: clips.length
    });

    // Critical low-memory behavior:
    // Render ONE clip at a time. Never fan one decoder into all clip branches.
    for (let i = 0; i < clips.length; i++) {
      const clipDuration = clipDurations[i];
      const clipStartProgress = 12 + (completedClipDuration / totalClipDuration) * 70;
      const clipSpan = (clipDuration / totalClipDuration) * 70;

      const segment = await renderClipSegment({
        clip: clips[i],
        previousClip: i > 0 ? clips[i - 1] : null,
        index: i,
        sourcePath,
        probe,
        workDir,
        width,
        height,
        fps,
        onProgress: fraction => emitProgress(
          onProgress,
          clipStartProgress + clipSpan * clamp(fraction, 0, 1),
          'rendering_clips',
          { clip: i + 1, totalClips: clips.length }
        )
      });

      completedClipDuration += clipDuration;
      emitProgress(
        onProgress,
        12 + (completedClipDuration / totalClipDuration) * 70,
        'rendering_clips',
        { clip: i + 1, totalClips: clips.length }
      );

      segmentPaths.push(segment.segmentPath);
      if (segment.stderrTail) ffmpegLogTail.push(segment.stderrTail);
    }

    emitProgress(onProgress, 83, 'concatenating');
    const concatenatedPath = await concatSegments(segmentPaths, workDir);
    emitProgress(onProgress, 86, 'finalizing');

    await applyFinalOverlays(
      concatenatedPath,
      outputPath,
      job.manifest.overlays || [],
      n(job.manifest?.timeline?.durationSeconds, 0),
      workDir,
      height,
      fraction => emitProgress(
        onProgress,
        86 + clamp(fraction, 0, 1) * 12,
        'finalizing'
      )
    );

    emitProgress(onProgress, 99, 'finalizing');

    return {
      schema: 'OLIVIA_RENDER_RESULT_V1',
      worker: 'V119-TRUE-PROGRESS',
      jobId: job.jobId,
      status: 'completed',
      outputPath,
      output: { width, height, aspect, fps },
      warnings: [
        ...plan.support.warnings,
        'V119 low-memory mode renders maximum 720p-class output, uses sequential clip passes, and reports true FFmpeg progress.'
      ],
      ffmpegLogTail: ffmpegLogTail.join('\n').slice(-12000)
    };
  } finally {
    if (String(process.env.KEEP_TEMP || '').toLowerCase() !== 'true') {
      await rm(workDir, { recursive: true, force: true }).catch(() => {});
    }
  }
}

