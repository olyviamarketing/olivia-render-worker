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
  return String(value || 'job')
    .replace(/[^a-zA-Z0-9._-]/g, '_')
    .slice(0, 96);
}

function ffPath(value) {
  return String(value)
    .replace(/\\/g, '/')
    .replace(/'/g, "\\'")
    .replace(/:/g, '\\:');
}

function escapeExprString(value) {
  return String(value).replace(/'/g, "\\'");
}

function reportProgress(options, progress, phase, detail = null) {
  if (typeof options?.onProgress !== 'function') return;

  try {
    options.onProgress(progress, phase, detail);
  } catch {
    // Progress reporting must never break the render itself.
  }
}

export function validateJob(job) {
  const errors = [];

  if (!job || typeof job !== 'object') {
    errors.push('Request body must be an object.');
  }

  if (job?.schema !== 'OLIVIA_RENDER_JOB_V1') {
    errors.push('schema must be OLIVIA_RENDER_JOB_V1.');
  }

  const manifest = job?.manifest;

  if (
    !manifest ||
    manifest.schema !== 'OLIVIA_RENDER_MANIFEST_V1'
  ) {
    errors.push(
      'manifest.schema must be OLIVIA_RENDER_MANIFEST_V1.'
    );
  }

  if (!manifest?.source?.url) {
    errors.push('manifest.source.url is required.');
  }

  if (
    !Array.isArray(manifest?.timeline?.clips) ||
    manifest.timeline.clips.length === 0
  ) {
    errors.push(
      'At least one active clip is required.'
    );
  }

  return {
    ok: errors.length === 0,
    errors
  };
}

export function chooseOutputSize(manifest) {
  const aspectRaw = String(
    manifest?.output?.requestedAspectRatio || ''
  ).replace(/\s/g, '');

  const longEdge = Math.max(
    640,
    n(process.env.LOW_MEMORY_LONG_EDGE, 1280)
  );

  if (aspectRaw.includes('9:16')) {
    return {
      width: 720,
      height: 1280,
      aspect: '9:16'
    };
  }

  if (aspectRaw.includes('1:1')) {
    return {
      width: 720,
      height: 720,
      aspect: '1:1'
    };
  }

  if (aspectRaw.includes('16:9')) {
    return {
      width: 1280,
      height: 720,
      aspect: '16:9'
    };
  }

  const sourceW = Math.max(
    2,
    n(manifest?.source?.width, 1280)
  );

  const sourceH = Math.max(
    2,
    n(manifest?.source?.height, 720)
  );

  const sourceLong = Math.max(
    sourceW,
    sourceH
  );

  const scale = Math.min(
    1,
    longEdge / sourceLong
  );

  return {
    width: even(sourceW * scale),
    height: even(sourceH * scale),
    aspect: aspectRaw || 'source'
  };
}

export function inspectSupport(manifest) {
  const blocking = [];
  const warnings = [];

  const clips =
    manifest?.timeline?.clips || [];

  for (const clip of clips) {
    const transition =
      clip?.video?.transitionOut?.type ||
      'none';

    if (
      ![
        'none',
        'dip-black',
        'dip-white',
        'zoom',
        'blur'
      ].includes(transition)
    ) {
      blocking.push(
        `Clip ${clip.clipNumber ?? clip.id}: transition '${transition}' is not supported by the OLIVIA worker.`
      );
    }

    const color =
      clip?.video?.color || {};

    if (
      Math.abs(n(color.warmth)) > EPS ||
      Math.abs(n(color.tint)) > EPS
    ) {
      blocking.push(
        `Clip ${clip.clipNumber ?? clip.id}: warmth/tint require Worker V2 CSS-colour matching.`
      );
    }
  }

  const overlays =
    manifest?.overlays || [];

  for (const overlay of overlays) {
    if (
      String(
        overlay.text || ''
      ).includes('\n') &&
      !Array.isArray(
        overlay.renderLines
      )
    ) {
      warnings.push(
        `Overlay ${overlay.id || '?'} is multiline but has no browser wrap metadata; fallback rendering may differ.`
      );
    }
  }

  return {
    blocking,
    warnings
  };
}

async function runProcess(
  command,
  args,
  {
    cwd,
    onStderr,
    captureLimit = 2_000_000
  } = {}
) {
  return new Promise(
    (resolve, reject) => {
      const child = spawn(
        command,
        args,
        {
          cwd,
          stdio: [
            'ignore',
            'pipe',
            'pipe'
          ],
          env: {
            ...process.env,
            OMP_NUM_THREADS:
              process.env
                .OMP_NUM_THREADS ||
              '1'
          }
        }
      );

      let stdout = '';
      let stderr = '';

      function appendLimited(
        current,
        addition
      ) {
        const next =
          current + addition;

        return next.length >
          captureLimit
          ? next.slice(
              -captureLimit
            )
          : next;
      }

      child.stdout.on(
        'data',
        data => {
          stdout =
            appendLimited(
              stdout,
              data.toString()
            );
        }
      );

      child.stderr.on(
        'data',
        data => {
          const text =
            data.toString();

          stderr =
            appendLimited(
              stderr,
              text
            );

          if (onStderr) {
            onStderr(text);
          }
        }
      );

      child.on(
        'error',
        reject
      );

      child.on(
        'close',
        code => {
          if (code === 0) {
            resolve({
              stdout,
              stderr
            });

            return;
          }

          reject(
            Object.assign(
              new Error(
                `${command} exited with code ${code}`
              ),
              {
                stdout,
                stderr,
                code
              }
            )
          );
        }
      );
    }
  );
}

export async function probeSource(
  sourcePath
) {
  const { stdout } =
    await runProcess(
      'ffprobe',
      [
        '-v',
        'error',

        '-show_entries',
        'stream=index,codec_type,width,height:format=duration',

        '-of',
        'json',

        sourcePath
      ]
    );

  const data =
    JSON.parse(stdout);

  const streams =
    data?.streams || [];

  const videoStream =
    streams.find(
      stream =>
        stream.codec_type ===
        'video'
    );

  return {
    duration:
      n(
        data?.format?.duration,
        0
      ),

    hasVideo:
      streams.some(
        stream =>
          stream.codec_type ===
          'video'
      ),

    hasAudio:
      streams.some(
        stream =>
          stream.codec_type ===
          'audio'
      ),

    width:
      n(
        videoStream?.width,
        0
      ),

    height:
      n(
        videoStream?.height,
        0
      )
  };
}

export async function downloadSource(
  url,
  destination,
  maxBytes = 2_000_000_000
) {
  const controller =
    new AbortController();

  const timeout =
    setTimeout(
      () =>
        controller.abort(),
      120_000
    );

  try {
    const response =
      await fetch(
        url,
        {
          redirect: 'follow',
          signal:
            controller.signal
        }
      );

    if (
      !response.ok ||
      !response.body
    ) {
      throw new Error(
        `Source download failed: HTTP ${response.status}`
      );
    }

    const length =
      n(
        response.headers.get(
          'content-length'
        ),
        0
      );

    if (
      length > maxBytes
    ) {
      throw new Error(
        `Source is larger than MAX_SOURCE_BYTES (${maxBytes}).`
      );
    }

    let received = 0;

    const reader =
      response.body
        .getReader();

    const stream =
      new Readable({
        async read() {
          try {
            const {
              done,
              value
            } =
              await reader.read();

            if (done) {
              this.push(null);
              return;
            }

            received +=
              value.byteLength;

            if (
              received >
              maxBytes
            ) {
              controller.abort();

              this.destroy(
                new Error(
                  `Source exceeded MAX_SOURCE_BYTES (${maxBytes}).`
                )
              );

              return;
            }

            this.push(
              Buffer.from(
                value
              )
            );
          } catch (error) {
            this.destroy(
              error
            );
          }
        }
      });

    await pipeline(
      stream,
      createWriteStream(
        destination
      )
    );

    return {
      bytes: received,

      contentType:
        response.headers.get(
          'content-type'
        ) || ''
    };
  } finally {
    clearTimeout(
      timeout
    );
  }
}

function colorFilters(
  clip
) {
  const color =
    clip?.video?.color ||
    {};

  const brightness =
    clamp(
      n(
        color.brightness,
        1
      ),
      0.5,
      1.5
    );

  const contrast =
    clamp(
      n(
        color.contrast,
        1
      ),
      0.5,
      1.5
    );

  const saturation =
    clamp(
      n(
        color.saturation,
        1
      ),
      0,
      2
    );

  const hue =
    clamp(
      n(
        color.hueDegrees,
        0
      ),
      -180,
      180
    );

  const filters = [];

  filters.push(
    `eq=brightness=${(
      brightness - 1
    ).toFixed(6)}:` +
    `contrast=${contrast.toFixed(6)}:` +
    `saturation=${saturation.toFixed(6)}`
  );

  if (
    Math.abs(hue) >
    EPS
  ) {
    filters.push(
      `hue=h=${hue.toFixed(4)}`
    );
  }

  return filters;
}

function videoFadeFilters(
  clip,
  previousClip,
  duration
) {
  const filters = [];

  const video =
    clip?.video || {};

  const fadeIn =
    clamp(
      n(
        video.fadeInSeconds
      ),
      0,
      duration
    );

  const fadeOut =
    clamp(
      n(
        video.fadeOutSeconds
      ),
      0,
      duration
    );

  if (
    fadeIn > EPS
  ) {
    filters.push(
      `fade=t=in:st=0:d=${fadeIn.toFixed(6)}:c=black`
    );
  }

  if (
    fadeOut > EPS
  ) {
    filters.push(
      `fade=t=out:` +
      `st=${Math.max(
        0,
        duration - fadeOut
      ).toFixed(6)}:` +
      `d=${fadeOut.toFixed(6)}:` +
      `c=black`
    );
  }

  const previousTransition =
    previousClip
      ?.video
      ?.transitionOut ||
    {
      type: 'none',
      durationSeconds: 0
    };

  if (
    previousTransition.type ===
      'dip-black' ||
    previousTransition.type ===
      'dip-white'
  ) {
    const half =
      Math.max(
        0.1,
        n(
          previousTransition
            .durationSeconds,
          0.8
        ) / 2
      );

    const d =
      Math.min(
        duration,
        half
      );

    filters.push(
      `fade=t=in:st=0:` +
      `d=${d.toFixed(6)}:` +
      `c=${
        previousTransition.type ===
        'dip-white'
          ? 'white'
          : 'black'
      }`
    );
  }

  const ownTransition =
    video.transitionOut ||
    {
      type: 'none',
      durationSeconds: 0
    };

  if (
    ownTransition.type ===
      'dip-black' ||
    ownTransition.type ===
      'dip-white'
  ) {
    const half =
      Math.max(
        0.1,
        n(
          ownTransition
            .durationSeconds,
          0.8
        ) / 2
      );

    const d =
      Math.min(
        duration,
        half
      );

    filters.push(
      `fade=t=out:` +
      `st=${Math.max(
        0,
        duration - d
      ).toFixed(6)}:` +
      `d=${d.toFixed(6)}:` +
      `c=${
        ownTransition.type ===
        'dip-white'
          ? 'white'
          : 'black'
      }`
    );
  }

  return filters;
}

function transitionHalfSeconds(
  transition
) {
  return Math.max(
    0.1,
    n(
      transition
        ?.durationSeconds,
      0.8
    ) / 2
  );
}

function blurSigmaAt(
  time,
  clip,
  previousClip,
  duration
) {
  let sigma = 0;

  const ownTransition =
    clip
      ?.video
      ?.transitionOut ||
    {
      type: 'none',
      durationSeconds: 0
    };

  const previousTransition =
    previousClip
      ?.video
      ?.transitionOut ||
    {
      type: 'none',
      durationSeconds: 0
    };

  /*
    Incoming Blur:
    Previous clip owns the transition.
    Starts at 14px and returns to 0px.
  */
  if (
    previousTransition.type ===
    'blur'
  ) {
    const half =
      transitionHalfSeconds(
        previousTransition
      );

    if (
      time >= 0 &&
      time < half
    ) {
      sigma =
        Math.max(
          sigma,
          14 *
          clamp(
            1 -
            time / half,
            0,
            1
          )
        );
    }
  }

  /*
    Outgoing Blur:
    Current clip owns the transition.
    Starts at 0px and reaches 14px.
  */
  if (
    ownTransition.type ===
    'blur'
  ) {
    const half =
      transitionHalfSeconds(
        ownTransition
      );

    const start =
      duration - half;

    if (
      time >= start &&
      time <= duration
    ) {
      sigma =
        Math.max(
          sigma,
          14 *
          clamp(
            (
              time -
              start
            ) /
            half,
            0,
            1
          )
        );
    }
  }

  return sigma;
}

function buildBlurCommands(
  clip,
  previousClip,
  duration,
  fps
) {
  const ownTransition =
    clip
      ?.video
      ?.transitionOut ||
    {
      type: 'none',
      durationSeconds: 0
    };

  const previousTransition =
    previousClip
      ?.video
      ?.transitionOut ||
    {
      type: 'none',
      durationSeconds: 0
    };

  const hasBlur =
    ownTransition.type ===
      'blur' ||
    previousTransition.type ===
      'blur';

  if (!hasBlur) {
    return null;
  }

  const safeFps =
    Math.max(
      1,
      Math.round(
        n(
          fps,
          30
        )
      )
    );

  const times =
    new Set();

  times.add(
    '0.000000'
  );

  times.add(
    duration.toFixed(6)
  );

  function addSampleRange(
    start,
    end
  ) {
    const safeStart =
      clamp(
        start,
        0,
        duration
      );

    const safeEnd =
      clamp(
        end,
        0,
        duration
      );

    if (
      safeEnd <
      safeStart
    ) {
      return;
    }

    const steps =
      Math.max(
        1,
        Math.ceil(
          (
            safeEnd -
            safeStart
          ) *
          safeFps
        )
      );

    for (
      let i = 0;
      i <= steps;
      i++
    ) {
      const ratio =
        i / steps;

      const t =
        safeStart +
        (
          safeEnd -
          safeStart
        ) *
        ratio;

      times.add(
        t.toFixed(6)
      );
    }
  }

  if (
    previousTransition.type ===
    'blur'
  ) {
    const half =
      transitionHalfSeconds(
        previousTransition
      );

    addSampleRange(
      0,
      Math.min(
        duration,
        half
      )
    );
  }

  if (
    ownTransition.type ===
    'blur'
  ) {
    const half =
      transitionHalfSeconds(
        ownTransition
      );

    addSampleRange(
      Math.max(
        0,
        duration - half
      ),
      duration
    );
  }

  const sortedTimes =
    Array.from(times)
      .map(Number)
      .filter(
        Number.isFinite
      )
      .sort(
        (a, b) =>
          a - b
      );

  const commands = [];

  let previousSigma =
    null;

  for (
    const t of
    sortedTimes
  ) {
    const sigma =
      blurSigmaAt(
        t,
        clip,
        previousClip,
        duration
      );

    const roundedSigma =
      Math.round(
        sigma * 1000
      ) / 1000;

    if (
      previousSigma !==
        null &&
      Math.abs(
        roundedSigma -
        previousSigma
      ) < 0.001
    ) {
      continue;
    }

    commands.push(
      `${t.toFixed(6)} ` +
      `olivia_blur sigma ${roundedSigma.toFixed(3)},` +
      `olivia_blur sigmaV ${roundedSigma.toFixed(3)}`
    );

    previousSigma =
      roundedSigma;
  }

  if (
    commands.length === 0
  ) {
    commands.push(
      '0.000000 olivia_blur sigma 0.000,' +
      'olivia_blur sigmaV 0.000'
    );
  }

  return commands.join(
    ';'
  );
}

function transitionVisualFilters(
  clip,
  previousClip,
  duration,
  width,
  height,
  fps
) {
  const filters = [];

  const ownTransition =
    clip
      ?.video
      ?.transitionOut ||
    {
      type: 'none',
      durationSeconds: 0
    };

  const previousTransition =
    previousClip
      ?.video
      ?.transitionOut ||
    {
      type: 'none',
      durationSeconds: 0
    };

  const hasZoom =
    ownTransition.type ===
      'zoom' ||
    previousTransition.type ===
      'zoom';

  const hasBlur =
    ownTransition.type ===
      'blur' ||
    previousTransition.type ===
      'blur';

  if (
    !hasZoom &&
    !hasBlur
  ) {
    return filters;
  }

  /*
    BLUR
    ----
    The editor preview uses:
    0 -> 14px on the outgoing half
    14 -> 0px on the incoming half.

    sendcmd updates gblur while FFmpeg renders.
  */
  if (hasBlur) {
    const commands =
      buildBlurCommands(
        clip,
        previousClip,
        duration,
        fps
      );

    filters.push(
      `sendcmd=c='${commands}'`
    );

    filters.push(
      'gblur@olivia_blur=' +
      'sigma=0:' +
      'sigmaV=0:' +
      'steps=1'
    );
  }

  /*
    ZOOM
    ----
    Matches editor preview:
    outgoing 1.00 -> 1.14
    incoming 1.14 -> 1.00
  */
  let incomingZoomExpr =
    '1';

  let outgoingZoomExpr =
    '1';

  if (
    previousTransition.type ===
    'zoom'
  ) {
    const half =
      transitionHalfSeconds(
        previousTransition
      );

    incomingZoomExpr =
      `if(lt(t,${half.toFixed(6)}),` +
      `1+0.14*(1-t/${half.toFixed(6)}),1)`;
  }

  if (
    ownTransition.type ===
    'zoom'
  ) {
    const half =
      transitionHalfSeconds(
        ownTransition
      );

    const start =
      duration - half;

    outgoingZoomExpr =
      `if(gte(t,${start.toFixed(6)}),` +
      `1+0.14*((t-${start.toFixed(6)})/` +
      `${half.toFixed(6)}),1)`;
  }

  const zoomExpr =
    `max(` +
    `${incomingZoomExpr},` +
    `${outgoingZoomExpr}` +
    `)`;

  /*
    The browser preview scales a blurred frame
    to 1.03 so the softened outer edge is hidden.
  */
  const blurConditions =
    [];

  if (
    previousTransition.type ===
    'blur'
  ) {
    const half =
      transitionHalfSeconds(
        previousTransition
      );

    blurConditions.push(
      `lt(t,${half.toFixed(6)})`
    );
  }

  if (
    ownTransition.type ===
    'blur'
  ) {
    const half =
      transitionHalfSeconds(
        ownTransition
      );

    const start =
      duration - half;

    blurConditions.push(
      `gte(t,${start.toFixed(6)})`
    );
  }

  const blurScaleExpr =
    blurConditions.length >
    0
      ? `if(gt(${blurConditions.join('+')},0),1.03,1)`
      : '1';

  /*
    If Zoom and Blur meet on the same clip,
    use whichever scale is larger instead of
    multiplying them.
  */
  const finalScaleExpr =
    `max(` +
    `${zoomExpr},` +
    `${blurScaleExpr}` +
    `)`;

  filters.push(
    `scale=` +
    `w='trunc(iw*(${finalScaleExpr})/2)*2':` +
    `h='trunc(ih*(${finalScaleExpr})/2)*2':` +
    `eval=frame`
  );

  filters.push(
    `crop=${width}:${height}:` +
    `(iw-${width})/2:` +
    `(ih-${height})/2`
  );

  return filters;
}

function volumeExpression(
  audio,
  duration
) {
  if (
    audio?.muted === true ||
    audio
      ?.audibleUnderSoloRule ===
      false
  ) {
    return '0';
  }

  const base =
    clamp(
      n(
        audio?.volume,
        1
      ),
      0,
      1
    );

  const automation =
    audio
      ?.volumeAutomation ||
    {};

  const startPosition =
    clamp(
      n(
        automation
          .startPosition,
        0
      ),
      0,
      0.98
    ) *
    duration;

  const endPosition =
    clamp(
      n(
        automation
          .endPosition,
        1
      ),
      0.02,
      1
    ) *
    duration;

  const startLevel =
    clamp(
      n(
        automation
          .startLevel,
        1
      ),
      0,
      1
    );

  const endLevel =
    clamp(
      n(
        automation
          .endLevel,
        1
      ),
      0,
      1
    );

  if (
    Math.abs(
      startLevel -
      endLevel
    ) < EPS
  ) {
    return (
      base *
      startLevel
    ).toFixed(6);
  }

  const span =
    Math.max(
      0.001,
      endPosition -
      startPosition
    );

  return (
    `${base.toFixed(6)}*` +
    `if(lt(t,${startPosition.toFixed(6)}),` +
    `${startLevel.toFixed(6)},` +
    `if(gt(t,${endPosition.toFixed(6)}),` +
    `${endLevel.toFixed(6)},` +
    `${startLevel.toFixed(6)}+` +
    `(${endLevel.toFixed(6)}-${startLevel.toFixed(6)})*` +
    `(t-${startPosition.toFixed(6)})/` +
    `${span.toFixed(6)}))`
  );
}

function audioFilters(
  clip,
  duration,
  hasAudio
) {
  const audio =
    clip?.audio || {};

  if (!hasAudio) {
    return {
      source:
        `anullsrc=` +
        `r=48000:` +
        `cl=stereo:` +
        `d=${duration.toFixed(6)}`,

      lavfi: true,

      filters: []
    };
  }

  const offset =
    clamp(
      n(
        audio.offsetSeconds,
        0
      ),
      -10,
      10
    );

  let sourceIn =
    Math.max(
      0,
      n(
        audio
          .sourceInSeconds,
        clip
          ?.source
          ?.inSeconds
      )
    );

  let sourceOut =
    Math.max(
      sourceIn + 0.001,
      n(
        audio
          .sourceOutSeconds,
        clip
          ?.source
          ?.outSeconds
      )
    );

  let delay = 0;

  if (
    offset > 0
  ) {
    delay =
      Math.min(
        duration,
        offset
      );
  } else if (
    offset < 0
  ) {
    sourceIn =
      Math.min(
        sourceOut -
        0.001,
        sourceIn +
        (-offset)
      );
  }

  const usableDuration =
    Math.max(
      0.001,
      Math.min(
        sourceOut -
        sourceIn,
        duration -
        delay
      )
    );

  sourceOut =
    sourceIn +
    usableDuration;

  const filters = [
    `atrim=` +
    `start=${sourceIn.toFixed(6)}:` +
    `end=${sourceOut.toFixed(6)}`,

    'asetpts=PTS-STARTPTS',

    'aformat=' +
    'sample_rates=48000:' +
    'channel_layouts=stereo'
  ];

  if (
    delay > EPS
  ) {
    const delayMs =
      Math.round(
        delay * 1000
      );

    filters.push(
      `adelay=` +
      `${delayMs}|${delayMs}`
    );
  }

  filters.push(
    `apad=` +
    `pad_dur=${Math.max(
      0,
      duration
    ).toFixed(6)}`
  );

  filters.push(
    `atrim=` +
    `duration=${duration.toFixed(6)}`
  );

  const fadeIn =
    clamp(
      n(
        audio.fadeInSeconds
      ),
      0,
      duration
    );

  const fadeOut =
    clamp(
      n(
        audio.fadeOutSeconds
      ),
      0,
      duration
    );

  if (
    fadeIn > EPS
  ) {
    filters.push(
      `afade=` +
      `t=in:` +
      `st=0:` +
      `d=${fadeIn.toFixed(6)}`
    );
  }

  if (
    fadeOut > EPS
  ) {
    filters.push(
      `afade=` +
      `t=out:` +
      `st=${Math.max(
        0,
        duration -
        fadeOut
      ).toFixed(6)}:` +
      `d=${fadeOut.toFixed(6)}`
    );
  }

  const volumeExpr =
    volumeExpression(
      audio,
      duration
    );

  const initialVolume =
    clamp(
      n(
        audio?.volume,
        1
      ),
      0,
      1
    ) *
    clamp(
      n(
        audio
          ?.volumeAutomation
          ?.startLevel,
        1
      ),
      0,
      1
    );

  filters.push(
    `volume=` +
    `'if(isnan(t),` +
    `${initialVolume.toFixed(6)},` +
    `${escapeExprString(
      volumeExpr
    )})':` +
    `eval=frame`
  );

  const pan =
    clamp(
      n(
        audio.pan,
        0
      ),
      -1,
      1
    );

  const leftGain =
    pan > 0
      ? 1 - pan
      : 1;

  const rightGain =
    pan < 0
      ? 1 + pan
      : 1;

  if (
    Math.abs(pan) >
    EPS
  ) {
    filters.push(
      `pan=stereo|` +
      `c0=${leftGain.toFixed(6)}*c0|` +
      `c1=${rightGain.toFixed(6)}*c1`
    );
  }

  return {
    sourceIn,
    sourceOut,
    lavfi: false,
    filters
  };
}

function lowMemoryFfmpegBaseArgs() {
  return [
    '-hide_banner',
    '-y',

    '-threads',
    process.env
      .FFMPEG_THREADS ||
    '1',

    '-filter_threads',
    process.env
      .FFMPEG_FILTER_THREADS ||
    '1',

    '-filter_complex_threads',
    process.env
      .FFMPEG_FILTER_THREADS ||
    '1'
  ];
}

function lowMemoryVideoEncoderArgs() {
  return [
    '-c:v',
    'libx264',

    '-preset',
    process.env
      .FFMPEG_PRESET ||
    'ultrafast',

    '-crf',
    process.env
      .FFMPEG_CRF ||
    '23',

    '-threads:v',
    process.env
      .FFMPEG_THREADS ||
    '1',

    '-pix_fmt',
    'yuv420p'
  ];
}

function lowMemoryAudioEncoderArgs() {
  return [
    '-c:a',
    'aac',

    '-b:a',
    process.env
      .AUDIO_BITRATE ||
    '128k',

    '-ar',
    '48000',

    '-ac',
    '2'
  ];
}

function videoFilterChain(
  clip,
  previousClip,
  duration,
  width,
  height,
  fps
) {
  const inSeconds =
    n(
      clip
        ?.source
        ?.inSeconds
    );

  const outSeconds =
    n(
      clip
        ?.source
        ?.outSeconds
    );

  return [
    `trim=` +
    `start=${inSeconds.toFixed(6)}:` +
    `end=${outSeconds.toFixed(6)}`,

    'setpts=PTS-STARTPTS',

    `scale=` +
    `${width}:` +
    `${height}:` +
    'force_original_aspect_ratio=increase',

    `crop=${width}:${height}`,

    `fps=${fps}`,

    'setsar=1',

    ...colorFilters(
      clip
    ),

    ...transitionVisualFilters(
      clip,
      previousClip,
      duration,
      width,
      height,
      fps
    ),

    ...videoFadeFilters(
      clip,
      previousClip,
      duration
    ),

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
  fps
}) {
  const inSeconds =
    n(
      clip
        ?.source
        ?.inSeconds
    );

  const outSeconds =
    n(
      clip
        ?.source
        ?.outSeconds
    );

  const duration =
    Math.max(
      0.001,
      outSeconds -
      inSeconds
    );

  const segmentPath =
    path.join(
      workDir,
      `segment-${String(
        index
      ).padStart(
        3,
        '0'
      )}.mp4`
    );

  const filters = [];

  const args = [
    ...lowMemoryFfmpegBaseArgs(),

    '-i',
    sourcePath
  ];

  filters.push(
    `[0:v]` +
    videoFilterChain(
      clip,
      previousClip,
      duration,
      width,
      height,
      fps
    ).join(',') +
    `[vout]`
  );

  if (
    probe.hasAudio
  ) {
    const audioPlan =
      audioFilters(
        clip,
        duration,
        true
      );

    filters.push(
      `[0:a]` +
      `${audioPlan.filters.join(',')}` +
      `[aout]`
    );
  } else {
    args.push(
      '-f',
      'lavfi',

      '-t',
      duration.toFixed(6),

      '-i',
      'anullsrc=r=48000:cl=stereo'
    );

    filters.push(
      `[1:a]` +
      `atrim=duration=${duration.toFixed(6)},` +
      `asetpts=PTS-STARTPTS` +
      `[aout]`
    );
  }

  args.push(
    '-filter_complex',
    filters.join(';'),

    '-map',
    '[vout]',

    '-map',
    '[aout]',

    ...lowMemoryVideoEncoderArgs(),

    ...lowMemoryAudioEncoderArgs(),

    '-t',
    duration.toFixed(6),

    '-movflags',
    '+faststart',

    segmentPath
  );

  const stderrTail =
    [];

  await runProcess(
    'ffmpeg',
    args,
    {
      cwd: workDir,

      captureLimit:
        512_000,

      onStderr(chunk) {
        stderrTail.push(
          chunk
        );

        if (
          stderrTail.length >
          12
        ) {
          stderrTail.shift();
        }
      }
    }
  );

  return {
    segmentPath,
    duration,

    stderrTail:
      stderrTail
        .join('')
        .slice(-8000)
  };
}

function concatFileLine(
  filePath
) {
  return (
    `file '` +
    String(filePath)
      .replace(
        /'/g,
        "'\\''"
      ) +
    `'`
  );
}

async function concatSegments(
  segmentPaths,
  workDir
) {
  const concatList =
    path.join(
      workDir,
      'concat.txt'
    );

  const concatPath =
    path.join(
      workDir,
      'concatenated.mp4'
    );

  await writeFile(
    concatList,

    segmentPaths
      .map(
        concatFileLine
      )
      .join('\n') +
      '\n',

    'utf8'
  );

  await runProcess(
    'ffmpeg',
    [
      ...lowMemoryFfmpegBaseArgs(),

      '-f',
      'concat',

      '-safe',
      '0',

      '-i',
      concatList,

      '-c',
      'copy',

      '-movflags',
      '+faststart',

      concatPath
    ],
    {
      cwd: workDir,

      captureLimit:
        512_000
    }
  );

  return concatPath;
}

function overlayText(
  overlay
) {
  if (
    Array.isArray(
      overlay?.renderLines
    ) &&
    overlay
      .renderLines
      .length > 0
  ) {
    return overlay
      .renderLines
      .map(
        line =>
          String(
            line ?? ''
          )
      )
      .join('\n');
  }

  return String(
    overlay?.text ||
    ''
  );
}

async function applyFinalOverlays(
  basePath,
  outputPath,
  overlays,
  timelineDuration,
  workDir,
  width,
  height
) {
  if (
    !Array.isArray(
      overlays
    ) ||
    overlays.length ===
    0
  ) {
    await copyFile(
      basePath,
      outputPath
    );

    return [];
  }

  const filters = [];
  const overlayFiles = [];

  let current =
    '0:v';

  for (
    let i = 0;
    i < overlays.length;
    i++
  ) {
    const overlay =
      overlays[i];

    const textPath =
      path.join(
        workDir,
        `overlay-${i}.txt`
      );

    await writeFile(
      textPath,
      overlayText(
        overlay
      ),
      'utf8'
    );

    overlayFiles.push(
      textPath
    );

    const next =
      `vov${i}`;

    const xPercent =
      clamp(
        n(
          overlay
            .xPercent
        ),
        0,
        100
      ) /
      100;

    const yPercent =
      clamp(
        n(
          overlay
            .yPercent
        ),
        0,
        100
      ) /
      100;

    const widthPercent =
      clamp(
        n(
          overlay
            .widthPercent,
          100
        ),
        0,
        100
      ) /
      100;

    /*
      V123/V126 preview bridge:
      fontSizePx was measured against
      editorFrameHeightPx.
    */
    const editorFrameHeight =
      Math.max(
        1,
        n(
          overlay
            .editorFrameHeightPx,
          640
        )
      );

    const fontSize =
      Math.max(
        8,
        n(
          overlay
            .fontSizePx,
          16
        ) *
        height /
        editorFrameHeight
      );

    const start =
      Math.max(
        0,
        n(
          overlay
            .startSeconds,
          0
        )
      );

    const end =
      Math.max(
        start,
        n(
          overlay
            .endSeconds,
          timelineDuration
        )
      );

    /*
      Horizontal text alignment in editor is centered
      inside the overlay box.
    */
    const x =
      `w*${xPercent.toFixed(8)}+` +
      `(w*${widthPercent.toFixed(8)}-text_w)/2`;

    const y =
      `h*${yPercent.toFixed(8)}`;

    filters.push(
      `[${current}]` +

      `drawtext=` +

      `textfile='${ffPath(
        textPath
      )}':` +

      `fontcolor=white:` +

      `fontsize=${fontSize.toFixed(3)}:` +

      `x='${x}':` +

      `y='${y}':` +

      `shadowcolor=black@0.75:` +

      `shadowx=0:` +

      `shadowy=2:` +

      `enable='between(t,` +
      `${start.toFixed(6)},` +
      `${end.toFixed(6)})'` +

      `[${next}]`
    );

    current =
      next;
  }

  await runProcess(
    'ffmpeg',
    [
      ...lowMemoryFfmpegBaseArgs(),

      '-i',
      basePath,

      '-filter_complex',
      filters.join(';'),

      '-map',
      `[${current}]`,

      '-map',
      '0:a?',

      ...lowMemoryVideoEncoderArgs(),

      '-c:a',
      'copy',

      '-movflags',
      '+faststart',

      outputPath
    ],
    {
      cwd: workDir,

      captureLimit:
        512_000
    }
  );

  return overlayFiles;
}

export async function buildFfmpegPlan(
  job,
  workDir,
  sourcePath,
  probe
) {
  const manifest =
    job.manifest;

  const support =
    inspectSupport(
      manifest
    );

  if (
    support
      .blocking
      .length
  ) {
    const error =
      new Error(
        'Manifest contains unsupported render features.'
      );

    error.code =
      'UNSUPPORTED_FEATURE';

    error.details =
      support;

    throw error;
  }

  const output =
    chooseOutputSize(
      manifest
    );

  return {
    output: {
      ...output,
      fps: 30
    },

    support,

    mode:
      'sequential-low-memory',

    clips:
      manifest
        .timeline
        .clips
        .length,

    overlays:
      (
        manifest
          .overlays ||
        []
      ).length,

    workDir,
    sourcePath,
    sourceProbe:
      probe
  };
}

export async function renderJob(
  job,
  options = {}
) {
  const validation =
    validateJob(
      job
    );

  if (
    !validation.ok
  ) {
    const error =
      new Error(
        'Invalid OLIVIA render job.'
      );

    error.code =
      'INVALID_JOB';

    error.details =
      validation.errors;

    throw error;
  }

  const outputDir =
    options.outputDir ||
    process.env
      .OUTPUT_DIR ||
    '/data/outputs';

  const tempRoot =
    options.tempRoot ||
    process.env
      .TEMP_DIR ||
    '/tmp/olivia-render';

  await mkdir(
    outputDir,
    {
      recursive: true
    }
  );

  await mkdir(
    tempRoot,
    {
      recursive: true
    }
  );

  const jobId =
    safeId(
      job.jobId ||
      `job-${Date.now()}`
    );

  const workDir =
    path.join(
      tempRoot,

      `${jobId}-${Math.random()
        .toString(36)
        .slice(
          2,
          8
        )}`
    );

  await mkdir(
    workDir,
    {
      recursive: true
    }
  );

  const sourcePath =
    path.join(
      workDir,
      'source-media'
    );

  const outputPath =
    path.join(
      outputDir,
      `${jobId}.mp4`
    );

  try {
    reportProgress(
      options,
      3,
      'downloading-source'
    );

    const maxSourceBytes =
      n(
        process.env
          .MAX_SOURCE_BYTES,
        1_000_000_000
      );

    await downloadSource(
      job.manifest
        .source
        .url,

      sourcePath,

      maxSourceBytes
    );

    reportProgress(
      options,
      8,
      'probing-source'
    );

    const probe =
      await probeSource(
        sourcePath
      );

    if (
      !probe.hasVideo
    ) {
      throw new Error(
        'Downloaded source has no video stream.'
      );
    }

    const plan =
      await buildFfmpegPlan(
        job,
        workDir,
        sourcePath,
        probe
      );

    const {
      width,
      height,
      aspect,
      fps
    } =
      plan.output;

    const clips =
      job
        .manifest
        .timeline
        .clips;

    const segmentPaths =
      [];

    const ffmpegLogTail =
      [];

    /*
      Low-memory render:
      one clip at a time.
    */
    for (
      let index = 0;
      index <
      clips.length;
      index++
    ) {
      const startProgress =
        10 +
        Math.floor(
          index /
          clips.length *
          65
        );

      reportProgress(
        options,

        startProgress,

        'rendering-clips',

        {
          clip:
            index + 1,

          totalClips:
            clips.length
        }
      );

      const segment =
        await renderClipSegment({
          clip:
            clips[index],

          previousClip:
            index > 0
              ? clips[
                  index - 1
                ]
              : null,

          index,
          sourcePath,
          probe,
          workDir,
          width,
          height,
          fps
        });

      segmentPaths.push(
        segment
          .segmentPath
      );

      if (
        segment
          .stderrTail
      ) {
        ffmpegLogTail.push(
          segment
            .stderrTail
        );
      }
    }

    reportProgress(
      options,
      78,
      'joining-clips'
    );

    const concatenatedPath =
      await concatSegments(
        segmentPaths,
        workDir
      );

    reportProgress(
      options,
      88,
      'rendering-overlays'
    );

    await applyFinalOverlays(
      concatenatedPath,

      outputPath,

      job
        .manifest
        .overlays ||
      [],

      n(
        job
          .manifest
          ?.timeline
          ?.durationSeconds,
        0
      ),

      workDir,

      width,

      height
    );

    reportProgress(
      options,
      98,
      'finalizing'
    );

    return {
      schema:
        'OLIVIA_RENDER_RESULT_V1',

      worker:
        'V130-ZOOM-BLUR',

      jobId:
        job.jobId,

      status:
        'completed',

      outputPath,

      output: {
        width,
        height,
        aspect,
        fps
      },

      warnings: [
        ...plan
          .support
          .warnings,

        'Low-memory sequential render mode is active.'
      ],

      ffmpegLogTail:
        ffmpegLogTail
          .join('\n')
          .slice(
            -12000
          )
    };
  } finally {
    if (
      String(
        process.env
          .KEEP_TEMP ||
        ''
      ).toLowerCase() !==
      'true'
    ) {
      await rm(
        workDir,
        {
          recursive: true,
          force: true
        }
      ).catch(
        () => {}
      );
    }
  }
}
