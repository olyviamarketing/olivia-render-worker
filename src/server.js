import http from 'node:http';
import https from 'node:https';
import { readFile, stat } from 'node:fs/promises';
import { createReadStream } from 'node:fs';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { createHmac, timingSafeEqual } from 'node:crypto';
import {
  renderJob,
  validateJob,
  inspectSupport,
  chooseOutputSize
} from './render-engine.js';

const PORT = Number(process.env.PORT || 8080);
const OUTPUT_DIR = process.env.OUTPUT_DIR || '/data/outputs';
const MAX_BODY_BYTES = Number(
  process.env.MAX_BODY_BYTES || 8_000_000
);

const WORKER_TOKEN = String(
  process.env.WORKER_TOKEN || ''
);

const PUBLIC_BASE_URL = String(
  process.env.PUBLIC_BASE_URL || ''
).replace(/\/$/, '');

const DODO_WEBHOOK_SECRET = String(
  process.env.DODO_WEBHOOK_SECRET || ''
).trim();

const BUBBLE_DODO_WEBHOOK_URL = String(
  process.env.BUBBLE_DODO_WEBHOOK_URL || ''
).trim();

const BUBBLE_DODO_WEBHOOK_TOKEN = String(
  process.env.BUBBLE_DODO_WEBHOOK_TOKEN || ''
).trim();

const DODO_WEBHOOK_TOLERANCE_SECONDS =
  5 * 60;

const renderJobs =
  new Map();


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
  const raw =
    String(
      job &&
      job.jobId ||
      ''
    ).trim();

  return (
    raw ||
    `olivia-${Date.now().toString(36)}-${Math.random()
      .toString(36)
      .slice(2, 10)}`
  );
}


function normaliseProgress(
  value,
  fallback = 0
) {
  const n =
    Number(value);

  if (
    !Number.isFinite(n)
  ) {
    return fallback;
  }

  return Math.max(
    0,
    Math.min(
      100,
      Math.round(n)
    )
  );
}


function updateJobProgress(
  state,
  value,
  phase,
  detail = null
) {
  if (!state) {
    return;
  }

  const next =
    normaliseProgress(
      value,
      state.progress || 0
    );

  if (
    next >=
    (state.progress || 0)
  ) {
    state.progress =
      next;
  }

  if (phase) {
    state.phase =
      String(phase);
  }

  if (
    detail !== undefined
  ) {
    state.progressDetail =
      detail;
  }

  state.updatedAt =
    new Date()
      .toISOString();
}


async function runRenderInBackground(
  job
) {
  const jobId =
    safeJobId(job);

  const state =
    renderJobs.get(
      jobId
    );

  if (!state) {
    return;
  }

  state.status =
    'processing';

  state.startedAt =
    new Date()
      .toISOString();

  updateJobProgress(
    state,
    1,
    'preparing'
  );

  try {
    const started =
      Date.now();

    const result =
      await renderJob(
        job,
        {
          outputDir:
            OUTPUT_DIR,

          onProgress(
            progress,
            phase,
            detail
          ) {
            updateJobProgress(
              state,
              progress,
              phase,
              detail
            );
          }
        }
      );

    const filename =
      path.basename(
        result.outputPath
      );

    state.status =
      'completed';

    updateJobProgress(
      state,
      100,
      'completed'
    );

    state.completedAt =
      new Date()
        .toISOString();

    state.elapsedMs =
      Date.now() -
      started;

    state.outputUrl =
      publicOutputUrl(
        filename
      );

    state.downloadUrl =
      publicDownloadUrl(
        filename
      );

    state.result = {
      ...result,

      outputUrl:
        state.outputUrl,

      downloadUrl:
        state.downloadUrl,

      elapsedMs:
        state.elapsedMs
    };

    delete state.result.outputPath;

  } catch (
    error
  ) {
    console.error(
      '[OLIVIA ASYNC RENDER ERROR]',
      error
    );

    state.status =
      'failed';

    state.phase =
      'failed';

    state.updatedAt =
      new Date()
        .toISOString();

    state.completedAt =
      new Date()
        .toISOString();

    state.error =
      error.message;

    state.code =
      error.code ||
      'RENDER_FAILED';

    state.details =
      error.details ||
      null;

    state.ffmpeg =
      error.stderr
        ? String(
            error.stderr
          ).slice(
            -12000
          )
        : null;
  }
}


function json(
  res,
  status,
  body
) {
  const payload =
    Buffer.from(
      JSON.stringify(
        body,
        null,
        2
      )
    );

  res.writeHead(
    status,
    {
      'content-type':
        'application/json; charset=utf-8',

      'content-length':
        payload.length,

      'cache-control':
        'no-store'
    }
  );

  res.end(
    payload
  );
}


function authorized(
  req
) {
  if (
    !WORKER_TOKEN
  ) {
    return true;
  }

  return (
    req.headers.authorization ===
    `Bearer ${WORKER_TOKEN}`
  );
}


async function readRawBody(
  req
) {
  let total =
    0;

  const chunks =
    [];

  for await (
    const chunk of req
  ) {
    total +=
      chunk.length;

    if (
      total >
      MAX_BODY_BYTES
    ) {
      throw Object.assign(
        new Error(
          'Request body too large.'
        ),
        {
          status:
            413
        }
      );
    }

    chunks.push(
      chunk
    );
  }

  return Buffer.concat(
    chunks
  );
}


async function readJson(
  req
) {
  const raw =
    await readRawBody(
      req
    );

  return JSON.parse(
    raw.toString(
      'utf8'
    ) || '{}'
  );
}


async function ffmpegVersion() {
  return new Promise(
    resolve => {
      const child =
        spawn(
          'ffmpeg',
          [
            '-version'
          ]
        );

      let out =
        '';

      child.stdout.on(
        'data',
        d => {
          out +=
            d.toString();
        }
      );

      child.on(
        'error',
        () =>
          resolve(
            null
          )
      );

      child.on(
        'close',
        code =>
          resolve(
            code === 0
              ? out.split(
                  '\n'
                )[0]
              : null
          )
      );
    }
  );
}


async function serveOutput(
  req,
  res,
  pathname
) {
  const name =
    path.basename(
      pathname.slice(
        '/outputs/'.length
      )
    );

  const tiktokVerifyFile =
    'tiktokF53uDBD6DYx1YzQK6GvmhwU2Gz2uijSx.txt';

  // TikTok URL-prefix verification file
  if (
    name ===
    tiktokVerifyFile
  ) {
    try {
      const contents =
        await readFile(
          new URL(
            `./${tiktokVerifyFile}`,
            import.meta.url
          )
        );

      res.writeHead(
        200,
        {
          'content-type':
            'text/plain; charset=utf-8',

          'content-length':
            contents.length,

          'cache-control':
            'no-store'
        }
      );

      return res.end(
        contents
      );

    } catch {
      return json(
        res,
        404,
        {
          error:
            'TikTok verification file not found.'
        }
      );
    }
  }

  // Normal OLIVIA rendered videos
  if (
    !/^[a-zA-Z0-9._-]+\.mp4$/.test(
      name
    )
  ) {
    return json(
      res,
      400,
      {
        error:
          'Invalid output name.'
      }
    );
  }

  const filePath =
    path.join(
      OUTPUT_DIR,
      name
    );

  try {
    const info =
      await stat(
        filePath
      );

    res.writeHead(
      200,
      {
        'content-type':
          'video/mp4',

        'content-length':
          info.size,

        'accept-ranges':
          'bytes',

        'cache-control':
          'private, max-age=3600'
      }
    );

    createReadStream(
      filePath
    ).pipe(
      res
    );

  } catch {
    json(
      res,
      404,
      {
        error:
          'Output not found.'
      }
    );
  }
}


async function serveDownload(
  req,
  res,
  pathname
) {
  const name =
    path.basename(
      pathname.slice(
        '/download/'.length
      )
    );

  if (
    !/^[a-zA-Z0-9._-]+\.mp4$/.test(
      name
    )
  ) {
    return json(
      res,
      400,
      {
        error:
          'Invalid output name.'
      }
    );
  }

  const filePath =
    path.join(
      OUTPUT_DIR,
      name
    );

  try {
    const info =
      await stat(
        filePath
      );

    res.writeHead(
      200,
      {
        'content-type':
          'video/mp4',

        'content-length':
          info.size,

        'content-disposition':
          `attachment; filename="${name}"`,

        'cache-control':
          'private, no-store'
      }
    );

    createReadStream(
      filePath
    ).pipe(
      res
    );

  } catch {
    json(
      res,
      404,
      {
        error:
          'Output not found.'
      }
    );
  }
}


/* =========================================================
   YOUTUBE UPLOAD HELPERS
========================================================= */


function textValue(
  value,
  fallback = ''
) {
  const raw =
    value === undefined ||
    value === null
      ? ''
      : String(
          value
        );

  const trimmed =
    raw.trim();

  return (
    trimmed ||
    fallback
  );
}


function cleanYouTubeTitle(
  value
) {
  const cleaned =
    textValue(
      value,
      'UploadPilot video'
    ).replace(
      /[<>]/g,
      ''
    );

  return (
    Array.from(
      cleaned
    )
      .slice(
        0,
        100
      )
      .join(
        ''
      ) ||
    'UploadPilot video'
  );
}


function truncateUtf8(
  value,
  maxBytes
) {
  const source =
    String(
      value ||
      ''
    );

  if (
    Buffer.byteLength(
      source,
      'utf8'
    ) <=
    maxBytes
  ) {
    return source;
  }

  let result =
    '';

  for (
    const ch of source
  ) {
    const next =
      result +
      ch;

    if (
      Buffer.byteLength(
        next,
        'utf8'
      ) >
      maxBytes
    ) {
      break;
    }

    result =
      next;
  }

  return result;
}


function cleanYouTubeDescription(
  value
) {
  return truncateUtf8(
    String(
      value ||
      ''
    ).replace(
      /[<>]/g,
      ''
    ),
    5000
  );
}


function cleanYouTubePrivacy(
  value
) {
  const privacy =
    textValue(
      value,
      'public'
    ).toLowerCase();

  if (
    privacy ===
      'public' ||
    privacy ===
      'private' ||
    privacy ===
      'unlisted'
  ) {
    return privacy;
  }

  return 'public';
}


function cleanYouTubeCategoryId(
  value
) {
  const categoryId =
    textValue(
      value,
      '22'
    );

  return /^\d+$/.test(
    categoryId
  )
    ? categoryId
    : '22';
}


function booleanValue(
  value,
  fallback = true
) {
  if (
    value === undefined ||
    value === null ||
    value === ''
  ) {
    return fallback;
  }

  if (
    typeof value ===
    'boolean'
  ) {
    return value;
  }

  const raw =
    String(
      value
    )
      .trim()
      .toLowerCase();

  if (
    [
      'true',
      'yes',
      '1'
    ].includes(
      raw
    )
  ) {
    return true;
  }

  if (
    [
      'false',
      'no',
      '0'
    ].includes(
      raw
    )
  ) {
    return false;
  }

  return fallback;
}


async function resolveYouTubeOutput(
  body
) {
  const candidate =
    textValue(
      body.filename ||
      body.render_url ||
      body.render_url_text ||
      body.video_url
    );

  if (!candidate) {
    throw Object.assign(
      new Error(
        'YouTube upload requires filename or render_url.'
      ),
      {
        status:
          400,

        code:
          'YOUTUBE_OUTPUT_REQUIRED'
      }
    );
  }

  let name =
    '';

  try {
    const parsed =
      new URL(
        candidate,
        PUBLIC_BASE_URL ||
        'http://localhost'
      );

    name =
      path.basename(
        parsed.pathname
      );

  } catch {
    name =
      path.basename(
        candidate.split(
          /[?#]/,
          1
        )[0]
      );
  }

  if (
    !/^[a-zA-Z0-9._-]+\.mp4$/.test(
      name
    )
  ) {
    throw Object.assign(
      new Error(
        'Invalid YouTube output filename.'
      ),
      {
        status:
          400,

        code:
          'YOUTUBE_OUTPUT_INVALID'
      }
    );
  }

  const filePath =
    path.join(
      OUTPUT_DIR,
      name
    );

  let info;

  try {
    info =
      await stat(
        filePath
      );

  } catch {
    throw Object.assign(
      new Error(
        'Rendered MP4 was not found on the worker.'
      ),
      {
        status:
          404,

        code:
          'YOUTUBE_OUTPUT_NOT_FOUND',

        details: {
          filename:
            name
        }
      }
    );
  }

  if (
    !info.isFile() ||
    info.size <=
      0
  ) {
    throw Object.assign(
      new Error(
        'Rendered MP4 is not a valid file.'
      ),
      {
        status:
          422,

        code:
          'YOUTUBE_OUTPUT_INVALID_FILE',

        details: {
          filename:
            name,

          size:
            info.size
        }
      }
    );
  }

  return {
    name,
    filePath,
    size:
      info.size
  };
}


function requestBuffer(
  target,
  {
    method = 'GET',
    headers = {},
    body = null
  } = {}
) {
  return new Promise(
    (
      resolve,
      reject
    ) => {
      const targetUrl =
        target instanceof URL
          ? target
          : new URL(
              target
            );

      const client =
        targetUrl.protocol ===
        'https:'
          ? https
          : http;

      const req =
        client.request(
          targetUrl,
          {
            method,
            headers
          },
          response => {
            const chunks =
              [];

            response.on(
              'data',
              chunk => {
                chunks.push(
                  chunk
                );
              }
            );

            response.on(
              'end',
              () => {
                resolve({
                  status:
                    response.statusCode ||
                    0,

                  headers:
                    response.headers,

                  body:
                    Buffer.concat(
                      chunks
                    ).toString(
                      'utf8'
                    )
                });
              }
            );
          }
        );

      req.on(
        'error',
        reject
      );

      if (body) {
        req.write(
          body
        );
      }

      req.end();
    }
  );
}


function uploadFileToUrl(
  target,
  {
    filePath,
    accessToken,
    size,
    contentType =
      'video/mp4'
  }
) {
  return new Promise(
    (
      resolve,
      reject
    ) => {
      const targetUrl =
        new URL(
          target
        );

      const client =
        targetUrl.protocol ===
        'https:'
          ? https
          : http;

      const req =
        client.request(
          targetUrl,
          {
            method:
              'PUT',

            headers: {
              authorization:
                `Bearer ${accessToken}`,

              'content-type':
                contentType,

              'content-length':
                size
            }
          },
          response => {
            const chunks =
              [];

            response.on(
              'data',
              chunk => {
                chunks.push(
                  chunk
                );
              }
            );

            response.on(
              'end',
              () => {
                resolve({
                  status:
                    response.statusCode ||
                    0,

                  headers:
                    response.headers,

                  body:
                    Buffer.concat(
                      chunks
                    ).toString(
                      'utf8'
                    )
                });
              }
            );
          }
        );

      req.on(
        'error',
        reject
      );

      const stream =
        createReadStream(
          filePath
        );

      stream.on(
        'error',
        error => {
          req.destroy(
            error
          );
        }
      );

      stream.pipe(
        req
      );
    }
  );
}


function parseJsonBody(
  raw
) {
  if (!raw) {
    return null;
  }

  try {
    return JSON.parse(
      raw
    );

  } catch {
    return null;
  }
}


function youtubeApiError(
  message,
  response,
  code
) {
  const parsed =
    parseJsonBody(
      response &&
      response.body
    );

  const apiMessage =
    parsed &&
    parsed.error &&
    parsed.error.message
      ? parsed.error.message
      : null;

  return Object.assign(
    new Error(
      apiMessage ||
      message
    ),
    {
      status:
        502,

      code,

      details: {
        googleStatus:
          response &&
          response.status,

        googleResponse:
          parsed ||
          (
            response &&
            response.body
          ) ||
          null
      }
    }
  );
}


async function uploadRenderedVideoToYouTube(
  body
) {
  const accessToken =
    textValue(
      body.access_token
    );

  if (!accessToken) {
    throw Object.assign(
      new Error(
        'YouTube access_token is required.'
      ),
      {
        status:
          400,

        code:
          'YOUTUBE_ACCESS_TOKEN_REQUIRED'
      }
    );
  }

  const output =
    await resolveYouTubeOutput(
      body
    );

  const title =
    cleanYouTubeTitle(
      body.title ||
      body.title_text
    );

  const description =
    cleanYouTubeDescription(
      body.description ||
      body.caption ||
      body.caption_text
    );

  const privacyStatus =
    cleanYouTubePrivacy(
      body.privacy_status
    );

  const categoryId =
    cleanYouTubeCategoryId(
      body.category_id
    );

  const metadata = {
    snippet: {
      title,
      description,
      categoryId
    },

    status: {
      privacyStatus
    }
  };

  const metadataBody =
    Buffer.from(
      JSON.stringify(
        metadata
      )
    );

  const sessionUrl =
    new URL(
      'https://www.googleapis.com/upload/youtube/v3/videos'
    );

  sessionUrl.searchParams.set(
    'uploadType',
    'resumable'
  );

  sessionUrl.searchParams.set(
    'part',
    'snippet,status'
  );

  if (
    body.notify_subscribers !==
    undefined
  ) {
    sessionUrl.searchParams.set(
      'notifySubscribers',
      booleanValue(
        body.notify_subscribers,
        true
      )
        ? 'true'
        : 'false'
    );
  }

  const sessionResponse =
    await requestBuffer(
      sessionUrl,
      {
        method:
          'POST',

        headers: {
          authorization:
            `Bearer ${accessToken}`,

          'content-type':
            'application/json; charset=UTF-8',

          'content-length':
            metadataBody.length,

          'x-upload-content-length':
            output.size,

          'x-upload-content-type':
            'video/mp4'
        },

        body:
          metadataBody
      }
    );

  if (
    sessionResponse.status <
      200 ||
    sessionResponse.status >=
      300
  ) {
    throw youtubeApiError(
      'YouTube rejected the resumable upload session.',
      sessionResponse,
      'YOUTUBE_SESSION_FAILED'
    );
  }

  const uploadUrl =
    sessionResponse.headers.location;

  if (!uploadUrl) {
    throw Object.assign(
      new Error(
        'YouTube did not return a resumable upload URL.'
      ),
      {
        status:
          502,

        code:
          'YOUTUBE_SESSION_LOCATION_MISSING'
      }
    );
  }

  const uploadResponse =
    await uploadFileToUrl(
      uploadUrl,
      {
        filePath:
          output.filePath,

        accessToken,

        size:
          output.size,

        contentType:
          'video/mp4'
      }
    );

  if (
    uploadResponse.status !==
      200 &&
    uploadResponse.status !==
      201
  ) {
    throw youtubeApiError(
      'YouTube video upload failed.',
      uploadResponse,

      uploadResponse.status ===
        308
        ? 'YOUTUBE_UPLOAD_INCOMPLETE'
        : 'YOUTUBE_UPLOAD_FAILED'
    );
  }

  const video =
    parseJsonBody(
      uploadResponse.body
    );

  const videoId =
    video &&
    video.id
      ? String(
          video.id
        )
      : '';

  if (!videoId) {
    throw Object.assign(
      new Error(
        'YouTube upload completed without returning a video id.'
      ),
      {
        status:
          502,

        code:
          'YOUTUBE_VIDEO_ID_MISSING',

        details: {
          googleResponse:
            video ||
            uploadResponse.body
        }
      }
    );
  }

  return {
    schema:
      'OLIVIA_YOUTUBE_UPLOAD_RESULT_V1',

    ok:
      true,

    status:
      'posted',

    video_id:
      videoId,

    youtube_url:
      `https://www.youtube.com/watch?v=${videoId}`,

    filename:
      output.name,

    bytes:
      output.size,

    title,

    privacy_status:
      privacyStatus,

    category_id:
      categoryId,

    video
  };
}


/* =========================================================
   DODO PAYMENTS WEBHOOK HELPERS

   - Verifies Standard Webhooks signatures with raw body bytes.
   - Rejects stale/future timestamps outside a 5 minute window.
   - Normalises Dodo events before forwarding them to Bubble.
========================================================= */


function decodeDodoWebhookSecret(
  secret
) {
  let value =
    String(
      secret ||
      ''
    ).trim();

  if (
    value.startsWith(
      'whsec_'
    )
  ) {
    value =
      value.slice(
        'whsec_'.length
      );
  }

  if (!value) {
    throw new Error(
      'DODO_WEBHOOK_SECRET is not configured.'
    );
  }

  const key =
    Buffer.from(
      value,
      'base64'
    );

  if (!key.length) {
    throw new Error(
      'DODO_WEBHOOK_SECRET is invalid.'
    );
  }

  return key;
}


function verifyDodoWebhook(
  rawBody,
  headers
) {
  const webhookId =
    String(
      headers[
        'webhook-id'
      ] ||
      ''
    ).trim();

  const timestampRaw =
    String(
      headers[
        'webhook-timestamp'
      ] ||
      ''
    ).trim();

  const signatureHeader =
    String(
      headers[
        'webhook-signature'
      ] ||
      ''
    ).trim();

  if (
    !webhookId ||
    !timestampRaw ||
    !signatureHeader
  ) {
    throw Object.assign(
      new Error(
        'Missing Dodo webhook signature headers.'
      ),
      {
        status:
          401,

        code:
          'DODO_WEBHOOK_HEADERS_MISSING'
      }
    );
  }

  const timestamp =
    Number(
      timestampRaw
    );

  if (
    !Number.isFinite(
      timestamp
    )
  ) {
    throw Object.assign(
      new Error(
        'Invalid Dodo webhook timestamp.'
      ),
      {
        status:
          401,

        code:
          'DODO_WEBHOOK_TIMESTAMP_INVALID'
      }
    );
  }

  const now =
    Math.floor(
      Date.now() /
      1000
    );

  if (
    now -
    timestamp >
    DODO_WEBHOOK_TOLERANCE_SECONDS
  ) {
    throw Object.assign(
      new Error(
        'Dodo webhook timestamp is too old.'
      ),
      {
        status:
          401,

        code:
          'DODO_WEBHOOK_TIMESTAMP_OLD'
      }
    );
  }

  if (
    timestamp -
    now >
    DODO_WEBHOOK_TOLERANCE_SECONDS
  ) {
    throw Object.assign(
      new Error(
        'Dodo webhook timestamp is too far in the future.'
      ),
      {
        status:
          401,

        code:
          'DODO_WEBHOOK_TIMESTAMP_FUTURE'
      }
    );
  }

  const key =
    decodeDodoWebhookSecret(
      DODO_WEBHOOK_SECRET
    );

  const prefix =
    Buffer.from(
      `${webhookId}.${timestampRaw}.`,
      'utf8'
    );

  const signedPayload =
    Buffer.concat([
      prefix,
      rawBody
    ]);

  const expected =
    createHmac(
      'sha256',
      key
    )
      .update(
        signedPayload
      )
      .digest();

  const candidates =
    signatureHeader
      .split(
        /\s+/
      )
      .filter(
        Boolean
      );

  const valid =
    candidates.some(
      entry => {
        const comma =
          entry.indexOf(
            ','
          );

        if (
          comma <= 0
        ) {
          return false;
        }

        const version =
          entry.slice(
            0,
            comma
          );

        const encoded =
          entry.slice(
            comma + 1
          );

        if (
          version !==
            'v1' ||
          !encoded
        ) {
          return false;
        }

        let supplied;

        try {
          supplied =
            Buffer.from(
              encoded,
              'base64'
            );

        } catch {
          return false;
        }

        return (
          supplied.length ===
            expected.length &&
          timingSafeEqual(
            supplied,
            expected
          )
        );
      }
    );

  if (!valid) {
    throw Object.assign(
      new Error(
        'Invalid Dodo webhook signature.'
      ),
      {
        status:
          401,

        code:
          'DODO_WEBHOOK_SIGNATURE_INVALID'
      }
    );
  }

  return webhookId;
}


function dodoChallengeResponse(
  challenge
) {
  const key =
    decodeDodoWebhookSecret(
      DODO_WEBHOOK_SECRET
    );

  const verificationKey =
    createHmac(
      'sha256',
      key
    )
      .update(
        'wev-v1',
        'utf8'
      )
      .digest();

  const signature =
    createHmac(
      'sha256',
      verificationKey
    )
      .update(
        String(
          challenge
        ),
        'utf8'
      )
      .digest(
        'base64'
      );

  return `v1,${signature}`;
}


function moneyFromMinorUnits(
  amount,
  currency
) {
  const value =
    Number(
      amount
    );

  if (
    !Number.isFinite(
      value
    )
  ) {
    return 0;
  }

  const zeroDecimal =
    new Set([
      'BIF',
      'CLP',
      'DJF',
      'GNF',
      'JPY',
      'KMF',
      'KRW',
      'MGA',
      'PYG',
      'RWF',
      'UGX',
      'VND',
      'VUV',
      'XAF',
      'XOF',
      'XPF'
    ]);

  return zeroDecimal.has(
    String(
      currency ||
      ''
    ).toUpperCase()
  )
    ? value
    : value / 100;
}


function normaliseDodoEvent(
  event,
  webhookId
) {
  const data =
    event &&
    typeof event ===
      'object'
      ? (
          event.data ||
          {}
        )
      : {};

  const metadata =
    data &&
    typeof data.metadata ===
      'object' &&
    data.metadata
      ? data.metadata
      : {};

  const customer =
    data &&
    typeof data.customer ===
      'object' &&
    data.customer
      ? data.customer
      : {};

  const eventType =
    textValue(
      event &&
      event.type
    );

  const currency =
    textValue(
      data.currency
    ).toUpperCase();

  const amount =
    moneyFromMinorUnits(
      data.recurring_pre_tax_amount,
      currency
    );

  return {
    event_type:
      eventType,

    provider_event_id:
      webhookId,

    provider:
      'Dodo',

    bubble_user_id:
      textValue(
        metadata.bubble_user_id ||
        metadata.bubbleUserId ||
        metadata.user_id ||
        metadata.userId
      ),

    plan:
      textValue(
        metadata.plan
      ),

    billing_customer_id:
      textValue(
        customer.customer_id
      ),

    billing_subscription_id:
      textValue(
        data.subscription_id
      ),

    provider_payment_id:
      textValue(
        data.payment_id
      ),

    product_id:
      textValue(
        data.product_id
      ),

    renewal_date:
      textValue(
        data.next_billing_date ||
        data.expires_at
      ),

    amount,

    currency,

    subscription_status:
      textValue(
        data.status
      ),

    customer_email:
      textValue(
        customer.email
      ),

    occurred_at:
      textValue(
        event &&
        event.timestamp
      ),

    raw_payload_type:
      textValue(
        data.payload_type
      )
  };
}


async function forwardDodoEventToBubble(
  payload
) {
  if (
    !BUBBLE_DODO_WEBHOOK_URL
  ) {
    throw Object.assign(
      new Error(
        'BUBBLE_DODO_WEBHOOK_URL is not configured.'
      ),
      {
        status:
          503,

        code:
          'BUBBLE_DODO_URL_MISSING'
      }
    );
  }

  if (
    !BUBBLE_DODO_WEBHOOK_TOKEN
  ) {
    throw Object.assign(
      new Error(
        'BUBBLE_DODO_WEBHOOK_TOKEN is not configured.'
      ),
      {
        status:
          503,

        code:
          'BUBBLE_DODO_TOKEN_MISSING'
      }
    );
  }

  const body =
    Buffer.from(
      JSON.stringify({
        ...payload,

        relay_token:
          BUBBLE_DODO_WEBHOOK_TOKEN
      })
    );

  const response =
    await requestBuffer(
      BUBBLE_DODO_WEBHOOK_URL,
      {
        method:
          'POST',

        headers: {
          'content-type':
            'application/json; charset=utf-8',

          'content-length':
            body.length
        },

        body
      }
    );

  if (
    response.status <
      200 ||
    response.status >=
      300
  ) {
    throw Object.assign(
      new Error(
        `Bubble Dodo relay returned HTTP ${response.status}.`
      ),
      {
        status:
          502,

        code:
          'BUBBLE_DODO_RELAY_FAILED',

        details: {
          bubbleStatus:
            response.status,

          bubbleResponse:
            response.body
        }
      }
    );
  }

  return response;
}


async function handleDodoWebhook(
  req,
  res
) {
  const challenge =
    textValue(
      req.headers[
        'webhook-challenge'
      ]
    );

  if (challenge) {
    if (
      !DODO_WEBHOOK_SECRET
    ) {
      return json(
        res,
        503,
        {
          ok:
            false,

          error:
            'DODO_WEBHOOK_SECRET is not configured.',

          code:
            'DODO_WEBHOOK_SECRET_MISSING'
        }
      );
    }

    const challengeResponse =
      dodoChallengeResponse(
        challenge
      );

    const payload =
      Buffer.from(
        JSON.stringify({
          ok:
            true,

          challenge:
            true
        })
      );

    res.writeHead(
      200,
      {
        'content-type':
          'application/json; charset=utf-8',

        'content-length':
          payload.length,

        'cache-control':
          'no-store',

        'webhook-challenge-response':
          challengeResponse
      }
    );

    return res.end(
      payload
    );
  }

  if (
    !DODO_WEBHOOK_SECRET
  ) {
    return json(
      res,
      503,
      {
        ok:
          false,

        error:
          'DODO_WEBHOOK_SECRET is not configured.',

        code:
          'DODO_WEBHOOK_SECRET_MISSING'
      }
    );
  }

  const rawBody =
    await readRawBody(
      req
    );

  const webhookId =
    verifyDodoWebhook(
      rawBody,
      req.headers
    );

  let event;

  try {
    event =
      JSON.parse(
        rawBody.toString(
          'utf8'
        ) || '{}'
      );

  } catch {
    throw Object.assign(
      new Error(
        'Invalid JSON in Dodo webhook body.'
      ),
      {
        status:
          400,

        code:
          'DODO_WEBHOOK_JSON_INVALID'
      }
    );
  }

  const normalised =
    normaliseDodoEvent(
      event,
      webhookId
    );

  console.log(
    '[DODO WEBHOOK VERIFIED]',
    JSON.stringify({
      event_type:
        normalised.event_type,

      provider_event_id:
        normalised.provider_event_id,

      bubble_user_id:
        normalised.bubble_user_id,

      plan:
        normalised.plan,

      billing_subscription_id:
        normalised.billing_subscription_id
    })
  );

  await forwardDodoEventToBubble(
    normalised
  );

  return json(
    res,
    200,
    {
      ok:
        true,

      received:
        true,

      event_type:
        normalised.event_type,

      provider_event_id:
        normalised.provider_event_id
    }
  );
}


/* =========================================================
   HTTP SERVER
========================================================= */


const server =
  http.createServer(
    async (
      req,
      res
    ) => {

      const url =
        new URL(
          req.url,
          `http://${req.headers.host || 'localhost'}`
        );


      /* -----------------------------------------------------
         HEALTH
      ----------------------------------------------------- */

      if (
        req.method ===
          'GET' &&
        url.pathname ===
          '/health'
      ) {
        const version =
          await ffmpegVersion();

        return json(
          res,
          version
            ? 200
            : 503,
          {
            ok:
              Boolean(
                version
              ),

            service:
              'olivia-render-worker',

            worker:
              'V129-DODO-WEBHOOK',

            ffmpeg:
              version
          }
        );
      }


      /* -----------------------------------------------------
         PUBLIC OUTPUT
      ----------------------------------------------------- */

      if (
        req.method ===
          'GET' &&
        url.pathname.startsWith(
          '/outputs/'
        )
      ) {
        return serveOutput(
          req,
          res,
          url.pathname
        );
      }


      /* -----------------------------------------------------
         PUBLIC DOWNLOAD
      ----------------------------------------------------- */

      if (
        req.method ===
          'GET' &&
        url.pathname.startsWith(
          '/download/'
        )
      ) {
        return serveDownload(
          req,
          res,
          url.pathname
        );
      }


      /*
        Dodo must reach this route without WORKER_TOKEN.
        Security is provided by Dodo's verified webhook signature.
      */

      if (
        req.method ===
          'POST' &&
        url.pathname ===
          '/dodo/webhook'
      ) {
        try {
          return await handleDodoWebhook(
            req,
            res
          );

        } catch (
          error
        ) {
          console.error(
            '[DODO WEBHOOK ERROR]',
            error
          );

          return json(
            res,
            error.status ||
              500,
            {
              ok:
                false,

              error:
                error.message,

              code:
                error.code ||
                'DODO_WEBHOOK_FAILED',

              details:
                error.details ||
                null
            }
          );
        }
      }


      /* -----------------------------------------------------
         PRIVATE ROUTES REQUIRE WORKER TOKEN
      ----------------------------------------------------- */

      if (
        !authorized(
          req
        )
      ) {
        return json(
          res,
          401,
          {
            error:
              'Unauthorized.'
          }
        );
      }


      /* -----------------------------------------------------
         YOUTUBE UPLOAD
      ----------------------------------------------------- */

      if (
        req.method ===
          'POST' &&
        url.pathname ===
          '/youtube/upload'
      ) {
        const started =
          Date.now();

        try {
          const body =
            await readJson(
              req
            );

          const result =
            await uploadRenderedVideoToYouTube(
              body
            );

          return json(
            res,
            200,
            {
              ...result,

              elapsedMs:
                Date.now() -
                started
            }
          );

        } catch (
          error
        ) {
          console.error(
            '[OLIVIA YOUTUBE UPLOAD ERROR]',
            error
          );

          return json(
            res,
            error.status ||
              502,
            {
              schema:
                'OLIVIA_YOUTUBE_UPLOAD_RESULT_V1',

              ok:
                false,

              status:
                'failed',

              error:
                error.message,

              code:
                error.code ||
                'YOUTUBE_UPLOAD_FAILED',

              details:
                error.details ||
                null,

              elapsedMs:
                Date.now() -
                started
            }
          );
        }
      }


      /* -----------------------------------------------------
         PLAN
      ----------------------------------------------------- */

      if (
        req.method ===
          'POST' &&
        url.pathname ===
          '/plan'
      ) {
        try {
          const job =
            await readJson(
              req
            );

          const validation =
            validateJob(
              job
            );

          if (
            !validation.ok
          ) {
            return json(
              res,
              400,
              {
                ok:
                  false,

                errors:
                  validation.errors
              }
            );
          }

          return json(
            res,
            200,
            {
              ok:
                true,

              schema:
                job.schema,

              jobId:
                job.jobId,

              output:
                chooseOutputSize(
                  job.manifest
                ),

              support:
                inspectSupport(
                  job.manifest
                )
            }
          );

        } catch (
          error
        ) {
          return json(
            res,
            error.status ||
              400,
            {
              error:
                error.message
            }
          );
        }
      }


      /* -----------------------------------------------------
         ASYNC RENDER START
      ----------------------------------------------------- */

      if (
        req.method ===
          'POST' &&
        url.pathname ===
          '/render/start'
      ) {
        try {
          const job =
            await readJson(
              req
            );

          const validation =
            validateJob(
              job
            );

          if (
            !validation.ok
          ) {
            return json(
              res,
              400,
              {
                schema:
                  'OLIVIA_RENDER_ACCEPTED_V1',

                status:
                  'rejected',

                errors:
                  validation.errors
              }
            );
          }

          const support =
            inspectSupport(
              job.manifest
            );

          if (
            support.blocking &&
            support.blocking.length
          ) {
            return json(
              res,
              200,
              {
                schema:
                  'OLIVIA_RENDER_ACCEPTED_V1',

                jobId:
                  safeJobId(
                    job
                  ),

                status:
                  'rejected',

                progress:
                  5,

                phase:
                  'failed',

                statusUrl:
                  '',

                error:
                  support.blocking.join(
                    ' | '
                  ),

                errors:
                  support.blocking,

                warnings:
                  support.warnings ||
                  []
              }
            );
          }

          const jobId =
            safeJobId(
              job
            );

          if (
            renderJobs.has(
              jobId
            )
          ) {
            const existing =
              renderJobs.get(
                jobId
              );

            return json(
              res,
              200,
              {
                schema:
                  'OLIVIA_RENDER_ACCEPTED_V1',

                jobId,

                status:
                  existing.status,

                progress:
                  existing.progress ??
                  0,

                phase:
                  existing.phase ||
                  existing.status,

                statusUrl:
                  publicStatusUrl(
                    jobId
                  ),

                duplicate:
                  true
              }
            );
          }

          renderJobs.set(
            jobId,
            {
              jobId,

              status:
                'queued',

              progress:
                0,

              phase:
                'queued',

              progressDetail:
                null,

              updatedAt:
                new Date()
                  .toISOString(),

              queuedAt:
                new Date()
                  .toISOString(),

              startedAt:
                null,

              completedAt:
                null,

              outputUrl:
                null,

              downloadUrl:
                null,

              error:
                null,

              code:
                null,

              details:
                null,

              ffmpeg:
                null,

              elapsedMs:
                null,

              result:
                null
            }
          );

          setImmediate(
            () => {
              runRenderInBackground({
                ...job,
                jobId
              }).catch(
                error => {
                  console.error(
                    '[OLIVIA BACKGROUND TASK ERROR]',
                    error
                  );
                }
              );
            }
          );

          return json(
            res,
            202,
            {
              schema:
                'OLIVIA_RENDER_ACCEPTED_V1',

              jobId,

              status:
                'queued',

              progress:
                0,

              phase:
                'queued',

              statusUrl:
                publicStatusUrl(
                  jobId
                )
            }
          );

        } catch (
          error
        ) {
          return json(
            res,
            error.status ||
              400,
            {
              schema:
                'OLIVIA_RENDER_ACCEPTED_V1',

              status:
                'rejected',

              error:
                error.message
            }
          );
        }
      }


      /* -----------------------------------------------------
         ASYNC RENDER STATUS
      ----------------------------------------------------- */

      if (
        req.method ===
          'GET' &&
        url.pathname.startsWith(
          '/render/status/'
        )
      ) {
        const jobId =
          decodeURIComponent(
            url.pathname.slice(
              '/render/status/'.length
            )
          );

        const state =
          renderJobs.get(
            jobId
          );

        if (!state) {
          return json(
            res,
            404,
            {
              schema:
                'OLIVIA_RENDER_STATUS_V1',

              jobId,

              status:
                'not_found',

              error:
                'Render job is not present in this worker process.'
            }
          );
        }

        return json(
          res,
          200,
          {
            schema:
              'OLIVIA_RENDER_STATUS_V1',

            jobId,

            status:
              state.status,

            progress:
              state.progress ??
              0,

            phase:
              state.phase ||
              state.status,

            progressDetail:
              state.progressDetail ||
              null,

            updatedAt:
              state.updatedAt ||
              null,

            queuedAt:
              state.queuedAt,

            startedAt:
              state.startedAt,

            completedAt:
              state.completedAt,

            outputUrl:
              state.outputUrl,

            downloadUrl:
              state.downloadUrl,

            error:
              state.error,

            code:
              state.code,

            details:
              state.details,

            elapsedMs:
              state.elapsedMs,

            result:
              state.result
          }
        );
      }


      /* -----------------------------------------------------
         SYNCHRONOUS RENDER
      ----------------------------------------------------- */

      if (
        req.method ===
          'POST' &&
        url.pathname ===
          '/render'
      ) {
        const started =
          Date.now();

        try {
          const job =
            await readJson(
              req
            );

          const result =
            await renderJob(
              job,
              {
                outputDir:
                  OUTPUT_DIR
              }
            );

          const filename =
            path.basename(
              result.outputPath
            );

          result.outputUrl =
            publicOutputUrl(
              filename
            );

          result.downloadUrl =
            publicDownloadUrl(
              filename
            );

          result.elapsedMs =
            Date.now() -
            started;

          delete result.outputPath;

          return json(
            res,
            200,
            result
          );

        } catch (
          error
        ) {
          console.error(
            '[OLIVIA RENDER ERROR]',
            error
          );

          return json(
            res,
            error.status ||
              422,
            {
              schema:
                'OLIVIA_RENDER_RESULT_V1',

              status:
                'failed',

              error:
                error.message,

              code:
                error.code ||
                'RENDER_FAILED',

              details:
                error.details ||
                null,

              ffmpeg:
                error.stderr
                  ? String(
                      error.stderr
                    ).slice(
                      -12000
                    )
                  : null,

              elapsedMs:
                Date.now() -
                started
            }
          );
        }
      }


      /* -----------------------------------------------------
         NOT FOUND
      ----------------------------------------------------- */

      json(
        res,
        404,
        {
          error:
            'Not found.'
        }
      );
    }
  );


server.listen(
  PORT,
  () => {
    console.log(
      `OLIVIA render worker listening on :${PORT}`
    );
  }
);
