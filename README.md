# OLIVIA Render Worker V1

This is the first real FFmpeg backend worker for the OLIVIA editor.

## What it does now

- Accepts `OLIVIA_RENDER_JOB_V1`
- Downloads the source video
- Trims and reorders active clips
- Uses the project aspect ratio and `object-fit: cover` style scale/crop
- Renders brightness / contrast / saturation / hue
- Renders video fade-in / fade-out
- Renders Dip Black / Dip White using the same two-half, non-overlap timeline model used by the OLIVIA preview
- Renders clip audio, mute / solo rule, volume, automation, pan, audio fade-in/out, detached audio ranges and offsets
- Concatenates final video + audio without shortening the OLIVIA sequence
- Burns basic text overlays from the V113/V114 manifest
- Produces H.264 + AAC MP4 with `faststart`
- Serves completed files from `/outputs/<jobId>.mp4`

## Intentionally blocked in Worker V1

The worker refuses a job instead of silently producing a mismatched video when it sees:

- `blur` transition
- `zoom` transition
- non-zero `warmth`
- non-zero `tint`

Those are the next exact-preview matching tasks.

Multiline overlay wrapping is allowed but reported as a warning because browser box wrapping and FFmpeg `drawtext` are not identical yet.

## Run locally

```bash
npm run check
npm run selftest
node src/server.js
```

Health:

```bash
curl http://localhost:8080/health
```

Plan only:

```bash
curl -X POST http://localhost:8080/plan \
  -H 'Content-Type: application/json' \
  --data-binary @render-job.json
```

Render:

```bash
curl -X POST http://localhost:8080/render \
  -H 'Content-Type: application/json' \
  --data-binary @render-job.json
```

## Docker

```bash
docker build -t olivia-render-worker .
docker run --rm -p 8080:8080 \
  -e PUBLIC_BASE_URL=http://localhost:8080 \
  -v "$PWD/data/outputs:/data/outputs" \
  olivia-render-worker
```

## Optional security

Set `WORKER_TOKEN`. Then Bubble must send:

`Authorization: Bearer <WORKER_TOKEN>`

## Bubble backend workflow

Your existing `RenderJob` record already stores the `OLIVIA_RENDER_JOB_V1` JSON.

The next Bubble backend action will POST that JSON to:

`POST /render`

On a successful response update the RenderJob:

- `status = completed`
- `output_url = response outputUrl`
- `progress = 100`
- `completed_at = Current date/time`

On failure:

- `status = failed`
- `error_message = response error`

For a first deployment, `/outputs` is fine for end-to-end testing. Production should later move rendered MP4s to durable object storage.
