FROM node:20-bookworm-slim

RUN apt-get update \
  && apt-get install -y --no-install-recommends ffmpeg ca-certificates fonts-dejavu-core \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY package.json ./
COPY src ./src

RUN mkdir -p /data/outputs

ENV PORT=8080
ENV OUTPUT_DIR=/data/outputs
EXPOSE 8080

CMD ["node", "src/server.js"]
