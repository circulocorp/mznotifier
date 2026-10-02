# ubuntu:20.04 (glibc 2.31): los nodos del swarm usan Docker < 20.10.10, cuyo seccomp bloquea
# clone3; con glibc >= 2.34 (ubuntu 22.04+) Node no puede crear hilos y aborta al arrancar.
FROM ubuntu:20.04

ARG NODE_MAJOR=24
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl gnupg \
 && curl -fsSL https://deb.nodesource.com/setup_${NODE_MAJOR}.x | bash - \
 && apt-get install -y --no-install-recommends nodejs \
 && apt-get purge -y curl gnupg \
 && apt-get autoremove -y \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY package.json package-lock.json* ./
RUN if [ -f package-lock.json ]; then npm ci --omit=dev; else npm install --omit=dev; fi \
 && npm cache clean --force
COPY config ./config
COPY src ./src

ENV NODE_ENV=production
RUN useradd --system --no-create-home --shell /usr/sbin/nologin mznotifier
USER mznotifier
CMD ["node", "src/index.js"]
