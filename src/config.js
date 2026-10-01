'use strict';

const fs = require('fs');
const path = require('path');
const environments = require('../config/environments.json');

// Igual que la versión Python: la variable `environment` elige el bloque (dev por defecto).
const envName = process.env.environment || 'dev';
if (!environments[envName]) {
  throw new Error(`Ambiente desconocido: "${envName}". Opciones: dev, stage, prod`);
}
const envCfg = { ...environments.defaults, ...environments[envName] };

const SECRETS_DIR = process.env.SECRETS_DIR || '/run/secrets';

// Variable de entorno > config/environments.json.
function setting(name) {
  const value = process.env[name] ?? envCfg[name];
  return value === undefined || value === '' ? undefined : value;
}

// Variable de entorno en mayúsculas (ECS / Secrets Manager) > archivo en /run/secrets (Docker secrets).
// Se conservan los nombres de secreto de la versión Python.
function secret(name) {
  const fromEnv = process.env[name.toUpperCase()];
  if (fromEnv) return fromEnv;
  try {
    return fs.readFileSync(path.join(SECRETS_DIR, name), 'utf8').replace(/\n+$/, '');
  } catch {
    return undefined;
  }
}

const config = {
  environment: envName,
  dryRun: String(setting('DRY_RUN')).toLowerCase() === 'true',
  apiUrl: setting('API_URL'),
  apiUser: setting('API_USER'),
  apiToken: secret('token_key'),
  pollIntervalMs: Number(setting('POLL_INTERVAL_SECONDS')) * 1000,
  lookbackHours: Number(setting('LOOKBACK_HOURS')),
  httpTimeoutMs: Number(setting('HTTP_TIMEOUT_SECONDS')) * 1000,
  mzone: {
    apiUrl: setting('MZONE_API_URL'),
    tokenUrl: setting('MZONE_TOKEN_URL'),
    clientId: setting('MZONE_CLIENT_ID'),
    clientSecret: secret('mzone_secret'),
    scope: setting('MZONE_SCOPE'),
  },
  rabbit: {
    host: setting('RABBITMQ_HOST'),
    port: Number(setting('RABBITMQ_PORT')),
    vhost: setting('RABBITMQ_VHOST'),
    user: secret('rabbitmq_user'),
    password: secret('rabbitmq_passw'),
    exchange: setting('RABBITMQ_EXCHANGE'),
    routingKey: setting('RABBITMQ_ROUTING_KEY'),
  },
};

const required = {
  API_URL: config.apiUrl,
  token_key: config.apiToken,
  mzone_secret: config.mzone.clientSecret,
  rabbitmq_user: config.rabbit.user,
  rabbitmq_passw: config.rabbit.password,
  RABBITMQ_HOST: config.rabbit.host,
};
const missing = Object.keys(required).filter((key) => !required[key]);
if (missing.length > 0) {
  throw new Error(`Faltan valores de configuración para "${envName}": ${missing.join(', ')}`);
}

module.exports = config;
