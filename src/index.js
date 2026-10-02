'use strict';

const config = require('./config');
const logger = require('./logger');
const MZone = require('./mzone');
const Publisher = require('./rabbit');

const publisher = new Publisher(config.rabbit);

async function getAccounts() {
  const auth = Buffer.from(`${config.apiUser}:${config.apiToken}`).toString('base64');
  const resp = await fetch(`${config.apiUrl}/api/notificationadmins`, {
    headers: { Authorization: `Basic ${auth}` },
    signal: AbortSignal.timeout(config.httpTimeoutMs),
  });
  if (!resp.ok) throw new Error(`notificationadmins respondió ${resp.status}`);
  const accounts = await resp.json();
  if (!Array.isArray(accounts)) throw new Error('notificationadmins no devolvió una lista');
  return accounts;
}

// Mismo formato que la versión Python: YYYY-MM-DDTHH:MM:SSZ
function since(hours) {
  return new Date(Date.now() - hours * 3600 * 1000).toISOString().slice(0, 19) + 'Z';
}

async function getSubscriberPhones(mz, template, account) {
  try {
    const subscriptions = await mz.getSubscriptions(`id eq ${template}`);
    const phones = subscriptions.map((s) => s.subscriber && s.subscriber.phoneMobile).filter(Boolean);
    return [...new Set(phones)];
  } catch (err) {
    logger.warn('Cant retrive phone subscriptions', { account: account.user, template, error: err.message });
    return [];
  }
}

async function processAccount(account) {
  logger.info('Searching notifications for ' + account.user);
  const mz = new MZone({
    ...config.mzone,
    user: account.user,
    password: account.password,
    timeoutMs: config.httpTimeoutMs,
  });

  try {
    await mz.getToken();
  } catch (err) {
    logger.error('Cant connect to MZone using ' + account.user, { account: account.user, error: err.message });
    return;
  }

  const notifications = await mz.getNotifications(
    `readUtcTimestamp eq null and utcTimestamp gt ${since(config.lookbackHours)}`
  );
  if (notifications.length === 0) {
    logger.info('No notifications found for ' + account.user);
    return;
  }
  logger.info('Reading notifications', { notifications });

  const messages = notifications.map((n) => ({ template: n.notificationTemplate_Id, text: n.message, id: n.id }));

  const phonesByTemplate = new Map();
  for (const template of new Set(messages.map((m) => m.template))) {
    phonesByTemplate.set(template, await getSubscriberPhones(mz, template, account));
  }

  const extraSubscribers = (account.extraSubscribers || '')
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean);

  const envelopes = [];
  for (const message of messages) {
    for (const phone of phonesByTemplate.get(message.template)) {
      envelopes.push({ message: message.text, address: phone });
    }
    for (const phone of extraSubscribers) {
      envelopes.push({ message: message.text, address: phone });
    }
  }

  if (envelopes.length === 0) {
    logger.info('There is nothing to send to RabbitMQ for ' + account.user);
    return;
  }

  const payload = { data: envelopes };
  if (config.dryRun) {
    logger.info('DRY_RUN: would post message to RabbitMQ and mark read ' + account.user, {
      message: JSON.stringify(payload),
    });
    return;
  }
  logger.info('Posting message to RabbitMQ', { message: JSON.stringify(payload) });
  try {
    await publisher.publish(payload);
  } catch (err) {
    // No se marcan como leídas: se reintentan en el siguiente ciclo.
    logger.error('Cant publish to rabbitmq ' + account.user, { error: err.message });
    return;
  }

  try {
    await mz.markAsRead(messages.map((m) => m.id));
    logger.info('Notifications set read mark', { notifications: messages });
  } catch (err) {
    logger.error('Problem setting read mark ' + account.user, { notifications: messages, error: err.message });
  }
}

async function runCycle() {
  let accounts;
  try {
    accounts = await getAccounts();
  } catch (err) {
    logger.error('Cant get accounts information', { error: err.message });
    return;
  }

  const started = Date.now();
  const results = await Promise.allSettled(accounts.map(processAccount));
  results.forEach((result, i) => {
    if (result.status === 'rejected') {
      logger.error('Error processing account ' + accounts[i].user, { error: result.reason.message });
    }
  });
  logger.info('Cycle finished', { accounts: accounts.length, durationMs: Date.now() - started });
}

async function main() {
  logger.info(`Starting mznotifier (${config.environment})`, { dryRun: config.dryRun });

  let stopping = false;
  let wake = null;
  const stop = (signal) => {
    logger.info(`Received ${signal}, shutting down`);
    stopping = true;
    if (wake) wake();
  };
  process.on('SIGTERM', () => stop('SIGTERM'));
  process.on('SIGINT', () => stop('SIGINT'));

  while (!stopping) {
    const started = Date.now();
    await runCycle();
    const wait = Math.max(0, config.pollIntervalMs - (Date.now() - started));
    await new Promise((resolve) => {
      wake = resolve;
      setTimeout(resolve, wait);
    });
  }

  await publisher.close();
  process.exit(0);
}

main().catch((err) => {
  logger.error('Fatal error', { error: err.message });
  process.exit(1);
});
