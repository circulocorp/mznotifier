'use strict';

const amqp = require('amqplib');
const logger = require('./logger');

const CONNECT_TIMEOUT_MS = 10000;
const CONFIRM_TIMEOUT_MS = 15000;

// Una sola conexión reutilizada; si se cae, se reabre en la siguiente publicación.
class Publisher {
  constructor({ host, port, vhost, user, password, exchange, routingKey }) {
    this.connectOptions = { protocol: 'amqp', hostname: host, port, vhost, username: user, password, heartbeat: 30 };
    this.exchange = exchange;
    this.routingKey = routingKey;
    this.connection = null;
    this.channel = null;
    this.connecting = null;
  }

  async getChannel() {
    if (this.channel) return this.channel;
    if (!this.connecting) {
      this.connecting = this.connect().finally(() => {
        this.connecting = null;
      });
    }
    return this.connecting;
  }

  async connect() {
    const connection = await amqp.connect(this.connectOptions, { timeout: CONNECT_TIMEOUT_MS });
    connection.on('error', (err) => logger.error('RabbitMQ connection error', { error: err.message }));
    connection.on('close', () => {
      if (this.connection === connection) {
        this.connection = null;
        this.channel = null;
      }
    });

    let channel;
    try {
      channel = await connection.createConfirmChannel();
      channel.on('error', (err) => logger.error('RabbitMQ channel error', { error: err.message }));
      channel.on('close', () => {
        if (this.channel === channel) this.channel = null;
      });
      await channel.assertExchange(this.exchange, 'direct', { durable: true });
    } catch (err) {
      connection.close().catch(() => {});
      throw err;
    }

    this.connection = connection;
    this.channel = channel;
    return channel;
  }

  // Resuelve solo cuando el broker confirma el mensaje. Si la confirmación no llega a tiempo,
  // descarta la conexión para que la siguiente publicación abra una nueva y no se congele el ciclo.
  async publish(payload) {
    const channel = await this.getChannel();
    const body = Buffer.from(JSON.stringify(payload));
    let timer;
    try {
      await Promise.race([
        new Promise((resolve, reject) => {
          channel.publish(this.exchange, this.routingKey, body, { persistent: true }, (err) =>
            err ? reject(err) : resolve()
          );
        }),
        new Promise((_, reject) => {
          timer = setTimeout(
            () => reject(new Error(`RabbitMQ no confirmó la publicación en ${CONFIRM_TIMEOUT_MS} ms`)),
            CONFIRM_TIMEOUT_MS
          );
        }),
      ]);
    } catch (err) {
      this.reset();
      throw err;
    } finally {
      clearTimeout(timer);
    }
  }

  reset() {
    const connection = this.connection;
    this.connection = null;
    this.channel = null;
    if (connection) connection.close().catch(() => {});
  }

  async close() {
    try {
      if (this.connection) await this.connection.close();
    } catch (err) {
      logger.warn('Error closing RabbitMQ connection', { error: err.message });
    }
  }
}

module.exports = Publisher;
