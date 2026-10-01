'use strict';

const amqp = require('amqplib');
const logger = require('./logger');

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
    const connection = await amqp.connect(this.connectOptions);
    connection.on('error', (err) => logger.error('RabbitMQ connection error', { error: err.message }));
    connection.on('close', () => {
      this.connection = null;
      this.channel = null;
    });

    const channel = await connection.createConfirmChannel();
    channel.on('error', (err) => logger.error('RabbitMQ channel error', { error: err.message }));
    channel.on('close', () => {
      this.channel = null;
    });
    await channel.assertExchange(this.exchange, 'direct', { durable: true });

    this.connection = connection;
    this.channel = channel;
    return channel;
  }

  // Resuelve solo cuando el broker confirma el mensaje.
  async publish(payload) {
    const channel = await this.getChannel();
    const body = Buffer.from(JSON.stringify(payload));
    await new Promise((resolve, reject) => {
      channel.publish(this.exchange, this.routingKey, body, { persistent: true }, (err) =>
        err ? reject(err) : resolve()
      );
    });
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
