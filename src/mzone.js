'use strict';

// Reemplazo de PydoNovosoft.scope.MZone: solo las llamadas que usa mznotifier.
class MZone {
  constructor({ user, password, clientId, clientSecret, apiUrl, tokenUrl, scope, timeoutMs }) {
    this.user = user;
    this.password = password;
    this.clientId = clientId;
    this.clientSecret = clientSecret;
    this.apiUrl = apiUrl;
    this.tokenUrl = tokenUrl;
    this.scope = scope;
    this.timeoutMs = timeoutMs;
    this.accessToken = null;
    this.validUntil = 0;
  }

  async getToken() {
    const body = new URLSearchParams({
      grant_type: 'password',
      username: this.user,
      password: this.password,
      client_id: this.clientId,
      client_secret: this.clientSecret,
      scope: this.scope,
    });
    const resp = await fetch(this.tokenUrl, {
      method: 'POST',
      body,
      signal: AbortSignal.timeout(this.timeoutMs),
    });
    if (!resp.ok) {
      throw new Error(`MZone rechazó el token (${resp.status}): ${await resp.text()}`);
    }
    const token = await resp.json();
    this.accessToken = token.access_token;
    this.validUntil = Date.now() + Number(token.expires_in) * 1000;
  }

  hasValidToken() {
    return Boolean(this.accessToken) && Date.now() < this.validUntil;
  }

  async request(pathAndQuery, options = {}) {
    if (!this.hasValidToken()) await this.getToken();
    const resp = await fetch(this.apiUrl + pathAndQuery, {
      ...options,
      headers: { ...options.headers, Authorization: `Bearer ${this.accessToken}` },
      signal: AbortSignal.timeout(this.timeoutMs),
    });
    if (!resp.ok) {
      throw new Error(`MZone ${pathAndQuery.split('?')[0]} respondió ${resp.status}: ${await resp.text()}`);
    }
    return resp;
  }

  async getNotifications(filter) {
    const resp = await this.request(`Notifications?$format=json&$filter=${encodeURIComponent(filter)}`);
    return (await resp.json()).value || [];
  }

  async getSubscriptions(filter) {
    const resp = await this.request(
      `NotificationTemplates/_.getForAllUsers?$format=json&$expand=subscriber&$filter=${encodeURIComponent(filter)}`
    );
    return (await resp.json()).value || [];
  }

  async markAsRead(notificationIds) {
    await this.request('Notifications/_.markAsRead', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ notificationIds }),
    });
  }
}

module.exports = MZone;
