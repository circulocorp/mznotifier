# mznotifier
MZone notifier SMS service

Cada `POLL_INTERVAL_SECONDS` (120 s) consulta las cuentas en `{API_URL}/api/notificationadmins`, lee las
notificaciones no leídas de MZone de las últimas `LOOKBACK_HOURS` (12 h), publica los SMS en RabbitMQ
(exchange `circulocorp`, routing key `notificaciones`, formato `{"data":[{"message","address"}]}`) y,
solo si el broker confirmó la publicación, las marca como leídas en MZone.

## Configuración

- `config/environments.json`: valores no secretos por ambiente (`defaults` + `dev` / `stage` / `prod`).
  La variable `environment` elige el bloque (por defecto `dev`). Cualquier clave se puede sobrescribir
  con una variable de entorno del mismo nombre.
- Secretos: variable de entorno en mayúsculas o, si no existe, archivo `/run/secrets/<nombre>`.

| Secreto           | Variable          | Uso                                       |
|-------------------|-------------------|-------------------------------------------|
| `token_key`       | `TOKEN_KEY`       | Basic Auth contra `notificationadmins`    |
| `mzone_secret`    | `MZONE_SECRET`    | `client_secret` OAuth2 de MZone           |
| `rabbitmq_user`   | `RABBITMQ_USER`   | Usuario RabbitMQ                          |
| `rabbitmq_passw`  | `RABBITMQ_PASSW`  | Contraseña RabbitMQ                       |

Los usuarios y contraseñas de MZone de cada cuenta vienen de `notificationadmins`.

## Prueba en producción sin enviar SMS

Con `DRY_RUN=true` el servicio consulta MZone y registra `DRY_RUN: would post message...` con el
mensaje exacto que enviaría, pero no publica en RabbitMQ ni marca notificaciones como leídas. Permite
correrlo en paralelo a la versión Python. `buildspec.yml` solo mueve el tag `latest` si `TAG_LATEST=true`.

## Ejecutar

```sh
npm install
cp .env.example .env   # llenar valores
node --env-file=.env src/index.js
```

Docker (base `ubuntu:24.04`, Node 24):

```sh
docker build -t mznotifier .
docker run --env-file .env mznotifier
```

El build y push a ECR lo hace `buildspec.yml` (AWS CodeBuild).

La versión anterior en Python y el código de PydoNovosoft quedan en `legacy/` (fuera de git).
