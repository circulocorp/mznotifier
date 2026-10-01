'use strict';

// Una línea JSON por evento, con los campos `app` y `label` que ya emitía la versión Python.
const APP = 'mznotifier';

function write(level, msg, props = {}) {
  const line = {
    type: 'log',
    written_at: new Date().toISOString(),
    logger: APP,
    level,
    msg,
    app: APP,
    label: APP,
    ...props,
  };
  process.stdout.write(JSON.stringify(line) + '\n');
}

module.exports = {
  info: (msg, props) => write('INFO', msg, props),
  warn: (msg, props) => write('WARNING', msg, props),
  error: (msg, props) => write('ERROR', msg, props),
};
