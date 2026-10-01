'use strict';

const fs = require('node:fs');

/** Read one bounded chunk at a time, including while a consumer awaits a query. */
async function* records(file, signal) {
    const stream = fs.createReadStream(file, { encoding: 'utf8' });
    let pending = '';
    function checkCancelled() {
        if (!signal?.aborted) return;
        throw Object.assign(new Error('Execução cancelada.'), { code: 'FLOW_CANCELLED' });
    }
    try {
        for await (const chunk of stream) {
            checkCancelled();
            pending += chunk;
            let start = 0, end;
            while ((end = pending.indexOf('\n', start)) !== -1) {
                checkCancelled();
                const line = pending.slice(start, end);
                start = end + 1;
                if (line.trim()) yield JSON.parse(line);
            }
            pending = pending.slice(start);
        }
        checkCancelled();
        if (pending.trim()) yield JSON.parse(pending);
    } finally { stream.destroy(); }
}

module.exports = { records };
