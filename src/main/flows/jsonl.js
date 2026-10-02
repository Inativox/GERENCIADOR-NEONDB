'use strict';

const fs = require('node:fs');

/** Read one bounded chunk at a time, including while a consumer awaits a query. */
async function* entries(file, signal, startOffset = 0) {
    if (/\.jsonl\.pack(?:\.tmp)?$/.test(file)) {
        yield* require('./packedJsonl').packedEntries(file, signal, startOffset);
        return;
    }
    const stream = fs.createReadStream(file, { start: startOffset });
    let pending = Buffer.alloc(0), offset = startOffset;
    function checkCancelled() {
        if (!signal?.aborted) return;
        throw Object.assign(new Error('Execução cancelada.'), { code: 'FLOW_CANCELLED' });
    }
    try {
        for await (const chunk of stream) {
            checkCancelled();
            pending = pending.length ? Buffer.concat([pending, chunk]) : chunk;
            let start = 0, end;
            while ((end = pending.indexOf(10, start)) !== -1) {
                checkCancelled();
                const line = pending.subarray(start, end).toString('utf8');
                start = end + 1;
                if (line.trim()) yield { row: JSON.parse(line), offset: offset + end + 1 };
            }
            offset += start;
            pending = pending.subarray(start);
        }
        checkCancelled();
        const last = pending.toString('utf8');
        if (last.trim()) yield { row: JSON.parse(last), offset: offset + pending.length };
    } finally { stream.destroy(); }
}

async function* records(file, signal, startOffset = 0) {
    for await (const entry of entries(file, signal, startOffset)) yield entry.row;
}
module.exports = { records, entries };
