'use strict';

const fs = require('node:fs');
const { gzip, gunzip } = require('node:zlib');
const { promisify } = require('node:util');
const compress = promisify(gzip), decompress = promisify(gunzip);
const MAX_RAW_BYTES = 16 * 1024 * 1024;
const MAX_PACKED_BYTES = MAX_RAW_BYTES + 65536;

// Independent gzip frames keep random access and durable batch checkpoints.
// A cursor contains the frame byte position and the next row inside that frame.
async function writeData(stream, data) {
    if (stream.destroyed || stream.errored) throw stream.errored || new Error('O arquivo de processamento foi fechado.');
    await new Promise((resolve, reject) => stream.write(data, error => error ? reject(error) : resolve()));
}

async function writeBuffered(stream, data) {
    const unavailable = () => stream.errored || new Error('O arquivo de processamento foi fechado.');
    if (stream.destroyed || stream.errored) throw unavailable();
    if (stream.write(data)) return;
    if (stream.destroyed || stream.errored) throw unavailable();
    await new Promise((resolve, reject) => {
        const cleanup = () => { stream.off('drain', drain); stream.off('error', error); stream.off('close', closed); };
        const drain = () => { cleanup(); resolve(); };
        const error = value => { cleanup(); reject(value); };
        const closed = () => { cleanup(); reject(unavailable()); };
        stream.once('drain', drain); stream.once('error', error); stream.once('close', closed);
    });
}

function createPackedWriter(stream, initialBytes = 0) {
    let lines = [], bytes = 0, outputBytes = initialBytes;
    async function flush() {
        if (!lines.length) return;
        const raw = Buffer.from(lines.join(''));
        const packed = await compress(raw, { level: 3 });
        const header = Buffer.alloc(8);
        header.writeUInt32LE(packed.length, 0); header.writeUInt32LE(raw.length, 4);
        await writeData(stream, Buffer.concat([header, packed]));
        outputBytes += 8 + packed.length;
        lines = []; bytes = 0;
    }
    return {
        get outputBytes() { return outputBytes; },
        async append(row) {
            const line = JSON.stringify(row) + '\n', size = Buffer.byteLength(line);
            if (size > MAX_RAW_BYTES) throw new Error('Registro excede o limite do cache.');
            if (bytes + size > MAX_RAW_BYTES) await flush();
            lines.push(line); bytes += size;
            if (lines.length >= 2000 || bytes >= 2 * 1024 * 1024) await flush();
        },
        flush,
    };
}

async function* packedEntries(file, signal, start = 0) {
    const cursor = start === 0 ? { frame: 0, row: 0 } : start;
    if (!cursor || !Number.isSafeInteger(cursor.frame) || cursor.frame < 0 || !Number.isSafeInteger(cursor.row) || cursor.row < 0) throw new Error('Posição do cache inválida.');
    const handle = await fs.promises.open(file, 'r');
    let position = cursor.frame, skip = cursor.row;
    function cancelled() { if (signal?.aborted) throw Object.assign(new Error('Execução cancelada.'), { code: 'FLOW_CANCELLED' }); }
    async function readExactly(buffer, offset, allowEof = false) {
        let received = 0;
        while (received < buffer.length) {
            cancelled();
            const { bytesRead } = await handle.read(buffer, received, buffer.length - received, offset + received);
            if (!bytesRead) {
                if (allowEof && !received) return false;
                throw new Error('O arquivo do cache está incompleto.');
            }
            received += bytesRead;
        }
        return true;
    }
    try {
        while (true) {
            cancelled();
            const header = Buffer.alloc(8);
            if (!await readExactly(header, position, true)) { if (skip) throw new Error('Posição do cache inválida.'); break; }
            const size = header.readUInt32LE(0), rawSize = header.readUInt32LE(4);
            if (!size || size > MAX_PACKED_BYTES || !rawSize || rawSize > MAX_RAW_BYTES) throw new Error('Bloco do cache inválido.');
            const buffer = Buffer.alloc(size);
            await readExactly(buffer, position + 8);
            const raw = await decompress(buffer, { maxOutputLength: MAX_RAW_BYTES });
            if (raw.length !== rawSize || raw[raw.length - 1] !== 10) throw new Error('Bloco do cache inválido.');
            const lines = raw.toString('utf8').slice(0, -1).split('\n');
            if (skip > lines.length) throw new Error('Posição do cache inválida.');
            const next = position + 8 + size;
            for (let index = skip; index < lines.length; index++) {
                cancelled();
                yield { row: JSON.parse(lines[index]), offset: index + 1 === lines.length ? { frame: next, row: 0 } : { frame: position, row: index + 1 } };
            }
            position = next; skip = 0;
        }
    } finally { await handle.close(); }
}

module.exports = { createPackedWriter, packedEntries, writeData, writeBuffered };
