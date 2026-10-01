'use strict';

const fs = require('fs');
const path = require('path');
const { createHash } = require('crypto');

const HISTORY_VERSION = 2;
const HISTORY_LIMIT = 200;
const UI_LOG_BYTES = 64 * 1024;
const clone = value => value === undefined ? undefined : JSON.parse(JSON.stringify(value));
const hash = value => createHash('sha256').update(value).digest('hex');
const invalidHistory = () => new Error('Histórico de fluxos inválido. Preserve o arquivo e restaure seu backup.');

function validJobIdentity(job) {
    return job && typeof job.id === 'string' && job.id.length > 0 && typeof job.owner === 'string' && job.owner.length > 0;
}
function indexEntry(job) {
    const entry = { id: job.id, owner: job.owner };
    if (typeof job.createdAt === 'string') entry.createdAt = job.createdAt;
    return entry;
}
function atomicWrite(filename, content) {
    const temporary = filename + '.tmp';
    try {
        fs.writeFileSync(temporary, content, { mode: 0o600 });
        fs.renameSync(temporary, filename);
    } catch (error) {
        // Never remove a directory used to simulate or report an unavailable disk.
        try { if (fs.statSync(temporary).isFile()) fs.unlinkSync(temporary); } catch { /* Best effort cleanup. */ }
        throw error;
    }
}

function boundedUiLogs(job) {
    if (!Array.isArray(job.logs)) return job;
    const originalCount = job.logs.length;
    const retained = [];
    let available = UI_LOG_BYTES, truncated = false;
    const logs = job.logs.slice(-300);
    for (let index = logs.length - 1; index >= 0 && available > 0; index--) {
        const buffer = Buffer.from(String(logs[index]), 'utf8');
        if (buffer.length > available) {
            let start = buffer.length - available;
            while (start < buffer.length && (buffer[start] & 0xc0) === 0x80) start++;
            retained.push(buffer.subarray(start).toString('utf8'));
            available = 0;
            truncated = true;
        } else {
            retained.push(buffer.toString('utf8'));
            available -= buffer.length;
        }
    }
    job.logs = retained.reverse();
    if (truncated || retained.length < originalCount) job.logsTruncated = true;
    return job;
}

function createUserStore(baseDirectory, username) {
    if (typeof username !== 'string' || !username) throw new Error('Usuário inválido para o histórico de fluxos.');
    const userDirectory = path.join(baseDirectory, hash(username));
    const file = path.join(userDirectory, 'history.json');
    const recordsDirectory = path.join(userDirectory, 'job-records');
    const recordFile = id => path.join(recordsDirectory, hash(id) + '.json');
    fs.mkdirSync(recordsDirectory, { recursive: true });
    let data = { version: HISTORY_VERSION, flows: [], jobs: [], layouts: [] };
    if (fs.existsSync(file)) {
        try {
            data = JSON.parse(fs.readFileSync(file, 'utf8'));
            if (![1, HISTORY_VERSION].includes(data.version) || !Array.isArray(data.flows) || !Array.isArray(data.jobs) || data.jobs.some(job => !validJobIdentity(job)) || new Set(data.jobs.map(job => job.id)).size !== data.jobs.length) throw invalidHistory();
            if (data.layouts !== undefined && (!Array.isArray(data.layouts) || data.layouts.length > 100 || data.layouts.some(layout => !layout || typeof layout.id !== 'string' || !layout.id.startsWith('custom-') || !Array.isArray(layout.colunas)) || new Set(data.layouts.map(layout => layout.id)).size !== data.layouts.length)) throw invalidHistory();
        } catch { throw invalidHistory(); }
        if (data.version === 1) {
            // Keep the original recoverable until every record and the new index
            // have been committed. Retrying an interrupted migration is safe.
            const backup = path.join(userDirectory, 'history.v1.backup.json');
            if (!fs.existsSync(backup)) fs.copyFileSync(file, backup, fs.constants.COPYFILE_EXCL);
            const migrated = { version: HISTORY_VERSION, flows: data.flows, jobs: data.jobs.map(indexEntry) };
            for (const job of data.jobs) atomicWrite(recordFile(job.id), JSON.stringify(job));
            atomicWrite(file, JSON.stringify(migrated));
            data = migrated;
        }
    }
    function persistIndex(candidate) {
        atomicWrite(file, JSON.stringify(candidate));
        data = candidate;
    }
    function readJob(entry) {
        let job;
        try { job = JSON.parse(fs.readFileSync(recordFile(entry.id), 'utf8')); }
        catch { throw new Error('Registro de execução indisponível ou inválido. Preserve o histórico e restaure seu backup.'); }
        if (!validJobIdentity(job) || job.id !== entry.id || job.owner !== entry.owner) throw invalidHistory();
        return job;
    }
    return {
        directory: userDirectory,
        listFlows: () => clone(data.flows),
        listLayouts: () => clone(data.layouts || []),
        getLayout: id => clone((data.layouts || []).find(layout => layout.id === id)),
        saveLayout(layout) {
            const stored = clone(layout), layouts = (data.layouts || []).slice();
            const index = layouts.findIndex(item => item.id === stored.id);
            if (index < 0) {
                if (layouts.length >= 100) throw new Error('Limite de 100 layouts próprios por usuário.');
                layouts.push(stored);
            } else layouts[index] = stored;
            persistIndex({ ...data, layouts });
            return clone(stored);
        },
        deleteLayout(id) { persistIndex({ ...data, layouts: (data.layouts || []).filter(layout => layout.id !== id) }); },
        listJobs() {
            // Read one record at a time; retain only bounded UI logs. Full records
            // remain on disk and getJob supplies them for ownership/resumption.
            return data.jobs.filter(entry => entry.owner === username).slice(-HISTORY_LIMIT).reverse().map(entry => boundedUiLogs(readJob(entry)));
        },
        getFlow: id => clone(data.flows.find(flow => flow.id === id)),
        saveFlow(flow) {
            const stored = clone(flow);
            const flows = data.flows.slice();
            const index = flows.findIndex(item => item.id === stored.id);
            if (index < 0) {
                if (flows.length >= 100) throw new Error('Limite de 100 fluxos por usuário.');
                flows.push(stored);
            } else flows[index] = stored;
            persistIndex({ ...data, flows });
            return clone(stored);
        },
        deleteFlow(id) { persistIndex({ ...data, flows: data.flows.filter(flow => flow.id !== id) }); },
        getJob(id) {
            const entry = data.jobs.find(job => job.id === id && job.owner === username);
            return entry ? readJob(entry) : undefined;
        },
        saveJob(job) {
            if (!validJobIdentity(job)) throw new Error('Registro de execução inválido.');
            if (job.owner !== username) throw new Error('Execução pertence a outro usuário.');
            const existing = data.jobs.find(entry => entry.id === job.id);
            if (existing && existing.owner !== username) throw new Error('Execução pertence a outro usuário.');
            const filename = recordFile(job.id);
            const serialized = JSON.stringify(job);
            atomicWrite(filename, serialized);
            if (!existing) {
                try { persistIndex({ ...data, jobs: [...data.jobs, indexEntry(job)] }); }
                catch (error) {
                    // The unindexed new record must never appear as a saved job.
                    try { fs.unlinkSync(filename); } catch { /* An orphan is ignored on next load. */ }
                    throw error;
                }
            }
            return JSON.parse(serialized);
        },
    };
}

module.exports = { createUserStore };
