// Flow pools have at most two connections and need session-level read-only settings.
// Neon transaction pooling rejects these startup options, so use the corresponding
// direct endpoint of the same database. Keep the credentials and TLS settings intact.
function readOnlyPoolOptions(connection, { max = 2, timeout = 60000, connectionTimeout = 15000 } = {}) {
    const source = typeof connection === 'string' ? { connectionString: connection } : { ...connection };
    let sessionOptions = source.options || '';
    if (source.connectionString) {
        try {
            const url = new URL(source.connectionString);
            if (url.hostname.endsWith('.neon.tech')) url.hostname = url.hostname.replace('-pooler.', '.');
            sessionOptions = url.searchParams.get('options') || sessionOptions;
            // pg gives URL parameters precedence over constructor options.
            url.searchParams.delete('options');
            url.searchParams.delete('statement_timeout');
            if (timeout === 0) url.searchParams.delete('query_timeout');
            source.connectionString = url.toString();
        } catch { /* pg reports an invalid connection; never expose its URL. */ }
    }
    return { ...source, max, connectionTimeoutMillis: connectionTimeout, idleTimeoutMillis: 10000,
        ...(timeout === 0 ? { query_timeout: 0 } : {}),
        statement_timeout: timeout, options: `${sessionOptions} -c default_transaction_read_only=on -c statement_timeout=${timeout}`.trim() };
}
module.exports = { readOnlyPoolOptions };
