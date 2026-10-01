const fs = require('node:fs');
const path = require('node:path');

// Locations only: never return the matched credentials to logs or callers.
function credentialLocations(content, secrets = []) {
    const locations = [];
    content.split('\n').forEach((line, index) => {
        let detected = secrets.some(secret => line.includes(secret))
            || /\bnpg_[A-Za-z0-9]{8,}\b|-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----/.test(line);
        for (const match of line.matchAll(/postgres(?:ql)?:\/\/[^\s'"`<>]+/gi)) {
            try {
                const url = new URL(match[0]);
                if (url.hostname.endsWith('.neon.tech') && url.username && url.password) detected = true;
            } catch { /* Incomplete examples are not connection credentials. */ }
        }
        if (detected) locations.push(index + 1);
    });
    return locations;
}

function localSecrets(root) {
    const secrets = new Set();
    const envPath = path.join(root, '.env');
    if (fs.existsSync(envPath)) {
        for (const [key, value] of Object.entries(require('dotenv').parse(fs.readFileSync(envPath)))) {
            if (/PASS|SECRET|TOKEN|API_KEY/i.test(key) && value.length >= 8) secrets.add(value);
            try {
                const url = new URL(value);
                if (/^postgres(?:ql)?:$/.test(url.protocol) && url.password) {
                    secrets.add(value);
                    if (url.password.length >= 8) secrets.add(url.password);
                    const decoded = decodeURIComponent(url.password);
                    if (decoded.length >= 8) secrets.add(decoded);
                }
            } catch { /* Ordinary environment values are not URLs. */ }
        }
    }
    return [...secrets];
}

module.exports = { credentialLocations, localSecrets };
