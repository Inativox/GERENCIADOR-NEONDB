'use strict';

const waitBuffer = new Int32Array(new SharedArrayBuffer(4));
const transient = new Set(['EPERM', 'EACCES', 'EBUSY']);

// Keep electron-store's atomic replacement. A short Windows file lock must
// neither truncate the existing configuration nor escape as a raw JS dialog.
module.exports = Store => class extends Store {
    _write(value) {
        for (let attempt = 0; ; attempt++) {
            try { return super._write(value); }
            catch (error) {
                if (!transient.has(error.code)) throw error;
                if (attempt === 4) {
                    throw Object.assign(new Error('Não foi possível salvar a configuração local. O arquivo está em uso ou sem permissão de gravação. Feche outras versões do aplicativo e tente novamente.'), { code: error.code, cause: error });
                }
                Atomics.wait(waitBuffer, 0, 0, 50 * (attempt + 1));
            }
        }
    }
};
