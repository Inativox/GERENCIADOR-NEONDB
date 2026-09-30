/** Logs textuais com fila circular e uma atualização de DOM por frame. */
export function createBoundedLog(element, {
    maxLines = 1000,
    placeholder = '',
    prefix = '> ',
    schedule = callback => requestAnimationFrame(callback),
} = {}) {
    const pending = new Array(maxLines);
    let start = 0;
    let size = 0;
    let scheduled = false;

    function flush() {
        scheduled = false;
        if (placeholder && element.textContent.trim() === placeholder) element.textContent = '';
        const fragment = element.ownerDocument.createDocumentFragment();
        for (let i = 0; i < size; i++) {
            const index = (start + i) % maxLines;
            const paragraph = element.ownerDocument.createElement('p');
            paragraph.textContent = prefix + pending[index];
            fragment.appendChild(paragraph);
            pending[index] = undefined;
        }
        size = 0;
        start = 0;
        element.appendChild(fragment);
        while (element.childElementCount > maxLines) element.removeChild(element.firstElementChild);
        element.scrollTop = element.scrollHeight;
    }

    return message => {
        // Limita também mensagens isoladas e logs recebidos enquanto a janela
        // está minimizada (requestAnimationFrame pode ficar suspenso).
        const lines = String(message ?? '').slice(0, 32000).split('\n');
        for (const line of lines) {
            const index = (start + size) % maxLines;
            pending[index] = line.trim().slice(0, 4000);
            if (size < maxLines) size++;
            else start = (start + 1) % maxLines;
        }
        if (!scheduled) {
            scheduled = true;
            schedule(flush);
        }
    };
}

const logs = new WeakMap();
export function appendBoundedLog(element, message, options) {
    if (!element) return;
    if (!logs.has(element)) logs.set(element, createBoundedLog(element, options));
    logs.get(element)(message);
}
