const COPY = {
    upload: {
        title: 'O envio para a Fastway está em andamento',
        detail: 'A Fastway só confirma depois de importar o mailing, o que pode levar alguns minutos. Se fechar agora, o arquivo pode chegar sem confirmação e a execução vai aparecer como interrompida.',
    },
    mailing: {
        title: 'O envio para a Fastway está em andamento',
        detail: 'A Fastway só confirma depois de importar o mailing, o que pode levar alguns minutos. Se fechar agora, o arquivo pode chegar sem confirmação. Confira o painel antes de enviar de novo.',
    },
    flow: {
        title: 'Há um fluxo em execução',
        detail: 'Se fechar agora, a execução será interrompida. Depois, você pode retomá-la da última etapa confirmada.',
    },
};

/** Asks before closing the app while a flow runs; the main process waits for the answer. */
export function setupCloseFlowModal(api, root = document) {
    const modal = root.getElementById('close-flow-modal');
    const keep = root.getElementById('close-flow-keep');
    const confirm = root.getElementById('close-flow-confirm');
    let returnFocus = null;

    function answer(close) {
        if (modal.classList.contains('hidden')) return;
        modal.classList.add('hidden');
        modal.inert = true;
        returnFocus?.focus?.();
        api.answerCloseDuringFlow(close);
    }
    keep.addEventListener('click', () => answer(false));
    confirm.addEventListener('click', () => answer(true));
    modal.addEventListener('click', event => { if (event.target === modal) answer(false); });
    modal.addEventListener('keydown', event => {
        if (event.key === 'Escape') { event.preventDefault(); answer(false); }
        if (event.key === 'Tab') { event.preventDefault(); (root.activeElement === keep ? confirm : keep).focus(); }
    });

    api.onConfirmCloseDuringFlow(({ uploading, mailing }) => {
        const copy = mailing ? COPY.mailing : uploading ? COPY.upload : COPY.flow;
        root.getElementById('close-flow-title').textContent = copy.title;
        root.getElementById('close-flow-detail').textContent = copy.detail;
        if (modal.classList.contains('hidden')) returnFocus = root.activeElement;
        modal.classList.remove('hidden');
        modal.inert = false;
        keep.focus();
    });
}
