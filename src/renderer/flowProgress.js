(function (root) {
    'use strict';
    const labels = { root: 'Preparando a raiz', generation: 'Geração Receita', enrichment: 'Enriquecimento', api: 'Consulta API', cleaning: 'Filtros finais', export: 'Exportação' };
    const number = value => Number(value).toLocaleString('pt-BR');
    function model(job) {
        const finished = ['completed', 'empty'].includes(job.status);
        const stages = ['generation', 'enrichment', ...(job.flowSnapshot?.api?.enabled ? ['api'] : []), 'cleaning', 'export'];
        const stage = job.stage === 'output' ? 'export' : job.stage;
        const counts = job.counts || {};
        let progress = job.progress?.stage === stage ? job.progress : null;
        // Also display accurate progress for history saved before progress events.
        if (!progress) {
            const processed = stage === 'cleaning' ? ['kept', 'removedRoot', 'removedCnae', 'removedBlocklist', 'repeatedDocuments', 'withoutPhones'].reduce((sum, key) => sum + (counts[key] || 0), 0)
                : stage === 'generation' ? counts.generated || 0 : stage === 'api' ? counts.apiConsulted || 0 : stage === 'export' ? counts.exported || 0 : 0;
            const total = stage === 'cleaning' ? (job.flowSnapshot?.api?.enabled ? counts.apiAvailable : counts.generated) : stage === 'enrichment' || stage === 'api' ? counts.generated : stage === 'export' ? counts.cleaned : null;
            progress = { processed, total: total ?? null, complete: finished };
        }
        const processed = progress.processed || 0;
        const total = progress.total;
        const percent = finished || progress.complete ? 100 : total > 0 ? Math.min(100, Math.max(0, processed / total * 100)) : null;
        const paused = ['failed', 'cancelled', 'interrupted'].includes(job.status);
        return { stages, stage, label: finished ? 'Concluído' : labels[stage] || 'Preparando execução', processed, total, percent,
            detail: total != null ? `${number(processed)} de ${number(total)} registros` : `${number(processed)} registros${stage === 'generation' ? ' gerados' : ' processados'}`,
            activity: paused ? 'Execução pausada' : stage === 'generation' ? 'Buscando registros' : 'Processando',
            paused, finished };
    }
    function render(job, jsx, jsxs) {
        const data = model(job);
        return jsxs('section', { className: 'flow-progress', 'aria-label': 'Progresso da execução', children: [
            jsx('ol', { className: 'flow-progress-stages', children: data.stages.map((stage, index) => jsx('li', {
                className: data.finished || index < data.stages.indexOf(data.stage) ? 'is-complete' : stage === data.stage ? 'is-current' : '',
                'aria-current': stage === data.stage ? 'step' : undefined, children: labels[stage],
            }, stage)) }),
            jsxs('div', { className: 'flow-progress-heading', children: [jsx('strong', { children: data.label }), jsx('span', { children: data.percent == null ? data.activity : `${data.percent.toLocaleString('pt-BR', { maximumFractionDigits: 1 })}%` })] }),
            data.percent == null
                ? jsx('div', { className: `flow-progress-activity${data.paused ? ' is-paused' : ''}`, role: 'progressbar', 'aria-label': `Progresso de ${data.label}`, 'aria-valuetext': `${data.activity}. ${data.detail}. Total desconhecido.`, children: jsx('span', { 'aria-hidden': true }) })
                : jsx('progress', { max: 100, value: data.percent, 'aria-label': `Progresso de ${data.label}` }),
            jsx('p', { className: 'flow-progress-detail', children: `${data.detail}${data.paused ? ' · execução pausada' : ''}` }),
            data.percent == null && data.stage === 'generation' ? jsx('p', { className: 'flow-progress-detail', children: 'O total será conhecido ao terminar a geração. A contagem avança a cada lote salvo.' }) : null,
            jsx('small', { children: 'A retomada preserva os lotes salvos.' }),
        ] });
    }
    if (typeof module === 'object' && module.exports) module.exports = { model, render };
    else root.renderFlowProgress = render;
})(typeof window === 'object' ? window : globalThis);
