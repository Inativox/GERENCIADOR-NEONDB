import { useEffect, useId, useRef, useState } from 'react';
import type { FlowAPI, ReceitaOption, ReceitaOptionsInput } from './flowTypes';

export function ReceitaSelect({ label, field, values, uf = [], cidade = [], disabled, api, onChange }: {
    label: string; field: ReceitaOptionsInput['field']; values: string[]; uf?: string[]; cidade?: string[];
    disabled?: boolean; api: FlowAPI; onChange: (values: string[]) => void;
}) {
    const id = useId(), root = useRef<HTMLDivElement>(null), searchInput = useRef<HTMLInputElement>(null);
    const [open, setOpen] = useState(false), [search, setSearch] = useState(''), [options, setOptions] = useState<ReceitaOption[]>([]);
    const [labels, setLabels] = useState<Record<string, string>>({});
    const [offset, setOffset] = useState(0), [hasMore, setHasMore] = useState(false), [loading, setLoading] = useState(false), [error, setError] = useState(''), [retry, setRetry] = useState(0);
    const filtersKey = JSON.stringify({ uf: field === 'cidade' || field === 'bairro' ? uf : [], cidade: field === 'bairro' ? cidade : [] });
    const sequence = useRef(0);
    useEffect(() => { setOffset(0); setOptions([]); sequence.current++; }, [search, filtersKey]);
    useEffect(() => {
        if (!open || disabled) return;
        const requestId = ++sequence.current;
        let active = true;
        setLoading(true); setError('');
        const timer = window.setTimeout(() => {
            Promise.resolve().then(() => {
                if (typeof api.flowsReceitaOptions !== 'function') throw new Error('Reabra o aplicativo para carregar os novos filtros da Receita.');
                return api.flowsReceitaOptions({ field, search, offset, ...JSON.parse(filtersKey) });
            }).then(result => {
                if (!active || requestId !== sequence.current) return;
                if (!result.success) throw new Error(result.message || 'Não foi possível carregar as opções.');
                const next = result.options || [];
                setOptions(previous => [...new Map([...(offset ? previous : []), ...next].map(option => [option.value, option])).values()]);
                setLabels(previous => ({ ...previous, ...Object.fromEntries(next.map(option => [option.value, option.label])) }));
                setHasMore(Boolean(result.hasMore));
                if (result.message) setError(result.message);
            }).catch(reason => { if (active && requestId === sequence.current) setError(reason instanceof Error ? reason.message : 'Não foi possível carregar as opções.'); })
                .finally(() => { if (active && requestId === sequence.current) setLoading(false); });
        }, search ? 450 : 0);
        return () => { active = false; window.clearTimeout(timer); };
    }, [open, disabled, api, field, search, offset, filtersKey, retry]);
    useEffect(() => {
        if (!open) return;
        searchInput.current?.focus();
        const close = (event: PointerEvent) => { if (!root.current?.contains(event.target as Node)) setOpen(false); };
        document.addEventListener('pointerdown', close);
        return () => document.removeEventListener('pointerdown', close);
    }, [open]);
    function toggle(value: string) { onChange(values.includes(value) ? values.filter(item => item !== value) : [...values, value]); }
    return <div className="receita-select" ref={root} onKeyDown={event => { if (event.key === 'Escape') { setOpen(false); root.current?.querySelector<HTMLButtonElement>('.receita-select-trigger')?.focus(); event.stopPropagation(); } }}>
        <span id={`${id}-label`} className="receita-select-label">{label}</span>
        <button type="button" className="receita-select-trigger" disabled={disabled} aria-labelledby={`${id}-label ${id}-summary`} aria-expanded={open} aria-controls={`${id}-panel`} onClick={() => { setOpen(previous => !previous); setOffset(0); }}><span id={`${id}-summary`}>{values.length ? `${values.length} selecionado${values.length > 1 ? 's' : ''}` : 'Todos'}</span><span aria-hidden="true">▾</span></button>
        {values.length > 0 && <ul className="receita-selected" aria-label={`${label} selecionados`}>{values.map(value => <li key={value}><span title={labels[value] || value}>{labels[value] || value}</span><button type="button" disabled={disabled} aria-label={`Remover ${value} de ${label}`} onClick={() => toggle(value)}>×</button></li>)}<li><button type="button" disabled={disabled} onClick={() => onChange([])}>Limpar seleção</button></li></ul>}
        {open && <div className="receita-select-panel" id={`${id}-panel`} aria-labelledby={`${id}-label`}>
            <input ref={searchInput} type="search" disabled={disabled} autoComplete="off" aria-label={`Buscar em ${label}`} placeholder={field === 'cnaes' || field === 'naturezas' ? 'Buscar código ou descrição' : 'Buscar opções no banco'} value={search} onChange={event => { setOffset(0); setSearch(event.target.value); }} />
            <div className="receita-select-options" role="group" aria-label={`Opções de ${label}`} aria-busy={loading}>
                {options.map(option => <label key={option.value}><input type="checkbox" disabled={disabled} checked={values.includes(option.value)} onChange={() => toggle(option.value)} /><span>{option.label}</span></label>)}
                {loading && <p role="status">Consultando Receita…</p>}
                {error && <p role="status">{error}</p>}
                {!loading && !error && !options.length && <p>Nenhuma opção encontrada.</p>}
            </div>
            <div className="receita-select-footer">{error && <button type="button" onClick={() => setRetry(previous => previous + 1)}>Tentar novamente</button>}{hasMore && !error && <button type="button" disabled={loading} onClick={() => setOffset(previous => previous + 50)}>Carregar mais</button>}<button type="button" onClick={() => setOpen(false)}>Concluir seleção</button></div>
        </div>}
    </div>;
}
