import { useEffect, useRef, useState } from 'react';
import type { CustomLayout, Flow, FlowAPI, FlowFormat, LayoutColumn, LayoutField, LayoutPreview } from './flowTypes';

function initialLayout(format: FlowFormat | null, asCopy: boolean): CustomLayout {
    const colunas = format?.colunas.map(column => {
        const value: LayoutColumn = typeof column === 'string' ? { header: column, campo: 'fixo_vazio' } : JSON.parse(JSON.stringify(column));
        const headerSlot = value.header.trim().match(/^(?:fone|telefone\s*)([1-9]\d*)$/i);
        const slot = headerSlot ? Number(headerSlot[1]) : ['telefone_principal', 'telefone_principal_num'].includes(value.campo) ? 1 : ['telefone_secundario', 'telefone_secundario_num'].includes(value.campo) ? 2 : 0;
        return slot ? { header: value.header, campo: `telefone_${slot}` } : value;
    }) || [{ header: 'nome', campo: 'razao_social' }, { header: 'CNPJ', campo: 'cnpj' }, { header: 'fone1', campo: 'telefone_1' }];
    const editing = Boolean(format?.custom && !asCopy);
    return { id: editing ? format!.id : '', nome: format ? editing ? format.nome : `${format.nome} · cópia` : 'Novo layout', colunas, custom: true, revision: editing ? format?.revision || 0 : 0 };
}

export function LayoutEditor({ format, asCopy, fields, flow, api, onSaved, onClose, onDeleted }: {
    format: FlowFormat | null; asCopy: boolean; fields: LayoutField[]; flow: Flow; api: FlowAPI;
    onSaved: (layout: CustomLayout) => void; onClose: () => void; onDeleted?: () => void;
}) {
    const [layout, setLayout] = useState(() => initialLayout(format, asCopy));
    const [dirty, setDirty] = useState(false);
    const [confirmClose, setConfirmClose] = useState(false);
    const [confirmDelete, setConfirmDelete] = useState(false);
    const [error, setError] = useState('');
    const [saving, setSaving] = useState(false);
    const [preview, setPreview] = useState<LayoutPreview | null>(null);
    const [previewError, setPreviewError] = useState('');
    const [previewBusy, setPreviewBusy] = useState(false);
    const mounted = useRef(true);
    const savingRef = useRef(false);
    useEffect(() => { mounted.current = true; return () => { mounted.current = false; }; }, []);
    useEffect(() => {
        let active = true;
        setPreview(null); setPreviewError(''); setPreviewBusy(true);
        const timer = window.setTimeout(() => {
            api.flowsPreviewLayout({ layout, operation: flow.operation, includeSituacao: flow.output.includeSituacao, fillCpf: flow.enrichment.enabled && flow.enrichment.fillCpf, fillLivre5: flow.cleaning.enabled && flow.cleaning.fillLivre5 })
                .then(result => { if (!active) return; if (!result.success || !result.preview) setPreviewError(result.message || 'Confira os campos do layout.'); else setPreview(result.preview); })
                .catch(() => { if (active) setPreviewError('Não foi possível atualizar a prévia.'); })
                .finally(() => { if (active) setPreviewBusy(false); });
        }, 300);
        return () => { active = false; window.clearTimeout(timer); };
    }, [layout, api, flow]);

    function update(next: CustomLayout) { setLayout(next); setDirty(true); setError(''); setConfirmClose(false); setConfirmDelete(false); }
    function change(index: number, patch: Partial<LayoutColumn>) { update({ ...layout, colunas: layout.colunas.map((column, position) => position === index ? { ...column, ...patch } : column) }); }
    function move(index: number, delta: number) { const colunas = [...layout.colunas]; [colunas[index], colunas[index + delta]] = [colunas[index + delta], colunas[index]]; update({ ...layout, colunas }); }
    const compositionFields = fields.filter(field => !['manual', 'composto'].includes(field.id) && !/^telefone/.test(field.id));
    async function save(asNew = false) {
        if (savingRef.current) return;
        savingRef.current = true; setSaving(true); setError('');
        try {
            const result = await api.flowsSaveLayout(asNew ? { ...layout, id: '', revision: 0 } : layout);
            if (!mounted.current) return;
            if (!result.success || !result.layout) throw new Error(result.message || 'Não foi possível salvar o layout.');
            onSaved(result.layout);
        } catch (reason) { if (mounted.current) setError(reason instanceof Error ? reason.message : 'Não foi possível salvar o layout.'); }
        finally { savingRef.current = false; if (mounted.current) setSaving(false); }
    }
    async function remove() {
        if (savingRef.current) return;
        savingRef.current = true; setSaving(true); setError('');
        try {
            const result = await api.flowsDeleteLayout(layout.id);
            if (!mounted.current) return;
            if (!result.success) throw new Error(result.message || 'Não foi possível excluir o layout.');
            onDeleted?.();
        } catch (reason) { if (mounted.current) setError(reason instanceof Error ? reason.message : 'Não foi possível excluir o layout.'); }
        finally { savingRef.current = false; if (mounted.current) setSaving(false); }
    }

    return <section className="layout-editor" aria-label="Editor de layout" aria-busy={saving}>
        <div className="layout-heading"><div><h3>{layout.id ? 'Editar layout próprio' : 'Criar layout próprio'}</h3><p>Defina as colunas na ordem em que devem aparecer no XLSX e CSV.</p></div><span>{layout.colunas.length}/60 colunas</span></div>
        {error && <p className="flow-error" role="alert">{error}</p>}
        <fieldset disabled={saving} className="flow-form-fields">
            <label className="flow-field"><span>Nome do layout</span><input value={layout.nome} maxLength={100} onChange={event => update({ ...layout, nome: event.target.value })} /></label>
            <p className="flow-hint">Os layouts próprios ficam na sua conta, nesta máquina. Modelos padrão são preservados. Telefones podem ter qualquer nome de coluna; selecione Telefone 1, Telefone 2, etc., sem pular posições.</p>
            <ol className="layout-columns">
                {layout.colunas.map((column, index) => <li key={index} className="layout-column">
                    <div className="layout-column-main"><span className="layout-column-number">{String(index + 1).padStart(2, '0')}</span>
                        <label className="flow-field"><span>Nome da coluna {index + 1}</span><input value={column.header} maxLength={80} onChange={event => change(index, { header: event.target.value })} /></label>
                        <label className="flow-field"><span>Origem da coluna {index + 1}</span><select value={column.campo} onChange={event => change(index, { campo: event.target.value, ...(event.target.value === 'composto' ? { partes: column.partes || ['estado', 'cidade'], sep: column.sep ?? ' - ' } : {}) })}>{fields.map(field => <option key={field.id} value={field.id}>{field.label}</option>)}</select></label>
                        <div className="layout-column-actions"><button type="button" disabled={index === 0} aria-label={`Subir coluna ${index + 1}`} title="Subir coluna" onClick={() => move(index, -1)}>↑</button><button type="button" disabled={index === layout.colunas.length - 1} aria-label={`Descer coluna ${index + 1}`} title="Descer coluna" onClick={() => move(index, 1)}>↓</button><button type="button" className="flow-delete" aria-label={`Remover coluna ${index + 1}`} onClick={() => update({ ...layout, colunas: layout.colunas.filter((_, position) => position !== index) })}>Remover</button></div>
                    </div>
                    {column.campo === 'manual' && <label className="flow-field layout-column-extra"><span>Texto fixo da coluna {index + 1}</span><input value={column.valor_manual || ''} maxLength={1000} onChange={event => change(index, { valor_manual: event.target.value })} placeholder="Deixe vazio para uma coluna sem valor" /></label>}
                    {column.campo === 'composto' && <div className="layout-column-extra"><p className="flow-hint">Campos combinados, na ordem abaixo. Valores ausentes são ignorados.</p><div className="layout-composition">{(column.partes || []).map((part, position) => <div key={position}><label className="flow-field"><span>Campo {position + 1} da coluna {index + 1}</span><select value={part} onChange={event => change(index, { partes: column.partes!.map((value, n) => n === position ? event.target.value : value) })}>{compositionFields.map(field => <option key={field.id} value={field.id}>{field.label}</option>)}</select></label><button type="button" disabled={column.partes!.length <= 2} aria-label={`Remover campo ${position + 1} da coluna ${index + 1}`} onClick={() => change(index, { partes: column.partes!.filter((_, n) => n !== position) })}>×</button></div>)}<button type="button" disabled={(column.partes?.length || 0) >= 8} onClick={() => change(index, { partes: [...(column.partes || []), 'cidade'] })}>Adicionar campo</button><label className="flow-field"><span>Separador da coluna {index + 1}</span><input value={column.sep ?? ' - '} maxLength={20} onChange={event => change(index, { sep: event.target.value })} /></label></div></div>}
                </li>)}
            </ol>
            <button type="button" disabled={layout.colunas.length >= 60} onClick={() => { let header = `coluna${layout.colunas.length + 1}`; while (layout.colunas.some(column => column.header === header)) header += '_'; update({ ...layout, colunas: [...layout.colunas, { header, campo: 'fixo_vazio' }] }); }}>+ Adicionar coluna</button>
        </fieldset>
        <div className="layout-preview" aria-busy={previewBusy}><h4>Prévia da exportação</h4><p className="flow-hint">Registro fictício. A prévia considera a situação cadastral, CPF e LIVRE5 escolhidos no fluxo.</p>{previewBusy ? <p className="flow-hint" role="status">Atualizando prévia…</p> : previewError ? <p className="flow-error" role="status">{previewError}</p> : preview && <div className="layout-preview-scroll" tabIndex={0} aria-label="Prévia das colunas exportadas"><table><thead><tr>{preview.headers.map((header, index) => <th key={index}>{header}</th>)}</tr></thead><tbody><tr>{preview.values.map((value, index) => <td key={index}>{value || '—'}</td>)}</tr></tbody></table></div>}</div>
        {confirmClose && <div className="layout-discard" role="alert"><p>Descartar as alterações deste layout?</p><button type="button" onClick={onClose}>Descartar e fechar</button><button type="button" onClick={() => setConfirmClose(false)}>Continuar editando layout</button></div>}
        {confirmDelete && <div className="layout-discard" role="alert"><p>Excluir o layout “{format?.nome}”? A exclusão só é permitida quando nenhum fluxo salvo usa este layout. O histórico será preservado.</p><button type="button" className="flow-delete" disabled={saving} onClick={() => void remove()}>Confirmar exclusão do layout</button><button type="button" disabled={saving} onClick={() => setConfirmDelete(false)}>Manter layout</button></div>}
        <footer className="layout-footer"><button type="button" className="flow-primary" disabled={saving} onClick={() => void save()}>{saving ? 'Salvando layout…' : layout.id ? 'Salvar alterações do layout' : 'Salvar novo layout'}</button>{layout.id && <><button type="button" disabled={saving} onClick={() => void save(true)}>Salvar como novo layout</button><button type="button" className="flow-delete" disabled={saving} onClick={() => { setConfirmClose(false); setConfirmDelete(true); }}>Excluir layout próprio</button></>}<button type="button" disabled={saving} onClick={() => dirty ? setConfirmClose(true) : onClose()}>Fechar editor</button><p className="flow-hint">Alterações valem para próximas execuções. O histórico mantém o layout original.</p></footer>
    </section>;
}
