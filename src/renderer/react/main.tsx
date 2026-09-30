import { Component } from 'react';
import type { ReactNode } from 'react';
import { createRoot } from 'react-dom/client';
import { LimpezaColunas } from './LimpezaColunas';
import './limpezaColunas.css';

class ColumnsBoundary extends Component<{ children: ReactNode }, { failed: boolean }> {
    state = { failed: false };
    static getDerivedStateFromError() { return { failed: true }; }
    render() {
        if (this.state.failed) return <div className="columns-app columns-error" role="alert">Não foi possível carregar esta aba. Reabra o aplicativo para tentar novamente.</div>;
        return this.props.children;
    }
}

function mount() {
    const element = document.getElementById('limpeza-colunas-react-root');
    if (element) createRoot(element).render(<ColumnsBoundary><LimpezaColunas /></ColumnsBoundary>);
}

if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', mount, { once: true });
else mount();
