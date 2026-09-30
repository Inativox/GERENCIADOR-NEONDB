        /* ── AUTO-UPDATE OVERLAY ── */
        (function () {
            const overlay  = document.getElementById('update-overlay');
            const titleEl  = document.getElementById('upd-title');
            const subEl    = document.getElementById('upd-sub');
            const barEl    = document.getElementById('upd-bar');

            if (!window.electronAPI) return;

            window.electronAPI.onUpdateDownloading(({ version }) => {
                titleEl.textContent = `Baixando atualização v${version}...`;
                subEl.textContent   = 'Você pode continuar trabalhando';
                barEl.style.width   = '0%';
                overlay.classList.add('visible');
            });

            window.electronAPI.onUpdateProgress(({ percent }) => {
                barEl.style.width = percent + '%';
                subEl.textContent = `${percent}% concluído`;
            });

            window.electronAPI.onUpdateReady(({ version }) => {
                titleEl.textContent = 'Atualização pronta';
                subEl.textContent   = `v${version} será instalada ao fechar o aplicativo`;
                barEl.style.width   = '100%';
                overlay.classList.add('visible');
                setTimeout(() => overlay.classList.remove('visible'), 8000);
            });
        })();
