        /* ── AUTO-UPDATE OVERLAY ── */
        (function () {
            const overlay  = document.getElementById('update-overlay');
            const titleEl  = document.getElementById('upd-title');
            const subEl    = document.getElementById('upd-sub');
            const barEl    = document.getElementById('upd-bar');

            if (!window.electronAPI) return;

            window.electronAPI.onUpdateDownloading(({ version }) => {
                titleEl.textContent = `Baixando atualização v${version}...`;
                subEl.textContent   = 'Por favor, aguarde';
                barEl.style.width   = '0%';
                overlay.classList.add('visible');
            });

            window.electronAPI.onUpdateProgress(({ percent }) => {
                barEl.style.width = percent + '%';
                subEl.textContent = `${percent}% concluído`;
            });

            window.electronAPI.onUpdateReady(({ version }) => {
                titleEl.textContent = 'Reiniciando para atualizar';
                subEl.textContent   = `v${version} instalada — reiniciando agora...`;
                barEl.style.width   = '100%';
            });
        })();

        /* ── TAB FX: TEXT SCRAMBLE + HUD RAILS ── */
        (function () {
            /* --- Text scramble on section headers --- */
            const CHARS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!#$%&@~:/\\';

            function scramble(el) {
                const orig = el.textContent;
                if (!orig.trim() || orig.length > 50) return;
                let f = 0;
                const FRAMES = 16;
                const tick = () => {
                    const pct = f / FRAMES;
                    el.textContent = [...orig].map((ch, i) => {
                        if (ch === ' ' || ch === '\n') return ch;
                        if (i / orig.length < pct) return ch;
                        return CHARS[Math.floor(Math.random() * CHARS.length)];
                    }).join('');
                    if (++f <= FRAMES) requestAnimationFrame(tick);
                    else el.textContent = orig;
                };
                requestAnimationFrame(tick);
            }

            document.addEventListener('tab-changed', ({ detail: { tabId } }) => {
                const page = document.getElementById(tabId);
                if (!page) return;
                page.querySelectorAll('.section-header h3').forEach((el, i) => {
                    setTimeout(() => scramble(el), 50 + i * 90);
                });
            });

            /* --- HUD ambient light rails --- */
            const sidebar = document.querySelector('.sidebar');
            const leftPos = sidebar ? (sidebar.offsetWidth + 'px') : '220px';

            [
                { cls: 'hud-rail--left',  left: leftPos,  dur: '8s',  delay: '0s'   },
                { cls: 'hud-rail--right', right: '0px',   dur: '13s', delay: '-6s'  },
            ].forEach(({ cls, left, right, dur, delay }) => {
                const el = document.createElement('div');
                el.className = `hud-rail ${cls}`;
                if (left)  el.style.left  = left;
                if (right) el.style.right = right;
                el.style.setProperty('--rail-dur',   dur);
                el.style.setProperty('--rail-delay', delay);
                document.body.appendChild(el);
            });
        })();
