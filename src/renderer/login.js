        // Particle animation script (unchanged)
        document.addEventListener('DOMContentLoaded', () => {
            const bgAnimation = document.querySelector('.bg-animation');
            if (bgAnimation) {
                const particleCount = 50;
                for (let i = 0; i < particleCount; i++) {
                    const particle = document.createElement('div');
                    particle.className = 'particle';
                    particle.style.left = `${Math.random() * 100}%`;
                    particle.style.top = `${Math.random() * 100}%`;
                    particle.style.animationDelay = `${Math.random() * 6}s`;
                    particle.style.animationDuration = `${Math.random() * 4 + 4}s`;
                    bgAnimation.appendChild(particle);
                }
            }
        });

        // --- Element Selectors ---
        const loginForm = document.getElementById('login-form');
        const usernameInput = document.getElementById('username');
        const passwordInput = document.getElementById('password');
        const rememberMeCheckbox = document.getElementById('remember-me');
        const loginBtn = document.getElementById('login-btn');
        const spinner = document.getElementById('spinner');
        const buttonText = document.getElementById('button-text');
        const messageDiv = document.getElementById('message');
        const logoutLink = document.getElementById('logout-link');
        const dbConnectionStringInput = document.getElementById('db-connection-string');
        const testDbBtn = document.getElementById('test-db-btn');
        const dbStatusMessage = document.getElementById('db-status-message');
        const receitaConnectionStringInput = document.getElementById('receita-connection-string');
        const testReceitaBtn = document.getElementById('test-receita-btn');
        const receitaStatusMessage = document.getElementById('receita-status-message');
        const databaseSettings = document.getElementById('database-settings');
        const databaseSummary = document.getElementById('database-status-summary');
        let databaseAccess = { neonConfigured: false, receitaConfigured: false };
        const updateDatabaseSummary = () => {
            databaseSummary.textContent = `Neon: ${databaseAccess.neonConfigured ? 'salvo' : 'pendente'} · Receita: ${databaseAccess.receitaConfigured ? 'salva' : 'pendente'}`;
        };
        let pendingDatabaseTests = 0;
        let loginInProgress = false;
        const importKeyBtn = document.getElementById('import-key-btn');
        const keyFileBadge = document.getElementById('key-file-badge');
        const keyFileStatusText = document.getElementById('key-file-status-text');
        const importAccessBtn = document.getElementById('import-access-btn');
        const accessStatus = document.getElementById('access-status');
        window.electronAPI.getAccessStatus().then(status => {
            accessStatus.textContent = status.configured ? 'Acesso configurado neste computador.' : (status.message || 'Importe o arquivo de acesso fornecido pela empresa.');
        }).catch(() => { accessStatus.textContent = 'Não foi possível verificar o acesso local.'; });
        importAccessBtn.addEventListener('click', async () => {
            importAccessBtn.disabled = true;
            try {
                const result = await window.electronAPI.importPrivateAccess();
                if (!result.cancelled) {
                    accessStatus.textContent = result.message;
                    if (result.success) {
                        const status = await window.electronAPI.getKeyFileStatus();
                        setKeyFileStatus(status.loaded);
                    }
                }
            } catch { accessStatus.textContent = 'Não foi possível importar o acesso.'; }
            finally { importAccessBtn.disabled = false; }
        });

        // --- Helper Functions ---
        const showMessage = (text, type = 'error') => {
            messageDiv.textContent = text;
            messageDiv.className = type === 'success' ? 'success-message' : 'error-message';
        };

        const setKeyFileStatus = (loaded) => {
            if (loaded) {
                keyFileBadge.classList.add('key-file-loaded');
                keyFileStatusText.textContent = 'Licença de API carregada';
            } else {
                keyFileBadge.classList.remove('key-file-loaded');
                keyFileStatusText.textContent = 'Nenhuma licença importada';
            }
        };

        const setDatabaseStatus = (element, text, type = 'info') => {
            element.textContent = text;
            element.className = 'db-status-message';
            element.style.color = type === 'success' ? 'var(--accent-green)' : type === 'error' ? 'var(--accent-red)' : 'var(--text-secondary)';
        };
        const setDbStatus = (text, type) => setDatabaseStatus(dbStatusMessage, text, type);
        const setReceitaStatus = (text, type) => setDatabaseStatus(receitaStatusMessage, text, type);

        const setLoadingState = (loading) => {
            loginInProgress = loading;
            checkFormValidity();
            spinner.style.display = loading ? 'inline-block' : 'none';
            buttonText.textContent = loading ? 'Entrando...' : 'Entrar';
        };

        const checkFormValidity = () => {
            const user = usernameInput.value.trim();
            const pass = passwordInput.value.trim();
            loginBtn.disabled = !user || !pass || pendingDatabaseTests > 0 || loginInProgress;
        };

        // --- Event Handlers ---
        const testDatabase = async (input, button, setStatus, save, label) => {
            const connectionString = input.value.trim();
            if (!connectionString) {
                setStatus('Cole a conexão para salvar e testar.', 'error');
                return;
            }
            pendingDatabaseTests++;
            checkFormValidity();
            input.disabled = true;
            button.disabled = true;
            button.textContent = 'Testando…';
            setStatus(`Verificando ${label}…`);

            try {
                const result = await save(connectionString);
                if (result.success) {
                    databaseAccess[button === testDbBtn ? 'neonConfigured' : 'receitaConfigured'] = true;
                    updateDatabaseSummary();
                    input.value = '';
                    input.placeholder = 'Conexão salva. Cole outra para substituir';
                    setStatus(`${label} configurado neste computador.`, 'success');
                    input.style.borderColor = 'var(--accent-green)';
                } else {
                    throw new Error(result.message);
                }
            } catch (error) {
                setStatus(error.message || 'Não foi possível salvar o acesso. Tente novamente.', 'error');
                input.style.borderColor = 'var(--accent-red)';
            } finally {
                pendingDatabaseTests--;
                input.disabled = false;
                button.disabled = false;
                button.textContent = 'Salvar e testar';
                checkFormValidity();
            }
        };
        const handleTestDbConnection = () => testDatabase(dbConnectionStringInput, testDbBtn, setDbStatus, uri => window.electronAPI.saveAndTestDbConnection(uri), 'Neon');
        const handleTestReceitaConnection = () => testDatabase(receitaConnectionStringInput, testReceitaBtn, setReceitaStatus, uri => window.electronAPI.saveAndTestReceitaConnection(uri), 'Banco da Receita');

        const handleLogin = async (event) => {
            event.preventDefault();
            const username = usernameInput.value.trim();
            const password = passwordInput.value.trim();
            const rememberMe = rememberMeCheckbox.checked;

            if (!username || !password) {
                showMessage('Preencha usuário e senha.');
                return;
            }
            if (pendingDatabaseTests || dbConnectionStringInput.value.trim() || receitaConnectionStringInput.value.trim()) {
                databaseSettings.open = true;
                showMessage('Use “Salvar e testar” para confirmar os acessos preenchidos antes de entrar.');
                return;
            }

            setLoadingState(true);
            showMessage('', 'info'); // Clear previous messages

            try {
                const result = await window.electronAPI.loginAttempt(username, password, rememberMe);
                if (result.success) {
                    showMessage('Login realizado com sucesso!', 'success');
                    // Main window will be opened by main.js
                } else {
                    showMessage(result.message || 'Credenciais inválidas.');
                    setLoadingState(false);
                }
            } catch (error) {
                showMessage('Erro de conexão com o sistema. Tente novamente.');
                setLoadingState(false);
            }
        };

        const handleImportKeyFile = async () => {
            importKeyBtn.disabled = true;
            importKeyBtn.textContent = 'Importando...';
            try {
                const result = await window.electronAPI.selectAndLoadKeyFile();
                if (result.cancelled) return;
                if (result.success) {
                    setKeyFileStatus(true);
                } else {
                    showMessage(result.message || 'Falha ao carregar licença.');
                }
            } finally {
                importKeyBtn.disabled = false;
                importKeyBtn.textContent = 'Importar';
            }
        };

        // --- Event Listeners ---
        loginForm.addEventListener('submit', handleLogin);
        testDbBtn.addEventListener('click', handleTestDbConnection);
        testReceitaBtn.addEventListener('click', handleTestReceitaConnection);
        importKeyBtn.addEventListener('click', handleImportKeyFile);

        dbConnectionStringInput.addEventListener('input', () => {
            dbConnectionStringInput.style.borderColor = 'var(--border-color)';
            setDbStatus('');
        });
        receitaConnectionStringInput.addEventListener('input', () => {
            receitaConnectionStringInput.style.borderColor = 'var(--border-color)';
            setReceitaStatus('');
        });

        [usernameInput, passwordInput].forEach(input => {
            input.addEventListener('input', () => {
                checkFormValidity();
                if (messageDiv.textContent) showMessage('');
            });
        });

        logoutLink.addEventListener('click', () => {
            window.electronAPI?.logout();
        });

        // --- Initialization ---
        document.addEventListener('DOMContentLoaded', async () => {
            try {
                const status = await window.electronAPI.getLoginDatabaseStatus();
                if (!status.success) throw new Error();
                databaseAccess = status;
                updateDatabaseSummary();
                setDbStatus(status.neonConfigured ? 'Neon salvo. Usado automaticamente pelo Gerenciador.' : 'Neon ainda não configurado.');
                setReceitaStatus(status.receitaConfigured ? 'Receita salva. Usada automaticamente nos fluxos e consultas.' : 'Receita ainda não configurada.');
                if (status.neonConfigured) dbConnectionStringInput.placeholder = 'Conexão salva. Cole outra para substituir';
                if (status.receitaConfigured) receitaConnectionStringInput.placeholder = 'Conexão salva. Cole outra para substituir';
            } catch {
                databaseSummary.textContent = 'Não foi possível verificar os bancos salvos.';
                setDbStatus('Não foi possível verificar o acesso salvo.');
                setReceitaStatus('Não foi possível verificar o acesso salvo.');
            }

            checkFormValidity();

            if (window.electronAPI?.getKeyFileStatus) {
                const status = await window.electronAPI.getKeyFileStatus();
                setKeyFileStatus(status.loaded);
            }

            window.electronAPI?.onAutoLoginFailed?.((message) => {
                showMessage(message);
                logoutLink.style.display = 'inline';
            });
        });
