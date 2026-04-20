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

        // --- Helper Functions ---
        const showMessage = (text, type = 'error') => {
            messageDiv.textContent = text;
            messageDiv.className = type === 'success' ? 'success-message' : 'error-message';
        };

        const setDbStatus = (text, type = 'info') => {
            dbStatusMessage.textContent = text;
            dbStatusMessage.className = 'db-status-message';
            if (type === 'success') dbStatusMessage.style.color = 'var(--accent-green)';
            else if (type === 'error') dbStatusMessage.style.color = 'var(--accent-red)';
            else dbStatusMessage.style.color = 'var(--text-secondary)';
        };

        const setLoadingState = (loading) => {
            loginBtn.disabled = loading;
            spinner.style.display = loading ? 'inline-block' : 'none';
            buttonText.textContent = loading ? 'Entrando...' : 'Entrar';
        };

        const checkFormValidity = () => {
            const user = usernameInput.value.trim();
            const pass = passwordInput.value.trim();
            loginBtn.disabled = !user || !pass;
        };

        // --- Event Handlers ---
        const handleTestDbConnection = async () => {
            const connectionString = dbConnectionStringInput.value.trim();
            if (!connectionString) {
                setDbStatus('Por favor, insira a chave de conexão para testar.', 'error');
                return;
            }

            testDbBtn.disabled = true;
            testDbBtn.textContent = 'Testando...';
            setDbStatus('Conectando ao banco de dados...', 'info');

            try {
                const result = await window.electronAPI.saveAndTestDbConnection(connectionString);
                if (result.success) {
                    setDbStatus('Conexão bem-sucedida e chave salva!', 'success');
                    dbConnectionStringInput.style.borderColor = 'var(--accent-green)';
                } else {
                    throw new Error(result.message);
                }
            } catch (error) {
                setDbStatus(`Falha na conexão: ${error.message}`, 'error');
                dbConnectionStringInput.style.borderColor = 'var(--danger)';
            } finally {
                testDbBtn.disabled = false;
                testDbBtn.textContent = 'Testar';
            }
        };

        const handleLogin = async (event) => {
            event.preventDefault();
            const username = usernameInput.value.trim();
            const password = passwordInput.value.trim();
            const rememberMe = rememberMeCheckbox.checked;

            if (!username || !password) {
                showMessage('Preencha usuário e senha.');
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

        // --- Event Listeners ---
        loginForm.addEventListener('submit', handleLogin);
        testDbBtn.addEventListener('click', handleTestDbConnection);

        dbConnectionStringInput.addEventListener('input', () => {
            dbConnectionStringInput.style.borderColor = 'var(--border-color)';
            setDbStatus('');
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
            if (window.electronAPI?.getDbConnectionString) {
                const savedConnectionString = await window.electronAPI.getDbConnectionString();
                if (savedConnectionString) {
                    dbConnectionStringInput.value = savedConnectionString;
                    setDbStatus('Chave de conexão carregada.', 'info');
                } else {
                    setDbStatus('Insira a chave de conexão se for administrador.', 'info');
                }
            }

            checkFormValidity();

            window.electronAPI?.onAutoLoginFailed?.((message) => {
                showMessage(message);
                logoutLink.style.display = 'inline';
            });
        });
