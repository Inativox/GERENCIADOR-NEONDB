console.log('--- RENDERER.JS CARREGADO - VERSÃO NOVA ---');
document.addEventListener('DOMContentLoaded', () => {
    const getBasename = (p) => p.split(/[\\/]/).pop();

    // --- SELETORES GLOBAIS ---
    const navLinks = document.querySelectorAll('.sidebar-nav .tab-button');
    const pageContents = document.querySelectorAll('.page-content');
    const currentTabTitle = document.getElementById('current-tab-title');
    const currentTabDescription = document.getElementById('current-tab-description');
    const mainTitle = document.getElementById("main-app-title");
    const sidebar = document.querySelector('.sidebar');
    const mainContent = document.querySelector('.main-content');
    const logoutBtn = document.getElementById('logoutBtn');
    const monitoringTeamTitle = document.getElementById('monitoring-team-title');
     const organizeDailySheetBtn = document.getElementById('organizeDailySheetBtn');

    if (organizeDailySheetBtn) {
        organizeDailySheetBtn.addEventListener('click', async () => {
            appendLog('Selecionando arquivo(s) para organizar...');
            const files = await window.electronAPI.selectFile({ title: 'Selecione as planilhas para organizar', multi: true });
            if (files && files.length > 0) {
                const organizationType = document.querySelector('input[name="organizeType"]:checked').value;
                appendLog(`Iniciando organização para ${files.length} arquivo(s) usando o formato: ${organizationType}`);
                for (const filePath of files) {
                    appendLog(`Organizando: ${getBasename(filePath)}`);
                    window.electronAPI.organizeDailySheet(filePath, organizationType);
                }
            } else {
                appendLog('Nenhum arquivo selecionado. Operação cancelada.');
            }
        });
    }

    let currentUserRole = null;
    let currentUserTeamId = null;

    // --- VARIÁVEIS GLOBAIS PARA DADOS ---
    let fastwaySummaryData = null;
    let bitrixSummaryData = null;
    let fastwayDetailData = null;
    let bitrixDetailData = null;


    window.electronAPI.onUserInfo(({ username, role, teamId }) => {
        currentUserRole = role;
        currentUserTeamId = teamId;

        const currentUserSpan = document.getElementById('currentUser');
        if (currentUserSpan) {
            currentUserSpan.textContent = username;
        }
        if (logoutBtn) {
            logoutBtn.style.display = 'inline-flex';
        }
        setupUIForRole(role, teamId);

        // NOVO: Lógica para forçar a verificação de blocklist para usuários específicos.
        const checkBlocklistCheckbox = document.getElementById('checkBlocklistCheckbox');
        if (checkBlocklistCheckbox) {
            // Se o usuário NÃO for 'Davi', a opção se torna obrigatória.
            if (username !== 'Davi') {
                checkBlocklistCheckbox.checked = true;
                checkBlocklistCheckbox.disabled = true;
                
                // Adiciona um feedback visual para o usuário.
                const parentSwitch = checkBlocklistCheckbox.closest('.toggle-switch');
                if (parentSwitch) {
                    parentSwitch.style.opacity = '0.7';
                    parentSwitch.style.cursor = 'not-allowed';
                    parentSwitch.title = 'Esta opção é obrigatória para o seu perfil de usuário.';
                }
            } else {
                // Garante que para o usuário 'Davi' a opção esteja sempre habilitada.
                checkBlocklistCheckbox.disabled = false;
            }
        }
    });

    if (logoutBtn) {
        logoutBtn.addEventListener('click', () => {
            window.electronAPI.logout();
        });
    }

    function setupUIForRole(role, teamId) {
        renderApiFilters(role, teamId);

        if (monitoringTeamTitle) {
            monitoringTeamTitle.style.display = 'none';
        }

        if (role === 'limited' || role === 'master') {
            navLinks.forEach(button => {
                const tabName = button.dataset.tabName;
                if (tabName !== 'monitoramento') { // Esta lógica pode ser ajustada se o monitoramento for reativado
                    button.parentElement.style.display = 'none';
                }
            });
            const monitoringTabButton = document.querySelector('.tab-button[data-tab-name="monitoramento"]');
            if (monitoringTabButton) {
                monitoringTabButton.click();
            }

            // Oculta a aba de monitoramento para todos, por enquanto
            document.getElementById('nav-monitoramento').style.display = 'none';

            if (role === 'limited' && teamId && monitoringTeamTitle) {
                const team = gruposOperador.find(g => g.id === teamId);
                const teamName = team ? team.name : `Equipe ID ${teamId}`;
                monitoringTeamTitle.textContent = `Time: ${teamName}`;
                monitoringTeamTitle.style.display = 'block';
            }

        } else if (role === 'admin') {
            navLinks.forEach(button => {
                button.parentElement.style.display = 'list-item';
            });
            const adminDefaultTab = document.querySelector('.tab-button[data-tab-name="local"]');
            if (adminDefaultTab) {
                adminDefaultTab.click();
            }

            // Oculta a aba de monitoramento para todos, por enquanto
            document.getElementById('nav-monitoramento').style.display = 'none';
        }
    }
    window.electronAPI.onUpdateMessage((message) => {
        const updateMessageElement = document.getElementById('update-message');
        if (updateMessageElement) {
            updateMessageElement.innerText = message;
        }
    });

    // --- LÓGICA DE TEMA (CLARO/ESCURO) ---
    const lightThemeBtn = document.getElementById('light-theme-btn');
    const darkThemeBtn = document.getElementById('dark-theme-btn');
    const body = document.body;

    const applyTheme = (theme) => {
        body.className = `${theme}-theme`;
        localStorage.setItem('app-theme', theme);
        lightThemeBtn.classList.toggle('active', theme === 'light');
        darkThemeBtn.classList.toggle('active', theme === 'dark');
    };

    lightThemeBtn.addEventListener('click', () => applyTheme('light'));
    darkThemeBtn.addEventListener('click', () => applyTheme('dark'));

    // Carrega o tema salvo ao iniciar
    const savedTheme = localStorage.getItem('app-theme') || 'dark';
    applyTheme(savedTheme);

    // Adiciona a classe do tema ao body para que as variáveis CSS funcionem
    body.classList.add(`${savedTheme}-theme`);

    window.electronAPI.onUpdateMessage((message) => {
        const updateMessageElement = document.getElementById('update-message');
        if (updateMessageElement) { updateMessageElement.innerText = message; }
    });

    // --- LÓGICA DA BARRA DE TÍTULO PERSONALIZADA ---
    document.getElementById('minimize-btn').addEventListener('click', () => window.electronAPI.minimizeWindow());
    document.getElementById('maximize-btn').addEventListener('click', () => window.electronAPI.maximizeWindow());
    document.getElementById('close-btn').addEventListener('click', () => window.electronAPI.closeWindow());


    // --- LÓGICA DOS PAINÉIS EXPANSÍVEIS ---
    const expandablePanelTriggers = document.querySelectorAll('[id^="toggle-"]');
    const panelOverlays = document.querySelectorAll('.expandable-panel-overlay');
    const panelCloseButtons = document.querySelectorAll('[data-close-panel]');

    expandablePanelTriggers.forEach(button => {
        button.addEventListener('click', () => {
            const panelId = button.id.replace('toggle-', '').replace('-btn', '') + '-panel';
            const panel = document.getElementById(panelId);
            if (panel) {
                panel.classList.add('active');
            }
        });
    });

    const closePanel = (panelId) => {
        const panel = document.getElementById(panelId);
        if (panel) {
            panel.classList.remove('active');
        }
    };

    panelCloseButtons.forEach(button => {
        button.addEventListener('click', () => closePanel(button.dataset.closePanel));
    });

    panelOverlays.forEach(overlay => {
        overlay.addEventListener('click', (e) => {
            if (e.target === overlay) { // Fecha somente se clicar no fundo escuro
                closePanel(overlay.id);
            }
        });
    });

    const gridConfigs = {
        'localGrid': 'local-sections',
        'apiGrid': 'api-sections',
        'enrichmentGrid': 'enrichment-sections',
        // NOVO: Adiciona a grid da nova aba
        'api-tools-panel': 'api-tools-sections',
        'blocklistGrid': 'blocklist-sections',
        'relacionamentoGrid': 'relacionamento-sections', // <-- Adicionado para a nova aba
    };

    let sortableInstances = {};

    // MODIFICADO: Salva também largura e altura
    function saveSectionState(tabId, sections) {
        const state = sections.map(section => ({
            id: section.dataset.sectionId,
            visible: !section.classList.contains('hidden'),
            width: section.style.width,
            height: section.style.height
        }));
        localStorage.setItem(`sections-layout-${tabId}`, JSON.stringify(state));
    }

    // MODIFICADO: Carrega do novo local de armazenamento
    function loadSectionState(tabId) {
        const saved = localStorage.getItem(`sections-layout-${tabId}`);
        return saved ? JSON.parse(saved) : null;
    }

    // MODIFICADO: Aplica também largura e altura
    function applySectionState(grid, state) {
        if (!state) return;

        const sections = Array.from(grid.querySelectorAll('.section'));

        // Primeiro, aplica os estilos e visibilidade
        sections.forEach(section => {
            const savedSection = state.find(s => s.id === section.dataset.sectionId);
            if (savedSection) {
                section.style.width = savedSection.width || '';
                section.style.height = savedSection.height || '';
            }
        });

        // Depois, reordena
        state.forEach(savedSection => {
            const section = sections.find(s => s.dataset.sectionId === savedSection.id);
            if (section) {
                grid.appendChild(section);
                if (!savedSection.visible) {
                    section.classList.add('hidden');
                    const hideBtn = section.querySelector('.hide-section');
                    if (hideBtn) {
                        hideBtn.textContent = '👁‍🗨';
                        hideBtn.title = 'Exibir';
                    }
                } else {
                    const hideBtn = section.querySelector('.hide-section');
                    if (hideBtn) {
                        hideBtn.textContent = '👁';
                        hideBtn.title = 'Ocultar';
                    }
                }
            }
        });
    }

    // NOVO: Função para resetar o layout de uma aba
    function resetLayout(gridId, storageKey) {
        localStorage.removeItem(`sections-layout-${storageKey}`);
        const grid = document.getElementById(gridId);
        if (grid) {
            grid.querySelectorAll('.section').forEach(section => {
                section.style.width = '';
                section.style.height = '';
                section.classList.remove('hidden');
            });
            // Recarrega a página para reordenar para o padrão do HTML
            location.reload();
        }
    }

    function initializeSortable(gridId, storageKey) {
        const grid = document.getElementById(gridId);
        if (!grid) return;

        const savedState = loadSectionState(storageKey);
        applySectionState(grid, savedState);

        const sortable = Sortable.create(grid, {
            animation: 300,
            ghostClass: 'sortable-ghost',
            chosenClass: 'sortable-chosen',
            dragClass: 'sortable-drag',
            handle: '.drag-handle',
            onEnd: function (evt) {
                const sections = Array.from(grid.querySelectorAll('.section'));
                saveSectionState(storageKey, sections);
            }
        });

        sortableInstances[gridId] = sortable;

        // NOVO: Observer para salvar o tamanho dos cards ao redimensionar
        const resizeObserver = new ResizeObserver(() => {
            // Usamos um timeout para evitar salvar em cada pixel do redimensionamento
            clearTimeout(grid.resizeTimeout);
            grid.resizeTimeout = setTimeout(() => {
                const sections = Array.from(grid.querySelectorAll('.section'));
                saveSectionState(storageKey, sections);
            }, 500);
        });

        // Adiciona listeners para os botões e o observer
        grid.addEventListener('click', function (e) {
            if (e.target.classList.contains('hide-section')) {
                const section = e.target.closest('.section');
                const isHidden = section.classList.contains('hidden');

                if (isHidden) {
                    section.classList.remove('hidden');
                    e.target.textContent = '👁';
                    e.target.title = 'Exibir';
                } else {
                    section.classList.add('hidden');
                    e.target.textContent = '👁‍🗨';
                    e.target.title = 'Exibir';
                }
                const sections = Array.from(grid.querySelectorAll('.section'));
                saveSectionState(storageKey, sections);
            }
        });

        grid.querySelectorAll('.section').forEach(section => {
            resizeObserver.observe(section);
        });
    }

    Object.entries(gridConfigs).forEach(([gridId, storageKey]) => {
        initializeSortable(gridId, storageKey);
    });

    const tabInfo = {
        'Limpeza Local': {
            title: 'Limpeza Local de Bases',
            description: 'Otimize suas bases de dados localmente, removendo duplicidades e ajustando informações com precisão. Ideal para manter seus registros impecáveis.'
        },
        'Consulta CNPJ (API)': {
            title: 'Consulta CNPJ via API',
            description: 'Realize consultas de CNPJ diretamente via API para obter a situação da empresa.'
        },
        'Enriquecimento': {
            title: 'Enriquecimento de Dados',
            description: 'Amplie suas bases com informações valiosas, como telefones e outros dados de contato, a partir de fontes confiáveis. Maximize o potencial de suas campanhas.'
        },
        'Blocklist': {
            title: 'Gerenciador de Blocklist',
            description: 'Administre a lista de telefones restritos. Alimente a base de dados e prepare arquivos para garantir a conformidade e eficiência das suas campanhas.'
        },
        'Monitoramento': {
            title: 'Monitoramento de Relatórios',
            description: 'Acompanhe os dados de chamadas em tempo real. Filtre e visualize informações para análise de performance e tomada de decisão.'
        },
        'Relacionamento': { // <-- Adicionado para a nova aba
            title: 'Pipeline de Relacionamento',
            description: 'Execute o pipeline de processamento de planilhas para gerar a base de elegíveis do modo padrão ou relacionamento.'
        }
    };

    const resetLayoutBtn = document.querySelector('.reset-layout-btn');
    let isTransitioning = false; // Flag para evitar cliques duplos durante a transição

    function openTab(event, tabNameId) {
        if (isTransitioning) return;
        isTransitioning = true;

        let activePage = document.querySelector('.page-content.active');

        navLinks.forEach(button => {
            button.classList.remove('active');
        });

        if (event && event.currentTarget) {
            event.currentTarget.classList.add('active');
        }

        const showNewPage = () => {
            pageContents.forEach(content => content.classList.remove('active', 'fade-out')); // Remove fade-out também
            const newPage = document.getElementById(tabNameId);
            if (newPage) {
                newPage.classList.add('active');
            }
            isTransitioning = false;
        };
        
        if (activePage && activePage.id !== tabNameId) {
            activePage.classList.add('fade-out'); // Adiciona a classe de fade-out
            // A duração do setTimeout deve corresponder à duração da animação fadeOut no CSS
            setTimeout(showNewPage, 300); 
        } else {
            showNewPage();
        }

        const mainContent = document.querySelector('.main-content');
        // Reseta as classes de tema, mas mantém as classes base
        mainContent.classList.remove('c6-theme', 'enrichment-theme', 'monitoring-theme', 'blocklist-theme');
        // mainContent.className = 'main-content custom-scrollbar'; // Isso remove outras classes que podem ser importantes
        // Melhor remover apenas as classes de tema

        let themeClass = '';
        if (tabNameId === 'api') {
            themeClass = 'c6-theme';
        } else if (tabNameId === 'enriquecimento') {
            themeClass = 'enrichment-theme';
        } else if (tabNameId === 'monitoramento') {
            themeClass = 'monitoring-theme';
        } else if (tabNameId === 'blocklist') {
            themeClass = 'blocklist-theme';
            updateBlocklistStats(); // Atualiza as estatísticas ao abrir a aba
        }
        // A nova aba 'relacionamento' não precisa de tema por enquanto
        if (themeClass) mainContent.classList.add(themeClass);


        const tabButtonText = event ? event.currentTarget.querySelector('span').textContent.trim() : '';
        if (tabInfo[tabButtonText]) {
            mainTitle.classList.add("hidden");
            currentTabTitle.textContent = tabInfo[tabButtonText].title;
            currentTabDescription.textContent = tabInfo[tabButtonText].description;
        } else {
            mainTitle.classList.remove("hidden");
            currentTabTitle.textContent = "";
            currentTabDescription.textContent = "";
        }

        // NOVO: Configura o botão de reset para a aba atual
        if (resetLayoutBtn) {
            const gridId = `${tabNameId}Grid`;
            const storageKey = gridConfigs[gridId];
            if (storageKey) {
                resetLayoutBtn.style.display = 'inline-flex';
                resetLayoutBtn.onclick = () => resetLayout(gridId, storageKey);
            } else {
                resetLayoutBtn.style.display = 'none';
            }
        }
    }

    navLinks.forEach(button => {
        button.addEventListener('click', (event) => {
            const tabNameId = event.currentTarget.dataset.tabName;
            if (tabNameId) {
                openTab(event, tabNameId);
            }
        });
    });

    // #################################################################
    // #           LÓGICA DA ABA DE LIMPEZA LOCAL E OUTRAS             #
    // #################################################################
    let rootFile = null;
    let cleanFiles = [];
    let mergeFiles = [];
    let backupEnabled = false;
    let autoAdjustPhones = false;
    let checkDbEnabled = false;
    let saveToDbEnabled = false;
    const selectRootBtn = document.getElementById('selectRootBtn');
    const autoRootBtn = document.getElementById('autoRootBtn');
    const feedRootBtn = document.getElementById('feedRootBtn');
    const updateBlocklistBtn = document.getElementById('updateBlocklistBtn');
    const addCleanFileBtn = document.getElementById('addCleanFileBtn');
    const startCleaningBtn = document.getElementById('startCleaningBtn');
    const resetLocalBtn = document.getElementById('resetLocalBtn');
    const adjustPhonesBtn = document.getElementById('adjustPhonesBtn');
    const backupCheckbox = document.getElementById('backupCheckbox').parentElement;
    const autoAdjustPhonesCheckbox = document.getElementById('autoAdjustPhonesCheckbox');
    const rootFilePathSpan = document.getElementById('rootFilePath');
    const selectedCleanFilesDiv = document.getElementById('selectedCleanFiles');
    const progressContainer = document.getElementById('progressContainer');
    const logDiv = document.getElementById('log');
    const rootColSelect = document.getElementById('rootCol');
    const destColSelect = document.getElementById('destCol');
    const selectMergeFilesBtn = document.getElementById('selectMergeFilesBtn');
    const startMergeBtn = document.getElementById('startMergeBtn');
    const selectedMergeFilesDiv = document.getElementById('selectedMergeFiles');
    const saveStoredCnpjsBtn = document.getElementById('saveStoredCnpjsBtn');
    const checkDbCheckbox = document.getElementById('checkDbCheckbox');
    const saveToDbCheckbox = document.getElementById('saveToDbCheckbox');
    const consultDbBtn = document.getElementById('consultDbBtn');
    const uploadProgressContainer = document.getElementById('uploadProgressContainer');
    const uploadProgressTitle = document.getElementById('uploadProgressTitle');
    const uploadProgressBarFill = document.getElementById('uploadProgressBarFill');
    const uploadProgressText = document.getElementById('uploadProgressText');
    const batchIdInput = document.getElementById('batchIdInput');
    const deleteBatchBtn = document.getElementById('deleteBatchBtn');
    const mergeStrategyRadios = document.querySelectorAll('input[name="mergeStrategy"]');
    const customMergeInputContainer = document.getElementById('customMergeInputContainer');
    const customMergeCountInput = document.getElementById('customMergeCount');
    const removeDuplicatesCheckbox = document.getElementById('removeDuplicatesCheckbox');
    const selectListToSplitBtn = document.getElementById('selectListToSplitBtn');
    const listToSplitPathDiv = document.getElementById('listToSplitPath');
    const linesPerSplitInput = document.getElementById('linesPerSplit');
    const splitListBtn = document.getElementById('splitListBtn');
    const shuffleResultCheckbox = document.getElementById('shuffleResultCheckbox');
    
    // NOVO: Captura dos novos elementos da Blocklist
    const feedBlocklistBtn = document.getElementById('feedBlocklistBtn');
    const checkBlocklistCheckbox = document.getElementById('checkBlocklistCheckbox');

    // --- INÍCIO: LÓGICA ABRANGENTE DE SALVAR/CARREGAR ESTADO DA UI ---

    // Função para salvar o estado atual dos controles da UI
    function saveCurrentUiSettings() {
        const settings = {
            // Aba Limpeza Local
            backup: backupCheckbox.querySelector('input').checked,
            autoAdjust: autoAdjustPhonesCheckbox.checked,
            checkDb: checkDbCheckbox.checked,
            saveToDb: saveToDbCheckbox.checked,
            checkBlocklist: checkBlocklistCheckbox.checked,
            autoRoot: autoRootBtn.dataset.on === 'true',
            rootCol: rootColSelect.value,
            destCol: destColSelect.value,
            organizeType: document.querySelector('input[name="organizeType"]:checked')?.value || 'bernardo',
            mergeStrategy: document.querySelector('input[name="mergeStrategy"]:checked')?.value || 'all',
            customMergeCount: customMergeCountInput.value,
            removeDuplicates: removeDuplicatesCheckbox.checked,
            shuffleResult: shuffleResultCheckbox.checked,
            linesPerSplit: linesPerSplitInput.value,

            // Aba API
            apiKeySelection: apiKeySelection.value,
            apiRemoveClients: document.getElementById('apiRemoveClientsCheckbox').checked,
            apiExtractClients: document.getElementById('apiExtractClientsCheckbox').checked,
            apiFishMode: document.getElementById('apiFishModeCheckbox').checked, // NOVO

            // Aba Enriquecimento
            masterFileYear: document.getElementById('master-file-year-input').value,
            enrichmentYear: document.getElementById('enrichment-year-input').value,
            enrichStrategy: document.querySelector('input[name="enrichStrategy"]:checked')?.value || 'append',

            // Aba Blocklist
            linesPerCsvSplit: linesPerCsvSplitInput.value,
        };
        window.electronAPI.saveUiSettings(settings);
    }

    // Função para carregar e aplicar as configurações salvas
    async function loadAndApplyUiSettings() {
        const settings = await window.electronAPI.getUiSettings();
        if (!settings || Object.keys(settings).length === 0) return; // Nenhuma configuração salva

        // Helper para definir valor e disparar evento 'change'
        const setValue = (element, value, event = 'change') => {
            if (element && value !== undefined && value !== null) {
                element.value = value;
                element.dispatchEvent(new Event(event));
            }
        };
        const setChecked = (element, checked, event = 'change') => {
            if (element && checked !== undefined && checked !== null) {
                element.checked = checked;
                element.dispatchEvent(new Event(event));
            }
        };
        const setRadio = (name, value, event = 'change') => {
            if (value) {
                const radio = document.querySelector(`input[name="${name}"][value="${value}"]`);
                if (radio) {
                    radio.checked = true;
                    radio.dispatchEvent(new Event(event));
                }
            }
        };

        // Aba Limpeza Local
        setChecked(backupCheckbox.querySelector('input'), settings.backup);
        setChecked(autoAdjustPhonesCheckbox, settings.autoAdjust);
        setChecked(checkDbCheckbox, settings.checkDb);
        setChecked(saveToDbCheckbox, settings.saveToDb);
        setChecked(checkBlocklistCheckbox, settings.checkBlocklist);
        setValue(rootColSelect, settings.rootCol);
        setValue(destColSelect, settings.destCol);
        setRadio('organizeType', settings.organizeType);
        setRadio('mergeStrategy', settings.mergeStrategy);
        setValue(customMergeCountInput, settings.customMergeCount);
        setChecked(removeDuplicatesCheckbox, settings.removeDuplicates);
        setChecked(shuffleResultCheckbox, settings.shuffleResult);
        setValue(linesPerSplitInput, settings.linesPerSplit);

        // Aplica a configuração do Auto Raiz
        if (settings.autoRoot && autoRootBtn.dataset.on !== 'true') {
            autoRootBtn.click(); // Simula o clique para garantir que a UI seja atualizada corretamente
        } else if (!settings.autoRoot && autoRootBtn.dataset.on === 'true') {
            autoRootBtn.click();
        }

        // Aba API
        setValue(apiKeySelection, settings.apiKeySelection);
        setChecked(document.getElementById('apiRemoveClientsCheckbox'), settings.apiRemoveClients);
        setChecked(document.getElementById('apiExtractClientsCheckbox'), settings.apiExtractClients);
        setChecked(document.getElementById('apiFishModeCheckbox'), settings.apiFishMode); // NOVO

        // Aba Enriquecimento
        setValue(document.getElementById('master-file-year-input'), settings.masterFileYear);
        setValue(document.getElementById('enrichment-year-input'), settings.enrichmentYear);
        setRadio('enrichStrategy', settings.enrichStrategy);

        // Aba Blocklist
        setValue(linesPerCsvSplitInput, settings.linesPerCsvSplit);

        appendLog('Configurações da última sessão foram restauradas.');
    }

    // Carrega as configurações quando o DOM estiver pronto
    loadAndApplyUiSettings();

    // Adiciona listeners para salvar as configurações em todos os elementos relevantes
    document.querySelectorAll('input[type="checkbox"], input[type="radio"], select, input[type="number"], input[type="text"]').forEach(el => {
        el.addEventListener('change', saveCurrentUiSettings);
        el.addEventListener('keyup', saveCurrentUiSettings); // Para campos de texto
    });
    document.querySelectorAll('button[data-on]').forEach(el => {
        el.addEventListener('click', saveCurrentUiSettings);
    });

    // --- FIM: LÓGICA ABRANGENTE DE SALVAR/CARREGAR ESTADO DA UI ---

    // --- INÍCIO: LÓGICA DO MODAL DE LISTA DE ARQUIVOS DE LIMPEZA ---
    const cleanFilesListModal = document.getElementById('clean-files-list-modal');
    const cleanFilesListContainer = document.getElementById('clean-files-list-items');
    const cleanFilesDivForModal = document.getElementById('selectedCleanFiles');

    if (cleanFilesListModal) {
        cleanFilesListModal.addEventListener('click', (e) => {
            if (e.target.classList.contains('modal-overlay') || e.target.dataset.closeModal === 'clean-files-list-modal') {
                cleanFilesListModal.classList.add('hidden');
            }
        });
    }

    if (cleanFilesDivForModal) {
        cleanFilesDivForModal.addEventListener('dblclick', () => {
            if (cleanFiles.length > 0) {
                cleanFilesListContainer.innerHTML = cleanFiles.map(f => `<li><span class="name">${getBasename(f.path)}</span></li>`).join('');
                cleanFilesListModal.classList.remove('hidden');
            }
        });
    }
    // --- FIM: LÓGICA DO MODAL ---

    function addFileToUI(container, filePath, isSingle) { if (isSingle) { container.innerHTML = ''; } const fileDiv = document.createElement('div'); fileDiv.className = 'file-item new-item'; fileDiv.textContent = getBasename(filePath); container.appendChild(fileDiv); setTimeout(() => { fileDiv.classList.remove('new-item'); }, 500); }
    function resetUploadProgress() { if (uploadProgressContainer) uploadProgressContainer.style.display = 'none'; if (uploadProgressBarFill) uploadProgressBarFill.style.width = '0%'; if (uploadProgressText) uploadProgressText.textContent = ''; }
    if (backupCheckbox) backupCheckbox.addEventListener('change', (e) => { backupEnabled = e.target.querySelector('input').checked; });
    if (autoAdjustPhonesCheckbox) autoAdjustPhonesCheckbox.addEventListener('change', () => { autoAdjustPhones = autoAdjustPhonesCheckbox.checked; });
    if (checkDbCheckbox) checkDbCheckbox.addEventListener('change', () => { checkDbEnabled = checkDbCheckbox.checked; appendLog(`Consulta ao Banco de Dados: ${checkDbEnabled ? 'ATIVADA' : 'DESATIVADA'}`); });
    if (saveToDbCheckbox) saveToDbCheckbox.addEventListener('change', () => { saveToDbEnabled = saveToDbCheckbox.checked; appendLog(`Salvar no Banco de Dados: ${saveToDbEnabled ? 'ATIVADO' : 'DESATIVADO'}`); });
    if (saveStoredCnpjsBtn) saveStoredCnpjsBtn.addEventListener('click', async () => { appendLog('Solicitando salvamento do histórico de CNPJs em Excel...'); const result = await window.electronAPI.saveStoredCnpjsToExcel(); appendLog(result.message); });
    if (deleteBatchBtn) deleteBatchBtn.addEventListener('click', async () => { const batchId = batchIdInput.value.trim(); if (!batchId) { appendLog('❌ ERRO: Por favor, insira um ID de Lote para excluir.'); return; } const confirmation = confirm(`ATENÇÃO!\n\nVocê tem certeza que deseja excluir PERMANENTEMENTE todos os CNPJs do lote "${batchId}" do banco de dados?\n\nEsta ação não pode ser desfeita.`); if (confirmation) { appendLog(`Enviando solicitação para excluir o lote: ${batchId}...`); const result = await window.electronAPI.deleteBatch(batchId); appendLog(result.message); if (result.success) { batchIdInput.value = ''; } } else { appendLog('Operação de exclusão cancelada pelo usuário.'); } });
    if (consultDbBtn) consultDbBtn.addEventListener('click', async () => { appendLog('Selecionando arquivos para consulta apenas pelo BD...'); const files = await window.electronAPI.selectFile({ title: 'Selecione arquivos para limpar apenas pelo BD', multi: true }); if (!files || files.length === 0) { appendLog('Nenhum arquivo selecionado.'); return; } window.electronAPI.startDbOnlyCleaning({ filesToClean: files, saveToDb: saveToDbEnabled }); });
    if (selectRootBtn) selectRootBtn.addEventListener('click', async () => { const files = await window.electronAPI.selectFile({ title: 'Selecione a Lista Raiz', multi: false }); if (files && files.length > 0) { rootFile = files[0]; addFileToUI(rootFilePathSpan, rootFile, true); appendLog(`Arquivo raiz selecionado: ${rootFile}`); } });
    if (autoRootBtn) autoRootBtn.addEventListener('click', () => { if (autoRootBtn.dataset.on) { delete autoRootBtn.dataset.on; autoRootBtn.textContent = "Auto Raiz: OFF"; rootFile = null; rootFilePathSpan.innerHTML = '<span style="color:var(--text-muted); font-style:italic;">Usará arquivo local selecionado</span>'; selectRootBtn.disabled = false; } else { autoRootBtn.dataset.on = 'true'; autoRootBtn.textContent = "Auto Raiz: ON"; rootFile = null; rootFilePathSpan.innerHTML = '<span style="color:var(--accent-light); font-weight: 600;">Usará a base de dados Raiz</span>'; selectRootBtn.disabled = true; } appendLog(`Auto Raiz: ${autoRootBtn.dataset.on ? 'ON (usando Banco de Dados)' : 'OFF'}`); });
    if (updateBlocklistBtn) updateBlocklistBtn.addEventListener('click', async () => { const result = await window.electronAPI.updateBlocklist(backupEnabled); appendLog(result.success ? result.message : `Erro: ${result.message}`); });
    if (addCleanFileBtn) {
        addCleanFileBtn.addEventListener('click', async () => {
            const files = await window.electronAPI.selectFile({ title: 'Selecione arquivos para limpar', multi: true });
            if (!files?.length) return;
            cleanFiles = [];
            selectedCleanFilesDiv.innerHTML = '';
            progressContainer.innerHTML = '';
            files.forEach(file => {
                const id = `clean-${cleanFiles.length}`;
                cleanFiles.push({ path: file, id });
                appendLog(`Adicionado para limpeza: ${file}`);
                progressContainer.innerHTML += `<div class="file-progress" style="margin-bottom: 15px;"><strong>${getBasename(file)}</strong><div class="progress-bar-container"><div class="progress-bar-fill" id="${id}"></div></div></div>`;
            });
            // Lógica de exibição resumida
            const firstFileName = getBasename(files[0]);
            const summaryText = files.length > 1 ? `${firstFileName} (e mais ${files.length - 1} arquivo(s))` : firstFileName;
            addFileToUI(selectedCleanFilesDiv, summaryText, true);
        });
    }
    
    if (startCleaningBtn) {
        startCleaningBtn.addEventListener('click', () => {
            const isAutoRoot = autoRootBtn.dataset.on === 'true';
            if (!isAutoRoot && !rootFile) { return appendLog('ERRO: Selecione o arquivo raiz ou ative o Auto Raiz.'); }
            if (!cleanFiles.length) { return appendLog('ERRO: Adicione ao menos um arquivo para limpar.'); }
            resetUploadProgress();
            appendLog('Iniciando limpeza...');
    
            // MODIFICADO: Adiciona o 'checkBlocklist' ao objeto de argumentos
            window.electronAPI.startCleaning({
                isAutoRoot,
                rootFile: isAutoRoot ? null : rootFile,
                cleanFiles,
                rootCol: rootColSelect.value,
                destCol: destColSelect.value,
                backup: backupEnabled,
                checkDb: checkDbEnabled,
                saveToDb: saveToDbEnabled,
                autoAdjust: autoAdjustPhones,
                checkBlocklist: checkBlocklistCheckbox.checked // Adicionado aqui
            });
        });
    }

    if (resetLocalBtn) resetLocalBtn.addEventListener('click', () => { rootFile = null; cleanFiles = []; mergeFiles = []; backupEnabled = false; autoAdjustPhones = false; checkDbEnabled = false; saveToDbEnabled = false; if (rootFilePathSpan) rootFilePathSpan.innerHTML = ''; if (selectedCleanFilesDiv) selectedCleanFilesDiv.innerHTML = ''; if (progressContainer) progressContainer.innerHTML = ''; if (logDiv) logDiv.textContent = ''; if (selectedMergeFilesDiv) selectedMergeFilesDiv.innerHTML = ''; if (batchIdInput) batchIdInput.value = ''; if (backupCheckbox) backupCheckbox.querySelector('input').checked = false; if (autoAdjustPhonesCheckbox) autoAdjustPhonesCheckbox.checked = false; if (checkDbCheckbox) checkDbCheckbox.checked = false; if (saveToDbCheckbox) saveToDbCheckbox.checked = false; if (checkBlocklistCheckbox) checkBlocklistCheckbox.checked = false; if (autoRootBtn) { delete autoRootBtn.dataset.on; autoRootBtn.textContent = 'Auto Raiz: OFF'; selectRootBtn.disabled = false; } resetUploadProgress(); appendLog('Módulo de Limpeza Local reiniciado.'); });
    if (adjustPhonesBtn) adjustPhonesBtn.addEventListener('click', async () => { const files = await window.electronAPI.selectFile({ title: 'Selecione arquivo para ajustar fones', multi: false }); if (!files?.length) return appendLog('Nenhum arquivo selecionado.'); window.electronAPI.startAdjustPhones({ filePath: files[0], backup: backupEnabled }); });
    if (selectMergeFilesBtn) selectMergeFilesBtn.addEventListener('click', async () => { const files = await window.electronAPI.selectFile({ title: 'Selecione arquivos para mesclar', multi: true }); if (!files?.length) return; mergeFiles = files; selectedMergeFilesDiv.innerHTML = ''; files.forEach(f => { addFileToUI(selectedMergeFilesDiv, f, false); }); });
    
    if (mergeStrategyRadios) {
        mergeStrategyRadios.forEach(radio => {
            radio.addEventListener('change', () => {
                if (radio.value === 'custom' && radio.checked) {
                    customMergeInputContainer.style.display = 'block';
                } else {
                    customMergeInputContainer.style.display = 'none';
                }
            });
        });
    }

    if (startMergeBtn) {
        startMergeBtn.addEventListener('click', () => {
            if (mergeFiles.length < 2) {
                return appendLog('ERRO: Por favor, selecione pelo menos dois arquivos para mesclar.');
            }
            const strategy = document.querySelector('input[name="mergeStrategy"]:checked').value;
            const customCount = parseInt(customMergeCountInput.value, 10) || 0;
            if (strategy === 'custom' && (!customCount || customCount <= 0)) {
                return appendLog('ERRO: Para mesclagem personalizada, insira um número de linhas válido e maior que zero.');
            }
            const mergeOptions = {
                files: mergeFiles,
                strategy: strategy,
                customCount: customCount,
                removeDuplicates: removeDuplicatesCheckbox.checked,
                shuffle: shuffleResultCheckbox.checked
            };
            
            appendLog('Iniciando mesclagem com as opções selecionadas...');
            window.electronAPI.startMerge(mergeOptions);
        });
    }

    // NOVO: Event listener para o botão de alimentar a blocklist
    if (feedBlocklistBtn) {
        feedBlocklistBtn.addEventListener('click', async () => {
            appendLog('Selecionando arquivo(s) para alimentar a Blocklist de Telefones...');
            const files = await window.electronAPI.selectFile({
                title: 'Selecione planilhas com telefones para a Blocklist',
                multi: true
            });
            if (!files || files.length === 0) {
                appendLog('Nenhum arquivo selecionado. Operação cancelada.');
                return;
            }
            feedBlocklistBtn.disabled = true;
            appendLog(`Iniciando o processo de alimentação da Blocklist com ${files.length} arquivo(s).`);
            window.electronAPI.feedBlocklist(files);
            // Re-habilita o botão após um tempo para evitar spam,
            // idealmente, você teria um evento 'feed-blocklist-finished' do main.js
            setTimeout(() => {
                feedBlocklistBtn.disabled = false;
            }, 5000); 
        });
    }

    let listToSplitFile = null;
    if (selectListToSplitBtn) {
        selectListToSplitBtn.addEventListener('click', async () => {
            const files = await window.electronAPI.selectFile({ title: 'Selecione a Lista para Dividir', multi: false });
            if (files && files.length > 0) {
                listToSplitFile = files[0];
                addFileToUI(listToSplitPathDiv, listToSplitFile, true);
                appendLog(`Arquivo para divisão selecionado: ${listToSplitFile}`);
            }
        });
    }
    if (splitListBtn) {
        splitListBtn.addEventListener('click', () => {
            const linesPerSplit = parseInt(linesPerSplitInput.value, 10);
            if (!listToSplitFile) {
                appendLog('❌ ERRO: Selecione um arquivo para dividir.');
                return;
            }
            if (!linesPerSplit || linesPerSplit <= 0) {
                appendLog('❌ ERRO: Insira um número de linhas válido e maior que zero.');
                return;
            }
            window.electronAPI.splitList({ filePath: listToSplitFile, linesPerSplit });
        });
    }

    if (feedRootBtn) feedRootBtn.addEventListener('click', async () => { appendLog('Selecionando arquivos para alimentar a base Raiz...'); const files = await window.electronAPI.selectFile({ title: 'Selecione planilhas com CNPJs para a Raiz', multi: true }); if (!files || files.length === 0) { appendLog('Nenhum arquivo selecionado. Operação cancelada.'); return; } feedRootBtn.disabled = true; appendLog(`Iniciando o processo de alimentação da Raiz com ${files.length} arquivo(s).`); window.electronAPI.feedRootDatabase(files); });
    window.electronAPI.onRootFeedFinished(() => { if (feedRootBtn) feedRootBtn.disabled = false; appendLog('✅ Processo de alimentação da Raiz finalizado.'); });
    window.electronAPI.onLog((msg) => appendLog(msg));
    window.electronAPI.onProgress(({ id, progress }) => { const bar = document.getElementById(id); if (bar) bar.style.width = `${progress}%`; });
    window.electronAPI.onUploadProgress(({ current, total }) => { uploadProgressContainer.style.display = 'block'; uploadProgressTitle.textContent = 'Enviando para o Banco de Dados Compartilhado:'; const percent = Math.round((current / total) * 100); uploadProgressBarFill.style.width = `${percent}%`; uploadProgressText.textContent = `Enviando lote ${current} de ${total}...`; if (current === total) { uploadProgressTitle.textContent = 'Envio para o Banco de Dados Concluído!'; } });
    // MODIFICADO: Função appendLog para melhor formatação
    function appendLog(msg) {
        if (!logDiv) return;
        if (logDiv.textContent.trim() === 'Aguardando início do sistema...') {
            logDiv.innerHTML = '';
        }
        const lines = msg.split('\n');
        lines.forEach(line => {
            const p = document.createElement('p');
            p.textContent = `> ${line.trim()}`;
            logDiv.appendChild(p);
        });
        logDiv.scrollTop = logDiv.scrollHeight;
    }
    const apiDropzone = document.getElementById('apiDropzone');
    const apiProcessingDiv = document.getElementById('apiProcessing');
    const apiPendingDiv = document.getElementById('apiPending');
    const apiCompletedDiv = document.getElementById('apiCompleted');
    const apiKeySelection = document.getElementById('apiKeySelection');
    const pauseApiBtn = document.getElementById('pauseApiBtn');
    const resumeApiBtn = document.getElementById('resumeApiBtn');
    const startApiBtn = document.getElementById('startApiBtn');
    const resetApiBtn = document.getElementById('resetApiBtn');
    const selectApiFileBtn = document.getElementById('selectApiFileBtn');
    const apiStatusSpan = document.getElementById('apiStatus');
    const apiProgressBarFill = document.getElementById('apiProgressBarFill');
    const apiLogDiv = document.getElementById('apiLog');
    // NOVO: Seletores para os inputs de delay da API
    const apiDelayBetweenBatchesInput = document.getElementById('apiDelayBetweenBatches');
    const apiRetryDelayInput = document.getElementById('apiRetryDelay');

    // NOVO: Função para enviar as configurações de tempo da API
    const sendApiTimingSettings = () => {
        const settings = {
            delayBetweenBatches: parseFloat(apiDelayBetweenBatchesInput.value) || null,
            retryDelay: parseFloat(apiRetryDelayInput.value) || null
        };
        window.electronAPI.updateApiTimingSettings(settings);
    };

    if (pauseApiBtn) pauseApiBtn.addEventListener('click', () => window.electronAPI.pauseApiQueue());
    if (resumeApiBtn) resumeApiBtn.addEventListener('click', () => window.electronAPI.resumeApiQueue());
    if (apiDropzone) { apiDropzone.addEventListener('dragover', (event) => { event.preventDefault(); event.stopPropagation(); apiDropzone.style.borderColor = 'var(--accent-color)'; apiDropzone.style.backgroundColor = 'var(--bg-lighter)'; }); apiDropzone.addEventListener('dragleave', (event) => { event.preventDefault(); event.stopPropagation(); apiDropzone.style.borderColor = 'var(--border-color)'; apiDropzone.style.backgroundColor = 'transparent'; }); apiDropzone.addEventListener('drop', (event) => { event.preventDefault(); event.stopPropagation(); apiDropzone.style.borderColor = 'var(--border-color)'; apiDropzone.style.backgroundColor = 'transparent'; const files = Array.from(event.dataTransfer.files).filter(file => file.path.endsWith('.xlsx') || file.path.endsWith('.xls') || file.path.endsWith('.csv')).map(file => file.path); if (files.length > 0) { window.electronAPI.addFilesToApiQueue(files); } }); }
    if (selectApiFileBtn) selectApiFileBtn.addEventListener('click', async () => { const files = await window.electronAPI.selectFile({ title: 'Selecione as planilhas de CNPJs', multi: true }); if (files && files.length > 0) { window.electronAPI.addFilesToApiQueue(files); } });
    if (startApiBtn) {
        startApiBtn.addEventListener('click', () => {
            startApiBtn.disabled = true;
            resetApiBtn.disabled = true;
            apiStatusSpan.textContent = 'Iniciando processamento da fila...';
            const removeClients = document.getElementById('apiRemoveClientsCheckbox').checked;
            const extractClients = document.getElementById('apiExtractClientsCheckbox').checked;
            const fishMode = document.getElementById('apiFishModeCheckbox').checked; // NOVO
            window.electronAPI.startApiQueue({ keyMode: apiKeySelection.value, removeClients: removeClients, extractClients: extractClients, isFishMode: fishMode }); // MODIFICADO
        });
    }
    if (resetApiBtn) resetApiBtn.addEventListener('click', () => { window.electronAPI.resetApiQueue(); });
    // NOVO: Listeners para os inputs de delay
    if (apiDelayBetweenBatchesInput) {
        apiDelayBetweenBatchesInput.addEventListener('input', sendApiTimingSettings);
    }
    if (apiRetryDelayInput) {
        apiRetryDelayInput.addEventListener('input', sendApiTimingSettings);
    }
    const apiCancelledDiv = document.getElementById('apiCancelled');
    if (!apiCancelledDiv) {
        console.log("WARNING: The element with id 'apiCancelled' was not found. The cancelled files list will not be displayed.");
    }

    function updateApiQueueUI(queue) {
        const { pending, processing, completed, cancelled, isPaused } = queue;
        if (!apiProcessingDiv || !apiPendingDiv || !apiCompletedDiv || !startApiBtn) return;

        const createFileItem = (file, type, index) => {
            const fileDiv = document.createElement('div');
            fileDiv.className = 'file-item';

            const fileName = document.createElement('span');
            fileName.className = 'file-name';
            fileName.textContent = getBasename(file);
            fileDiv.appendChild(fileName);

            const actionsDiv = document.createElement('div');
            actionsDiv.className = 'file-actions';

            if (type === 'pending') {
                if (index > 0) {
                    const prioritizeBtn = document.createElement('button');
                    prioritizeBtn.className = 'queue-action-btn';
                    prioritizeBtn.innerHTML = '&#x25B2;'; // Up arrow
                    prioritizeBtn.title = 'Priorizar';
                    prioritizeBtn.onclick = () => window.electronAPI.prioritizeInApiQueue(file);
                    actionsDiv.appendChild(prioritizeBtn);
                }
                const removeBtn = document.createElement('button');
                removeBtn.className = 'queue-action-btn remove';
                removeBtn.innerHTML = '&#x2716;'; // X mark
                removeBtn.title = 'Remover';
                removeBtn.onclick = () => window.electronAPI.removeFromApiQueue(file);
                actionsDiv.appendChild(removeBtn);
            } else if (type === 'processing') {
                const cancelBtn = document.createElement('button');
                cancelBtn.className = 'queue-action-btn remove';
                cancelBtn.innerHTML = '&#x2716;'; // X mark
                cancelBtn.title = 'Cancelar';
                cancelBtn.onclick = () => {
                    if (confirm(`Tem certeza que deseja cancelar o processamento de: ${getBasename(file)}?`)) {
                        window.electronAPI.cancelCurrentApiTask();
                    }
                };
                actionsDiv.appendChild(cancelBtn);
            }
            fileDiv.appendChild(actionsDiv);
            return fileDiv;
        };

        // Processing
        apiProcessingDiv.innerHTML = '';
        if (processing) {
            apiProcessingDiv.appendChild(createFileItem(processing, 'processing'));
        } else {
            apiProcessingDiv.innerHTML = `<span style="color:var(--text-secondary)">Nenhum</span>`;
        }

        // Pending
        apiPendingDiv.innerHTML = '';
        if (pending && pending.length > 0) {
            pending.forEach((file, index) => apiPendingDiv.appendChild(createFileItem(file, 'pending', index)));
        } else {
            apiPendingDiv.innerHTML = `<span style="color:var(--text-secondary)">Nenhum arquivo na fila</span>`;
        }

        // Completed
        apiCompletedDiv.innerHTML = '';
        if (completed && completed.length > 0) {
            completed.forEach(file => apiCompletedDiv.appendChild(createFileItem(file, 'completed')));
        } else {
            apiCompletedDiv.innerHTML = `<span style="color:var(--text-secondary)">Nenhum arquivo concluído</span>`;
        }
        
        // Cancelled
        if(apiCancelledDiv) {
            apiCancelledDiv.innerHTML = '';
            if (cancelled && cancelled.length > 0) {
                cancelled.forEach(file => apiCancelledDiv.appendChild(createFileItem(file, 'cancelled')));
            } else {
                apiCancelledDiv.innerHTML = `<span style="color:var(--text-secondary)">Nenhum</span>`;
            }
        }

        const isRunning = !!processing;
        const hasFiles = pending.length > 0 || completed.length > 0 || cancelled.length > 0 || isRunning;

        startApiBtn.style.display = !isRunning ? 'inline-flex' : 'none';
        startApiBtn.disabled = pending.length === 0 || isRunning;

        pauseApiBtn.style.display = isRunning && !isPaused ? 'inline-flex' : 'none';
        resumeApiBtn.style.display = isRunning && isPaused ? 'inline-flex' : 'none';

        resetApiBtn.disabled = !hasFiles;
    }

    window.electronAPI.onApiQueueUpdate((queue) => { 
        updateApiQueueUI(queue); 
        if (queue.isPaused) {
            apiStatusSpan.textContent = 'Pausado';
        } else if (!queue.processing && queue.pending.length === 0 && (queue.completed.length > 0 || (queue.cancelled && queue.cancelled.length > 0))) { 
            apiStatusSpan.textContent = 'Fila concluída!';
        } else if (queue.processing) {
            apiStatusSpan.textContent = `Processando...`;
        } else {
            apiStatusSpan.textContent = 'Aguardando início';
        }
    });
    // MODIFICADO: Função appendApiLog para melhor formatação
    window.electronAPI.onApiLog((msg) => {
        if (!apiLogDiv) return;
        // apiLogDiv não tem mensagem inicial, apenas anexa
        const lines = msg.split('\n');
        lines.forEach(line => {
            const p = document.createElement('p');
            p.textContent = `> ${line.trim()}`;
            apiLogDiv.appendChild(p);
        });
        apiLogDiv.scrollTop = apiLogDiv.scrollHeight;
    });
    window.electronAPI.onApiProgress(({ current, total }) => { const percent = Math.round((current / total) * 100); apiProgressBarFill.style.width = `${percent}%`; apiStatusSpan.textContent = `Processando Lote ${current} de ${total}`; });
    function appendApiLog(msg) { if (apiLogDiv) { apiLogDiv.innerHTML += `> ${msg.replace(/\n/g, '<br>> ')}\n`; apiLogDiv.scrollTop = apiLogDiv.scrollHeight; } }
    const selectMasterFilesBtn = document.getElementById('selectMasterFilesBtn');
    const selectedMasterFilesDiv = document.getElementById('selectedMasterFiles');
    const startLoadToDbBtn = document.getElementById('startLoadToDbBtn');
    const selectEnrichFilesBtn = document.getElementById('selectEnrichFilesBtn');
    const selectedEnrichFilesDiv = document.getElementById('selectedEnrichFiles');
    const startEnrichmentBtn = document.getElementById('startEnrichmentBtn');
    const enrichmentLogDiv = document.getElementById('enrichmentLog');
    const enrichmentProgressContainer = document.getElementById('enrichmentProgressContainer');
    const enrichedCnpjCountSpan = document.getElementById('enrichedCnpjCount');
    const refreshCountBtn = document.getElementById('refreshCountBtn');
    const downloadEnrichedDataBtn = document.getElementById('downloadEnrichedDataBtn');
    const dbLoadProgressContainer = document.getElementById('dbLoadProgressContainer');
    const dbLoadProgressTitle = document.getElementById('dbLoadProgressTitle');
    const dbLoadProgressPercent = document.getElementById('dbLoadProgressPercent');
    const dbLoadProgressBarFill = document.getElementById('dbLoadProgressBarFill');
    const dbLoadProgressText = document.getElementById('dbLoadProgressText');
    const dbLoadProgressStats = document.getElementById('dbLoadProgressStats');
    let enrichmentMasterFiles = [];
    let enrichmentEnrichFiles = [];
    // MODIFICADO: Função appendEnrichmentLog para melhor formatação
    function appendEnrichmentLog(msg) {
        if (!enrichmentLogDiv) return;
        if (enrichmentLogDiv.textContent.trim() === 'Aguardando início...') {
            enrichmentLogDiv.innerHTML = '';
        }
        const lines = msg.split('\n');
        lines.forEach(line => {
            const p = document.createElement('p');
            p.textContent = `> ${line.trim()}`;
            enrichmentLogDiv.appendChild(p);
        });
        enrichmentLogDiv.scrollTop = enrichmentLogDiv.scrollHeight;
    }
    async function updateEnrichedCnpjCount() { if (!enrichedCnpjCountSpan) return; try { enrichedCnpjCountSpan.textContent = 'Carregando...'; const count = await window.electronAPI.getEnrichedCnpjCount(); enrichedCnpjCountSpan.textContent = count.toLocaleString('pt-BR'); } catch (error) { enrichedCnpjCountSpan.textContent = 'Erro'; appendEnrichmentLog(`❌ Erro ao carregar contador: ${error.message}`); } }
    if (enrichedCnpjCountSpan) updateEnrichedCnpjCount();
    if (refreshCountBtn) refreshCountBtn.addEventListener('click', updateEnrichedCnpjCount);
    if (downloadEnrichedDataBtn) downloadEnrichedDataBtn.addEventListener('click', async () => { downloadEnrichedDataBtn.disabled = true; downloadEnrichedDataBtn.textContent = 'Preparando download...'; try { const result = await window.electronAPI.downloadEnrichedData(); if (result.success) { appendEnrichmentLog(`✅ ${result.message}`); } else { appendEnrichmentLog(`❌ ${result.message}`); } } catch (error) { appendEnrichmentLog(`❌ Erro no download: ${error.message}`); } finally { downloadEnrichedDataBtn.disabled = false; downloadEnrichedDataBtn.innerHTML = `<svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" fill="currentColor" viewBox="0 0 16 16"><path d="M.5 9.9a.5.5 0 0 1 .5.5v2.5a1 1 0 0 0 1 1h12a1 1 0 0 0 1-1v-2.5a.5.5 0 0 1 1 0v2.5a2 2 0 0 1-2 2H2a2 2 0 0 1-2-2v-2.5a.5.5 0 0 1 .5-.5z"/><path d="M7.646 1.146a.5.5 0 0 1 .708 0l3 3a.5.5 0 0 1-.708.708L8.5 2.707V11.5a.5.5 0 0 1-1 0V2.707L5.354 4.854a.5.5 0 1 1-.708-.708l3-3z"/></svg>Baixar Dados Enriquecidos`; } });
    if (selectMasterFilesBtn) selectMasterFilesBtn.addEventListener('click', async () => { const files = await window.electronAPI.selectFile({ title: 'Selecione as Planilhas Mestras', multi: true }); if (!files?.length) return; enrichmentMasterFiles = files; selectedMasterFilesDiv.innerHTML = ''; files.forEach(file => { addFileToUI(selectedMasterFilesDiv, file, false); }); });

    if (startLoadToDbBtn) {
        startLoadToDbBtn.addEventListener('click', () => {
            if (enrichmentMasterFiles.length === 0) {
                return appendEnrichmentLog('❌ ERRO: Selecione pelo menos uma planilha mestra.');
            }
            const masterFileYearInput = document.getElementById('master-file-year-input');
            const year = masterFileYearInput.value;
            if (!year || isNaN(parseInt(year))) {
                return appendEnrichmentLog('❌ ERRO: Por favor, insira um ano válido para a base de dados.');
            }
            startLoadToDbBtn.disabled = true;
            dbLoadProgressContainer.style.display = 'block';
            dbLoadProgressBarFill.style.width = '0%';
            dbLoadProgressPercent.textContent = '0%';
            dbLoadProgressText.textContent = 'Iniciando...';
            dbLoadProgressStats.textContent = '';
            appendEnrichmentLog(`Iniciando carga para o banco de dados para o ano de ${year}...`);
            window.electronAPI.startDbLoad({ masterFiles: enrichmentMasterFiles, year: parseInt(year) });
        });
    }

    if (selectEnrichFilesBtn) selectEnrichFilesBtn.addEventListener('click', async () => { const files = await window.electronAPI.selectFile({ title: 'Selecione Arquivos para Enriquecer', multi: true }); if (!files?.length) return; window.electronAPI.prepareEnrichmentFiles(files); enrichmentEnrichFiles = []; selectedEnrichFilesDiv.innerHTML = ''; enrichmentProgressContainer.innerHTML = ''; files.forEach(file => { const id = `enrich-${enrichmentEnrichFiles.length}`; enrichmentEnrichFiles.push({ path: file, id }); appendEnrichmentLog(`Adicionado para enriquecimento: ${file}`); addFileToUI(selectedEnrichFilesDiv, file, false); enrichmentProgressContainer.innerHTML += `<div class="file-progress" style="margin-bottom: 15px;"><div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 4px;"><strong>${getBasename(file)}</strong><span id="eta-${id}" style="font-size: 12px; color: var(--text-secondary);"></span></div><div class="progress-bar-container"><div class="progress-bar-fill" id="${id}"></div></div></div>`; }); });

    if (startEnrichmentBtn) {
        startEnrichmentBtn.addEventListener('click', async () => { // MODIFICADO para async
            if (enrichmentEnrichFiles.length === 0) {
                return appendEnrichmentLog('❌ ERRO: Selecione pelo menos um arquivo para enriquecer.');
            }
            const useAllDb = document.getElementById('enrichment-all-db-checkbox').checked; // NOVO
            const enrichmentYearInput = document.getElementById('enrichment-year-input');
            const year = enrichmentYearInput.value;
            const batchSizeInput = document.getElementById('enrichment-batch-size-input'); // NOVO
            const batchSize = parseInt(batchSizeInput.value, 10); // NOVO

            // MODIFICADO: A validação do ano só é necessária se "Todo Banco" não estiver marcado
            if (!useAllDb && (!year || isNaN(parseInt(year)) || year.length !== 4)) {
                return appendEnrichmentLog('❌ ERRO: Por favor, insira um ano válido para pesquisar no banco.');
            }

            // NOVO: Lógica de confirmação para lotes grandes
            if (batchSize >= 10000) {
                const confirmed = await window.electronAPI.showConfirmDialog({
                    title: 'Confirmação de Lote Grande',
                    message: `Você selecionou um tamanho de lote de ${batchSize.toLocaleString('pt-BR')} registros. Valores altos podem causar lentidão ou travamentos. Deseja continuar?`
                });
                if (!confirmed) {
                    return appendEnrichmentLog('⚠️ Operação cancelada pelo usuário devido ao tamanho do lote.');
                }
            }

            startEnrichmentBtn.disabled = true;
            const strategy = document.querySelector('input[name="enrichStrategy"]:checked').value;
            const backup = document.getElementById('backupCheckbox').checked;
            const usePadrao = document.getElementById('enrichment-padrao-checkbox').checked;

            appendEnrichmentLog(`Iniciando enriquecimento com a estratégia: ${strategy.toUpperCase()}`);
            // MODIFICADO: Envia as novas opções
            window.electronAPI.startEnrichment({ filesToEnrich: enrichmentEnrichFiles, strategy, backup, year: parseInt(year) || null, batchSize: batchSize || null, usePadrao, useAllDb });
        });
    }

    // NOVO: Lógica para desabilitar/habilitar inputs com base na seleção de "Todo Banco"
    const enrichmentAllDbCheckbox = document.getElementById('enrichment-all-db-checkbox');
    const enrichmentYearInput = document.getElementById('enrichment-year-input');
    const enrichmentPadraoCheckbox = document.getElementById('enrichment-padrao-checkbox');

    if (enrichmentAllDbCheckbox) {
        enrichmentAllDbCheckbox.addEventListener('change', () => {
            const isChecked = enrichmentAllDbCheckbox.checked;
            enrichmentYearInput.disabled = isChecked;
            enrichmentPadraoCheckbox.disabled = isChecked;
            
            if (isChecked) {
                enrichmentYearInput.value = ''; // Limpa o valor do ano
                enrichmentPadraoCheckbox.checked = false; // Desmarca a outra opção
            }
        });
    }

    window.electronAPI.onEnrichmentLog((msg) => appendEnrichmentLog(msg));
    window.electronAPI.onEnrichmentProgress(({ id, progress, eta }) => { const bar = document.getElementById(id); if (bar) bar.style.width = `${progress}%`; const etaElement = document.getElementById(`eta-${id}`); if (etaElement) { etaElement.textContent = eta ? `ETA: ${eta}` : ''; if (progress === 100) { etaElement.textContent = 'Concluído!'; } } });
    window.electronAPI.onDbLoadProgress(({ current, total, fileName, cnpjsProcessed }) => { const percent = Math.round((current / total) * 100); dbLoadProgressBarFill.style.width = `${percent}%`; dbLoadProgressPercent.textContent = `${percent}%`; dbLoadProgressText.textContent = `Processando: ${fileName}`; dbLoadProgressStats.textContent = `${cnjsProcessed} CNPJs processados`; });
    window.electronAPI.onDbLoadFinished(() => { if (startLoadToDbBtn) startLoadToDbBtn.disabled = false; updateEnrichedCnpjCount(); setTimeout(() => { dbLoadProgressContainer.style.display = 'none'; }, 3000); dbLoadProgressTitle.textContent = 'Carga Concluída!'; dbLoadProgressBarFill.style.width = '100%'; dbLoadProgressPercent.textContent = '100%'; dbLoadProgressText.textContent = 'Finalizado com sucesso'; });
    window.electronAPI.onEnrichmentFinished(() => { if (startEnrichmentBtn) startEnrichmentBtn.disabled = false; });

    // #################################################################
    // #           NOVA LÓGICA - ABA BLOCKLIST                       #
    // #################################################################
    const blocklistLogDiv = document.getElementById('blocklistLog');
    const refreshBlocklistStatsBtn = document.getElementById('refreshBlocklistStatsBtn');
    const blocklistTotalCountSpan = document.getElementById('blocklist-total-count');
    const blocklistTodayCountSpan = document.getElementById('blocklist-today-count');
    const feedBlocklistFromTabBtn = document.getElementById('feedBlocklistFromTabBtn');
    const selectCsvToSplitBtn = document.getElementById('selectCsvToSplitBtn');
    const csvToSplitPathDiv = document.getElementById('csvToSplitPath');
    const linesPerCsvSplitInput = document.getElementById('linesPerCsvSplit');
    const splitCsvBtn = document.getElementById('splitCsvBtn');
    let csvToSplitFile = null;

    const checkNumbersInput = document.getElementById('check-numbers-input');
    const checkNumbersBtn = document.getElementById('check-numbers-btn');
    const checkNumbersResults = document.getElementById('check-numbers-results');

    // MODIFICADO: Função appendBlocklistLog para melhor formatação
    function appendBlocklistLog(msg) {
        if (!blocklistLogDiv) return;
        if (blocklistLogDiv.textContent.trim() === 'Aguardando início...') {
            blocklistLogDiv.innerHTML = '';
        }
        const lines = msg.split('\n');
        lines.forEach(line => {
            const p = document.createElement('p');
            p.textContent = `> ${line.trim()}`;
            blocklistLogDiv.appendChild(p);
        });
        blocklistLogDiv.scrollTop = blocklistLogDiv.scrollHeight;
    }

    async function updateBlocklistStats() {
        if (!blocklistTotalCountSpan || !blocklistTodayCountSpan) return;
        appendBlocklistLog('Atualizando estatísticas da blocklist...');
        blocklistTotalCountSpan.textContent = 'Carregando...';
        blocklistTodayCountSpan.textContent = 'Carregando...';
        const result = await window.electronAPI.getBlocklistStats();
        if (result.success) {
            blocklistTotalCountSpan.textContent = result.data.total.toLocaleString('pt-BR');
            blocklistTodayCountSpan.textContent = result.data.addedToday.toLocaleString('pt-BR');
            appendBlocklistLog('Estatísticas atualizadas com sucesso.');
        } else {
            blocklistTotalCountSpan.textContent = 'Erro';
            blocklistTodayCountSpan.textContent = 'Erro';
            appendBlocklistLog(`❌ Erro ao buscar estatísticas: ${result.message}`);
        }
    }

    if (refreshBlocklistStatsBtn) {
        refreshBlocklistStatsBtn.addEventListener('click', updateBlocklistStats);
    }

    if (feedBlocklistFromTabBtn) {
        feedBlocklistFromTabBtn.addEventListener('click', async () => {
            appendBlocklistLog('Selecionando arquivos para alimentar a Blocklist...');
            const files = await window.electronAPI.selectFile({ title: 'Selecione planilhas com telefones', multi: true });
            if (!files || files.length === 0) return appendBlocklistLog('Nenhum arquivo selecionado.');
            appendBlocklistLog(`Iniciando alimentação com ${files.length} arquivo(s).`);
            const sendEmail = document.getElementById('sendBlocklistEmailCheckbox').checked;
            window.electronAPI.feedBlocklist({ filePaths: files, sendEmail }); // MODIFICADO: Envia o objeto com a opção de e-mail
        });
    }

    if (selectCsvToSplitBtn) {
        selectCsvToSplitBtn.addEventListener('click', async () => {
            const files = await window.electronAPI.selectFile({ title: 'Selecione o arquivo CSV para dividir', multi: false, filters: [{ name: "CSV", extensions: ["csv"] }] });
            if (files && files.length > 0) { csvToSplitFile = files[0]; addFileToUI(csvToSplitPathDiv, csvToSplitFile, true); appendBlocklistLog(`Arquivo para divisão selecionado: ${csvToSplitFile}`); }
        });
    }

    if (splitCsvBtn) { splitCsvBtn.addEventListener('click', () => { const linesPerSplit = parseInt(linesPerCsvSplitInput.value, 10); if (!csvToSplitFile) return appendBlocklistLog('❌ ERRO: Selecione um arquivo CSV para dividir.'); if (!linesPerSplit || linesPerSplit <= 0) return appendBlocklistLog('❌ ERRO: Insira um número de linhas válido e maior que zero.'); window.electronAPI.splitLargeCsv({ filePath: csvToSplitFile, linesPerSplit }); }); }
    window.electronAPI.onBlocklistLog((msg) => appendBlocklistLog(msg));

    // NOVO: Lógica para verificação de números na blocklist
    if (checkNumbersBtn) {
        checkNumbersBtn.addEventListener('click', async () => {
            const rawInput = checkNumbersInput.value.trim();
            if (!rawInput) {
                appendBlocklistLog('⚠️ Por favor, insira um ou mais números para verificar.');
                return;
            }

            const numbers = rawInput
                .split(/[,;\s\n]+/) // Divide por vírgula, ponto e vírgula, espaços ou quebras de linha
                .map(num => num.replace(/\D/g, '').trim()) // Remove não-dígitos e espaços
                .filter(num => num.length >= 8); // Filtra números válidos

            if (numbers.length === 0) {
                appendBlocklistLog('⚠️ Nenhum número válido encontrado na entrada.');
                return;
            }

            appendBlocklistLog(`Verificando ${numbers.length} número(s) na blocklist...`);
            checkNumbersBtn.disabled = true;
            checkNumbersResults.innerHTML = '<p>Verificando...</p>';

            const result = await window.electronAPI.checkBlocklistNumbers(numbers);

            if (result.success) {
                const { found, notFound } = result.data;
                let resultHTML = '';
                if (found.length > 0) {
                    // MODIFICADO: Agora itera sobre objetos e exibe a data de adição.
                    const foundItems = found.map(item => 
                        `<li>${item.telefone} <span style="color: var(--text-muted); font-size: 11px;">(Adicionado em: ${item.data_adicao})</span></li>`
                    ).join('');
                    resultHTML += `<h4>Encontrados na Blocklist (${found.length}):</h4><ul class="result-list found">${foundItems}</ul>`;
                }
                if (notFound.length > 0) {
                    resultHTML += `<h4>Não Encontrados (${notFound.length}):</h4><ul class="result-list not-found"><li>${notFound.join('</li><li>')}</li></ul>`;
                }
                checkNumbersResults.innerHTML = resultHTML || '<p>Nenhum resultado para exibir.</p>';
                appendBlocklistLog(`Verificação concluída. Encontrados: ${found.length}, Não encontrados: ${notFound.length}.`);
            } else {
                checkNumbersResults.innerHTML = `<p class="error">Erro: ${result.message}</p>`;
                appendBlocklistLog(`❌ Erro na verificação: ${result.message}`);
            }
            checkNumbersBtn.disabled = false;
        });
    }

    // #################################################################
    // #           LÓGICA PARA A ABA DE MONITORAMENTO (ATUALIZADA)     #
    // #################################################################

    let lastSuspiciousCalls = [];
    const SUSPICIOUS_TABULATIONS = ['MUDO/ENCERRAR [43]', 'MUDO [33]'];
    const SUSPICIOUS_DURATION_SECONDS = 180;
    const getDurationInSeconds = (timeString) => { if (!timeString || typeof timeString !== 'string') return 0; const parts = timeString.split(':'); if (parts.length === 3) { return parseInt(parts[0], 10) * 3600 + parseInt(parts[1], 10) * 60 + parseInt(parts[2], 10); } return 0; };
    const selectionModal = document.getElementById('selection-modal');
    const modalTitle = document.getElementById('modal-title');
    const modalCloseBtn = selectionModal.querySelector('.modal-close-btn');
    const modalSearchInput = document.getElementById('modal-search-input');
    const modalListContainer = document.getElementById('modal-list-container');
    let currentModalContext = null;
    const operadores = [{ id: '143', name: 'Adriene Rodrigues' }, { id: '58', name: 'Ana Carolina' }, { id: '46', name: 'Ana Clara Lopes' }, { id: '105', name: 'Ana Maia' }, { id: '117', name: 'Ana Rovere' }, { id: '40', name: 'Anna Barbosa' }, { id: '156', name: 'Arthur Medeiros' }, { id: '112', name: 'Beatriz Martins' }, { id: '92', name: 'Bianca Antunes' }, { id: '179', name: 'Brenda Ewald' }, { id: '202', name: 'Bruna Lobato' }, { id: '144', name: 'Cairo Motta' }, { id: '126', name: 'Camila Nogueira' }, { id: '205', name: 'Daiany Porto' }, { id: '115', name: 'Daniel Neves' }, { id: '174', name: 'Diana Viana' }, { id: '138', name: 'Douglas Reis' }, { id: '77', name: 'Erik Freitas' }, { id: '64', name: 'Felipe Martins' }, { id: '150', name: 'Fernanda Novaes' }, { id: '94', name: 'Gabriela Pitzer' }, { id: '189', name: 'Giselle Mota' }, { id: '108', name: 'Giselly Salles' }, { id: '192', name: 'Graça Vitória' }, { id: '139', name: 'Guilherme Maudonet' }, { id: '104', name: 'Heloisa Bispo' }, { id: '146', name: 'Ian Branco' }, { id: '55', name: 'Jennyfer Vieira' }, { id: '111', name: 'Jessica Oliveira' }, { id: '109', name: 'João Honorato' }, { id: '195', name: 'Joao Soares' }, { id: '78', name: 'Joyce Menezes' }, { id: '132', name: 'Juliana Oliveira' }, { id: '122', name: 'Karolina Silva' }, { id: '147', name: 'Kauã Oliveira' }, { id: '194', name: 'Kawan Gabriel' }, { id: '57', name: 'Larissa Moroni' }, { id: '71', name: 'Larissa Oliveira' }, { id: '135', name: 'Lohana Soares' }, { id: '68', name: 'Luana Alves' }, { id: '136', name: 'Luana Ribeiro' }, { id: '178', name: 'Manuela Giraldes' }, { id: '126', name: 'Marcos Vinicius' }, { id: '133', name: 'Maria Cristina' }, { id: '182', name: 'Maria Luna' }, { id: '175', name: 'Maria Martins' }, { id: '103', name: 'Mariana Oliveira' }, { id: '104', name: 'Maria Seixas' }, { id: '171', name: 'Maria Sotelino' }, { id: '127', name: 'Matheus Ribeiro' }, { id: '180', name: 'Mauricio Freitas' }, { id: '173', name: 'Mirella Lira' }, { id: '116', name: 'Nicolle Santos' }, { id: '129', name: 'Paula Santos' }, { id: '61', name: 'Ramon Gonçalves' }, { id: '34', name: 'Raphael Machado' }, { id: '37', name: 'Renata Souza' }, { id: '193', name: 'Ricardo França' }, { id: '30', name: 'Rodrigo Santana' }, { id: '128', name: 'Samara Gomes' }, { id: '96', name: 'Samella Figueira' }, { id: '98', name: 'Sarah Leite' }, { id: '157', name: 'Thais Maciel' }, { id: '74', name: 'Thays Florencio' }, { id: '93', name: 'Vanessa Barros' }, { id: '42', name: 'Vanessa dos Santos' }, { id: '177', name: 'Victor Alves' }, { id: '204', name: 'Vitor Faria' }, { id: '149', name: 'Vivian Ferreira' }, { id: '190', name: 'Vivian Simplicio' }, { id: '134', name: 'Wanessa Fernandes' }];
    const servicos = [
        { id: '224', name: '[C6 BANK] - ESTEIRA DIGITAL PRINCIPAL', category: 'DIGITAL' }, { id: '209', name: 'ESTEIRA DIGITAL LENTO', category: 'DIGITAL' },
        { id: '132', name: '[C6 BANK] - MAQUININHA MP', category: 'MERCADO PAGO' }, { id: '209', name: 'ESTEIRA DIGITAL LENTO', category: 'DIGITAL' },
        { id: '117', name: '[C6 BANK] - RELACIONAMENTO MELISSA', category: 'DIGITAL' }, { id: '34', name: 'LEMBRETE ABERTURA DE CONTA', category: 'DIGITAL' },
        { id: '159', name: '[C6 BANK] - EQUIPE BRUNA', category: 'ABERTURA' }, { id: '235', name: '[C6 BANK] - EQUIPE CAMILA', category: 'ABERTURA' },
        { id: '160', name: '[C6 BANK] - EQUIPE LAIANE', category: 'ABERTURA' }, { id: '233', name: '[C6 BANK] - EQUIPE TEF', category: 'ABERTURA' },
        { id: '161', name: '[C6 BANK] - EQUIPE WALESKA', category: 'ABERTURA' }, { id: '194', name: '[C6 BANK] -pt2 NOVO TRANSBORDO', category: 'ABERTURA' },
        { id: '124', name: '[C6 BANK] - CADÊNCIA GERAL', category: 'ABERTURA' }, { id: '181', name: '[C6/MB] C6 Pay Relacionamento ANTONIO', category: 'Relacionamento' },
        { id: '232', name: '[C6/MB] C6 Pay Relacionamento JOAO AVILA', category: 'Relacionamento' }, { id: '180', name: '[C6/MB] C6 Pay Relacionamento RAPHAELA CALDERON', category: 'Relacionamento' },
        { id: '168', name: 'Relacionamento Ana Clara', category: 'Relacionamento' }, { id: '227', name: 'Relacionamento Anna Barbosa', category: 'Relacionamento' },
        { id: '154', name: 'Relacionamento Antonio Costa', category: 'Relacionamento' }, { id: '169', name: 'Relacionamento Cairo Motta', category: 'Relacionamento' },
        { id: '203', name: 'Relacionamento Diana Viana', category: 'Relacionamento' }, { id: '186', name: 'Relacionamento digite1', category: 'Relacionamento' },
        { id: '202', name: 'Relacionamento Douglas Reis', category: 'Relacionamento' }, { id: '176', name: 'Relacionamento Fernanda Novaes', category: 'Relacionamento' },
        { id: '171', name: 'Relacionamento Guilherme Maudonet', category: 'Relacionamento' }, { id: '155', name: 'Relacionamento Higor Campos', category: 'Relacionamento' },
        { id: '229', name: 'Relacionamento Jennyfer Vieira', category: 'Relacionamento' }, { id: '172', name: 'Relacionamento Jessica Oliveira', category: 'Relacionamento' },
        { id: '148', name: 'Relacionamento João Avila', category: 'Relacionamento' }, { id: '201', name: 'Relacionamento João Honorato', category: 'Relacionamento' },
        { id: '150', name: 'Relacionamento Juliana Oliveira', category: 'Relacionamento' }, { id: '146', name: 'Relacionamento Karolina', category: 'Relacionamento' },
        { id: '228', name: 'Relacionamento Larissa Oliveira', category: 'Relacionamento' }, { id: '158', name: 'Relacionamento Luana Ribeiro', category: 'Relacionamento' },
        { id: '174', name: 'Relacionamento Marcos Vinicius', category: 'Relacionamento' }, { id: '188', name: 'Relacionamento Maria Cristina', category: 'Relacionamento' },
        { id: '225', name: 'Relacionamento Maria Seixas', category: 'Relacionamento' }, { id: '151', name: 'Relacionamento Matheus Ribeiro', category: 'Relacionamento' },
        { id: '153', name: 'Relacionamento Paula Santos', category: 'Relacionamento' }, { id: '193', name: 'Relacionamento Raphaela Calderon', category: 'Relacionamento' },
        { id: '177', name: 'Relacionamento Raphael Machado', category: 'Relacionamento' }, { id: '173', name: 'Relacionamento Renata Souza', category: 'Relacionamento' },
        { id: '170', name: 'Relacionamento Ricardo França', category: 'Relacionamento' }, { id: '226', name: 'Relacionamento Roberto Bianna', category: 'Relacionamento' },
        { id: '231', name: 'Relacionamento Rodrigo Santana', category: 'Relacionamento' }];
    const gruposOperador = [
        { id: '85', name: 'Equipe Bruna' }, { id: '120', name: 'Equipe Camila' }, { id: '123', name: 'Equipe Laiane' },
        { id: '146', name: 'Equipe Ricardo' }, { id: '87', name: 'Equipe Waleska' }, { id: '106', name: 'Equipe Mayko' }, { id: '133', name: 'Equipe Joao Avila' }
    ];
    const tabulacoes = [{ id: '96', name: 'CHAMAR NO WHATSAPP MAQUINA' }, { id: '80', name: 'Confirmação' }, { id: '82', name: 'Conta Ativa' }, { id: '47', name: 'Inapto' }, { id: '33', name: 'MUDO' }, { id: '43', name: 'MUDO/ENCERRAR' }, { id: '95', name: 'Maquina vendida' }, { id: '79', name: 'Promessa' }, { id: '83', name: 'Relacionamento' }, { id: '81', name: 'Retorno' }, { id: '44', name: 'CHAMAR NO WHATSAPP' }, { id: '34', name: 'CLIENTE ABRIU A CONTA' }, { id: '38', name: 'CLIENTE JÁ POSSUI CONTA' }, { id: '69', name: 'Inapto na receita federal' }, { id: '84', name: 'MEI' }, { id: '39', name: 'NÃO TEM INTERESSE' }, { id: '52', name: 'NÃO É O RESPONSAVEL' }, { id: '37', name: 'OUTRO ECE' }, { id: '42', name: 'PROBLEMA NO APLICATIVO' }, { id: '67', name: 'RECUSADA PELO BANCO' }, { id: '41', name: 'REDISCAR CLIENTE/ CAIU A LIGAÇÃO' }, { id: '40', name: 'TELEFONE INCORRETO' }, { id: '36', name: 'BLOCKLIST' }].filter((v, i, a) => a.findIndex(t => (t.id === v.id)) === i);



// Adicione esta função em renderer.js
function renderComparisonList(fastwayData, bitrixData) {
    if (!fastwayData || !fastwayData.length) {
        console.log('Dados do Fastway não disponíveis para comparação.');
        return;
    }

    const bitrixMap = new Map(bitrixData.map(item => [item.nome, item.total]));
    
    let htmlContent = '<ul class="comparison-list">';

    fastwayData.forEach(fastwayItem => {
        const fastwayCount = fastwayItem.total;
        const bitrixCount = bitrixMap.get(fastwayItem.nome) || 0;
        
        htmlContent += `
            <li>
                <div class="operator-name">${fastwayItem.nome}</div>
                <div class="call-counts">
                    <span class="fastway-count">${fastwayCount}</span>
                    <span class="separator">/</span>
                    <span class="bitrix-count">${bitrixCount}</span>
                </div>
            </li>
        `;
    });

    htmlContent += '</ul>';

    const comparisonBox = document.getElementById('comparison-box');
    if (comparisonBox) {
        comparisonBox.innerHTML = htmlContent;
    } else {
        console.error('Elemento com ID "comparison-box" não encontrado para renderizar a lista.');
    }
}



    function renderModalList(items, searchTerm = '') {
        const lowerSearchTerm = searchTerm.toLowerCase();
        const filteredItems = items.filter(item => item.name.toLowerCase().includes(lowerSearchTerm));
        let html = '<ul class="custom-select-list">';
        if (currentModalContext.type === 'servico') {
            const grouped = filteredItems.reduce((acc, servico) => {
                const category = servico.category || 'Outros';
                (acc[category] = acc[category] || []).push(servico);
                return acc;
            }, {});
            const categoryOrder = ['ABERTURA', 'DIGITAL', 'MERCADO PAGO', 'Relacionamento', 'Outros'];
            for (const category of categoryOrder) {
                if (grouped[category] && grouped[category].length > 0) {
                    html += `<li class="group-header">${category}</li>`;
                    html += grouped[category].map(s => `<li data-id="${s.id}" data-name="${s.name}">${s.name}</li>`).join('');
                }
            }
        } else {
            html += filteredItems.map(item => `<li data-id="${item.id}" data-name="${item.name}">${item.name}</li>`).join('');
        }
        html += '</ul>';
        modalListContainer.innerHTML = html;
    }

    function openSelectionModal(context) {
        currentModalContext = context;
        modalTitle.textContent = context.title;
        modalSearchInput.value = '';
        renderModalList(context.data, '');
        selectionModal.classList.remove('hidden');
        modalSearchInput.focus();
    }

    function closeSelectionModal() {
        selectionModal.classList.add('hidden');
        currentModalContext = null;
    }

    if (selectionModal) {
        modalCloseBtn.addEventListener('click', closeSelectionModal);
        selectionModal.addEventListener('click', e => { if (e.target === selectionModal) closeSelectionModal(); });
        modalSearchInput.addEventListener('input', () => { if (currentModalContext) { renderModalList(currentModalContext.data, modalSearchInput.value); } });
        modalListContainer.addEventListener('click', e => { const target = e.target; if (target && target.tagName === 'LI' && !target.classList.contains('group-header')) { const { id, name } = target.dataset; currentModalContext.searchEl.value = name; currentModalContext.hiddenInputEl.value = id; closeSelectionModal(); } });
    }

    const multiSelectionModal = document.getElementById('multi-selection-modal');
    const multiModalTitle = document.getElementById('multi-modal-title');
    const multiModalCloseBtn = multiSelectionModal.querySelector('.modal-close-btn');
    const multiModalSearchInput = document.getElementById('multi-modal-search-input');
    const multiModalListContainer = document.getElementById('multi-modal-list-container');
    const multiModalConfirmBtn = document.getElementById('multi-modal-confirm-btn');
    const multiModalCancelBtn = document.getElementById('multi-modal-cancel-btn');
    let multiSelectContext = null;

    function renderMultiSelectModalList(items, searchTerm = '') {
        const lowerSearchTerm = searchTerm.toLowerCase();
        const filteredItems = items.filter(item => item.name.toLowerCase().includes(lowerSearchTerm));
        const currentSelection = multiSelectContext.hiddenInputEl.value.split(',').filter(Boolean);
        const listHtml = filteredItems.map(item => `<li><input type="checkbox" id="multi-check-${item.id}" data-id="${item.id}" ${currentSelection.includes(item.id) ? 'checked' : ''}><label for="multi-check-${item.id}">${item.name} [${item.id}]</label></li>`).join('');
        multiModalListContainer.innerHTML = `<ul class="modal-list-multi">${listHtml}</ul>`;
    }

    function openMultiSelectionModal(context) {
        multiSelectContext = context;
        multiModalTitle.textContent = context.title;
        multiModalSearchInput.value = '';
        renderMultiSelectModalList(context.data, '');
        multiSelectionModal.classList.remove('hidden');
        multiModalSearchInput.focus();
    }

    function closeMultiSelectionModal() {
        multiSelectionModal.classList.add('hidden');
        multiSelectContext = null;
    }

    if (multiSelectionModal) {
        multiModalCloseBtn.addEventListener('click', closeMultiSelectionModal);
        multiModalCancelBtn.addEventListener('click', closeMultiSelectionModal);
        multiSelectionModal.addEventListener('click', e => { if (e.target === multiSelectionModal) closeMultiSelectionModal(); });
        multiModalSearchInput.addEventListener('input', () => { if (multiSelectContext) { renderMultiSelectModalList(multiSelectContext.data, multiModalSearchInput.value); } });
        multiModalConfirmBtn.addEventListener('click', () => { if (!multiSelectContext) return; const selectedIds = []; multiModalListContainer.querySelectorAll('input[type="checkbox"]:checked').forEach(checkbox => { selectedIds.push(checkbox.dataset.id); }); multiSelectContext.hiddenInputEl.value = selectedIds.join(','); multiSelectContext.displayEl.textContent = selectedIds.length > 0 ? `${selectedIds.length} selecionada(s)` : 'Selecionar Tabulações...'; closeMultiSelectionModal(); });
    }

    const suspiciousCallsModal = document.getElementById('suspicious-calls-modal');
    const suspiciousCallsList = document.getElementById('suspicious-calls-list');
    const suspiciousCallsCloseBtn = suspiciousCallsModal.querySelector('.modal-close-btn');

    function showSuspiciousCallsModal() {
        let tableHTML = `<table class="modal-table"><thead><tr><th>Operador</th><th>CNPJ Cliente</th><th>Duração</th><th>Tabulação</th></tr></thead><tbody>`;
        if (lastSuspiciousCalls.length > 0) {
            lastSuspiciousCalls.forEach(call => { tableHTML += `<tr><td>${call.nome_operador || 'N/A'}</td><td>${call.cpf || 'N/A'}</td><td>${call.tempo_ligacao || '00:00:00'}</td><td>${call.tabulacao || 'N/A'}</td></tr>`; });
        } else {
            tableHTML += '<tr><td colspan="4" style="text-align:center;">Nenhuma chamada suspeita encontrada.</td></tr>';
        }
        tableHTML += '</tbody></table>';
        suspiciousCallsList.innerHTML = tableHTML;
        suspiciousCallsModal.classList.remove('hidden');
    }

    function closeSuspiciousCallsModal() {
        suspiciousCallsModal.classList.add('hidden');
    }

    if (suspiciousCallsCloseBtn) suspiciousCallsCloseBtn.addEventListener('click', closeSuspiciousCallsModal);
    if (suspiciousCallsModal) suspiciousCallsModal.addEventListener('click', (e) => { if (e.target === suspiciousCallsModal) closeSuspiciousCallsModal(); });

    const apiParametersContainer = document.getElementById('api-parameters');
    const generateReportBtn = document.getElementById('generateReportBtn');
    const monitoringLog = document.getElementById('monitoring-log');
    const dashboardSummary = document.getElementById('dashboard-summary');
    const dashboardDetails = document.getElementById('dashboard-details');
    const bitrixDetailsContainer = document.getElementById('bitrix-details-container');
    const dataInicioInput = document.getElementById('data_inicio_monitor');
    const dataFimInput = document.getElementById('data_fim_monitor');
    const monitoringSearchInput = document.getElementById('monitoringSearchInput');
    const dateFilterMenu = document.getElementById('date-filter-menu');
    const operatorTimesContainer = document.getElementById('operator-times-container');
    const operatorTimesTableWrapper = document.getElementById('operator-times-table-wrapper');
    const summaryToggleBar = document.getElementById('summary-toggle-bar');
    const showFastwaySummaryBtn = document.getElementById('showFastwaySummary');
    const showBitrixSummaryBtn = document.getElementById('showBitrixSummary');
    const monitoringTab = document.getElementById('monitoramento');

    function hasActiveFilter() {
        if (monitoringSearchInput && monitoringSearchInput.value.trim() !== '') { return true; }
        const advancedFilterCheckboxes = document.querySelectorAll('#api-parameters input[type="checkbox"]');
        for (const checkbox of advancedFilterCheckboxes) { if (checkbox.checked) { return true; } }
        return false; 
    }

    const apiParams = [
        { name: 'id', label: 'Call ID' }, { name: 'nome', label: 'Nome Cliente' }, { name: 'chave', label: 'Chave' },
        { name: 'cpf', label: 'CPF' }, { name: 'operador_id', label: 'Operador' }, { name: 'fone_origem', label: 'Fone Origem' },
        { name: 'fone_destino', label: 'Telefone (sem 55)' }, { name: 'sentido', label: 'Sentido' }, { name: 'tronco_id', label: 'ID Tronco' },
        { name: 'digitado', label: 'Digitado' }, { name: 'resultado', label: 'Resultado' }, { name: 'tabulacao_id', label: 'Tabulações' },
        { name: 'operacao_id', label: 'ID Operação' }, { name: 'tipoServico', label: 'Tipo Serviço' }, { name: 'servico_id', label: 'Desempenho de campanha' },
        { name: 'grupo_operador_id', label: 'Desempenho de equipes' },
    ];
    
    function renderApiFilters(role, teamId) {
        const isAdmin = role === 'admin';
        const isMaster = role === 'master';
        apiParametersContainer.innerHTML = '';
        const filtersToHideForLimited = ['chave', 'fone_origem', 'sentido', 'tronco_id', 'resultado', 'operacao_id', 'tipoServico', 'servico_id','cpf','nome'];
        const visibleParams = (isAdmin || isMaster) ? apiParams : apiParams.filter(p => !filtersToHideForLimited.includes(p.name));
        visibleParams.forEach(param => {
            const paramItem = document.createElement('div');
            paramItem.className = 'param-item';
            let inputHtml = '';
            if (param.name === 'grupo_operador_id' && role === 'limited' && teamId) {
                const team = gruposOperador.find(g => g.id === teamId);
                const teamName = team ? team.name : `Equipe ID ${teamId}`;
                const displayInput = `<input type="text" value="${teamName}" readonly disabled style="cursor: not-allowed; background-color: var(--bg-dark);">`;
                const hiddenInput = `<input type="hidden" id="input-grupo_operador_id" value="${teamId}">`;
                inputHtml = `<div class="custom-select-container">${displayInput}${hiddenInput}</div>`;
                const toggleHtml = `<div class="toggle-switch"><label class="switch"><input type="checkbox" id="check-grupo_operador_id" checked disabled><span class="slider"></span></label><span class="toggle-label">${param.label}</span></div>`;
                paramItem.innerHTML = toggleHtml + inputHtml;
                apiParametersContainer.appendChild(paramItem);
            } else {
                const isSelectable = ['operador_id', 'servico_id', 'grupo_operador_id'].includes(param.name);
                const containerId = `input-container-${param.name}`;
                if (param.name === 'tabulacao_id') {
                    inputHtml = `<div id="${containerId}" class="multi-select-container"><button class="multi-select-button" id="tabulacao-multi-select-btn">Selecionar Tabulações...</button><input type="hidden" id="input-${param.name}"></div>`;
                } else if (isSelectable) {
                    inputHtml = `<div id="${containerId}" class="custom-select-container"><input type="text" id="${param.name}-search" readonly placeholder="Clique para selecionar..." style="cursor: pointer;"><input type="hidden" id="input-${param.name}"></div>`;
                } else {
                    inputHtml = `<input type="text" id="${containerId}" placeholder="Valor...">`;
                }
                const toggleHtml = `<div class="toggle-switch"><label class="switch"><input type="checkbox" id="check-${param.name}" data-param-name="${param.name}"><span class="slider"></span></label><span class="toggle-label">${param.label}</span></div>`;
                paramItem.innerHTML = toggleHtml + inputHtml;
                apiParametersContainer.appendChild(paramItem);
                const checkbox = document.getElementById(`check-${param.name}`);
                const inputContainer = document.getElementById(containerId);
                if (inputContainer) { inputContainer.classList.add('hidden'); }
                if (checkbox) {
                    checkbox.addEventListener('change', () => {
                        inputContainer.classList.toggle('hidden', !checkbox.checked);
                        if (!checkbox.checked) {
                            const searchInput = document.getElementById(`${param.name}-search`);
                            if (searchInput) searchInput.value = '';
                            const hiddenInput = document.getElementById(`input-${param.name}`);
                            if (hiddenInput) hiddenInput.value = '';
                            if (inputContainer.tagName === 'INPUT') inputContainer.value = '';
                            if (param.name === 'tabulacao_id') document.getElementById('tabulacao-multi-select-btn').textContent = 'Selecionar Tabulações...';
                        }
                    });
                }
                if (param.name === 'tabulacao_id') {
                    document.getElementById('tabulacao-multi-select-btn').addEventListener('click', () => openMultiSelectionModal({ title: 'Selecionar Tabulações', data: tabulacoes, hiddenInputEl: document.getElementById('input-tabulacao_id'), displayEl: document.getElementById('tabulacao-multi-select-btn') }));
                } else if (isSelectable) {
                    document.getElementById(`${param.name}-search`).addEventListener('click', () => {
                        if (!document.getElementById(`check-${param.name}`).checked) return;
                        let data, type;
                        if (param.name === 'operador_id') { data = operadores; type = 'operador'; }
                        else if (param.name === 'servico_id') { data = servicos; type = 'servico'; }
                        else { data = gruposOperador; type = 'grupo_operador'; }
                        openSelectionModal({ type, title: `Selecionar ${param.label}`, data, searchEl: document.getElementById(`${param.name}-search`), hiddenInputEl: document.getElementById(`input-${param.name}`) });
                    });
                }
            }
        });
        if (monitoringSearchInput) {
            const foneDestinoCheckbox = document.getElementById('check-fone_destino');
            const foneDestinoInput = document.getElementById('input-container-fone_destino');
            if (foneDestinoCheckbox && foneDestinoInput) {
                monitoringSearchInput.addEventListener('input', () => {
                    const searchTerm = monitoringSearchInput.value.trim();
                    foneDestinoInput.value = searchTerm;
                    const hasSearchTerm = searchTerm !== '';
                    foneDestinoCheckbox.checked = hasSearchTerm;
                    foneDestinoInput.classList.toggle('hidden', !hasSearchTerm);
                });
            }
        }
    }

    const getHtmlDate = (date) => { const year = date.getFullYear(); const month = String(date.getMonth() + 1).padStart(2, '0'); const day = String(date.getDate()).padStart(2, '0'); return `${year}-${month}-${day}`; }
    const getApiDate = (dateString) => { if (!dateString) return ''; const [year, month, day] = dateString.split('-'); return `${day}/${month}/${year}`; }
    if (dateFilterMenu) { dateFilterMenu.addEventListener('click', (e) => { if (e.target.tagName === 'LI') { const period = e.target.dataset.period; const today = new Date(); let startDate, endDate = new Date(); switch (period) { case 'today': startDate = today; endDate = today; break; case 'yesterday': startDate = new Date(today); startDate.setDate(today.getDate() - 1); endDate = startDate; break; case 'this_week': startDate = new Date(today); const dayOfWeek = today.getDay(); startDate.setDate(today.getDate() - dayOfWeek + (dayOfWeek === 0 ? -6 : 1)); endDate = today; break; case 'last_week': startDate = new Date(today); startDate.setDate(today.getDate() - today.getDay() - 6); endDate = new Date(startDate); endDate.setDate(startDate.getDate() + 6); break; case 'this_month': startDate = new Date(today.getFullYear(), today.getMonth(), 1); endDate = today; break; } if (dataInicioInput) dataInicioInput.value = getHtmlDate(startDate); if (dataFimInput) dataFimInput.value = getHtmlDate(endDate); e.target.closest('details').removeAttribute('open'); } }); }

    function formatSeconds(totalSeconds) {
        if (isNaN(totalSeconds) || totalSeconds < 0) { return "00:00:00"; }
        const hours = Math.floor(totalSeconds / 3600);
        const minutes = Math.floor((totalSeconds % 3600) / 60);
        const seconds = Math.floor(totalSeconds % 60);
        return `${String(hours).padStart(2, '0')}:${String(minutes).padStart(2, '0')}:${String(seconds).padStart(2, '0')}`;
    }

    function formatTMA(totalSeconds) {
        if (isNaN(totalSeconds) || totalSeconds < 0) { return "00:00"; }
        const minutes = Math.floor(totalSeconds / 60);
        const seconds = Math.floor(totalSeconds % 60);
        return `${String(minutes).padStart(2, '0')}:${String(seconds).padStart(2, '0')}`;
    }

    function normalizeName(name) {
        if (!name) return "";
        const normalized = name.toString().toLowerCase().normalize("NFD").replace(/[\u0300-\u036f]/g, "").replace(/[^\w\s]/g, '').trim();
        const parts = normalized.split(/\s+/);
        if (parts.length > 1) { return `${parts[0]} ${parts[parts.length - 1]}`; }
        return parts[0] || "";
    }
    
    function renderSummaryCards(source) {
        const card1 = document.getElementById('summary-card-1'), title1 = document.getElementById('summary-title-1'), value1 = document.getElementById('summary-value-1');
        const card2 = document.getElementById('summary-card-2'), title2 = document.getElementById('summary-title-2'), value2 = document.getElementById('summary-value-2');
        const card3 = document.getElementById('summary-card-3'), value3 = document.getElementById('summary-value-3');
        const card4 = document.getElementById('summary-card-4'), title4 = document.getElementById('summary-title-4'), value4 = document.getElementById('summary-value-4');

        if (source === 'bitrix' && bitrixSummaryData) {
            card1.style.display = 'block'; title1.textContent = 'Chamadas da Equipe (Bitrix)'; value1.textContent = (bitrixSummaryData.totalCalls || 0).toLocaleString('pt-BR');
            card2.style.display = 'block'; title2.textContent = 'TMA da Equipe (Bitrix)'; value2.textContent = formatTMA(bitrixSummaryData.generalTma);
            card4.style.display = 'block'; title4.textContent = 'Tempo Falado (Bitrix)'; value4.textContent = formatSeconds(bitrixSummaryData.totalDuration);
            card3.style.display = 'none';
        } else if (source === 'fastway' && fastwaySummaryData) {
            card1.style.display = 'block'; title1.textContent = 'Total de Chamadas (Fastway)'; value1.textContent = (fastwaySummaryData.totalCalls || 0).toLocaleString('pt-BR');
            card2.style.display = 'block'; title2.textContent = 'TMA (Fastway)'; value2.textContent = fastwaySummaryData.tma || '00:00';
            card3.style.display = 'block'; value3.textContent = fastwaySummaryData.suspiciousCount || 0; card3.disabled = fastwaySummaryData.suspiciousCount === 0;
            card4.style.display = 'block'; title4.textContent = 'Operadores Envolvidos'; value4.textContent = fastwaySummaryData.operatorCount || 0;
        } else {
            dashboardSummary.innerHTML = '<p style="color: var(--text-muted); text-align: center; grid-column: 1 / -1;">Nenhum dado de resumo para exibir.</p>';
        }
    }

    // --- LÓGICA DE TROCA DE VIEWS ---
    if(showFastwaySummaryBtn && showBitrixSummaryBtn) {
        showFastwaySummaryBtn.addEventListener('click', () => {
            monitoringTab.classList.remove('bitrix-view-active');
            renderSummaryCards('fastway');
            renderFastwayDetails();
            showFastwaySummaryBtn.classList.add('active');
            showBitrixSummaryBtn.classList.remove('active');
        });
        showBitrixSummaryBtn.addEventListener('click', () => {
            monitoringTab.classList.add('bitrix-view-active');
            renderSummaryCards('bitrix');
            renderBitrixDetails();
            showBitrixSummaryBtn.classList.add('active');
            showFastwaySummaryBtn.classList.remove('active');
        });
    }

    function processBitrixData(bitrixData, allowedOperatorNames = null) {
        if (!bitrixData || bitrixData.message) {
            bitrixSummaryData = null; bitrixDetailData = null; return;
        }
    
        let finalOperatorStats = bitrixData.operatorStats;
    
        if (currentUserRole === 'limited' && allowedOperatorNames && allowedOperatorNames.size > 0) {
            finalOperatorStats = bitrixData.operatorStats.filter(operator => {
                const normalizedBitrixName = normalizeName(operator.name);
                return Array.from(allowedOperatorNames).some(allowedName => {
                    return normalizedBitrixName.startsWith(allowedName) || allowedName.startsWith(normalizedBitrixName);
                });
            });
        }
    
        const totalCalls = finalOperatorStats.reduce((sum, op) => sum + op.callCount, 0);
        const totalDuration = finalOperatorStats.reduce((sum, op) => sum + op.totalDuration, 0);
        const generalTma = totalCalls > 0 ? totalDuration / totalCalls : 0;
    
        bitrixSummaryData = {
            totalCalls: totalCalls,
            generalTma: generalTma,
            totalDuration: totalDuration
        };
        bitrixDetailData = finalOperatorStats;
    }

    function renderBitrixDetails() {
        if (!bitrixDetailsContainer) return;
        bitrixDetailsContainer.innerHTML = '';
    
        if (!bitrixDetailData) {
            bitrixDetailsContainer.innerHTML = '<p style="color: var(--text-muted); text-align: center;">Nenhum dado do Bitrix para exibir.</p>';
            return;
        }
        
        let content = '<h3>Desempenho por Operador (Bitrix)</h3>';
        if (bitrixDetailData.length === 0) {
            content += '<p style="color: var(--text-muted);">Nenhum operador da sua equipe foi encontrado nos registros do Bitrix para este período.</p>';
        } else {
            const sortedStats = [...bitrixDetailData].sort((a, b) => b.callCount - a.callCount);
            content += '<table class="bitrix-report-table"><thead><tr><th>Operador</th><th>Total de Chamadas</th><th>TMA Individual</th><th>Tempo Falado</th></tr></thead><tbody>';
            sortedStats.forEach(stats => {
                content += `<tr><td>${stats.name}</td><td>${stats.callCount}</td><td>${formatTMA(stats.tma)}</td><td>${formatSeconds(stats.totalDuration)}</td></tr>`;
            });
            content += '</tbody></table>';
        }
        bitrixDetailsContainer.innerHTML = content;
    }




    // --- BOTÃO PRINCIPAL DE GERAR RELATÓRIO ---
    if (generateReportBtn) {
        generateReportBtn.addEventListener('click', async () => {
            if (!hasActiveFilter()) {
                alert('Selecione pelo menos 1 filtro ou preencha o campo de pesquisa.');
                return;
            }

            generateReportBtn.disabled = true;
            monitoringLog.innerHTML = '> 🌀 Gerando relatórios... Por favor, aguarde.';
            dashboardSummary.innerHTML = '';
            dashboardDetails.innerHTML = '';
            if (bitrixDetailsContainer) bitrixDetailsContainer.innerHTML = '';
            operatorTimesContainer.style.display = 'none';
            operatorTimesTableWrapper.innerHTML = '';
            summaryToggleBar.style.display = 'none';
            fastwaySummaryData = null; bitrixSummaryData = null;
            fastwayDetailData = null; bitrixDetailData = null;

            let baseUrl = 'https://mbfinance.fastssl.com.br/api/relatorio/captura_valores_analitico.php?';
            let params = [];
            apiParams.map(p => p.name).forEach(paramName => {
                const checkbox = document.getElementById(`check-${paramName}`);
                let value = '';
                if (checkbox && checkbox.checked) {
                    const input = document.getElementById(`input-${paramName}`) || document.getElementById(`input-container-${paramName}`);
                    if (input) value = input.value;
                }
                params.push(`${paramName}=${encodeURIComponent(value ?? '')}`);
            });
            const dataInicio = getApiDate(dataInicioInput.value);
            const dataFim = getApiDate(dataFimInput.value);
            params.push(`data_inicio=${dataInicio}`);
            params.push(`data_fim=${dataFim}`);
            params.push('formato=json');
            const finalUrl = baseUrl + params.join('&');

            let originalReportPayload = { 
                reportUrl: finalUrl, 
                operatorTimesParams: null,
            };
            if (document.getElementById('check-operador_id')?.checked || document.getElementById('check-grupo_operador_id')?.checked) {
                originalReportPayload.operatorTimesParams = { data_inicio: dataInicio, data_fim: dataFim, operador_id: document.getElementById('input-operador_id')?.value || '', grupo_operador_id: document.getElementById('input-grupo_operador_id')?.value || '' };
            }
            const bitrixPayload = { startDate: dataInicioInput.value, endDate: dataFimInput.value };

            monitoringLog.innerHTML = '> 🌀 Buscando dados do sistema principal (Fastway)...<br>> 🌀 Buscando dados do Bitrix24...';
            
            const [originalResult, bitrixResult] = await Promise.allSettled([
                window.electronAPI.fetchMonitoringReport(originalReportPayload),
                window.electronAPI.fetchBitrixReport(bitrixPayload)
            ]);
            
            let logMessages = [];
            let allowedOperatorNames = null;

            if (originalResult.status === 'fulfilled' && originalResult.value.success) {
                const result = originalResult.value;
                const { data, operatorTimesData } = result;
                let monitoringData = data || [];
                
                if (currentUserRole === 'limited') {
                    if (operatorTimesData) {
                        const rows = operatorTimesData.trim().split('\n');
                        if (rows.length > 1) {
                            allowedOperatorNames = new Set();
                            const headers = rows[0].split(';').map(h => h.trim().toUpperCase());
                            const opIndex = headers.indexOf('OPERADOR');
                            if (opIndex !== -1) {
                                for(let i = 1; i < rows.length; i++) {
                                    const operatorName = rows[i].split(';')[opIndex];
                                    if (operatorName) {
                                        allowedOperatorNames.add(normalizeName(operatorName));
                                    }
                                }
                            }
                        }
                    }
                    if (!allowedOperatorNames || allowedOperatorNames.size === 0) {
                        if (monitoringData && monitoringData.length > 0) {
                            allowedOperatorNames = new Set();
                            monitoringData.forEach(call => {
                                if (call.nome_operador) {
                                    allowedOperatorNames.add(normalizeName(call.nome_operador));
                                }
                            });
                            logMessages.push(`⚠️ Usando lista de operadores do relatório principal como fallback.`);
                        }
                    }
                     if (allowedOperatorNames) {
                        logMessages.push(`✅ Filtro de equipe com ${allowedOperatorNames.size} operadores criado.`);
                    }
                }
                
                processFastwayData(monitoringData);
                logMessages.push(`✅ Relatório Fastway processado. ${monitoringData.length} registros válidos.`);
                
                if (operatorTimesData) {
                    renderOperatorTimesTable(operatorTimesData);
                    logMessages.push('✅ Dados de tempos dos operadores carregados.');
                }
            } else {
                const errorMessage = originalResult.reason?.message || originalResult.value?.message || 'Falha ao buscar dados da API principal.';
                logMessages.push(`❌ ERRO (Fastway): ${errorMessage}`);
                processFastwayData([]);
            }
            
            if (bitrixResult.status === 'fulfilled' && bitrixResult.value.success) {
                processBitrixData(bitrixResult.value.data, allowedOperatorNames);
                logMessages.push('✅ Relatório do Bitrix24 processado e cruzado com sucesso.');
            } else {
                const errorMessage = bitrixResult.reason?.message || bitrixResult.value?.message || 'Falha ao buscar dados do Bitrix24.';
                logMessages.push(`❌ ERRO (Bitrix): ${errorMessage}`);
                processBitrixData(null);
            }

            // --- RENDERIZAÇÃO FINAL ---
            dashboardSummary.innerHTML = `<div class="summary-card" id="summary-card-1" style="display: none;"><div class="summary-card-title" id="summary-title-1"></div><div class="summary-card-value" id="summary-value-1"></div></div><div class="summary-card" id="summary-card-2" style="display: none;"><div class="summary-card-title" id="summary-title-2"></div><div class="summary-card-value" id="summary-value-2"></div></div><button class="summary-card summary-card-button" id="summary-card-3" style="display: none;"><div class="summary-card-title">Tabulações Suspeitas</div><div class="summary-card-value warning" id="summary-value-3">0</div></button><div class="summary-card" id="summary-card-4" style="display: none;"><div class="summary-card-title" id="summary-title-4"></div><div class="summary-card-value" id="summary-value-4"></div></div>`;
            document.getElementById('summary-card-3').addEventListener('click', showSuspiciousCallsModal);
            
            // --- CORREÇÃO APLICADA AQUI ---
            if(fastwaySummaryData || bitrixSummaryData) { 
                summaryToggleBar.style.display = 'flex';
            }
            
            showFastwaySummaryBtn.click();

            monitoringLog.innerHTML = logMessages.map(m => `> ${m}`).join('<br>');
            generateReportBtn.disabled = false;
        });
    }

    function renderOperatorTimesTable(csvData) {
        if (!csvData) { operatorTimesContainer.style.display = 'none'; return; }
        const rows = csvData.trim().split('\n');
        if (rows.length < 2) { operatorTimesTableWrapper.innerHTML = '<p>Nenhum dado de tempo encontrado para a seleção.</p>'; operatorTimesContainer.style.display = 'block'; return; }
        const headers = rows[0].split(';'); const data = rows.slice(1).map(row => row.split(';'));
        let tableHtml = '<table class="operator-times-table"><thead><tr>';
        headers.forEach(header => { tableHtml += `<th>${header.trim()}</th>`; }); tableHtml += '</tr></thead><tbody>';
        data.forEach(rowData => { if (rowData.length < headers.length) return; tableHtml += '<tr>'; rowData.forEach(cell => { tableHtml += `<td>${cell.trim()}</td>`; }); tableHtml += '</tr>'; });
        tableHtml += '</tbody></table>';
        operatorTimesTableWrapper.innerHTML = tableHtml; operatorTimesContainer.style.display = 'block';
    }

    function processFastwayData(data) {
        if (!data || !Array.isArray(data) || data.length === 0) {
            fastwaySummaryData = null; fastwayDetailData = null; return;
        }
        const totalCalls = data.length;
        const aggregators = { tabulacao: {}, resultado: {}, nome_operador: {}, nome_campanha: {}, };
        const detailedTabulations = {};
        let totalDurationSeconds = 0;
        let suspiciousCalls = [];
        data.forEach(item => {
            const tabulacao = item.tabulacao || 'Não Preenchido';
            const duration = getDurationInSeconds(item.tempo_ligacao);
            for (const key in aggregators) { const value = item[key] || 'Não Preenchido'; aggregators[key][value] = (aggregators[key][value] || 0) + 1; }
            if (!detailedTabulations[tabulacao]) { detailedTabulations[tabulacao] = []; }
            detailedTabulations[tabulacao].push(item);
            if (SUSPICIOUS_TABULATIONS.includes(tabulacao) && duration >= SUSPICIOUS_DURATION_SECONDS) { suspiciousCalls.push(item); }
            totalDurationSeconds += duration;
        });
        lastSuspiciousCalls = suspiciousCalls;
        const avgDurationSeconds = totalCalls > 0 ? totalDurationSeconds / totalCalls : 0;
        const roundedAvgSeconds = Math.round(avgDurationSeconds);
        const avgMinutes = Math.floor(roundedAvgSeconds / 60);
        const avgSeconds = roundedAvgSeconds % 60;
        
        fastwaySummaryData = { totalCalls: totalCalls, tma: `${String(avgMinutes).padStart(2, '0')}:${String(avgSeconds).padStart(2, '0')}`, suspiciousCount: lastSuspiciousCalls.length, operatorCount: Object.keys(aggregators.nome_operador).length };
        fastwayDetailData = { aggregators, detailedTabulations };
    }

    function renderFastwayDetails() {
        dashboardDetails.innerHTML = '';
        if (!fastwayDetailData) {
            dashboardDetails.innerHTML = '<p style="color: var(--text-muted); text-align: center;">Nenhum dado da Fastway para exibir.</p>';
            return;
        }

        const { aggregators, detailedTabulations } = fastwayDetailData;
        const isOperatorFiltered = document.getElementById('check-operador_id')?.checked;
        const createDetailCard = (title, dataObject) => { if (title === 'Top Tabulações' && isOperatorFiltered) { return createInteractiveTabulationCard('Top Tabulações', detailedTabulations); } const sortedData = Object.entries(dataObject).sort(([, a], [, b]) => b - a); let listItems = sortedData.map(([name, count]) => `<li><span class="name" title="${name}">${name}</span><span class="count">${count.toLocaleString('pt-BR')}</span></li>`).join(''); if (!listItems) listItems = '<li>Nenhum dado.</li>'; return `<div class="detail-card"><h3>${title}</h3><ul class="detail-list custom-scrollbar">${listItems}</ul></div>`; }

        const createInteractiveTabulationCard = (title, detailedDataObject) => {
            const sortedTabulations = Object.keys(detailedDataObject).sort((a, b) => detailedDataObject[b].length - detailedDataObject[a].length);
            let detailsHtml = sortedTabulations.map(tabulationName => {
                const calls = detailedDataObject[tabulationName];
                const isSuspicious = SUSPICIOUS_TABULATIONS.includes(tabulationName);
                const callListHtml = calls.map((call, index) => {
                    const callId = call.id || '', chave = call.chave || '', protocolo = call.protocolo || '';
                    const downloadUrl = `http://mbfinance.fastssl.com.br/api/gravacao/index.php?id=${callId}&chave=${chave}&protocolo=${protocolo}&tipo_download=1&checkout_step=`;
                    const operatorFirstName = (call.nome_operador || '').split(' ')[0], cnpj = call.cpf || '';
                    let fileName = 'gravacao_desconhecida.mp3';
                    if (operatorFirstName && cnpj) { fileName = `${operatorFirstName}_${cnpj}.mp3`; } else if (cnpj) { fileName = `${cnpj}.mp3`; } else if (callId) { fileName = `gravacao_${callId}.mp3`; }
                    const downloadButtonId = `download-btn-${tabulationName.replace(/[^a-zA-Z0-9]/g, '-')}-${index}`;
                    setTimeout(() => {
                        const btn = document.getElementById(downloadButtonId);
                        if (btn) {
                            btn.addEventListener('click', async () => {
                                const originalContent = btn.innerHTML;
                                btn.disabled = true; btn.innerHTML = `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" viewBox="0 0 16 16" style="animation: spin 1s linear infinite;"><path d="M8 3a5 5 0 1 0 4.546 2.914.5.5 0 0 1 .908-.417A6 6 0 1 1 8 2v1z"/><path d="M8 4.466V.534a.25.25 0 0 1 .41-.192l2.36 1.966c.12.1.12.284 0 .384L8.41 4.658A.25.25 0 0 1 8 4.466z"/></svg>`;
                                appendLog(`Solicitando download para: ${fileName}`);
                                try { const result = await window.electronAPI.downloadRecording(downloadUrl, fileName); appendLog(`✅ ${result.message}`); } catch (err) { appendLog(`❌ Erro no processo de download: ${err.message}`); } finally { btn.disabled = false; btn.innerHTML = originalContent; }
                            });
                        }
                    }, 0);
                    return `<li><div class="call-info"><span class="call-cnpj">CNPJ: ${call.cpf || 'N/A'}</span><span class="call-phone">Tel: ${call.fone || 'N/A'}</span></div><div class="call-actions"><span class="call-duration">Duração: ${call.tempo_ligacao || '00:00:00'}</span><button id="${downloadButtonId}" class="download-link" title="Baixar gravação: ${fileName}"><svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="#ffffff" viewBox="0 0 16 16"><path d="M.5 9.9a.5.5 0 0 1 .5.5v2.5a1 1 0 0 0 1 1h12a1 1 0 0 0 1-1v-2.5a.5.5 0 0 1 1 0v2.5a2 2 0 0 1-2 2H2a2 2 0 0 1-2-2v-2.5a.5.5 0 0 1 .5-.5z"/><path d="M7.646 1.146a.5.5 0 0 1 .708 0l3 3a.5.5 0 0 1-.708.708L8.5 2.707V11.5a.5.5 0 0 1-1 0V2.707L5.354 4.854a.5.5 0 1 1-.708-.708l3-3z"/></svg></button></div></li>`;
                }).join('');
                return `<details><summary class="${isSuspicious ? 'suspicious-summary' : ''}"><span>${tabulationName}</span><span>${calls.length} chamadas</span></summary><ul class="tabulation-call-list custom-scrollbar">${callListHtml}</ul></details>`;
            }).join('');
            return `<div class="detail-card interactive-tabulation"><h3>${title} (Detalhado)</h3><div class="custom-scrollbar" style="max-height: 400px; overflow-y: auto; padding-right: 5px;">${detailsHtml}</div></div>`;
        };

        dashboardDetails.innerHTML += createDetailCard('Top Tabulações', aggregators.tabulacao);
        dashboardDetails.innerHTML += createDetailCard('Resultados por Chamada', aggregators.resultado);
        dashboardDetails.innerHTML += createDetailCard('Top Operadores', aggregators.nome_operador);
        dashboardDetails.innerHTML += createDetailCard('Top Campanhas', aggregators.nome_campanha);
    }

    // #################################################################
    // #           NOVA LÓGICA - AGENDAMENTO FISH (API)              #
    // #################################################################
    const toggleApiToolsBtn = document.getElementById('toggle-api-tools-btn');
    const fishScheduleTitle = document.getElementById('fish-schedule-title');
    const selectFishFilesBtn = document.getElementById('selectFishScheduleFilesBtn');
    const selectedFishFilesDiv = document.getElementById('selectedFishScheduleFiles');
    const scheduleDateInput = document.getElementById('fishScheduleDate');
    const scheduleTimeInput = document.getElementById('fishScheduleTime');
    const scheduleKeySelect = document.getElementById('fishScheduleApiKeySelection');
    const scheduleFishBtn = document.getElementById('scheduleFishBtn');
    const scheduleRemoveClientsCheckbox = document.getElementById('scheduleRemoveClientsCheckbox');
    const scheduleExtractClientsCheckbox = document.getElementById('scheduleExtractClientsCheckbox');
    const scheduleFishModeCheckbox = document.getElementById('scheduleFishModeCheckbox');
    const fishScheduleStatusDiv = document.getElementById('fish-schedule-status');

    let fishScheduleFiles = [];

    // NOVO: Função para renderizar a lista de arquivos do agendamento com botão de remoção
    function renderFishScheduleFiles() {
        selectedFishFilesDiv.innerHTML = ''; // Limpa a lista atual
        if (fishScheduleFiles.length === 0) {
            selectedFishFilesDiv.innerHTML = '<span style="color:var(--text-muted); font-style:italic;">Nenhum arquivo selecionado</span>';
            return;
        }

        fishScheduleFiles.forEach(file => {
            const fileDiv = document.createElement('div');
            fileDiv.className = 'file-item';

            const fileName = document.createElement('span');
            fileName.className = 'file-name';
            fileName.textContent = getBasename(file);
            fileName.title = file; // Mostra o caminho completo no hover
            fileDiv.appendChild(fileName);

            const removeBtn = document.createElement('button');
            removeBtn.className = 'queue-action-btn remove';
            removeBtn.innerHTML = '&#x2716;';
            removeBtn.title = 'Remover';
            removeBtn.onclick = (e) => {
                e.stopPropagation(); // Impede que o clique se propague para outros elementos
                fishScheduleFiles = fishScheduleFiles.filter(f => f !== file); // Remove o arquivo da lista
                renderFishScheduleFiles(); // Re-renderiza a UI
            };
            fileDiv.appendChild(removeBtn);
            selectedFishFilesDiv.appendChild(fileDiv);
        });
    }

    if (selectFishFilesBtn) {
        selectFishFilesBtn.addEventListener('click', async () => {
            const files = await window.electronAPI.selectFile({ title: 'Selecione as listas para o agendamento', multi: true });
            if (files && files.length > 0) {
                // MODIFICADO: Acumula arquivos e remove duplicatas, em vez de substituir
                const newFiles = files.filter(f => !fishScheduleFiles.includes(f));
                fishScheduleFiles.push(...newFiles);
                renderFishScheduleFiles(); // Re-renderiza a lista completa
            }
        });
    }

    if (scheduleFishBtn) {
        scheduleFishBtn.addEventListener('click', () => {
            const date = scheduleDateInput.value;
            const time = scheduleTimeInput.value;

            if (fishScheduleFiles.length === 0) {
                alert('Por favor, selecione pelo menos um arquivo para agendar.');
                return;
            }
            if (!date || !time) {
                alert('Por favor, selecione a data e a hora para o agendamento.');
                return;
            }

            const startDateTime = new Date(`${date}T${time}`);
            if (startDateTime < new Date()) {
                alert('A data e hora do agendamento não podem ser no passado.');
                return;
            }

            const scheduleOptions = {
                files: fishScheduleFiles,
                startTime: startDateTime.toISOString(),
                apiOptions: {
                    keyMode: scheduleKeySelect.value, // Captura a seleção de chaves
                    removeClients: scheduleRemoveClientsCheckbox.checked, // Captura a opção de remover clientes
                    extractClients: scheduleExtractClientsCheckbox.checked, // Captura a opção de extrair clientes
                    isFishMode: scheduleFishModeCheckbox.checked, // Captura a opção de modo FISH
                }
            };

            window.electronAPI.scheduleFishCleanup(scheduleOptions);
            closePanel('api-tools-panel'); // Fecha o painel após agendar
        });
    }

    window.electronAPI.onFishScheduleUpdate((schedule) => {
        if (schedule && schedule.startTime) {
            const scheduleDate = new Date(schedule.startTime);
            const formattedDate = scheduleDate.toLocaleString('pt-BR', { day: '2-digit', month: '2-digit', year: 'numeric', hour: '2-digit', minute: '2-digit' });
            fishScheduleStatusDiv.style.display = 'block';
            fishScheduleStatusDiv.innerHTML = `
                <p style="margin: 0;"><strong>Agendamento Ativo:</strong> ${schedule.files.length} arquivo(s) para ${formattedDate}.</p>
                <button id="cancel-fish-schedule-btn" class="btn-danger" style="padding: 4px 8px; font-size: 12px; margin-top: 8px;">Cancelar Agendamento</button>
            `;
            toggleApiToolsBtn.classList.add('scheduled');
            fishScheduleTitle.textContent = "Agendamento Ativo";

            document.getElementById('cancel-fish-schedule-btn').addEventListener('click', () => {
                if (confirm('Tem certeza que deseja cancelar o agendamento?')) {
                    window.electronAPI.cancelFishSchedule();
                }
            });
        } else {
            fishScheduleStatusDiv.style.display = 'none';
            fishScheduleStatusDiv.innerHTML = '';
            toggleApiToolsBtn.classList.remove('scheduled');
            fishScheduleTitle.textContent = "Agendamento FISH";
        }
    });

    // --- INÍCIO: LÓGICA DA ABA DE RELACIONAMENTO ---
    const relacionamentoLog = document.getElementById('relacionamentoLog');
    
    // Variáveis para armazenar os caminhos dos arquivos
    let relatorioFile = null;
    let bitrixFile = null;
    let timeFile = null;
    let contatosFile = null;

    // Botões
    const relatorioBtn = document.getElementById('relacionamentoSelectRelatorioBtn');
    const bitrixBtn = document.getElementById('relacionamentoSelectBitrixBtn');
    const timeBtn = document.getElementById('relacionamentoSelectTimeBtn');
    const contatosBtn = document.getElementById('relacionamentoSelectContatosBtn');
    const startBtn = document.getElementById('relacionamentoStartBtn');

    // Divs para exibir nomes dos arquivos
    const relatorioPathDiv = document.getElementById('relacionamentoRelatorioPath');
    const bitrixPathDiv = document.getElementById('relacionamentoBitrixPath');
    const timePathDiv = document.getElementById('relacionamentoTimePath');
    const contatosPathDiv = document.getElementById('relacionamentoContatosPath');

    // Função de Log específica da aba
    function appendRelacionamentoLog(msg) {
        if (!relacionamentoLog) return;
        if (relacionamentoLog.textContent.trim() === 'Aguardando o início do processo...') {
            relacionamentoLog.innerHTML = '';
        }
        const p = document.createElement('p');
        p.textContent = `> ${msg.trim()}`;
        relacionamentoLog.appendChild(p);
        relacionamentoLog.scrollTop = relacionamentoLog.scrollHeight;
    }

    // Função auxiliar para criar seletores de arquivo
    async function createFileSelector(button, pathDiv, fileVariableSetter) {
        button.addEventListener('click', async () => {
            const files = await window.electronAPI.selectFile({ title: 'Selecione o arquivo', multi: false });
            if (files && files.length > 0) {
                const filePath = files[0];
                fileVariableSetter(filePath); // Define a variável de path
                addFileToUI(pathDiv, filePath, true); // Mostra o nome no UI
                appendRelacionamentoLog(`Arquivo ${button.id.replace('relacionamentoSelect', '').replace('Btn', '')} selecionado: ${getBasename(filePath)}`);
            }
        });
    }

    // Configura os 4 botões de seleção de arquivo
    createFileSelector(relatorioBtn, relatorioPathDiv, (path) => relatorioFile = path);
    createFileSelector(bitrixBtn, bitrixPathDiv, (path) => bitrixFile = path);
    createFileSelector(timeBtn, timePathDiv, (path) => timeFile = path);
    createFileSelector(contatosBtn, contatosPathDiv, (path) => contatosFile = path);

    // Lógica do Botão Iniciar
    if (startBtn) {
        startBtn.addEventListener('click', () => {
            // Verifica se todos os arquivos foram selecionados
            if (!relatorioFile || !bitrixFile || !timeFile || !contatosFile) {
                appendRelacionamentoLog('❌ ERRO: Por favor, selecione todos os quatro arquivos antes de iniciar.');
                return;
            }

            const modo = document.querySelector('input[name="relacionamentoModo"]:checked').value;

            appendRelacionamentoLog('Iniciando processo... Enviando arquivos para o backend.');
            startBtn.disabled = true;
            startBtn.innerHTML = 'Processando...';

            const filePaths = {
                relatorio: relatorioFile,
                bitrix: bitrixFile,
                time: timeFile,
                contatos: contatosFile
            };

            // Envia os caminhos e o modo para o main process
            window.electronAPI.runRelacionamentoPipeline(filePaths, modo);
        });
    }

    // Ouve por logs do pipeline de relacionamento
    window.electronAPI.onRelacionamentoLog((logMsg) => {
        appendRelacionamentoLog(logMsg);
    });

    // Ouve pelo evento de finalização
    window.electronAPI.onRelacionamentoFinished((success) => {
        if (success) {
            appendRelacionamentoLog('✅ Processo concluído com sucesso!');
        } else {
            appendRelacionamentoLog('❌ Ocorreu um erro durante o processo. Verifique os logs.');
        }
        startBtn.disabled = false;
        startBtn.innerHTML = '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" viewBox="0 0 16 16"><path d="M11.596 8.697l-6.363 3.692c-.54.313-1.233-.066-1.233-.697V4.308c0-.63.692-1.01 1.233-.696l6.363 3.692a.802.802 0 0 1 0 1.393z"/></svg> Iniciar Processo';
    });
    // --- FIM: LÓGICA DA ABA DE RELACIONAMENTO ---
});