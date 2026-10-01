export type Operation = 'c6' | 'santander' | 'pagbank' | 'mercadopago';
export type RootSource = 'bq' | 'neon' | 'file' | 'none';
export interface Flow {
    id: string;
    name: string;
    operation: Operation;
    pipelines: number[];
    revision: number;
    generation: {
        limit: number | null; uf: string[]; cidade: string[]; bairro: string[]; cnaes: string[]; naturezas: string[];
        dateFrom: string; dateTo: string; mei: 'all' | 'yes' | 'no';
        phone: 'all' | 'with' | 'without'; email: 'all' | 'with' | 'without'; situacoes: string[];
    };
    enrichment: { enabled: boolean; strategy: 'append' | 'overwrite' | 'ignore'; fillCpf: boolean };
    api?: { enabled: boolean; keyMode: 'dupla'; delayMs: 60000 };
    cleaning: {
        enabled: boolean; rootSource: RootSource; rootFile: string; blocklist: boolean; invalidPhones: boolean;
        removeLandlines: boolean; fillLivre5: boolean; prohibitedCnaes: string[];
    };
    output: { formatId: string; csv: boolean; rowsPerFile: number; includeSituacao: boolean };
}
export type JobStatus = 'running' | 'completed' | 'empty' | 'failed' | 'cancelled' | 'interrupted';
export interface FlowJob {
    id: string; flowId: string; flowName: string; owner: string; status: JobStatus; stage: string;
    createdAt: string; updatedAt: string; counts: Record<string, number | null | undefined>;
    outputs: { path: string; kind: string; rows: number }[]; logs: string[]; error?: string; errorCode?: string;
    flowSnapshot?: Flow;
    rootInfo?: { source?: string; count?: number; rows?: number; documents?: number; pipelines?: number[]; queriedAt?: string; coverage?: string; skipped?: number; restored?: number };
}
export interface LayoutColumn { header: string; campo: string; valor_manual?: string; partes?: string[]; sep?: string }
export interface CustomLayout { id: string; nome: string; colunas: LayoutColumn[]; custom: boolean; revision: number }
export interface FlowFormat { id: string; nome: string; colunas: (string | LayoutColumn)[]; custom?: boolean; revision?: number }
export interface LayoutField { id: string; label: string }
export interface LayoutPreview { headers: string[]; values: string[] }
export interface LayoutPreviewInput { layout: CustomLayout; operation: Operation; includeSituacao: boolean; fillCpf: boolean; fillLivre5: boolean }
export interface FlowOperation { id: Operation; name?: string; nome?: string; label?: string; pipelines?: number[] }
export interface BqAuthStatus { owner?: string; state: 'idle' | 'renewing' | 'ready' | 'failed'; message: string }
export interface FlowBootstrap {
    success: boolean; message?: string; user?: { username: string; role: string }; flows?: Flow[];
    jobs?: FlowJob[]; formats?: FlowFormat[]; layoutFields?: LayoutField[]; operations?: FlowOperation[]; defaults?: Partial<Flow>;
    access?: { receitaConfigured: boolean; apiConfigured?: boolean; bqConfigured: boolean; bqLoginMode?: string; bqAutoLogin?: boolean; bqAuth?: BqAuthStatus }; limits?: { maxRows: number };
}
export interface FlowResult { success: boolean; message?: string; flow?: Flow; job?: FlowJob }
export interface ReceitaOption { value: string; label: string }
export interface ReceitaOptionsInput { field: 'uf' | 'cidade' | 'bairro' | 'cnaes' | 'naturezas'; search?: string; offset?: number; uf?: string[]; cidade?: string[] }
export interface ReceitaOptionsResult extends FlowResult { options?: ReceitaOption[]; hasMore?: boolean }
export interface FlowAPI {
    flowsBootstrap(): Promise<FlowBootstrap>;
    flowsSave(flow: Flow): Promise<FlowResult>;
    flowsReceitaOptions(input: ReceitaOptionsInput): Promise<ReceitaOptionsResult>;
    flowsSaveLayout(layout: CustomLayout): Promise<FlowResult & { layout?: CustomLayout }>;
    flowsDeleteLayout(id: string): Promise<FlowResult>;
    flowsPreviewLayout(input: LayoutPreviewInput): Promise<FlowResult & { preview?: LayoutPreview }>;
    flowsDelete(id: string): Promise<FlowResult>;
    flowsSelectFolder(): Promise<{ success: boolean; path?: string; cancelled?: boolean; message?: string }>;
    flowsStart(input: { flowId: string; outputDirectory: string }): Promise<FlowResult>;
    flowsCancel(jobId: string): Promise<FlowResult>;
    flowsResume(jobId: string): Promise<FlowResult>;
    flowsOpenOutput(input: { jobId: string; path: string }): Promise<FlowResult>;
    flowsConfigureReceita(input: { connectionString: string }): Promise<FlowResult>;
    flowsConfigureBq(): Promise<FlowResult>;
    flowsTestBq(): Promise<FlowResult>;
    flowsRenewBq(): Promise<FlowResult>;
    flowsBqAutoLogin(enabled: boolean): Promise<FlowResult>;
    onFlowBqAuthUpdate(callback: (status: BqAuthStatus) => void): () => void;
    onFlowUpdate(callback: (job: FlowJob) => void): () => void;
    selectFile(options: { title: string; multi: boolean }): Promise<string[] | null>;
}
