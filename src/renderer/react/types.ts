export type ColumnsProgress = { current: number; total: number; fileName: string };
export type ColumnsResult = { success: boolean; processados: number; pulados: number; message?: string };
type Unsubscribe = () => void;

export interface ColumnsAPI {
    selectFile(options: { title: string; multi: boolean }): Promise<string[] | null>;
    startLimpezaColunas(paths: string[]): void;
    onLimpezaColunasLog(callback: (message: string) => void): Unsubscribe;
    onLimpezaColunasProgress(callback: (progress: ColumnsProgress) => void): Unsubscribe;
    onLimpezaColunasFinished(callback: (result: ColumnsResult) => void): Unsubscribe;
}

declare global {
    interface Window { electronAPI: ColumnsAPI }
}
