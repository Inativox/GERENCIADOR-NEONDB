// Ícones de traço usados pelo Importar Mailing e pela Situação Receita, para as duas telas falarem a mesma língua.
const icon = { width: 18, height: 18, viewBox: '0 0 24 24', fill: 'none', stroke: 'currentColor', strokeWidth: 1.8, strokeLinecap: 'round' as const, strokeLinejoin: 'round' as const, 'aria-hidden': true };
export const FileIcon = () => <svg {...icon}><path d="M14 3H7a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h10a2 2 0 0 0 2-2V8Z" /><path d="M14 3v5h5M9 13h6M9 17h4" /></svg>;
export const CheckIcon = () => <svg {...icon}><circle cx="12" cy="12" r="9" /><path d="m8 12.5 2.6 2.5L16 9.5" /></svg>;
export const AlertIcon = () => <svg {...icon}><circle cx="12" cy="12" r="9" /><path d="M12 7.5v5.5M12 16.5v.01" /></svg>;
export const SpinnerIcon = () => <svg {...icon}><path d="M21 12a9 9 0 1 1-6.2-8.56" /></svg>;
export const KeyIcon = () => <svg {...icon}><circle cx="8" cy="15" r="4" /><path d="m10.8 12.2 8.7-8.7M16 7l2.5 2.5M14 9l2 2" /></svg>;
export const SearchIcon = () => <svg {...icon}><circle cx="11" cy="11" r="6.5" /><path d="m20 20-4.4-4.4" /></svg>;
