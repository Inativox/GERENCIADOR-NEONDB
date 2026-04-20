# Estilo de código

- JavaScript sem TypeScript. Sem frameworks de UI (React, Vue, etc).
- `async/await` para operações assíncronas. Nunca `.then()` em código novo.
- Nomes de variáveis e funções em camelCase. Constantes em UPPER_SNAKE_CASE.
- Funções pequenas e com responsabilidade única — se passou de 40 linhas, provavelmente deve ser dividida.
- Logs de debug com `console.log` apenas durante desenvolvimento. Em produção, usar `electron-log`.
