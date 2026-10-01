# Recuperação da instalação 1.8.0

Os arquivos de execução nesta branch foram extraídos de `C:\Program Files\Gerenciador de Bases\resources\app.asar`, instalado em 30/09/2026 às 19:38 (horário de Brasília). SHA-256 do `app.asar`: `EB8AC8F6808D20710DA55ED69BE322DA6F82382A5C2845E37381ADFD4D24C143`.

O repositório remoto aponta a tag `v1.8.0` para o commit `fab5b72` de 30/09/2026 às 12:42. Nesse commit, `package.json` ainda declara `1.7.0`. No commit inicial de recuperação `3f65c2e`, os 55 arquivos do aplicativo extraídos do pacote e copiados para esta branch correspondem byte a byte aos arquivos instalados; `package.json` foi mantido com seus scripts de desenvolvimento e atualizado para `1.8.0`. Correções posteriores nesta branch podem modificar esse código recuperado.

O instalador não contém os arquivos `src/renderer/react/**`, que foram excluídos pela configuração de empacotamento. Os arquivos nessa pasta vieram do commit remoto e geram um painel diferente: o bundle JS gerado tem 226.510 bytes, enquanto o instalado tem 277.223 bytes. Os arquivos `renderer/react.js` e `renderer/react.css` neste diretório são cópias do painel instalado.

Use `npm run start:installed` para restaurar o painel compilado do instalador em `out/renderer` e iniciar esta versão. `npm start`, `npm run build:renderer`, `npm run dist` e `npm run publish` recompilam os fontes antigos do painel; para editar ou publicar a interface 1.8.0, é necessário recuperar os fontes React usados no instalador ou reconstruí-los a partir do bundle.

## Correção local da consulta Receita

Em 30/09/2026, a consulta de paginação e situação passou a converter os parâmetros para o tipo das colunas, preservando o uso dos índices de CNPJ e situação quando o banco usa CHAR. O teste de leitura com os filtros do fluxo que falhou retornou o primeiro lote de 2.000 registros em 181 ms; anteriormente, a consulta excedia o tempo limite de 60 segundos. A suíte passou com 83 testes. Não houve alteração de esquema ou de dados no banco.

A aplicação corrigida foi empacotada a partir do app.asar instalado, sobrepondo apenas src/main/flows/receita.js e src/main/handlers/fluxos.js, e aberta em C:\Users\dabra\AppData\Local\Programs\Gerenciador de Bases - Receita corrigida. O atalho Gerenciador de Bases - Receita corrigida está na Área de Trabalho. Essa cópia conserva o nome do aplicativo e usa as configurações e o histórico existentes em AppData/Roaming/gerenciador-de-bases. A instalação em Program Files permanece na versão anterior porque sua alteração exige administrador.

SHA-256 do pacote corrigido: 67b33fb78d996958b512fa1f7defc24fd3d4e22c2fd449771f1daec8660e3fa3. O fluxo completo ainda precisa ser retomado no aplicativo; a verificação no banco cobriu o lote inicial.
