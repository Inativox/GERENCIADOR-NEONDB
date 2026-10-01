# Recuperação da instalação 1.8.0

Os arquivos de execução nesta branch foram extraídos de `C:\Program Files\Gerenciador de Bases\resources\app.asar`, instalado em 30/09/2026 às 19:38 (horário de Brasília). SHA-256 do `app.asar`: `EB8AC8F6808D20710DA55ED69BE322DA6F82382A5C2845E37381ADFD4D24C143`.

O repositório remoto aponta a tag `v1.8.0` para o commit `fab5b72` de 30/09/2026 às 12:42. Nesse commit, `package.json` ainda declara `1.7.0`. No commit inicial de recuperação `3f65c2e`, os 55 arquivos do aplicativo extraídos do pacote e copiados para esta branch correspondem byte a byte aos arquivos instalados; `package.json` foi mantido com seus scripts de desenvolvimento e atualizado para `1.8.0`. Correções posteriores nesta branch podem modificar esse código recuperado.

O instalador não contém os arquivos `src/renderer/react/**`, que foram excluídos pela configuração de empacotamento. Os arquivos nessa pasta vieram do commit remoto e geram um painel diferente: o bundle JS gerado tem 226.510 bytes, enquanto o instalado tem 277.223 bytes. Os arquivos `renderer/react.js` e `renderer/react.css` neste diretório são cópias do painel instalado.

Use `npm run start:installed` para restaurar o painel compilado do instalador em `out/renderer` e iniciar esta versão. `npm start`, `npm run build:renderer`, `npm run dist` e `npm run publish` recompilam os fontes antigos do painel; para editar ou publicar a interface 1.8.0, é necessário recuperar os fontes React usados no instalador ou reconstruí-los a partir do bundle.
