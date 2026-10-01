# Recuperação da instalação 1.8.0

Os arquivos de execução nesta branch foram extraídos de `C:\Program Files\Gerenciador de Bases\resources\app.asar`, instalado em 30/09/2026 às 19:38 (horário de Brasília). SHA-256 do `app.asar`: `EB8AC8F6808D20710DA55ED69BE322DA6F82382A5C2845E37381ADFD4D24C143`.

O repositório remoto aponta a tag `v1.8.0` para o commit `fab5b72` de 30/09/2026 às 12:42. Nesse commit, `package.json` ainda declara `1.7.0`. No commit inicial de recuperação `3f65c2e`, os 55 arquivos do aplicativo extraídos do pacote e copiados para esta branch correspondem byte a byte aos arquivos instalados; `package.json` foi mantido com seus scripts de desenvolvimento e atualizado para `1.8.0`. Correções posteriores nesta branch podem modificar esse código recuperado.

O instalador não contém os arquivos `src/renderer/react/**`, que foram excluídos pela configuração de empacotamento. Os arquivos nessa pasta vieram do commit remoto e geram um painel diferente: o bundle JS gerado tem 226.510 bytes, enquanto o instalado tem 277.223 bytes. Os arquivos `renderer/react.js` e `renderer/react.css` neste diretório são cópias do painel instalado.

Use `npm run start:installed` para restaurar o painel compilado do instalador em `out/renderer` e iniciar esta versão. `npm start`, `npm run build:renderer`, `npm run dist` e `npm run publish` recompilam os fontes antigos do painel; para editar ou publicar a interface 1.8.0, é necessário recuperar os fontes React usados no instalador ou reconstruí-los a partir do bundle.

## Correção local da consulta Receita

Em 30/09/2026, a consulta de paginação e situação passou a converter os parâmetros para o tipo das colunas, preservando o uso dos índices de CNPJ e situação quando o banco usa CHAR. O teste de leitura com os filtros do fluxo que falhou retornou o primeiro lote de 2.000 registros em 181 ms; anteriormente, a consulta excedia o tempo limite de 60 segundos. A suíte passou com 83 testes. Não houve alteração de esquema ou de dados no banco.

A aplicação corrigida foi empacotada a partir do app.asar instalado, sobrepondo apenas src/main/flows/receita.js e src/main/handlers/fluxos.js, e aberta em C:\Users\dabra\AppData\Local\Programs\Gerenciador de Bases - Receita corrigida. O atalho Gerenciador de Bases - Receita corrigida está na Área de Trabalho. Essa cópia conserva o nome do aplicativo e usa as configurações e o histórico existentes em AppData/Roaming/gerenciador-de-bases. A instalação em Program Files permanece na versão anterior porque sua alteração exige administrador.

SHA-256 do pacote corrigido: 67b33fb78d996958b512fa1f7defc24fd3d4e22c2fd449771f1daec8660e3fa3. O fluxo completo ainda precisa ser retomado no aplicativo; a verificação no banco cobriu o lote inicial.

## Correção da leitura JSONL no enriquecimento

A execução C6 ABERTURA concluiu a geração de 13.582.365 registros, com arquivo de 13,36 GB, mas o worker excedeu o limite de memória no enriquecimento. A leitura anterior anexava a cada linha novas reações a uma promessa de erro que permanecia pendente. Foi substituída por leitura assíncrona direta de chunks, que propaga erros do stream e respeita cancelamento sem manter uma promessa pendente por registro.

A reprodução da leitura anterior falhou por `ERR_WORKER_OUT_OF_MEMORY` com heap de 128 MB. A leitura corrigida processou 500 mil registros do arquivo real com heap limitado a 64 MB, com amostras de uso entre 9 e 23 MB. A suíte passou com 87 testes, incluindo leitura UTF-8 entre chunks, arquivo ausente, JSON inválido, cancelamento e leitura de volume sob limite de memória.

O novo pacote mantém as correções de conexão e paginação, e acrescenta `src/main/flows/jsonl.js` e a atualização de `src/main/flows/pipeline.js`. SHA-256: `91c2749ba441a05abef2a99ba14e2041e7f5362bf4ff3f9f67145d2ed004eb93`. O checkpoint da geração e o arquivo confirmado permanecem preservados para retomar o enriquecimento. A conclusão das etapas posteriores deve ser conferida no histórico do aplicativo.

## Telefones antigos e contadores da limpeza

A geração e o enriquecimento terminaram, mas a limpeza considerava todo telefone de 10 dígitos como fixo. O exportador anterior da mesma base (`D:/BASES/exportar_leads.py`, função `inserir9`) acrescenta 9 após o DDD em números de 10 dígitos com primeiro dígito do assinante entre 6 e 9. Essa regra de compatibilidade foi restaurada somente nos fluxos, em `src/main/flows/telefones.js`. Ela replica a regra existente do usuário; não verifica se uma linha continua atribuída ou alcançável. Números truncados ou sem DDD não recebem dígitos por essa regra. A remoção de fixos passou a exigir 10 dígitos e primeiro dígito do assinante entre 2 e 5; números de 10 dígitos com prefixo 0 ou 1 nessa posição são tratados como sujos.

As consultas à blocklist e aos telefones inválidos incluem a grafia normalizada e a antiga com e sem DDI, e as respostas são normalizadas antes da comparação. O fluxo mantém as etapas de geração e enriquecimento confirmadas e recalcula somente a limpeza. A contagem de linhas sem telefone aproveitável foi dividida em três motivos: sem contato válido antes dos filtros, sem contato após os filtros e só com contatos repetidos. Telefones removidos e nonos dígitos ajustados contam contatos, enquanto descartes de linhas contam empresas.

O bundle do painel instalado foi atualizado diretamente apenas nos rótulos dos contadores e na explicação, mantendo a interface recuperada. Seus fontes React originais continuam indisponíveis. A suíte passou com 90 testes, incluindo preservação de celular antigo, bloqueio pela grafia antiga e conciliação dos contadores. SHA-256 do pacote atualizado: `158af4de76eeeb16510adac0b1b3b0725de5ce89c601f178466ed92dcf6cdcfc`.

## Nono dígito embutido na geração

Conforme a correção de requisito do usuário, o ajuste foi movido para a primeira etapa. `iterateReceita` já entrega `telefone_principal` e `telefone_secundario` com celulares completos no primeiro lote, e `canonical` garante o mesmo formato para todos os campos de telefone e para o array `phones` antes de salvar `generation.jsonl`. Contatos adicionais do enriquecimento recebem o mesmo formato ao entrar nessa etapa. A limpeza não acrescenta o nono dígito e o contador de ajustes foi removido do painel.

O checkpoint de geração registra `phoneFormatVersion: 1`. A retomada recusa uma geração histórica sem esse marcador para impedir cruzamentos que misturem contatos anteriores e posteriores ao ajuste. Para a execução atual, o checkpoint anterior foi preservado em `checkpoint-before-generation-phone-fix.json` e a geração foi reiniciada a partir da Receita, com a mesma configuração e raiz histórica já consultada. Os arquivos derivados anteriores serão substituídos somente quando a respectiva etapa concluir por renomeação atômica.

A auditoria integral dos 13.582.365 registros da geração anterior identificou 711.999 empresas sem nenhum telefone bruto na origem, 272.335 apenas com valores que não produziram telefone normalizável, 5.546.008 apenas com fixos, 7.028.390 com celular após normalização de geração e 23.633 com outros formatos. Essas categorias são exclusivas e somam o total. Foram identificadas 7.305.602 ocorrências de celular no formato antigo; ocorrências de telefone não equivalem a empresas ou contatos únicos. Os números da limpeza incluem também descartes por filtros e repetição, por isso não medem exclusivamente telefone ausente na origem.

A suíte passou com 92 testes, incluindo o primeiro lote Receita, o conteúdo do arquivo de geração antes da primeira consulta de enriquecimento e a recusa de checkpoint histórico incompatível. SHA-256 do pacote atualizado: `f9944748769c633b628fbf989124de8806f81b47b520c88e797fac098562411e`.
