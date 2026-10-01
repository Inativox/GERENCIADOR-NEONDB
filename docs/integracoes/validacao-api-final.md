# Validação final pela API C6

Configuração atual de Davi: consultar a Limpeza API depois do enriquecimento e antes dos filtros finais, com chave dupla e intervalo de um minuto.

O fluxo é Receita → enriquecimento → disponibilidade C6 opcional → limpeza final → exportação. Apenas os CNPJs retornados como disponíveis e que sobreviverem à limpeza saem nos arquivos. A consulta é online para os CNPJs enriquecidos, sem depender do status histórico do Neon.

A configuração é opcional e pertence a cada fluxo C6; Davi confirmou que a etapa só se aplica ao C6. Novos fluxos C6 e fluxos C6 antigos ao iniciar uma nova execução recebem a etapa ativa por padrão, podendo desmarcá-la antes de salvar; outras operações não oferecem a opção. Execuções já congeladas preservam suas regras. A interface informa que ambas as chaves consultam disponibilidade no C6.

Cada rodada aceita até 40.000 CNPJs e divide a entrada entre C6/IM, até 20.000 por chave. O intervalo mínimo é 60.000 ms entre rodadas e retentativas. Respostas válidas de cada chave são confirmadas em arquivos privados por lote. Ao retomar, esses resultados são reutilizados, inclusive se a outra chave falhou. Respostas malformadas, CNPJs inesperados e erros de autenticação não viram resultado vazio nem status cliente. CNPJs alfanuméricos são preservados.

Credenciais ficam somente no processo principal e na memória do worker, nunca no renderer, configurações de fluxo, logs ou checkpoints. A reserva das chaves ocorre apenas na etapa API e é compartilhada com a fila manual, com aquisição atômica no Neon, heartbeat e liberação ao concluir, falhar ou cancelar. Uma reserva perdida interrompe a etapa antes de exportar.

O cancelamento interrompe a espera e requisições. Se não restarem registros, não há chamadas nem arquivos vazios. A tela mostra consultados, disponíveis e clientes removidos. O nono dígito dos celulares é corrigido durante a geração, antes do enriquecimento. Não há varredura de todos os CNPJs da Receita.

Validação usa respostas HTTP, credenciais, banco e planilhas sintéticos. Nenhuma consulta real à API ou atualização da base Receita faz parte dos testes.
