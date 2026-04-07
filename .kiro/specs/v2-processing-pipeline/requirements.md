# Documento de Requisitos — Pipeline de Processamento V2

## Introdução

O sistema 3F Qigger DB Gerenciador possui um pipeline de processamento orquestrado pelo `processar_completo.py` que atualmente grava dados em dois bancos em paralelo: o legado (`portabilidade.db`) e o novo V2 (`portabilidade_v2.db`). Os 6 scripts geradores de homologação (`gerar_homologacao_wpp.py`, `gerar_homologacao_reabertura.py`, `gerar_homologacao_aprovisionamento.py`, `gerar_homologacao_erro_aprovisionamento.py`, `gerar_homologacao_entrega_baixa.py`, `gerar_homologacao_consulta.py`) são responsáveis por consultar o banco, aplicar regras de negócio (filtragem, deduplicação, validação, resolução de templates) e gerar os arquivos `.xlsx` de saída com colunas e formatos específicos para cada processo downstream. Atualmente todos leem do banco legado. Este documento especifica os requisitos para migrar os geradores para usar o banco V2 como fonte primária, manter a compatibilidade com plugins que consomem a tabela `base_coverte_prop` no banco legado, e integrar o reprocessamento de endereços inválidos do projeto externo `3F_Endereco_invalido` como nova etapa do pipeline.

## Glossário

- **Pipeline_V2**: O pipeline de processamento refatorado que usa `portabilidade_v2.db` como fonte primária para geração de arquivos de saída
- **Banco_Legado**: O banco de dados `portabilidade.db` cujas tabelas legadas (`base_coverte_prop`, `portabilidade_records`, `relatorio_objetos`, `portabilidade_processamento`) são a fonte atual dos geradores
- **Banco_V2**: O banco de dados `portabilidade_v2.db` com schema normalizado, versionado e INSERT-only (18 tabelas de dados + 9 de controle + views)
- **Gerador_WPP**: Script `gerar_homologacao_wpp.py` que gera arquivo `.xlsx` para régua de comunicação WhatsApp, consultando `base_coverte_prop` + `portabilidade_records` + `relatorio_objetos`, aplicando filtros de crivo_vendas, resolução de templates, preview de mensagem, normalização de endereço e integração com histórico Google Sheets
- **Gerador_Reabertura**: Script `gerar_homologacao_reabertura.py` que gera arquivo `.xlsx` para reabertura de portabilidade cancelada, usando CTE para registro mais recente por `codigo_externo`, contagem de classificações e último `novo_status_bilhete`
- **Gerador_Aprovisionamento**: Script `gerar_homologacao_aprovisionamento.py` que gera arquivo `.xlsx` com todas as linhas em status "Em Aprovisionamento" (múltiplos `numero_acesso`/`numero_ordem` por `codigo_externo`), excluindo "Erro no Aprovisionamento"
- **Gerador_Erro_Aprovisionamento**: Script `gerar_homologacao_erro_aprovisionamento.py` que gera arquivo `.xlsx` com linhas em status "Erro no Aprovisionamento", exigindo histórico de entrega (dados de `relatorio_objetos` ou `ObjectsLoader`)
- **Gerador_Entrega_Baixa**: Script `gerar_homologacao_entrega_baixa.py` que gera arquivo `.xlsx` filtrando vendas com status de entrega cancelada, baixa, remetente, aguardando correios ou extraviada, usando CTE para registro mais recente de `relatorio_objetos` por `codigo_externo`
- **Gerador_Consulta**: Script `gerar_homologacao_consulta.py` que gera arquivo `.xlsx` com 4 colunas (Cpf, Número de acesso, Número da ordem, Código externo) para vendas com entrega confirmada, gerando 2 linhas para Portabilidade (número portado + nova linha) e 1 linha para Aquisição
- **QueriesV2**: Classe em `src/database/queries_v2.py` com métodos de consulta otimizados para as views do Banco_V2
- **Processador_Completo**: Script `processar_completo.py` que orquestra todas as etapas do pipeline
- **Reprocessador_Endereco**: Módulo integrado a partir do projeto externo `3F_Endereco_invalido` que valida e corrige endereços via APIs de CEP e geocodificação reversa, utilizando mecanismo de proxy dinâmica para rotacionar entre proxies válidas
- **Proxy_Dinamica**: Mecanismo que mantém um pool de proxies válidas, testa conectividade, rotaciona automaticamente entre elas e descarta as que falham, garantindo disponibilidade das APIs de geocodificação
- **Base_Coverte_Prop**: Tabela no Banco_Legado importada da planilha COVERTE BASE PROP via `processar_excel_unificado.py`; contém dados de venda, cliente, endereço, status de entrega (status_correios, status_loggi, status_entrega_prevista), crivo_vendas, bluechip_status
- **Portabilidade_Records**: Tabela no Banco_Legado importada de CSVs de portabilidade via `processar_atualizacoes_gerar_finais.py`; contém dados Siebel (status_bilhete, status_ordem, numero_acesso, numero_ordem, motivos, datas de processamento)
- **Relatorio_Objetos**: Tabela no Banco_Legado importada de Excel de relatório de objetos; contém dados de logística (rastreio, transportadora, status, iccid, nu_pedido, ultima_ocorrencia, data_entrega)
- **Portabilidade_Processamento**: Tabela no Banco_Legado com dados de processamento TIM; usada para validação de rejeição SMS e lookup de proposta_isize
- **Cache_Unificada**: Tabela `cache_base_unificada` no Banco_V2 que materializa a view `vw_base_unificada`
- **Pasta_Saida**: Diretório configurado em `config.PASTA_SAIDA_HOMOLOGACAO` onde os arquivos de homologação são copiados
- **Tipo_Entrega**: Classificação adicionada pelo Reprocessador_Endereco: "Express" (entrega em até 2 dias da data de venda) ou "Correios" (transportadora Correios ou prazo superior a 2 dias)
- **Rejeicao_SMS**: Regra de exclusão aplicada por todos os geradores: registros com `status_bilhete LIKE '%rejeicao sms%'` ou `motivo_recusa/motivo_cancelamento LIKE '%rejei%cliente%sms%'` em `portabilidade_records`, `portabilidade_processamento` ou `base_unificada` são excluídos da geração

## Mapeamento de Migração (Legado → V2)

| Tabela Legada | View/Tabela V2 | Uso Principal |
|---|---|---|
| `base_coverte_prop` | `vw_base_unificada` / `cache_base_unificada` | Dados de venda, cliente, endereço, status entrega |
| `portabilidade_records` | `vw_consulta_siebel_corrente` + `vw_decisoes_corrente` | Dados Siebel (status bilhete/ordem, motivos, datas) |
| `relatorio_objetos` | `vw_logistica_corrente` | Dados de logística (rastreio, ICCID, entrega) |
| `portabilidade_processamento` | `vw_portabilidade_tim_corrente` | Validação rejeição SMS, lookup proposta_isize |

## Requisitos

### Requisito 1: Gerador WPP — Migração para Banco V2

**User Story:** Como operador, eu quero que o gerador de homologação WPP leia do banco V2 mantendo toda a lógica de negócio atual (filtros, templates, preview de mensagem, histórico Google Sheets), para que os disparos WhatsApp reflitam dados atualizados do schema normalizado.

#### Critérios de Aceitação

1. WHEN o Gerador_WPP é executado, THE Gerador_WPP SHALL consultar registros usando `QueriesV2.buscar_registros_wpp()` no Banco_V2, substituindo a consulta direta às tabelas `base_coverte_prop` + `portabilidade_records` + `relatorio_objetos` do Banco_Legado.
2. WHEN o Gerador_WPP consulta registros, THE QueriesV2 SHALL aplicar os filtros equivalentes ao legado: últimos 180 dias por data de venda, `crivo_vendas = 'APROVADA'`, `telefone_portado` não nulo, exclusão de entregas canceladas e exclusão de registros com Rejeicao_SMS.
3. WHEN o Gerador_WPP processa os registros retornados, THE Gerador_WPP SHALL aplicar a resolução de templates via `TemplateMapper` usando os campos `acao_a_realizar` e `tipo_mensagem` da `vw_base_unificada`, gerando as colunas `Template_Triggers`, `O_Que_Aconteceu`, `Acao_Realizar` e `Preview_Mensagem`.
4. WHEN o Gerador_WPP processa endereços, THE Gerador_WPP SHALL normalizar o campo `Numero` extraindo apenas dígitos e movendo letras/texto para o campo `Complemento` usando a função `extrair_numero_e_complemento()`.
5. WHEN o Gerador_WPP verifica histórico de envios, THE Gerador_WPP SHALL consultar o Google Sheets compartilhado para controle de tentativas (máximo 3 tentativas por template, intervalo mínimo de 48h entre envios).
6. WHEN IDs forçados existem em `data/ids_forcar_wpp.txt`, THE Gerador_WPP SHALL incluir esses registros no arquivo de saída independentemente das regras de crivo_vendas e template.
7. THE Gerador_WPP SHALL gerar arquivo `.xlsx` com as colunas: Proposta_iSize, Cpf, NomeCliente, Telefone_Contato, Endereco, Numero, Complemento, Bairro, Cidade, UF, Cep, Ponto_Referencia, Cod_Rastreio, Data_Venda, Data_Conectada, Tipo_Comunicacao, Tentativas, Total_Classificacoes, Houve_Reclassificacao, Status_Disparo, DataHora_Disparo, Template_Triggers, O_Que_Aconteceu, Acao_Realizar, Crivo_Vendas, Preview_Mensagem.
8. IF o Banco_V2 não contém a view `vw_base_unificada`, THEN THE Gerador_WPP SHALL registrar erro no log e interromper a geração sem afetar os demais geradores.

### Requisito 2: Gerador Reabertura — Migração para Banco V2

**User Story:** Como operador, eu quero que o gerador de homologação de reabertura leia do banco V2 mantendo a lógica de CTE para registro mais recente, contagem de classificações e deduplicação, para que o arquivo de reabertura reflita dados atualizados.

#### Critérios de Aceitação

1. WHEN o Gerador_Reabertura é executado, THE Gerador_Reabertura SHALL consultar registros usando `QueriesV2.buscar_registros_reabertura()` no Banco_V2, substituindo a consulta direta às tabelas `portabilidade_records` + `base_coverte_prop` + `relatorio_objetos` do Banco_Legado.
2. WHEN o Gerador_Reabertura consulta registros, THE QueriesV2 SHALL aplicar CTE equivalente ao legado: (a) selecionar o registro mais recente por `codigo_externo` (MAX(id) em `portabilidade_records`, equivalente a `vw_consulta_siebel_corrente`), (b) contar classificações com status de reabertura por `codigo_externo`, (c) obter o `novo_status_bilhete` mais recente não-nulo.
3. WHEN o Gerador_Reabertura filtra registros, THE QueriesV2 SHALL incluir apenas registros onde o status mais recente é `status_bilhete = 'Portabilidade Cancelada'` OU `motivo_cancelamento` preenchido (não nulo, não vazio, não 'NULL'), dentro dos últimos 180 dias por data de venda, excluindo registros com Rejeicao_SMS.
4. WHEN o Gerador_Reabertura gera o arquivo, THE Gerador_Reabertura SHALL validar registros contra `portabilidade_processamento` (equivalente V2: `vw_portabilidade_tim_corrente`) usando `filtrar_registros_validos()` e aplicar deduplicação por (cpf, numero_acesso).
5. THE Gerador_Reabertura SHALL gerar arquivo `.xlsx` via `CSVGenerator.generate_reabertura_csv()` com colunas no formato Siebel incluindo: cpf, numero_acesso, numero_ordem, codigo_externo, status_bilhete, status_ordem, operadora_doadora, novo_status_bilhete, data_portabilidade, motivo_cancelamento, motivo_recusa, preco_ordem, total_classificacoes, houve_reclassificacao, e dados de logística (ro_nu_pedido, ro_rastreio, ro_status_entrega, ro_transportadora).
6. THE Gerador_Reabertura SHALL aplicar fallback de CPF com prioridade: `base_coverte_prop` > `portabilidade_records` > `relatorio_objetos` (equivalente V2: `vw_base_unificada` > `vw_consulta_siebel_corrente` > `vw_logistica_corrente`).

### Requisito 3: Gerador Aprovisionamento — Migração para Banco V2

**User Story:** Como operador, eu quero que o gerador de homologação de aprovisionamento leia do banco V2 mantendo a inclusão de todas as linhas por `codigo_externo` e a exclusão de "Erro no Aprovisionamento", para que o arquivo reflita dados atualizados.

#### Critérios de Aceitação

1. WHEN o Gerador_Aprovisionamento é executado, THE Gerador_Aprovisionamento SHALL consultar registros usando `QueriesV2.buscar_registros_aprovisionamento()` no Banco_V2, substituindo a consulta direta às tabelas `portabilidade_records` + `base_coverte_prop` + `relatorio_objetos` do Banco_Legado.
2. WHEN o Gerador_Aprovisionamento consulta registros, THE QueriesV2 SHALL incluir TODAS as linhas com `status_ordem = 'Em Aprovisionamento'` OU `status_bilhete = 'Em Aprovisionamento'` (múltiplos `numero_acesso`/`numero_ordem` por `codigo_externo`), excluindo explicitamente linhas com `status_ordem = 'Erro no Aprovisionamento'` ou `status_bilhete = 'Erro no Aprovisionamento'`.
3. WHEN o Gerador_Aprovisionamento filtra registros, THE QueriesV2 SHALL aplicar: últimos 90 dias por data de venda, exclusão de registros com Rejeicao_SMS (motivo_recusa + motivo_cancelamento ambos contendo 'rejei%cliente%sms' combinado com status_ordem = 'Erro no Aprovisionamento').
4. WHEN o Gerador_Aprovisionamento gera o arquivo, THE Gerador_Aprovisionamento SHALL validar registros contra `portabilidade_processamento` (equivalente V2: `vw_portabilidade_tim_corrente`) usando `filtrar_registros_validos()`.
5. THE Gerador_Aprovisionamento SHALL gerar arquivo `.xlsx` com os headers HEADERS_APROV: Cpf, Número de acesso, Número da ordem, Código externo, ICCID, ToutBox, Número do bilhete, Status do bilhete, Operadora doadora, Data da portabilidade, Motivo da recusa, Motivo do cancelamento, Último bilhete de portabilidade?, Status da ordem, Preço da ordem, Data da conclusão da ordem, Motivo de não ter sido consultado, Motivo de não ter sido cancelado, Motivo de não ter sido aberto, Motivo de não ter sido reagendado, Novo status do bilhete, Nova data da portabilidade, Responsável pelo processamento, Data inicial do processamento, Data final do processamento, Registro válido?, Ajustes registro, Número de acesso válido?, Ajustes número de acesso, Status da entrega, Data da entrega, Parâmetro de Identificação, Data Última Atualização Coleta, Tipo de Venda.
6. THE Gerador_Aprovisionamento SHALL aplicar fallback de CPF e codigo_externo com prioridade: `base_coverte_prop` > `portabilidade_records` > `relatorio_objetos` (equivalente V2: `vw_base_unificada` > `vw_consulta_siebel_corrente` > `vw_logistica_corrente`).

### Requisito 4: Gerador Erro Aprovisionamento — Migração para Banco V2

**User Story:** Como operador, eu quero que o gerador de homologação de erro no aprovisionamento leia do banco V2 mantendo a exigência de histórico de entrega e a inclusão de todas as linhas por `codigo_externo`, para que o arquivo reflita dados atualizados.

#### Critérios de Aceitação

1. WHEN o Gerador_Erro_Aprovisionamento é executado, THE Gerador_Erro_Aprovisionamento SHALL consultar registros usando `QueriesV2.buscar_registros_erro_aprovisionamento()` no Banco_V2, substituindo a consulta direta às tabelas `portabilidade_records` + `base_coverte_prop` + `relatorio_objetos` do Banco_Legado.
2. WHEN o Gerador_Erro_Aprovisionamento consulta registros, THE QueriesV2 SHALL incluir TODAS as linhas com `status_ordem = 'Erro no Aprovisionamento'` OU `status_bilhete = 'Erro no Aprovisionamento'` (múltiplos `numero_acesso`/`numero_ordem` por `codigo_externo`), dentro dos últimos 90 dias por data de venda.
3. WHEN o Gerador_Erro_Aprovisionamento filtra registros, THE QueriesV2 SHALL exigir que o registro possua histórico de entrega (dados de `vw_logistica_corrente` ou match via `ObjectsLoader`) para ser incluído no arquivo de saída.
4. WHEN o Gerador_Erro_Aprovisionamento filtra registros, THE QueriesV2 SHALL excluir registros com Rejeicao_SMS (motivo_recusa + motivo_cancelamento ambos contendo 'rejei%cliente%sms' combinado com status_ordem = 'Erro no Aprovisionamento').
5. THE Gerador_Erro_Aprovisionamento SHALL gerar arquivo `.xlsx` com os mesmos headers HEADERS_APROV do Gerador_Aprovisionamento (estrutura idêntica), incluindo dados de logística (ro_iccid, ro_status_entrega, ro_data_entrega, ro_rastreio, ro_transportadora, ro_ultima_ocorrencia).
6. THE Gerador_Erro_Aprovisionamento SHALL incluir contagem de classificações com status "Erro no Aprovisionamento" por `codigo_externo` e indicador `houve_reclassificacao` (SIM/NAO).

### Requisito 5: Gerador Entrega/Baixa — Migração para Banco V2

**User Story:** Como operador, eu quero que o gerador de homologação de entrega/baixa leia do banco V2 mantendo a lógica de CTE para registro mais recente de logística e os filtros de status de entrega problemática, para que o arquivo reflita dados atualizados.

#### Critérios de Aceitação

1. WHEN o Gerador_Entrega_Baixa é executado, THE Gerador_Entrega_Baixa SHALL consultar registros usando `QueriesV2.buscar_registros_entrega_baixa()` no Banco_V2, substituindo a consulta direta às tabelas `base_coverte_prop` + `relatorio_objetos` do Banco_Legado.
2. WHEN o Gerador_Entrega_Baixa consulta registros, THE QueriesV2 SHALL aplicar CTE equivalente ao legado: selecionar o registro mais recente de `relatorio_objetos` por `codigo_externo` (MAX de `updated_at` ou `created_at`), equivalente a usar `vw_logistica_corrente` no V2.
3. WHEN o Gerador_Entrega_Baixa filtra registros, THE QueriesV2 SHALL incluir apenas registros dos últimos 90 dias onde o status de entrega (em `ultima_ocorrencia`, `status` de logística, `status_correios`, `status_loggi` ou `status_entrega_prevista`) contém um dos padrões: 'cancelad', 'baixa', 'remetente', 'aguardando correios', 'extravi'.
4. WHEN o Gerador_Entrega_Baixa filtra registros, THE QueriesV2 SHALL excluir registros com Rejeicao_SMS verificando em `portabilidade_records` (equivalente: `vw_consulta_siebel_corrente`), `portabilidade_processamento` (equivalente: `vw_portabilidade_tim_corrente`) e `base_unificada`.
5. WHEN o Gerador_Entrega_Baixa gera o arquivo, THE Gerador_Entrega_Baixa SHALL aplicar deduplicação primeiro por `codigo_externo` e depois por (cpf, telefone), e normalizar o campo `Numero` extraindo apenas dígitos e movendo texto para `Complemento`.
6. THE Gerador_Entrega_Baixa SHALL gerar arquivo `.xlsx` com as mesmas colunas do Gerador_WPP acrescidas da coluna `Status_Entrega`, incluindo: codigo_externo, cpf, cliente_nome, telefone_portado, data_venda, data_conectada, plano, endereco, numero, complemento, bairro, cidade, uf, cep, ponto_referencia, crivo_vendas, status_entrega.

### Requisito 6: Gerador Consulta — Migração para Banco V2

**User Story:** Como operador, eu quero que o gerador de homologação de consulta leia do banco V2 mantendo a lógica de 2 linhas para Portabilidade e 1 linha para Aquisição, para que o arquivo de consulta reflita dados atualizados.

#### Critérios de Aceitação

1. WHEN o Gerador_Consulta é executado, THE Gerador_Consulta SHALL consultar registros usando `QueriesV2.buscar_registros_consulta()` no Banco_V2, substituindo a consulta direta às tabelas `base_coverte_prop` + `relatorio_objetos` + `portabilidade_records` do Banco_Legado.
2. WHEN o Gerador_Consulta consulta registros, THE QueriesV2 SHALL filtrar vendas com confirmação de entrega onde o status de entrega (em `status_correios`, `status_loggi`, `status_entrega_prevista`, `ro.status` ou `ro.ultima_ocorrencia`) contém 'entregue' ou 'pedido entregue'.
3. WHEN o Gerador_Consulta consulta registros, THE QueriesV2 SHALL aplicar CTE para registro mais recente de `relatorio_objetos` por `codigo_externo` (MAX de `updated_at`/`created_at`), equivalente a usar `vw_logistica_corrente` no V2.
4. WHEN o Gerador_Consulta consulta registros, THE QueriesV2 SHALL excluir registros com Rejeicao_SMS verificando em `portabilidade_processamento` (equivalente: `vw_portabilidade_tim_corrente`) e `portabilidade_records` (equivalente: `vw_consulta_siebel_corrente`).
5. WHEN o Gerador_Consulta processa registros de Portabilidade (telefone_portado válido e diferente de numero_linha), THE Gerador_Consulta SHALL gerar 2 linhas no arquivo: uma com o número portado como `Número de acesso` e outra com o numero_linha como `Número de acesso`.
6. WHEN o Gerador_Consulta processa registros de Aquisição (sem telefone_portado válido ou telefone_portado igual a numero_linha), THE Gerador_Consulta SHALL gerar 1 linha no arquivo com numero_linha como `Número de acesso`.
7. THE Gerador_Consulta SHALL gerar arquivo `.xlsx` com apenas 4 colunas: Cpf, Número de acesso, Número da ordem, Código externo, aplicando deduplicação por (cpf, numero_acesso) e limite de 20.000 registros.

### Requisito 7: Expansão da Classe QueriesV2

**User Story:** Como desenvolvedor, eu quero que a classe QueriesV2 tenha métodos completos para todos os 6 geradores com a mesma lógica de negócio dos scripts legados, para que os geradores possam ser migrados como substituição drop-in.

#### Critérios de Aceitação

1. THE QueriesV2 SHALL fornecer o método `buscar_registros_entrega_baixa(dias_limite=90)` que retorne registros com status de entrega problemática (cancelada, baixa, remetente, aguardando correios, extraviada) a partir de `vw_base_unificada` + `vw_logistica_corrente`, com CTE para registro mais recente de logística por `proposta_isize`.
2. WHEN qualquer método `buscar_registros_*` é chamado, THE QueriesV2 SHALL retornar registros com os mesmos nomes de colunas (aliases) que os geradores de homologação existentes esperam, garantindo compatibilidade drop-in.
4. WHEN qualquer método `buscar_registros_*` é chamado, THE QueriesV2 SHALL aplicar a regra de exclusão de Rejeicao_SMS equivalente ao legado, verificando em `vw_portabilidade_tim_corrente` (MOTIVO_CONFLITO, MOTIVO_CANCELAMENTO) e `vw_consulta_siebel_corrente` (status_bilhete, motivo_recusa, motivo_cancelamento).
5. WHEN qualquer método `buscar_registros_*` é chamado e a view necessária não existe no Banco_V2, THE QueriesV2 SHALL registrar warning no log e retornar lista vazia.
6. THE QueriesV2 SHALL aplicar fallback de CPF e codigo_externo com a mesma prioridade do legado: `vw_base_unificada` > `vw_consulta_siebel_corrente` > `vw_logistica_corrente` (equivalente a `base_coverte_prop` > `portabilidade_records` > `relatorio_objetos`).

### Requisito 8: Manutenção da Tabela base_coverte_prop no Banco Legado

**User Story:** Como operador, eu quero que o processamento V2 continue atualizando a tabela `base_coverte_prop` no banco legado da rede, para que os plugins externos que a consomem continuem funcionando sem alteração.

#### Critérios de Aceitação

1. WHEN a planilha COVERTE BASE PROP é importada pelo Processador_Completo, THE Pipeline_V2 SHALL gravar os dados na tabela `base_coverte_prop` do Banco_Legado usando o fluxo existente (`processar_excel_unificado.py`), mantendo a tabela atualizada na rede SMB para consumo dos plugins.
2. WHEN a planilha COVERTE BASE PROP é importada pelo Processador_Completo, THE Pipeline_V2 SHALL gravar os dados normalizados nas tabelas do Banco_V2 usando o Importador V2 na mesma execução.
3. THE Pipeline_V2 SHALL executar a gravação no Banco_Legado antes da gravação no Banco_V2 para garantir que plugins tenham acesso aos dados atualizados o mais cedo possível.
4. IF a gravação no Banco_V2 falha, THEN THE Pipeline_V2 SHALL registrar o erro no log e continuar o processamento sem afetar a gravação no Banco_Legado.
5. IF a gravação no Banco_Legado falha, THEN THE Pipeline_V2 SHALL registrar o erro como crítico e interromper a etapa de importação COVERTE, pois plugins dependem dessa tabela na rede.
6. WHEN o pipeline conclui a etapa de backup (ETAPA 6), THE Pipeline_V2 SHALL replicar o Banco_Legado atualizado para a rede SMB (07 Backoffice) garantindo que a `base_coverte_prop` esteja disponível para os plugins na rede.

### Requisito 9: Atualização do Cache Unificado Após Importação

**User Story:** Como operador, eu quero que o cache materializado (`cache_base_unificada`) seja atualizado após cada importação de dados, para que as consultas dos geradores de homologação retornem dados atualizados.

#### Critérios de Aceitação

1. WHEN um lote de registros é importado no Banco_V2, THE Pipeline_V2 SHALL atualizar a Cache_Unificada para cada `proposta_isize` afetada no lote.
2. WHEN o Processador_Completo conclui todas as importações (ETAPA 1 e ETAPA 2), THE Pipeline_V2 SHALL executar uma atualização completa da Cache_Unificada antes de iniciar a geração de homologação (ETAPA 4).
3. IF a atualização da Cache_Unificada falha para um registro específico, THEN THE Pipeline_V2 SHALL registrar o erro no log e continuar a atualização dos demais registros.
4. THE Pipeline_V2 SHALL registrar métricas da atualização do cache (quantidade atualizada, quantidade com erro, tempo de execução) na tabela `metricas_processamento` do Banco_V2.

### Requisito 10: Integração do Reprocessamento de Endereços Inválidos (Ida e Volta)

**User Story:** Como operador, eu quero que o reprocessamento de endereços inválidos (atualmente no projeto externo `3F_Endereco_invalido`) seja integrado como etapa do pipeline com fluxo de ida e volta — gerando a base para correção, corrigindo os endereços via proxy dinâmica, e reimportando o arquivo corrigido de volta no processamento — para que vendas com endereço incorreto sejam corrigidas e os dados corrigidos sejam incorporados automaticamente com integridade total de todas as linhas.

#### Critérios de Aceitação

1. WHEN o Processador_Completo executa o pipeline, THE Pipeline_V2 SHALL incluir uma etapa de reprocessamento de endereços após a geração de homologação e antes do backup.
2. WHEN a etapa de reprocessamento é executada, THE Reprocessador_Endereco SHALL consultar o Banco_V2 usando a query `TIM_REPROCESSAMENTO` para obter registros dos últimos 180 dias que necessitam correção de endereço.
3. WHEN um registro com endereço inválido é identificado, THE Reprocessador_Endereco SHALL validar e corrigir o endereço usando APIs de CEP e geocodificação reversa com fuzzy matching, roteando as requisições através do mecanismo de proxy dinâmica.
4. THE Reprocessador_Endereco SHALL implementar um mecanismo de proxy dinâmica que: (a) mantém um pool de proxies válidas, (b) testa e valida proxies antes do uso, (c) rotaciona automaticamente entre proxies válidas a cada requisição ou grupo de requisições, (d) descarta proxies que falham e substitui por outras do pool, (e) registra no log quais proxies foram usadas e suas taxas de sucesso/falha.
5. WHEN os endereços são corrigidos, THE Reprocessador_Endereco SHALL salvar o arquivo corrigido com sufixo `_pronto_tratamento.xlsx` na Pasta_Saida, contendo os dados completos com endereços corrigidos e a coluna "Tipo entrega".
6. THE arquivo `_pronto_tratamento.xlsx` SHALL conter TODAS as linhas processadas com integridade total: cada linha DEVE ter todos os campos de endereço preenchidos — ou com o dado original (quando já estava correto) ou com o dado corrigido e normalizado. Nenhuma linha pode ter campos de endereço vazios no arquivo de saída.
7. WHEN o arquivo `_pronto_tratamento.xlsx` é gerado, THE Processador_Completo SHALL reimportar automaticamente os dados corrigidos de volta no Banco_V2, atualizando os campos de endereço (endereco, numero, complemento, bairro, cidade, uf, cep) nas tabelas normalizadas correspondentes (`clientes`) como nova versão dos registros afetados.
8. WHEN os dados corrigidos são reimportados no Banco_V2, THE Pipeline_V2 SHALL registrar a importação como um lote de tipo 'reprocessamento' na tabela `lotes_importacao` com referência ao arquivo `_pronto_tratamento.xlsx`.
9. WHEN os dados corrigidos são reimportados no Banco_V2, THE Pipeline_V2 SHALL atualizar a Cache_Unificada para cada `proposta_isize` afetada pela correção de endereço.
10. WHEN o reprocessamento é executado com dados do banco (modo `--banco`), THE Reprocessador_Endereco SHALL considerar o período dos últimos 180 dias a partir da data de execução.
11. THE Reprocessador_Endereco SHALL adicionar a coluna "Tipo entrega" a cada registro corrigido, classificando como "Express" quando a entrega ocorre em até 2 dias da data de venda, e como "Correios" quando a transportadora é Correios ou o prazo excede 2 dias.
12. IF as APIs de CEP ou geocodificação estão indisponíveis E todas as proxies do pool falharam, THEN THE Reprocessador_Endereco SHALL registrar o erro no log e manter o endereço original sem interromper o pipeline, garantindo que a linha permaneça no arquivo com os dados originais.
13. THE Pipeline_V2 SHALL permitir executar a etapa de reprocessamento de endereços de forma independente via flag `--apenas-reprocessamento`.

### Requisito 11: Integração do Módulo de Reprocessamento no Projeto

**User Story:** Como desenvolvedor, eu quero que o código de reprocessamento de endereços do projeto externo `3F_Endereco_invalido` seja integrado como módulo dentro do projeto principal, incluindo o mecanismo de proxy dinâmica e garantia de integridade de dados, para que não dependa de caminhos externos e possa ser mantido junto com o pipeline.

#### Critérios de Aceitação

1. THE Pipeline_V2 SHALL incluir o módulo de reprocessamento de endereços como pacote dentro de `src/reprocessamento/` no projeto principal.
2. THE módulo de reprocessamento SHALL expor uma função principal que aceite como parâmetros: caminho do Banco_V2, período em dias (padrão 180), diretório de saída e configuração de proxies (lista de proxies ou arquivo de proxies).
3. WHEN o módulo é executado, THE Reprocessador_Endereco SHALL consultar o Banco_V2 diretamente usando as views `vw_base_unificada` e `vw_logistica_corrente` ao invés de exportar para Excel intermediário.
4. THE módulo de reprocessamento SHALL incluir o gerenciador de proxy dinâmica (`ProxyManager`) que: (a) carrega proxies de um arquivo ou lista configurável, (b) valida proxies com teste de conectividade antes do uso, (c) rotaciona entre proxies válidas de forma dinâmica, (d) remove proxies que falham consecutivamente e as substitui por outras do pool, (e) expõe métricas de uso (proxies ativas, falhas, taxa de sucesso).
5. THE módulo de reprocessamento SHALL manter a lógica de classificação de "Tipo entrega" (Express vs Correios) conforme implementada no projeto original.
6. THE módulo de reprocessamento SHALL garantir integridade total de todas as linhas no arquivo de saída: cada registro DEVE ter TODOS os campos preenchidos — campos de endereço com o dado corrigido/normalizado quando houve correção, ou com o dado original quando já estava correto. Nenhum campo de endereço pode ficar vazio no arquivo final.
7. WHEN um campo de endereço não pode ser corrigido (API indisponível, proxy esgotada, endereço não encontrado), THE Reprocessador_Endereco SHALL manter o valor original do campo, garantindo que a linha permaneça completa no arquivo de saída.
8. IF o módulo de reprocessamento é executado de forma independente (fora do pipeline), THE Reprocessador_Endereco SHALL aceitar flag `--banco` para operar no modo banco com período de 180 dias.

### Requisito 12: Ordem de Etapas do Pipeline Refatorado

**User Story:** Como operador, eu quero que o pipeline mantenha uma ordem clara e documentada de etapas, para que o processamento seja previsível e cada etapa tenha acesso aos dados necessários.

#### Critérios de Aceitação

1. THE Processador_Completo SHALL executar as etapas na seguinte ordem obrigatória: (a) ETAPA 1 — Importar COVERTE BASE PROP da rede SMB para `base_coverte_prop` no Banco_Legado e tabelas normalizadas no Banco_V2, (b) ETAPA 1b — Coletar BS_VENDA_DU, (c) ETAPA 2 — Processar arquivos das pastas de entrada (CSV portabilidade, Excel COVERTE, GROSS, relatórios de objetos, relatórios de faturamento, TIM pré-controle, estornos) gravando em ambos os bancos, (d) ETAPA 3 — Fallback COVERTE se SMB falhou na ETAPA 1, (e) ETAPA 3b — Atualizar Cache_Unificada no Banco_V2, (f) ETAPA 4 — Executar os 6 geradores de homologação a partir do Banco_V2 (WPP, Reabertura, Aprovisionamento, Erro Aprovisionamento, Entrega/Baixa, Consulta), (g) ETAPA 5 — Reprocessar endereços inválidos (gerar base → corrigir endereços → reimportar dados corrigidos no Banco_V2 → atualizar Cache_Unificada), (h) ETAPA 6 — Backup e replicação para rede.
2. WHEN uma etapa falha, THE Processador_Completo SHALL registrar o erro e prosseguir para a próxima etapa, exceto quando a falha é na ETAPA 1 de gravação no Banco_Legado (crítica para plugins).
3. THE Processador_Completo SHALL registrar o início e fim de cada etapa na tabela `execucoes_processamento` do Banco_V2 com timestamp, status e contagem de registros.
4. THE Processador_Completo SHALL suportar as flags existentes (`--apenas-bases`, `--apenas-homologacao`, `--skip-excel`, `--apenas-d1-entrega`) e adicionar `--apenas-reprocessamento` e `--skip-reprocessamento`.

### Requisito 13: Compatibilidade de Formato nos Arquivos de Saída

**User Story:** Como operador, eu quero que os arquivos de homologação gerados a partir do Banco_V2 tenham exatamente as mesmas colunas e formatos que os arquivos atuais, para que os processos downstream (importação no Siebel, envio WPP, backoffice) continuem funcionando.

#### Critérios de Aceitação

1. IF um campo retornado pelo QueriesV2 é NULL no Banco_V2 mas era preenchido no Banco_Legado, THEN THE Pipeline_V2 SHALL tratar o valor como vazio (string vazia) no arquivo de saída para manter compatibilidade de formato.
2. THE Pipeline_V2 SHALL gerar os arquivos de homologação com encoding `utf-8-sig` para compatibilidade com Excel e separador `;` para arquivos CSV.
3. WHEN o Gerador_Aprovisionamento ou Gerador_Erro_Aprovisionamento gera o arquivo, THE Pipeline_V2 SHALL gerar CSV intermediário com separador `;` e converter para `.xlsx` mantendo os headers HEADERS_APROV.
4. WHEN o Gerador_Consulta gera o arquivo, THE Pipeline_V2 SHALL gerar `.xlsx` com apenas 4 colunas (Cpf, Número de acesso, Número da ordem, Código externo) sem colunas adicionais.
5. THE Pipeline_V2 SHALL copiar os arquivos de homologação gerados para a Pasta_Saida (`config.PASTA_SAIDA_HOMOLOGACAO`) após a geração.
6. THE Pipeline_V2 SHALL garantir integridade total de todas as linhas em todos os arquivos de saída: cada linha DEVE ter TODAS as colunas preenchidas — com o dado existente quando correto, ou com o dado corrigido e normalizado. Nenhuma linha pode ser emitida com campos obrigatórios vazios; se um campo não puder ser preenchido, a linha inteira deve ser excluída do arquivo e registrada no log como registro incompleto.

### Requisito 14: Registro de Execução e Métricas do Pipeline V2

**User Story:** Como operador, eu quero que cada execução do pipeline V2 seja registrada com métricas detalhadas, para que eu possa monitorar performance e identificar problemas.

#### Critérios de Aceitação

1. WHEN o Processador_Completo inicia, THE Pipeline_V2 SHALL criar um registro na tabela `execucoes_processamento` do Banco_V2 com tipo "processamento_completo" e status "em_andamento".
2. WHEN cada etapa do pipeline é concluída, THE Pipeline_V2 SHALL registrar métricas na tabela `metricas_processamento` incluindo: nome da etapa, quantidade de registros processados, tempo de execução em milissegundos e registros com erro.
3. WHEN o pipeline conclui todas as etapas, THE Pipeline_V2 SHALL atualizar o registro de execução com status "concluido", contagem total de registros processados e duração total em segundos.
4. IF o pipeline é interrompido por erro, THEN THE Pipeline_V2 SHALL atualizar o registro de execução com status "erro", etapa onde ocorreu a falha e detalhes do erro.

### Requisito 15: Fallback para Banco Legado Durante Transição

**User Story:** Como operador, eu quero que o pipeline tenha um mecanismo de fallback para o banco legado caso o Banco_V2 apresente problemas, para que o processamento não seja interrompido durante o período de transição.

#### Critérios de Aceitação

1. IF o Banco_V2 não está acessível ou apresenta erro de integridade ao iniciar o pipeline, THEN THE Pipeline_V2 SHALL registrar warning no log e gerar os arquivos de homologação a partir do Banco_Legado usando os geradores atuais.
2. WHEN o fallback para Banco_Legado é ativado, THE Pipeline_V2 SHALL registrar o evento na tabela `execucoes_processamento` com detalhes do motivo do fallback.
3. THE Pipeline_V2 SHALL suportar flag `--forcar-legado` para forçar a geração de homologação a partir do Banco_Legado independentemente do estado do Banco_V2.
4. THE Pipeline_V2 SHALL suportar flag `--forcar-v2` para forçar a geração de homologação exclusivamente a partir do Banco_V2, falhando se o banco não estiver disponível.
