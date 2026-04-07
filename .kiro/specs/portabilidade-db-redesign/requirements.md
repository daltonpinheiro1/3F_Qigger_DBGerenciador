# Documento de Requisitos — Redesign do Banco de Dados de Portabilidade

## Introdução

O sistema 3F Qigger DB Gerenciador processa registros de portabilidade de números telefônicos utilizando um motor de decisão com 23 regras. O banco de dados atual (SQLite) apresenta falhas críticas: dados não são atualizados corretamente durante o "Reprocessar Completo", dados tratados não são salvos adequadamente, e o campo `id_proposta_isize` frequentemente contém CPF ao invés do valor correto. Este documento especifica os requisitos para a criação de um novo banco de dados do zero, com normalização adequada, auditoria completa, rastreabilidade de importações, escalabilidade e — fundamentalmente — uma arquitetura de imutabilidade onde registros nunca são atualizados (UPDATE), apenas inseridos (INSERT) como novas versões, preservando o histórico completo de movimentação de cada proposta.

O novo schema deve acomodar dados provenientes de 7 fontes de dados distintas: Propostas de Venda (COVERTE BASE PROP), Base de Portabilidade TIM, Relatório GROSS, Relatório de Objetos (Logística), Resultado GROSS, Propostas Backoffice e Consulta Siebel.

## Glossário

- **Sistema_DB**: O novo banco de dados SQLite redesenhado para portabilidade
- **Motor_Decisao**: Engine que aplica as 23 regras de negócio (triggers.xlsx) aos registros
- **Processador_Completo**: Orquestrador principal que executa o pipeline de processamento (processar_completo.py)
- **Importador**: Módulo responsável por importar arquivos CSV/Excel para o banco de dados
- **Gerador_Homologacao**: Módulos que geram arquivos de saída (WPP, aprovisionamento, reabertura, consulta, erro)
- **Registro_Portabilidade**: Registro individual de portabilidade com CPF, número de acesso, número de ordem e código externo
- **Base_Coverte_Prop**: Planilha Excel principal (COVERTE BASE PROP — Arquivo 1) com 76 colunas de dados de propostas de venda
- **Base_Portabilidade_TIM**: Planilha Excel (Arquivo 2) com 39 colunas de status de portabilidade da operadora TIM
- **Relatorio_GROSS**: Planilha Excel (Arquivo 3) com 8 colunas de dados de ativação GROSS
- **Relatorio_Objetos**: Planilha Excel (Arquivo 4) com 78 colunas de dados logísticos completos (rastreio, entrega, transportadora)
- **Resultado_GROSS**: Planilha Excel (Arquivo 5) com 8 colunas de resultados de processamento GROSS
- **Propostas_Backoffice**: Planilha Excel (Arquivo 6) com 31 colunas de acompanhamento de propostas pelo backoffice
- **Consulta_Siebel**: Arquivo CSV (Arquivo 7) com 29 colunas de resultados de consulta de portabilidade no Siebel
- **Proposta_Isize**: Identificador único da proposta no sistema iSize — chave principal de associação entre arquivos
- **Chave_Composta**: Combinação de Proposta_Isize + telefone_portado + numero_ordem que identifica unicamente um registro
- **Lote_Importacao**: Conjunto de registros importados de um mesmo arquivo em uma mesma execução
- **Trilha_Auditoria**: Registro cronológico de todas as operações realizadas sobre os dados
- **Versao_Registro**: Número sequencial que identifica cada versão de um registro, incrementado a cada nova inserção
- **Registro_Corrente**: A versão mais recente (maior versao) de um registro, utilizada como visão ativa
- **Snapshot_Temporal**: Estado completo de um registro em um ponto específico no tempo, preservado por inserção de nova versão

## Requisitos

### Requisito 1: Esquema Normalizado do Banco de Dados com Suporte a Todas as Fontes

**User Story:** Como desenvolvedor, eu quero um banco de dados normalizado com separação clara de responsabilidades que acomode todos os campos das 7 fontes de dados, para que os dados sejam consistentes, completos e sem redundância.

#### Critérios de Aceitação

1. THE Sistema_DB SHALL armazenar dados de clientes em uma tabela dedicada `clientes` contendo: CPF (chave natural), nome_cliente, data_nascimento, nome_mae, endereco, numero, complemento, bairro, cidade, uf, cep, ponto_referencia, ddd_1, telefone_1, ddd_2, telefone_2, email, score, versao, created_at.
2. THE Sistema_DB SHALL armazenar dados de propostas de venda em uma tabela dedicada `propostas` contendo: proposta_isize (chave primária de negócio), cpf (FK para clientes), data_venda, produto, plano, forma_pagamento, vencimento, tipo_chip, conta_online, vivo_pay, app_adicional, plataforma, nome_equipe, nome_vendedor, login_externo, nome_supervisor, matricula_discador, avulsa, sms_previo, observacoes, versao, created_at.
3. THE Sistema_DB SHALL armazenar dados de status de venda em uma tabela `status_venda` contendo: proposta_isize (FK para propostas), status_venda, motivo_rejeicao_cancelamento, flag, auditoria, qualidade, conectada, data_conectada, versao, created_at.
4. THE Sistema_DB SHALL armazenar dados de portabilidade em uma tabela `portabilidade` contendo: proposta_isize (FK para propostas), telefone_portabilidade, numero_linha, portabilidade_status, complemento_portabilidade, portabilidade_antecipada, data_marcacao_port_antecipada, quem_marcou_port_antecipada, versao, created_at.
5. THE Sistema_DB SHALL armazenar dados de portabilidade TIM em uma tabela `portabilidade_tim` contendo: proposta_isize (FK para propostas), acesso, acesso_temporario, ddd, data_solicitacao, mes_solicitacao, data_ativacao, mes_ativacao, data_conclusao, sky_contrato, sky_cliente, protocolo, operadora_n1, tipo_pre_pos_controle, tecnologia, voz_dados, doadora, receptora, tipo, status, tipo_segmento_1, tipo_segmento_2, tipo_familia_plano, nivel_plano, canal_n0, canal_n1, canal_n2, canal_n3, canal_n4, grupo_economico, custcode, cpf_cnpj, portabilidade, motivo_conflito, motivo_cancelamento, self_portin, canal_portabilidade, tentativas, cart_canal_n1, cart_canal_n2, versao, created_at.
6. THE Sistema_DB SHALL armazenar dados de logística em uma tabela `logistica` contendo: proposta_isize (FK para propostas), nu_pedido, rastreio, iccid, numero_pedido_marketplace, nota_fiscal, serie_nf, data_emissao_nf, chave_nota_fiscal, valor_nf, valor_frete, id_canal_venda, id_warehouse, id_erp, id_transportadora, transportadora, id_servico, nome_servico, destinatario, documento, email, telefone, cidade, uf, cep, data_insercao, data_primeiro_patch, data_ultimo_patch, data_postagem, previsao_entrega, data_prometida, prazo_dias_corridos, prazo_dias_uteis, prazo_efetivo, status, tentativas_entrega, data_entrega, ultima_ocorrencia, data_ultima_ocorrencia, local_ultima_ocorrencia, cidade_ultima_ocorrencia, estado_ultima_ocorrencia, ultima_ocorrencia_cronologica, motivo_devolucao, retorno_fluxo, protocolo_logistica, motivo_abertura_protocolo, status_protocolo, reversa, codigo_coleta_postagem, cd, dispatch, versao, created_at.
7. THE Sistema_DB SHALL armazenar dados de GROSS em uma tabela `gross` contendo: proposta_isize (FK para propostas), acesso, ddd, custcode, operadora_n1, classificacao_cr, data_gross, nome_pdv, mes, versao, created_at.
8. THE Sistema_DB SHALL armazenar dados de resultado GROSS em uma tabela `resultado_gross` contendo: proposta_isize (FK para propostas), numero_acesso, data_gross, cpf, iccid, data_arquivo, arquivo_origem, resultado, versao, created_at.
9. THE Sistema_DB SHALL armazenar dados de backoffice em uma tabela `backoffice` contendo: proposta_isize (FK para propostas), pedido, blue_chip, data_venda, tipo_plano, plano_ativado, plano_fidelizado, portabilidade, numero_provisorio, numero_portado, cpf, nome_cliente, endereco, cep, uf, login_vendedor, vendedor, login_bko, bko, data_input_siebel, iccid, data_envio_chip, data_entrega_chip, data_abertura_bp, data_conclusao_bp, status_pedido, detalhe_status, data_atualizacao_status, tempo_tratamento_total, obs_bo, protocolo_conectada, nome_equipe, versao, created_at.
10. THE Sistema_DB SHALL armazenar dados de consulta Siebel em uma tabela `consulta_siebel` contendo: proposta_isize (FK para propostas), cpf, numero_acesso, numero_ordem, codigo_externo, numero_temporario, bilhete_temporario, numero_bilhete, status_bilhete, operadora_doadora, data_portabilidade, motivo_recusa, motivo_cancelamento, ultimo_bilhete, status_ordem, preco_ordem, data_conclusao_ordem, motivo_nao_consultado, motivo_nao_cancelado, motivo_nao_aberto, motivo_nao_reagendado, novo_status_bilhete, nova_data_portabilidade, responsavel_processamento, data_inicial_processamento, data_final_processamento, registro_valido, ajustes_registro, numero_acesso_valido, ajustes_numero_acesso, versao, created_at.
11. THE Sistema_DB SHALL armazenar dados de Bluechip em uma tabela `bluechip` contendo: proposta_isize (FK para propostas), bluechip_status, bluechip_data_status, resposta_envio_pedido, pedido_bluechip, bluechip_data_enviado, data_maxima_prevista_entrega, status_entrega_prevista, cd_bluechip, remessa_bluechip, qtd_remessas, versao, created_at.
12. THE Sistema_DB SHALL armazenar dados de rastreio de entregas em uma tabela `rastreio_entregas` contendo: proposta_isize (FK para propostas), rastreio_correios, rastreio_loggi, data_status_correios, status_correios, data_status_loggi, status_loggi, versao, created_at.
13. THE Sistema_DB SHALL armazenar regras de decisão em uma tabela `regras_decisao` espelhando o triggers.xlsx com campo `regra_id` como chave primária.
14. THE Sistema_DB SHALL armazenar resultados de aplicação de regras em uma tabela `decisoes` com referências para `consulta_siebel` e `regras_decisao`, incluindo versao e created_at.
15. THE Sistema_DB SHALL armazenar templates de comunicação WhatsApp em uma tabela `templates_wpp` com mapeamento para tipos de comunicação.
16. THE Sistema_DB SHALL armazenar dados de internet e TV em uma tabela `servicos_adicionais` contendo: proposta_isize (FK para propostas), vivo_internet, vivo_tv, id_play_vivo, versao, created_at.
17. THE Sistema_DB SHALL armazenar dados de processamento do robô em uma tabela `robo_processamento` contendo: proposta_isize (FK para propostas), robo_inicio_proc, robo_fim_proc, versao, created_at.
18. THE Sistema_DB SHALL impor restrições de integridade referencial (FOREIGN KEY) entre todas as tabelas relacionadas, usando proposta_isize como chave de ligação principal.
19. THE Sistema_DB SHALL utilizar a Chave_Composta (proposta_isize, telefone_portabilidade, numero_ordem) como constraint UNIQUE combinada com versao na tabela `portabilidade`.

### Requisito 2: Registro e Rastreabilidade de Importações

**User Story:** Como operador, eu quero que cada importação de arquivo seja registrada com data, nome do arquivo e origem, para que eu possa rastrear a procedência de cada dado.

#### Critérios de Aceitação

1. WHEN um arquivo é importado, THE Importador SHALL criar um registro na tabela `lotes_importacao` contendo: nome do arquivo, caminho de origem, tipo do arquivo (coverte_prop, portabilidade_tim, gross, relatorio_objetos, resultado_gross, backoffice, consulta_siebel), data/hora da importação, quantidade de registros e hash SHA-256 do arquivo.
2. THE Sistema_DB SHALL associar cada registro importado ao respectivo `lote_importacao_id` via foreign key em todas as tabelas de dados.
3. WHEN o mesmo arquivo (mesmo hash SHA-256) é importado novamente, THE Importador SHALL rejeitar a importação duplicada e registrar o evento na Trilha_Auditoria.
4. THE Sistema_DB SHALL armazenar o conteúdo original do arquivo importado (caminho ou cópia) na tabela `arquivos_importados` para referência futura.
5. WHEN um Lote_Importacao é consultado, THE Sistema_DB SHALL retornar a lista completa de registros associados àquele lote com seus status de processamento.

### Requisito 3: Processamento Completo Confiável com Inserção Exclusiva

**User Story:** Como operador, eu quero que o comando "Reprocessar Completo" insira novos registros versionados de forma atômica sem alterar registros existentes, para que não haja dados inconsistentes e o histórico completo seja preservado.

#### Critérios de Aceitação

1. WHEN o Processador_Completo inicia uma execução, THE Sistema_DB SHALL criar um registro na tabela `execucoes_processamento` com timestamp de início, parâmetros utilizados e status "em_andamento".
2. WHEN o Processador_Completo processa um lote de registros, THE Sistema_DB SHALL executar todas as operações de escrita dentro de uma transação atômica (BEGIN/COMMIT).
3. IF uma transação falha durante o processamento, THEN THE Sistema_DB SHALL executar ROLLBACK completo e registrar o erro na tabela `execucoes_processamento` com detalhes da falha.
4. WHEN o Processador_Completo conclui com sucesso, THE Sistema_DB SHALL atualizar o registro de execução com timestamp de fim, contagem de registros processados e status "concluido".
5. THE Processador_Completo SHALL processar as etapas na ordem obrigatória: base_coverte_prop, arquivos da pasta, fallback COVERTE, homologação, backup.
6. WHEN um registro existente recebe novos dados, THE Sistema_DB SHALL inserir uma nova linha com versao incrementada (versao anterior + 1) ao invés de executar UPDATE na linha existente.
7. THE Sistema_DB SHALL validar a integridade do banco (PRAGMA integrity_check) após cada etapa de processamento e registrar o resultado.
8. THE Sistema_DB SHALL manter views que filtram apenas o Registro_Corrente (MAX(versao)) de cada entidade para uso operacional.

### Requisito 4: Validação e Correção de Proposta_Isize

**User Story:** Como desenvolvedor, eu quero que o campo Proposta_Isize seja validado automaticamente na importação, para que não contenha CPF ou valores incorretos.

#### Critérios de Aceitação

1. WHEN um registro é importado com Proposta_Isize, THE Importador SHALL validar que o valor possui formato correto (diferente de CPF: 11 dígitos numéricos puros).
2. IF o Proposta_Isize contém um valor com exatamente 11 dígitos numéricos (padrão CPF), THEN THE Importador SHALL buscar o Proposta_Isize correto na tabela `propostas` usando as estratégias de fallback: CPF, numero_ordem, numero_acesso, remessa_bluechip, pedido_bluechip, codigo_externo, telefone_portado (nesta ordem de prioridade).
3. IF o Proposta_Isize correto é encontrado via fallback, THEN THE Importador SHALL corrigir o valor e registrar a correção na Trilha_Auditoria com valor original e valor corrigido.
4. IF o Proposta_Isize correto não é encontrado via fallback, THEN THE Importador SHALL marcar o registro com status "proposta_isize_pendente" e registrar na tabela `registros_pendentes`.
5. THE Sistema_DB SHALL manter um índice único na coluna proposta_isize da tabela `propostas` para garantir unicidade.

### Requisito 5: Planilha Unificada com Chaves de Associação entre Fontes

**User Story:** Como operador, eu quero uma visão unificada dos dados com todas as chaves de associação entre os 7 arquivos, para que eu possa consultar qualquer registro de forma rápida e cruzar informações entre fontes.

#### Critérios de Aceitação

1. THE Sistema_DB SHALL manter uma view materializada `vw_base_unificada` que consolida dados de `clientes`, `propostas`, `status_venda`, `portabilidade`, `portabilidade_tim`, `logistica`, `gross`, `resultado_gross`, `backoffice`, `consulta_siebel`, `bluechip`, `rastreio_entregas`, `servicos_adicionais` e `decisoes`, filtrando apenas o Registro_Corrente (MAX(versao)) de cada tabela.
2. THE Sistema_DB SHALL utilizar as seguintes chaves de associação entre fontes: proposta_isize (Arquivo 1) = codigo_externo (Arquivo 7) = proposta (Arquivo 5) = pedido (Arquivo 6) = id_auxiliar1 (Arquivo 4) como chave primária de ligação.
3. THE Sistema_DB SHALL utilizar telefone_portabilidade (Arquivo 1) = acesso (Arquivo 2) = numero_acesso (Arquivo 7) = numero_acesso (Arquivo 5) = numero_portado (Arquivo 6) como chave secundária de ligação por telefone portado.
4. THE Sistema_DB SHALL utilizar numero_os (Arquivo 1) = numero_ordem (Arquivo 7) = id_erp (Arquivo 4) como chave de ligação por número de ordem/OS.
5. THE Sistema_DB SHALL utilizar numero_linha (Arquivo 1) = acesso_temporario (Arquivo 2) = numero_provisorio (Arquivo 6) = numero_temporario (Arquivo 7) como chave de ligação por número provisório/temporário.
6. THE Sistema_DB SHALL utilizar CPF como chave de ligação universal presente em todas as 7 fontes (CPF, CPF_CNPJ, Documento).
7. THE Sistema_DB SHALL utilizar acesso (Arquivo 3 GROSS) como chave de ligação com telefones portados das demais fontes.
8. THE Sistema_DB SHALL criar índices compostos otimizados para busca por proposta_isize, telefone_portabilidade, numero_ordem, numero_linha e CPF individualmente e em combinação.
9. WHEN qualquer tabela fonte recebe nova inserção, THE Sistema_DB SHALL atualizar a view materializada (tabela `cache_base_unificada`) com os dados consolidados do Registro_Corrente.
10. THE Sistema_DB SHALL permitir busca por qualquer uma das chaves de associação retornando resultados em tempo adequado para o volume atual de dados.

### Requisito 6: Trilha de Auditoria Completa

**User Story:** Como auditor, eu quero um registro completo de todas as operações realizadas no banco de dados, para que eu possa rastrear qualquer alteração.

#### Critérios de Aceitação

1. WHEN um registro é inserido em qualquer tabela principal, THE Sistema_DB SHALL registrar a operação na tabela `auditoria` com: tabela afetada, tipo de operação (INSERT), valores inseridos, versao do registro, timestamp e identificador do processo/lote de importação.
2. THE Sistema_DB SHALL implementar triggers SQLite para captura automática de inserções nas tabelas: `clientes`, `propostas`, `status_venda`, `portabilidade`, `portabilidade_tim`, `logistica`, `gross`, `resultado_gross`, `backoffice`, `consulta_siebel`, `bluechip`, `rastreio_entregas`, `servicos_adicionais` e `decisoes`.
3. THE Sistema_DB SHALL registrar na Trilha_Auditoria cada aplicação de regra do Motor_Decisao com: registro afetado, regra aplicada, resultado da decisão e tempo de execução em milissegundos.
4. THE Sistema_DB SHALL manter a Trilha_Auditoria por tempo indeterminado, sem exclusão automática de registros antigos.
5. WHEN a tabela `auditoria` é consultada, THE Sistema_DB SHALL suportar filtros por: período, tabela, tipo de operação, versao e identificador do registro.

### Requisito 7: Índices Otimizados para Performance

**User Story:** Como desenvolvedor, eu quero índices otimizados para as consultas mais frequentes, para que o processamento em lote seja eficiente.

#### Critérios de Aceitação

1. THE Sistema_DB SHALL criar índices individuais nas colunas: CPF (tabela clientes), proposta_isize (tabela propostas), codigo_externo (tabela consulta_siebel), numero_ordem (tabela consulta_siebel), telefone_portabilidade (tabela portabilidade), acesso (tabela portabilidade_tim), numero_portado (tabela backoffice), nu_pedido (tabela logistica), status_bilhete (tabela consulta_siebel).
2. THE Sistema_DB SHALL criar índices compostos para as consultas de matching de regras: (status_bilhete, operadora_doadora, motivo_recusa) na tabela consulta_siebel.
3. THE Sistema_DB SHALL criar índices compostos para busca de Registro_Corrente: (proposta_isize, versao DESC) em todas as tabelas versionadas.
4. THE Sistema_DB SHALL criar índices para ordenação temporal: (created_at DESC) nas tabelas principais.
5. THE Sistema_DB SHALL executar PRAGMA optimize periodicamente para manter estatísticas de índices atualizadas.
6. THE Sistema_DB SHALL utilizar as configurações de performance: WAL journal mode, cache de 128MB, mmap de 512MB, temp_store em memória.

### Requisito 8: Backup e Recuperação

**User Story:** Como operador, eu quero um sistema de backup confiável com replicação para rede, para que eu possa recuperar dados em caso de falha.

#### Critérios de Aceitação

1. WHEN o Processador_Completo conclui com sucesso, THE Sistema_DB SHALL criar um backup local usando o comando nativo `sqlite3 .backup` para garantir snapshot consistente.
2. THE Sistema_DB SHALL replicar o backup para o compartilhamento SMB (07 Backoffice) via cópia segura (backup local temporário seguido de cópia para rede).
3. THE Sistema_DB SHALL manter os últimos 10 backups locais e remover automaticamente os mais antigos.
4. THE Sistema_DB SHALL registrar cada operação de backup na tabela `historico_backups` com: timestamp, tamanho do arquivo, destino (local/rede) e status (sucesso/falha).
5. IF o compartilhamento SMB não está montado, THEN THE Sistema_DB SHALL tentar montar automaticamente e, em caso de falha, registrar aviso sem interromper o processamento.
6. WHEN um backup é restaurado, THE Sistema_DB SHALL validar a integridade do banco restaurado antes de disponibilizá-lo para uso.

### Requisito 9: Nomenclatura Consistente e Versionamento do Schema

**User Story:** Como desenvolvedor, eu quero uma nomenclatura padronizada e controle de versão do schema, para que migrações sejam seguras e previsíveis.

#### Critérios de Aceitação

1. THE Sistema_DB SHALL utilizar nomenclatura snake_case para todas as tabelas, colunas e índices.
2. THE Sistema_DB SHALL prefixar índices com `idx_`, views com `vw_` e triggers com `trg_`.
3. THE Sistema_DB SHALL manter uma tabela `schema_versao` com: número da versão, descrição da migração, data de aplicação e script SQL executado.
4. WHEN uma migração é aplicada, THE Sistema_DB SHALL executar o script de migração dentro de uma transação atômica.
5. THE Sistema_DB SHALL suportar migração incremental (aplicar apenas versões pendentes) e validar a sequência de versões.
6. THE Sistema_DB SHALL padronizar colunas de controle em todas as tabelas: `versao INTEGER NOT NULL DEFAULT 1` e `created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP`.

### Requisito 10: Escalabilidade e Processamento em Lote

**User Story:** Como operador, eu quero que o sistema processe grandes volumes de dados de forma eficiente, para que o tempo de processamento completo seja previsível.

#### Critérios de Aceitação

1. THE Processador_Completo SHALL processar registros em lotes configuráveis (padrão: 100 registros por lote) com commit ao final de cada lote.
2. THE Sistema_DB SHALL suportar processamento paralelo de geradores de homologação com número configurável de workers.
3. WHEN o volume de dados na tabela `auditoria` ultrapassa 1 milhão de registros, THE Sistema_DB SHALL particionar os dados por mês de criação para manter performance de consulta.
4. THE Sistema_DB SHALL executar VACUUM incremental automaticamente após operações de exclusão em massa.
5. THE Sistema_DB SHALL registrar métricas de performance (tempo de execução, registros por segundo) na tabela `metricas_processamento` para cada execução do Processador_Completo.

### Requisito 11: Migração de Dados do Banco Atual

**User Story:** Como desenvolvedor, eu quero migrar os dados do banco atual para o novo schema, para que nenhum dado histórico seja perdido.

#### Critérios de Aceitação

1. THE Sistema_DB SHALL fornecer um script de migração que leia todas as 11 tabelas do banco atual e popule as tabelas do novo schema.
2. WHEN a migração é executada, THE Sistema_DB SHALL mapear os dados de `portabilidade_records` para as novas tabelas `clientes`, `propostas`, `consulta_siebel` e demais tabelas normalizadas com desnormalização reversa.
3. WHEN a migração é executada, THE Sistema_DB SHALL preservar todos os registros de `decision_history` e `rules_log` na nova tabela `decisoes` com referências corretas.
4. THE Sistema_DB SHALL validar a contagem de registros pós-migração comparando com o banco original e reportar discrepâncias.
5. IF a migração encontra registros com Proposta_Isize incorreto (padrão CPF), THEN THE Sistema_DB SHALL aplicar a correção automática usando as estratégias de fallback antes de inserir no novo schema.
6. THE Sistema_DB SHALL criar um backup completo do banco atual antes de iniciar a migração.
7. THE Sistema_DB SHALL inserir todos os registros migrados com versao = 1, preservando o created_at original quando disponível.

### Requisito 12: Tratamento de Dados da Base COVERTE BASE PROP

**User Story:** Como operador, eu quero que os dados da planilha COVERTE BASE PROP (76 colunas) sejam processados e armazenados corretamente no novo banco, para que a base de propostas esteja sempre atualizada.

#### Critérios de Aceitação

1. WHEN a planilha COVERTE BASE PROP é importada, THE Importador SHALL extrair e normalizar todos os 76 campos distribuindo-os nas tabelas: `clientes` (CPF, Cliente, Nascimento, Mae, Endereco, Numero, Complemento, Bairro, Cidade, UF, Cep, Ponto Referencia, DDD, Telefone, DDD.1, Telefone.1, Email, Score), `propostas` (Proposta iSize, Data venda, Produto, Plano, Forma Pagamento, Vencimento, Tipo Chip, Conta Online, Vivo Pay, App Adicional, Plataforma, Nome Equipe, Nome vendedor, Login Externo, Nome Supervisor, Matricula Discador, Avulsa, SMS Previo, Observacoes), `status_venda` (Status venda, Motivo Rejeicao Cancelamento, Flag, Auditoria, Qualidade, Conectada, Data Conectada), `portabilidade` (Telefone Portabilidade, Numero linha, Portabilidade, Complemento Portabilidade, Portabilidade Antecipada, Data marcacao Port. Antecipada, Quem marcou Port. Antecipada), `bluechip` (Bluechip Status, Bluechip Data Status, Resposta Envio Pedido, Pedido Bluechip, Bluechip Data enviado, Data Maxima Prevista Entrega, Status Entrega Prevista, CD Bluechip, Remessa Bluechip, Qtd Remessas), `rastreio_entregas` (Rastreio Correios, Rastreio Loggi, Data Status Correios, Status Correios, Data Status Loggi, Status Loggi), `servicos_adicionais` (Vivo Internet, Vivo TV, ID PLAY Vivo) e `robo_processamento` (Robo Inicio Proc., Robo Fim Proc.).
2. WHEN um registro da COVERTE BASE PROP já existe no banco (mesmo proposta_isize), THE Importador SHALL inserir uma nova versão (versao incrementada) ao invés de atualizar o registro existente.
3. THE Importador SHALL registrar o lote de importação com referência ao arquivo COVERTE BASE PROP processado e tipo "coverte_prop".
4. IF a planilha COVERTE BASE PROP não está acessível via SMB, THEN THE Importador SHALL buscar cópia local na pasta `data/entrada/excel` como fallback.
5. THE Importador SHALL mapear o campo Numero OS (Arquivo 1) para associação com numero_ordem (Arquivo 7) e id_erp (Arquivo 4).

### Requisito 13: Fontes de Dados e Mapeamento de Campos

**User Story:** Como desenvolvedor, eu quero uma documentação clara de todas as 7 fontes de dados, seus campos e como se associam no banco unificado, para que a importação seja precisa e rastreável.

#### Critérios de Aceitação

1. THE Importador SHALL reconhecer e processar os 7 tipos de arquivo de entrada: (a) COVERTE BASE PROP — 76 colunas de propostas de venda, (b) Base de Portabilidade TIM — 39 colunas de status de portabilidade, (c) 3F GROSS — 8 colunas de ativação GROSS, (d) Relatório de Objetos — 78 colunas de logística, (e) Resultado GROSS — 8 colunas de resultados de processamento, (f) Propostas Backoffice — 31 colunas de acompanhamento, (g) Consulta Siebel — 29 colunas de consulta de portabilidade.
2. THE Importador SHALL identificar automaticamente o tipo de arquivo com base no padrão de colunas do cabeçalho, sem depender exclusivamente do nome do arquivo.
3. THE Importador SHALL aplicar as seguintes associações de chaves entre fontes ao inserir dados: Proposta_iSize (Arq.1) = Código externo (Arq.7) = Proposta (Arq.5) = PEDIDO (Arq.6) = Id Auxiliar1 (Arq.4), validando a consistência dessas chaves.
4. THE Importador SHALL aplicar a associação de telefone portado: Telefone Portabilidade (Arq.1) = ACESSO (Arq.2) = Número de acesso (Arq.7) = Numero Acesso (Arq.5) = NUMERO_PORTADO (Arq.6).
5. THE Importador SHALL aplicar a associação de número de ordem: Numero OS (Arq.1) = Número da ordem (Arq.7) = ID ERP (Arq.4).
6. THE Importador SHALL aplicar a associação de número provisório: Numero linha (Arq.1) = ACESSO_TEMPORARIO (Arq.2) = NUMERO_PROVISORIO (Arq.6) = Número temporário (Arq.7).
7. THE Importador SHALL normalizar CPF removendo pontuação e zeros à esquerda para garantir matching correto entre CPF (Arq.1), CPF_CNPJ (Arq.2), Documento (Arq.4), CPF (Arq.5), CPF (Arq.6) e Cpf (Arq.7).
8. THE Importador SHALL registrar na Trilha_Auditoria cada associação realizada entre fontes, incluindo as chaves utilizadas e o grau de confiança da associação (match exato, match por fallback, sem match).
9. IF um registro de uma fonte não possui proposta_isize diretamente, THEN THE Importador SHALL tentar resolver a associação usando as chaves secundárias (telefone portado, numero_ordem, CPF) nesta ordem de prioridade.

### Requisito 14: Imutabilidade de Registros e Histórico de Movimentação

**User Story:** Como operador, eu quero que nenhum registro existente seja alterado (UPDATE) no banco de dados, para que o histórico completo de movimentação de cada proposta seja preservado e auditável.

#### Critérios de Aceitação

1. THE Sistema_DB SHALL proibir operações UPDATE em todas as tabelas de dados principais: `clientes`, `propostas`, `status_venda`, `portabilidade`, `portabilidade_tim`, `logistica`, `gross`, `resultado_gross`, `backoffice`, `consulta_siebel`, `bluechip`, `rastreio_entregas`, `servicos_adicionais`, `robo_processamento` e `decisoes`.
2. THE Sistema_DB SHALL implementar triggers SQLite do tipo BEFORE UPDATE em todas as tabelas de dados principais que impeçam a execução de UPDATE e lancem erro com mensagem explicativa.
3. WHEN dados de um registro existente precisam ser alterados, THE Importador SHALL inserir uma nova linha na mesma tabela com o campo `versao` incrementado em 1 em relação à versão anterior daquele registro.
4. THE Sistema_DB SHALL garantir que cada nova versão de um registro contenha todos os campos (snapshot completo), copiando valores inalterados da versão anterior e aplicando apenas os campos modificados.
5. THE Sistema_DB SHALL manter uma constraint UNIQUE composta por (chave_de_negocio, versao) em cada tabela, onde chave_de_negocio é: proposta_isize para `propostas`, `status_venda`, `portabilidade`, `portabilidade_tim`, `logistica`, `gross`, `resultado_gross`, `backoffice`, `consulta_siebel`, `bluechip`, `rastreio_entregas`, `servicos_adicionais`, `robo_processamento` e `decisoes`; CPF para `clientes`.
6. THE Sistema_DB SHALL criar views `vw_<tabela>_corrente` para cada tabela principal que retornem apenas o Registro_Corrente (linha com MAX(versao)) de cada chave de negócio.
7. WHEN a view `vw_base_unificada` é consultada, THE Sistema_DB SHALL utilizar exclusivamente os Registros_Correntes de cada tabela, ignorando versões anteriores.
8. THE Sistema_DB SHALL permitir consulta do histórico completo de um registro por proposta_isize, retornando todas as versões ordenadas por versao ASC com seus respectivos created_at.
9. THE Sistema_DB SHALL registrar na coluna `lote_importacao_id` de cada nova versão inserida a referência ao lote que originou a alteração, permitindo rastrear qual importação causou cada mudança.
10. THE Sistema_DB SHALL permitir operações UPDATE apenas nas tabelas de controle/metadados: `execucoes_processamento`, `schema_versao`, `historico_backups`, `metricas_processamento` e `cache_base_unificada`.
