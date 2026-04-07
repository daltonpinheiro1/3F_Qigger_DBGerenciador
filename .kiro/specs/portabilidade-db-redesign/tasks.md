# Plano de Implementação: Redesign do Banco de Dados de Portabilidade

## Visão Geral

Implementação incremental do novo schema SQLite normalizado, imutável e versionado. Cada tarefa constrói sobre a anterior, começando pelo schema SQL puro, passando pelo DatabaseManager, módulo de importação, migração, pipeline de processamento e geradores de homologação. Testes de propriedade (Hypothesis) e integração acompanham cada etapa.

## Tarefas

- [x] 1. Criar o schema SQL do novo banco de dados
  - [x] 1.1 Criar arquivo `src/database/schema.py` com todo o SQL DDL do novo schema
    - Definir constante `SCHEMA_VERSION = 1`
    - Definir constantes com SQL para as 9 tabelas de controle: `schema_versao`, `lotes_importacao`, `arquivos_importados`, `execucoes_processamento`, `auditoria`, `historico_backups`, `metricas_processamento`, `registros_pendentes`, `cache_base_unificada`
    - Definir constantes com SQL para as 18 tabelas de dados: `clientes`, `propostas`, `status_venda`, `portabilidade`, `portabilidade_tim`, `logistica`, `gross`, `resultado_gross`, `backoffice`, `consulta_siebel`, `bluechip`, `rastreio_entregas`, `servicos_adicionais`, `robo_processamento`, `decisoes`, `regras_decisao`, `templates_wpp`, `tipo_comunicacao_template`
    - Incluir todos os índices (individuais e compostos) conforme design seções 1.x e 2.x
    - Incluir os 15 triggers BEFORE UPDATE (bloqueio de UPDATE) conforme design seção 3.1
    - Incluir os 15 triggers AFTER INSERT (auditoria automática) conforme design seção 3.2
    - Incluir as 15 views `vw_<tabela>_corrente` conforme design seção 4
    - Incluir a view unificada `vw_base_unificada` conforme design seção 4.1
    - Incluir PRAGMAs de performance (WAL, cache 128MB, mmap 512MB, foreign_keys ON)
    - Incluir função `criar_schema(conn)` que executa todo o DDL em ordem
    - _Requisitos: 1.1–1.19, 7.1–7.6, 9.1–9.6, 14.1–14.6_

  - [ ]* 1.2 Teste de propriedade: nomenclatura padronizada (P16)
    - **Propriedade 16: Nomenclatura padronizada**
    - Verificar que todas as tabelas, colunas, índices, views e triggers seguem snake_case
    - Verificar prefixos: `idx_` para índices, `vw_` para views, `trg_` para triggers
    - **Valida: Requisitos 9.1, 9.2**

  - [ ]* 1.3 Teste de propriedade: colunas de controle padronizadas (P17)
    - **Propriedade 17: Colunas de controle padronizadas**
    - Verificar que todas as tabelas de dados possuem `versao INTEGER NOT NULL DEFAULT 1` e `created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP`
    - **Valida: Requisitos 9.6**

- [x] 2. Checkpoint — Verificar schema
  - Garantir que todos os testes passam, perguntar ao usuário se há dúvidas.

- [x] 3. Implementar o novo DatabaseManager
  - [x] 3.1 Criar `src/database/db_manager_v2.py` com a classe `DatabaseManagerV2`
    - Construtor recebe `db_path`, chama `criar_schema()` do `schema.py`
    - Método `_apply_pragmas(conn)` para configurações de performance
    - Método `inserir_registro(tabela, dados, lote_id)` com versionamento automático (busca MAX(versao) e incrementa)
    - Método `buscar_corrente(tabela, chave_negocio, valor)` via views `vw_<tabela>_corrente`
    - Método `buscar_historico(tabela, chave_negocio, valor)` retornando todas as versões
    - Método `criar_lote(nome_arquivo, tipo, hash_sha256)` para registrar lote de importação
    - Método `finalizar_lote(lote_id, qtd_inseridos, qtd_erros, status)` para atualizar lote
    - Método `atualizar_cache_unificada(proposta_isize)` para refresh do cache materializado
    - Método `validar_integridade()` com `PRAGMA integrity_check` e `PRAGMA foreign_key_check`
    - Método `registrar_execucao(tipo, parametros)` e `finalizar_execucao(exec_id, status, registros)`
    - Context manager `transacao()` para BEGIN/COMMIT/ROLLBACK atômico
    - _Requisitos: 1.18, 2.1, 2.2, 2.5, 3.1–3.8, 5.9, 10.1_

  - [ ]* 3.2 Teste de propriedade: round-trip de dados (P1)
    - **Propriedade 1: Round-trip de dados em tabelas versionadas**
    - Gerar registros aleatórios com Hypothesis para cada tabela, inserir via `inserir_registro()`, consultar de volta e comparar
    - **Valida: Requisitos 1.1–1.17**

  - [ ]* 3.3 Teste de propriedade: bloqueio de UPDATE em tabelas de dados (P2)
    - **Propriedade 2: Bloqueio de UPDATE em tabelas de dados**
    - Para cada tabela de dados, inserir registro e tentar UPDATE — deve falhar com mensagem explicativa
    - **Valida: Requisitos 14.1, 14.2**

  - [ ]* 3.4 Teste de propriedade: UPDATE permitido em tabelas de controle (P3)
    - **Propriedade 3: UPDATE permitido em tabelas de controle**
    - Para tabelas de controle, inserir registro e executar UPDATE — deve ser bem-sucedido
    - **Valida: Requisitos 14.10**

  - [ ]* 3.5 Teste de propriedade: versionamento incremental (P4)
    - **Propriedade 4: Versionamento incremental**
    - Gerar sequência de versões para mesma chave, verificar que view corrente retorna apenas a mais recente
    - **Valida: Requisitos 3.6, 3.8, 12.2, 14.3, 14.6**

  - [ ]* 3.6 Teste de propriedade: unicidade (chave_negocio, versao) (P5)
    - **Propriedade 5: Unicidade de (chave_negocio, versao)**
    - Tentar inserir duplicata de (chave, versao) — deve falhar com constraint UNIQUE
    - **Valida: Requisitos 1.19, 14.5**

  - [ ]* 3.7 Teste de propriedade: integridade referencial (P6)
    - **Propriedade 6: Integridade referencial (Foreign Keys)**
    - Inserir registro com proposta_isize inexistente — deve falhar com erro de FK
    - **Valida: Requisitos 1.18, 2.2**

  - [ ]* 3.8 Teste de propriedade: auditoria automática (P7)
    - **Propriedade 7: Auditoria automática via triggers**
    - Inserir registro em qualquer tabela de dados e verificar que existe registro correspondente em `auditoria`
    - **Valida: Requisitos 6.1, 6.2**

  - [ ]* 3.9 Teste de propriedade: view corrente retorna MAX(versao) (P10)
    - **Propriedade 10: View corrente retorna apenas MAX(versao)**
    - Inserir múltiplas versões, verificar que view retorna exatamente uma linha com maior versão
    - **Valida: Requisitos 3.8, 14.6, 14.7**

  - [ ]* 3.10 Teste de propriedade: histórico completo preservado (P11)
    - **Propriedade 11: Histórico completo preservado**
    - Inserir K versões, consultar sem filtro de view, verificar K registros retornados
    - **Valida: Requisitos 14.8**

  - [ ]* 3.11 Teste de propriedade: snapshot completo (P12)
    - **Propriedade 12: Snapshot completo em cada versão**
    - Para registro com versão > 1, verificar que campos NOT NULL estão preenchidos
    - **Valida: Requisitos 14.4**

  - [ ]* 3.12 Teste de propriedade: atomicidade de transações (P15)
    - **Propriedade 15: Atomicidade de transações**
    - Gerar lote com registro inválido no meio, verificar ROLLBACK completo
    - **Valida: Requisitos 3.2, 3.3**

- [x] 4. Checkpoint — Verificar DatabaseManager
  - Garantir que todos os testes passam, perguntar ao usuário se há dúvidas.

- [x] 5. Implementar módulo de importação
  - [x] 5.1 Criar `src/database/importador.py` com a classe `Importador`
    - Método `identificar_tipo_arquivo(colunas_cabecalho)` que detecta o tipo de arquivo pelos nomes das colunas (7 tipos: coverte_prop, portabilidade_tim, gross, relatorio_objetos, resultado_gross, backoffice, consulta_siebel)
    - Método `calcular_hash_sha256(caminho_arquivo)` para detecção de duplicatas
    - Método `importar_arquivo(caminho, db_manager)` que orquestra: hash → verificar duplicata → criar lote → ler arquivo → validar → inserir registros → finalizar lote
    - Método `_verificar_duplicata(hash_sha256, db_manager)` que rejeita importação se hash já existe
    - Lógica de leitura de CSV (pandas/csv) e Excel (openpyxl/pandas)
    - Mapeamento de colunas de cada tipo de arquivo para as tabelas de destino conforme design seção 5.1
    - Processamento em lotes configuráveis (padrão: 100 registros por lote com commit)
    - _Requisitos: 2.1, 2.3, 2.4, 10.1, 12.1, 12.3, 13.1, 13.2_

  - [x] 5.2 Implementar validação e correção de `proposta_isize` no `Importador`
    - Método `validar_proposta_isize(valor)` que detecta CPF (11 dígitos numéricos puros)
    - Método `resolver_proposta_isize(valor_cpf, db_manager)` com fallback em ordem: CPF → numero_ordem → numero_acesso → remessa_bluechip → pedido_bluechip → codigo_externo → telefone_portado
    - Método `normalizar_cpf(cpf_str)` que remove pontuação e zeros à esquerda (idempotente)
    - Registrar correções na tabela `auditoria` com operação 'CORRECAO'
    - Registrar pendências na tabela `registros_pendentes` quando fallback falha
    - _Requisitos: 4.1–4.5, 13.7, 13.9_

  - [x] 5.3 Implementar mapeamento de colunas COVERTE BASE PROP → 8 tabelas normalizadas
    - Método `_mapear_coverte_prop(row, lote_id)` que distribui os 76 campos nas tabelas: clientes, propostas, status_venda, portabilidade, bluechip, rastreio_entregas, servicos_adicionais, robo_processamento
    - Normalização de CPF (remover pontuação)
    - Tratamento de campos vazios/NaN → NULL
    - Fallback para cópia local se SMB inacessível
    - _Requisitos: 12.1–12.5, 13.3–13.6_

  - [ ]* 5.4 Teste de propriedade: rejeição de hash duplicado (P8)
    - **Propriedade 8: Rejeição de importação duplicada (hash SHA-256)**
    - Gerar hash, inserir lote, tentar inserir novamente — deve falhar com constraint UNIQUE
    - **Valida: Requisitos 2.3**

  - [ ]* 5.5 Teste de propriedade: rastreabilidade de lotes (P9)
    - **Propriedade 9: Rastreabilidade de lotes de importação**
    - Importar arquivo, verificar que lote existe e todos os registros têm `lote_importacao_id` correto
    - **Valida: Requisitos 2.1, 2.4, 12.3, 14.9**

  - [ ]* 5.6 Teste de propriedade: normalização de CPF (P13)
    - **Propriedade 13: Normalização de CPF**
    - Gerar CPFs com formatos variados (com/sem pontuação, zeros à esquerda), verificar idempotência
    - **Valida: Requisitos 13.7**

  - [ ]* 5.7 Teste de propriedade: validação de proposta_isize (P14)
    - **Propriedade 14: Validação de Proposta_Isize (detecção de CPF)**
    - Gerar strings de 11 dígitos (deve detectar como CPF) e strings não-11 dígitos (deve aceitar)
    - **Valida: Requisitos 4.1, 4.2**

  - [ ]* 5.8 Teste de propriedade: identificação de tipo de arquivo (P18)
    - **Propriedade 18: Identificação automática de tipo de arquivo**
    - Gerar cabeçalhos de cada um dos 7 tipos, verificar detecção correta; cabeçalhos desconhecidos devem retornar erro
    - **Valida: Requisitos 13.1, 13.2**

- [x] 6. Checkpoint — Verificar módulo de importação
  - Garantir que todos os testes passam, perguntar ao usuário se há dúvidas.

- [x] 7. Implementar script de migração do banco atual
  - [x] 7.1 Criar `src/database/migrar_banco.py` com a classe `MigradorBanco`
    - Método `executar_migracao(db_antigo_path, db_novo_path)` que orquestra toda a migração
    - Criar backup do banco atual antes de iniciar (`sqlite3 .backup`)
    - Criar lote de migração em `lotes_importacao` com tipo 'migracao'
    - Implementar migração `base_coverte_prop → 8 tabelas` conforme design seção 5.1
    - Implementar migração `portabilidade_records → consulta_siebel` conforme design seção 5.2
    - Implementar migração `portabilidade_processamento → portabilidade_tim` conforme design seção 5.3
    - Implementar migração `relatorio_objetos → logistica` conforme design seção 5.4
    - Implementar migração `decision_history + rules_log → decisoes` conforme design seção 5.5
    - Implementar migração `triggers_rules → regras_decisao` conforme design seção 5.6
    - Implementar migração `templates_wpp` e `tipo_comunicacao_template` conforme design seção 5.7
    - Implementar migração `unmapped_records → registros_pendentes` conforme design seção 5.8
    - Aplicar correção de proposta_isize (11 dígitos → fallback) durante migração
    - Todos os registros migrados com `versao = 1`, preservando `created_at` original
    - _Requisitos: 11.1–11.7_

  - [x] 7.2 Implementar validação pós-migração em `MigradorBanco`
    - Método `validar_migracao(db_antigo_path, db_novo_path)` que compara contagens de registros
    - Comparar: base_coverte_prop vs propostas, portabilidade_records vs consulta_siebel, relatorio_objetos vs logistica, decision_history vs decisoes, triggers_rules vs regras_decisao
    - Executar `PRAGMA integrity_check` e `PRAGMA foreign_key_check` no banco novo
    - Reportar discrepâncias com detalhes
    - _Requisitos: 11.4, 11.6_

  - [ ]* 7.3 Teste de propriedade: migração preserva contagem (P20)
    - **Propriedade 20: Migração preserva contagem de registros**
    - Verificar que contagem de registros migrados = contagem de registros válidos na origem, todos com versao = 1
    - **Valida: Requisitos 11.2, 11.3, 11.4, 11.7**

- [x] 8. Checkpoint — Verificar migração
  - Garantir que todos os testes passam, perguntar ao usuário se há dúvidas.

- [x] 9. Atualizar pipeline de processamento
  - [x] 9.1 Refatorar `processar_completo.py` para usar `DatabaseManagerV2`
    - Substituir instanciação de `DatabaseManager` por `DatabaseManagerV2`
    - Adaptar chamadas de inserção para usar `inserir_registro()` com versionamento
    - Implementar registro de execução via `registrar_execucao()` / `finalizar_execucao()`
    - Manter ordem obrigatória de etapas: base_coverte_prop → arquivos da pasta → fallback COVERTE → homologação → backup
    - Usar context manager `transacao()` para operações atômicas por lote
    - Registrar métricas de performance em `metricas_processamento`
    - Validar integridade (`PRAGMA integrity_check`) após cada etapa
    - _Requisitos: 3.1–3.8, 10.1, 10.5_

  - [x] 9.2 Atualizar `src/engine/qigger_decision_engine.py` para gravar decisões no novo schema
    - Adaptar para inserir resultados na tabela `decisoes` via `DatabaseManagerV2`
    - Registrar tempo de execução em milissegundos por regra aplicada
    - Registrar na auditoria cada aplicação de regra com operação 'REGRA_APLICADA'
    - _Requisitos: 6.3_

  - [x] 9.3 Atualizar `src/utils/data_unifier.py` para usar `cache_base_unificada`
    - Substituir lógica de `base_unificada` pela tabela `cache_base_unificada`
    - Implementar atualização do cache via `atualizar_cache_unificada(proposta_isize)` após cada inserção
    - _Requisitos: 5.1, 5.9_

  - [ ]* 9.4 Teste de propriedade: associação de chaves na view unificada (P19)
    - **Propriedade 19: Associação de chaves entre fontes**
    - Inserir registros em múltiplas tabelas com mesma proposta_isize, verificar consolidação na `vw_base_unificada`
    - **Valida: Requisitos 5.1–5.7, 13.3–13.6**

  - [ ]* 9.5 Teste de propriedade: filtros de auditoria (P21)
    - **Propriedade 21: Filtros de auditoria**
    - Gerar registros de auditoria variados, aplicar filtros por período/tabela/operação/versão, verificar resultados
    - **Valida: Requisitos 6.5**

- [x] 10. Checkpoint — Verificar pipeline
  - Garantir que todos os testes passam, perguntar ao usuário se há dúvidas.

- [x] 11. Atualizar geradores de homologação
  - [x] 11.1 Refatorar `gerar_todos_arquivos_homologacao.py` para usar novo schema
    - Substituir queries que usam tabelas antigas por queries nas views `vw_<tabela>_corrente` e `vw_base_unificada`
    - Adaptar geração de WPP, aprovisionamentos, reabertura, consulta e erro para usar dados do novo schema
    - _Requisitos: 3.5, 5.1_

  - [x] 11.2 Atualizar `src/utils/wpp_output_generator.py` e `src/utils/regua_comunicacao.py`
    - Adaptar queries para usar `vw_base_unificada` e `vw_decisoes_corrente`
    - Usar `templates_wpp` e `tipo_comunicacao_template` do novo schema
    - _Requisitos: 1.15, 5.1_

  - [x] 11.3 Atualizar `src/utils/csv_generator.py` e `src/utils/file_output_manager.py`
    - Adaptar geração de CSVs de saída para usar views correntes do novo schema
    - _Requisitos: 5.1_

- [x] 12. Atualizar sistema de backup
  - [x] 12.1 Refatorar `backup_database.py` para usar novo schema
    - Usar `sqlite3 .backup` nativo para snapshot consistente
    - Registrar cada backup na tabela `historico_backups` via `DatabaseManagerV2`
    - Manter últimos 10 backups locais, remover mais antigos
    - Replicar para SMB (07 Backoffice) com tentativa de montagem automática
    - Em caso de falha SMB, registrar aviso sem interromper processamento
    - _Requisitos: 8.1–8.6_

- [x] 13. Testes de integração
  - [ ]* 13.1 Criar `tests/test_integration_pipeline.py`
    - Testar fluxo completo: criar banco → importar arquivo CSV de teste → processar → gerar homologação
    - Verificar que lotes, auditoria, versões e cache são criados corretamente
    - Verificar atomicidade (simular falha no meio do lote e verificar rollback)
    - _Requisitos: 3.1–3.8, 10.1_

  - [ ]* 13.2 Criar `tests/test_integration_migration.py`
    - Testar migração completa com banco de teste (subset dos dados reais)
    - Verificar contagens, integridade referencial e dados migrados
    - _Requisitos: 11.1–11.7_

- [x] 14. Checkpoint final — Verificar integração completa
  - Garantir que todos os testes passam, perguntar ao usuário se há dúvidas.

## Notas

- Tarefas marcadas com `*` são opcionais e podem ser puladas para um MVP mais rápido
- Cada tarefa referencia requisitos específicos para rastreabilidade
- Checkpoints garantem validação incremental
- Testes de propriedade (Hypothesis) validam propriedades universais de corretude
- Testes unitários e de integração validam exemplos específicos e fluxos completos
- O `DatabaseManagerV2` é criado como arquivo separado para permitir migração gradual sem quebrar o sistema atual
