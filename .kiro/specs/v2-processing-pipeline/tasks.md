# Plano de Implementação: Pipeline de Processamento V2

## Visão Geral

Migrar os 6 geradores de homologação para usar o banco V2 via `QueriesV2`, integrar o módulo de reprocessamento de endereços (`src/reprocessamento/`), expandir o pipeline com ETAPA 3b (cache) e ETAPA 5 (reprocessamento), e implementar fallback V2 → Legado. Todas as tarefas usam Python.

## Tarefas

- [x] 1. Expandir QueriesV2 e schema
  - [x] 1.1 Adicionar método `buscar_registros_entrega_baixa()` em `src/database/queries_v2.py`
    - Implementar query com `vw_base_unificada` + `vw_logistica_corrente`
    - Filtrar status de entrega problemática (cancelad, baixa, remetente, aguardando correios, extravi)
    - Aplicar CTE para registro mais recente de logística por `proposta_isize`
    - Aplicar exclusão de Rejeição SMS e fallback de CPF via COALESCE
    - Retornar aliases compatíveis com `gerar_homologacao_entrega_baixa.py`
    - _Requisitos: 5.1, 5.2, 5.3, 5.4, 7.1_

  - [x] 1.2 Adicionar filtro de Rejeição SMS em todos os métodos `buscar_registros_*` existentes
    - Adicionar subconsulta NOT EXISTS contra `vw_portabilidade_tim_corrente` e `vw_consulta_siebel_corrente`
    - Verificar padrões `'rejei%cliente%sms'` e `'rejeicao sms'` em motivo_conflito, motivo_cancelamento, status_bilhete, motivo_recusa
    - Aplicar em: `buscar_registros_wpp`, `buscar_registros_aprovisionamento`, `buscar_registros_reabertura`, `buscar_registros_consulta`, `buscar_registros_erro_aprovisionamento`
    - _Requisitos: 1.2, 2.3, 3.3, 4.4, 6.4, 7.4_

  - [x] 1.3 Adicionar fallback de CPF via COALESCE em todos os métodos `buscar_registros_*`
    - Prioridade: `vw_base_unificada.cpf` > `vw_consulta_siebel_corrente.cpf` > `vw_logistica_corrente.documento`
    - Aplicar mesma lógica para `codigo_externo`
    - _Requisitos: 2.6, 3.6, 7.6_

  - [x] 1.4 Expandir CHECK constraint de `lotes_importacao.tipo_arquivo` em `src/database/schema.py`
    - Adicionar valor `'reprocessamento'` à lista de valores permitidos
    - _Requisitos: 10.8_

  - [ ]* 1.5 Escrever teste de propriedade — Propriedade 1: Exclusão universal de Rejeição SMS
    - **Propriedade 1: Exclusão universal de Rejeição SMS**
    - Gerar registros com indicadores de rejeição SMS em `vw_portabilidade_tim_corrente` e `vw_consulta_siebel_corrente`
    - Verificar que nenhum método `buscar_registros_*` retorna esses registros
    - Usar banco SQLite in-memory com dados seed via `hypothesis`
    - **Valida: Requisitos 1.2, 2.3, 3.3, 4.4, 5.4, 6.4, 7.4**

  - [ ]* 1.6 Escrever teste de propriedade — Propriedade 9: Filtros de status retornam apenas registros correspondentes
    - **Propriedade 9: Filtros de status retornam apenas registros correspondentes**
    - Gerar registros com status variados (Em Aprovisionamento, Erro no Aprovisionamento, outros)
    - Verificar que `buscar_registros_aprovisionamento` e `buscar_registros_erro_aprovisionamento` retornam conjuntos disjuntos
    - **Valida: Requisitos 3.2, 4.2**

  - [ ]* 1.7 Escrever teste de propriedade — Propriedade 3: Fallback de CPF respeita prioridade
    - **Propriedade 3: Fallback de CPF respeita prioridade**
    - Gerar registros com CPF presente em diferentes combinações de views
    - Verificar que o CPF retornado segue a cadeia de prioridade
    - **Valida: Requisitos 2.6, 3.6, 7.6**

  - [ ]* 1.8 Escrever teste de propriedade — Propriedade 2: CTE seleciona registro mais recente por chave
    - **Propriedade 2: CTE seleciona registro mais recente por chave**
    - Gerar múltiplas versões de registros por `proposta_isize`
    - Verificar que apenas o registro com maior versão/data é retornado
    - **Valida: Requisitos 2.2, 5.2, 6.3**

  - [ ]* 1.9 Escrever teste de propriedade — Propriedade 13: Aliases de colunas são compatíveis com geradores
    - **Propriedade 13: Aliases de colunas são compatíveis com geradores**
    - Para cada método `buscar_registros_*`, verificar que as chaves dos dicts retornados contêm os campos esperados pelo gerador correspondente
    - **Valida: Requisito 7.2**

- [x] 2. Checkpoint — Verificar QueriesV2
  - Garantir que todos os testes passam, perguntar ao usuário se houver dúvidas.

- [x] 3. Migrar geradores de homologação para usar QueriesV2
  - [x] 3.1 Migrar `gerar_homologacao_wpp.py` para usar `QueriesV2.buscar_registros_wpp()`
    - Substituir consulta direta a `base_coverte_prop` + `portabilidade_records` + `relatorio_objetos`
    - Manter lógica de `TemplateMapper`, `extrair_numero_e_complemento()`, preview de mensagem, histórico Google Sheets
    - Manter suporte a IDs forçados de `data/ids_forcar_wpp.txt`
    - Verificar view `vw_base_unificada` antes de executar; registrar erro e interromper se ausente
    - _Requisitos: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8_

  - [x] 3.2 Migrar `gerar_homologacao_reabertura.py` para usar `QueriesV2.buscar_registros_reabertura()`
    - Substituir consulta direta às tabelas legadas
    - Manter lógica de `filtrar_registros_validos()` contra `vw_portabilidade_tim_corrente`
    - Manter deduplicação por (cpf, numero_acesso) e fallback de CPF
    - _Requisitos: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6_

  - [x] 3.3 Migrar `gerar_homologacao_aprovisionamento.py` para usar `QueriesV2.buscar_registros_aprovisionamento()`
    - Substituir consulta direta às tabelas legadas
    - Manter inclusão de TODAS as linhas por `codigo_externo` com status "Em Aprovisionamento"
    - Manter exclusão de "Erro no Aprovisionamento" e validação via `filtrar_registros_validos()`
    - _Requisitos: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6_

  - [x] 3.4 Migrar `gerar_homologacao_erro_aprovisionamento.py` para usar `QueriesV2.buscar_registros_erro_aprovisionamento()`
    - Substituir consulta direta às tabelas legadas
    - Manter exigência de histórico de entrega (dados de `vw_logistica_corrente`)
    - Manter contagem de classificações e indicador `houve_reclassificacao`
    - _Requisitos: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6_

  - [x] 3.5 Migrar `gerar_homologacao_entrega_baixa.py` para usar `QueriesV2.buscar_registros_entrega_baixa()`
    - Substituir consulta direta a `base_coverte_prop` + `relatorio_objetos`
    - Manter deduplicação por `codigo_externo` e depois por (cpf, telefone)
    - Manter normalização de campo `Numero` via `extrair_numero_e_complemento()`
    - _Requisitos: 5.1, 5.2, 5.3, 5.4, 5.5, 5.6_

  - [x] 3.6 Migrar `gerar_homologacao_consulta.py` para usar `QueriesV2.buscar_registros_consulta()`
    - Substituir consulta direta às tabelas legadas
    - Manter lógica de 2 linhas para Portabilidade e 1 linha para Aquisição
    - Manter deduplicação por (cpf, numero_acesso) e limite de 20.000 registros
    - Gerar `.xlsx` com apenas 4 colunas: Cpf, Número de acesso, Número da ordem, Código externo
    - _Requisitos: 6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 6.7_

  - [x] 3.7 Implementar validação de integridade de linhas nos geradores
    - Criar função `validar_integridade_linha(row, campos_obrigatorios)` reutilizável
    - Excluir linhas com campos obrigatórios vazios e registrar no log
    - Tratar NULL/None como string vazia no arquivo de saída (nunca literal "None" ou "NULL")
    - Aplicar em todos os 6 geradores
    - _Requisitos: 13.1, 13.6_

  - [ ]* 3.8 Escrever teste de propriedade — Propriedade 4: Normalização de Numero é correta
    - **Propriedade 4: Normalização de Numero é correta**
    - Gerar strings de endereço com combinações aleatórias de dígitos e texto via `hypothesis`
    - Verificar que `Numero` contém apenas dígitos e `Complemento` contém o texto restante
    - Verificar round-trip de conteúdo (concatenação preserva informação original)
    - **Valida: Requisitos 1.4, 5.5**

  - [ ]* 3.9 Escrever teste de propriedade — Propriedade 5: Expansão de linhas Portabilidade vs Aquisição
    - **Propriedade 5: Expansão de linhas Portabilidade vs Aquisição**
    - Gerar registros com telefone_portado válido/inválido e numero_linha variados
    - Verificar que Portabilidade gera exatamente 2 linhas e Aquisição gera exatamente 1 linha
    - **Valida: Requisitos 6.5, 6.6**

  - [ ]* 3.10 Escrever teste de propriedade — Propriedade 6: Integridade total de linhas nos arquivos de saída
    - **Propriedade 6: Integridade total de linhas nos arquivos de saída**
    - Gerar registros com campos obrigatórios aleatoriamente nulos
    - Verificar que nenhuma linha no arquivo de saída tem campos obrigatórios vazios
    - **Valida: Requisitos 10.6, 11.6, 11.7, 13.6**

  - [ ]* 3.11 Escrever teste de propriedade — Propriedade 12: NULL no V2 vira string vazia no arquivo de saída
    - **Propriedade 12: NULL no V2 vira string vazia no arquivo de saída**
    - Gerar registros com campos NULL variados
    - Verificar que o arquivo de saída contém string vazia, nunca "None" ou "NULL"
    - **Valida: Requisito 13.1**

  - [ ]* 3.12 Escrever teste de propriedade — Propriedade 10: IDs forçados ignoram filtros
    - **Propriedade 10: IDs forçados ignoram filtros**
    - Gerar IDs forçados com crivo_vendas e acao_a_realizar variados
    - Verificar que IDs forçados aparecem no arquivo de saída independentemente dos filtros
    - **Valida: Requisito 1.6**

  - [ ]* 3.13 Escrever teste de propriedade — Propriedade 11: Contagem de classificações é precisa
    - **Propriedade 11: Contagem de classificações é precisa**
    - Gerar N registros históricos de classificação por `codigo_externo`
    - Verificar que `total_classificacoes` = N e `houve_reclassificacao` = "SIM" se N > 1
    - **Valida: Requisito 4.6**

- [x] 4. Checkpoint — Verificar migração dos geradores
  - Garantir que todos os testes passam, perguntar ao usuário se houver dúvidas.

- [x] 5. Criar módulo `src/reprocessamento/`
  - [x] 5.1 Criar `src/reprocessamento/__init__.py` e `src/reprocessamento/proxy_manager.py`
    - Implementar classe `ProxyManager` com pool de proxies, rotação round-robin, report_success/failure
    - Implementar `ProxyInfo` dataclass
    - Implementar `validate_all()` para testar conectividade
    - Remover proxy após N falhas consecutivas
    - Expor métricas de uso (ativos, falhas, taxa de sucesso)
    - _Requisitos: 10.4, 11.4_

  - [x] 5.2 Criar `src/reprocessamento/address_corrector.py`
    - Implementar classe `AddressCorrector` com estratégia: API CEP → geocodificação reversa → manter original
    - Integrar com `ProxyManager` para rotação de proxy em cada requisição
    - Implementar retry com backoff exponencial (3 tentativas)
    - Garantir que campos não corrigidos mantêm valor original (integridade)
    - _Requisitos: 10.3, 10.12, 11.6, 11.7_

  - [x] 5.3 Criar `src/reprocessamento/queries_reprocessamento.py`
    - Implementar query `TIM_REPROCESSAMENTO` adaptada para views V2
    - Consultar registros dos últimos 180 dias que necessitam correção de endereço
    - _Requisitos: 10.2, 10.10_

  - [x] 5.4 Criar `src/reprocessamento/reprocessador.py`
    - Implementar classe `ReprocessadorEndereco` com fluxo: consultar → corrigir → salvar → reimportar
    - Implementar `_classificar_tipo_entrega()`: Express (≤ 2 dias) vs Correios
    - Salvar arquivo `_pronto_tratamento.xlsx` com TODAS as linhas e campos preenchidos
    - Reimportar dados corrigidos no V2 como nova versão (lote tipo 'reprocessamento')
    - Atualizar `cache_base_unificada` para cada `proposta_isize` afetada
    - Aceitar flag `--banco` para execução independente com período de 180 dias
    - _Requisitos: 10.1, 10.2, 10.3, 10.5, 10.6, 10.7, 10.8, 10.9, 10.10, 10.11, 10.13, 11.1, 11.2, 11.3, 11.5, 11.6, 11.8_

  - [ ]* 5.5 Escrever teste de propriedade — Propriedade 8: ProxyManager rotaciona e remove proxies corretamente
    - **Propriedade 8: ProxyManager rotaciona e remove proxies corretamente**
    - Gerar sequências aleatórias de get_proxy/report_success/report_failure
    - Verificar round-robin entre proxies ativas, remoção após N falhas, nunca retorna proxy inativa
    - Verificar métricas consistentes: total_requests = successes + failures
    - **Valida: Requisitos 10.4, 11.4**

  - [ ]* 5.6 Escrever teste de propriedade — Propriedade 7: Classificação de Tipo Entrega
    - **Propriedade 7: Classificação de Tipo Entrega**
    - Gerar registros com `data_venda` e `data_entrega` aleatórias
    - Verificar que "Express" quando diferença ≤ 2 dias, "Correios" quando transportadora é Correios ou diferença > 2 dias
    - **Valida: Requisitos 10.11, 11.5**

- [x] 6. Checkpoint — Verificar módulo de reprocessamento
  - Garantir que todos os testes passam, perguntar ao usuário se houver dúvidas.

- [x] 7. Integrar pipeline em `processar_completo.py`
  - [x] 7.1 Adicionar ETAPA 3b — Atualizar Cache Unificada após importações
    - Chamar `atualizar_cache_unificada()` para cada `proposta_isize` afetada nos lotes importados
    - Executar antes da ETAPA 4 (geração de homologação)
    - Registrar métricas (quantidade atualizada, erros, tempo) em `metricas_processamento`
    - Continuar se falha em registro individual
    - _Requisitos: 9.1, 9.2, 9.3, 9.4_

  - [x] 7.2 Implementar função `usar_v2()` e fallback V2 → Legado
    - Verificar acessibilidade e integridade do V2 via `validar_integridade()`
    - Respeitar flags `--forcar-legado` e `--forcar-v2`
    - Se V2 indisponível, gerar homologação via geradores legados
    - Registrar evento de fallback em `execucoes_processamento`
    - _Requisitos: 15.1, 15.2, 15.3, 15.4_

  - [x] 7.3 Integrar ETAPA 4 — Geração de homologação via V2 com fallback
    - Chamar os 6 geradores migrados usando `QueriesV2` quando `usar_v2()` retorna True
    - Chamar geradores legados quando `usar_v2()` retorna False
    - Manter dual-write: gravar Legado primeiro, V2 depois (ETAPA 1)
    - Se gravação V2 falha, registrar erro e continuar (Legado já atualizado)
    - Se gravação Legado falha, registrar erro crítico e interromper etapa COVERTE
    - _Requisitos: 8.1, 8.2, 8.3, 8.4, 8.5, 8.6, 12.1, 12.2_

  - [x] 7.4 Integrar ETAPA 5 — Reprocessamento de endereços no pipeline
    - Instanciar `ReprocessadorEndereco` e chamar `executar()` após ETAPA 4
    - Registrar métricas do reprocessamento em `metricas_processamento`
    - Pular se flag `--skip-reprocessamento` ativa
    - _Requisitos: 10.1, 10.13, 12.1_

  - [x] 7.5 Adicionar novas flags CLI ao `argparse` de `processar_completo.py`
    - `--apenas-reprocessamento`: executar apenas ETAPA 5
    - `--skip-reprocessamento`: pular ETAPA 5
    - `--forcar-legado`: forçar geração via banco legado
    - `--forcar-v2`: forçar geração via banco V2
    - _Requisitos: 10.13, 12.4, 15.3, 15.4_

  - [x] 7.6 Implementar registro de execução e métricas do pipeline
    - Criar registro em `execucoes_processamento` no início (tipo "processamento_completo", status "em_andamento")
    - Registrar métricas por etapa em `metricas_processamento` (registros processados, tempo, erros)
    - Atualizar registro com status "concluido" ou "erro" ao final
    - _Requisitos: 14.1, 14.2, 14.3, 14.4_

  - [x] 7.7 Garantir compatibilidade de formato nos arquivos de saída
    - Encoding `utf-8-sig`, separador `;` para CSV
    - CSV intermediário → `.xlsx` para Aprovisionamento e Erro Aprovisionamento
    - Copiar arquivos gerados para `config.PASTA_SAIDA_HOMOLOGACAO`
    - _Requisitos: 13.2, 13.3, 13.4, 13.5_

  - [ ]* 7.8 Escrever teste de propriedade — Propriedade 14: Cache atualizado reflete dados importados
    - **Propriedade 14: Cache atualizado reflete dados importados**
    - Importar registros no V2, chamar `atualizar_cache_unificada()`, verificar que cache reflete dados mais recentes
    - **Valida: Requisito 9.1**

- [x] 8. Checkpoint final — Verificar pipeline completo
  - Garantir que todos os testes passam, perguntar ao usuário se houver dúvidas.

## Notas

- Tarefas marcadas com `*` são opcionais e podem ser puladas para um MVP mais rápido
- Cada tarefa referencia requisitos específicos para rastreabilidade
- Checkpoints garantem validação incremental
- Testes de propriedade validam propriedades universais de corretude (Propriedades 1-14 do design)
- Testes unitários validam cenários específicos e edge cases
