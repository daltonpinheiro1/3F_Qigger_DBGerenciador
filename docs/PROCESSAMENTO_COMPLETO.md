# Pipeline de Processamento Completo V2

## Fluxo de Etapas

```
ETAPA 1  → BS_VENDA_DU
ETAPA 2  → Arquivos da pasta de entrada (detecção por cabeçalho)
ETAPA 3b → Atualizar Cache Unificada (bulk INSERT)
ETAPA 4  → Geração de Homologação (6 geradores, V2 primário)
ETAPA 5  → Reprocessamento de endereços inválidos
ETAPA 6  → Backup e replicação SMB
```

## ETAPA 1 — BS_VENDA_DU

Coleta dados de BS_VENDA_DU.xlsx da rede SMB e insere na tabela `bs_venda_du` do banco legado.

## ETAPA 2 — Arquivos da Pasta de Entrada

Processa todos os arquivos em `data/entrada/` e `/Applications/Documentos/IMPORTACOES_QIGGER/`.

Detecção automática por cabeçalho:
- CSV portabilidade Siebel → `portabilidade_records` (legado) + V2
- Excel Relatório de Objetos → `relatorio_objetos` (legado) + V2
- Excel COVERTE/GROSS → `base_coverte_prop` (legado) + V2
- Excel TIM portabilidade → `tim_pre_controle` (legado) + V2
- CSV Relatório Faturamento → `relatorio_faturamento` (legado)
- Excel Estornos → `estornos` (legado)

Arquivos processados são enviados para a Lixeira do macOS.

## ETAPA 3b — Cache Unificada

Reconstrói `cache_base_unificada` via bulk `INSERT OR REPLACE ... SELECT FROM vw_base_unificada`. Executa sempre (inclusive com `--apenas-homologacao`).

## ETAPA 4 — Geração de Homologação

6 geradores executados em paralelo (ProcessPoolExecutor):
- WPP, Reabertura, Aprovisionamento, Erro Aprovisionamento, Entrega/Baixa, Consulta

Todos usam V2 (QueriesV2) como fonte primária. Flags `--forcar-legado` / `--forcar-v2` propagadas via variáveis de ambiente.

## ETAPA 5 — Reprocessamento de Endereços

Consulta registros com endereço problemático (CEP vazio/inválido, endereço/cidade/UF vazios) que não foram corrigidos anteriormente. Corrige via APIs de CEP e geocodificação. Reimporta no V2 e atualiza cache (bulk).

## ETAPA 6 — Backup e Replicação

Replica `portabilidade.db` e `portabilidade_v2.db` para rede SMB (07 Backoffice).

## Dual-Write

O pipeline grava em ambos os bancos:
1. Banco legado (`portabilidade.db`) — para plugins externos
2. Banco V2 (`portabilidade_v2.db`) — fonte primária dos geradores

## Fallback V2 → Legado

Se o V2 não está disponível ou retorna 0 registros, os geradores caem para o banco legado automaticamente (com warning no log).
