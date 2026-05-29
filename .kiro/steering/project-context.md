# 3F Qigger DB Gerenciador — Contexto do Projeto

## Visão Geral

Sistema de gerenciamento de portabilidade e vendas para a 3F Contact Center (canal TIM).
Pipeline ETL completo: coleta → importação → processamento → decisão → homologação → reprocessamento.

## Arquitetura

- **Linguagem**: Python 3.14
- **Banco principal**: SQLite (`portabilidade_v2.db`) — schema INSERT-only versionado
- **Banco legado**: SQLite (`portabilidade.db`) — tabela flat `base_coverte_prop`
- **Execução**: `python3 run.py completo --workers 12`

## Banco de Dados v2 — Tabelas Principais

| Tabela | Fonte | Chave | Atualização |
|--------|-------|-------|-------------|
| propostas | coverte_prop (dia a dia) | proposta_isize + versao | Diária |
| clientes | coverte_prop | cpf + versao | Diária |
| status_venda | coverte_prop | proposta_isize + versao | Diária |
| portabilidade | coverte_prop | proposta_isize + versao | Diária |
| bluechip | coverte_prop | proposta_isize + versao | Diária |
| rastreio_entregas | coverte_prop | proposta_isize + versao | Diária |
| logistica | Relatório de Objetos | proposta_isize + nu_pedido + versao | Diária |
| consulta_siebel | CSV Siebel | proposta_isize + numero_acesso + numero_ordem + versao | Sob demanda |
| portabilidade_tim | Arquivo TIM | proposta_isize + acesso + versao | Semanal |
| gross | 3F_GROSS xlsx | proposta_isize + acesso + versao | Diária |
| backoffice | Arquivo BKO | proposta_isize + versao | Diária |
| decisoes | QiggerDecisionEngine | proposta_isize + regra_id + versao | Via migração (parado) |
| resultado_gross | Arquivo resultado | proposta_isize + versao | Nunca importado |

## Importador — Tipos de Arquivo

| Tipo | Assinatura | Mapeador |
|------|-----------|----------|
| coverte_prop | Proposta iSize, Cliente, Data venda, Plano | _mapear_coverte_prop |
| portabilidade_tim | DATA_SOLICITACAO, ACESSO, DOADORA, RECEPTORA, STATUS | _mapear_portabilidade_tim |
| gross | CLASSIFICACAO_CR, ACESSO, CUSTCODE, OPERADORA_N1 | _mapear_gross |
| relatorio_objetos | Nu Pedido, Rastreio, Transportadora, Última Ocorrencia | _mapear_relatorio_objetos |
| resultado_gross | Proposta, Numero Acesso, Data gross, Resultado, ICCID | _mapear_resultado_gross |
| backoffice | PEDIDO, BLUE_CHIP, STATUS_PEDIDO, NUMERO_PORTADO | _mapear_backoffice |
| consulta_siebel | Cpf, Número de acesso, Número da ordem, Código externo, Status do bilhete | _mapear_consulta_siebel |

## Resolução de proposta_isize (Cascata)

Para GROSS: ACESSO → portabilidade.telefone_portabilidade → portabilidade.numero_linha → consulta_siebel.numero_acesso → clientes (ddd+tel) → CUSTCODE → ICCID (resultado_gross/backoffice/logistica) → CPF_CNPJ → PROTOCOLO

Para Relatório de Objetos: Id Auxiliar1 → Nu Pedido (26-0XXXXXXXXX) → ID ERP (consulta_siebel) → ICCID → Documento/CPF

## Queries SQL Operacionais

| Arquivo | Objetivo |
|---------|----------|
| `deprecated/scripts_obsoletos/query_kpi_v2.sql` | KPI Funil de Vendas completo |
| `data/3F_GROSS_QUERY_ENTREGAS_MAIO.sql` | Entregas vs GROSS vs Siebel vs TIM |
| `data/3F_QUERY_CANCELAMENTOS_MAIO.sql` | Cancelamentos de entrega para reprocessamento |
| `data/3F_QUERY_VERIFICACAO_PEDIDOS_ERRADOS.sql` | Envios duplicados indevidos (consolidado) |
| `data/3F_QUERY_VERIFICACAO_PEDIDOS_INDEVIDOS_DETALHE.sql` | Pedidos indevidos linha a linha |
| `data/TIM_REPROCESSAMENTO_ID_ISIZE.sql` | Gerar arquivo reprocessamento TIM por IDs |

## Regras de Negócio Críticas

### Funil de Vendas (Status_Resposta_Envio_Pedido)
1. Reprovada no crivo → REPROVADA
2. Pendente no crivo → PENDENTE CRIVO
3. Aprovada + resposta OK envio → ENVIADO
4. Aprovada + histórico logística → ENVIADO
5. Aprovada + E-Sim (tipo_chip) → ENVIADO
6. Aprovada + GROSS efetivo → ENVIADO
7. Aprovada + sem logística + não E-Sim + sem GROSS → PENDENTE

### Tipo Chip
- Valor no banco: `E-Sim` (com hífen)
- Normalização: `UPPER(TRIM(REPLACE(tipo_chip, '-', '')))` = 'ESIM'

### GROSS — Cascata de Associação
1. gross.proposta_isize
2. gross.acesso = telefone_portabilidade
3. gross.acesso = numero_linha
4. gross.acesso = acesso_temporario TIM
5. gross.iccid = iccid (logística)
6. siebel.proposta_isize (status_ordem Concluído)
7. siebel.numero_acesso = telefone_portabilidade
8. siebel.numero_acesso = numero_linha

### Cancelamentos — Exclusões
- Excluir se mesmo proposta tem envio ativo posterior (data_insercao >= data_cancelamento)
- Excluir se mesmo CPF tem outra proposta com envio ativo posterior
- Excluir se mesmo telefone tem outra proposta com envio ativo posterior
- Excluir se Siebel tem status Concluído posterior
- Excluir se TIM tem status ATIVA/PENDENTE/CONFIRMADO
- Excluir motivos: REJEICAO DO CLIENTE VIA SMS, NUMERO VAGO, CLIENTE TIM

### Pedidos Indevidos — Lógica
- Agrupar por numero_acesso (telefone portado)
- Ordenar cronologicamente por data_insercao ASC, nu_pedido ASC
- Último cancelado → próximo = LEGITIMO
- Após legítimo sem cancelamento = INDEVIDO

## Normalização de Datas

Formatos no banco: `dd/mm/yyyy`, `yyyy-mm-dd`, `yyyy-mm-ddTHH:MM:SS`, `yyyy-mm-dd HH:MM:SS`

Padrão de conversão para ISO:
```sql
CASE
    WHEN campo LIKE '__/__/____'
    THEN SUBSTR(campo,7,4)||'-'||SUBSTR(campo,4,2)||'-'||SUBSTR(campo,1,2)
    ELSE campo
END
```

Padrão de output dd/mm/aaaa:
```sql
CASE
    WHEN campo LIKE '____-__-__%'
    THEN SUBSTR(campo,9,2)||'/'||SUBSTR(campo,6,2)||'/'||SUBSTR(campo,1,4)
    WHEN campo LIKE '__/__/____' THEN campo
    ELSE ''
END
```

## Padrões DBA

- ROW_NUMBER sempre com `ORDER BY versao DESC` ou `ORDER BY id DESC` (mais recente)
- TRIM + UPPER para comparações de texto
- COALESCE(..., '') para evitar NULLs no output
- Filtros com `IS NOT NULL AND TRIM(campo) != ''`
- Datas sempre normalizadas antes de DATE() ou comparação

## Problemas Conhecidos

1. **decisoes**: parada em março — QiggerDecisionEngine não insere no v2
2. **resultado_gross**: nunca importado (sem arquivo fonte)
3. **consulta_siebel**: depende de importação manual dos CSVs do Siebel
4. **WAL corruption**: rodar `PRAGMA wal_checkpoint(TRUNCATE)` se erro SQLITE_CORRUPT

## Fluxo de Processamento (run.py completo)

1. Sincronizar Bot Oracle → Local
2. BS_VENDA_DU (coleta e atualização)
3. Arquivos pasta entrada (coverte_prop, gross, relatorio_objetos, backoffice, tim, siebel)
4. Auditoria vendas TIM (EVA + Cruzamento)
5. Cache unificada
6. Resolver pendentes
7. Geração homologação (6 scripts paralelos)
8. Reprocessamento endereços
9. Backup rede SMB
