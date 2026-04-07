# Regras dos geradores de homologação

Este documento descreve as regras de negócio e dependências de cada script que gera arquivos de homologação no fluxo `processar_completo.py`.

---

## Ordem de execução (Etapa 4)

1. **gerar_homologacao_wpp.py** – WhatsApp (Régua de Comunicação)  
2. **gerar_homologacao_reabertura.py** – Reabertura  
3. **gerar_homologacao_aprovisionamento.py** – Aprovisionamento  
4. **gerar_homologacao_erro_aprovisionamento.py** – Erro no Aprovisionamento  
5. **gerar_homologacao_entrega_baixa.py** – Entrega/Baixa  
6. **gerar_homologacao_consulta.py** – Consulta  

Cada script roda em **subprocesso independente**; falha em um não interrompe os demais.

---

## 1. WPP (WhatsApp)

| Item | Detalhe |
|------|--------|
| **Script** | `gerar_homologacao_wpp.py` |
| **Saída** | `data/homologacao_wpp.xlsx` |
| **Tabelas lidas** | portabilidade_records, base_coverte_prop, relatorio_objetos, templates_wpp, tipo_comunicacao_template; opcional: portabilidade_processamento |

### Regras de negócio (detalhadas)

- **Crivo**: apenas clientes com `crivo_vendas = 'APROVADA'` (em base_coverte_prop) entram na fila; exceção: IDs listados em `data/ids_forcar_wpp.txt` são incluídos independente de crivo ou template.
- **Período**: apenas últimos **90 dias** (DIAS_LIMITE_HOMOLOGACAO); usa `data_conectada` ou `data_venda` para filtrar.
- **Telefone**: apenas vendas com `telefone_portado` preenchido (não “nova linha”).
- **Entrega**: exclui entregas com status CANCELADA, CANCELADO, EXTRAVIADA, EXTRAVIADO, EXTRAVIO, BAIXA, REMETENTE (status_correios, status_loggi, status_entrega_prevista).
- **Rejeição SMS**: exclui registros com `status_bilhete` contendo “rejeicao sms”.
- **Unicidade**: um registro por (codigo_externo/proposta_isize, telefone_portado); ordenação por data_conectada mais recente.
- **Templates**: usa tipo_mensagem/template de portabilidade_records; se já tem template 1, retorna template 2; máximo **3 tentativas** por template 1 ou 2, depois sai da fila.
- **Controle de envio**: mínimo **48 horas** entre envios para o mesmo cliente (HORAS_ENTRE_ENVIOS); histórico pode ser consultado via Google Sheets (opcional).
- **IDs forçados**: arquivo `data/ids_forcar_wpp.txt` — um ID (proposta_isize/codigo_externo) por linha; linhas com # são ignoradas.

### Dependências

- Nenhum outro gerador; é a base de crivo usada por Reabertura, Aprovisionamento e Erro Aprovisionamento.

---

## 2. Reabertura

| Item | Detalhe |
|------|--------|
| **Script** | `gerar_homologacao_reabertura.py` |
| **Saída** | `data/homologacao_reabertura.xlsx` |
| **Tabelas lidas** | base_coverte_prop, portabilidade_records, relatorio_objetos, portabilidade_processamento (validação) |

### Regras de negócio (detalhadas)

- **Foco**: registros **cancelados** (reabertura de portabilidade).
- **Filtro de status**: `status_bilhete = 'Portabilidade Cancelada'` **ou** `motivo_cancelamento` preenchido (não vazio e não 'NULL').
- **Período**: últimos **90 dias** (DIAS_LIMITE_HOMOLOGACAO); usa data da venda/conectada para filtrar.
- **Unicidade**: um registro **mais recente** por `codigo_externo` (MAX(id)); o status considerado é o atual, não histórico.
- **Rejeição SMS**: exclui registros com `status_bilhete` contendo “rejeicao sms”.
- **Validação**: após a query, aplica **filtrar_registros_validos** usando a tabela **portabilidade_processamento** (STATUS, MOTIVO_CONFLITO, etc.); apenas registros válidos vão para o Excel.
- **Saída**: Excel gerado via CSVGenerator (colunas: CPF, numero_acesso, codigo_externo, status_bilhete, status_ordem, motivo_cancelamento, etc.).

### Dependências

- base_coverte_prop + portabilidade_records (+ relatorio_objetos se existir); validação com portabilidade_processamento.

---

## 3. Aprovisionamento

| Item | Detalhe |
|------|--------|
| **Script** | `gerar_homologacao_aprovisionamento.py` |
| **Saída** | `data/homologacao_aprovisionamento.xlsx` |
| **Tabelas lidas** | base_coverte_prop, portabilidade_records, relatorio_objetos, portabilidade_processamento (validação) |

### Regra unificada (Aprovisionamento e Erro no Aprovisionamento)

Se a **proposta avaliada de forma única** tiver **algum histórico de entrega** e o **status da ordem** estiver em **Em Aprovisionamento** ou **Erro no Aprovisionamento**, ela **retorna nos arquivos processados de retorno** (um arquivo para cada tipo).  
**Histórico de entrega** = existir dado de logística (relatorio_objetos/Relatório de Objetos): nu_pedido, rastreio, status_entrega, ultima_ocorrencia, data_entrega, iccid ou correspondência no ObjectsLoader. Não é exigido status “entregue”.

### Regras de negócio (detalhadas) – Aprovisionamento

- **Foco**: registros **em aprovisionamento** com **histórico de entrega** (não exige “entregue”).
- **Filtro de status**: `status_ordem = 'Em Aprovisionamento'` **ou** `status_bilhete = 'Em Aprovisionamento'` (exclui Erro no Aprovisionamento).
- **Histórico de entrega**: obrigatório (ro_* ou Relatório de Objetos); sem isso o registro não entra no arquivo.
- **Período**: últimos **90 dias** (DIAS_LIMITE); usa data da venda/conectada para filtrar.
- **Unicidade**: registro **mais recente** por `codigo_externo` (MAX(id)); status considerado é o atual.
- **Rejeição SMS**: exclui registros com `status_bilhete` contendo “rejeicao sms”.
- **Validação**: **filtrar_registros_validos** com portabilidade_processamento; apenas registros válidos no Excel.
- **Saída**: Excel com colunas de portabilidade + novo_status_bilhete, nova_data_portabilidade, etc.

### Dependências

- base_coverte_prop, portabilidade_records, relatorio_objetos; portabilidade_processamento para validação.

---

## 4. Erro no Aprovisionamento

| Item | Detalhe |
|------|--------|
| **Script** | `gerar_homologacao_erro_aprovisionamento.py` |
| **Saída** | `data/homologacao_erro_aprovisionamento.xlsx` |
| **Tabelas lidas** | base_coverte_prop, portabilidade_records, relatorio_objetos, portabilidade_processamento (validação) |

### Regras de negócio (detalhadas) – Erro no Aprovisionamento

- **Foco**: registros com **erro no aprovisionamento** que tenham **histórico de entrega** (mesma regra unificada acima).
- **Filtro de status**: `status_ordem = 'Erro no Aprovisionamento'` **ou** `status_bilhete = 'Erro no Aprovisionamento'`.
- **Histórico de entrega**: obrigatório (ro_* ou Relatório de Objetos); sem isso o registro não entra no arquivo.
- **Período**: últimos **90 dias** (DIAS_LIMITE); usa data da venda/conectada para filtrar.
- **Unicidade**: **todas** as linhas com esse status (vários numero_acesso/numero_ordem por codigo_externo), sem deduplicar por codigo_externo.
- **Validação**: **filtrar_registros_validos** com portabilidade_processamento; apenas registros válidos no Excel.
- **Saída**: Excel gerado diretamente (sem CSVGenerator) com colunas de portabilidade + status/data entrega (ro_*), etc.

### Dependências

- base_coverte_prop, portabilidade_records, relatorio_objetos; portabilidade_processamento para validação.

---

## 5. Entrega/Baixa

| Item | Detalhe |
|------|--------|
| **Script** | `gerar_homologacao_entrega_baixa.py` |
| **Saída** | `data/homologacao_entrega_baixa.xlsx` |
| **Tabelas lidas** | base_coverte_prop, relatorio_objetos, portabilidade_records, portabilidade_processamento, base_unificada (exclusão) |

### Regras de negócio (detalhadas)

- **Foco**: vendas em que a **situação mais recente de entrega** está: cancelada, baixa, remetente/aguardando correios, extraviada.
- **Relatório de objetos**: quando usado, considera **apenas o registro mais recente por codigo_externo**, pela **data de atualização** (`updated_at`, fallback `created_at`), para evitar múltiplas linhas por pedido.
- **Status de entrega considerados** (em qualquer um: ultima_ocorrencia, status de relatorio_objetos, status_correios, status_loggi, status_entrega_prevista): contendo **cancelada**, **baixa**, **remetente**, **aguardando correios**, **extraviada** (case insensitive).
- **Período**: últimos **90 dias** (DIAS_LIMITE_HOMOLOGACAO); usa `data_venda` (ou `data_insercao` quando só relatorio_objetos).
- **Fonte**: **base_coverte_prop** quando existir; se não existir, usa **apenas relatorio_objetos** (mais recente por updated_at), sem abortar.
- **Exclusão rejeição SMS**: **não inclui** vendas com rejeição SMS em:
  - **portabilidade_records**: status_bilhete like '%rejeicao sms%' ou motivo_recusa/motivo_cancelamento like '%rejei%cliente%sms%';
  - **portabilidade_processamento**: MOTIVO_CONFLITO ou MOTIVO_CANCELAMENTO like '%rejei%cliente%sms%';
  - **base_unificada** (se existir e tiver as colunas): status_bilhete, motivo_recusa, motivo_cancelamento com rejeição SMS.
- **Saída**: CSV com mesmo modelo de cabeçalho do WPP (codigo_externo, cpf, cliente_nome, telefone_portado, endereço, status_entrega, rastreio, etc.); número apenas dígitos, complemento no campo complemento.

### Dependências

- base_coverte_prop (quando existir) ou apenas relatorio_objetos (mais recente por updated_at); portabilidade_records e portabilidade_processamento para exclusão de rejeição SMS.

---

## 6. Consulta

| Item | Detalhe |
|------|--------|
| **Script** | `gerar_homologacao_consulta.py` |
| **Saída** | `data/homologacao_consulta.xlsx` |
| **Tabelas lidas** | base_coverte_prop, relatorio_objetos (mais recente por updated_at), portabilidade_records, portabilidade_processamento (exclusão) |

### Regras de negócio (detalhadas)

- **Foco**: **todas as vendas com confirmação de entrega** (entregue / pedido entregue), para consulta de situação.
- **Fonte**: **base_coverte_prop** como principal, com **fallback em relatorio_objetos** (registro mais recente por `updated_at` por codigo_externo), para assegurar que pedido entregue ou “entregue” tenha sua situação consultada.
- **Filtro de entrega**: status/ultima_ocorrencia (em bc ou ro) contendo **entregue** ou **pedido entregue** (case insensitive).
- **Exclusão rejeição SMS**: não inclui vendas com rejeição SMS em portabilidade_records e portabilidade_processamento (mesmo critério dos demais geradores).
- **Saída**: Excel com colunas Cpf, Número de acesso, Número da ordem, Código externo; gerado em `data/homologacao_consulta.xlsx` e copiado pelo `processar_completo` na Etapa 4.

---

## Filtro de período

Todos os geradores de homologação usam **90 dias** (DIAS_LIMITE_HOMOLOGACAO ou DIAS_LIMITE) para limitar o período dos dados, alinhado às regras do WPP. A data de referência usada varia por script (data_venda, data_conectada, etc.).

---

## Validação pós-geração

- O `processar_completo.py` valida cada arquivo gerado: existência e tamanho > 0.
- Arquivos vazios geram aviso no log.
- Os arquivos são copiados com timestamp para **PASTA_SAIDA_HOMOLOGACAO** (Retornos do gerenciador) e o original em `data/` é movido para `data/processados`.

---

## Resumo rápido por gerador

| Gerador        | Filtro principal                                      | Período | Validação (portabilidade_processamento) |
|----------------|--------------------------------------------------------|--------|----------------------------------------|
| WPP            | crivo APROVADA + template + telefone_portado          | 90 dias | —                                      |
| Reabertura     | status_bilhete = Portabilidade Cancelada ou motivo_cancelamento | 90 dias | Sim                                    |
| Aprovisionamento | status_ordem/bilhete = Em Aprovisionamento (exclui Erro) | 90 dias | Sim                                    |
| Erro Aprovisionamento | status_ordem/bilhete = Erro no Aprovisionamento   | 90 dias | Sim                                    |
| Entrega/Baixa  | status entrega: cancelada/baixa/remetente/extraviada   | 90 dias | Exclusão rejeição SMS                  |
| Consulta       | Confirmação de entrega (entregue/pedido entregue), base_coverte_prop + fallback ro | —       | Exclusão rejeição SMS; no fluxo completo |
