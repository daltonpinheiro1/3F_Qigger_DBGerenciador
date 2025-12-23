# Análise do Cabeçalho Solicitado vs Banco Unificado

## Campos do Cabeçalho Solicitado

### ✅ CAMPOS JÁ IMPLEMENTADOS

1. **ID_ISIZE** → `id_isize` ✅
2. **CLIENTE_CPF** → `cpf` ✅
3. **CHIP_ID** → `chip_id` ✅
4. **CLIENTE_NOME** → `cliente_nome` ✅
5. **DATA_BRUTA** → `data_bruta` ✅
6. **STATUS_PRECRIVO** → `status_precrivo` ✅
7. **DATA_HORA PRECRIVO** → `data_precrivo`, `hora_precrivo` ✅
8. **STATUS_LOG** → `status_logistica` ✅
9. **DATA_LOGISTICA** → `data_logistica` ✅
10. **DATA_ENTREGA** → `data_entrega` ✅
11. **PRAZO DE ENTREGA** → `prazo_entrega` ✅
12. **STATUS_GROSS** → `status_bilhete` ✅ (ou `data_gross`)
13. **DATA_GROSS** → `data_gross` ✅
14. **STATUS BP INICIAL** → `status_bp_inicial` ✅
15. **STATUS BP ATUAL** → `status_bp_atual` ✅
16. **MOTIVO CANCELAMENTO** → `motivo_cancelamento` ✅
17. **MOTIVO RECUSA** → `motivo_recusa` ✅
18. **REAGENDAMENTOS DE BP** → `houve_reagendamento`, `data_reagendamento_crm` ✅

### ⚠️ CAMPOS QUE PRECISAM DE AJUSTE/TRATAMENTO

1. **CLIENTE_TELEFONE (NUMERO PORTADO OU PROVISORIO SE NULL NUMERO CONTATO TRATADO DDD+TELEFONE)**
   - **Status**: Precisa de função calculada
   - **Campos disponíveis**: `telefone_portado`, `numero_provisorio`, `cliente_telefone`
   - **Lógica necessária**: 
     ```
     SE telefone_portado IS NOT NULL → usar telefone_portado
     SENÃO SE numero_provisorio IS NOT NULL → usar numero_provisorio
     SENÃO → usar cliente_telefone (com DDD tratado)
     ```

2. **TELEFONE PORTADO** → `telefone_portado` ✅ (já existe)

3. **STATUS AUDITORIA**
   - **Status**: Precisa verificar mapeamento
   - **Possível origem**: `status_ordem` ou campo específico
   - **Pergunta**: Qual é o campo equivalente no SQL? (pode ser `STATUS_ATUAL` da tabela #VENDA)

4. **DATA_HORA ENVIO API** → `data_envio` ✅ (já existe, mas precisa confirmar formato data+hora)

5. **AUDITOR_NOME**
   - **Status**: ❌ NÃO EXISTE
   - **Necessário**: Adicionar campo
   - **Pergunta**: Onde buscar este dado? (pode vir de alguma tabela de auditoria)

6. **STATUS_ENTREGA**
   - **Status**: Precisa verificar
   - **Possível**: `status_logistica` ou campo específico
   - **Pergunta**: É diferente de `status_logistica`?

7. **TRATATIVA ATUAL**
   - **Status**: Possível mapeamento
   - **Campos disponíveis**: `acao_a_realizar` (pode ser este)
   - **Pergunta**: Confirma que é `acao_a_realizar`?

8. **SPIN NUMVAGO**
   - **Status**: ❌ NÃO EXISTE
   - **Necessário**: Adicionar campo
   - **Pergunta**: O que é SPIN NUMVAGO? De qual fonte vem?

9. **FOI NUMVAGO? (SE POSSUI HISTORICO)**
   - **Status**: ❌ NÃO EXISTE
   - **Necessário**: Função calculada ou campo
   - **Lógica**: Verificar histórico se já houve "NUMERO VAGO"
   - **Pergunta**: Verificar na tabela de auditoria/histórico?

10. **WHATSAPP ENVIADAS (SE EXISTE NO WPP)**
    - **Status**: ❌ NÃO EXISTE
    - **Necessário**: Contar registros na tabela de disparos WPP
    - **Pergunta**: Existe tabela de disparos WPP? Onde estão armazenados?

11. **QUANTOS DISPAROS TEMPLETE 1 a 15**
    - **Status**: ❌ NÃO EXISTE
    - **Necessário**: Contar disparos por template_id
    - **Pergunta**: Onde estão armazenados os disparos? Qual tabela?

12. **DATA_ULTIMO_DISPARO**
    - **Status**: ❌ NÃO EXISTE
    - **Necessário**: Data do último disparo WPP
    - **Pergunta**: Mesma tabela de disparos?

13. **MÊS ANO GROSS**
    - **Status**: Precisa de função calculada
    - **Campo base**: `data_gross`
    - **Lógica**: `MONTH(data_gross) + '/' + YEAR(data_gross)`

14. **MÊS ANO VB**
    - **Status**: Precisa de função calculada
    - **Campo base**: `data_bruta` (DATA_BRUTA = VENDA_DATA)
    - **Lógica**: `MONTH(data_bruta) + '/' + YEAR(data_bruta)`

15. **SLA_INPUT (FIM DO ATENDIMENTO - DATAE HORA FIM DO PROCESSAMENTO DO ROBO) <120 MIN SIM, >=120 NÃO)**
    - **Status**: ❌ NÃO EXISTE
    - **Necessário**: Campo ou função calculada
    - **Lógica**: 
      - Calcular diferença entre `data_bruta` (ou `hora_bruta`) e data/hora fim do processamento do robô
      - Se < 120 minutos → 'SIM'
      - Se >= 120 minutos → 'NÃO'
    - **Pergunta**: 
      - Onde está armazenado "DATAE HORA FIM DO PROCESSAMENTO DO ROBO"?
      - Pode ser `data_final_processamento` ou campo específico?

## RESUMO

### Total de Campos: 47

- ✅ **Implementados/Disponíveis**: 18 campos
- ⚠️ **Precisam Ajuste/Tratamento**: 8 campos
- ❌ **Não Existem/Precisam Ser Criados**: 9 campos
- ❓ **Precisam Especificação**: 12 campos

## PRÓXIMOS PASSOS

### 1. Campos que Precisam Ser Adicionados ao Schema

```sql
-- Campos a adicionar na tabela tim_unificado
ALTER TABLE tim_unificado ADD COLUMN auditor_nome TEXT;
ALTER TABLE tim_unificado ADD COLUMN status_auditoria TEXT;
ALTER TABLE tim_unificado ADD COLUMN status_entrega TEXT;
ALTER TABLE tim_unificado ADD COLUMN spin_numvago TEXT;
ALTER TABLE tim_unificado ADD COLUMN foi_numvago INTEGER; -- 0 ou 1
ALTER TABLE tim_unificado ADD COLUMN sla_input TEXT; -- 'SIM' ou 'NÃO'
ALTER TABLE tim_unificado ADD COLUMN data_hora_fim_processamento_robo TEXT;
```

### 2. Tabela de Disparos WPP (se não existir)

```sql
CREATE TABLE IF NOT EXISTS tim_unificado_disparos_wpp (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    id_isize TEXT NOT NULL,
    template_id INTEGER NOT NULL,
    data_disparo TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status_disparo TEXT,
    FOREIGN KEY (id_isize) REFERENCES tim_unificado(id_isize)
);
```

### 3. Funções Calculadas/Views Necessárias

1. **CLIENTE_TELEFONE**: View ou função para calcular telefone prioritário
2. **MÊS ANO GROSS**: Função para formatar `data_gross`
3. **MÊS ANO VB**: Função para formatar `data_bruta`
4. **SLA_INPUT**: Função para calcular SLA
5. **FOI NUMVAGO?**: Query para verificar histórico
6. **WHATSAPP ENVIADAS**: COUNT de disparos
7. **QUANTOS DISPAROS TEMPLETE X**: COUNT por template_id

## ESPECIFICAÇÕES NECESSÁRIAS

Por favor, forneça informações sobre os seguintes campos:

### Campos que Precisam de Especificação

1. **AUDITOR_NOME**: 
   - ❓ De qual fonte/tabela vem este dado?
   - ❓ Está na base analítica ou em alguma tabela de auditoria?

2. **STATUS_AUDITORIA**: 
   - ❓ É o mesmo que `status_ordem` ou é diferente?
   - ❓ No SQL fornecido, parece ser `STATUS_ATUAL` da tabela #VENDA?
   - ❓ Confirmar mapeamento

3. **STATUS_ENTREGA**: 
   - ❓ É diferente de `status_logistica`?
   - ❓ No SQL, parece ser o `TEXTO TRATADO 2` da tabela #LOG2 (ENTREGA, EM ROTA, FINALIZADOR, etc)?

4. **SPIN NUMVAGO**: 
   - ❓ O que é exatamente? 
   - ❓ É um campo da tabela de ineficiências? (vi `NUMERO VAGO` no SQL)
   - ❓ É o mesmo que `tipo_ineficiencia = 'NUMERO VAGO'`?

5. **TRATATIVA ATUAL**: 
   - ❓ Confirma que é `acao_a_realizar`?
   - ❓ Ou é outro campo?

6. **WHATSAPP ENVIADAS e DISPAROS TEMPLATE 1-15**: 
   - ❓ Atualmente os disparos são gerados apenas como CSV (`homologacao_wpp.csv`, `WPP_Regua_Output.csv`)
   - ❓ Precisamos criar uma tabela para armazenar disparos realizados?
   - ❓ Ou já existe algum sistema externo que rastreia isso?
   - ⚠️ **SUGESTÃO**: Criar tabela `tim_unificado_disparos_wpp` para rastrear disparos

7. **DATA HORA FIM PROCESSAMENTO ROBO**: 
   - ❓ Qual campo/tabela tem esta informação?
   - ❓ É `data_final_processamento` do banco de portabilidade?
   - ❓ Ou vem de outra fonte?

8. **DATA_HORA ENVIO API**: 
   - ❓ Já temos `data_envio` no banco
   - ❓ Precisa incluir hora também? (criar campo separado `hora_envio`?)

### Campos com Lógica Específica (Baseado no SQL Fornecido)

Analisando o SQL fornecido, identifiquei a lógica para alguns campos:

#### CLIENTE_TELEFONE (tratado)
```sql
-- Lógica do SQL:
CASE WHEN A.TELEFONE_PORTABILIDADE IS NULL 
     THEN A.CLIENTE_TELEFONE 
     ELSE A.TELEFONE_PORTABILIDADE 
END AS [TELEFONE PORTADO],
-- Depois usar PROVISORIO tratado se não houver
```

#### PROVISORIO (tratado)
```sql
CASE WHEN LEN(A.NUMERO_PROVISORIO) = 9 
     THEN CONCAT(LEFT(A.CLIENTE_TELEFONE,2),A.NUMERO_PROVISORIO)
     WHEN LEN(A.NUMERO_PROVISORIO) = 11 
     THEN A.NUMERO_PROVISORIO
     WHEN LEN(A.NUMERO_PROVISORIO) IS NULL OR A.NUMERO_PROVISORIO in ('')
     THEN A.CLIENTE_TELEFONE
     WHEN LEN(A.NUMERO_PROVISORIO) = 10 
     THEN CONCAT(LEFT(A.NUMERO_PROVISORIO,2),'9',RIGHT(A.NUMERO_PROVISORIO,8))
     ELSE A.NUMERO_PROVISORIO 
END AS [PROVISORIO]
```

#### STATUS_ENTREGA
No SQL, é o `TEXTO TRATADO 2` que categoriza como:
- ENTREGA
- EM ROTA  
- FINALIZADOR
- REAGENDADO
- AGUARDANDO RETIRADA CORREIO

#### FOI NUMVAGO?
Precisaria verificar histórico na tabela de ineficiências ou auditoria se já houve registro com `NUMERO VAGO`.

## MAPEAMENTO PROPOSTO (Baseado no SQL)

Com base no SQL fornecido, sugiro o seguinte mapeamento:

| Campo Solicitado | Mapeamento Proposto | Origem |
|-----------------|---------------------|--------|
| STATUS AUDITORIA | `order_status` ou `status_ordem` | SQL: ORDER_STATUS_ATUAL |
| STATUS_ENTREGA | `status_log_real` (TEXTO TRATADO 2) | SQL: #LOG2 |
| SPIN NUMVAGO | Verificar `tipo_ineficiencia = 'NUMERO VAGO'` | SQL: #INEFICIENCIA |
| FOI NUMVAGO? | Verificar histórico de `tipo_ineficiencia` | SQL: #INEFICIENCIA |
| DATA_HORA ENVIO API | `data_envio` + campo `hora_envio` (adicionar) | SQL: #REENVIOUNIQUE_DATA |
| TRATATIVA ATUAL | `acao_a_realizar` | Trigger rules |

## PRÓXIMOS PASSOS SUGERIDOS

1. **Confirmar especificações** dos campos marcados com ❓
2. **Adicionar campos faltantes** ao schema
3. **Criar tabela de disparos WPP** (se necessário)
4. **Implementar funções calculadas** para campos derivados
5. **Criar view ou função** para gerar o cabeçalho padronizado

