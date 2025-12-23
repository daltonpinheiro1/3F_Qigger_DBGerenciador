# ESPECIFICAÇÕES NECESSÁRIAS - Cabeçalho Padronizado

## 📊 RESUMO GERAL

**Total de Campos Solicitados**: 47 campos

- ✅ **Já Implementados**: 18 campos
- ⚠️ **Precisam Ajuste/Tratamento**: 8 campos  
- ❌ **Não Existem - Precisam Ser Criados**: 9 campos
- ❓ **Precisam Especificação**: 12 campos

---

## ✅ CAMPOS JÁ IMPLEMENTADOS (18)

1. ✅ ID_ISIZE → `id_isize`
2. ✅ CLIENTE_CPF → `cpf`
3. ✅ CHIP_ID → `chip_id`
4. ✅ CLIENTE_NOME → `cliente_nome`
5. ✅ DATA_BRUTA → `data_bruta`
6. ✅ STATUS_PRECRIVO → `status_precrivo`
7. ✅ DATA_HORA PRECRIVO → `data_precrivo`, `hora_precrivo`
8. ✅ STATUS_LOG → `status_logistica`
9. ✅ DATA_LOGISTICA → `data_logistica`
10. ✅ DATA_ENTREGA → `data_entrega`
11. ✅ PRAZO DE ENTREGA → `prazo_entrega`
12. ✅ STATUS_GROSS → `status_bilhete`
13. ✅ DATA_GROSS → `data_gross`
14. ✅ STATUS BP INICIAL → `status_bp_inicial`
15. ✅ STATUS BP ATUAL → `status_bp_atual`
16. ✅ MOTIVO CANCELAMENTO → `motivo_cancelamento`
17. ✅ MOTIVO RECUSA → `motivo_recusa`
18. ✅ REAGENDAMENTOS DE BP → `houve_reagendamento`, `data_reagendamento_crm`

---

## ⚠️ CAMPOS QUE PRECISAM DE AJUSTE/TRATAMENTO (8)

### 1. CLIENTE_TELEFONE (NUMERO PORTADO OU PROVISORIO SE NULL NUMERO CONTATO TRATADO DDD+TELEFONE)

**Campos Disponíveis**: `telefone_portado`, `numero_provisorio`, `cliente_telefone`

**Lógica Necessária** (baseada no SQL):
```
1. SE telefone_portado IS NOT NULL → usar telefone_portado
2. SENÃO SE numero_provisorio IS NOT NULL → tratar numero_provisorio:
   - Se len = 9 → CONCAT(LEFT(cliente_telefone,2), numero_provisorio)
   - Se len = 11 → usar numero_provisorio
   - Se len = 10 → CONCAT(LEFT(numero_provisorio,2),'9',RIGHT(numero_provisorio,8))
   - Senão → usar numero_provisorio
3. SENÃO → usar cliente_telefone (com DDD tratado)
```

**Ação**: Criar função calculada ou campo tratado

---

### 2. TELEFONE PORTADO

✅ Já existe: `telefone_portado`

---

### 3. STATUS AUDITORIA

**Status**: ⚠️ Precisa confirmar mapeamento

**Possíveis Mapeamentos**:
- `status_ordem` (mais provável)
- Campo específico de auditoria

**No SQL fornecido**: `STATUS_ATUAL` da tabela #VENDA ou `ORDER STATUS ATUAL`

**Pergunta**: É o mesmo que `status_ordem` ou `order_status`?

---

### 4. DATA_HORA ENVIO API

✅ Campo existe: `data_envio`

**Pergunta**: Precisa incluir hora também? Criar campo `hora_envio` separado ou incluir na data?

---

### 5. STATUS_ENTREGA

**Status**: ⚠️ Precisa confirmar mapeamento

**No SQL fornecido**: É o `TEXTO TRATADO 2` que categoriza:
- ENTREGA
- EM ROTA
- FINALIZADOR
- REAGENDADO
- AGUARDANDO RETIRADA CORREIO

**Campo disponível**: `status_log_real` ou `status_logistica`

**Pergunta**: É diferente de `status_logistica`? Ou usar `status_log_real`?

---

### 6. TRATATIVA ATUAL

**Status**: ⚠️ Possível mapeamento

**Campo disponível**: `acao_a_realizar`

**Pergunta**: Confirma que `TRATATIVA ATUAL` = `acao_a_realizar`?

---

### 7. MÊS ANO GROSS

**Status**: ⚠️ Função calculada necessária

**Campo base**: `data_gross`

**Lógica**: `MONTH(data_gross) + '/' + YEAR(data_gross)`

**Ação**: Criar função ou campo calculado

---

### 8. MÊS ANO VB

**Status**: ⚠️ Função calculada necessária

**Campo base**: `data_bruta` (DATA_BRUTA = VENDA_DATA)

**Lógica**: `MONTH(data_bruta) + '/' + YEAR(data_bruta)`

**Ação**: Criar função ou campo calculado

---

## ❌ CAMPOS QUE NÃO EXISTEM - PRECISAM SER CRIADOS (9)

### 1. AUDITOR_NOME

**Status**: ❌ NÃO EXISTE

**Pergunta**: 
- De qual fonte/tabela vem este dado?
- Está na base analítica?
- Vem de alguma tabela de auditoria?

**Ação**: Adicionar campo `auditor_nome TEXT` ao schema

---

### 2. SPIN NUMVAGO

**Status**: ❌ NÃO EXISTE

**No SQL fornecido**: Parece estar relacionado a `NUMERO VAGO` na tabela de ineficiências

**Pergunta**: 
- O que é exatamente SPIN NUMVAGO?
- É o mesmo que `tipo_ineficiencia = 'NUMERO VAGO'`?
- É um campo separado?

**Ação**: 
- Adicionar campo `spin_numvago TEXT` ao schema
- OU usar lógica: verificar se `tipo_ineficiencia = 'NUMERO VAGO'`

---

### 3. FOI NUMVAGO? (SE POSSUI HISTORICO)

**Status**: ❌ NÃO EXISTE

**Lógica Necessária**: Verificar histórico se já houve registro com `NUMERO VAGO`

**Pergunta**: 
- Verificar na tabela de auditoria/histórico?
- Verificar versões anteriores do registro no banco unificado?

**Ação**: 
- Criar função/query para verificar histórico
- OU adicionar campo calculado `foi_numvago INTEGER` (0 ou 1)

---

### 4. WHATSAPP ENVIADAS (SE EXISTE NO WPP)

**Status**: ❌ NÃO EXISTE

**Situação Atual**: 
- Disparos são gerados apenas como CSV (`homologacao_wpp.csv`, `WPP_Regua_Output.csv`)
- Não há tabela no banco para armazenar disparos

**Pergunta**: 
- Precisamos criar uma tabela para armazenar disparos realizados?
- Ou já existe algum sistema externo que rastreia isso?
- Os disparos são registrados em algum lugar?

**Ação Sugerida**: Criar tabela `tim_unificado_disparos_wpp`:

```sql
CREATE TABLE tim_unificado_disparos_wpp (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    id_isize TEXT NOT NULL,
    template_id INTEGER NOT NULL,
    data_disparo TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status_disparo TEXT,
    telefone_destino TEXT,
    FOREIGN KEY (id_isize) REFERENCES tim_unificado(id_isize)
);
```

---

### 5-19. QUANTOS DISPAROS TEMPLETE 1 a 15

**Status**: ❌ NÃO EXISTE

**Depende de**: Tabela de disparos WPP (campo 4)

**Lógica**: COUNT de disparos por `template_id` para cada `id_isize`

**Ação**: 
- Criar funções/queries para contar disparos por template
- OU campos calculados `disparos_template_1`, `disparos_template_2`, etc.

---

### 20. DATA_ULTIMO_DISPARO

**Status**: ❌ NÃO EXISTE

**Depende de**: Tabela de disparos WPP (campo 4)

**Lógica**: MAX(data_disparo) para cada `id_isize`

**Ação**: Campo calculado ou função

---

### 21. SLA_INPUT (FIM DO ATENDIMENTO - DATAE HORA FIM DO PROCESSAMENTO DO ROBO) <120 MIN SIM, >=120 NÃO)

**Status**: ❌ NÃO EXISTE

**Lógica Necessária**:
```
Calcular diferença entre:
- Data/Hora fim do atendimento (data_bruta/hora_bruta?)
- Data/Hora fim do processamento do robô

SE diferença < 120 minutos → 'SIM'
SENÃO → 'NÃO'
```

**Perguntas**:
- Onde está armazenado "DATAE HORA FIM DO PROCESSAMENTO DO ROBO"?
- É `data_final_processamento` do banco de portabilidade?
- É um campo específico de outra tabela?

**Ação**: 
- Adicionar campo `data_hora_fim_processamento_robo TEXT`
- Criar função calculada `sla_input` para calcular a diferença

---

## ❓ CAMPOS QUE PRECISAM DE ESPECIFICAÇÃO DETALHADA (12)

### Prioridade ALTA (afetam estrutura do banco)

1. **AUDITOR_NOME** - De onde vem?
2. **SPIN NUMVAGO** - O que é exatamente?
3. **DATA HORA FIM PROCESSAMENTO ROBO** - Qual campo/tabela?
4. **Tabela de Disparos WPP** - Criar ou já existe?

### Prioridade MÉDIA (ajustes/mapeamentos)

5. **STATUS_AUDITORIA** - É `status_ordem` ou campo separado?
6. **STATUS_ENTREGA** - É `status_log_real` ou `status_logistica`?
7. **TRATATIVA ATUAL** - Confirma que é `acao_a_realizar`?
8. **DATA_HORA ENVIO API** - Precisa hora separada?

### Prioridade BAIXA (funções calculadas)

9. **CLIENTE_TELEFONE** - Confirmar lógica de tratamento
10. **FOI NUMVAGO?** - Confirmar onde verificar histórico
11. **MÊS ANO GROSS** - Confirmar formato (MM/YYYY ou M/YYYY?)
12. **MÊS ANO VB** - Confirmar formato (MM/YYYY ou M/YYYY?)

---

## 📋 CHECKLIST DE IMPLEMENTAÇÃO

### Fase 1: Especificações
- [ ] Confirmar origem de AUDITOR_NOME
- [ ] Confirmar o que é SPIN NUMVAGO
- [ ] Confirmar campo de DATA HORA FIM PROCESSAMENTO ROBO
- [ ] Decidir sobre tabela de Disparos WPP (criar ou usar externa)
- [ ] Confirmar mapeamentos de STATUS_AUDITORIA e STATUS_ENTREGA
- [ ] Confirmar TRATATIVA ATUAL = acao_a_realizar

### Fase 2: Schema (Alterações no Banco)
- [ ] Adicionar campo `auditor_nome TEXT`
- [ ] Adicionar campo `spin_numvago TEXT` (se necessário)
- [ ] Adicionar campo `status_auditoria TEXT` (se diferente)
- [ ] Adicionar campo `status_entrega TEXT` (se diferente)
- [ ] Adicionar campo `hora_envio TEXT`
- [ ] Adicionar campo `data_hora_fim_processamento_robo TEXT`
- [ ] Criar tabela `tim_unificado_disparos_wpp` (se necessário)

### Fase 3: Funções Calculadas
- [ ] Implementar função `CLIENTE_TELEFONE` (tratado)
- [ ] Implementar função `FOI NUMVAGO?` (verificar histórico)
- [ ] Implementar função `MÊS ANO GROSS`
- [ ] Implementar função `MÊS ANO VB`
- [ ] Implementar função `SLA_INPUT`
- [ ] Implementar contadores de disparos por template (1-15)
- [ ] Implementar função `DATA_ULTIMO_DISPARO`
- [ ] Implementar função `WHATSAPP ENVIADAS` (COUNT)

### Fase 4: View/Query Padronizada
- [ ] Criar VIEW ou função que retorna todos os campos no formato solicitado
- [ ] Testar com dados reais
- [ ] Validar performance

---

## 🎯 PRÓXIMOS PASSOS IMEDIATOS

1. **Você precisa fornecer as especificações** dos campos marcados com ❓
2. **Após especificações**, implemento os campos faltantes
3. **Criamos as funções calculadas** para campos derivados
4. **Criamos view/função** para gerar o cabeçalho padronizado

---

**Status**: Aguardando suas especificações para prosseguir com a implementação completa! 🚀

