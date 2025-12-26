# Testes de Homologação

## 📋 Visão Geral

Este diretório contém os arquivos de teste para homologação dos seguintes itens:

1. **WhatsApp** - Templates, variáveis e geração de arquivo de homologação
2. **Aprovisionadas** - Com confirmação de entrega
3. **Reabertura** - Vendas canceladas e novo status de order

## 📁 Arquivos de Teste

### 1. `test_homologacao_wpp.py`
**Objetivo**: Testar toda a funcionalidade relacionada ao WhatsApp

**Cobertura**:
- Mapeamento de templates (1, 2, 3, 4)
- Geração de variáveis para cada template
- Formatação de variáveis como string
- Geração de arquivo CSV de homologação
- Enriquecimento com informações de template
- Geração de links de rastreio
- Validação de dados do cliente
- Validação de Status_Disparo e DataHora_Disparo

**Classes de Teste**:
- `TestHomologacaoWPP`: Testes principais de homologação WPP

**Total de Testes**: 25+

### 2. `test_homologacao_aprovisionadas.py`
**Objetivo**: Testar funcionalidade de aprovisionadas com confirmação de entrega

**Cobertura**:
- Filtro de registros em aprovisionamento
- Validação de confirmação de entrega
- Geração de CSV de aprovisionamentos
- Validação de link de rastreio obrigatório
- Validação de dados completos de entrega
- Múltiplos registros

**Classes de Teste**:
- `TestHomologacaoAprovisionadas`: Testes de aprovisionadas

**Total de Testes**: 15+

### 3. `test_homologacao_reabertura.py`
**Objetivo**: Testar funcionalidade de vendas canceladas e reabertura de orders

**Cobertura**:
- Filtro de registros cancelados
- Validação de novo status de order para reabertura
- Geração de CSV de reabertura
- Validação de transição de status
- Validação de ações (REABRIR, REAGENDAR)
- Múltiplos registros

**Classes de Teste**:
- `TestHomologacaoReabertura`: Testes de reabertura

**Total de Testes**: 20+

## 🚀 Como Executar

### Executar todos os testes de homologação:
```bash
pytest tests/test_homologacao_*.py -v
```

### Executar testes específicos:
```bash
# Apenas WhatsApp
pytest tests/test_homologacao_wpp.py -v

# Apenas Aprovisionadas
pytest tests/test_homologacao_aprovisionadas.py -v

# Apenas Reabertura
pytest tests/test_homologacao_reabertura.py -v
```

### Executar com cobertura:
```bash
pytest tests/test_homologacao_*.py --cov=src --cov-report=html -v
```

### Executar um teste específico:
```bash
pytest tests/test_homologacao_wpp.py::TestHomologacaoWPP::test_template_mapper_get_template_id -v
```

## ✅ Checklist de Validação

### WhatsApp
- [x] Templates mapeados corretamente (1, 2, 3, 4)
- [x] Variáveis geradas para cada template
- [x] Arquivo CSV de homologação gerado
- [x] Status_Disparo sempre FALSE
- [x] DataHora_Disparo sempre vazio
- [x] Links de rastreio presentes
- [x] Dados do cliente completos

### Aprovisionadas
- [x] Filtro por status do bilhete
- [x] Filtro por status da ordem
- [x] Filtro por resultado de decisão
- [x] Confirmação de entrega validada
- [x] CSV gerado corretamente
- [x] Link de rastreio obrigatório

### Reabertura
- [x] Filtro por status cancelado
- [x] Filtro por motivo de cancelamento
- [x] Novo status de order validado
- [x] Transição de status validada
- [x] CSV gerado corretamente
- [x] Ações de reabertura validadas

## 📊 Estrutura dos Testes

Cada arquivo de teste segue a mesma estrutura:

1. **Fixtures**: Dados de teste reutilizáveis
2. **Testes de Filtro**: Validar critérios de filtragem
3. **Testes de Geração**: Validar geração de CSVs
4. **Testes de Validação**: Validar dados e campos
5. **Testes de Múltiplos Registros**: Validar processamento em lote

## 🔍 Detalhes dos Testes

### Testes de WhatsApp
- `test_template_mapper_get_template_id`: Mapeamento de tipos de comunicação para templates
- `test_template_mapper_generate_variables_template_3`: Geração de variáveis para template 3
- `test_template_mapper_generate_variables_template_4`: Geração de variáveis para template 4
- `test_wpp_output_generator_generate_csv`: Geração de CSV de homologação
- `test_homologacao_template_1_confirma_portabilidade`: Homologação completa template 1
- `test_homologacao_template_2_pendencia_sms`: Homologação completa template 2
- `test_homologacao_template_3_retirada_correios`: Homologação completa template 3
- `test_homologacao_template_4_confirmacao_endereco`: Homologação completa template 4

### Testes de Aprovisionadas
- `test_filtrar_aprovisionados_por_status_bilhete`: Filtro por status do bilhete
- `test_filtrar_aprovisionados_por_status_ordem`: Filtro por status da ordem
- `test_gerar_csv_aprovisionamentos`: Geração de CSV
- `test_validar_confirmacao_entrega_por_status_logistica`: Validação de entrega
- `test_validar_link_rastreio_em_aprovisionados`: Validação de link de rastreio

### Testes de Reabertura
- `test_filtrar_cancelados_por_status_bilhete`: Filtro por status cancelado
- `test_filtrar_cancelados_por_motivo_cancelamento`: Filtro por motivo
- `test_validar_novo_status_bilhete`: Validação de novo status
- `test_validar_novo_status_ordem`: Validação de novo status de ordem
- `test_gerar_csv_reabertura`: Geração de CSV
- `test_validar_acao_reabertura`: Validação de ação de reabertura

## 📝 Notas Importantes

1. **Fixtures**: Todos os testes usam fixtures para criar dados de teste consistentes
2. **Arquivos Temporários**: Os testes criam arquivos temporários que são limpos automaticamente
3. **Mocks**: Alguns testes usam mocks para simular dependências externas
4. **Validação**: Todos os testes validam tanto sucesso quanto falha dos cenários

## 🐛 Troubleshooting

### Erro: "ModuleNotFoundError"
- Verifique se todas as dependências estão instaladas: `pip install -r requirements.txt`

### Erro: "FileNotFoundError"
- Verifique se os arquivos de teste estão no diretório correto
- Verifique se os caminhos dos arquivos temporários estão corretos

### Erro: "AssertionError"
- Revise o teste que falhou
- Verifique se os dados de teste estão corretos
- Verifique se a lógica do código está correta

## 📚 Documentação Relacionada

- `docs/REVISAO_PROJETO_HOMOLOGACAO.md`: Revisão completa do projeto
- `README.md`: Documentação principal do projeto
- `CHANGELOG.md`: Histórico de mudanças

---

**Última Atualização**: 2025-12-22  
**Status**: ✅ Completo e Pronto para Execução

