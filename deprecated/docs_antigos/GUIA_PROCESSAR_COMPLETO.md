# Guia de Uso - Script de Processamento Completo

## 📋 Visão Geral

O script `processar_completo.py` é um script unificado que processa **todas as bases** e gera **todos os arquivos de homologação** em sequência, eliminando a necessidade de executar múltiplos scripts manualmente.

## 🚀 Uso Básico

### Processar Tudo (Recomendado)

```bash
cd /Applications/Documentos/Projetos_python/3F_Qigger_DBGerenciador
source venv/bin/activate  # Ativar ambiente virtual
python3 processar_completo.py
```

Este comando executa **todas as etapas**:
1. ✅ Processa Excel COVERTE BASE PROP.xlsx → `base_coverte_prop`
2. ✅ Processa arquivos CSV de atualização → `portabilidade_records`
3. ✅ Processa Relatorio_Objetos.xlsx → `relatorio_objetos`
4. ✅ Gera todos os arquivos de homologação:
   - WPP (WhatsApp)
   - Reabertura
   - Aprovisionamento
   - Erro no Aprovisionamento

## ⚙️ Opções Disponíveis

### Apenas Processar Bases (sem gerar homologação)

```bash
python3 processar_completo.py --apenas-bases
```

Útil quando você só quer atualizar o banco de dados sem gerar arquivos de homologação.

### Apenas Gerar Homologação (sem processar bases)

```bash
python3 processar_completo.py --apenas-homologacao
```

Útil quando o banco já está atualizado e você só precisa gerar os arquivos de homologação.

### Pular Etapas Específicas

```bash
# Pular processamento do Excel
python3 processar_completo.py --skip-excel

# Pular processamento de CSV
python3 processar_completo.py --skip-csv

# Pular processamento de Relatorio_Objetos
python3 processar_completo.py --skip-objetos

# Combinar opções
python3 processar_completo.py --skip-excel --skip-csv
```

## 📊 O Que o Script Faz

### ETAPA 1: Processar Excel COVERTE BASE PROP
- Busca o arquivo em múltiplas pastas (rede, local, entrada)
- Processa e insere/atualiza na tabela `base_coverte_prop`
- Mostra estatísticas de processamento

### ETAPA 2: Processar CSV de Atualização
- Busca arquivos CSV nas pastas de importação e entrada
- Processa e mapeia para `portabilidade_records`
- Move arquivos processados para pasta `processados/`

### ETAPA 3: Processar Relatorio_Objetos
- Busca arquivos `Relatorio_Objetos*.xlsx`
- Sincroniza com a tabela `relatorio_objetos`
- Move arquivo processado para pasta `processados/`

### ETAPA 4: Gerar Arquivos de Homologação
- Executa cada script de homologação em sequência
- Copia arquivos gerados para pasta de saída com timestamp
- Pasta de saída: `/Applications/Documentos/Projetos_python/Retornos do gerenciador`

## 📁 Arquivos Gerados

Os arquivos de homologação são salvos em duas localizações:

1. **Localização original** (no projeto):
   - `data/homologacao_wpp.csv`
   - `data/homologacao_reabertura.xlsx`
   - `data/homologacao_aprovisionamento.xlsx`
   - `data/homologacao_erro_aprovisionamento.xlsx`

2. **Pasta de saída** (com timestamp):
   - `/Applications/Documentos/Projetos_python/Retornos do gerenciador/`
   - Formato: `homologacao_wpp_YYYYMMDD_HHMMSS.csv`
   - Formato: `homologacao_reabertura_YYYYMMDD_HHMMSS.xlsx`
   - etc.

## 📝 Logs

Os logs são salvos em:
- `logs/processar_completo.log`

## 🔍 Exemplos Práticos

### Exemplo 1: Processamento Completo Diário

```bash
cd /Applications/Documentos/Projetos_python/3F_Qigger_DBGerenciador
source venv/bin/activate
python3 processar_completo.py
```

### Exemplo 2: Apenas Atualizar Banco (sem homologação)

```bash
python3 processar_completo.py --apenas-bases
```

### Exemplo 3: Gerar Homologação com Banco Já Atualizado

```bash
python3 processar_completo.py --apenas-homologacao
```

### Exemplo 4: Processar Tudo Exceto Excel

```bash
python3 processar_completo.py --skip-excel
```

## ⚠️ Requisitos

- Ambiente virtual ativado (`source venv/bin/activate`)
- Dependências instaladas (`pip install -r requirements.txt`)
- Arquivo `config.py` configurado com caminhos corretos

## 🐛 Troubleshooting

### Erro: "No module named 'pandas'"
```bash
source venv/bin/activate
pip install -r requirements.txt
```

### Erro: "Script não encontrado"
Verifique se você está no diretório correto:
```bash
cd /Applications/Documentos/Projetos_python/3F_Qigger_DBGerenciador
```

### Arquivos não encontrados
O script verifica múltiplas pastas automaticamente. Se nenhum arquivo for encontrado, verifique:
- Caminhos configurados em `config.py`
- Permissões de acesso às pastas
- Arquivos existem nas pastas esperadas

## 📞 Ajuda

Para ver todas as opções disponíveis:
```bash
python3 processar_completo.py --help
```
