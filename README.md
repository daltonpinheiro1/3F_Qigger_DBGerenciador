# 3F Qigger DB Gerenciador

Sistema de gerenciamento de portabilidade com motor de decisão baseado em regras de negócio.

## 📋 Descrição

O **3F Qigger DB Gerenciador** é um sistema completo para processamento e gerenciamento de portabilidade de números telefônicos. O sistema utiliza uma engine de decisão que aplica regras de negócio para processar registros de portabilidade de forma automatizada e inteligente.

## 🚀 Características

- **Motor de Decisão**: Engine de decisão com regras de validação, status e motivos
- **Banco de Dados SQLite**: Armazenamento persistente em `data/portabilidade.db`
- **Conexão SMB**: Conexão automática com rede para carregar COVERTE BASE PROP.xlsx
- **Parser CSV/Excel**: Importação de arquivos CSV e Excel
- **Geração de Arquivos**: WPP, Reabertura, Aprovisionamento, Erro Aprovisionamento
- **Saída Organizada**: Arquivos gerados em `/Applications/Documentos/Projetos_python/Retornos do gerenciador`
- **Google Sheets**: Integração com histórico de envios WhatsApp

## 📁 Estrutura do Projeto

```
3F_Qigger_DBGerenciador/
├── config.py                          # Configurações centralizadas
├── processar_completo.py              # Script principal de processamento
├── processar_excel_unificado.py       # Processador do COVERTE BASE PROP
├── processar_atualizacoes_gerar_finais.py  # Processador de CSVs de atualização
│
├── gerar_homologacao_wpp.py           # Gerador WPP (WhatsApp)
├── gerar_homologacao_reabertura.py    # Gerador Reabertura
├── gerar_homologacao_aprovisionamento.py   # Gerador Aprovisionamento
├── gerar_homologacao_aprovisionamentos.py  # Gerador Aprovisionamentos (CSV)
├── gerar_homologacao_erro_aprovisionamento.py  # Gerador Erro Aprovisionamento
├── gerar_homologacao_consulta.py      # Gerador de Consultas
├── gerar_todos_arquivos_homologacao.py # Gera todos os arquivos
│
├── src/                               # Código fonte principal
│   ├── database/
│   │   ├── db_manager.py              # Gerenciador do banco principal
│   │   └── unified_db.py              # Banco unificado
│   ├── engine/
│   │   ├── qigger_decision_engine.py  # Motor de decisão
│   │   └── trigger_loader.py          # Carregador de triggers
│   ├── models/
│   │   └── portabilidade.py           # Modelos de dados
│   ├── monitor/
│   │   └── folder_monitor.py          # Monitor de pasta
│   └── utils/
│       ├── csv_parser.py              # Parser CSV
│       ├── csv_generator.py           # Gerador CSV
│       ├── objects_loader.py          # Loader de objetos
│       ├── templates_wpp.py           # Templates WPP
│       ├── db_fallback.py             # Busca com fallback
│       ├── regua_comunicacao.py       # Régua de comunicação
│       └── regua_comunicacao_dinamica.py  # Régua dinâmica
│
├── data/                              # Dados e banco SQLite
│   └── portabilidade.db               # Banco de dados principal
├── logs/                              # Logs de execução
├── backups/                           # Backups do banco
├── tests/                             # Testes unitários
├── docs/                              # Documentação
├── deprecated/                        # Arquivos antigos (backup)
│
├── backup_database.py                 # Script de backup
├── migrate_database.py                # Script de migração
├── otimizar_banco.py                  # Otimizador do banco
│
├── triggers.xlsx                      # Regras de decisão
├── requirements.txt                   # Dependências Python
└── README.md                          # Este arquivo
```

## 🛠️ Instalação

### Pré-requisitos

- Python 3.8 ou superior
- macOS (testado em Ventura+)

### Passos de Instalação

1. **Clone ou baixe o projeto**

2. **Crie um ambiente virtual**
```bash
python3 -m venv venv
```

3. **Ative o ambiente virtual**
```bash
source venv/bin/activate
```

4. **Instale as dependências**
```bash
pip install -r requirements.txt
```

## 📖 Uso

### Processamento Completo

```bash
# Processa tudo: bases + arquivos de homologação
python3 processar_completo.py

# Apenas processa bases
python3 processar_completo.py --apenas-bases

# Apenas gera arquivos de homologação
python3 processar_completo.py --apenas-homologacao

# Com conexão SMB automática
python3 processar_completo.py --smb
```

### Gerar Arquivos Individuais

```bash
# WhatsApp
python3 gerar_homologacao_wpp.py

# Reabertura
python3 gerar_homologacao_reabertura.py

# Aprovisionamento
python3 gerar_homologacao_aprovisionamento.py

# Erro no Aprovisionamento
python3 gerar_homologacao_erro_aprovisionamento.py

# Todos de uma vez
python3 gerar_todos_arquivos_homologacao.py
```

### Processar Excel COVERTE BASE PROP

```bash
# Com conexão SMB (rede)
python3 processar_excel_unificado.py --smb

# Arquivo local
python3 processar_excel_unificado.py /caminho/para/arquivo.xlsx
```

## 📂 Saída dos Arquivos

Os arquivos de homologação são salvos em:
```
/Applications/Documentos/Projetos_python/Retornos do gerenciador/
├── homologacao_wpp.csv
├── homologacao_wpp.xlsx
├── homologacao_reabertura.xlsx
├── homologacao_aprovisionamento.xlsx
├── homologacao_aprovisionamentos.csv
├── homologacao_erro_aprovisionamento.xlsx
└── homologacao_consulta.xlsx
```

## 🗄️ Banco de Dados

O sistema utiliza SQLite com as seguintes tabelas principais:

- **portabilidade_records**: Registros de portabilidade
- **base_coverte_prop**: Dados do COVERTE BASE PROP
- **relatorio_objetos**: Relatório de objetos (logística)
- **base_unificada**: Base unificada de dados
- **triggers_rules**: Regras de triggers
- **templates_wpp**: Templates de mensagens WhatsApp

### Localização
```
/Applications/Documentos/Projetos_python/3F_Qigger_DBGerenciador/data/portabilidade.db
```

## 📝 Configuração

O arquivo `config.py` centraliza todas as configurações:

- Caminhos de entrada/saída
- Configurações de conexão SMB
- Formatos de data suportados
- Configurações de logging

## 🧪 Testes

```bash
# Executar todos os testes
pytest

# Com cobertura
pytest --cov=src tests/

# Testes específicos
pytest tests/test_homologacao_wpp.py -v
```

## 📋 Workflow

1. **Carregar Base**: `processar_excel_unificado.py` carrega COVERTE BASE PROP
2. **Processar CSVs**: `processar_atualizacoes_gerar_finais.py` processa atualizações
3. **Gerar Arquivos**: Scripts `gerar_homologacao_*.py` geram arquivos finais

Ou simplesmente:
```bash
python3 processar_completo.py --smb
```

## 📄 Licença

Este projeto é proprietário da 3F Contact Center.

---

**Versão**: 3.0.0  
**Última atualização**: Janeiro 2026
