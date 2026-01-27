# 3F Qigger DB Gerenciador

Sistema de gerenciamento de portabilidade com motor de decisão baseado em regras de negócio.

## Descrição

O **3F Qigger DB Gerenciador** é um sistema completo para processamento e gerenciamento de portabilidade de números telefônicos. O sistema utiliza uma engine de decisão que aplica 23 regras de negócio para processar registros de portabilidade de forma automatizada e inteligente.

## Características Principais

- **23 Regras de Decisão**: Motor de decisão completo com regras de validação, status e motivos
- **Banco de Dados SQLite**: Armazenamento persistente com histórico de decisões
- **Parser CSV**: Importação de arquivos CSV do Siebel
- **Monitoramento de Pasta**: Processamento automático de arquivos CSV usando watchdog
- **Logging Completo**: Sistema de logs para auditoria e debug
- **Testes Unitários**: Cobertura completa de testes para todas as regras
- **WhatsApp (WPP)**: Sistema completo de homologação e geração de mensagens WhatsApp
- **Base Unificada**: Integração com COVERTE BASE PROP.xlsx via SMB

## Estrutura do Projeto

```
3F_Qigger_DBGerenciador/
├── src/                                    # Código fonte principal
│   ├── database/
│   │   ├── db_manager.py                   # Gerenciador de banco de dados
│   │   └── unified_db.py                   # Banco unificado
│   ├── engine/
│   │   ├── qigger_decision_engine.py       # Motor de decisão com 23 regras
│   │   └── trigger_loader.py               # Carregador de triggers
│   ├── models/
│   │   └── portabilidade.py                # Modelos de dados
│   ├── utils/
│   │   ├── csv_parser.py                   # Parser de arquivos CSV
│   │   ├── csv_generator.py                # Gerador de arquivos CSV
│   │   ├── templates_wpp.py                # Mapeamento de templates WPP
│   │   ├── objects_loader.py               # Loader de Relatório de Objetos
│   │   ├── progress_bar.py                 # Barra de progresso
│   │   ├── validar_processamento.py        # Validação de processamento
│   │   └── regua_comunicacao.py            # Régua de comunicação
│   └── monitor/
│       └── folder_monitor.py               # Monitor de pasta com watchdog
├── tests/                                  # Testes unitários
├── data/                                   # Banco de dados e arquivos
├── logs/                                   # Logs do sistema
├── deprecated/                             # Scripts e validadores antigos
│
├── config.py                               # Configurações centralizadas
├── processar_completo.py                   # Processamento completo (principal)
├── processar_excel_unificado.py            # Processar COVERTE BASE PROP
├── gerar_homologacao_wpp.py                # Homologação WhatsApp
├── gerar_homologacao_aprovisionamento.py   # Homologação Aprovisionamento
├── gerar_homologacao_reabertura.py         # Homologação Reabertura
├── gerar_homologacao_erro_aprovisionamento.py
├── gerar_homologacao_consulta.py           # Homologação Consulta
├── processar_atualizacoes_gerar_finais.py  # Processar atualizações
├── processar_bp_fechados.py                # Processar BP_FECHADOS
├── corrigir_id_proposta_isize.py           # Correção de IDs
├── backup_database.py                      # Backup do banco
├── migrate_database.py                     # Migrações
├── otimizar_banco.py                       # Otimização do banco
│
├── triggers.xlsx                           # Regras de decisão
├── requirements.txt                        # Dependências
└── README.md                               # Este arquivo
```

## Instalação

### Pré-requisitos

- Python 3.8 ou superior
- pip (gerenciador de pacotes Python)

### Passos de Instalação

1. **Clone ou baixe o projeto**

2. **Crie um ambiente virtual (recomendado)**
```bash
python3 -m venv venv
```

3. **Ative o ambiente virtual**
```bash
# Mac/Linux
source venv/bin/activate

# Windows
venv\Scripts\activate
```

4. **Instale as dependências**
```bash
pip install -r requirements.txt
```

## Uso

### Processamento Completo (Recomendado)

Processa todas as bases e gera todos os arquivos de homologação:

```bash
python3 processar_completo.py
```

Opções disponíveis:
```bash
python3 processar_completo.py --apenas-bases      # Apenas processa bases
python3 processar_completo.py --apenas-homologacao # Apenas gera homologação
python3 processar_completo.py --skip-excel        # Pula processamento do Excel
python3 processar_completo.py --skip-csv          # Pula processamento de CSV
python3 processar_completo.py --no-smb            # Desativa conexão SMB automática
```

### Processar Excel COVERTE BASE PROP

```bash
python3 processar_excel_unificado.py
python3 processar_excel_unificado.py --smb        # Conectar via SMB automaticamente
python3 processar_excel_unificado.py --copiar-local # Copiar arquivo para local
```

### Gerar Arquivos de Homologação

```bash
# WhatsApp
python3 gerar_homologacao_wpp.py

# Aprovisionamento (entregues)
python3 gerar_homologacao_aprovisionamento.py

# Reabertura (cancelados)
python3 gerar_homologacao_reabertura.py

# Erro no Aprovisionamento
python3 gerar_homologacao_erro_aprovisionamento.py

# Consulta
python3 gerar_homologacao_consulta.py
```

### Processar Atualizações

```bash
python3 processar_atualizacoes_gerar_finais.py
```

### Processar BP_FECHADOS

```bash
python3 processar_bp_fechados.py
```

### Corrigir IDs (utilitário)

```bash
python3 corrigir_id_proposta_isize.py --dry-run   # Simulação
python3 corrigir_id_proposta_isize.py             # Correção real
```

## Banco de Dados

O sistema utiliza SQLite como banco de dados. As principais tabelas são:

- **base_coverte_prop**: Dados do Excel COVERTE BASE PROP (fonte principal)
- **portabilidade_records**: Registros de portabilidade processados
- **relatorio_objetos**: Dados de logística do Relatório de Objetos
- **portabilidade_processamento**: Validação cruzada de registros
- **base_unificada**: Base unificada consolidada

## Regras de Decisão (23 Regras)

### Regras de Validação
1. Validar formato e consistência do CPF
2. Validar número de acesso (mínimo 11 caracteres)
3. Validar campos obrigatórios
22. Validar consistência de datas

### Regras de Status
4-12, 18-21: Regras para diferentes status de portabilidade

### Regras de Motivos
13-17: Regras para motivos específicos (rejeição SMS, CPF inválido, etc.)

### Regras Especiais
23. Priorizar último bilhete de portabilidade

## WhatsApp (WPP)

O sistema inclui funcionalidades completas para geração de mensagens WhatsApp:

### Templates Disponíveis
1. **Template 1** - Confirmação de Portabilidade
2. **Template 2** - Pendência SMS Portabilidade
3. **Template 3** - Confirmação de Endereço
4. **Template 4** - Outros casos

### Funcionalidades
- Geração de arquivo de homologação CSV
- Mapeamento automático de templates
- Enriquecimento de dados com Base Analítica
- Normalização de telefones e CEPs
- Geração automática de links de rastreio
- Verificação de histórico de envios via Google Sheets

## Testes

```bash
# Executar todos os testes
pytest

# Executar com cobertura
pytest --cov=src tests/

# Executar testes específicos
pytest tests/test_qigger_decision_engine.py
pytest tests/test_homologacao_wpp.py
```

## Logs

Os logs são salvos em `logs/`:
- `qigger.log` - Logs principais
- `homologacao_wpp.log` - Logs de homologação WPP
- `processar_completo.log` - Logs do processamento completo
- `processar_excel_unificado.log` - Logs do processamento Excel

## Configuração

As configurações estão centralizadas em `config.py`:

- `DB_PATH` - Caminho do banco de dados
- `PASTA_IMPORTACOES` - Pasta de importações
- `PASTA_BASE_COVERTE_NETWORK` - Caminho de rede do COVERTE BASE PROP
- `PASTA_SAIDA_HOMOLOGACAO` - Pasta de saída para arquivos de homologação

## Licença

Este projeto é proprietário da 3F.

---

**Versão**: 2.1.0  
**Última atualização**: Janeiro 2026
