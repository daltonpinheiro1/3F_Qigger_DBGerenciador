# 3F Qigger DB Gerenciador

Sistema de gerenciamento de portabilidade com motor de decisão baseado em regras de negócio e pipeline V2 com banco normalizado.

## Descrição

O **3F Qigger DB Gerenciador** processa registros de portabilidade de números telefônicos de forma automatizada. Utiliza uma engine de decisão com 23 regras de negócio, banco de dados V2 normalizado (INSERT-only com versionamento), e pipeline completo de processamento com 6 etapas.

## Arquitetura

O sistema opera com dois bancos em paralelo (dual-write):
- **Banco Legado** (`portabilidade.db`) — mantido para plugins externos
- **Banco V2** (`portabilidade_v2.db`) — normalizado, versionado, fonte primária dos geradores

## Estrutura do Projeto

```
3F_Qigger_DBGerenciador/
├── src/                                    # Código fonte principal
│   ├── database/
│   │   ├── db_manager.py                   # Gerenciador banco legado
│   │   ├── db_manager_v2.py                # Gerenciador banco V2
│   │   ├── queries_v2.py                   # Queries otimizadas para V2
│   │   ├── data_unifier.py                 # Cache materializado (V2)
│   │   ├── importador.py                   # Importador de arquivos para V2
│   │   └── schema.py                       # Schema do banco V2
│   ├── engine/
│   │   ├── qigger_decision_engine.py       # Motor de decisão (23 regras)
│   │   └── trigger_loader.py               # Carregador de triggers
│   ├── models/
│   │   └── portabilidade.py                # Modelos de dados
│   ├── reprocessamento/
│   │   ├── reprocessador.py                # Correção de endereços inválidos
│   │   ├── proxy_manager.py                # Pool de proxies dinâmico
│   │   ├── address_corrector.py            # APIs de CEP e geocodificação
│   │   └── queries_reprocessamento.py      # Query de registros pendentes
│   ├── utils/
│   │   ├── csv_parser.py                   # Parser de arquivos CSV
│   │   ├── csv_generator.py                # Gerador de arquivos CSV
│   │   ├── templates_wpp.py                # Templates WhatsApp
│   │   ├── objects_loader.py               # Loader de Relatório de Objetos
│   │   ├── data_integrity.py               # Sanitização e validação
│   │   ├── progress_bar.py                 # Barra de progresso
│   │   └── validar_processamento.py        # Validação de processamento
│   └── monitor/
│       └── folder_monitor.py               # Monitor de pasta (watchdog)
│
├── processar_completo.py                   # Pipeline principal (6 etapas)
├── gerar_homologacao_wpp.py                # Homologação WhatsApp
├── gerar_homologacao_reabertura.py         # Homologação Reabertura
├── gerar_homologacao_aprovisionamento.py   # Homologação Aprovisionamento
├── gerar_homologacao_erro_aprovisionamento.py  # Homologação Erro Aprovisionamento
├── gerar_homologacao_entrega_baixa.py      # Homologação Entrega/Baixa
├── gerar_homologacao_consulta.py           # Homologação Consulta (V2 only)
├── processar_atualizacoes_gerar_finais.py  # Processar CSVs de portabilidade
├── processar_excel_unificado.py            # Processar COVERTE BASE PROP
├── processar_bs_venda_du.py                # Processar BS_VENDA_DU
├── processar_tim_pre_controle.py           # Processar TIM PRE CONTROLE
├── processar_relatorio_faturamento.py      # Processar Relatório Faturamento
├── processar_estornos.py                   # Processar Estornos
├── processar_bp_fechados.py                # Processar BP_FECHADOS
├── backup_database.py                      # Backup e replicação SMB
├── revisar_tabelas_db.py                   # Revisão de integridade
├── validar_fluxo_completo.py               # Validação DBA/MIS
├── config.py                               # Configurações centralizadas
├── run.py                                  # Runner unificado
├── run_processar_completo.sh               # Shell script com venv
├── triggers.xlsx                           # Regras de decisão
└── requirements.txt                        # Dependências
```

## Pipeline de Processamento (6 Etapas)

```
ETAPA 1  → BS_VENDA_DU (coleta e atualização)
ETAPA 2  → Arquivos da pasta de entrada (CSV, Excel, Objetos)
           Detecção automática por cabeçalho → legado + V2
ETAPA 3b → Atualizar Cache Unificada (V2)
ETAPA 4  → Geração de Homologação (6 relatórios via V2)
ETAPA 5  → Reprocessamento de endereços inválidos
ETAPA 6  → Backup e replicação SMB
```

## Uso

### Processamento Completo

```bash
python3 processar_completo.py --workers 4 --cafinate
```

### Opções disponíveis

```bash
--workers N              # Workers paralelos para homologação (recomendado: 4)
--cafinate               # macOS: impede o sistema de dormir
--apenas-bases           # Apenas processa bases (sem homologação)
--apenas-homologacao     # Apenas gera homologação (com cache atualizada)
--apenas-reprocessamento # Apenas reprocessamento de endereços
--skip-excel             # Pula processamento de Excel
--skip-csv               # Pula processamento de CSV
--skip-objetos           # Pula Relatório de Objetos
--skip-reprocessamento   # Pula reprocessamento de endereços
--forcar-legado          # Forçar geração via banco legado
--forcar-v2              # Forçar geração via banco V2
--no-smb                 # Desativar conexão SMB
```

### Runner unificado

```bash
python run.py completo --workers 4
python run.py validar --homologacao
python run.py revisar
python run.py backup
```

## Relatórios Gerados

| Arquivo | Filtro | Descrição |
|---|---|---|
| homologacao_wpp.xlsx | 90 dias | Régua de comunicação WhatsApp |
| homologacao_reabertura.xlsx | 180 dias | Portabilidade cancelada |
| homologacao_aprovisionamento.xlsx | 90 dias | Em aprovisionamento |
| homologacao_erro_aprovisionamento.xlsx | 90 dias | Erro no aprovisionamento |
| homologacao_entrega_baixa.xlsx | 90 dias | Entregas canceladas/baixa |
| homologacao_consulta.xlsx | 180 dias | Vendas com entrega confirmada |
| *_pronto_tratamento.xlsx | 180 dias | Endereços corrigidos |

## Instalação

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Testes

```bash
pytest
pytest --cov=src tests/
```

## Configuração

Configurações centralizadas em `config.py`. Credenciais SMB em `.env`.

## Licença

Proprietário — 3F.

**Versão**: 3.0.0
**Última atualização**: Abril 2026
