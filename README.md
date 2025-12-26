# 3F Qigger DB Gerenciador

Sistema de gerenciamento de portabilidade com motor de decisão baseado em regras de negócio.

## 📋 Descrição

O **3F Qigger DB Gerenciador** é um sistema completo para processamento e gerenciamento de portabilidade de números telefônicos. O sistema utiliza uma engine de decisão que aplica 23 regras de negócio para processar registros de portabilidade de forma automatizada e inteligente.

## 🚀 Características

- **23 Regras de Decisão**: Motor de decisão completo com regras de validação, status e motivos
- **Banco de Dados SQLite**: Armazenamento persistente com histórico de decisões
- **Parser CSV**: Importação de arquivos CSV do Siebel
- **Monitoramento de Pasta**: Processamento automático de arquivos CSV usando watchdog
- **Logging Completo**: Sistema de logs para auditoria e debug
- **Testes Unitários**: Cobertura completa de testes para todas as regras
- **📱 WhatsApp (WPP)**: Sistema completo de homologação e geração de mensagens WhatsApp
  - Geração de arquivos de homologação WPP
  - Mapeamento automático de templates (1, 2, 3, 4)
  - Enriquecimento de dados com Base Analítica e Relatório de Objetos
  - Normalização de telefones (prioridade: Telefone Portabilidade > DDD+Telefone)
  - Normalização de CEPs e datas
  - Geração automática de links de rastreio
  - Sempre usa o nu_pedido mais recente quando há múltiplos pedidos

## 📁 Estrutura do Projeto

```
3F_Qigger_DBGerenciador/
├── src/
│   ├── __init__.py
│   ├── engine/
│   │   ├── __init__.py
│   │   └── qigger_decision_engine.py    # Motor de decisão com 23 regras
│   ├── models/
│   │   ├── __init__.py
│   │   └── portabilidade.py              # Modelos de dados
│   ├── database/
│   │   ├── __init__.py
│   │   └── db_manager.py                  # Gerenciador de banco de dados
│   ├── utils/
│   │   ├── __init__.py
│   │   ├── csv_parser.py                   # Parser de arquivos CSV
│   │   ├── templates_wpp.py                # Mapeamento de templates WPP
│   │   ├── wpp_output_generator.py         # Gerador de arquivos WPP
│   │   ├── objects_loader.py               # Loader de Relatório de Objetos
│   │   └── regua_comunicacao.py            # Régua de comunicação
│   └── monitor/
│       ├── __init__.py
│       └── folder_monitor.py                # Monitor de pasta com watchdog
├── tests/
│   ├── __init__.py
│   ├── test_qigger_decision_engine.py      # Testes das 23 regras
│   ├── test_csv_parser.py                  # Testes do parser
│   ├── test_database.py                    # Testes do banco de dados
│   ├── test_folder_monitor.py              # Testes do monitor de pasta
│   ├── test_homologacao_wpp.py             # Testes de homologação WPP
│   ├── test_homologacao_aprovisionadas.py  # Testes de aprovisionamento
│   └── test_homologacao_reabertura.py      # Testes de reabertura
├── scripts/                                 # Scripts .bat organizados
├── docs/                                    # Documentação
│   ├── REVISAO_PROJETO_HOMOLOGACAO.md      # Revisão completa
│   └── RESUMO_HOMOLOGACAO_AJUSTES.md       # Resumo de ajustes
├── gerar_homologacao_wpp.py                # Gerador de homologação WPP
├── validar_homologacao.py                   # Validador de homologação
└── triggers.xlsx                           # Regras de decisão
├── data/                                    # Diretório do banco de dados
├── logs/                                    # Diretório de logs
├── main.py                                  # Arquivo principal
├── requirements.txt                         # Dependências
└── README.md                                # Este arquivo
```

## 🛠️ Instalação

### Pré-requisitos

- Python 3.8 ou superior
- pip (gerenciador de pacotes Python)

### Passos de Instalação

1. **Clone ou baixe o projeto**

2. **Crie um ambiente virtual (recomendado)**
```bash
python -m venv venv
```

3. **Ative o ambiente virtual**

   **Windows:**
   ```bash
   venv\Scripts\activate
   ```

   **Linux/Mac:**
   ```bash
   source venv/bin/activate
   ```

4. **Instale as dependências**
```bash
pip install -r requirements.txt
```

## 📖 Uso

### Processar Arquivo CSV

```bash
python main.py --csv caminho/para/arquivo.csv
```

### Monitorar Pasta (Processamento Automático)

```bash
python main.py --watch caminho/para/pasta
```

Com opções adicionais:

```bash
# Com pasta para arquivos processados
python main.py --watch pasta/entrada --processed-folder pasta/processados

# Com pasta para arquivos com erro
python main.py --watch pasta/entrada --error-folder pasta/erros

# Sem monitoramento recursivo de subpastas
python main.py --watch pasta/entrada --no-recursive
```

### Executar Exemplo

```bash
python main.py --example
```

### Listar Todas as Regras

```bash
python main.py --list-rules
```

### Gerar Arquivo de Homologação WPP

```bash
python gerar_homologacao_wpp.py
```

Este script gera um arquivo CSV completo de homologação para WhatsApp com:
- Dados do cliente (CPF, Nome, Telefone, Endereço completo)
- Template mapeado automaticamente
- Variáveis do template preenchidas
- Preview da mensagem
- Link de rastreio formatado
- Status de disparo (sempre FALSE em homologação)

O arquivo será salvo em `data/homologacao_wpp.csv`.

### Validar Arquivo de Homologação

```bash
python validar_homologacao.py
```

Valida o arquivo de homologação gerado, verificando:
- Ordem das colunas
- Normalização de telefones (11 dígitos)
- Normalização de CEPs (8 dígitos)
- Formato de datas (DD/MM/AAAA)
- Status de disparo (sempre FALSE)
- Links de rastreio válidos

### Uso Programático

```python
from src.engine import QiggerDecisionEngine
from src.database import DatabaseManager
from src.models.portabilidade import PortabilidadeRecord, PortabilidadeStatus

# Criar engine e banco de dados
db_manager = DatabaseManager("data/portabilidade.db")
engine = QiggerDecisionEngine(db_manager)

# Criar registro
record = PortabilidadeRecord(
    cpf="12345678901",
    numero_acesso="11987654321",
    numero_ordem="1-1234567890123",
    codigo_externo="250001234",
    status_bilhete=PortabilidadeStatus.CANCELADA
)

# Processar registro
results = engine.process_record(record)

# Exibir resultados
for result in results:
    print(f"{result.rule_name}: {result.decision}")
    print(f"  Ação: {result.action}")
    print(f"  Detalhes: {result.details}")
```

## 📋 Regras de Decisão (23 Regras)

### Regras de Validação
1. **Rule 1**: Validar formato e consistência do CPF
2. **Rule 2**: Validar número de acesso (mínimo 11 caracteres)
3. **Rule 3**: Validar campos obrigatórios
22. **Rule 22**: Validar consistência de datas

### Regras de Status
4. **Rule 4**: Cliente sem cadastro no sistema
5. **Rule 5**: Portabilidade cancelada
6. **Rule 6**: Portabilidade pendente
7. **Rule 7**: Portabilidade concluída com sucesso
8. **Rule 8**: Conflito detectado na portabilidade
9. **Rule 9**: Falha parcial na portabilidade
10. **Rule 10**: Erro no aprovisionamento
11. **Rule 11**: Erro do sistema
12. **Rule 12**: Nenhum bilhete de portabilidade encontrado
18. **Rule 18**: Portabilidade suspensa
19. **Rule 19**: Ordem concluída
20. **Rule 20**: Ordem pendente
21. **Rule 21**: Em aprovisionamento

### Regras de Motivos
13. **Rule 13**: Rejeição do cliente via SMS
14. **Rule 14**: Cancelamento automático pela BDR
15. **Rule 15**: CPF inválido
16. **Rule 16**: Portabilidade de número vago
17. **Rule 17**: Sem resposta do SMS do cliente

### Regras Especiais
23. **Rule 23**: Priorizar último bilhete de portabilidade

## 🧪 Testes

Execute todos os testes:

```bash
pytest
```

Execute com cobertura:

```bash
pytest --cov=src tests/
```

Execute testes específicos:

```bash
pytest tests/test_qigger_decision_engine.py
pytest tests/test_csv_parser.py
pytest tests/test_database.py
pytest tests/test_homologacao_wpp.py
pytest tests/test_homologacao_aprovisionadas.py
pytest tests/test_homologacao_reabertura.py
```

### Testes de Homologação

Execute todos os testes de homologação:

```bash
pytest tests/test_homologacao_*.py -v
```

Veja o guia completo em `tests/README_HOMOLOGACAO.md`.

## 🗄️ Banco de Dados

O sistema utiliza SQLite como banco de dados padrão. O banco é criado automaticamente no diretório `data/` na primeira execução.

### Tabelas

- **portabilidade_records**: Armazena os registros de portabilidade
- **decision_history**: Histórico de decisões tomadas pela engine
- **rules_log**: Log de execução de cada regra

## 📝 Logs

Os logs são salvos em:
- `logs/qigger.log` - Logs principais do sistema
- `logs/homologacao_wpp.log` - Logs de geração de homologação WPP
- `logs/regua_comunicacao.log` - Logs da régua de comunicação
- `logs/regua_dinamica.log` - Logs da régua dinâmica

Todos os logs também são exibidos no console.

## 🔒 Segurança

- Validação rigorosa de dados de entrada
- Sanitização de dados antes de inserção no banco
- Logs de auditoria para todas as decisões

## 🤝 Contribuindo

1. Faça um fork do projeto
2. Crie uma branch para sua feature (`git checkout -b feature/AmazingFeature`)
3. Commit suas mudanças (`git commit -m 'Add some AmazingFeature'`)
4. Push para a branch (`git push origin feature/AmazingFeature`)
5. Abra um Pull Request

## 📄 Licença

Este projeto é proprietário da 3F.

## 👥 Autores

- **3F Team** - Desenvolvimento inicial

## 📞 Suporte

Para suporte, entre em contato com a equipe de desenvolvimento.

---

## 📱 WhatsApp (WPP) - Régua de Comunicação

O sistema inclui funcionalidades completas para geração de mensagens WhatsApp:

### Funcionalidades

- **Geração de Homologação**: Arquivo CSV completo para validação antes do envio
- **Mapeamento de Templates**: Mapeamento automático de templates baseado em regras
- **Enriquecimento de Dados**: Preenchimento automático de dados do cliente
- **Normalização**: Telefones, CEPs e datas normalizados automaticamente
- **Links de Rastreio**: Geração automática de links formatados

### Templates Disponíveis

1. **Template 1** - Confirmação de Portabilidade
2. **Template 2** - Pendência SMS Portabilidade
3. **Template 3** - Confirmação de Endereço
4. **Template 4** - Outros casos

### Prioridade de Telefone

1. **Telefone Portabilidade** (se disponível)
2. **DDD + Telefone** normalizado (se Telefone Portabilidade vazio)

### Nu Pedido

O sistema sempre usa o **nu_pedido mais recente** quando há múltiplos pedidos para o mesmo código externo, baseado na data de inserção.

### Formato do Arquivo de Homologação

O arquivo gerado segue a ordem imutável de colunas:
- Proposta_iSize, Cpf, NomeCliente, Telefone_Contato
- Endereco, Numero, Complemento, Bairro, Cidade, UF, Cep, Ponto_Referencia
- Cod_Rastreio, Data_Venda, Tipo_Comunicacao
- Status_Disparo (sempre FALSE), DataHora_Disparo (sempre vazio)
- Template_Triggers, O_Que_Aconteceu, Acao_Realizar (apenas homologação)

### Enriquecimento de Dados

O sistema enriquece automaticamente os dados usando:
- **Relatório de Objetos**: Dados de logística e entrega
- **Base Analítica Final**: Dados completos do cliente (endereço, telefone, etc.)

**Versão**: 2.0.0  
**Última atualização**: Dezembro 2025

