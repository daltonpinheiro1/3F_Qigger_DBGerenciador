# Sistema de Banco de Dados Unificado - TIM

## Visão Geral

O Sistema de Banco de Dados Unificado foi criado para consolidar dados de múltiplas fontes em um único repositório com versionamento completo, permitindo acompanhamento ponto a ponto de todas as etapas do processo de portabilidade.

## Características Principais

### 1. Versionamento Completo
- Cada atualização cria uma nova linha mantendo o histórico completo
- O campo `id_isize` permanece constante através de todas as versões
- O campo `is_latest` indica a versão mais recente
- Histórico completo disponível para auditoria e análise

### 2. Unificação de Múltiplas Fontes
- **Base Analítica Final**: Dados de vendas e clientes
- **Relatório de Objetos**: Dados de logística e entrega
- **Gerenciador/Siebel**: Dados de portabilidade e bilhetes

### 3. Foco em Campos Críticos
- **Número da Ordem**: Chave principal de rastreamento
- **Status da Ordem**: Acompanhamento do status atual
- **Status Logística**: Status de entrega e logística
- **Status do Bilhete**: Status da portabilidade
- **Motivos de Cancelamentos e Recusas**: Rastreamento de problemas

## Estrutura do Banco

### Tabela Principal: `tim_unificado`

#### Campos de Identificação
- `id_isize`: ID único do iSize (mantém o mesmo para todas as versões)
- `registro_id`: ID interno (auto-incremento)
- `versao`: Número da versão (incrementa a cada atualização)
- `numero_ordem`: Número da ordem (chave principal de rastreamento)
- `codigo_externo`: Código externo
- `proposta_isize`: ID da proposta iSize

#### Campos de Versionamento
- `data_armazenamento`: Data/hora de armazenamento desta versão
- `origem_dados`: Origem dos dados ('base_analitica', 'relatorio_objetos', 'gerenciador')
- `hash_dados`: Hash para detectar mudanças
- `is_latest`: Flag indicando se é a versão mais recente (1) ou histórico (0)

#### Campos de Dados do Cliente
- `cpf`, `cliente_nome`, `cliente_telefone`, `telefone_portado`
- `endereco`, `numero`, `complemento`, `bairro`, `cidade`, `uf`, `cep`, `ponto_referencia`

#### Campos de Status (FOCO)
- `status_ordem`: Status atual da ordem
- `status_ordem_anterior`: Status anterior (para comparação)
- `status_logistica`: Status atual da logística
- `status_logistica_anterior`: Status anterior
- `status_bilhete`: Status atual do bilhete
- `status_bilhete_anterior`: Status anterior

#### Campos de Motivos (FOCO)
- `motivo_recusa`: Motivo de recusa
- `motivo_cancelamento`: Motivo de cancelamento
- `motivo_nao_consultado`: Motivo de não consultado
- `motivo_nao_cancelado`, `motivo_nao_aberto`, `motivo_nao_reagendado`

#### Campos Adicionais
- Logística detalhada (rastreio, transportadora, datas)
- Dados de portabilidade (datas, reagendamentos)
- Dados de BP (Bonus Portabilidade)
- Triggers e regras aplicadas

### Tabelas Auxiliares

#### `tim_unificado_changes`
Registra todas as mudanças detectadas entre versões:
- `id_isize`, `versao`
- `campo_alterado`
- `valor_anterior`, `valor_novo`
- `data_mudanca`, `origem_mudanca`

#### `tim_unificado_sync`
Rastreamento de sincronizações:
- `fonte_dados`
- `data_sincronizacao`
- `registros_processados`, `registros_atualizados`, `registros_novos`
- `status`, `observacoes`

## Uso

### Sincronização de Dados

```bash
python sincronizar_dados_unificados.py
```

Este script:
1. Carrega dados da Base Analítica Final
2. Carrega dados do Relatório de Objetos (mais recente)
3. Carrega registros do banco de portabilidade existente
4. Unifica todos no banco unificado com versionamento

### Consultas

```bash
# Consultar por ID iSize
python consultar_banco_unificado.py --id 250003874

# Consultar por status
python consultar_banco_unificado.py --status-ordem "Concluído" --limit 20

# Estatísticas
python consultar_banco_unificado.py --stats
```

### Uso Programático

```python
from src.database.unified_db import UnifiedDatabaseManager
from src.utils.data_unifier import DataUnifier

# Inicializar
db = UnifiedDatabaseManager("data/tim_unificado.db")
unifier = DataUnifier(db)

# Buscar versão mais recente
record = db.get_latest_record("250003874")

# Buscar histórico completo
history = db.get_record_history("250003874")

# Buscar por status
records = db.get_records_by_status(
    status_ordem="Concluído",
    status_logistica="Entregue"
)
```

## Versionamento

### Como Funciona

1. **Primeira Inserção**: Cria versão 1 com `is_latest=1`
2. **Atualização com Mudanças**: 
   - Marca versão anterior como `is_latest=0`
   - Cria nova versão com `versao` incrementado
   - Calcula hash dos campos críticos
   - Se hash mudou, cria nova versão
3. **Atualização sem Mudanças**: Mantém versão existente (não cria nova)

### Detecção de Mudanças

O sistema detecta mudanças nos campos críticos:
- `status_ordem`
- `status_logistica`
- `status_bilhete`
- `motivo_recusa`
- `motivo_cancelamento`
- `data_portabilidade`
- `data_entrega`
- `data_logistica`

### Campos Preservados

Quando uma nova versão é criada, campos não alterados da versão anterior são copiados automaticamente, garantindo continuidade dos dados.

## Performance

O banco utiliza otimizações avançadas:
- **WAL Mode**: Write-Ahead Logging para concorrência
- **Índices Otimizados**: Para buscas rápidas por ID, status, datas
- **Cache de 128MB**: Para queries frequentes
- **MMAP de 512MB**: Para leitura eficiente

## Manutenção

### Backup
O banco está localizado em `data/tim_unificado.db`. Faça backups regulares deste arquivo.

### Limpeza
O sistema mantém histórico completo. Se necessário limpar versões antigas, use:

```sql
-- CUIDADO: Isso remove histórico
DELETE FROM tim_unificado 
WHERE is_latest = 0 
  AND data_armazenamento < date('now', '-90 days');
```

## Integração com Sistema Existente

O banco unificado é complementar ao banco de portabilidade existente (`portabilidade.db`):
- Não substitui o banco existente
- Funciona em paralelo
- Permite migração gradual
- Facilita análise e relatórios unificados

