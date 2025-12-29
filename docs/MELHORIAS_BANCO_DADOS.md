# Melhorias do Banco de Dados - Versionamento e DBA

## 📋 Visão Geral

O banco de dados `portabilidade.db` foi atualizado com versionamento completo e melhorias de DBA para garantir histórico preservado, performance otimizada e manutenção facilitada.

## 🔄 Versionamento

### Tabela `relatorio_objetos`

A tabela `relatorio_objetos` agora possui versionamento completo:

- **`registro_id_base`**: ID único do registro (nu_pedido + codigo_externo)
- **`versao`**: Número da versão (incrementa a cada mudança)
- **Histórico preservado**: Cada mudança cria uma nova versão, não sobrescreve

### Como Funciona

1. **Primeira inserção**: Cria versão 1
2. **Sem mudanças**: Apenas atualiza `updated_at` (não cria nova versão)
3. **Com mudanças**: Cria nova versão preservando histórico anterior

### Campos Monitorados para Mudanças

- `id_erp` (número da ordem)
- `rastreio`
- `iccid`
- `status`
- `data_entrega`
- `ultima_ocorrencia`
- `local_ultima_ocorrencia`
- `cidade_ultima_ocorrencia`
- `estado_ultima_ocorrencia`

## 📊 Métodos de Sincronização

### `sync_relatorio_objetos(objects_loader)`

Sincroniza dados do ObjectsLoader para o banco com versionamento inteligente.

**Retorna:**
```python
{
    'processados': int,      # Total processado
    'inseridos': int,        # Novos registros (versão 1)
    'novas_versoes': int,    # Novas versões criadas
    'sem_mudancas': int,     # Sem mudanças (apenas updated_at)
    'erros': int            # Erros durante processamento
}
```

## 🔍 Métodos de Busca

Todos os métodos de busca retornam a **versão mais recente** automaticamente:

- `get_relatorio_objeto_by_codigo(codigo_externo)`
- `get_relatorio_objeto_by_cpf(cpf)`
- `get_relatorio_objeto_by_id_erp(id_erp)`
- `get_relatorio_objeto_best_match(codigo_externo, id_erp, cpf)`

## 🛠️ Métodos de Manutenção

### `get_database_size()`

Retorna informações sobre o tamanho do banco:

```python
{
    'file_size_mb': float,
    'file_exists': bool,
    'tables': {table_name: row_count},
    'total_rows': int
}
```

### `cleanup_old_versions(days_to_keep=90, keep_min_versions=5)`

Remove versões antigas mantendo apenas as N mais recentes:

- **`days_to_keep`**: Dias para manter versões (padrão: 90)
- **`keep_min_versions`**: Mínimo de versões a manter (padrão: 5)

**Retorna:**
```python
{
    'removidos': int,
    'registros_afetados': int
}
```

### `validate_database_integrity()`

Valida integridade do banco de dados:

**Retorna:**
```python
{
    'integrity_check': 'OK' | 'ERROR',
    'foreign_keys': 'OK' | 'ERROR',
    'orphaned_records': {
        'decision_history': int,
        'rules_log': int
    },
    'errors': [str]
}
```

### `rebuild_indexes()`

Reconstrói todos os índices do banco para otimizar performance.

## ⚡ Melhorias de Performance

### Otimizações Aplicadas

- **Cache**: 128MB (aumentado de 64MB)
- **Mmap**: 512MB (aumentado de 256MB)
- **WAL Mode**: Write-Ahead Logging para melhor concorrência
- **PRAGMA optimize**: Análise automática de queries
- **Foreign Keys**: Habilitadas para integridade referencial

### Índices Otimizados

- `idx_objetos_registro_base`: Busca por registro base
- `idx_objetos_versao`: Busca por versão (composite)
- `idx_objetos_data_insercao`: Ordenação por data
- `idx_objetos_iccid`: Busca por ICCID (partial index)

## 📈 Estatísticas

### `get_relatorio_objetos_stats()`

Retorna estatísticas detalhadas:

```python
{
    'total_registros': int,           # Total de versões
    'codigos_unicos': int,            # Registros únicos (versões mais recentes)
    'total_versoes': int,             # Total de versões (histórico)
    'registros_com_historico': int,   # Registros com múltiplas versões
    'com_iccid': int,                 # Registros com ICCID
    'entregues': int,                 # Registros com data de entrega
    'ultima_atualizacao': str         # Data da última atualização
}
```

## 🔄 Migração

### Schema Version 5

A migração v5 adiciona automaticamente:

1. Campos `registro_id_base` e `versao` à tabela `relatorio_objetos`
2. Migra registros existentes para versão 1
3. Cria índices otimizados
4. Preserva todos os dados existentes

## 📝 Exemplos de Uso

### Sincronização

```python
from src.database import DatabaseManager
from src.utils.objects_loader import ObjectsLoader

db_manager = DatabaseManager("data/portabilidade.db")
objects_loader = ObjectsLoader("Relatorio_Objetos.xlsx")

# Sincronizar com versionamento
stats = db_manager.sync_relatorio_objetos(objects_loader)
print(f"Novos: {stats['inseridos']}, Versões: {stats['novas_versoes']}")
```

### Busca

```python
# Buscar versão mais recente
obj = db_manager.get_relatorio_objeto_by_codigo("250001234")
if obj:
    print(f"Versão: {obj['versao']}, ICCID: {obj['iccid']}")
```

### Manutenção

```python
# Validar integridade
integrity = db_manager.validate_database_integrity()
if integrity['integrity_check'] != 'OK':
    print(f"Erros: {integrity['errors']}")

# Limpar versões antigas
cleanup = db_manager.cleanup_old_versions(days_to_keep=90, keep_min_versions=5)
print(f"Removidas {cleanup['removidos']} versões antigas")

# Obter tamanho do banco
size = db_manager.get_database_size()
print(f"Tamanho: {size['file_size_mb']} MB, Total: {size['total_rows']} linhas")
```

## 🎯 Boas Práticas

1. **Validação periódica**: Execute `validate_database_integrity()` semanalmente
2. **Limpeza mensal**: Execute `cleanup_old_versions()` mensalmente
3. **Otimização**: Execute `rebuild_indexes()` após grandes importações
4. **Monitoramento**: Use `get_database_size()` para acompanhar crescimento

## 🔒 Integridade

- **Foreign Keys**: Habilitadas para garantir integridade referencial
- **Unique Constraints**: `(registro_id_base, versao)` garante unicidade
- **Validação**: Métodos de validação detectam registros órfãos
- **Rollback**: Transações com rollback automático em caso de erro

## 📚 Referências

- Schema Version: 5
- Última atualização: 2025-01-XX
- Arquivo: `src/database/db_manager.py`

