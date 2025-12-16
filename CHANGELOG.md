# Changelog - Melhorias e Correções

## [1.1.1] - 2025-12-12

### 🧹 Limpeza e Organização
- ✅ Removido import duplicado de `sys` no `main.py`
- ✅ Removidos arquivos de teste duplicados (`testar_processamento.py`, `teste_processamento_completo.py`, `teste_rapido.py`)
- ✅ Consolidada documentação duplicada
- ✅ Removido arquivo pessoal `CAMINHOS_IMPORTANTES.txt` do repositório

### 🐛 Correções
- Corrigido import duplicado de `sys` no `main.py`
- Removido comentário duplicado sobre encoding

## [1.1.0] - 2025-12-11

### ✨ Melhorias Implementadas

#### 1. Correções de Encoding no Console Windows
- ✅ Criado módulo `src/utils/console_utils.py` para configuração automática de UTF-8
- ✅ Configuração automática do código de página do console Windows (chcp 65001)
- ✅ Handler de logging com encoding UTF-8 e fallback seguro
- ✅ Substituição de caracteres especiais problemáticos (→ por >>)

#### 2. Melhorias na Organização de Arquivos Processados
- ✅ Adicionado parâmetro `--move-processed` para mover arquivos após processamento
- ✅ Movimentação automática com timestamp para evitar sobrescrita
- ✅ Criação automática de pastas de destino
- ✅ Tratamento de erros na movimentação de arquivos

#### 3. Otimizações de Performance
- ✅ **Processamento em lote**: Novo método `process_records_batch()` na engine
- ✅ **Inserção em lote no banco**: Método `insert_records_batch()` no DatabaseManager
- ✅ **Parâmetro `--batch-size`**: Configurável (padrão: 100 registros)
- ✅ **Logging otimizado**: Modo `--verbose` para logs detalhados (desabilitado por padrão)
- ✅ **Redução de commits**: Inserções em lote reduzem commits ao banco
- ✅ **Filtro de logs**: Exibe apenas regras de alta prioridade (priority <= 2) quando não verbose

#### 4. Melhorias de Validação e Tratamento de Erros
- ✅ **Validação de CPF aprimorada**: Algoritmo de validação de dígitos verificadores
- ✅ **Tratamento de encoding melhorado**: Suporte automático a múltiplos encodings (UTF-8, Latin-1, CP1252, ISO-8859-1)
- ✅ **DatabaseManager**: Melhor tratamento de exceções SQLite com rollback automático
- ✅ **Timeout de conexão**: Adicionado timeout de 30 segundos para conexões de banco
- ✅ **Foreign keys habilitadas**: Melhor integridade referencial

### 📊 Ganhos de Performance Esperados
- **Processamento em lote**: ~3-5x mais rápido para arquivos grandes
- **Redução de I/O**: Menos operações de escrita no banco
- **Logs mais limpos**: Apenas informações relevantes por padrão

### 🔧 Novos Parâmetros de Linha de Comando

```bash
# Processar com movimentação automática
py main.py --csv arquivo.csv --move-processed pasta/processados

# Modo verbose (logs detalhados)
py main.py --csv arquivo.csv --verbose

# Ajustar tamanho do lote
py main.py --csv arquivo.csv --batch-size 50

# Combinar opções
py main.py --csv arquivo.csv --move-processed pasta/processados --batch-size 200 --verbose
```

### 🐛 Correções
- Corrigido encoding no console Windows
- Corrigido método de inserção em lote (busca correta de IDs)
- Melhorado tratamento de erros em processamento em lote
- Corrigido erro de ortografia na Regra 16: "Portabillidade" → "Portabilidade"
- Corrigido erro no csv_parser.py: UnicodeDecodeError trocado por ValueError apropriado

### 📝 Arquivos Modificados
- `main.py` - Função `process_csv_file()` otimizada, import duplicado removido
- `src/utils/console_utils.py` - Novo módulo para encoding
- `src/database/db_manager.py` - Método `insert_records_batch()` adicionado, melhor tratamento de erros
- `src/engine/qigger_decision_engine.py` - Método `process_records_batch()` adicionado, validação CPF aprimorada
- `src/utils/csv_parser.py` - Suporte a múltiplos encodings, melhor tratamento de erros
- `src/utils/__init__.py` - Exportação de novos utilitários

