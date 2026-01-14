# Changelog - Melhorias e Correções

## [3.0.0] - 2025-01-XX

### 🔄 Versionamento Completo do Banco de Dados

#### 1. Tabela `relatorio_objetos` com Versionamento
- ✅ **Versionamento completo**: Cada mudança cria nova versão preservando histórico
- ✅ **Campos de versionamento**: `registro_id_base` e `versao` adicionados
- ✅ **Verificação inteligente**: Só cria versão se houver mudanças reais
- ✅ **Sem mudanças**: Apenas atualiza `updated_at` (não cria nova versão)
- ✅ **Campos monitorados**: `id_erp`, `rastreio`, `iccid`, `status`, `data_entrega`, `ultima_ocorrencia`, etc.

#### 2. Migração v5
- ✅ **Migração automática**: Preserva todos os dados existentes
- ✅ **Registros existentes**: Migrados para versão 1 automaticamente
- ✅ **Índices otimizados**: Criados para busca por versão

#### 3. Métodos de Sincronização Melhorados
- ✅ **`sync_relatorio_objetos()`**: Retorna estatísticas detalhadas
  - `inseridos`: Novos registros (versão 1)
  - `novas_versoes`: Novas versões criadas
  - `sem_mudancas`: Registros sem alterações
  - `erros`: Erros durante processamento

### 🛠️ Métodos de Manutenção e DBA

#### 1. Métodos de Manutenção
- ✅ **`get_database_size()`**: Informações sobre tamanho do banco
- ✅ **`cleanup_old_versions()`**: Limpeza de versões antigas mantendo as N mais recentes
- ✅ **`validate_database_integrity()`**: Validação completa de integridade
- ✅ **`rebuild_indexes()`**: Reconstrução de índices para otimização

#### 2. Melhorias de Performance
- ✅ **Cache aumentado**: 64MB → 128MB
- ✅ **Mmap aumentado**: 256MB → 512MB
- ✅ **PRAGMA optimize**: Análise automática de queries
- ✅ **Foreign Keys**: Habilitadas para integridade referencial

#### 3. Índices Otimizados
- ✅ **`idx_objetos_registro_base`**: Busca por registro base
- ✅ **`idx_objetos_versao`**: Busca por versão (composite)
- ✅ **`idx_objetos_data_insercao`**: Ordenação por data
- ✅ **`idx_objetos_iccid`**: Busca por ICCID (partial index)

### 📊 Estatísticas Melhoradas

#### `get_relatorio_objetos_stats()`
- ✅ **`total_versoes`**: Total de versões (histórico completo)
- ✅ **`registros_com_historico`**: Registros com múltiplas versões
- ✅ **`codigos_unicos`**: Registros únicos (apenas versões mais recentes)

### 🔄 Sincronização Automática

#### Processamento de Arquivos
- ✅ **Sincronização automática**: Relatório de Objetos sincronizado automaticamente
- ✅ **Deleção de arquivos**: Arquivos deletados após processamento bem-sucedido
- ✅ **Logs detalhados**: Estatísticas de sincronização exibidas

### 📚 Documentação

- ✅ **`docs/MELHORIAS_BANCO_DADOS.md`**: Documentação completa das melhorias
- ✅ **Exemplos de uso**: Exemplos práticos de todos os métodos
- ✅ **Boas práticas**: Guia de manutenção e otimização

## [2.0.0] - 2025-12-22

### 🚀 Melhorias de Performance

#### 1. Otimização do ObjectsLoader
- ✅ **Indexação múltipla**: Índices por código externo, ID ERP, CPF e Nu Pedido
- ✅ **Cache de buscas**: Cache LRU para evitar buscas repetidas
- ✅ **Pré-ordenação**: Registros ordenados por data para priorização automática do mais recente
- ✅ **Novo método `find_by_nu_pedido()`**: Busca pelo número do pedido original
- ✅ **Método `clear_cache()`**: Limpeza manual do cache quando necessário

#### 2. Otimização do TriggerLoader
- ✅ **Índice por status_bilhete**: Busca O(1) para regras por status
- ✅ **Índice por regra_id**: Busca direta por ID da regra
- ✅ **Cache de matching**: Cache por chave composta MD5 para evitar reavaliações
- ✅ **Early returns**: Retornos antecipados no algoritmo de matching
- ✅ **Novo método `get_rules_by_status()`**: Busca todas regras de um status

#### 3. Otimização do Engine de Decisão
- ✅ **Enriquecimento em batch**: Método `_batch_enrich_logistics()` para processar múltiplos registros
- ✅ **Processamento paralelo opcional**: Suporte a ThreadPoolExecutor para grandes lotes
- ✅ **Salvamento em batch otimizado**: Método `_batch_save_to_db()`
- ✅ **Métricas de performance**: Log de registros/segundo no processamento

### ✨ Nova Funcionalidade: Link de Rastreio

#### Implementação do Link https://tim.trakin.co/o/{numero_pedido}
- ✅ **Método `gerar_link_rastreio()`**: Gera link automaticamente a partir do código do pedido
- ✅ **Priorização de fontes**: 
  1. Nu Pedido do Relatório de Objetos (mais atualizado)
  2. Código externo do registro
  3. Fallback para rastreio original
- ✅ **Campo Cod_Rastreio**: Adicionado em todas as planilhas de saída:
  - Retornos_Qigger.csv (Google Drive)
  - Aprovisionamentos.csv (Backoffice)
  - Reabertura.csv (Backoffice)
  - WPP_Regua_Output.csv (Régua de Comunicação)

### 🐛 Correções de Bugs

#### Model PortabilidadeRecord
- ✅ Adicionados campos faltantes que causavam erros no csv_generator:
  - `motivo_nao_cancelado`
  - `motivo_nao_aberto`
  - `motivo_nao_reagendado`
  - `numero_acesso_valido`
  - `ajustes_registro`
  - `ajustes_numero_acesso`
  - `novo_status_bilhete`
  - `nova_data_portabilidade`
- ✅ Atualizado método `to_dict()` para incluir todos os campos

### 📊 Melhorias na Integração de Bases

#### Régua de Comunicação Dinâmica
- ✅ **Priorização de dados para envio**: Dados do Relatório de Objetos têm prioridade
- ✅ **Consolidação inteligente**: Dados mais recentes prevalecem
- ✅ **Fallback automático**: Se não houver logística, usa dados da base analítica
- ✅ **Link de rastreio garantido**: Sempre gera link mesmo sem dados de logística

### 📁 Arquivos Modificados

```
src/models/portabilidade.py
  - Novos campos adicionados
  - Método gerar_link_rastreio()
  - Método enrich_with_logistics() atualizado
  - Método to_wpp_dict() atualizado

src/utils/objects_loader.py
  - Versão 2.0 com indexação otimizada
  - Cache de buscas
  - Métodos de busca otimizados

src/engine/trigger_loader.py
  - Versão 2.0 com cache e índices
  - Early returns no matching
  - Geração de cache key MD5

src/engine/qigger_decision_engine.py
  - Versão 3.1 com batch otimizado
  - Geração automática de links de rastreio
  - Suporte a processamento paralelo

src/utils/csv_generator.py
  - Campo Cod_Rastreio em todas as planilhas
  - Geração automática de links

src/utils/regua_comunicacao.py
  - Integração com links de rastreio

src/utils/regua_comunicacao_dinamica.py
  - Priorização de dados do Relatório de Objetos
  - Geração de links de rastreio
```

### 📱 Mapeamento de Templates WhatsApp

Novo módulo `src/utils/templates_wpp.py` com mapeamento dos templates:

| ID | Nome_modelo | Uso |
|----|-------------|-----|
| 1 | `confirma_portabilidade_v1` | Confirmação de portabilidade processada |
| 2 | `pendencia_sms_portabilidade` | Pendência de validação SMS |
| 3 | `aviso_retirada_correios_v1` | Chip aguardando retirada nos Correios |
| 4 | `confirmacao_endereco_v1` | Confirmação de endereço de entrega |

**Mapeamento Tipo_Comunicacao -> Template:**
- 1, 2, 3 (Portabilidade) → `confirma_portabilidade_v1`
- 5, 6 (Reagendar/Pendente) → `pendencia_sms_portabilidade`
- 14 (Aguardando Retirada) → `aviso_retirada_correios_v1`
- 43 (Endereço Incorreto) → `confirmacao_endereco_v1`

**Novos campos na saída WPP:**
- `Template_ID`: ID do template (1, 2, 3, 4)
- `Template_Nome`: Nome do modelo do template
- `Template_Variaveis`: Variáveis formatadas (ex: `{{1}}=João;{{2}}=ABC123`)

### 📈 Ganhos de Performance Esperados
- **Busca de regras**: ~5-10x mais rápido com índices
- **Busca de objetos**: ~3-5x mais rápido com cache
- **Processamento batch**: ~2-3x mais rápido com enriquecimento em batch

---

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

