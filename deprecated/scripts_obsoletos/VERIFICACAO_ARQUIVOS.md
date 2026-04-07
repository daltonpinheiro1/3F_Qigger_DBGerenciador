# Verificação de Arquivos e Comandos do Projeto

## 1. Arquivos de homologação gerados pelo `processar_completo`

| Script | Arquivo gerado | Destino | Status |
|--------|----------------|---------|--------|
| gerar_homologacao_wpp.py | data/homologacao_wpp.xlsx | PASTA_SAIDA (Retornos do gerenciador) | ✅ OK |
| gerar_homologacao_reabertura.py | data/homologacao_reabertura.xlsx | PASTA_SAIDA | ✅ OK |
| gerar_homologacao_aprovisionamento.py | data/homologacao_aprovisionamento.xlsx | PASTA_SAIDA | ✅ OK |
| gerar_homologacao_erro_aprovisionamento.py | data/homologacao_erro_aprovisionamento.xlsx | PASTA_SAIDA | ✅ OK |
| gerar_homologacao_entrega_baixa.py | data/homologacao_entrega_baixa.xlsx | PASTA_SAIDA | ✅ OK |

**Fluxo:** Cada script gera em `data/` → processar_completo copia com timestamp para PASTA_SAIDA → original é movido para data/processados.

---

## 2. Banco 3f-entrega-local.sqlite (D1 Entrega)

**Comando:** `python3 processar_completo.py --apenas-d1-entrega`

**Etapas executadas:**
1. `wrangler d1 export 3f-entrega-db --remote --output=3f-entrega-backup.sql`
2. Remove `3f-entrega-local.sqlite` antigo (import limpo)
3. `sqlite3 3f-entrega-local.sqlite < 3f-entrega-backup.sql`

**Localização:** `/Applications/Documentos/Projetos_python/3F_Mensagens/cloudflare-worker-entrega/`

**Requisitos:**
- `wrangler` instalado e autenticado (`wrangler login`)
- `sqlite3` disponível no sistema (macOS já inclui)
- Banco D1 `3f-entrega-db` configurado no wrangler.toml

**Observação:** Este comando **não** roda no fluxo principal do processar_completo. Use `--apenas-d1-entrega` para sincronizar.

---

## 3. Outros arquivos gerados pelo projeto

| Origem | Arquivo | Local |
|--------|---------|-------|
| processar_atualizacoes_gerar_finais | Retornos_Qigger.csv | data/retornos/google_drive/ |
| processar_atualizacoes_gerar_finais | Reabertura_*.csv | data/retornos/backoffice/ |
| processar_atualizacoes_gerar_finais | Aprovisionamentos_*.csv | data/retornos/backoffice/ |
| gerar_homologacao_consulta | homologacao_consulta.xlsx | Retornos do gerenciador |
| gerar_wpp_ids_forcados | homologacao_wpp_forcados.csv | data/ |

---

## 4. Banco principal

- **portabilidade.db:** `data/portabilidade.db`
- **Replicação:** Etapa 5 do processar_completo replica para SMB 07 Backoffice

---

## 5. Ordem de execução (processar_completo)

**Obrigatório:** O portabilidade.db deve ser atualizado ANTES de gerar qualquer arquivo de homologação.

1. **COVERTE BASE PROP** (rede SMB) → base_coverte_prop
2. **Arquivos da pasta** (entrada + IMPORTACOES_QIGGER) → portabilidade_records, relatorio_objetos, etc.
3. **Fallback COVERTE** (se não processou em 1)
4. **Gerar homologação** (WPP, Reabertura, Aprovisionamento, Erro, Entrega/Baixa)

## 6. Regra dos 180 dias

Todos os scripts de homologação usam `DIAS_LIMITE_HOMOLOGACAO = 180`:
- Filtro: `data_venda` ou `data_conectada` >= (hoje - 180 dias)
- Scripts: gerar_homologacao_wpp, reabertura, aprovisionamento, erro_aprovisionamento, entrega_baixa
- Não há datas fixas (ex.: 2026-01-01 foi removida)

## 7. Correções aplicadas (fev/2026)

1. **sincronizar_d1_entrega:** Remove banco local antes do import; captura stderr; valida tamanho.
2. **gerar_homologacao_wpp:** Fallback usa `data/homologacao_wpp.xlsx`; data fixa 2026-01-01 substituída por filtro dinâmico (180 dias).
3. **processar_completo:** COVERTE processado PRIMEIRO (rede SMB) antes dos arquivos da pasta.
