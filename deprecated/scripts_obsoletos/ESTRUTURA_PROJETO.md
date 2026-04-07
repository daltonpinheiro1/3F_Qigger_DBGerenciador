# Estrutura e reorganização do projeto

Sugestão de organização para manter o projeto escalável e claro.

---

## Estrutura atual (resumida)

- **Raiz**: `run.py` (runner unificado: completo, validar, revisar, backup), scripts principais (`processar_completo.py`, `gerar_homologacao_*.py`, `processar_*.py`, `backup_database.py`, `revisar_tabelas_db.py`, `validar_fluxo_completo.py`), `config.py`, `triggers.xlsx`, `requirements.txt`, `README.md`
- **src/**: código reutilizável (database, engine, models, utils, monitor)
- **data/**: banco, entrada, processados, backups
- **logs/**: logs de execução
- **docs/**: documentação de fluxo e regras (este arquivo, PROCESSAMENTO_COMPLETO.md, REGRAS_GERADORES_HOMOLOGACAO.md)
- **tests/**: testes unitários
- **deprecated/**: scripts e validadores antigos

---

## Reorganização opcional (futura)

Se quiser separar melhor “executáveis” de “biblioteca”:

```
3F_Qigger_DBGerenciador/
├── src/                    # inalterado
├── scripts/                # (opcional) scripts de linha de comando
│   ├── run_processar_completo.sh
│   └── ...
├── docs/
├── tests/
├── data/
├── logs/
├── config.py
├── processar_completo.py   # manter na raiz (ponto de entrada principal)
├── gerar_homologacao_*.py
├── processar_*.py
├── backup_database.py
├── revisar_tabelas_db.py
├── validar_fluxo_completo.py
└── ...
```

- Manter **processar_completo.py** na raiz garante que `python processar_completo.py` e `run_processar_completo.sh` continuem funcionando sem alterar `PYTHONPATH` ou imports.
- Scripts como `revisar_tabelas_db.py` e `validar_fluxo_completo.py` podem ficar na raiz (como hoje) ou, se preferir, em `scripts/` com um `scripts/run.sh` que chame o Python do projeto.

---

## Boas práticas aplicadas

1. **Configuração**: `config.py` centralizado; credenciais SMB em `.env` (não versionado).
2. **Integridade**: verificação de banco após cargas (Etapas 1 e 2); `revisar_tabelas_db.py` para colunas duplicadas e PRAGMA.
3. **Rollback**: `processar_relatorio_faturamento` com `rollback` em exceção; `DatabaseManager` com rollback no context manager.
4. **Validação de saída**: arquivos de homologação checados (existência e tamanho) após geração.
5. **Documentação**: fluxo em `docs/PROCESSAMENTO_COMPLETO.md`; regras dos geradores em `docs/REGRAS_GERADORES_HOMOLOGACAO.md`.
6. **Auditoria**: contagens das tabelas principais após Etapa 2; script `validar_fluxo_completo.py` para checagem sob demanda.
