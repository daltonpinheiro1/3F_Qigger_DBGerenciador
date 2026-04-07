#!/usr/bin/env python3
"""
Validação do fluxo processar_completo (DBA/MIS).
- Verifica integridade do banco (PRAGMA integrity_check / quick_check)
- Lista contagens das tabelas principais
- Opcional: verifica existência e tamanho dos arquivos de homologação esperados

Uso:
  python validar_fluxo_completo.py
  python validar_fluxo_completo.py --db /caminho/portabilidade.db
  python validar_fluxo_completo.py --homologacao   # também valida arquivos de saída
"""
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from config import DB_PATH
    DEFAULT_DB_PATH = str(DB_PATH)
except ImportError:
    DEFAULT_DB_PATH = str(PROJECT_ROOT / "data" / "portabilidade.db")

# Arquivos de homologação esperados (após Etapa 4)
ARQUIVOS_HOMOLOGACAO = [
    "data/homologacao_wpp.xlsx",
    "data/homologacao_reabertura.xlsx",
    "data/homologacao_aprovisionamento.xlsx",
    "data/homologacao_erro_aprovisionamento.xlsx",
    "data/homologacao_entrega_baixa.xlsx",
]


def main():
    import argparse
    p = argparse.ArgumentParser(description="Validação do fluxo (integridade DB e opcionalmente saídas)")
    p.add_argument("--db", default=DEFAULT_DB_PATH, help="Caminho do banco")
    p.add_argument("--homologacao", action="store_true", help="Validar também arquivos de homologação")
    args = p.parse_args()

    db_path = str(args.db)
    if not Path(db_path).exists():
        print(f"❌ Banco não encontrado: {db_path}")
        return 1

    print("=" * 60)
    print("VALIDAÇÃO DO FLUXO (DBA/MIS)")
    print("=" * 60)
    print(f"Banco: {db_path}\n")

    # 1) Integridade
    try:
        from processar_completo import verificar_integridade_banco
        r = verificar_integridade_banco(db_path)
        if r.get("ok"):
            print("[1] Integridade do banco: OK")
        else:
            print(f"[1] Integridade do banco: ERRO - {r.get('integrity_check', r.get('errors'))}")
    except Exception as e:
        print(f"[1] Integridade: erro ao verificar - {e}")

    # 2) Contagens
    try:
        from processar_completo import contagens_tabelas_principais
        counts = contagens_tabelas_principais(db_path)
        print("\n[2] Contagens das tabelas principais:")
        for t, c in counts.items():
            print(f"    {t}: {c if c is not None else 'N/A'}")
    except Exception as e:
        print(f"[2] Contagens: {e}")

    # 3) Arquivos de homologação (opcional)
    if args.homologacao:
        print("\n[3] Arquivos de homologação:")
        for rel in ARQUIVOS_HOMOLOGACAO:
            f = PROJECT_ROOT / rel
            if f.exists():
                size = f.stat().st_size
                status = "✓" if size > 0 else "⚠ vazio"
                print(f"    {status} {rel} ({size} bytes)")
            else:
                print(f"    ✗ {rel} (não encontrado)")

    print("\n" + "=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
