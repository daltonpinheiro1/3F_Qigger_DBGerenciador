#!/usr/bin/env python3
"""
Verifica arquivos na pasta de importações que ainda não foram processados
ou que foram processados e não foram movidos para processados.

Uso: python3 verificar_arquivos_importacoes.py
"""
from pathlib import Path
from datetime import datetime

try:
    from config import PASTA_IMPORTACOES
    PASTA = Path(PASTA_IMPORTACOES)
except ImportError:
    PASTA = Path("/Applications/Documentos/IMPORTACOES_QIGGER")

PROCESSADOS = PASTA / "processados"


def main():
    print("=" * 70)
    print("VERIFICAÇÃO DE ARQUIVOS - IMPORTACOES_QIGGER")
    print("=" * 70)
    print(f"Pasta: {PASTA}")
    print(f"Processados: {PROCESSADOS}")
    print()

    if not PASTA.exists():
        print("⚠ Pasta de importações não encontrada.")
        return

    # Arquivos na raiz (candidatos a processamento)
    xlsx_raiz = list(PASTA.glob("*.xlsx"))
    csv_raiz = list(PASTA.glob("*.csv"))
    # Excluir arquivos de sistema / temporários
    xlsx_raiz = [f for f in xlsx_raiz if not f.name.startswith("~$")]
    csv_raiz = [f for f in csv_raiz if not f.name.startswith("~$")]

    print("[1] Arquivos na raiz (aguardando processamento ou já processados e não movidos)")
    print()
    if not xlsx_raiz and not csv_raiz:
        print("    Nenhum .xlsx ou .csv na raiz.")
    else:
        for f in sorted(xlsx_raiz + csv_raiz, key=lambda x: (x.suffix, x.name)):
            mtime = datetime.fromtimestamp(f.stat().st_mtime).strftime("%d/%m/%Y %H:%M")
            # Classificar se será processado pelo fluxo ou não
            nome = f.name.lower()
            if "3f_gross" in nome or "gross" in nome and "pre" in nome and "fechamento" in nome:
                tipo = "→ Será processado (Etapa 1b GROSS) e movido para processados"
            elif "relatorio_objetos" in nome:
                tipo = "→ Será processado (Etapa 3 Relatório Objetos) e movido para processados"
            elif f.suffix.lower() == ".csv":
                tipo = "→ Será processado (Etapa 2 CSV) e movido para processados"
            else:
                tipo = "→ NÃO é processado pelo fluxo atual (mover manualmente para processados se desejar)"
            print(f"    • {f.name}")
            print(f"      Modificado: {mtime}  {tipo}")
        print()

    # Contagem em processados
    if PROCESSADOS.exists():
        n_xlsx = len(list(PROCESSADOS.glob("*.xlsx")))
        n_csv = len(list(PROCESSADOS.glob("*.csv")))
        print(f"[2] Pasta processados: {n_xlsx} .xlsx e {n_csv} .csv")
    else:
        print("[2] Pasta processados ainda não existe.")

    print()
    print("=" * 70)
    print("Para processar e mover os arquivos, execute: python3 processar_completo.py")
    print("=" * 70)


if __name__ == "__main__":
    main()
