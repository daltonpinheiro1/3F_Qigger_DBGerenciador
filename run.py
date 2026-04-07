#!/usr/bin/env python3
"""
Runner unificado - ponto de entrada para os principais comandos do projeto.
Uso:
  python run.py completo [--workers N] [--no-caffeinate] [--apenas-bases] ...
  python run.py validar [--homologacao]
  python run.py revisar [--exportar-schema] [--corrigir]
  python run.py backup [--apenas-rede] [--apenas-local] [--limpar]
  python run.py bs_venda_du   # Processar BS_VENDA_DU.xlsx → tabela bs_venda_du

Ou use os scripts diretos: processar_completo.py, validar_fluxo_completo.py, etc.
"""
import sys
import subprocess
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent


def _venv_python():
    for py in (PROJECT_ROOT / ".venv" / "bin" / "python", PROJECT_ROOT / "venv" / "bin" / "python"):
        if py.exists() and py.is_file():
            return str(py)
    return sys.executable


def _run(script: str, args: list) -> int:
    script_path = PROJECT_ROOT / script
    if not script_path.exists():
        print(f"Script não encontrado: {script_path}")
        return 1
    cmd = [_venv_python(), str(script_path)] + args
    return subprocess.run(cmd, cwd=PROJECT_ROOT).returncode


def main():
    if len(sys.argv) < 2:
        print("Uso: python run.py <comando> [opções]")
        print("")
        print("Comandos:")
        print("  completo    Processamento completo (bases + homologação + backup)")
        print("              Ex.: run.py completo --cafinate")
        print("  validar     Valida integridade do banco e contagens; --homologacao para checar saídas")
        print("  revisar      Revisão de tabelas (colunas duplicadas, integridade); --exportar-schema, --corrigir")
        print("  backup      Backup do banco (local + rede); --apenas-rede, --apenas-local, --limpar")
        print("  bs_venda_du Processar BS_VENDA_DU.xlsx → tabela bs_venda_du")
        print("")
        print("Para opções de cada comando, use: python <script>.py --help")
        return 0

    comando = sys.argv[1].lower()
    args = sys.argv[2:]

    if comando == "completo":
        return _run("processar_completo.py", args)
    if comando == "validar":
        return _run("validar_fluxo_completo.py", args)
    if comando == "revisar":
        return _run("revisar_tabelas_db.py", args)
    if comando == "backup":
        return _run("backup_database.py", args)
    if comando == "bs_venda_du":
        return _run("processar_bs_venda_du.py", args)

    print(f"Comando desconhecido: {comando}")
    print("Comandos: completo, validar, revisar, backup, bs_venda_du")
    return 1


if __name__ == "__main__":
    sys.exit(main())
