#!/bin/bash
# Roda processar_completo.py usando o Python do ambiente virtual (.venv).
# Uso: ./run_processar_completo.sh [--cafinate] [outros argumentos]
cd "$(dirname "$0")"
# Remove o separador "--" se o usuário passou (ex.: ./run_processar_completo.sh --cafinate -- --workers 3)
args=()
for a in "$@"; do
    [ "$a" != "--" ] || continue
    args+=("$a")
done
if [ -d ".venv" ] && [ -x ".venv/bin/python" ]; then
    exec .venv/bin/python processar_completo.py "${args[@]}"
else
    echo "Ambiente virtual não encontrado. Crie com: python3 -m venv .venv && .venv/bin/pip install -r requirements.txt"
    exit 1
fi
