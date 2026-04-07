#!/usr/bin/env bash
# Gera apenas o arquivo de homologação de Consulta (para testar rapidamente).
# Uso: ./atualizar_consulta.sh
cd "$(dirname "$0")"
if [ -d ".venv" ] && [ -x ".venv/bin/python" ]; then
    exec .venv/bin/python gerar_homologacao_consulta.py "$@"
elif [ -d "venv" ] && [ -x "venv/bin/python" ]; then
    exec venv/bin/python gerar_homologacao_consulta.py "$@"
else
    echo "Ambiente virtual não encontrado. Use: ./run_processar_completo.sh --apenas-homologacao"
    exit 1
fi
