"""
Funções reutilizáveis de validação de integridade de dados.

Usadas por todos os geradores de homologação para garantir que:
- Linhas com campos obrigatórios vazios sejam excluídas
- Valores NULL/None/'None'/'NULL'/NaN sejam convertidos para string vazia
"""
import logging
import math
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

# Valores que devem ser tratados como vazio
_EMPTY_LITERALS = frozenset(('none', 'null', 'nan', 'nat'))


def sanitizar_valor(valor) -> str:
    """
    Converte None, 'None', 'NULL', 'nan', NaN, NaT, inf para string vazia.
    Nunca retorna literal 'None' ou 'NULL'.

    Args:
        valor: Qualquer valor a ser sanitizado.

    Returns:
        String sanitizada (nunca 'None' ou 'NULL').
    """
    if valor is None:
        return ''
    if isinstance(valor, float) and (math.isnan(valor) or math.isinf(valor)):
        return ''
    # Handle pandas NaT / NaT-like objects
    try:
        import pandas as pd
        if isinstance(valor, type(pd.NaT)) and pd.isna(valor):
            return ''
    except (ImportError, TypeError, ValueError):
        pass
    s = str(valor).strip()
    if s.lower() in _EMPTY_LITERALS:
        return ''
    return s


def validar_integridade_linha(
    row: Dict[str, Any],
    campos_obrigatorios: List[str],
    contexto: str = '',
) -> bool:
    """
    Valida que todos os campos obrigatórios estão preenchidos.

    Args:
        row: Dicionário com os dados da linha.
        campos_obrigatorios: Lista de nomes de campos que devem estar preenchidos.
        contexto: Identificador para log (ex: proposta_isize).

    Returns:
        True se todos os campos obrigatórios estão preenchidos, False caso contrário.
    """
    ctx = contexto or row.get('codigo_externo', row.get('Código externo', 'desconhecido'))
    for campo in campos_obrigatorios:
        valor = row.get(campo)
        if valor is None:
            logger.info(
                "Registro incompleto excluído: %s — campo '%s' vazio",
                ctx, campo,
            )
            return False
        s = str(valor).strip()
        if s == '' or s.lower() in _EMPTY_LITERALS:
            logger.info(
                "Registro incompleto excluído: %s — campo '%s' vazio",
                ctx, campo,
            )
            return False
    return True


def safe_str(valor: Any, default: str = '') -> str:
    """
    Converte valor para string segura para arquivo de saída.
    NULL/None/NaN/NaT/inf vira string vazia, nunca literal 'None' ou 'NULL'.
    """
    if valor is None:
        return default
    if isinstance(valor, float) and (math.isnan(valor) or math.isinf(valor)):
        return default
    try:
        import pandas as pd
        if isinstance(valor, type(pd.NaT)) and pd.isna(valor):
            return default
    except (ImportError, TypeError, ValueError):
        pass
    s = str(valor).strip()
    if s.lower() in _EMPTY_LITERALS:
        return default
    return s
