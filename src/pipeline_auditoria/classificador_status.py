"""
Classificador de status para retornos RPA de vendas TIM Pré/Controle.

Aplica regras de parametrização ordenadas para classificar o status
de cada venda com base no protocolo e motivo informados.
"""

from typing import Dict, List, Optional


class ClassificadorStatus:
    """Classifica vendas com base em protocolo e motivo do retorno RPA.

    Regras (em ordem de prioridade):
    1. Protocolo preenchido → "Emitida"
    2. Primeira regra cujo padrão está contido no motivo → status da regra
    3. Fallback → "NAO PARAMETRIZADO"
    """

    def __init__(self, regras: Optional[List[Dict[str, str]]] = None):
        """
        Args:
            regras: Lista ordenada de regras [{padrao, status}].
                    Se None, carrega PARAMETRIZACAO_STATUS de config.py.
        """
        if regras is None:
            from config import PARAMETRIZACAO_STATUS
            self.regras = PARAMETRIZACAO_STATUS
        else:
            self.regras = regras

    def classificar(self, protocolo: str, motivo: str) -> str:
        """Classifica o status com base no protocolo e motivo.

        Args:
            protocolo: Protocolo do retorno RPA. Se preenchido
                       (não None/vazio/whitespace), retorna "Emitida".
            motivo: Motivo de não ter sido migrado. Usado para
                    buscar correspondência nas regras de parametrização.

        Returns:
            Status classificado como string.
        """
        if protocolo and str(protocolo).strip():
            return "Emitida"

        motivo_str = str(motivo) if motivo is not None else ""

        if not motivo_str.strip():
            return "PENDENTE"

        for regra in self.regras:
            if regra["padrao"] in motivo_str:
                return regra["status"]

        return "NAO PARAMETRIZADO"
