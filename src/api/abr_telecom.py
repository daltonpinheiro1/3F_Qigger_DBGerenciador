"""
Cliente da API ABR Telecom (r-access-data v2.1.0) via PMID TIM.

Consulta operadora, status (ativo/inativo) e tipo de plano de um número.

Fluxo:
    1. Autenticação OAuth2 (Client Credentials) → Bearer token
    2. POST /oauth/access/v2/accessData com CPF + MSISDN
    3. Retorno: operadora, ativo, tipo plano

Uso:
    from src.api.abr_telecom import ABRTelecom
    abr = ABRTelecom()
    resultado = abr.consultar('12345678901', '11999887766')
"""
import json
import logging
import os
import time
import uuid
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)

# Configuração via .env
_DEFAULT_BASE_URL = "https://api-gw.pmid.tim.com.br"
_OAUTH_TOKEN_URL = "https://api-gw.pmid.tim.com.br/oauth/accesstoken"


class ABRTelecom:
    """Cliente para consulta ABR Telecom via PMID."""

    def __init__(
        self,
        client_id: str = None,
        client_secret: str = None,
        scopes: str = None,
        base_url: str = None,
    ):
        self.client_id = client_id or os.getenv("TIM_CLIENT_ID", "")
        self.client_secret = client_secret or os.getenv("TIM_CLIENT_SECRET", "")
        self.scopes = scopes or os.getenv("TIM_SCOPES", "TIMVarejoRede.sales TIMVarejoRede.partner")
        self.base_url = base_url or os.getenv("TIM_API_BASE_URL", _DEFAULT_BASE_URL)
        self.token_url = os.getenv("TIM_OAUTH_TOKEN_URL", _OAUTH_TOKEN_URL)

        self._access_token: Optional[str] = None
        self._token_expires_at: float = 0
        self._abr_token: Optional[str] = None

        if not self.client_id or not self.client_secret:
            logger.warning(
                "TIM_CLIENT_ID ou TIM_CLIENT_SECRET não configurados no .env"
            )

    # ------------------------------------------------------------------
    # OAuth2 — Client Credentials
    # ------------------------------------------------------------------

    def _obter_token_oauth(self) -> str:
        """Obtém ou renova o token OAuth2 via Client Credentials."""
        if self._access_token and time.time() < self._token_expires_at:
            return self._access_token

        logger.info("Obtendo token OAuth2...")
        try:
            resp = requests.post(
                self.token_url,
                data={
                    "grant_type": "client_credentials",
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "scope": self.scopes,
                },
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()

            self._access_token = data.get("access_token", "")
            expires_in = int(data.get("expires_in", 3600))
            self._token_expires_at = time.time() + expires_in - 60

            logger.info("Token OAuth2 obtido (expira em %ds)", expires_in)
            return self._access_token

        except requests.RequestException as e:
            logger.error("Falha ao obter token OAuth2: %s", e)
            raise

    # ------------------------------------------------------------------
    # Consulta ABR
    # ------------------------------------------------------------------

    def consultar(
        self,
        cpf: str,
        msisdn: str,
        flag_pos: bool = True,
        flag_pre: bool = False,
    ) -> Dict[str, Any]:
        """
        Consulta dados de um acesso na ABR Telecom.

        Args:
            cpf: CPF do cliente (apenas dígitos).
            msisdn: Número completo com DDD (ex: '11999887766').
            flag_pos: Consultar base pós-pago.
            flag_pre: Consultar base pré-pago.

        Returns:
            Dict com:
                - status: '0'=encontrado, '1'=divergente, '2'=não encontrado
                - company: operadora (ex: 'TIM')
                - active: True/False
                - plan_type: '0'=pós, '1'=pré
                - raw: resposta completa
                - error: mensagem de erro (se houver)
        """
        cpf_limpo = "".join(c for c in str(cpf) if c.isdigit())
        msisdn_limpo = "".join(c for c in str(msisdn) if c.isdigit())

        if len(msisdn_limpo) < 10:
            return {"status": "99", "error": "MSISDN inválido", "raw": {}}

        # Extrair DDD (2 primeiros) e terminal (restante)
        ddd = msisdn_limpo[:2]
        terminal = msisdn_limpo[2:]

        token = self._obter_token_oauth()
        message_id = str(uuid.uuid4())

        url = f"{self.base_url}/oauth/access/v2/accessData"

        headers = {
            "Content-Type": "application/json",
            "messageId": message_id,
            "clientId": self.client_id,
            "Authorization": f"Bearer {token}",
        }

        body = {
            "socialSecNo": cpf_limpo,
            "msisdn": msisdn_limpo,
            "flagPos": flag_pos,
            "flagPre": flag_pre,
        }

        logger.debug(
            "ABR consulta: cpf=%s, msisdn=%s, ddd=%s, terminal=%s",
            cpf_limpo[:3] + "***",
            ddd + "***",
            ddd,
            terminal[:3] + "***",
        )

        try:
            resp = requests.post(url, json=body, headers=headers, timeout=30)

            if resp.status_code == 200:
                data = resp.json()
                status = str(data.get("status", ""))
                company = data.get("company", "")
                active_raw = data.get("active", "")
                plan_type = str(data.get("planTypeId", ""))

                # Converter active
                if isinstance(active_raw, bool):
                    active = active_raw
                elif str(active_raw).lower() in ("true", "1"):
                    active = True
                else:
                    active = False

                resultado = {
                    "status": status,
                    "company": company,
                    "active": active,
                    "plan_type": plan_type,
                    "raw": data,
                    "error": None,
                }

                logger.info(
                    "ABR resultado: msisdn=%s***, operadora=%s, ativo=%s, plano=%s",
                    ddd,
                    company,
                    active,
                    "POS" if plan_type == "0" else "PRE",
                )
                return resultado

            elif resp.status_code == 401:
                # Token expirado, renovar e tentar novamente
                self._access_token = None
                self._token_expires_at = 0
                logger.warning("Token expirado, renovando...")
                return self.consultar(cpf, msisdn, flag_pos, flag_pre)

            else:
                error_data = {}
                try:
                    error_data = resp.json()
                except Exception:
                    pass

                error_msg = error_data.get(
                    "description",
                    f"HTTP {resp.status_code}",
                )
                logger.error("ABR erro: %s — %s", resp.status_code, error_msg)
                return {
                    "status": str(resp.status_code),
                    "error": error_msg,
                    "raw": error_data,
                }

        except requests.Timeout:
            logger.error("ABR timeout para msisdn=%s***", ddd)
            return {"status": "timeout", "error": "Timeout na consulta", "raw": {}}
        except requests.RequestException as e:
            logger.error("ABR erro de conexão: %s", e)
            return {"status": "erro", "error": str(e), "raw": {}}

    # ------------------------------------------------------------------
    # Consulta em lote
    # ------------------------------------------------------------------

    def consultar_lote(
        self,
        registros: List[Dict[str, str]],
        delay: float = 0.5,
    ) -> List[Dict[str, Any]]:
        """
        Consulta múltiplos números na ABR.

        Args:
            registros: Lista de dicts com 'cpf' e 'msisdn'.
            delay: Delay entre consultas em segundos (rate limiting).

        Returns:
            Lista de resultados.
        """
        resultados = []
        total = len(registros)

        for i, reg in enumerate(registros, 1):
            cpf = reg.get("cpf", "")
            msisdn = reg.get("msisdn", "")

            if not cpf or not msisdn:
                resultados.append({
                    "cpf": cpf,
                    "msisdn": msisdn,
                    "status": "skip",
                    "error": "CPF ou MSISDN vazio",
                })
                continue

            resultado = self.consultar(cpf, msisdn)
            resultado["cpf"] = cpf
            resultado["msisdn"] = msisdn
            resultados.append(resultado)

            if i % 100 == 0:
                logger.info("ABR lote: %d/%d consultados", i, total)

            if delay > 0 and i < total:
                time.sleep(delay)

        logger.info(
            "ABR lote concluído: %d/%d consultados",
            len(resultados),
            total,
        )
        return resultados
