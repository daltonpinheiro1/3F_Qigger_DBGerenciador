"""Validação e correção de endereços via APIs de CEP e geocodificação."""
import logging
import re
import time
from typing import Dict, Optional

import requests

from src.reprocessamento.proxy_manager import ProxyManager

logger = logging.getLogger(__name__)

MAX_RETRIES = 3
BACKOFF_BASE = 1.0


class AddressCorrector:
    """Valida e corrige endereços via APIs de CEP e geocodificação reversa."""

    def __init__(self, proxy_manager: ProxyManager):
        self.proxy_manager = proxy_manager

    def corrigir(self, endereco: Dict[str, str]) -> Dict[str, str]:
        """
        Tenta corrigir endereço inválido.

        Estratégia:
        1. Se CEP presente e válido (8 dígitos), consultar API de CEP
        2. Se falhar, geocodificação reversa
        3. Se tudo falhar, retorna endereço original (integridade)

        Returns:
            Dict com: endereco, numero, complemento, bairro, cidade, uf, cep
        """
        resultado = dict(endereco)

        cep = re.sub(r'\D', '', str(endereco.get('cep', '') or ''))
        if len(cep) == 8:
            dados_cep = self._consultar_cep(cep)
            if dados_cep:
                resultado = self._merge(endereco, dados_cep)
                return resultado

        # Geocodificação reversa como fallback
        partes = [
            str(endereco.get(k, '') or '').strip()
            for k in ('endereco', 'numero', 'bairro', 'cidade', 'uf')
            if str(endereco.get(k, '') or '').strip()
        ]
        if partes:
            endereco_completo = ', '.join(partes)
            dados_geo = self._geocodificar_reverso(endereco_completo)
            if dados_geo:
                resultado = self._merge(endereco, dados_geo)
                return resultado

        return resultado

    def _merge(
        self, original: Dict[str, str], corrigido: Dict[str, str]
    ) -> Dict[str, str]:
        """Merge: corrigido sobrescreve, mas campos vazios mantêm original."""
        merged = dict(original)
        for k, v in corrigido.items():
            if v and str(v).strip():
                merged[k] = str(v).strip()
        return merged

    def _consultar_cep(self, cep: str) -> Optional[Dict[str, str]]:
        """Consulta ViaCEP e BrasilAPI com retry e backoff."""
        apis = [
            (f'https://viacep.com.br/ws/{cep}/json/', self._parse_viacep),
            (f'https://brasilapi.com.br/api/cep/v2/{cep}', self._parse_brasilapi),
        ]

        for url, parser in apis:
            for attempt in range(MAX_RETRIES):
                proxy = self.proxy_manager.get_proxy()
                proxies = proxy if proxy else None
                proxy_url = proxy.get('http') if proxy else None
                try:
                    resp = requests.get(url, proxies=proxies, timeout=10)
                    if proxy_url:
                        self.proxy_manager.report_success(proxy_url)
                    if resp.status_code == 200:
                        data = resp.json()
                        if data and not data.get('erro'):
                            return parser(data)
                except Exception:
                    if proxy_url:
                        self.proxy_manager.report_failure(proxy_url)
                    time.sleep(BACKOFF_BASE * (2 ** attempt))

        return None

    def _geocodificar_reverso(
        self, endereco_completo: str
    ) -> Optional[Dict[str, str]]:
        """Geocodificação via Nominatim com proxy e retry."""
        for attempt in range(MAX_RETRIES):
            proxy = self.proxy_manager.get_proxy()
            proxies = proxy if proxy else None
            proxy_url = proxy.get('http') if proxy else None

            try:
                resp = requests.get(
                    'https://nominatim.openstreetmap.org/search',
                    params={
                        'q': endereco_completo,
                        'format': 'json',
                        'addressdetails': 1,
                        'limit': 1,
                        'countrycodes': 'br',
                    },
                    headers={
                        'User-Agent': '3F-Qigger-Reprocessamento/1.0',
                    },
                    proxies=proxies,
                    timeout=10,
                )
                if proxy_url:
                    self.proxy_manager.report_success(proxy_url)

                if resp.status_code == 200:
                    results = resp.json()
                    if results:
                        return self._parse_nominatim(results[0])
            except Exception:
                if proxy_url:
                    self.proxy_manager.report_failure(proxy_url)
                time.sleep(BACKOFF_BASE * (2 ** attempt))

        return None

    @staticmethod
    def _parse_viacep(data: dict) -> Dict[str, str]:
        """Converte resposta ViaCEP para formato padrão."""
        return {
            'endereco': data.get('logradouro', ''),
            'complemento': data.get('complemento', ''),
            'bairro': data.get('bairro', ''),
            'cidade': data.get('localidade', ''),
            'uf': data.get('uf', ''),
            'cep': data.get('cep', '').replace('-', ''),
        }

    @staticmethod
    def _parse_brasilapi(data: dict) -> Dict[str, str]:
        """Converte resposta BrasilAPI para formato padrão."""
        return {
            'endereco': data.get('street', ''),
            'bairro': data.get('neighborhood', ''),
            'cidade': data.get('city', ''),
            'uf': data.get('state', ''),
            'cep': data.get('cep', '').replace('-', ''),
        }

    @staticmethod
    def _parse_nominatim(result: dict) -> Dict[str, str]:
        """Converte resposta Nominatim para formato padrão."""
        addr = result.get('address', {})
        return {
            'endereco': addr.get('road', ''),
            'bairro': addr.get('suburb', addr.get('neighbourhood', '')),
            'cidade': addr.get('city', addr.get('town', addr.get('village', ''))),
            'uf': addr.get('state', ''),
            'cep': addr.get('postcode', '').replace('-', ''),
        }
