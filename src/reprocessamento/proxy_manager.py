"""Gerenciador de pool de proxies com rotação dinâmica."""
import logging
import threading
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import requests

logger = logging.getLogger(__name__)

MAX_CONSECUTIVE_FAILURES = 3


@dataclass
class ProxyInfo:
    """Informações de um proxy no pool."""

    url: str
    failures: int = 0
    successes: int = 0
    active: bool = True
    last_used: Optional[datetime] = None


class ProxyManager:
    """Gerenciador de pool de proxies com rotação round-robin."""

    def __init__(self, config: Union[str, List[str], None] = None):
        """
        Args:
            config: Caminho para arquivo de proxies (um por linha)
                    ou lista de URLs de proxy. None = sem proxies.
        """
        self._pool: List[ProxyInfo] = []
        self._lock = threading.Lock()
        self._current_index = 0
        self._stats: Dict[str, int] = {
            'total_requests': 0,
            'successes': 0,
            'failures': 0,
        }

        if config is not None:
            self._load_proxies(config)

        logger.info("ProxyManager inicializado com %d proxies", len(self._pool))

    def _load_proxies(self, config: Union[str, List[str]]) -> None:
        """Carrega proxies de arquivo ou lista."""
        urls: List[str] = []

        if isinstance(config, str):
            path = Path(config)
            if path.is_file():
                with open(path, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith('#'):
                            urls.append(line)
                logger.info("Carregados %d proxies de %s", len(urls), config)
            else:
                logger.warning("Arquivo de proxies não encontrado: %s", config)
        elif isinstance(config, list):
            urls = [u.strip() for u in config if u and u.strip()]

        for url in urls:
            self._pool.append(ProxyInfo(url=url))

    def get_proxy(self) -> Optional[Dict[str, str]]:
        """Retorna próximo proxy ativo em round-robin."""
        with self._lock:
            active = [p for p in self._pool if p.active]
            if not active:
                return None

            idx = self._current_index % len(active)
            proxy = active[idx]
            proxy.last_used = datetime.now()
            self._current_index = idx + 1
            self._stats['total_requests'] += 1

            return {'http': proxy.url, 'https': proxy.url}

    def report_success(self, proxy_url: str) -> None:
        """Registra sucesso para o proxy, reseta falhas consecutivas."""
        with self._lock:
            for p in self._pool:
                if p.url == proxy_url:
                    p.successes += 1
                    p.failures = 0
                    self._stats['successes'] += 1
                    return

    def report_failure(self, proxy_url: str) -> None:
        """Registra falha. Desativa proxy após MAX_CONSECUTIVE_FAILURES."""
        with self._lock:
            for p in self._pool:
                if p.url == proxy_url:
                    p.failures += 1
                    self._stats['failures'] += 1
                    if p.failures >= MAX_CONSECUTIVE_FAILURES:
                        p.active = False
                        logger.warning(
                            "Proxy desativado após %d falhas: %s",
                            p.failures, proxy_url,
                        )
                    return

    def validate_all(self) -> int:
        """Testa cada proxy com GET a httpbin.org/ip. Retorna qtd válidos."""
        valid = 0
        for proxy in self._pool:
            try:
                resp = requests.get(
                    'https://httpbin.org/ip',
                    proxies={'http': proxy.url, 'https': proxy.url},
                    timeout=5,
                )
                if resp.status_code == 200:
                    with self._lock:
                        proxy.active = True
                    valid += 1
                else:
                    with self._lock:
                        proxy.active = False
            except Exception:
                with self._lock:
                    proxy.active = False

        logger.info("Validação de proxies: %d/%d válidos", valid, len(self._pool))
        return valid

    @property
    def metrics(self) -> Dict[str, Any]:
        """Retorna métricas do pool."""
        with self._lock:
            active = sum(1 for p in self._pool if p.active)
            inactive = sum(1 for p in self._pool if not p.active)
            return {
                'total_proxies': len(self._pool),
                'active': active,
                'inactive': inactive,
                'total_requests': self._stats['total_requests'],
                'successes': self._stats['successes'],
                'failures': self._stats['failures'],
            }
