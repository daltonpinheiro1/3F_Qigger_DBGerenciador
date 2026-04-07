"""
Utilitário para exibir barras de progresso durante processamento
Suporta múltiplos modos: tqdm (se disponível) ou barra customizada simples
Opcionalmente registra progresso no logger em intervalos (para arquivo de log)
"""
import sys
import time
import logging
from typing import Optional, Iterator, Any
from datetime import datetime

# Tentar importar tqdm (opcional)
try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False
    tqdm = None


class ProgressBar:
    """
    Barra de progresso que funciona com ou sem tqdm
    """
    
    def __init__(
        self,
        total: int,
        desc: str = "Processando",
        unit: str = "it",
        disable: bool = False,
        logger: Optional[logging.Logger] = None,
        log_interval_pct: float = 10.0
    ):
        """
        Inicializa barra de progresso.
        logger: se informado, registra progresso no log a cada log_interval_pct%%.
        """
        self.total = total
        self.desc = desc
        self.unit = unit
        self.disable = disable
        self.current = 0
        self.start_time = time.time()
        self.last_update = 0
        self.logger = logger
        self.log_interval_pct = max(0.1, float(log_interval_pct))
        self._last_log_pct = -1.0
        
        if not disable and HAS_TQDM:
            self.bar = tqdm(
                total=total,
                desc=desc,
                unit=unit,
                ncols=100,
                bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]'
            )
        else:
            self.bar = None
            if not disable:
                print(f"{desc}: 0/{total} {unit} (0%)", end='', flush=True)
        if self.logger and self.total > 0:
            self.logger.info(f"[Progresso] {desc}: 0/{total} {unit} (0%)")
    
    def update(self, n: int = 1):
        """Atualiza a barra de progresso"""
        self.current += n
        
        if self.bar:
            self.bar.update(n)
        elif not self.disable:
            # Barra customizada simples
            percent = (self.current / self.total * 100) if self.total > 0 else 0
            elapsed = time.time() - self.start_time
            
            # Atualizar apenas a cada 1% ou a cada segundo
            if (percent - self.last_update >= 1.0) or (time.time() - self.last_update >= 1.0):
                rate = self.current / elapsed if elapsed > 0 else 0
                remaining = (self.total - self.current) / rate if rate > 0 else 0
                
                # Limpar linha anterior e escrever nova
                print(f"\r{self.desc}: {self.current}/{self.total} {self.unit} ({percent:.1f}%) "
                      f"[{self._format_time(elapsed)}<{self._format_time(remaining)}, {rate:.1f}{self.unit}/s]",
                      end='', flush=True)
                self.last_update = percent
        
        # Registrar no log em intervalos (ex.: 10%, 20%, ...)
        if self.logger and self.total > 0:
            pct = (self.current / self.total * 100)
            if pct >= self._last_log_pct + self.log_interval_pct or self.current >= self.total:
                self._last_log_pct = (pct // self.log_interval_pct) * self.log_interval_pct
                elapsed = time.time() - self.start_time
                rate = self.current / elapsed if elapsed > 0 else 0
                self.logger.info(
                    f"[Progresso] {self.desc}: {self.current}/{self.total} {self.unit} ({pct:.1f}%) "
                    f"[{self._format_time(elapsed)}, {rate:.1f}{self.unit}/s]"
                )
    
    def set_description(self, desc: str):
        """Atualiza a descrição"""
        self.desc = desc
        if self.bar:
            self.bar.set_description(desc)
    
    def set_postfix(self, **kwargs):
        """Define informações adicionais"""
        if self.bar:
            self.bar.set_postfix(**kwargs)
    
    def close(self):
        """Fecha a barra de progresso"""
        if self.bar:
            self.bar.close()
        elif not self.disable:
            # Completar linha
            elapsed = time.time() - self.start_time
            print(f"\r{self.desc}: {self.current}/{self.total} {self.unit} (100%) "
                  f"[{self._format_time(elapsed)}] - Concluído!                    ")
            print()  # Nova linha
        if self.logger and self.total > 0:
            elapsed = time.time() - self.start_time
            self.logger.info(
                f"[Progresso] {self.desc}: {self.current}/{self.total} {self.unit} (100%) - Concluído em {self._format_time(elapsed)}"
            )
    
    def _format_time(self, seconds: float) -> str:
        """Formata tempo em formato legível"""
        if seconds < 60:
            return f"{seconds:.0f}s"
        elif seconds < 3600:
            return f"{seconds/60:.1f}m"
        else:
            return f"{seconds/3600:.1f}h"
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()


def progress_bar(
    iterable: Iterator[Any],
    total: Optional[int] = None,
    desc: str = "Processando",
    unit: str = "it",
    disable: bool = False
) -> Iterator[Any]:
    """
    Wrapper para criar barra de progresso em um iterável
    
    Args:
        iterable: Iterável a processar
        total: Total de itens (se None, tenta usar len())
        desc: Descrição do processo
        unit: Unidade
        disable: Se True, desabilita a barra
        
    Yields:
        Itens do iterável com barra de progresso
    """
    if total is None:
        try:
            total = len(iterable)
        except TypeError:
            total = None
    
    if HAS_TQDM and not disable:
        yield from tqdm(iterable, total=total, desc=desc, unit=unit, ncols=100)
    else:
        # Sem tqdm, apenas iterar normalmente
        if not disable and total:
            bar = ProgressBar(total, desc, unit, disable)
            try:
                for item in iterable:
                    yield item
                    bar.update(1)
            finally:
                bar.close()
        else:
            yield from iterable


def log_progress(
    current: int,
    total: int,
    desc: str = "Processando",
    update_interval: int = 1
):
    """
    Log simples de progresso (sem barra visual)
    
    Args:
        current: Item atual
        total: Total de itens
        desc: Descrição
        update_interval: Intervalo de atualização (em percentual)
    """
    if total == 0:
        return
    
    percent = (current / total * 100)
    
    # Atualizar apenas a cada X%
    if current == 1 or current == total or (percent % update_interval == 0):
        print(f"{desc}: {current}/{total} ({percent:.1f}%)", flush=True)
