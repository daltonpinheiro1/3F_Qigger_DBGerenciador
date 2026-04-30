"""Orquestrador do reprocessamento de endereços inválidos."""
import hashlib
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Union

import pandas as pd

from src.database.db_manager_v2 import DatabaseManagerV2
from src.database.data_unifier import DataUnifier
from src.reprocessamento.address_corrector import AddressCorrector
from src.reprocessamento.proxy_manager import ProxyManager
from src.reprocessamento.queries_reprocessamento import QueriesReprocessamento
from src.utils.progress_bar import ProgressBar

logger = logging.getLogger(__name__)


class ReprocessadorEndereco:
    """Orquestra o fluxo de reprocessamento de endereços inválidos."""

    def __init__(
        self,
        db_v2_path: str,
        periodo_dias: int = 90,
        diretorio_saida: str = None,
        config_proxies: Union[str, List[str], None] = None,
        workers: int = 4,
    ):
        self.db_v2_path = db_v2_path
        self.periodo_dias = periodo_dias
        self.diretorio_saida = diretorio_saida or str(
            Path(__file__).parent.parent.parent / 'data'
        )
        self.workers = workers

        self.queries = QueriesReprocessamento(db_v2_path)
        self.proxy_manager = ProxyManager(config_proxies)
        self.corrector = AddressCorrector(self.proxy_manager)
        self.db_v2 = DatabaseManagerV2(db_v2_path)
        self.data_unifier = DataUnifier(self.db_v2)

    def executar(self) -> Dict[str, Any]:
        """Fluxo completo: consultar → corrigir → salvar → reimportar."""
        inicio = time.time()
        stats: Dict[str, Any] = {
            'total': 0,
            'corrigidos': 0,
            'mantidos_original': 0,
            'erros': 0,
            'arquivo_saida': None,
            'tempo_execucao': 0,
        }

        # 1. Consultar registros
        registros = self._consultar_registros()
        stats['total'] = len(registros)
        if not registros:
            logger.info("Nenhum registro para reprocessamento")
            return stats

        # 2. Converter para DataFrame
        df = pd.DataFrame(registros)

        # 3. Corrigir endereços
        df = self._corrigir_enderecos(df, stats)

        # 4. Classificar tipo entrega
        df['tipo_entrega'] = df.apply(self._classificar_tipo_entrega, axis=1)

        # 5. Salvar arquivo
        arquivo = self._salvar_arquivo(df)
        stats['arquivo_saida'] = str(arquivo)

        # 6. Reimportar no V2
        self._reimportar_no_v2(arquivo, df)

        stats['tempo_execucao'] = round(time.time() - inicio, 2)
        logger.info(
            "Reprocessamento concluído: total=%d, corrigidos=%d, "
            "mantidos=%d, erros=%d, tempo=%.2fs",
            stats['total'], stats['corrigidos'],
            stats['mantidos_original'], stats['erros'],
            stats['tempo_execucao'],
        )
        return stats

    def _consultar_registros(self) -> List[Dict[str, Any]]:
        """Executa TIM_REPROCESSAMENTO contra V2."""
        return self.queries.buscar_registros_reprocessamento(self.periodo_dias)

    def _corrigir_enderecos(
        self, df: pd.DataFrame, stats: Dict[str, Any]
    ) -> pd.DataFrame:
        """Corrige endereços inválidos via APIs com processamento paralelo.

        Usa ThreadPoolExecutor com ``self.workers`` threads para
        geocodificação concorrente. O ProxyManager já é thread-safe.
        """
        campos_endereco = [
            'endereco', 'numero', 'complemento',
            'bairro', 'cidade', 'uf', 'cep',
        ]
        total = len(df)
        if total == 0:
            return df

        # Lock para atualizar stats e DataFrame de forma segura
        lock = threading.Lock()

        def _corrigir_linha(idx_row):
            idx, row = idx_row
            endereco_original = {
                c: str(row.get(c, '') or '').strip()
                for c in campos_endereco
            }
            try:
                corrigido = self.corrector.corrigir(endereco_original)
                return (idx, corrigido, endereco_original, None)
            except Exception as e:
                return (idx, None, endereco_original, e)

        logger.info(
            "Corrigindo endereços: %d registros com %d workers",
            total, self.workers,
        )

        with ProgressBar(
            total=total,
            desc="Correção de endereços",
            unit="reg",
            logger=logger,
            log_interval_pct=10.0,
        ) as progress:
            with ThreadPoolExecutor(
                max_workers=self.workers
            ) as executor:
                futures = {
                    executor.submit(_corrigir_linha, (idx, row)): idx
                    for idx, row in df.iterrows()
                }
                for future in as_completed(futures):
                    idx, corrigido, original, erro = future.result()
                    with lock:
                        if erro is not None:
                            logger.warning(
                                "Erro ao corrigir endereço para %s: %s",
                                df.at[idx, 'proposta_isize']
                                if 'proposta_isize' in df.columns
                                else idx,
                                erro,
                            )
                            stats['erros'] += 1
                            stats['mantidos_original'] += 1
                        else:
                            for campo in campos_endereco:
                                valor = corrigido.get(campo, '').strip()
                                if valor:
                                    df.at[idx, campo] = valor
                            if corrigido != original:
                                stats['corrigidos'] += 1
                            else:
                                stats['mantidos_original'] += 1
                        progress.update(1)

        return df

    def _classificar_tipo_entrega(self, row: pd.Series) -> str:
        """Express se entrega <= 2 dias da venda, senão Correios."""
        try:
            transportadora = str(row.get('transportadora', '') or '').strip().lower()
            if 'correios' in transportadora:
                return 'Correios'

            data_venda = row.get('data_venda')
            data_entrega = row.get('data_entrega')
            if data_venda and data_entrega:
                dv = pd.to_datetime(data_venda, errors='coerce')
                de = pd.to_datetime(data_entrega, errors='coerce')
                if (
                    dv is not None
                    and de is not None
                    and not pd.isna(dv)
                    and not pd.isna(de)
                ):
                    diff = (de - dv).days
                    if diff <= 2:
                        return 'Express'
        except Exception:
            pass
        return 'Correios'

    def _salvar_arquivo(self, df: pd.DataFrame) -> Path:
        """Salva _pronto_tratamento.xlsx na pasta de saída."""
        pasta = Path(self.diretorio_saida)
        pasta.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        nome = f"reprocessamento_{timestamp}_pronto_tratamento.xlsx"
        caminho = pasta / nome
        df.to_excel(caminho, index=False, engine='openpyxl')
        logger.info("Arquivo salvo: %s (%d registros)", caminho, len(df))
        return caminho

    def _reimportar_no_v2(
        self, arquivo: Path, df: pd.DataFrame
    ) -> Dict[str, Any]:
        """Reimporta dados corrigidos no V2 como nova versão."""
        if df.empty:
            logger.info("Reimportação: DataFrame vazio, nada a reimportar")
            return {'inseridos': 0, 'erros': 0, 'propostas_atualizadas': 0}

        hash_sha256 = hashlib.sha256(str(arquivo).encode()).hexdigest()
        lote_id = self.db_v2.criar_lote(
            arquivo.name, 'reprocessamento', hash_sha256
        )

        inseridos = 0
        erros = 0
        propostas_afetadas: set = set()
        total = len(df)

        with ProgressBar(
            total=total,
            desc="Reimportação V2",
            unit="reg",
            logger=logger,
            log_interval_pct=10.0,
        ) as progress:
            for _, row in df.iterrows():
                try:
                    cpf = str(row.get('cpf', '') or '').strip()
                    if not cpf:
                        progress.update(1)
                        continue

                    self.db_v2.inserir_registro('clientes', {
                        'cpf': cpf,
                        'nome_cliente': str(
                            row.get('nome_cliente', '') or ''
                        ).strip() or None,
                        'endereco': str(
                            row.get('endereco', '') or ''
                        ).strip() or None,
                        'numero': str(
                            row.get('numero', '') or ''
                        ).strip() or None,
                        'complemento': str(
                            row.get('complemento', '') or ''
                        ).strip() or None,
                        'bairro': str(
                            row.get('bairro', '') or ''
                        ).strip() or None,
                        'cidade': str(
                            row.get('cidade', '') or ''
                        ).strip() or None,
                        'uf': str(
                            row.get('uf', '') or ''
                        ).strip() or None,
                        'cep': str(
                            row.get('cep', '') or ''
                        ).strip() or None,
                    }, lote_id)
                    inseridos += 1

                    proposta = str(
                        row.get('proposta_isize', '') or ''
                    ).strip()
                    if proposta:
                        propostas_afetadas.add(proposta)
                except Exception as e:
                    erros += 1
                    logger.warning("Erro ao reimportar registro: %s", e)
                progress.update(1)

        self.db_v2.finalizar_lote(lote_id, inseridos, erros)

        # Atualizar cache para propostas afetadas (bulk)
        if propostas_afetadas:
            logger.info(
                "Atualizando cache para %d propostas afetadas (bulk)...",
                len(propostas_afetadas),
            )
            try:
                with self.db_v2._get_connection() as conn:
                    cursor = conn.cursor()
                    # Deletar apenas as propostas afetadas do cache
                    placeholders = ','.join(['?'] * len(propostas_afetadas))
                    cursor.execute(
                        f"DELETE FROM cache_base_unificada "
                        f"WHERE proposta_isize IN ({placeholders})",
                        list(propostas_afetadas),
                    )
                    # Reinserir do view apenas as afetadas
                    cursor.execute(
                        f"INSERT OR REPLACE INTO cache_base_unificada ("
                        f"  proposta_isize, cpf, nome_cliente, "
                        f"  telefone_portabilidade, numero_linha, "
                        f"  numero_ordem, data_venda, produto, plano, "
                        f"  status_venda, portabilidade_status, "
                        f"  status_tim, data_ativacao_tim, "
                        f"  status_logistica, rastreio, data_entrega, "
                        f"  data_gross, classificacao_cr, "
                        f"  resultado_gross, status_pedido, "
                        f"  detalhe_status, status_bilhete, "
                        f"  status_ordem, bluechip_status, "
                        f"  pedido_bluechip, regra_id, "
                        f"  acao_a_realizar, tipo_mensagem, "
                        f"  atualizado_em"
                        f") SELECT "
                        f"  proposta_isize, cpf, nome_cliente, "
                        f"  telefone_portabilidade, numero_linha, "
                        f"  numero_ordem, data_venda, produto, plano, "
                        f"  status_venda, portabilidade_status, "
                        f"  status_tim, data_ativacao_tim, "
                        f"  status_logistica, rastreio_logistica, "
                        f"  data_entrega, data_gross, classificacao_cr, "
                        f"  resultado_gross, status_pedido, "
                        f"  detalhe_status, status_bilhete, "
                        f"  status_ordem, bluechip_status, "
                        f"  pedido_bluechip, regra_id, "
                        f"  acao_a_realizar, tipo_mensagem, "
                        f"  CURRENT_TIMESTAMP "
                        f"FROM vw_base_unificada "
                        f"WHERE proposta_isize IN ({placeholders}) "
                        f"GROUP BY proposta_isize",
                        list(propostas_afetadas),
                    )
                    conn.commit()
                logger.info(
                    "Cache atualizado (bulk) para %d propostas",
                    len(propostas_afetadas),
                )
            except Exception as e:
                logger.warning(
                    "Erro no bulk cache update, tentando individual: %s", e
                )
                # Fallback individual
                for proposta in propostas_afetadas:
                    try:
                        self.data_unifier.atualizar_cache(proposta)
                    except Exception:
                        pass

        logger.info(
            "Reimportação: inseridos=%d, erros=%d, "
            "cache atualizado para %d propostas",
            inseridos, erros, len(propostas_afetadas),
        )
        return {
            'inseridos': inseridos,
            'erros': erros,
            'propostas_atualizadas': len(propostas_afetadas),
        }
