"""
Script de migração do banco de dados antigo para o novo schema normalizado.

Responsável por:
- Criar backup do banco antigo antes da migração
- Migrar dados de 11 tabelas do banco antigo para o novo schema
- Validar contagens e integridade pós-migração
- Reportar discrepâncias e registros ignorados

Mapeamento de migração:
    - base_coverte_prop (38.220) → clientes, propostas, status_venda, portabilidade,
      bluechip, rastreio_entregas, servicos_adicionais, robo_processamento
    - portabilidade_records (48.568) → consulta_siebel
    - portabilidade_processamento (17.439) → portabilidade_tim
    - relatorio_objetos (167.194) → logistica
    - decision_history (503.009) → decisoes
    - triggers_rules (2.521) → regras_decisao
    - templates_wpp (4) → templates_wpp
    - tipo_comunicacao_template (7) → tipo_comunicacao_template
    - unmapped_records (53) → registros_pendentes

Uso:
    python src/database/migrar_banco.py --origem data/portabilidade.db --destino data/portabilidade_v2.db
    python src/database/migrar_banco.py --validar --origem data/portabilidade.db --destino data/portabilidade_v2.db
"""

import argparse
import json
import logging
import re
import sqlite3
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Garantir que o projeto raiz está no sys.path para execução direta
_project_root = str(Path(__file__).resolve().parent.parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from src.database.db_manager_v2 import DatabaseManagerV2
from src.database.importador import Importador

logger = logging.getLogger(__name__)

# Tamanho do lote para inserções em massa
BATCH_SIZE = 500


class MigradorBanco:
    """
    Migrador do banco de dados antigo para o novo schema normalizado.

    Orquestra a leitura do banco antigo, transformação dos dados e inserção
    no novo banco usando INSERT direto com cursor.executemany() para performance.
    """

    def __init__(self):
        """Inicializa o migrador."""
        self._registros_ignorados = 0
        self._registros_migrados = 0
        self._erros = []
        logger.info("MigradorBanco inicializado")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _limpar_valor(v: Any) -> Optional[str]:
        """
        Converte NaN, None, strings vazias e 'nan' para None.

        Args:
            v: Valor a ser limpo.

        Returns:
            String limpa ou None.
        """
        if v is None:
            return None
        s = str(v).strip()
        if s == '' or s.lower() in ('nan', 'none', 'nat'):
            return None
        return s

    @staticmethod
    def _normalizar_cpf(cpf_str: Any) -> Optional[str]:
        """
        Normaliza CPF removendo pontuação.

        Args:
            cpf_str: String do CPF.

        Returns:
            CPF normalizado ou None.
        """
        if cpf_str is None:
            return None
        resultado = re.sub(r'[.\-/\s]', '', str(cpf_str).strip())
        if not resultado or resultado.lower() in ('nan', 'none', ''):
            return None
        return resultado

    @staticmethod
    def _obter_proposta_isize(row: dict, campo_principal: str,
                               campo_fallback: str = None) -> Optional[str]:
        """
        Obtém proposta_isize de uma linha, com fallback opcional.

        Valida que o valor não é CPF (11 dígitos numéricos puros).

        Args:
            row: Dicionário com os dados da linha.
            campo_principal: Nome do campo principal.
            campo_fallback: Nome do campo de fallback (opcional).

        Returns:
            proposta_isize válido ou None.
        """
        valor = row.get(campo_principal)
        if valor is not None:
            valor = str(valor).strip()
            if valor and valor.lower() not in ('nan', 'none', ''):
                if Importador.validar_proposta_isize(valor):
                    return valor

        if campo_fallback:
            valor = row.get(campo_fallback)
            if valor is not None:
                valor = str(valor).strip()
                if valor and valor.lower() not in ('nan', 'none', ''):
                    if Importador.validar_proposta_isize(valor):
                        return valor

        return None

    # ------------------------------------------------------------------
    # Backup
    # ------------------------------------------------------------------

    @staticmethod
    def _criar_backup(db_path: str) -> str:
        """
        Cria backup do banco de dados usando sqlite3 .backup.

        Args:
            db_path: Caminho do banco de dados.

        Returns:
            Caminho do arquivo de backup criado.
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_path = f"{db_path}.backup_{timestamp}"
        logger.info("Criando backup: %s → %s", db_path, backup_path)

        try:
            subprocess.run(
                ['sqlite3', db_path, f'.backup {backup_path}'],
                check=True, capture_output=True, text=True, timeout=300
            )
            logger.info("Backup criado com sucesso: %s", backup_path)
            return backup_path
        except (subprocess.CalledProcessError, FileNotFoundError) as e:
            logger.warning("sqlite3 CLI não disponível, usando fallback Python: %s", e)
            src_conn = sqlite3.connect(db_path)
            dst_conn = sqlite3.connect(backup_path)
            src_conn.backup(dst_conn)
            dst_conn.close()
            src_conn.close()
            logger.info("Backup criado via Python: %s", backup_path)
            return backup_path

    # ------------------------------------------------------------------
    # Gerenciamento de triggers
    # ------------------------------------------------------------------

    @staticmethod
    def _listar_triggers(conn: sqlite3.Connection) -> List[Tuple[str, str]]:
        """
        Lista todos os triggers do banco.

        Args:
            conn: Conexão SQLite.

        Returns:
            Lista de tuplas (nome, sql).
        """
        cursor = conn.cursor()
        cursor.execute("SELECT name, sql FROM sqlite_master WHERE type='trigger'")
        return [(row[0], row[1]) for row in cursor.fetchall()]

    @staticmethod
    def _dropar_triggers(conn: sqlite3.Connection, triggers: List[Tuple[str, str]]):
        """
        Remove triggers temporariamente para performance na migração.

        Args:
            conn: Conexão SQLite.
            triggers: Lista de tuplas (nome, sql) dos triggers.
        """
        cursor = conn.cursor()
        for nome, _ in triggers:
            cursor.execute(f"DROP TRIGGER IF EXISTS {nome}")
        conn.commit()
        logger.info("Removidos %d triggers temporariamente", len(triggers))

    @staticmethod
    def _recriar_triggers(conn: sqlite3.Connection, triggers: List[Tuple[str, str]]):
        """
        Recria triggers após a migração.

        Args:
            conn: Conexão SQLite.
            triggers: Lista de tuplas (nome, sql) dos triggers.
        """
        cursor = conn.cursor()
        for nome, sql in triggers:
            if sql:
                cursor.execute(sql)
        conn.commit()
        logger.info("Recriados %d triggers", len(triggers))

    # ------------------------------------------------------------------
    # Migração principal
    # ------------------------------------------------------------------

    def executar_migracao(self, db_antigo_path: str, db_novo_path: str) -> Dict[str, Any]:
        """
        Executa a migração completa do banco antigo para o novo schema.

        Fluxo:
        1. Backup do banco antigo
        2. Criação do novo banco com DatabaseManagerV2
        3. Criação do lote de migração
        4. Desabilitar triggers no novo banco
        5. Migrar dados na ordem correta
        6. Recriar triggers
        7. Retornar estatísticas

        Args:
            db_antigo_path: Caminho do banco de dados antigo.
            db_novo_path: Caminho do novo banco de dados.

        Returns:
            Dicionário com estatísticas da migração.
        """
        inicio = time.time()
        stats = {
            'backup_path': None,
            'lote_id': None,
            'tabelas_migradas': {},
            'registros_migrados': 0,
            'registros_ignorados': 0,
            'erros': [],
            'duracao_segundos': 0,
        }

        logger.info("=" * 60)
        logger.info("INICIANDO MIGRAÇÃO")
        logger.info("Origem: %s", db_antigo_path)
        logger.info("Destino: %s", db_novo_path)
        logger.info("=" * 60)

        # 1. Backup
        try:
            stats['backup_path'] = self._criar_backup(db_antigo_path)
        except Exception as e:
            logger.error("Falha ao criar backup: %s", e)
            stats['erros'].append(f"Backup falhou: {e}")
            return stats

        # 2. Criar novo banco
        logger.info("Criando novo banco de dados...")
        db_novo = DatabaseManagerV2(db_novo_path)

        # 3. Criar lote de migração
        conn_novo = sqlite3.connect(db_novo_path)
        conn_novo.row_factory = sqlite3.Row
        cursor_novo = conn_novo.cursor()

        # Desabilitar FK temporariamente para migração
        cursor_novo.execute("PRAGMA foreign_keys = OFF")

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        hash_migracao = f"MIGRACAO_V1_{timestamp}"

        cursor_novo.execute(
            """INSERT INTO lotes_importacao (nome_arquivo, tipo_arquivo, hash_sha256, status)
               VALUES (?, 'migracao', ?, 'em_andamento')""",
            (f'migracao_{timestamp}', hash_migracao)
        )
        conn_novo.commit()
        lote_id = cursor_novo.lastrowid
        stats['lote_id'] = lote_id
        logger.info("Lote de migração criado: id=%d", lote_id)

        # 4. Desabilitar triggers para performance
        triggers = self._listar_triggers(conn_novo)
        self._dropar_triggers(conn_novo, triggers)

        # 5. Conectar ao banco antigo
        conn_antigo = sqlite3.connect(db_antigo_path)
        conn_antigo.row_factory = sqlite3.Row

        try:
            # Migrar na ordem correta (respeitando FKs)
            # A. base_coverte_prop → 8 tabelas
            self._migrar_base_coverte_prop(conn_antigo, conn_novo, lote_id, stats)

            # B. portabilidade_records → consulta_siebel
            self._migrar_portabilidade_records(conn_antigo, conn_novo, lote_id, stats)

            # C. portabilidade_processamento → portabilidade_tim
            self._migrar_portabilidade_processamento(conn_antigo, conn_novo, lote_id, stats)

            # D. relatorio_objetos → logistica
            self._migrar_relatorio_objetos(conn_antigo, conn_novo, lote_id, stats)

            # E. decision_history → decisoes
            self._migrar_decision_history(conn_antigo, conn_novo, lote_id, stats)

            # F. triggers_rules → regras_decisao
            self._migrar_triggers_rules(conn_antigo, conn_novo, lote_id, stats)

            # G. templates_wpp
            self._migrar_templates_wpp(conn_antigo, conn_novo, lote_id, stats)

            # H. tipo_comunicacao_template
            self._migrar_tipo_comunicacao_template(conn_antigo, conn_novo, lote_id, stats)

            # I. unmapped_records → registros_pendentes
            self._migrar_unmapped_records(conn_antigo, conn_novo, lote_id, stats)

        except Exception as e:
            logger.error("Erro durante migração: %s", e)
            stats['erros'].append(str(e))
        finally:
            conn_antigo.close()

        # 6. Recriar triggers
        self._recriar_triggers(conn_novo, triggers)

        # Reabilitar FK
        cursor_novo.execute("PRAGMA foreign_keys = ON")

        # Finalizar lote
        total_migrados = sum(v for v in stats['tabelas_migradas'].values())
        stats['registros_migrados'] = total_migrados
        stats['registros_ignorados'] = self._registros_ignorados

        cursor_novo.execute(
            """UPDATE lotes_importacao
               SET qtd_inseridos = ?, qtd_erros = ?, status = 'concluido',
                   finalizado_em = CURRENT_TIMESTAMP
               WHERE id = ?""",
            (total_migrados, len(stats['erros']), lote_id)
        )
        conn_novo.commit()
        conn_novo.close()

        stats['duracao_segundos'] = round(time.time() - inicio, 2)

        logger.info("=" * 60)
        logger.info("MIGRAÇÃO CONCLUÍDA")
        logger.info("Registros migrados: %d", total_migrados)
        logger.info("Registros ignorados: %d", self._registros_ignorados)
        logger.info("Erros: %d", len(stats['erros']))
        logger.info("Duração: %.2f segundos", stats['duracao_segundos'])
        logger.info("=" * 60)

        return stats

    # ------------------------------------------------------------------
    # A. base_coverte_prop → 8 tabelas
    # ------------------------------------------------------------------

    def _migrar_base_coverte_prop(self, conn_antigo, conn_novo, lote_id, stats):
        """
        Migra base_coverte_prop para 8 tabelas normalizadas.

        Tabelas destino: clientes, propostas, status_venda, portabilidade,
        bluechip, rastreio_entregas, servicos_adicionais, robo_processamento.

        Args:
            conn_antigo: Conexão ao banco antigo.
            conn_novo: Conexão ao novo banco.
            lote_id: ID do lote de migração.
            stats: Dicionário de estatísticas.
        """
        logger.info("--- Migrando base_coverte_prop → 8 tabelas ---")
        cursor_antigo = conn_antigo.cursor()
        cursor_novo = conn_novo.cursor()

        cursor_antigo.execute("SELECT COUNT(*) FROM base_coverte_prop")
        total = cursor_antigo.fetchone()[0]
        logger.info("Total de registros em base_coverte_prop: %d", total)

        cpfs_inseridos = set()
        count_clientes = 0
        count_propostas = 0
        count_ignorados = 0

        cursor_antigo.execute("SELECT * FROM base_coverte_prop")
        batch_clientes = []
        batch_propostas = []
        batch_status = []
        batch_port = []
        batch_blue = []
        batch_rastreio = []
        batch_servicos = []
        batch_robo = []
        processados = 0

        for row in cursor_antigo:
            row = dict(row)
            lv = self._limpar_valor

            cpf = self._normalizar_cpf(row.get('cpf'))
            proposta = self._obter_proposta_isize(
                row, 'proposta_isize', 'codigo_externo'
            )
            created_at = lv(row.get('data_importacao')) or lv(row.get('updated_at'))

            if not proposta:
                count_ignorados += 1
                self._registros_ignorados += 1
                if count_ignorados <= 10:
                    logger.warning(
                        "base_coverte_prop: registro sem proposta_isize (cpf=%s)",
                        cpf
                    )
                continue

            # 1. clientes (dedup por CPF)
            if cpf and cpf not in cpfs_inseridos:
                cpfs_inseridos.add(cpf)
                batch_clientes.append((
                    cpf, lv(row.get('cliente_nome')),
                    lv(row.get('nascimento')), lv(row.get('mae')),
                    lv(row.get('endereco')), lv(row.get('numero')),
                    lv(row.get('complemento')), lv(row.get('bairro')),
                    lv(row.get('cidade')), lv(row.get('uf')),
                    lv(row.get('cep')), lv(row.get('ponto_referencia')),
                    lv(row.get('telefone_1')),  # já combinado DDD+tel no antigo
                    None,  # ddd_1 separado não existe no antigo
                    lv(row.get('telefone_2')),
                    None,  # ddd_2
                    lv(row.get('email')), lv(row.get('score')),
                    1, lote_id, created_at,
                ))

            # 2. propostas
            batch_propostas.append((
                proposta, cpf, lv(row.get('data_venda')),
                lv(row.get('produto_vendido')), lv(row.get('plano')),
                lv(row.get('forma_pagamento')), lv(row.get('vencimento')),
                None,  # tipo_chip não existe no antigo
                lv(row.get('conta_online')), lv(row.get('vivo_pay')),
                lv(row.get('app_adicional')), lv(row.get('plataforma')),
                lv(row.get('nome_equipe')), lv(row.get('nome_vendedor')),
                lv(row.get('login_externo')), lv(row.get('nome_supervisor')),
                lv(row.get('matricula_discador')), lv(row.get('avulsa')),
                lv(row.get('sms_previo')), lv(row.get('observacoes')),
                1, lote_id, created_at,
            ))

            # 3. status_venda
            batch_status.append((
                proposta, lv(row.get('status_venda')),
                lv(row.get('motivo_rejeicao_cancelamento')),
                lv(row.get('flag')), lv(row.get('auditoria')),
                lv(row.get('qualidade')), lv(row.get('conectada')),
                lv(row.get('data_conectada')),
                1, lote_id, created_at,
            ))

            # 4. portabilidade
            batch_port.append((
                proposta, lv(row.get('telefone_portado')),
                lv(row.get('numero_linha')), lv(row.get('portabilidade')),
                lv(row.get('complemento_portabilidade')),
                lv(row.get('portabilidade_antecipada')),
                lv(row.get('data_marcacao_port_antecipada')),
                lv(row.get('quem_marcou_port_antecipada')),
                1, lote_id, created_at,
            ))

            # 5. bluechip
            batch_blue.append((
                proposta, lv(row.get('bluechip_status')),
                lv(row.get('bluechip_data_status')),
                lv(row.get('resposta_envio_pedido')),
                lv(row.get('pedido_bluechip')),
                lv(row.get('bluechip_data_enviado')),
                lv(row.get('data_maxima_prevista_entrega')),
                lv(row.get('status_entrega_prevista')),
                lv(row.get('cd_bluechip')),
                lv(row.get('remessa_bluechip')),
                lv(row.get('qtd_remessas')),
                1, lote_id, created_at,
            ))

            # 6. rastreio_entregas
            batch_rastreio.append((
                proposta, lv(row.get('rastreio_correios')),
                lv(row.get('rastreio_loggi')),
                lv(row.get('data_status_correios')),
                lv(row.get('status_correios')),
                lv(row.get('data_status_loggi')),
                lv(row.get('status_loggi')),
                1, lote_id, created_at,
            ))

            # 7. servicos_adicionais
            batch_servicos.append((
                proposta, lv(row.get('vivo_internet')),
                lv(row.get('vivo_tv')), lv(row.get('id_play_vivo')),
                1, lote_id, created_at,
            ))

            # 8. robo_processamento
            batch_robo.append((
                proposta, lv(row.get('robo_inicio_proc')),
                lv(row.get('robo_fim_proc')),
                1, lote_id, created_at,
            ))

            processados += 1

            # Flush em lotes de BATCH_SIZE
            if processados % BATCH_SIZE == 0:
                self._flush_coverte_batches(
                    cursor_novo, conn_novo,
                    batch_clientes, batch_propostas, batch_status,
                    batch_port, batch_blue, batch_rastreio,
                    batch_servicos, batch_robo,
                )
                batch_clientes.clear()
                batch_propostas.clear()
                batch_status.clear()
                batch_port.clear()
                batch_blue.clear()
                batch_rastreio.clear()
                batch_servicos.clear()
                batch_robo.clear()

                if processados % 1000 == 0:
                    logger.info(
                        "base_coverte_prop: %d/%d processados", processados, total
                    )

        # Flush restante
        self._flush_coverte_batches(
            cursor_novo, conn_novo,
            batch_clientes, batch_propostas, batch_status,
            batch_port, batch_blue, batch_rastreio,
            batch_servicos, batch_robo,
        )

        count_propostas = processados
        count_clientes = len(cpfs_inseridos)

        stats['tabelas_migradas']['clientes'] = count_clientes
        stats['tabelas_migradas']['propostas'] = count_propostas
        stats['tabelas_migradas']['status_venda'] = count_propostas
        stats['tabelas_migradas']['portabilidade'] = count_propostas
        stats['tabelas_migradas']['bluechip'] = count_propostas
        stats['tabelas_migradas']['rastreio_entregas'] = count_propostas
        stats['tabelas_migradas']['servicos_adicionais'] = count_propostas
        stats['tabelas_migradas']['robo_processamento'] = count_propostas

        logger.info(
            "base_coverte_prop concluído: clientes=%d, propostas=%d, ignorados=%d",
            count_clientes, count_propostas, count_ignorados,
        )

    def _flush_coverte_batches(self, cursor, conn,
                                batch_clientes, batch_propostas, batch_status,
                                batch_port, batch_blue, batch_rastreio,
                                batch_servicos, batch_robo):
        """
        Insere lotes acumulados das 8 tabelas do coverte_prop.

        Args:
            cursor: Cursor do novo banco.
            conn: Conexão do novo banco.
            batch_*: Listas de tuplas para cada tabela.
        """
        if batch_clientes:
            cursor.executemany(
                """INSERT OR IGNORE INTO clientes
                   (cpf, nome_cliente, data_nascimento, nome_mae, endereco,
                    numero, complemento, bairro, cidade, uf, cep,
                    ponto_referencia, telefone_1, ddd_1, telefone_2, ddd_2,
                    email, score, versao, lote_importacao_id, created_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                batch_clientes,
            )
        if batch_propostas:
            cursor.executemany(
                """INSERT OR IGNORE INTO propostas
                   (proposta_isize, cpf, data_venda, produto, plano,
                    forma_pagamento, vencimento, tipo_chip, conta_online,
                    vivo_pay, app_adicional, plataforma, nome_equipe,
                    nome_vendedor, login_externo, nome_supervisor,
                    matricula_discador, avulsa, sms_previo, observacoes,
                    versao, lote_importacao_id, created_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                batch_propostas,
            )
        if batch_status:
            cursor.executemany(
                """INSERT OR IGNORE INTO status_venda
                   (proposta_isize, status_venda, motivo_rejeicao_cancelamento,
                    flag, auditoria, qualidade, conectada, data_conectada,
                    versao, lote_importacao_id, created_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                batch_status,
            )
        if batch_port:
            cursor.executemany(
                """INSERT OR IGNORE INTO portabilidade
                   (proposta_isize, telefone_portabilidade, numero_linha,
                    portabilidade_status, complemento_portabilidade,
                    portabilidade_antecipada, data_marcacao_port_antecipada,
                    quem_marcou_port_antecipada,
                    versao, lote_importacao_id, created_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                batch_port,
            )
        if batch_blue:
            cursor.executemany(
                """INSERT OR IGNORE INTO bluechip
                   (proposta_isize, bluechip_status, bluechip_data_status,
                    resposta_envio_pedido, pedido_bluechip,
                    bluechip_data_enviado, data_maxima_prevista_entrega,
                    status_entrega_prevista, cd_bluechip, remessa_bluechip,
                    qtd_remessas, versao, lote_importacao_id, created_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                batch_blue,
            )
        if batch_rastreio:
            cursor.executemany(
                """INSERT OR IGNORE INTO rastreio_entregas
                   (proposta_isize, rastreio_correios, rastreio_loggi,
                    data_status_correios, status_correios,
                    data_status_loggi, status_loggi,
                    versao, lote_importacao_id, created_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                batch_rastreio,
            )
        if batch_servicos:
            cursor.executemany(
                """INSERT OR IGNORE INTO servicos_adicionais
                   (proposta_isize, vivo_internet, vivo_tv, id_play_vivo,
                    versao, lote_importacao_id, created_at)
                   VALUES (?,?,?,?,?,?,?)""",
                batch_servicos,
            )
        if batch_robo:
            cursor.executemany(
                """INSERT OR IGNORE INTO robo_processamento
                   (proposta_isize, robo_inicio_proc, robo_fim_proc,
                    versao, lote_importacao_id, created_at)
                   VALUES (?,?,?,?,?,?)""",
                batch_robo,
            )
        conn.commit()

    # ------------------------------------------------------------------
    # B. portabilidade_records → consulta_siebel
    # ------------------------------------------------------------------

    def _migrar_portabilidade_records(self, conn_antigo, conn_novo, lote_id, stats):
        """
        Migra portabilidade_records para consulta_siebel.

        Mapeia codigo_externo → proposta_isize, converte ultimo_bilhete
        (1→'Sim', 0→'Não') e registro_valido (1→'Sim', 0→'Não').

        Args:
            conn_antigo: Conexão ao banco antigo.
            conn_novo: Conexão ao novo banco.
            lote_id: ID do lote de migração.
            stats: Dicionário de estatísticas.
        """
        logger.info("--- Migrando portabilidade_records → consulta_siebel ---")
        cursor_antigo = conn_antigo.cursor()
        cursor_novo = conn_novo.cursor()

        cursor_antigo.execute("SELECT COUNT(*) FROM portabilidade_records")
        total = cursor_antigo.fetchone()[0]
        logger.info("Total de registros em portabilidade_records: %d", total)

        cursor_antigo.execute("SELECT * FROM portabilidade_records")
        batch = []
        processados = 0
        ignorados = 0

        for row in cursor_antigo:
            row = dict(row)
            lv = self._limpar_valor

            proposta = self._obter_proposta_isize(row, 'codigo_externo')
            if not proposta:
                ignorados += 1
                self._registros_ignorados += 1
                continue

            # Converter ultimo_bilhete: 1→'Sim', 0→'Não'
            ub = row.get('ultimo_bilhete')
            ultimo_bilhete_str = None
            if ub is not None:
                ultimo_bilhete_str = 'Sim' if ub == 1 else 'Não'

            # Converter registro_valido: 1→'Sim', 0→'Não'
            rv = row.get('registro_valido')
            registro_valido_str = None
            if rv is not None:
                registro_valido_str = 'Sim' if rv == 1 else 'Não'

            created_at = lv(row.get('created_at'))

            batch.append((
                proposta,
                lv(row.get('cpf')),
                lv(row.get('numero_acesso')),
                lv(row.get('numero_ordem')),
                lv(row.get('codigo_externo')),
                lv(row.get('numero_temporario')),
                lv(row.get('bilhete_temporario')),
                lv(row.get('numero_bilhete')),
                lv(row.get('status_bilhete')),
                lv(row.get('operadora_doadora')),
                lv(row.get('data_portabilidade')),
                lv(row.get('motivo_recusa')),
                lv(row.get('motivo_cancelamento')),
                ultimo_bilhete_str,
                lv(row.get('status_ordem')),
                lv(row.get('preco_ordem')),
                lv(row.get('data_conclusao_ordem')),
                lv(row.get('motivo_nao_consultado')),
                lv(row.get('motivo_nao_cancelado')),
                lv(row.get('motivo_nao_aberto')),
                lv(row.get('motivo_nao_reagendado')),
                lv(row.get('novo_status_bilhete')),
                None,  # nova_data_portabilidade (não existe no antigo)
                lv(row.get('responsavel_processamento')),
                lv(row.get('data_inicial_processamento')),
                lv(row.get('data_final_processamento')),
                registro_valido_str,
                None,  # ajustes_registro (não existe no antigo)
                None,  # numero_acesso_valido (não existe no antigo)
                lv(row.get('ajustes_numero_acesso_trigger')),
                1, lote_id, created_at,
            ))

            processados += 1

            if processados % BATCH_SIZE == 0:
                cursor_novo.executemany(
                    """INSERT OR IGNORE INTO consulta_siebel
                       (proposta_isize, cpf, numero_acesso, numero_ordem,
                        codigo_externo, numero_temporario, bilhete_temporario,
                        numero_bilhete, status_bilhete, operadora_doadora,
                        data_portabilidade, motivo_recusa, motivo_cancelamento,
                        ultimo_bilhete, status_ordem, preco_ordem,
                        data_conclusao_ordem, motivo_nao_consultado,
                        motivo_nao_cancelado, motivo_nao_aberto,
                        motivo_nao_reagendado, novo_status_bilhete,
                        nova_data_portabilidade, responsavel_processamento,
                        data_inicial_processamento, data_final_processamento,
                        registro_valido, ajustes_registro,
                        numero_acesso_valido, ajustes_numero_acesso,
                        versao, lote_importacao_id, created_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    batch,
                )
                conn_novo.commit()
                batch.clear()
                if processados % 1000 == 0:
                    logger.info("portabilidade_records: %d/%d", processados, total)

        # Flush restante
        if batch:
            cursor_novo.executemany(
                """INSERT OR IGNORE INTO consulta_siebel
                   (proposta_isize, cpf, numero_acesso, numero_ordem,
                    codigo_externo, numero_temporario, bilhete_temporario,
                    numero_bilhete, status_bilhete, operadora_doadora,
                    data_portabilidade, motivo_recusa, motivo_cancelamento,
                    ultimo_bilhete, status_ordem, preco_ordem,
                    data_conclusao_ordem, motivo_nao_consultado,
                    motivo_nao_cancelado, motivo_nao_aberto,
                    motivo_nao_reagendado, novo_status_bilhete,
                    nova_data_portabilidade, responsavel_processamento,
                    data_inicial_processamento, data_final_processamento,
                    registro_valido, ajustes_registro,
                    numero_acesso_valido, ajustes_numero_acesso,
                    versao, lote_importacao_id, created_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                batch,
            )
            conn_novo.commit()

        stats['tabelas_migradas']['consulta_siebel'] = processados
        logger.info(
            "portabilidade_records concluído: migrados=%d, ignorados=%d",
            processados, ignorados,
        )

    # ------------------------------------------------------------------
    # C. portabilidade_processamento → portabilidade_tim
    # ------------------------------------------------------------------

    def _migrar_portabilidade_processamento(self, conn_antigo, conn_novo, lote_id, stats):
        """
        Migra portabilidade_processamento para portabilidade_tim.

        Usa id_proposta_isize como chave, com fallback para codigo_externo.

        Args:
            conn_antigo: Conexão ao banco antigo.
            conn_novo: Conexão ao novo banco.
            lote_id: ID do lote de migração.
            stats: Dicionário de estatísticas.
        """
        logger.info("--- Migrando portabilidade_processamento → portabilidade_tim ---")
        cursor_antigo = conn_antigo.cursor()
        cursor_novo = conn_novo.cursor()

        cursor_antigo.execute("SELECT COUNT(*) FROM portabilidade_processamento")
        total = cursor_antigo.fetchone()[0]
        logger.info("Total de registros em portabilidade_processamento: %d", total)

        cursor_antigo.execute("SELECT * FROM portabilidade_processamento")
        batch = []
        processados = 0
        ignorados = 0

        for row in cursor_antigo:
            row = dict(row)
            lv = self._limpar_valor

            proposta = self._obter_proposta_isize(
                row, 'id_proposta_isize', 'codigo_externo'
            )
            if not proposta:
                ignorados += 1
                self._registros_ignorados += 1
                continue

            created_at = lv(row.get('created_at'))

            batch.append((
                proposta,
                lv(row.get('ACESSO')),
                lv(row.get('ACESSO_TEMPORARIO')),
                lv(row.get('DDD')),
                lv(row.get('DATA_SOLICITACAO')),
                lv(row.get('MES_SOLICITACAO')),
                lv(row.get('DATA_ATIVACAO')),
                lv(row.get('MES_ATIVACAO')),
                lv(row.get('DATA_CONCLUSAO')),
                lv(row.get('SKY_CONTRATO')),
                lv(row.get('SKY_CLIENTE')),
                lv(row.get('PROTOCOLO')),
                lv(row.get('OPERADORA_N1')),
                lv(row.get('TIPO_PRE_POS_CONTROLE')),
                lv(row.get('TECNOLOGIA')),
                lv(row.get('VOZ_DADOS')),
                lv(row.get('DOADORA')),
                lv(row.get('RECEPTORA')),
                lv(row.get('TIPO')),
                lv(row.get('STATUS')),
                lv(row.get('TIPO_SEGMENTO_1')),
                lv(row.get('TIPO_SEGMENTO_2')),
                lv(row.get('TIPO_FAMILIA_PLANO')),
                lv(row.get('NIVEL_PLANO')),
                lv(row.get('CANAL_N0')),
                lv(row.get('CANAL_N1')),
                lv(row.get('CANAL_N2')),
                lv(row.get('CANAL_N3')),
                lv(row.get('CANAL_N4')),
                lv(row.get('GRUPO_ECONOMICO')),
                lv(row.get('CUSTCODE')),
                lv(row.get('CPF_CNPJ')),
                lv(row.get('PORTABILIDADE')),
                lv(row.get('MOTIVO_CONFLITO')),
                lv(row.get('MOTIVO_CANCELAMENTO')),
                lv(row.get('SELF_PORTIN')),
                lv(row.get('CANAL_PORTABILIDADE')),
                lv(row.get('TENTATIVAS')),
                None,  # cart_canal_n1 (não existe no antigo)
                None,  # cart_canal_n2 (não existe no antigo)
                1, lote_id, created_at,
            ))

            processados += 1

            if processados % BATCH_SIZE == 0:
                cursor_novo.executemany(
                    """INSERT OR IGNORE INTO portabilidade_tim
                       (proposta_isize, acesso, acesso_temporario, ddd,
                        data_solicitacao, mes_solicitacao, data_ativacao,
                        mes_ativacao, data_conclusao, sky_contrato,
                        sky_cliente, protocolo, operadora_n1,
                        tipo_pre_pos_controle, tecnologia, voz_dados,
                        doadora, receptora, tipo, status,
                        tipo_segmento_1, tipo_segmento_2, tipo_familia_plano,
                        nivel_plano, canal_n0, canal_n1, canal_n2, canal_n3,
                        canal_n4, grupo_economico, custcode, cpf_cnpj,
                        portabilidade, motivo_conflito, motivo_cancelamento,
                        self_portin, canal_portabilidade, tentativas,
                        cart_canal_n1, cart_canal_n2,
                        versao, lote_importacao_id, created_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    batch,
                )
                conn_novo.commit()
                batch.clear()
                if processados % 1000 == 0:
                    logger.info("portabilidade_processamento: %d/%d", processados, total)

        if batch:
            cursor_novo.executemany(
                """INSERT OR IGNORE INTO portabilidade_tim
                   (proposta_isize, acesso, acesso_temporario, ddd,
                    data_solicitacao, mes_solicitacao, data_ativacao,
                    mes_ativacao, data_conclusao, sky_contrato,
                    sky_cliente, protocolo, operadora_n1,
                    tipo_pre_pos_controle, tecnologia, voz_dados,
                    doadora, receptora, tipo, status,
                    tipo_segmento_1, tipo_segmento_2, tipo_familia_plano,
                    nivel_plano, canal_n0, canal_n1, canal_n2, canal_n3,
                    canal_n4, grupo_economico, custcode, cpf_cnpj,
                    portabilidade, motivo_conflito, motivo_cancelamento,
                    self_portin, canal_portabilidade, tentativas,
                    cart_canal_n1, cart_canal_n2,
                    versao, lote_importacao_id, created_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                batch,
            )
            conn_novo.commit()

        stats['tabelas_migradas']['portabilidade_tim'] = processados
        logger.info(
            "portabilidade_processamento concluído: migrados=%d, ignorados=%d",
            processados, ignorados,
        )

    # ------------------------------------------------------------------
    # D. relatorio_objetos → logistica
    # ------------------------------------------------------------------

    def _migrar_relatorio_objetos(self, conn_antigo, conn_novo, lote_id, stats):
        """
        Migra relatorio_objetos para logistica.

        Usa codigo_externo como proposta_isize.

        Args:
            conn_antigo: Conexão ao banco antigo.
            conn_novo: Conexão ao novo banco.
            lote_id: ID do lote de migração.
            stats: Dicionário de estatísticas.
        """
        logger.info("--- Migrando relatorio_objetos → logistica ---")
        cursor_antigo = conn_antigo.cursor()
        cursor_novo = conn_novo.cursor()

        cursor_antigo.execute("SELECT COUNT(*) FROM relatorio_objetos")
        total = cursor_antigo.fetchone()[0]
        logger.info("Total de registros em relatorio_objetos: %d", total)

        cursor_antigo.execute("SELECT * FROM relatorio_objetos")
        batch = []
        processados = 0
        ignorados = 0

        for row in cursor_antigo:
            row = dict(row)
            lv = self._limpar_valor

            proposta = self._obter_proposta_isize(row, 'codigo_externo')
            if not proposta:
                ignorados += 1
                self._registros_ignorados += 1
                continue

            created_at = lv(row.get('created_at'))
            versao_orig = row.get('versao', 1) or 1

            batch.append((
                proposta,
                lv(row.get('nu_pedido')),
                lv(row.get('rastreio')),
                lv(row.get('iccid')),
                lv(row.get('numero_pedido_marketplace')),
                lv(row.get('nota_fiscal_e_serie')),  # nota_fiscal
                None,  # serie_nf (separado no novo, combinado no antigo)
                lv(row.get('data_emissao_nf')),
                lv(row.get('chave_nota_fiscal')),
                lv(row.get('valor_nf')),
                lv(row.get('valor_do_frete')),
                lv(row.get('id_canal_de_venda')),
                lv(row.get('id_warehouse')),
                lv(row.get('id_erp')),
                lv(row.get('id_da_transportadora')),
                lv(row.get('transportadora')),
                lv(row.get('id_servico')),
                lv(row.get('nome_servico')),
                lv(row.get('destinatario')),
                lv(row.get('documento')),
                lv(row.get('email')),
                lv(row.get('telefone')),
                lv(row.get('cidade')),
                lv(row.get('uf')),
                lv(row.get('cep')),
                lv(row.get('data_insercao')),
                lv(row.get('data_primeiro_patch')),
                lv(row.get('data_ultimo_patch_efetuado')),
                lv(row.get('data_postagem')),
                lv(row.get('previsao_entrega')),
                lv(row.get('data_prometida')),
                lv(row.get('prazo_dias_corridos')),
                lv(row.get('prazo_dias_uteis')),
                lv(row.get('prazo_efetivo')),
                lv(row.get('status')),
                lv(row.get('tentativas_de_entrega')),
                lv(row.get('data_entrega')),
                lv(row.get('ultima_ocorrencia')),
                lv(row.get('data_ultima_ocorrencia')),
                lv(row.get('local_ultima_ocorrencia')),
                lv(row.get('cidade_ultima_ocorrencia')),
                lv(row.get('estado_ultima_ocorrencia')),
                lv(row.get('ultima_ocorrencia_cronologica')),
                lv(row.get('motivo_devolucao_ao_remetente')),
                lv(row.get('retorno_ao_fluxo')),
                lv(row.get('protocolo')),
                lv(row.get('motivo_abertura_protocolo')),
                lv(row.get('status_do_protocolo')),
                lv(row.get('reversa')),
                lv(row.get('codigo_de_coleta_postagem')),
                lv(row.get('cd')),
                lv(row.get('dispatch')),
                1,  # versao
                lote_id,
                created_at,
            ))

            processados += 1

            if processados % BATCH_SIZE == 0:
                self._flush_logistica_batch(cursor_novo, conn_novo, batch)
                batch.clear()
                if processados % 1000 == 0:
                    logger.info("relatorio_objetos: %d/%d", processados, total)

        if batch:
            self._flush_logistica_batch(cursor_novo, conn_novo, batch)

        stats['tabelas_migradas']['logistica'] = processados
        logger.info(
            "relatorio_objetos concluído: migrados=%d, ignorados=%d",
            processados, ignorados,
        )

    def _flush_logistica_batch(self, cursor, conn, batch):
        """
        Insere lote de registros na tabela logistica.

        Args:
            cursor: Cursor do novo banco.
            conn: Conexão do novo banco.
            batch: Lista de tuplas.
        """
        if not batch:
            return
        cursor.executemany(
            """INSERT OR IGNORE INTO logistica
               (proposta_isize, nu_pedido, rastreio, iccid,
                numero_pedido_marketplace, nota_fiscal, serie_nf,
                data_emissao_nf, chave_nota_fiscal, valor_nf, valor_frete,
                id_canal_venda, id_warehouse, id_erp, id_transportadora,
                transportadora, id_servico, nome_servico, destinatario,
                documento, email, telefone, cidade, uf, cep,
                data_insercao, data_primeiro_patch, data_ultimo_patch,
                data_postagem, previsao_entrega, data_prometida,
                prazo_dias_corridos, prazo_dias_uteis, prazo_efetivo,
                status, tentativas_entrega, data_entrega,
                ultima_ocorrencia, data_ultima_ocorrencia,
                local_ultima_ocorrencia, cidade_ultima_ocorrencia,
                estado_ultima_ocorrencia, ultima_ocorrencia_cronologica,
                motivo_devolucao, retorno_fluxo, protocolo_logistica,
                motivo_abertura_protocolo, status_protocolo, reversa,
                codigo_coleta_postagem, cd, dispatch,
                versao, lote_importacao_id, created_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            batch,
        )
        conn.commit()

    # ------------------------------------------------------------------
    # E. decision_history → decisoes
    # ------------------------------------------------------------------

    def _migrar_decision_history(self, conn_antigo, conn_novo, lote_id, stats):
        """
        Migra decision_history para decisoes.

        Faz JOIN com portabilidade_records para obter codigo_externo → proposta_isize.

        Args:
            conn_antigo: Conexão ao banco antigo.
            conn_novo: Conexão ao novo banco.
            lote_id: ID do lote de migração.
            stats: Dicionário de estatísticas.
        """
        logger.info("--- Migrando decision_history → decisoes ---")
        cursor_antigo = conn_antigo.cursor()
        cursor_novo = conn_novo.cursor()

        cursor_antigo.execute("SELECT COUNT(*) FROM decision_history")
        total = cursor_antigo.fetchone()[0]
        logger.info("Total de registros em decision_history: %d", total)

        # JOIN com portabilidade_records para obter proposta_isize
        cursor_antigo.execute("""
            SELECT dh.*, pr.codigo_externo
            FROM decision_history dh
            LEFT JOIN portabilidade_records pr ON dh.record_id = pr.id
        """)

        batch = []
        processados = 0
        ignorados = 0

        for row in cursor_antigo:
            row = dict(row)
            lv = self._limpar_valor

            proposta = self._obter_proposta_isize(row, 'codigo_externo')
            if not proposta:
                ignorados += 1
                self._registros_ignorados += 1
                continue

            created_at = lv(row.get('created_at'))

            batch.append((
                proposta,
                row.get('regra_id'),
                lv(row.get('decision')) or 'migrado',
                lv(row.get('o_que_aconteceu')),
                lv(row.get('acao_a_realizar')),
                lv(row.get('details')),
                1, lote_id, created_at,
            ))

            processados += 1

            if processados % BATCH_SIZE == 0:
                cursor_novo.executemany(
                    """INSERT OR IGNORE INTO decisoes
                       (proposta_isize, regra_id, decisao,
                        o_que_aconteceu, acao_a_realizar, detalhes,
                        versao, lote_importacao_id, created_at)
                       VALUES (?,?,?,?,?,?,?,?,?)""",
                    batch,
                )
                conn_novo.commit()
                batch.clear()
                if processados % 1000 == 0:
                    logger.info("decision_history: %d/%d", processados, total)

        if batch:
            cursor_novo.executemany(
                """INSERT OR IGNORE INTO decisoes
                   (proposta_isize, regra_id, decisao,
                    o_que_aconteceu, acao_a_realizar, detalhes,
                    versao, lote_importacao_id, created_at)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                batch,
            )
            conn_novo.commit()

        stats['tabelas_migradas']['decisoes'] = processados
        logger.info(
            "decision_history concluído: migrados=%d, ignorados=%d",
            processados, ignorados,
        )

    # ------------------------------------------------------------------
    # F. triggers_rules → regras_decisao
    # ------------------------------------------------------------------

    def _migrar_triggers_rules(self, conn_antigo, conn_novo, lote_id, stats):
        """
        Migra triggers_rules para regras_decisao (cópia direta).

        Args:
            conn_antigo: Conexão ao banco antigo.
            conn_novo: Conexão ao novo banco.
            lote_id: ID do lote de migração.
            stats: Dicionário de estatísticas.
        """
        logger.info("--- Migrando triggers_rules → regras_decisao ---")
        cursor_antigo = conn_antigo.cursor()
        cursor_novo = conn_novo.cursor()

        cursor_antigo.execute("SELECT COUNT(*) FROM triggers_rules")
        total = cursor_antigo.fetchone()[0]

        cursor_antigo.execute("SELECT * FROM triggers_rules")
        batch = []
        processados = 0

        for row in cursor_antigo:
            row = dict(row)
            lv = self._limpar_valor
            created_at = lv(row.get('created_at'))

            batch.append((
                row.get('regra_id'),
                lv(row.get('status_bilhete')),
                lv(row.get('operadora_doadora')),
                lv(row.get('motivo_recusa')),
                lv(row.get('motivo_cancelamento')),
                row.get('ultimo_bilhete'),
                lv(row.get('motivo_nao_consultado')),
                lv(row.get('novo_status_bilhete')),
                lv(row.get('ajustes_numero_acesso')),
                lv(row.get('o_que_aconteceu')),
                lv(row.get('acao_a_realizar')),
                lv(row.get('tipo_mensagem')),
                lv(row.get('template')),
                row.get('ativo', 1),
                1, lote_id, created_at,
            ))
            processados += 1

        if batch:
            cursor_novo.executemany(
                """INSERT OR IGNORE INTO regras_decisao
                   (regra_id, status_bilhete, operadora_doadora,
                    motivo_recusa, motivo_cancelamento, ultimo_bilhete,
                    motivo_nao_consultado, novo_status_bilhete,
                    ajustes_numero_acesso, o_que_aconteceu, acao_a_realizar,
                    tipo_mensagem, template, ativo,
                    versao, lote_importacao_id, created_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                batch,
            )
            conn_novo.commit()

        stats['tabelas_migradas']['regras_decisao'] = processados
        logger.info("triggers_rules concluído: migrados=%d", processados)

    # ------------------------------------------------------------------
    # G. templates_wpp
    # ------------------------------------------------------------------

    def _migrar_templates_wpp(self, conn_antigo, conn_novo, lote_id, stats):
        """
        Migra templates_wpp (cópia direta).

        Args:
            conn_antigo: Conexão ao banco antigo.
            conn_novo: Conexão ao novo banco.
            lote_id: ID do lote de migração.
            stats: Dicionário de estatísticas.
        """
        logger.info("--- Migrando templates_wpp ---")
        cursor_antigo = conn_antigo.cursor()
        cursor_novo = conn_novo.cursor()

        cursor_antigo.execute("SELECT * FROM templates_wpp")
        processados = 0

        for row in cursor_antigo:
            row = dict(row)
            lv = self._limpar_valor
            created_at = lv(row.get('created_at'))

            cursor_novo.execute(
                """INSERT OR IGNORE INTO templates_wpp
                   (id, nome_modelo, categoria, cabecalho_texto,
                    corpo_mensagem, rodape, tipo_botao, botao_texto,
                    botao_url, variaveis, ativo,
                    versao, lote_importacao_id, created_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    row.get('id'), lv(row.get('nome_modelo')),
                    lv(row.get('categoria')), lv(row.get('cabecalho_texto')),
                    lv(row.get('corpo_mensagem')), lv(row.get('rodape')),
                    lv(row.get('tipo_botao')), lv(row.get('botao_texto')),
                    lv(row.get('botao_url')), lv(row.get('variaveis')),
                    row.get('ativo', 1), 1, lote_id, created_at,
                ),
            )
            processados += 1

        conn_novo.commit()
        stats['tabelas_migradas']['templates_wpp'] = processados
        logger.info("templates_wpp concluído: migrados=%d", processados)

    # ------------------------------------------------------------------
    # H. tipo_comunicacao_template
    # ------------------------------------------------------------------

    def _migrar_tipo_comunicacao_template(self, conn_antigo, conn_novo, lote_id, stats):
        """
        Migra tipo_comunicacao_template (cópia direta).

        Args:
            conn_antigo: Conexão ao banco antigo.
            conn_novo: Conexão ao novo banco.
            lote_id: ID do lote de migração.
            stats: Dicionário de estatísticas.
        """
        logger.info("--- Migrando tipo_comunicacao_template ---")
        cursor_antigo = conn_antigo.cursor()
        cursor_novo = conn_novo.cursor()

        cursor_antigo.execute("SELECT * FROM tipo_comunicacao_template")
        processados = 0

        for row in cursor_antigo:
            row = dict(row)
            lv = self._limpar_valor
            created_at = lv(row.get('created_at'))

            cursor_novo.execute(
                """INSERT OR IGNORE INTO tipo_comunicacao_template
                   (tipo_comunicacao, tipo_descricao, template_id, ativo,
                    versao, lote_importacao_id, created_at)
                   VALUES (?,?,?,?,?,?,?)""",
                (
                    lv(row.get('tipo_comunicacao')),
                    lv(row.get('tipo_descricao')),
                    row.get('template_id'),
                    row.get('ativo', 1),
                    1, lote_id, created_at,
                ),
            )
            processados += 1

        conn_novo.commit()
        stats['tabelas_migradas']['tipo_comunicacao_template'] = processados
        logger.info("tipo_comunicacao_template concluído: migrados=%d", processados)

    # ------------------------------------------------------------------
    # I. unmapped_records → registros_pendentes
    # ------------------------------------------------------------------

    def _migrar_unmapped_records(self, conn_antigo, conn_novo, lote_id, stats):
        """
        Migra unmapped_records para registros_pendentes.

        Converte campos em JSON e define tipo_pendencia='proposta_isize_pendente'.

        Args:
            conn_antigo: Conexão ao banco antigo.
            conn_novo: Conexão ao novo banco.
            lote_id: ID do lote de migração.
            stats: Dicionário de estatísticas.
        """
        logger.info("--- Migrando unmapped_records → registros_pendentes ---")
        cursor_antigo = conn_antigo.cursor()
        cursor_novo = conn_novo.cursor()

        cursor_antigo.execute("SELECT * FROM unmapped_records")
        processados = 0

        for row in cursor_antigo:
            row = dict(row)
            lv = self._limpar_valor

            # Converter todos os campos em JSON
            dados_json = json.dumps(
                {k: str(v) if v is not None else None for k, v in row.items()},
                ensure_ascii=False,
            )
            chave_original = str(row.get('record_id', ''))

            cursor_novo.execute(
                """INSERT INTO registros_pendentes
                   (tabela_origem, dados_json, chave_original,
                    tipo_pendencia, lote_importacao_id)
                   VALUES (?, ?, ?, 'proposta_isize_pendente', ?)""",
                ('unmapped_records', dados_json, chave_original, lote_id),
            )
            processados += 1

        conn_novo.commit()
        stats['tabelas_migradas']['registros_pendentes'] = processados
        logger.info("unmapped_records concluído: migrados=%d", processados)

    # ------------------------------------------------------------------
    # Validação pós-migração (Task 7.2)
    # ------------------------------------------------------------------

    def validar_migracao(self, db_antigo_path: str, db_novo_path: str) -> Dict[str, Any]:
        """
        Valida a migração comparando contagens e verificando integridade.

        Executa:
        1. Comparação de contagens entre tabelas antigas e novas
        2. PRAGMA integrity_check no novo banco
        3. PRAGMA foreign_key_check no novo banco
        4. Relatório de discrepâncias

        Args:
            db_antigo_path: Caminho do banco antigo.
            db_novo_path: Caminho do novo banco.

        Returns:
            Dicionário com resultados da validação:
                - contagens: comparação de registros por tabela
                - integrity_check: resultado do PRAGMA integrity_check
                - foreign_key_check: resultado do PRAGMA foreign_key_check
                - discrepancias: lista de problemas encontrados
                - ok: True se tudo está correto
        """
        logger.info("=" * 60)
        logger.info("VALIDAÇÃO PÓS-MIGRAÇÃO")
        logger.info("=" * 60)

        resultado = {
            'contagens': {},
            'integrity_check': [],
            'foreign_key_check': [],
            'discrepancias': [],
            'ok': True,
        }

        conn_antigo = sqlite3.connect(db_antigo_path)
        conn_novo = sqlite3.connect(db_novo_path)

        try:
            self._validar_contagens(conn_antigo, conn_novo, resultado)
            self._validar_integridade(conn_novo, resultado)
            self._validar_foreign_keys(conn_novo, resultado)
        finally:
            conn_antigo.close()
            conn_novo.close()

        if resultado['discrepancias']:
            resultado['ok'] = False

        logger.info("Validação concluída: ok=%s", resultado['ok'])
        if resultado['discrepancias']:
            for d in resultado['discrepancias']:
                logger.warning("Discrepância: %s", d)

        return resultado

    def _validar_contagens(self, conn_antigo, conn_novo, resultado):
        """
        Compara contagens de registros entre banco antigo e novo.

        Args:
            conn_antigo: Conexão ao banco antigo.
            conn_novo: Conexão ao novo banco.
            resultado: Dicionário de resultados (modificado in-place).
        """
        mapeamentos = [
            ('base_coverte_prop', 'propostas',
             "Propostas migradas de base_coverte_prop"),
            ('portabilidade_records', 'consulta_siebel',
             "Registros migrados de portabilidade_records"),
            ('portabilidade_processamento', 'portabilidade_tim',
             "Registros migrados de portabilidade_processamento"),
            ('relatorio_objetos', 'logistica',
             "Registros migrados de relatorio_objetos"),
            ('decision_history', 'decisoes',
             "Registros migrados de decision_history"),
            ('triggers_rules', 'regras_decisao',
             "Registros migrados de triggers_rules"),
            ('templates_wpp', 'templates_wpp',
             "Registros migrados de templates_wpp"),
            ('tipo_comunicacao_template', 'tipo_comunicacao_template',
             "Registros migrados de tipo_comunicacao_template"),
            ('unmapped_records', 'registros_pendentes',
             "Registros migrados de unmapped_records"),
        ]

        cursor_antigo = conn_antigo.cursor()
        cursor_novo = conn_novo.cursor()

        for tabela_antiga, tabela_nova, descricao in mapeamentos:
            try:
                cursor_antigo.execute(f"SELECT COUNT(*) FROM [{tabela_antiga}]")
                count_antigo = cursor_antigo.fetchone()[0]
            except sqlite3.OperationalError:
                count_antigo = 0

            try:
                cursor_novo.execute(f"SELECT COUNT(*) FROM [{tabela_nova}]")
                count_novo = cursor_novo.fetchone()[0]
            except sqlite3.OperationalError:
                count_novo = 0

            resultado['contagens'][tabela_antiga] = {
                'origem': count_antigo,
                'destino_tabela': tabela_nova,
                'destino': count_novo,
                'descricao': descricao,
            }

            logger.info(
                "  %s → %s: %d → %d",
                tabela_antiga, tabela_nova, count_antigo, count_novo,
            )

            # Registros ignorados são esperados (proposta_isize inválido)
            if count_novo > count_antigo:
                resultado['discrepancias'].append(
                    f"{tabela_nova} tem MAIS registros ({count_novo}) "
                    f"que {tabela_antiga} ({count_antigo})"
                )

    def _validar_integridade(self, conn_novo, resultado):
        """
        Executa PRAGMA integrity_check no novo banco.

        Args:
            conn_novo: Conexão ao novo banco.
            resultado: Dicionário de resultados (modificado in-place).
        """
        cursor = conn_novo.cursor()
        cursor.execute("PRAGMA integrity_check")
        checks = [row[0] for row in cursor.fetchall()]
        resultado['integrity_check'] = checks

        if checks != ['ok']:
            resultado['discrepancias'].append(
                f"integrity_check falhou: {checks[:5]}"
            )
            logger.error("PRAGMA integrity_check FALHOU: %s", checks[:5])
        else:
            logger.info("PRAGMA integrity_check: OK")

    def _validar_foreign_keys(self, conn_novo, resultado):
        """
        Executa PRAGMA foreign_key_check no novo banco.

        Args:
            conn_novo: Conexão ao novo banco.
            resultado: Dicionário de resultados (modificado in-place).
        """
        cursor = conn_novo.cursor()
        cursor.execute("PRAGMA foreign_keys = ON")

        try:
            cursor.execute("PRAGMA foreign_key_check")
            fk_issues = cursor.fetchall()
            resultado['foreign_key_check'] = [
                {'table': r[0], 'rowid': r[1], 'parent': r[2], 'fkid': r[3]}
                for r in fk_issues
            ]

            if fk_issues:
                count = len(fk_issues)
                resultado['discrepancias'].append(
                    f"foreign_key_check encontrou {count} violações"
                )
                logger.warning(
                    "PRAGMA foreign_key_check: %d violações encontradas", count
                )
                # Mostrar primeiras 5
                for fk in fk_issues[:5]:
                    logger.warning(
                        "  FK violação: tabela=%s, rowid=%s, parent=%s",
                        fk[0], fk[1], fk[2],
                    )
            else:
                logger.info("PRAGMA foreign_key_check: OK")
        except sqlite3.OperationalError as e:
            resultado['foreign_key_check'] = [{'error': str(e)}]
            logger.warning("foreign_key_check falhou: %s", e)


def main():
    """
    Ponto de entrada para execução via linha de comando.

    Uso:
        python src/database/migrar_banco.py --origem data/portabilidade.db --destino data/portabilidade_v2.db
        python src/database/migrar_banco.py --validar --origem data/portabilidade.db --destino data/portabilidade_v2.db
    """
    parser = argparse.ArgumentParser(
        description='Migração do banco de dados de portabilidade para o novo schema normalizado.',
    )
    parser.add_argument(
        '--origem', required=True,
        help='Caminho do banco de dados antigo (origem)',
    )
    parser.add_argument(
        '--destino', required=True,
        help='Caminho do novo banco de dados (destino)',
    )
    parser.add_argument(
        '--validar', action='store_true',
        help='Apenas validar migração existente (não executa migração)',
    )
    parser.add_argument(
        '--log-level', default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help='Nível de log (padrão: INFO)',
    )

    args = parser.parse_args()

    # Configurar logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    # Validar caminhos
    if not Path(args.origem).exists():
        logger.error("Banco de origem não encontrado: %s", args.origem)
        return

    migrador = MigradorBanco()

    if args.validar:
        if not Path(args.destino).exists():
            logger.error("Banco de destino não encontrado: %s", args.destino)
            return
        resultado = migrador.validar_migracao(args.origem, args.destino)
        print(json.dumps(resultado, indent=2, ensure_ascii=False, default=str))
    else:
        stats = migrador.executar_migracao(args.origem, args.destino)
        print(json.dumps(stats, indent=2, ensure_ascii=False, default=str))


if __name__ == '__main__':
    main()
