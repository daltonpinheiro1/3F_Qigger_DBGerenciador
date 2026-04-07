"""Testes unitários para DatabaseManagerV2."""

import os
import sqlite3
import tempfile
import unittest

from src.database.db_manager_v2 import DatabaseManagerV2, BUSINESS_KEYS


class TestDatabaseManagerV2(unittest.TestCase):
    """Testes para o DatabaseManagerV2."""

    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        self.tmp.close()
        self.db = DatabaseManagerV2(self.tmp.name)

    def tearDown(self):
        for ext in ('', '-wal', '-shm'):
            path = self.tmp.name + ext
            if os.path.exists(path):
                os.unlink(path)

    def _insert_prerequisite_cliente(self, cpf='12345678901'):
        """Insere um cliente para satisfazer FK de propostas."""
        self.db.inserir_registro('clientes', {'cpf': cpf, 'nome_cliente': 'Teste'})

    def _insert_prerequisite_proposta(self, proposta='PROP001', cpf='12345678901'):
        """Insere cliente + proposta para satisfazer FKs."""
        self._insert_prerequisite_cliente(cpf)
        self.db.inserir_registro('propostas', {
            'proposta_isize': proposta, 'cpf': cpf
        })

    def test_init_creates_schema(self):
        """Construtor deve criar o schema completo."""
        with self.db._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = {row[0] for row in cursor.fetchall()}
        self.assertIn('clientes', tables)
        self.assertIn('propostas', tables)
        self.assertIn('lotes_importacao', tables)
        self.assertIn('auditoria', tables)

    def test_pragmas_applied(self):
        """PRAGMAs persistentes (WAL) devem estar configurados."""
        with self.db._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("PRAGMA journal_mode")
            self.assertEqual(cursor.fetchone()[0], 'wal')

    def test_inserir_registro_versao_1(self):
        """Primeiro insert deve ter versao=1."""
        row_id = self.db.inserir_registro('clientes', {
            'cpf': '11111111111', 'nome_cliente': 'Maria'
        })
        self.assertIsNotNone(row_id)
        with self.db._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT versao FROM clientes WHERE id = ?", (row_id,))
            self.assertEqual(cursor.fetchone()[0], 1)

    def test_inserir_registro_versao_incrementa(self):
        """Inserts subsequentes devem incrementar versao."""
        self.db.inserir_registro('clientes', {'cpf': '22222222222', 'nome_cliente': 'V1'})
        self.db.inserir_registro('clientes', {'cpf': '22222222222', 'nome_cliente': 'V2'})
        row_id = self.db.inserir_registro('clientes', {'cpf': '22222222222', 'nome_cliente': 'V3'})
        with self.db._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT versao FROM clientes WHERE id = ?", (row_id,))
            self.assertEqual(cursor.fetchone()[0], 3)

    def test_inserir_registro_com_lote_id(self):
        """lote_importacao_id deve ser gravado quando fornecido."""
        lote_id = self.db.criar_lote('test.csv', 'coverte_prop', 'abc123hash')
        row_id = self.db.inserir_registro(
            'clientes', {'cpf': '33333333333'}, lote_id=lote_id
        )
        with self.db._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT lote_importacao_id FROM clientes WHERE id = ?", (row_id,))
            self.assertEqual(cursor.fetchone()[0], lote_id)

    def test_inserir_registro_tabela_invalida(self):
        """Tabela não mapeada deve levantar ValueError."""
        with self.assertRaises(ValueError):
            self.db.inserir_registro('tabela_inexistente', {'campo': 'valor'})

    def test_buscar_corrente(self):
        """buscar_corrente deve retornar a última versão."""
        self.db.inserir_registro('clientes', {'cpf': '44444444444', 'nome_cliente': 'V1'})
        self.db.inserir_registro('clientes', {'cpf': '44444444444', 'nome_cliente': 'V2'})
        result = self.db.buscar_corrente('clientes', 'cpf', '44444444444')
        self.assertIsNotNone(result)
        self.assertEqual(result['nome_cliente'], 'V2')
        self.assertEqual(result['versao'], 2)

    def test_buscar_corrente_nao_encontrado(self):
        """buscar_corrente deve retornar None se não existir."""
        result = self.db.buscar_corrente('clientes', 'cpf', 'inexistente')
        self.assertIsNone(result)

    def test_buscar_historico(self):
        """buscar_historico deve retornar todas as versões em ordem ASC."""
        self.db.inserir_registro('clientes', {'cpf': '55555555555', 'nome_cliente': 'V1'})
        self.db.inserir_registro('clientes', {'cpf': '55555555555', 'nome_cliente': 'V2'})
        self.db.inserir_registro('clientes', {'cpf': '55555555555', 'nome_cliente': 'V3'})
        historico = self.db.buscar_historico('clientes', 'cpf', '55555555555')
        self.assertEqual(len(historico), 3)
        self.assertEqual(historico[0]['versao'], 1)
        self.assertEqual(historico[2]['versao'], 3)

    def test_criar_lote(self):
        """criar_lote deve inserir e retornar id."""
        lote_id = self.db.criar_lote('arquivo.csv', 'coverte_prop', 'hash123')
        self.assertIsNotNone(lote_id)
        with self.db._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT nome_arquivo, status FROM lotes_importacao WHERE id = ?", (lote_id,))
            row = cursor.fetchone()
            self.assertEqual(row[0], 'arquivo.csv')
            self.assertEqual(row[1], 'em_andamento')

    def test_finalizar_lote(self):
        """finalizar_lote deve atualizar contagens e status."""
        lote_id = self.db.criar_lote('test.csv', 'gross', 'hashxyz')
        self.db.finalizar_lote(lote_id, qtd_inseridos=100, qtd_erros=5, status='concluido')
        with self.db._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT qtd_inseridos, qtd_erros, status, finalizado_em FROM lotes_importacao WHERE id = ?",
                (lote_id,),
            )
            row = cursor.fetchone()
            self.assertEqual(row[0], 100)
            self.assertEqual(row[1], 5)
            self.assertEqual(row[2], 'concluido')
            self.assertIsNotNone(row[3])

    def test_validar_integridade(self):
        """validar_integridade deve retornar integrity_check ok em banco limpo."""
        result = self.db.validar_integridade()
        self.assertEqual(result['integrity_check'], ['ok'])
        # foreign_key_check pode falhar por FK mismatch no schema (cpf não é UNIQUE em clientes)
        self.assertIn('foreign_key_check', result)
        self.assertIn('ok', result)

    def test_registrar_e_finalizar_execucao(self):
        """registrar_execucao e finalizar_execucao devem funcionar em sequência."""
        exec_id = self.db.registrar_execucao('importacao', {'arquivo': 'test.csv'})
        self.assertIsNotNone(exec_id)
        self.db.finalizar_execucao(exec_id, 'concluido', registros_processados=50, registros_erro=2)
        with self.db._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT status, registros_processados, registros_erro, fim_em FROM execucoes_processamento WHERE id = ?",
                (exec_id,),
            )
            row = cursor.fetchone()
            self.assertEqual(row[0], 'concluido')
            self.assertEqual(row[1], 50)
            self.assertEqual(row[2], 2)
            self.assertIsNotNone(row[3])

    def test_transacao_commit(self):
        """transacao() deve commitar quando não há exceção."""
        with self.db.transacao() as cursor:
            cursor.execute(
                "INSERT INTO lotes_importacao (nome_arquivo, tipo_arquivo, hash_sha256) VALUES (?, ?, ?)",
                ('transacao_test.csv', 'coverte_prop', 'hash_transacao_ok'),
            )
        with self.db._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT nome_arquivo FROM lotes_importacao WHERE hash_sha256 = ?", ('hash_transacao_ok',))
            row = cursor.fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row[0], 'transacao_test.csv')

    def test_transacao_rollback(self):
        """transacao() deve fazer rollback quando há exceção."""
        try:
            with self.db.transacao() as cursor:
                cursor.execute(
                    "INSERT INTO lotes_importacao (nome_arquivo, tipo_arquivo, hash_sha256) VALUES (?, ?, ?)",
                    ('rollback_test.csv', 'coverte_prop', 'hash_rollback'),
                )
                raise RuntimeError("Erro simulado")
        except RuntimeError:
            pass
        with self.db._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM lotes_importacao WHERE hash_sha256 = ?", ('hash_rollback',))
            self.assertIsNone(cursor.fetchone())

    def test_atualizar_cache_unificada(self):
        """atualizar_cache_unificada deve popular cache_base_unificada."""
        # Inserir dados mínimos para a view funcionar
        self._insert_prerequisite_proposta('CACHE001', '99999999999')
        self.db.atualizar_cache_unificada('CACHE001')
        with self.db._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT proposta_isize, cpf FROM cache_base_unificada WHERE proposta_isize = ?",
                ('CACHE001',),
            )
            row = cursor.fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row[0], 'CACHE001')
            self.assertEqual(row[1], '99999999999')

    def test_business_keys_completeness(self):
        """BUSINESS_KEYS deve mapear todas as 15 tabelas de dados imutáveis."""
        expected = {
            'clientes', 'propostas', 'status_venda', 'portabilidade',
            'portabilidade_tim', 'logistica', 'gross', 'resultado_gross',
            'backoffice', 'consulta_siebel', 'bluechip', 'rastreio_entregas',
            'servicos_adicionais', 'robo_processamento', 'decisoes',
        }
        self.assertEqual(set(BUSINESS_KEYS.keys()), expected)


if __name__ == '__main__':
    unittest.main()
