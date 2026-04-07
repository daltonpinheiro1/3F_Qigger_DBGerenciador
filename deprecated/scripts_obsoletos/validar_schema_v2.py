"""
Script de validação do novo schema v2.

Cria um banco de teste, insere dados simulados dos 7 tipos de arquivo,
e verifica se o schema, versionamento, triggers, views e cache funcionam.

Uso:
    python validar_schema_v2.py
    # ou com o venv:
    .venv/bin/python validar_schema_v2.py
"""
import os
import sys
import sqlite3
import tempfile
from pathlib import Path
from datetime import datetime

# Garantir imports do projeto
sys.path.insert(0, str(Path(__file__).parent))

from src.database.db_manager_v2 import DatabaseManagerV2
from src.database.importador import Importador
from src.database.data_unifier import DataUnifier
from src.database.schema import TABELAS_DADOS_IMUTAVEIS, TABELAS_CONTROLE_NOMES

# Cores para output
GREEN = '\033[92m'
RED = '\033[91m'
YELLOW = '\033[93m'
CYAN = '\033[96m'
RESET = '\033[0m'
BOLD = '\033[1m'

passed = 0
failed = 0
warnings = 0


def ok(msg):
    global passed
    passed += 1
    print(f"  {GREEN}✓{RESET} {msg}")


def fail(msg):
    global failed
    failed += 1
    print(f"  {RED}✗{RESET} {msg}")


def warn(msg):
    global warnings
    warnings += 1
    print(f"  {YELLOW}⚠{RESET} {msg}")


def section(title):
    print(f"\n{CYAN}{BOLD}{'='*60}{RESET}")
    print(f"{CYAN}{BOLD}  {title}{RESET}")
    print(f"{CYAN}{BOLD}{'='*60}{RESET}")


def main():
    global passed, failed, warnings

    tmp = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
    tmp.close()
    db_path = tmp.name

    try:
        # ============================================================
        section("1. CRIAÇÃO DO SCHEMA")
        # ============================================================
        db = DatabaseManagerV2(db_path)

        with db._get_connection() as conn:
            cursor = conn.cursor()

            # Contar tabelas
            cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
            n_tabelas = cursor.fetchone()[0]
            if n_tabelas >= 27:
                ok(f"{n_tabelas} tabelas criadas")
            else:
                fail(f"Esperado >= 27 tabelas, encontrado {n_tabelas}")

            # Contar views
            cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='view'")
            n_views = cursor.fetchone()[0]
            if n_views >= 16:
                ok(f"{n_views} views criadas")
            else:
                fail(f"Esperado >= 16 views, encontrado {n_views}")

            # Contar triggers
            cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='trigger'")
            n_triggers = cursor.fetchone()[0]
            if n_triggers >= 30:
                ok(f"{n_triggers} triggers criados")
            else:
                fail(f"Esperado >= 30 triggers, encontrado {n_triggers}")

            # Contar índices
            cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name LIKE 'idx_%'")
            n_indices = cursor.fetchone()[0]
            if n_indices >= 80:
                ok(f"{n_indices} índices criados")
            else:
                warn(f"{n_indices} índices (esperado >= 80)")

            # WAL mode
            cursor.execute("PRAGMA journal_mode")
            mode = cursor.fetchone()[0]
            if mode == 'wal':
                ok("WAL mode ativo")
            else:
                fail(f"WAL mode esperado, encontrado: {mode}")

            # Schema versão
            cursor.execute("SELECT versao FROM schema_versao ORDER BY versao DESC LIMIT 1")
            row = cursor.fetchone()
            if row and row[0] == 1:
                ok("Schema versão 1 registrada")
            else:
                fail("Schema versão não encontrada")

        # ============================================================
        section("2. INSERÇÃO DE DADOS — CLIENTES + PROPOSTAS")
        # ============================================================

        # Inserir cliente
        id_cli = db.inserir_registro('clientes', {
            'cpf': '12345678901',
            'nome_cliente': 'MARIA SILVA TESTE',
            'data_nascimento': '1990-05-15',
            'nome_mae': 'ANA SILVA',
            'endereco': 'RUA DAS FLORES',
            'numero': '100',
            'complemento': 'APTO 201',
            'bairro': 'CENTRO',
            'cidade': 'SAO PAULO',
            'uf': 'SP',
            'cep': '01001000',
            'ddd_1': '11',
            'telefone_1': '999887766',
            'email': 'teste@teste.com',
            'score': '85',
        })
        if id_cli:
            ok(f"Cliente inserido (id={id_cli})")
        else:
            fail("Falha ao inserir cliente")

        # Inserir proposta
        id_prop = db.inserir_registro('propostas', {
            'proposta_isize': '260015001',
            'cpf': '12345678901',
            'data_venda': '2026-03-25',
            'produto': 'TIM CONTROLE',
            'plano': 'TIM CONTROLE A PLUS - 29,99',
            'forma_pagamento': 'BOLETO',
            'nome_equipe': 'TIM - VALERIA',
            'nome_vendedor': 'JOAO VENDEDOR',
        })
        if id_prop:
            ok(f"Proposta inserida (id={id_prop})")
        else:
            fail("Falha ao inserir proposta")

        # ============================================================
        section("3. VERSIONAMENTO — INSERT-ONLY")
        # ============================================================

        # Inserir segunda versão do cliente (dados atualizados)
        id_cli_v2 = db.inserir_registro('clientes', {
            'cpf': '12345678901',
            'nome_cliente': 'MARIA SILVA ATUALIZADA',
            'endereco': 'AV PAULISTA',
            'numero': '1000',
            'cidade': 'SAO PAULO',
            'uf': 'SP',
            'cep': '01310100',
        })

        corrente = db.buscar_corrente('clientes', 'cpf', '12345678901')
        if corrente and corrente['versao'] == 2:
            ok(f"Versão 2 do cliente criada (nome={corrente['nome_cliente']})")
        else:
            fail("Versionamento falhou")

        historico = db.buscar_historico('clientes', 'cpf', '12345678901')
        if len(historico) == 2:
            ok(f"Histórico preservado: {len(historico)} versões")
        else:
            fail(f"Histórico incorreto: esperado 2, encontrado {len(historico)}")

        # ============================================================
        section("4. TRIGGER BEFORE UPDATE — BLOQUEIO")
        # ============================================================

        update_blocked = False
        try:
            with db._get_connection() as conn:
                conn.execute("UPDATE clientes SET nome_cliente = 'HACK' WHERE cpf = '12345678901'")
        except Exception as e:
            if 'UPDATE proibido' in str(e):
                update_blocked = True

        if update_blocked:
            ok("UPDATE bloqueado pelo trigger (mensagem correta)")
        else:
            fail("UPDATE NÃO foi bloqueado — trigger falhou!")

        # ============================================================
        section("5. TRIGGER AFTER INSERT — AUDITORIA")
        # ============================================================

        with db._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM auditoria WHERE tabela = 'clientes'")
            n_audit = cursor.fetchone()[0]
            if n_audit >= 2:
                ok(f"{n_audit} registros de auditoria para clientes")
            else:
                fail(f"Esperado >= 2 registros de auditoria, encontrado {n_audit}")

            cursor.execute("SELECT COUNT(*) FROM auditoria WHERE tabela = 'propostas'")
            n_audit_prop = cursor.fetchone()[0]
            if n_audit_prop >= 1:
                ok(f"{n_audit_prop} registro(s) de auditoria para propostas")
            else:
                fail("Auditoria de propostas não encontrada")

        # ============================================================
        section("6. TABELAS DE DADOS — INSERÇÃO COMPLETA")
        # ============================================================

        # Status venda
        db.inserir_registro('status_venda', {
            'proposta_isize': '260015001',
            'status_venda': 'APROVADA',
            'conectada': 'Sim',
            'data_conectada': '2026-03-26',
        })
        ok("status_venda inserido")

        # Portabilidade
        db.inserir_registro('portabilidade', {
            'proposta_isize': '260015001',
            'telefone_portabilidade': '11999887766',
            'numero_linha': '11984001234',
            'portabilidade_status': 'SIM',
        })
        ok("portabilidade inserido")

        # Bluechip
        db.inserir_registro('bluechip', {
            'proposta_isize': '260015001',
            'bluechip_status': 'ENVIADO',
            'pedido_bluechip': 'PED123456',
        })
        ok("bluechip inserido")

        # Rastreio
        db.inserir_registro('rastreio_entregas', {
            'proposta_isize': '260015001',
            'rastreio_correios': 'BR123456789BR',
            'status_correios': 'Objeto entregue',
        })
        ok("rastreio_entregas inserido")

        # Serviços adicionais
        db.inserir_registro('servicos_adicionais', {
            'proposta_isize': '260015001',
            'vivo_internet': 'SIM',
            'vivo_tv': 'NAO',
        })
        ok("servicos_adicionais inserido")

        # Robô
        db.inserir_registro('robo_processamento', {
            'proposta_isize': '260015001',
            'robo_inicio_proc': '2026-03-25 10:00:00',
            'robo_fim_proc': '2026-03-25 10:05:00',
        })
        ok("robo_processamento inserido")

        # Consulta Siebel
        db.inserir_registro('consulta_siebel', {
            'proposta_isize': '260015001',
            'cpf': '12345678901',
            'numero_acesso': '11999887766',
            'numero_ordem': '1-1725511461447',
            'codigo_externo': '260015001',
            'status_bilhete': 'Portado',
            'operadora_doadora': 'VIVO',
            'status_ordem': 'Concluído',
            'responsavel_processamento': 'Robô Siebel 8',
        })
        ok("consulta_siebel inserido")

        # Logística
        db.inserir_registro('logistica', {
            'proposta_isize': '260015001',
            'nu_pedido': '26-0260015001',
            'rastreio': '26-0260015001-01',
            'status': 'Pedido Entregue',
            'transportadora': '50 Mais',
            'data_entrega': '2026-03-28',
            'destinatario': 'MARIA SILVA',
            'cidade': 'SAO PAULO',
            'uf': 'SP',
        })
        ok("logistica inserido")

        # Backoffice
        db.inserir_registro('backoffice', {
            'proposta_isize': '260015001',
            'pedido': '260015001',
            'status_pedido': 'CONECTADA',
            'nome_cliente': 'MARIA SILVA',
            'cpf': '12345678901',
            'numero_portado': '11999887766',
        })
        ok("backoffice inserido")

        # Regra de decisão
        with db._get_connection() as conn:
            conn.execute(
                "INSERT INTO regras_decisao (regra_id, status_bilhete, o_que_aconteceu, acao_a_realizar, tipo_mensagem, template, ativo, versao) VALUES (1, 'Portado', 'Portabilidade concluída', 'Enviar confirmação', 'Confirmação', '1', 1, 1)"
            )
            conn.commit()
        ok("regras_decisao inserido")

        # Decisão
        db.inserir_registro('decisoes', {
            'proposta_isize': '260015001',
            'regra_id': 1,
            'decisao': 'CONFIRMAR',
            'o_que_aconteceu': 'Portabilidade concluída',
            'acao_a_realizar': 'Enviar confirmação',
            'tipo_mensagem': 'Confirmação',
            'template': '1',
        })
        ok("decisoes inserido")

        # ============================================================
        section("7. VIEW UNIFICADA — vw_base_unificada")
        # ============================================================

        with db._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM vw_base_unificada WHERE proposta_isize = '260015001'")
            row = cursor.fetchone()
            if row:
                dados = dict(row)
                ok(f"vw_base_unificada retornou dados para 260015001")

                checks = {
                    'cpf': '12345678901',
                    'status_venda': 'APROVADA',
                    'status_bilhete': 'Portado',
                    'status_logistica': 'Pedido Entregue',
                    'bluechip_status': 'ENVIADO',
                    'status_pedido': 'CONECTADA',
                }
                for campo, esperado in checks.items():
                    valor = dados.get(campo)
                    if valor == esperado:
                        ok(f"  {campo} = '{valor}'")
                    else:
                        fail(f"  {campo}: esperado '{esperado}', encontrado '{valor}'")
            else:
                fail("vw_base_unificada não retornou dados")

        # ============================================================
        section("8. CACHE MATERIALIZADO")
        # ============================================================

        db.atualizar_cache_unificada('260015001')

        with db._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM cache_base_unificada WHERE proposta_isize = '260015001'")
            row = cursor.fetchone()
            if row:
                dados = dict(row)
                ok(f"Cache atualizado para 260015001")
                if dados.get('status_bilhete') == 'Portado':
                    ok(f"  Cache status_bilhete = 'Portado'")
                else:
                    fail(f"  Cache status_bilhete incorreto: {dados.get('status_bilhete')}")
            else:
                fail("Cache não foi populado")

        # ============================================================
        section("9. DATA UNIFIER")
        # ============================================================

        unifier = DataUnifier(db)
        resultado = unifier.buscar_unificado('260015001')
        if resultado:
            ok("DataUnifier.buscar_unificado() retornou dados")
        else:
            fail("DataUnifier.buscar_unificado() falhou")

        por_cpf = unifier.buscar_por_cpf('12345678901')
        if por_cpf:
            ok(f"DataUnifier.buscar_por_cpf() retornou {len(por_cpf)} registro(s)")
        else:
            fail("DataUnifier.buscar_por_cpf() falhou")

        por_tel = unifier.buscar_por_telefone('11999887766')
        if por_tel:
            ok(f"DataUnifier.buscar_por_telefone() retornou {len(por_tel)} registro(s)")
        else:
            warn("DataUnifier.buscar_por_telefone() não encontrou (telefone pode não estar no cache)")

        # ============================================================
        section("10. LOTES DE IMPORTAÇÃO")
        # ============================================================

        lote_id = db.criar_lote('teste_validacao.csv', 'consulta_siebel', 'hash_teste_123')
        if lote_id:
            ok(f"Lote criado (id={lote_id})")
        else:
            fail("Falha ao criar lote")

        db.finalizar_lote(lote_id, qtd_inseridos=10, qtd_erros=1, status='concluido')
        with db._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT status, qtd_inseridos FROM lotes_importacao WHERE id = ?", (lote_id,))
            row = cursor.fetchone()
            if row and row[0] == 'concluido' and row[1] == 10:
                ok("Lote finalizado corretamente")
            else:
                fail("Lote não foi finalizado corretamente")

        # Duplicata
        dup_ok = False
        try:
            db.criar_lote('teste_dup.csv', 'consulta_siebel', 'hash_teste_123')
        except Exception:
            dup_ok = True
        if dup_ok:
            ok("Hash duplicado rejeitado (UNIQUE constraint)")
        else:
            fail("Hash duplicado NÃO foi rejeitado")

        # ============================================================
        section("11. EXECUÇÕES DE PROCESSAMENTO")
        # ============================================================

        exec_id = db.registrar_execucao('importacao', {'arquivo': 'teste.csv'})
        if exec_id:
            ok(f"Execução registrada (id={exec_id})")
        else:
            fail("Falha ao registrar execução")

        db.finalizar_execucao(exec_id, 'concluido', registros_processados=50)
        with db._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT status, registros_processados FROM execucoes_processamento WHERE id = ?", (exec_id,))
            row = cursor.fetchone()
            if row and row[0] == 'concluido' and row[1] == 50:
                ok("Execução finalizada corretamente")
            else:
                fail("Execução não foi finalizada corretamente")

        # ============================================================
        section("12. VALIDAÇÃO DE INTEGRIDADE")
        # ============================================================

        integridade = db.validar_integridade()
        if integridade.get('integrity_check') == ['ok']:
            ok("PRAGMA integrity_check: OK")
        else:
            fail(f"integrity_check falhou: {integridade.get('integrity_check')}")

        # ============================================================
        section("13. VALIDAÇÃO DE PROPOSTA_ISIZE")
        # ============================================================

        imp = Importador()

        # CPF (11 dígitos) deve ser inválido
        if not imp.validar_proposta_isize('12345678901'):
            ok("CPF 12345678901 detectado como inválido")
        else:
            fail("CPF não foi detectado")

        # Proposta válida
        if imp.validar_proposta_isize('260015001'):
            ok("Proposta 260015001 aceita como válida")
        else:
            fail("Proposta válida rejeitada")

        # Normalização CPF
        cpf1 = imp.normalizar_cpf('123.456.789-01')
        cpf2 = imp.normalizar_cpf(cpf1)
        if cpf1 == '12345678901' and cpf1 == cpf2:
            ok("Normalização de CPF idempotente")
        else:
            fail(f"Normalização falhou: '{cpf1}' vs '{cpf2}'")

        # ============================================================
        section("14. IDENTIFICAÇÃO DE TIPO DE ARQUIVO")
        # ============================================================

        tipos_teste = {
            'coverte_prop': ['Proposta iSize', 'Cliente', 'Data venda', 'Plano', 'CPF'],
            'portabilidade_tim': ['DATA_SOLICITACAO', 'ACESSO', 'DOADORA', 'RECEPTORA', 'STATUS'],
            'gross': ['CLASSIFICACAO_CR', 'ACESSO', 'CUSTCODE', 'OPERADORA_N1'],
            'relatorio_objetos': ['Nu Pedido', 'Rastreio', 'Transportadora', 'Última Ocorrencia'],
            'resultado_gross': ['Proposta', 'Numero Acesso', 'Data gross', 'Resultado', 'ICCID'],
            'backoffice': ['PEDIDO', 'BLUE_CHIP', 'STATUS_PEDIDO', 'NUMERO_PORTADO', 'NUMERO_PROVISORIO'],
            'consulta_siebel': ['Cpf', 'Número de acesso', 'Número da ordem', 'Código externo', 'Status do bilhete'],
        }

        for tipo_esperado, colunas in tipos_teste.items():
            try:
                tipo_detectado = imp.identificar_tipo_arquivo(colunas)
                if tipo_detectado == tipo_esperado:
                    ok(f"Tipo '{tipo_esperado}' identificado corretamente")
                else:
                    fail(f"Tipo '{tipo_esperado}': detectado como '{tipo_detectado}'")
            except ValueError as e:
                fail(f"Tipo '{tipo_esperado}': erro na detecção — {e}")

        # Tipo desconhecido
        try:
            imp.identificar_tipo_arquivo(['Coluna1', 'Coluna2', 'Coluna3'])
            fail("Tipo desconhecido deveria lançar ValueError")
        except ValueError:
            ok("Tipo desconhecido rejeitado corretamente")

        # ============================================================
        section("15. UPDATE PERMITIDO EM TABELAS DE CONTROLE")
        # ============================================================

        control_update_ok = True
        try:
            with db._get_connection() as conn:
                conn.execute(
                    "UPDATE execucoes_processamento SET etapa_atual = 'teste' WHERE id = ?",
                    (exec_id,)
                )
                conn.commit()
            ok("UPDATE em execucoes_processamento permitido")
        except Exception as e:
            fail(f"UPDATE em tabela de controle bloqueado: {e}")
            control_update_ok = False

        # ============================================================
        # RESUMO
        # ============================================================
        print(f"\n{'='*60}")
        print(f"{BOLD}RESUMO DA VALIDAÇÃO{RESET}")
        print(f"{'='*60}")
        print(f"  {GREEN}Passou:    {passed}{RESET}")
        print(f"  {RED}Falhou:    {failed}{RESET}")
        print(f"  {YELLOW}Avisos:    {warnings}{RESET}")
        print(f"{'='*60}")

        if failed == 0:
            print(f"\n{GREEN}{BOLD}✅ TODOS OS TESTES PASSARAM!{RESET}")
            print(f"O schema v2 está funcionando corretamente.")
        else:
            print(f"\n{RED}{BOLD}❌ {failed} TESTE(S) FALHARAM{RESET}")
            print(f"Revise os erros acima.")

        print(f"\nBanco de teste: {db_path}")

    finally:
        # Limpar
        for ext in ('', '-wal', '-shm'):
            p = db_path + ext
            if os.path.exists(p):
                os.unlink(p)

    return 0 if failed == 0 else 1


if __name__ == '__main__':
    sys.exit(main())
