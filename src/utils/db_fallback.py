"""
Utilitário de Fallback de Dados entre Tabelas
Padroniza a busca de dados com fallback entre múltiplas tabelas do banco de dados
Usa cache persistente (tabela dados_fallback_cache) para melhorar performance
"""
import logging
import hashlib
from typing import Dict, Any, Optional, List
from datetime import datetime
from src.database.db_manager import DatabaseManager

logger = logging.getLogger(__name__)


def _calcular_hash_dados(dados: Dict[str, Any]) -> str:
    """
    Calcula hash dos dados para detectar mudanças.
    
    Args:
        dados: Dicionário com os dados
        
    Returns:
        Hash MD5 dos dados relevantes
    """
    campos_relevantes = [
        str(dados.get('plano', '')),
        str(dados.get('preco', '')),
        str(dados.get('numero_ordem', '')),
        str(dados.get('telefone_portado', '')),
        str(dados.get('numero_linha', ''))
    ]
    hash_str = '|'.join(campos_relevantes)
    return hashlib.md5(hash_str.encode()).hexdigest()


def buscar_dados_com_fallback(
    db_manager: DatabaseManager,
    identificador: str,
    tipo_identificador: str = 'codigo_externo',
    campos_desejados: Optional[List[str]] = None,
    usar_cache: bool = True
) -> Dict[str, Any]:
    """
    Busca dados com fallback entre múltiplas tabelas do banco de dados.
    
    **Estratégia de Cache:**
    1. Verifica cache (dados_fallback_cache) se usar_cache=True
    2. Se encontrado e válido: retorna do cache
    3. Se não encontrado ou inválido: faz busca real com fallback
    4. Armazena resultado no cache para próximas buscas
    
    **Prioridade de busca (quando não usa cache ou cache inválido):**
    1. base_coverte_prop (dados mais completos)
    2. base_unificada (dados consolidados)
    3. tim_unificado (versão mais recente, is_latest=1)
    4. portabilidade_records (dados de portabilidade)
    5. relatorio_objetos (dados de logística)
    
    Args:
        db_manager: Instância do DatabaseManager
        identificador: Valor do identificador (codigo_externo, cpf, numero_ordem, etc.)
        tipo_identificador: Tipo do identificador ('codigo_externo', 'cpf', 'numero_ordem', 'proposta_isize')
        campos_desejados: Lista de campos específicos a buscar. Se None, busca todos os campos padrão.
        usar_cache: Se deve usar cache persistente (padrão: True)
        
    Returns:
        Dict com os dados encontrados. Campos padrão:
        - plano: Nome do plano
        - preco: Preço do plano/ordem
        - numero_ordem: Número da ordem
        - telefone_portado: Telefone para portabilidade
        - numero_linha: Número de linha provisório
        - cliente_nome: Nome do cliente
        - cpf: CPF do cliente
        - codigo_externo: Código externo
        - proposta_isize: Proposta iSize
    """
    resultado = {
        'plano': '',
        'preco': '',
        'numero_ordem': '',
        'telefone_portado': '',
        'numero_linha': '',
        'cliente_nome': '',
        'cpf': '',
        'codigo_externo': '',
        'proposta_isize': ''
    }
    
    if not db_manager or not identificador:
        return resultado
    
    identificador_limpo = str(identificador).strip()
    if not identificador_limpo:
        return resultado
    
    # PRIORIDADE 1: Verificar cache (dados_fallback_cache) se habilitado
    if usar_cache:
        try:
            with db_manager._get_connection() as conn:
                cursor = conn.cursor()
                
                # Construir query de busca no cache baseado no tipo de identificador
                if tipo_identificador == 'codigo_externo':
                    query_cache = """
                    SELECT plano, preco, numero_ordem_consolidado, telefone_portado, numero_linha, 
                           cliente_nome, cpf, codigo_externo, proposta_isize, hash_dados
                    FROM dados_fallback_cache
                    WHERE is_valido = 1
                      AND (codigo_externo = ? OR proposta_isize = ?)
                    LIMIT 1
                    """
                    params_cache = (identificador_limpo, identificador_limpo)
                elif tipo_identificador == 'cpf':
                    query_cache = """
                    SELECT plano, preco, numero_ordem_consolidado, telefone_portado, numero_linha, 
                           cliente_nome, cpf, codigo_externo, proposta_isize, hash_dados
                    FROM dados_fallback_cache
                    WHERE is_valido = 1 AND cpf = ?
                    LIMIT 1
                    """
                    params_cache = (identificador_limpo,)
                elif tipo_identificador == 'numero_ordem':
                    query_cache = """
                    SELECT plano, preco, numero_ordem_consolidado, telefone_portado, numero_linha, 
                           cliente_nome, cpf, codigo_externo, proposta_isize, hash_dados
                    FROM dados_fallback_cache
                    WHERE is_valido = 1 AND numero_ordem = ?
                    LIMIT 1
                    """
                    params_cache = (identificador_limpo,)
                elif tipo_identificador == 'proposta_isize':
                    query_cache = """
                    SELECT plano, preco, numero_ordem_consolidado, telefone_portado, numero_linha, 
                           cliente_nome, cpf, codigo_externo, proposta_isize, hash_dados
                    FROM dados_fallback_cache
                    WHERE is_valido = 1 AND proposta_isize = ?
                    LIMIT 1
                    """
                    params_cache = (identificador_limpo,)
                else:
                    query_cache = None
                    params_cache = None
                
                if query_cache:
                    cursor.execute(query_cache, params_cache)
                    row = cursor.fetchone()
                    
                    if row:
                        # Encontrou no cache, atualizar contador e data de última busca
                        cursor.execute("""
                            UPDATE dados_fallback_cache 
                            SET total_buscas = total_buscas + 1,
                                data_ultima_busca = CURRENT_TIMESTAMP
                            WHERE id = (
                                SELECT id FROM dados_fallback_cache
                                WHERE is_valido = 1
                                  AND (
                                      codigo_externo = ? OR proposta_isize = ? OR 
                                      cpf = ? OR numero_ordem = ?
                                  )
                                LIMIT 1
                            )
                        """, (identificador_limpo, identificador_limpo, identificador_limpo, identificador_limpo))
                        conn.commit()
                        
                        # Preencher resultado do cache
                        # Nota: numero_ordem_consolidado no cache = numero_ordem no resultado
                        resultado['plano'] = str(row[0]).strip() if row[0] else ''
                        resultado['preco'] = str(row[1]).strip() if row[1] else ''
                        resultado['numero_ordem'] = str(row[2]).strip() if row[2] else ''  # numero_ordem_consolidado no cache
                        resultado['telefone_portado'] = str(row[3]).strip() if row[3] else ''
                        resultado['numero_linha'] = str(row[4]).strip() if row[4] else ''
                        resultado['cliente_nome'] = str(row[5]).strip() if row[5] else ''
                        resultado['cpf'] = str(row[6]).strip() if row[6] else ''
                        resultado['codigo_externo'] = str(row[7]).strip() if row[7] else ''
                        resultado['proposta_isize'] = str(row[8]).strip() if row[8] else ''
                        
                        logger.debug(f"Dados encontrados no cache para {tipo_identificador}: {identificador_limpo}")
                        
                        # Filtrar apenas campos desejados se especificado
                        if campos_desejados:
                            resultado = {k: v for k, v in resultado.items() if k in campos_desejados}
                        
                        return resultado
        except Exception as e:
            logger.debug(f"Erro ao buscar no cache (continuando com busca real): {e}")
    
    # PRIORIDADE 2: Busca real com fallback (se não encontrou no cache ou cache desabilitado)
    
    try:
        with db_manager._get_connection() as conn:
            cursor = conn.cursor()
            
            # Verificar quais tabelas existem
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tabelas_existentes = [row[0] for row in cursor.fetchall()]
            
            # Construir condições WHERE baseado no tipo de identificador
            if tipo_identificador == 'codigo_externo':
                # Buscar por codigo_externo ou proposta_isize
                condicao_base = """
                    TRIM(COALESCE(CAST(bc.proposta_isize AS TEXT), CAST(bc.codigo_externo AS TEXT), '')) = ?
                    OR TRIM(CAST(bc.codigo_externo AS TEXT)) = ?
                """
                condicao_pr = "TRIM(CAST(pr.codigo_externo AS TEXT)) = ?"
                condicao_ro = "TRIM(CAST(ro.codigo_externo AS TEXT)) = ?"
                condicao_bu = "TRIM(CAST(bu.codigo_externo AS TEXT)) = ?"
                condicao_tu = "TRIM(CAST(tu.codigo_externo AS TEXT)) = ?"
                valores = (identificador_limpo, identificador_limpo, identificador_limpo, 
                          identificador_limpo, identificador_limpo, identificador_limpo)
            elif tipo_identificador == 'cpf':
                condicao_base = "TRIM(CAST(bc.cpf AS TEXT)) = ?"
                condicao_pr = "TRIM(CAST(pr.cpf AS TEXT)) = ?"
                condicao_ro = "TRIM(CAST(ro.documento AS TEXT)) = ?"
                condicao_bu = "TRIM(CAST(bu.cpf AS TEXT)) = ?"
                condicao_tu = "TRIM(CAST(tu.cpf AS TEXT)) = ?"
                valores = (identificador_limpo,) * 5
            elif tipo_identificador == 'numero_ordem':
                condicao_base = "TRIM(CAST(bc.numero_ordem AS TEXT)) = ?"
                condicao_pr = "TRIM(CAST(pr.numero_ordem AS TEXT)) = ?"
                condicao_bu = "TRIM(CAST(bu.numero_ordem AS TEXT)) = ?"
                condicao_tu = "TRIM(CAST(tu.numero_ordem AS TEXT)) = ?"
                condicao_ro = "1=0"  # relatorio_objetos não tem numero_ordem
                valores = (identificador_limpo,) * 4 + (None,)
            elif tipo_identificador == 'proposta_isize':
                condicao_base = "TRIM(CAST(bc.proposta_isize AS TEXT)) = ?"
                condicao_pr = "TRIM(CAST(pr.codigo_externo AS TEXT)) = ?"
                condicao_ro = "TRIM(CAST(ro.codigo_externo AS TEXT)) = ?"
                condicao_bu = "TRIM(CAST(bu.codigo_externo AS TEXT)) = ?"
                condicao_tu = "TRIM(CAST(tu.proposta_isize AS TEXT)) = ?"
                valores = (identificador_limpo,) * 5
            else:
                logger.warning(f"Tipo de identificador desconhecido: {tipo_identificador}")
                return resultado
            
            # PRIORIDADE 1: base_coverte_prop (tabela mais completa)
            if 'base_coverte_prop' in tabelas_existentes:
                query_base = f"""
                SELECT 
                    bc.plano,
                    bc.preco,
                    bc.numero_ordem,
                    bc.telefone_portado,
                    bc.numero_linha,
                    bc.cliente_nome,
                    bc.cpf,
                    bc.codigo_externo,
                    bc.proposta_isize
                FROM base_coverte_prop bc
                WHERE {condicao_base}
                LIMIT 1
                """
                
                cursor.execute(query_base, valores[:2] if tipo_identificador == 'codigo_externo' else valores[:1])
                row = cursor.fetchone()
                
                if row and any(row):
                    resultado['plano'] = str(row[0]).strip() if row[0] else ''
                    resultado['preco'] = str(row[1]).strip() if row[1] else ''
                    resultado['numero_ordem'] = str(row[2]).strip() if row[2] else ''
                    resultado['telefone_portado'] = str(row[3]).strip() if row[3] else ''
                    resultado['numero_linha'] = str(row[4]).strip() if row[4] else ''
                    resultado['cliente_nome'] = str(row[5]).strip() if row[5] else ''
                    resultado['cpf'] = str(row[6]).strip() if row[6] else ''
                    resultado['codigo_externo'] = str(row[7]).strip() if row[7] else ''
                    resultado['proposta_isize'] = str(row[8]).strip() if row[8] else ''
            
            # PRIORIDADE 2: base_unificada (se campos ainda faltando)
            if 'base_unificada' in tabelas_existentes:
                campos_faltando = [k for k, v in resultado.items() if not v]
                if campos_faltando:
                    query_fallback = """
                    SELECT 
                        bu.plano,
                        bu.preco,
                        bu.numero_ordem,
                        bu.telefone_portado,
                        bu.numero_linha,
                        bu.cliente_nome,
                        bu.cpf,
                        bu.codigo_externo,
                        NULL AS proposta_isize
                    FROM base_unificada bu
                    WHERE {condicao}
                    LIMIT 1
                    """.format(condicao=condicao_bu)
                    
                    cursor.execute(query_fallback, valores[4:5] if tipo_identificador == 'codigo_externo' else valores[3:4])
                    row = cursor.fetchone()
                    
                    if row:
                        if not resultado['plano'] and row[0]:
                            resultado['plano'] = str(row[0]).strip()
                        if not resultado['preco'] and row[1]:
                            resultado['preco'] = str(row[1]).strip()
                        if not resultado['numero_ordem'] and row[2]:
                            resultado['numero_ordem'] = str(row[2]).strip()
                        if not resultado['telefone_portado'] and row[3]:
                            resultado['telefone_portado'] = str(row[3]).strip()
                        if not resultado['numero_linha'] and row[4]:
                            resultado['numero_linha'] = str(row[4]).strip()
                        if not resultado['cliente_nome'] and row[5]:
                            resultado['cliente_nome'] = str(row[5]).strip()
                        if not resultado['cpf'] and row[6]:
                            resultado['cpf'] = str(row[6]).strip()
                        if not resultado['codigo_externo'] and row[7]:
                            resultado['codigo_externo'] = str(row[7]).strip()
            
            # PRIORIDADE 3: tim_unificado (versão mais recente)
            if 'tim_unificado' in tabelas_existentes:
                campos_faltando = [k for k, v in resultado.items() if not v]
                if campos_faltando:
                    query_fallback = """
                    SELECT 
                        tu.plano,
                        NULL AS preco,
                        tu.numero_ordem,
                        tu.telefone_portado,
                        tu.numero_provisorio AS numero_linha,
                        tu.cliente_nome,
                        tu.cpf,
                        tu.codigo_externo,
                        tu.proposta_isize
                    FROM tim_unificado tu
                    WHERE tu.is_latest = 1
                        AND {condicao}
                    LIMIT 1
                    """.format(condicao=condicao_tu)
                    
                    cursor.execute(query_fallback, valores[5:6] if tipo_identificador == 'codigo_externo' else valores[4:5])
                    row = cursor.fetchone()
                    
                    if row:
                        if not resultado['plano'] and row[0]:
                            resultado['plano'] = str(row[0]).strip()
                        if not resultado['numero_ordem'] and row[2]:
                            resultado['numero_ordem'] = str(row[2]).strip()
                        if not resultado['telefone_portado'] and row[3]:
                            resultado['telefone_portado'] = str(row[3]).strip()
                        if not resultado['numero_linha'] and row[4]:
                            resultado['numero_linha'] = str(row[4]).strip()
                        if not resultado['cliente_nome'] and row[5]:
                            resultado['cliente_nome'] = str(row[5]).strip()
                        if not resultado['cpf'] and row[6]:
                            resultado['cpf'] = str(row[6]).strip()
                        if not resultado['codigo_externo'] and row[7]:
                            resultado['codigo_externo'] = str(row[7]).strip()
                        if not resultado['proposta_isize'] and row[8]:
                            resultado['proposta_isize'] = str(row[8]).strip()
            
            # PRIORIDADE 4: portabilidade_records (preco e numero_ordem)
            if 'portabilidade_records' in tabelas_existentes:
                if not resultado['preco'] or not resultado['numero_ordem']:
                    query_fallback = """
                    SELECT 
                        NULL AS plano,
                        pr.preco_ordem AS preco,
                        pr.numero_ordem,
                        NULL AS telefone_portado,
                        NULL AS numero_linha,
                        NULL AS cliente_nome,
                        pr.cpf,
                        pr.codigo_externo,
                        NULL AS proposta_isize
                    FROM portabilidade_records pr
                    WHERE {condicao}
                    LIMIT 1
                    """.format(condicao=condicao_pr)
                    
                    cursor.execute(query_fallback, valores[2:3] if tipo_identificador == 'codigo_externo' else valores[1:2])
                    row = cursor.fetchone()
                    
                    if row:
                        if not resultado['preco'] and row[1]:
                            resultado['preco'] = str(row[1]).strip()
                        if not resultado['numero_ordem'] and row[2]:
                            resultado['numero_ordem'] = str(row[2]).strip()
                        if not resultado['cpf'] and row[6]:
                            resultado['cpf'] = str(row[6]).strip()
                        if not resultado['codigo_externo'] and row[7]:
                            resultado['codigo_externo'] = str(row[7]).strip()
            
            # PRIORIDADE 5: relatorio_objetos (logística - geralmente não tem dados de plano/preço)
            # Não incluído aqui pois relatorio_objetos não tem dados de plano/preço/numero_ordem
            # Usado principalmente para dados de logística (status, rastreio, etc.)
            
            # PRIORIDADE 3: Armazenar resultado no cache se usar_cache está habilitado
            if usar_cache and any(resultado.values()):  # Só armazenar se encontrou algum dado
                try:
                    hash_dados = _calcular_hash_dados(resultado)
                    
                    # Determinar origem dos dados (qual tabela forneceu cada campo)
                    # Prioridade: base_coverte_prop > base_unificada > tim_unificado > portabilidade_records
                    origem_plano = ''
                    origem_preco = ''
                    origem_numero_ordem = ''
                    origem_telefone_portado = ''
                    origem_numero_linha = ''
                    
                    # Verificar origem real baseado nas buscas feitas acima
                    # (Para simplificar, vamos marcar como 'consolidado' indicando que veio do fallback)
                    if resultado.get('plano'):
                        origem_plano = 'consolidado_fallback'
                    if resultado.get('preco'):
                        origem_preco = 'consolidado_fallback'
                    if resultado.get('numero_ordem'):
                        origem_numero_ordem = 'consolidado_fallback'
                    if resultado.get('telefone_portado'):
                        origem_telefone_portado = 'consolidado_fallback'
                    if resultado.get('numero_linha'):
                        origem_numero_linha = 'consolidado_fallback'
                    
                    # Verificar se já existe registro no cache
                    identificador_cache = resultado.get('codigo_externo') or resultado.get('proposta_isize') or identificador_limpo
                    
                    cursor.execute("""
                        SELECT id, hash_dados FROM dados_fallback_cache
                        WHERE is_valido = 1
                          AND (
                              codigo_externo = ? OR proposta_isize = ? OR 
                              cpf = ? OR numero_ordem = ?
                          )
                        LIMIT 1
                    """, (
                        identificador_cache,
                        identificador_cache,
                        resultado.get('cpf', ''),
                        resultado.get('numero_ordem', '')
                    ))
                    
                    existing_cache = cursor.fetchone()
                    
                    if existing_cache:
                        # Se hash mudou, atualizar cache
                        if existing_cache[1] != hash_dados:
                            cursor.execute("""
                                UPDATE dados_fallback_cache SET
                                    plano = ?,
                                    preco = ?,
                                    numero_ordem_consolidado = ?,
                                    telefone_portado = ?,
                                    numero_linha = ?,
                                    cliente_nome = ?,
                                    cpf = ?,
                                    codigo_externo = ?,
                                    proposta_isize = ?,
                                    origem_plano = ?,
                                    origem_preco = ?,
                                    origem_numero_ordem = ?,
                                    origem_telefone_portado = ?,
                                    origem_numero_linha = ?,
                                    hash_dados = ?,
                                    data_ultima_atualizacao = CURRENT_TIMESTAMP,
                                    total_buscas = total_buscas + 1,
                                    updated_at = CURRENT_TIMESTAMP
                                WHERE id = ?
                            """, (
                                resultado.get('plano', ''),
                                resultado.get('preco', ''),
                                resultado.get('numero_ordem', ''),
                                resultado.get('telefone_portado', ''),
                                resultado.get('numero_linha', ''),
                                resultado.get('cliente_nome', ''),
                                resultado.get('cpf', ''),
                                resultado.get('codigo_externo', ''),
                                resultado.get('proposta_isize', ''),
                                origem_plano,
                                origem_preco,
                                origem_numero_ordem,
                                origem_telefone_portado,
                                origem_numero_linha,
                                hash_dados,
                                existing_cache[0]
                            ))
                        else:
                            # Hash não mudou, apenas atualizar contador e data de busca
                            cursor.execute("""
                                UPDATE dados_fallback_cache SET
                                    total_buscas = total_buscas + 1,
                                    data_ultima_busca = CURRENT_TIMESTAMP
                                WHERE id = ?
                            """, (existing_cache[0],))
                    else:
                        # Inserir novo registro no cache
                        cursor.execute("""
                            INSERT INTO dados_fallback_cache (
                                codigo_externo, proposta_isize, cpf, numero_ordem,
                                plano, preco, numero_ordem_consolidado, telefone_portado, numero_linha, cliente_nome,
                                origem_plano, origem_preco, origem_numero_ordem, origem_telefone_portado, origem_numero_linha,
                                hash_dados, total_buscas, is_valido
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 1)
                        """, (
                            resultado.get('codigo_externo', ''),
                            resultado.get('proposta_isize', ''),
                            resultado.get('cpf', ''),
                            resultado.get('numero_ordem', ''),
                            resultado.get('plano', ''),
                            resultado.get('preco', ''),
                            resultado.get('numero_ordem', ''),
                            resultado.get('telefone_portado', ''),
                            resultado.get('numero_linha', ''),
                            resultado.get('cliente_nome', ''),
                            origem_plano,
                            origem_preco,
                            origem_numero_ordem,
                            origem_telefone_portado,
                            origem_numero_linha,
                            hash_dados
                        ))
                    
                    conn.commit()
                    logger.debug(f"Dados armazenados no cache para {tipo_identificador}: {identificador_limpo}")
                    
                except Exception as e:
                    logger.debug(f"Erro ao armazenar no cache (continuando normalmente): {e}")
                    # Não interrompe o fluxo se houver erro no cache
            
    except Exception as e:
        logger.debug(f"Erro ao buscar dados do banco com fallback: {e}")
        import traceback
        logger.debug(traceback.format_exc())
    
    # Filtrar apenas campos desejados se especificado
    if campos_desejados:
        resultado = {k: v for k, v in resultado.items() if k in campos_desejados}
    
    return resultado


def invalidar_cache_fallback(
    db_manager: DatabaseManager,
    identificador: Optional[str] = None,
    tipo_identificador: Optional[str] = None,
    invalidar_todos: bool = False
) -> int:
    """
    Invalida registros no cache de fallback.
    
    Args:
        db_manager: Instância do DatabaseManager
        identificador: Valor do identificador (codigo_externo, cpf, numero_ordem, etc.)
                      Se None, invalida todos se invalidar_todos=True
        tipo_identificador: Tipo do identificador ('codigo_externo', 'cpf', 'numero_ordem', 'proposta_isize')
        invalidar_todos: Se True, invalida todos os registros do cache
        
    Returns:
        Número de registros invalidados
    """
    if not db_manager:
        return 0
    
    try:
        with db_manager._get_connection() as conn:
            cursor = conn.cursor()
            
            if invalidar_todos:
                cursor.execute("""
                    UPDATE dados_fallback_cache 
                    SET is_valido = 0, updated_at = CURRENT_TIMESTAMP
                """)
                count = cursor.rowcount
                conn.commit()
                logger.info(f"Cache invalidado: {count} registros marcados como inválidos")
                return count
            elif identificador and tipo_identificador:
                identificador_limpo = str(identificador).strip()
                
                if tipo_identificador == 'codigo_externo':
                    cursor.execute("""
                        UPDATE dados_fallback_cache 
                        SET is_valido = 0, updated_at = CURRENT_TIMESTAMP
                        WHERE (codigo_externo = ? OR proposta_isize = ?)
                    """, (identificador_limpo, identificador_limpo))
                elif tipo_identificador == 'cpf':
                    cursor.execute("""
                        UPDATE dados_fallback_cache 
                        SET is_valido = 0, updated_at = CURRENT_TIMESTAMP
                        WHERE cpf = ?
                    """, (identificador_limpo,))
                elif tipo_identificador == 'numero_ordem':
                    cursor.execute("""
                        UPDATE dados_fallback_cache 
                        SET is_valido = 0, updated_at = CURRENT_TIMESTAMP
                        WHERE numero_ordem = ?
                    """, (identificador_limpo,))
                elif tipo_identificador == 'proposta_isize':
                    cursor.execute("""
                        UPDATE dados_fallback_cache 
                        SET is_valido = 0, updated_at = CURRENT_TIMESTAMP
                        WHERE proposta_isize = ?
                    """, (identificador_limpo,))
                else:
                    return 0
                
                count = cursor.rowcount
                conn.commit()
                logger.debug(f"Cache invalidado para {tipo_identificador}: {identificador_limpo} - {count} registro(s)")
                return count
    except Exception as e:
        logger.error(f"Erro ao invalidar cache: {e}")
        return 0
    
    return 0


def limpar_cache_antigo(
    db_manager: DatabaseManager,
    dias_antigos: int = 90
) -> int:
    """
    Remove registros antigos do cache (dados não consultados há mais de X dias).
    
    Args:
        db_manager: Instância do DatabaseManager
        dias_antigos: Número de dias sem consulta para considerar antigo (padrão: 90)
        
    Returns:
        Número de registros removidos
    """
    if not db_manager:
        return 0
    
    try:
        with db_manager._get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                DELETE FROM dados_fallback_cache
                WHERE data_ultima_busca < datetime('now', '-' || ? || ' days')
                  AND is_valido = 0
            """, (dias_antigos,))
            
            count = cursor.rowcount
            conn.commit()
            logger.info(f"Cache limpo: {count} registros antigos removidos (mais de {dias_antigos} dias)")
            return count
    except Exception as e:
        logger.error(f"Erro ao limpar cache antigo: {e}")
        return 0


def estatisticas_cache(db_manager: DatabaseManager) -> Dict[str, Any]:
    """
    Retorna estatísticas sobre o cache de fallback.
    
    Args:
        db_manager: Instância do DatabaseManager
        
    Returns:
        Dict com estatísticas do cache
    """
    stats = {
        'total_registros': 0,
        'registros_validos': 0,
        'registros_invalidos': 0,
        'total_buscas': 0,
        'cache_hits': 0,
        'cache_misses': 0
    }
    
    if not db_manager:
        return stats
    
    try:
        with db_manager._get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("SELECT COUNT(*) FROM dados_fallback_cache")
            stats['total_registros'] = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM dados_fallback_cache WHERE is_valido = 1")
            stats['registros_validos'] = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM dados_fallback_cache WHERE is_valido = 0")
            stats['registros_invalidos'] = cursor.fetchone()[0]
            
            cursor.execute("SELECT SUM(total_buscas) FROM dados_fallback_cache WHERE is_valido = 1")
            total_buscas = cursor.fetchone()[0]
            stats['total_buscas'] = total_buscas if total_buscas else 0
            
            # Cache hit rate estimado (total_buscas / registros validos)
            if stats['registros_validos'] > 0:
                stats['cache_hits'] = stats['total_buscas']
                stats['cache_misses'] = stats['total_registros'] - stats['registros_validos']
    except Exception as e:
        logger.error(f"Erro ao obter estatísticas do cache: {e}")
    
    return stats


def verificar_tabelas_disponiveis(db_manager: DatabaseManager) -> Dict[str, bool]:
    """
    Verifica quais tabelas estão disponíveis no banco de dados.
    
    Args:
        db_manager: Instância do DatabaseManager
        
    Returns:
        Dict indicando quais tabelas existem:
        {
            'base_coverte_prop': True/False,
            'portabilidade_records': True/False,
            'relatorio_objetos': True/False,
            'base_unificada': True/False,
            'tim_unificado': True/False
        }
    """
    tabelas = {
        'base_coverte_prop': False,
        'portabilidade_records': False,
        'relatorio_objetos': False,
        'base_unificada': False,
        'tim_unificado': False
    }
    
    if not db_manager:
        return tabelas
    
    try:
        with db_manager._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tabelas_existentes = [row[0] for row in cursor.fetchall()]
            
            for tabela in tabelas.keys():
                tabelas[tabela] = tabela in tabelas_existentes
                
    except Exception as e:
        logger.error(f"Erro ao verificar tabelas disponíveis: {e}")
    
    return tabelas
