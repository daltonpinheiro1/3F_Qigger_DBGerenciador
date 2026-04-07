"""
Utilitário para validar dados da tabela portabilidade_processamento
antes da geração de arquivos de homologação
"""
import logging
from typing import Dict, Any, Optional, List, Tuple
from src.database.db_manager import DatabaseManager

logger = logging.getLogger(__name__)


def _carregar_indice_processamento(db_manager: DatabaseManager) -> Dict[str, Dict[str, Any]]:
    """
    Carrega toda a tabela portabilidade_processamento em memória, indexada por
    id_proposta_isize, codigo_externo e cpf. Mantém o registro mais recente por data_importacao.
    Uma única query em vez de N queries.
    """
    indice = {}
    with db_manager._get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='portabilidade_processamento'
        """)
        if not cursor.fetchone():
            return indice

        cursor.execute("""
            SELECT id_proposta_isize, codigo_externo, cpf, CPF_CNPJ,
                   STATUS, MOTIVO_CONFLITO, MOTIVO_CANCELAMENTO, data_importacao
            FROM portabilidade_processamento
            ORDER BY data_importacao DESC
        """)
        colunas = [d[0] for d in cursor.description]
        for row in cursor.fetchall():
            d = dict(zip(colunas, row))
            id_prop = str(d.get('id_proposta_isize') or '').strip()
            cod_ext = str(d.get('codigo_externo') or '').strip()
            cpf_val = str(d.get('cpf') or d.get('CPF_CNPJ') or '').strip()
            for chave in [id_prop, cod_ext, cpf_val]:
                if chave and chave not in indice:
                    indice[chave] = d
    return indice


def _validar_registro_em_memoria(dados: Dict[str, Any], id_registro: str) -> Tuple[bool, List[str]]:
    """Valida um registro de processamento já carregado em memória."""
    erros = []
    status = (dados.get('STATUS') or '').strip().upper()
    if not status:
        erros.append('STATUS não informado na tabela portabilidade_processamento')
    elif status in ['CANCELADO', 'CANCELAMENTO', 'BLOQUEADO', 'ERRO']:
        erros.append(f'STATUS indica problema: {status}')
    motivo_conflito = (dados.get('MOTIVO_CONFLITO') or '').strip()
    if motivo_conflito:
        erros.append(f'Conflito identificado: {motivo_conflito}')
    motivo_cancelamento = (dados.get('MOTIVO_CANCELAMENTO') or '').strip()
    if motivo_cancelamento:
        erros.append(f'Cancelamento identificado: {motivo_cancelamento}')
    return (len(erros) == 0, erros)


def validar_dados_processamento(
    db_manager: DatabaseManager,
    id_proposta_isize: Optional[str] = None,
    codigo_externo: Optional[str] = None,
    cpf: Optional[str] = None
) -> Tuple[bool, List[str], Optional[Dict[str, Any]]]:
    """
    Valida dados na tabela portabilidade_processamento antes de gerar arquivos
    
    Args:
        db_manager: Instância do DatabaseManager
        id_proposta_isize: ID da proposta iSize
        codigo_externo: Código externo
        cpf: CPF do cliente
        
    Returns:
        Tupla (is_valido, erros, dados) onde:
        - is_valido: True se os dados são válidos para processamento
        - erros: Lista de erros encontrados
        - dados: Dicionário com dados validados ou None se não encontrado
    """
    erros = []
    dados = None
    
    with db_manager._get_connection() as conn:
        cursor = conn.cursor()
        
        # Verificar se tabela existe
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='portabilidade_processamento'
        """)
        if not cursor.fetchone():
            logger.debug("Tabela portabilidade_processamento não existe")
            return True, [], None  # Tabela não existe, não há validação a fazer
        
        # Buscar registro na tabela
        query = "SELECT * FROM portabilidade_processamento WHERE 1=1"
        params = []
        
        if id_proposta_isize:
            query += " AND id_proposta_isize = ?"
            params.append(id_proposta_isize)
        elif codigo_externo:
            query += " AND codigo_externo = ?"
            params.append(codigo_externo)
        elif cpf:
            query += " AND (cpf = ? OR CPF_CNPJ = ?)"
            params.extend([cpf, cpf])
        else:
            # Sem identificador, não pode validar
            return True, [], None
        
        query += " ORDER BY data_importacao DESC LIMIT 1"
        
        cursor.execute(query, params)
        row = cursor.fetchone()
        
        if not row:
            # Registro não encontrado na tabela de processamento
            # Isso não é um erro, apenas não há validação a fazer
            return True, [], None
        
        # Converter para dicionário
        dados = {key: row[key] for key in row.keys()}
        
        # Validar STATUS
        status = dados.get('STATUS', '').strip().upper() if dados.get('STATUS') else ''
        if not status:
            erros.append('STATUS não informado na tabela portabilidade_processamento')
        elif status in ['CANCELADO', 'CANCELAMENTO', 'BLOQUEADO', 'ERRO']:
            erros.append(f'STATUS indica problema: {status}')
        
        # Validar MOTIVO_CONFLITO
        motivo_conflito = dados.get('MOTIVO_CONFLITO', '').strip() if dados.get('MOTIVO_CONFLITO') else ''
        if motivo_conflito:
            erros.append(f'Conflito identificado: {motivo_conflito}')
        
        # Validar MOTIVO_CANCELAMENTO
        motivo_cancelamento = dados.get('MOTIVO_CANCELAMENTO', '').strip() if dados.get('MOTIVO_CANCELAMENTO') else ''
        if motivo_cancelamento:
            erros.append(f'Cancelamento identificado: {motivo_cancelamento}')
        
        # Se há erros, não é válido para processamento
        is_valido = len(erros) == 0
        
        if not is_valido:
            logger.debug(
                f"Registro {id_proposta_isize or codigo_externo or cpf} "
                f"não é válido para processamento: {', '.join(erros)}"
            )
        
        return is_valido, erros, dados


def filtrar_registros_validos(
    db_manager: DatabaseManager,
    registros: List[Dict[str, Any]]
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Filtra registros válidos baseado na validação da tabela portabilidade_processamento.
    Usa validação em lote (1 query) em vez de 1 query por registro.
    """
    registros_validos = []
    registros_invalidos = []
    indice = _carregar_indice_processamento(db_manager)
    if not indice:
        logger.debug("Tabela portabilidade_processamento vazia ou inexistente - todos considerados válidos")
        return registros, []

    for registro in registros:
        id_proposta = (str(registro.get('id_proposta_isize') or registro.get('codigo_externo') or registro.get('proposta_isize') or '')).strip()
        codigo_externo = (str(registro.get('codigo_externo') or '')).strip()
        cpf = (str(registro.get('cpf') or '')).strip()
        chave = id_proposta or codigo_externo or cpf
        dados_validacao = (indice.get(id_proposta) or indice.get(codigo_externo) or indice.get(cpf)) if chave else None

        if not dados_validacao:
            registros_validos.append(registro)
            continue

        is_valido, erros = _validar_registro_em_memoria(dados_validacao, chave)
        if is_valido:
            registro['_validacao_status'] = dados_validacao.get('STATUS')
            registro['_validacao_data_importacao'] = dados_validacao.get('data_importacao')
            registros_validos.append(registro)
        else:
            registro['_erros_validacao'] = erros
            registros_invalidos.append(registro)
            logger.debug(f"Registro {chave} inválido: {', '.join(erros)}")

    logger.info(
        f"Validação concluída: {len(registros_validos)} válidos, "
        f"{len(registros_invalidos)} inválidos"
    )
    return registros_validos, registros_invalidos


def obter_estatisticas_validacao(db_manager: DatabaseManager) -> Dict[str, Any]:
    """
    Obtém estatísticas de validação da tabela portabilidade_processamento
    
    Args:
        db_manager: Instância do DatabaseManager
        
    Returns:
        Dicionário com estatísticas
    """
    stats = {
        'total_registros': 0,
        'com_status': 0,
        'com_conflito': 0,
        'com_cancelamento': 0,
        'validos': 0,
        'invalidos': 0,
        'por_status': {}
    }
    
    with db_manager._get_connection() as conn:
        cursor = conn.cursor()
        
        # Verificar se tabela existe
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='portabilidade_processamento'
        """)
        if not cursor.fetchone():
            return stats
        
        # Total de registros
        cursor.execute("SELECT COUNT(*) FROM portabilidade_processamento")
        stats['total_registros'] = cursor.fetchone()[0]
        
        # Registros com STATUS
        cursor.execute("SELECT COUNT(*) FROM portabilidade_processamento WHERE STATUS IS NOT NULL AND STATUS != ''")
        stats['com_status'] = cursor.fetchone()[0]
        
        # Registros com conflito
        cursor.execute("SELECT COUNT(*) FROM portabilidade_processamento WHERE MOTIVO_CONFLITO IS NOT NULL AND MOTIVO_CONFLITO != ''")
        stats['com_conflito'] = cursor.fetchone()[0]
        
        # Registros com cancelamento
        cursor.execute("SELECT COUNT(*) FROM portabilidade_processamento WHERE MOTIVO_CANCELAMENTO IS NOT NULL AND MOTIVO_CANCELAMENTO != ''")
        stats['com_cancelamento'] = cursor.fetchone()[0]
        
        # Registros válidos (sem conflito e sem cancelamento)
        cursor.execute("""
            SELECT COUNT(*) FROM portabilidade_processamento 
            WHERE (MOTIVO_CONFLITO IS NULL OR MOTIVO_CONFLITO = '')
            AND (MOTIVO_CANCELAMENTO IS NULL OR MOTIVO_CANCELAMENTO = '')
            AND (STATUS IS NOT NULL AND STATUS != '' AND STATUS NOT IN ('CANCELADO', 'CANCELAMENTO', 'BLOQUEADO', 'ERRO'))
        """)
        stats['validos'] = cursor.fetchone()[0]
        
        # Registros inválidos
        stats['invalidos'] = stats['total_registros'] - stats['validos']
        
        # Distribuição por STATUS
        cursor.execute("""
            SELECT STATUS, COUNT(*) as count 
            FROM portabilidade_processamento 
            WHERE STATUS IS NOT NULL AND STATUS != ''
            GROUP BY STATUS
            ORDER BY count DESC
        """)
        stats['por_status'] = {row[0]: row[1] for row in cursor.fetchall()}
    
    return stats
