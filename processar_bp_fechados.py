"""
Script para processar arquivo Excel BP_FECHADOS_TIM_OFICIAL.xlsx
e inserir na tabela portabilidade_processamento do banco portabilidade.db

Características:
- Processa arquivo BP_FECHADOS_TIM_OFICIAL.xlsx na mesma pasta do projeto
- Validações cruzadas com STATUS, MOTIVO_CONFLITO, MOTIVO_CANCELAMENTO
- Garante integridade dos dados antes da geração dos arquivos
"""
import sys
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List
import pandas as pd
from datetime import datetime

# Configurar encoding UTF-8 para o console
from src.utils.console_utils import setup_windows_console
setup_windows_console()

# Configurar logging
import io

Path('logs').mkdir(exist_ok=True)

if sys.platform == 'win32':
    try:
        console_handler = logging.StreamHandler(io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace'))
    except Exception:
        console_handler = logging.StreamHandler(sys.stdout)
else:
    console_handler = logging.StreamHandler(sys.stdout)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/processar_bp_fechados.log', encoding='utf-8'),
        console_handler
    ]
)

logger = logging.getLogger(__name__)

from src.database.db_manager import DatabaseManager
from src.utils.progress_bar import ProgressBar, log_progress
from config import DB_PATH, PROJECT_ROOT, PASTA_IMPORTACOES


def limpar_valor(valor: Any) -> Optional[str]:
    """Limpa e normaliza valores do Excel"""
    if pd.isna(valor) or valor is None:
        return None
    valor_str = str(valor).strip()
    if valor_str in ['', 'nan', 'None', 'NULL', 'null']:
        return None
    return valor_str


def limpar_cpf(cpf: Any) -> Optional[str]:
    """Limpa CPF removendo caracteres não numéricos"""
    if pd.isna(cpf) or cpf is None:
        return None
    cpf_str = ''.join(c for c in str(cpf) if c.isdigit())
    return cpf_str if len(cpf_str) >= 11 else None


def buscar_id_proposta_isize_base_coverte(
    db_manager: DatabaseManager,
    dados: Dict[str, Any]
) -> Optional[str]:
    """
    Busca id_proposta_isize na tabela base_coverte_prop usando múltiplos campos de associação
    
    Estratégias de busca (em ordem de prioridade):
    1. CPF_CNPJ ou CPF
    2. Número de ordem (numero_ordem)
    3. Acesso provisório (numero_acesso, numero_linha, ACESSO_TEMPORARIO)
    4. Número de remessa (remessa_bluechip, pedido_bluechip)
    5. Código externo
    6. Telefone portado
    
    Args:
        db_manager: Instância do DatabaseManager
        dados: Dicionário com dados do registro do Excel
        
    Returns:
        id_proposta_isize encontrado ou None
    """
    with db_manager._get_connection() as conn:
        cursor = conn.cursor()
        
        # Verificar se tabela existe
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='base_coverte_prop'
        """)
        if not cursor.fetchone():
            logger.debug("Tabela base_coverte_prop não existe")
            return None
        
        # Extrair valores para busca
        cpf_cnpj = dados.get('CPF_CNPJ') or dados.get('cpf')
        numero_ordem = dados.get('numero_ordem') or dados.get('NUMERO_ORDEM')
        numero_acesso = dados.get('numero_acesso') or dados.get('ACESSO') or dados.get('ACESSO_TEMPORARIO')
        numero_linha = dados.get('numero_linha')
        remessa_bluechip = dados.get('remessa_bluechip')
        pedido_bluechip = dados.get('pedido_bluechip')
        codigo_externo = dados.get('codigo_externo') or dados.get('CODIGO_EXTERNO')
        telefone_portado = dados.get('telefone_portado') or dados.get('TELEFONE_PORTADO')
        
        # Limpar CPF/CNPJ
        if cpf_cnpj:
            cpf_limpo = ''.join(c for c in str(cpf_cnpj) if c.isdigit())
        else:
            cpf_limpo = None
        
        # Estratégia 1: Buscar por CPF/CNPJ
        # IMPORTANTE: Retornar apenas proposta_isize, nunca CPF
        if cpf_limpo and len(cpf_limpo) >= 11:
            cursor.execute("""
                SELECT proposta_isize, codigo_externo
                FROM base_coverte_prop
                WHERE cpf = ?
                ORDER BY data_importacao DESC, updated_at DESC
                LIMIT 1
            """, (cpf_limpo,))
            row = cursor.fetchone()
            if row and row[0]:
                proposta_isize = str(row[0]).strip()
                # Validar que não é CPF (CPF tem 11 dígitos, proposta_isize geralmente tem 9 dígitos ou menos)
                # proposta_isize válido: não tem 11 dígitos, não é igual ao CPF buscado, e tem no máximo 15 caracteres
                if (len(proposta_isize) <= 15 and 
                    proposta_isize != cpf_limpo and 
                    not (len(proposta_isize) == 11 and proposta_isize.isdigit())):
                    logger.debug(f"Encontrado id_proposta_isize por CPF: {proposta_isize}")
                    return proposta_isize
            if row and row[1]:
                codigo = str(row[1]).strip()
                # Validar que código externo não é CPF
                if (len(codigo) <= 15 and 
                    codigo != cpf_limpo and 
                    not (len(codigo) == 11 and codigo.isdigit())):
                    logger.debug(f"Encontrado codigo_externo por CPF (usando como fallback): {codigo}")
                    return codigo
            logger.debug(f"Busca por CPF não retornou proposta_isize válido (pode ter retornado CPF)")
        
        # Estratégia 2: Buscar por número de ordem
        if numero_ordem:
            numero_ordem_limpo = str(numero_ordem).strip()
            cursor.execute("""
                SELECT proposta_isize, codigo_externo
                FROM base_coverte_prop
                WHERE numero_ordem = ? OR numero_ordem LIKE ?
                ORDER BY data_importacao DESC, updated_at DESC
                LIMIT 1
            """, (numero_ordem_limpo, f"%{numero_ordem_limpo}%"))
            row = cursor.fetchone()
            if row and row[0]:
                proposta_isize = str(row[0]).strip()
                # Validar que não é CPF
                if len(proposta_isize) <= 15 and not (len(proposta_isize) == 11 and proposta_isize.isdigit()):
                    logger.debug(f"Encontrado id_proposta_isize por número de ordem: {proposta_isize}")
                    return proposta_isize
            if row and row[1]:
                codigo = str(row[1]).strip()
                # Validar que código externo não é CPF
                if len(codigo) <= 15 and not (len(codigo) == 11 and codigo.isdigit()):
                    logger.debug(f"Encontrado codigo_externo por número de ordem (usando como fallback): {codigo}")
                    return codigo
        
        # Estratégia 3: Buscar por acesso provisório (numero_acesso, numero_linha)
        if numero_acesso:
            numero_acesso_limpo = str(numero_acesso).strip()
            cursor.execute("""
                SELECT proposta_isize, codigo_externo
                FROM base_coverte_prop
                WHERE numero_acesso = ? OR numero_linha = ?
                ORDER BY data_importacao DESC, updated_at DESC
                LIMIT 1
            """, (numero_acesso_limpo, numero_acesso_limpo))
            row = cursor.fetchone()
            if row and row[0]:
                proposta_isize = str(row[0]).strip()
                # Validar que não é CPF
                if len(proposta_isize) <= 15 and not (len(proposta_isize) == 11 and proposta_isize.isdigit()):
                    logger.debug(f"Encontrado id_proposta_isize por acesso provisório: {proposta_isize}")
                    return proposta_isize
            if row and row[1]:
                codigo = str(row[1]).strip()
                # Validar que código externo não é CPF
                if len(codigo) <= 15 and not (len(codigo) == 11 and codigo.isdigit()):
                    logger.debug(f"Encontrado codigo_externo por acesso provisório (usando como fallback): {codigo}")
                    return codigo
        
        if numero_linha:
            numero_linha_limpo = str(numero_linha).strip()
            cursor.execute("""
                SELECT proposta_isize, codigo_externo
                FROM base_coverte_prop
                WHERE numero_linha = ? OR numero_acesso = ?
                ORDER BY data_importacao DESC, updated_at DESC
                LIMIT 1
            """, (numero_linha_limpo, numero_linha_limpo))
            row = cursor.fetchone()
            if row and row[0]:
                logger.debug(f"Encontrado id_proposta_isize por número de linha: {row[0]}")
                return str(row[0]).strip()
            elif row and row[1]:
                logger.debug(f"Encontrado codigo_externo por número de linha (usando como fallback): {row[1]}")
                return str(row[1]).strip()
        
        # Estratégia 4: Buscar por número de remessa (remessa_bluechip, pedido_bluechip)
        if remessa_bluechip:
            remessa_limpo = str(remessa_bluechip).strip()
            cursor.execute("""
                SELECT proposta_isize, codigo_externo
                FROM base_coverte_prop
                WHERE remessa_bluechip = ? OR pedido_bluechip = ?
                ORDER BY data_importacao DESC, updated_at DESC
                LIMIT 1
            """, (remessa_limpo, remessa_limpo))
            row = cursor.fetchone()
            if row and row[0]:
                proposta_isize = str(row[0]).strip()
                # Validar que não é CPF
                if len(proposta_isize) <= 15 and not (len(proposta_isize) == 11 and proposta_isize.isdigit()):
                    logger.debug(f"Encontrado id_proposta_isize por remessa: {proposta_isize}")
                    return proposta_isize
            if row and row[1]:
                codigo = str(row[1]).strip()
                # Validar que código externo não é CPF
                if len(codigo) <= 15 and not (len(codigo) == 11 and codigo.isdigit()):
                    logger.debug(f"Encontrado codigo_externo por remessa (usando como fallback): {codigo}")
                    return codigo
        
        if pedido_bluechip:
            pedido_limpo = str(pedido_bluechip).strip()
            cursor.execute("""
                SELECT proposta_isize, codigo_externo
                FROM base_coverte_prop
                WHERE pedido_bluechip = ? OR remessa_bluechip = ?
                ORDER BY data_importacao DESC, updated_at DESC
                LIMIT 1
            """, (pedido_limpo, pedido_limpo))
            row = cursor.fetchone()
            if row and row[0]:
                proposta_isize = str(row[0]).strip()
                # Validar que não é CPF
                if len(proposta_isize) <= 15 and not (len(proposta_isize) == 11 and proposta_isize.isdigit()):
                    logger.debug(f"Encontrado id_proposta_isize por pedido bluechip: {proposta_isize}")
                    return proposta_isize
            if row and row[1]:
                codigo = str(row[1]).strip()
                # Validar que código externo não é CPF
                if len(codigo) <= 15 and not (len(codigo) == 11 and codigo.isdigit()):
                    logger.debug(f"Encontrado codigo_externo por pedido bluechip (usando como fallback): {codigo}")
                    return codigo
        
        # Estratégia 5: Buscar por código externo
        if codigo_externo:
            codigo_limpo = str(codigo_externo).strip()
            cursor.execute("""
                SELECT proposta_isize, codigo_externo
                FROM base_coverte_prop
                WHERE codigo_externo = ? OR proposta_isize = ?
                ORDER BY data_importacao DESC, updated_at DESC
                LIMIT 1
            """, (codigo_limpo, codigo_limpo))
            row = cursor.fetchone()
            if row and row[0]:
                logger.debug(f"Encontrado id_proposta_isize por código externo: {row[0]}")
                return str(row[0]).strip()
            elif row and row[1]:
                logger.debug(f"Encontrado codigo_externo (usando como fallback): {row[1]}")
                return str(row[1]).strip()
        
        # Estratégia 6: Buscar por telefone portado
        if telefone_portado:
            telefone_limpo = ''.join(c for c in str(telefone_portado) if c.isdigit())
            if telefone_limpo:
                cursor.execute("""
                    SELECT proposta_isize, codigo_externo
                    FROM base_coverte_prop
                    WHERE telefone_portado = ? OR telefone_portado LIKE ?
                    ORDER BY data_importacao DESC, updated_at DESC
                    LIMIT 1
                """, (telefone_limpo, f"%{telefone_limpo}%"))
                row = cursor.fetchone()
                if row and row[0]:
                    proposta_isize = str(row[0]).strip()
                    # Validar que não é CPF
                    if len(proposta_isize) <= 15 and not (len(proposta_isize) == 11 and proposta_isize.isdigit()):
                        logger.debug(f"Encontrado id_proposta_isize por telefone portado: {proposta_isize}")
                        return proposta_isize
                if row and row[1]:
                    codigo = str(row[1]).strip()
                    # Validar que código externo não é CPF
                    if len(codigo) <= 15 and not (len(codigo) == 11 and codigo.isdigit()):
                        logger.debug(f"Encontrado codigo_externo por telefone portado (usando como fallback): {codigo}")
                        return codigo
        
        logger.debug("Não foi possível encontrar id_proposta_isize na base_coverte_prop")
        return None


def mapear_campos_excel(row: pd.Series, db_manager: Optional[DatabaseManager] = None) -> Optional[Dict[str, Any]]:
    """
    Mapeia colunas do Excel BP_FECHADOS_TIM_OFICIAL para a tabela portabilidade_processamento
    
    Args:
        row: Linha do DataFrame do Excel
        db_manager: Instância do DatabaseManager para buscar id_proposta_isize na base_coverte_prop
    """
    def safe_get(key, default=None, clean_func=None):
        """Busca valor do Excel com tratamento de erros"""
        try:
            # Buscar exatamente como está no Excel
            if key in row.index:
                valor = row[key]
                if clean_func:
                    return clean_func(valor)
                return limpar_valor(valor)
            
            # Tentar variações se não encontrar
            for col_name in [key.upper(), key.lower(), key.title()]:
                if col_name in row.index:
                    valor = row[col_name]
                    if clean_func:
                        return clean_func(valor)
                    return limpar_valor(valor)
            
            # Buscar por substring (case insensitive)
            matching_cols = [col for col in row.index if str(key).lower() in str(col).lower()]
            if matching_cols:
                valor = row[matching_cols[0]]
                if clean_func:
                    return clean_func(valor)
                return limpar_valor(valor)
        except (KeyError, IndexError, AttributeError, TypeError):
            pass
        return default
    
    # Mapear todos os campos conforme especificação
    dados = {
        # Campos obrigatórios
        'id_proposta_isize': safe_get('ID_ISIZE') or safe_get('Id_proposta_isize') or safe_get('ID PROPOSTA ISIZE'),
        
        # Campos de controle
        'STATUS': safe_get('STATUS') or safe_get('Status'),
        'MOTIVO_CONFLITO': safe_get('MOTIVO_CONFLITO') or safe_get('Motivo Conflito') or safe_get('MOTIVO CONFLITO'),
        'MOTIVO_CANCELAMENTO': safe_get('MOTIVO_CANCELAMENTO') or safe_get('Motivo Cancelamento') or safe_get('MOTIVO CANCELAMENTO'),
        
        # Campos de dados
        'DATA_SOLICITACAO': safe_get('DATA_SOLICITACAO') or safe_get('Data Solicitação') or safe_get('DATA SOLICITACAO'),
        'MES_SOLICITACAO': safe_get('MES_SOLICITACAO') or safe_get('Mês Solicitação') or safe_get('MES SOLICITACAO'),
        'DATA_ATIVACAO': safe_get('DATA_ATIVACAO') or safe_get('Data Ativação') or safe_get('DATA ATIVACAO'),
        'MES_ATIVACAO': safe_get('MES_ATIVACAO') or safe_get('Mês Ativação') or safe_get('MES ATIVACAO'),
        'DATA_CONCLUSAO': safe_get('DATA_CONCLUSAO') or safe_get('Data Conclusão') or safe_get('DATA CONCLUSAO'),
        'SKY_CONTRATO': safe_get('SKY_CONTRATO') or safe_get('Sky Contrato') or safe_get('SKY CONTRATO'),
        'SKY_CLIENTE': safe_get('SKY_CLIENTE') or safe_get('Sky Cliente') or safe_get('SKY CLIENTE'),
        'PROTOCOLO': safe_get('PROTOCOLO') or safe_get('Protocolo'),
        'ACESSO': safe_get('ACESSO') or safe_get('Acesso'),
        'ACESSO_TEMPORARIO': safe_get('ACESSO_TEMPORARIO') or safe_get('Acesso Temporário') or safe_get('ACESSO TEMPORARIO'),
        'DDD': safe_get('DDD') or safe_get('Ddd'),
        'OPERADORA_N1': safe_get('OPERADORA_N1') or safe_get('Operadora N1') or safe_get('OPERADORA N1'),
        'TIPO_PRE_POS_CONTROLE': safe_get('TIPO_PRE_POS_CONTROLE') or safe_get('Tipo Pré/Pós Controle') or safe_get('TIPO PRE POS CONTROLE'),
        'TECNOLOGIA': safe_get('TECNOLOGIA') or safe_get('Tecnologia'),
        'VOZ_DADOS': safe_get('VOZ_DADOS') or safe_get('Voz Dados') or safe_get('VOZ DADOS'),
        'DOADORA': safe_get('DOADORA') or safe_get('Doadora'),
        'RECEPTORA': safe_get('RECEPTORA') or safe_get('Receptora'),
        'TIPO': safe_get('TIPO') or safe_get('Tipo'),
        'TIPO_SEGMENTO_1': safe_get('TIPO_SEGMENTO_1') or safe_get('Tipo Segmento 1') or safe_get('TIPO SEGMENTO 1'),
        'TIPO_SEGMENTO_2': safe_get('TIPO_SEGMENTO_2') or safe_get('Tipo Segmento 2') or safe_get('TIPO SEGMENTO 2'),
        'TIPO_FAMILIA_PLANO': safe_get('TIPO_FAMILIA_PLANO') or safe_get('Tipo Família Plano') or safe_get('TIPO FAMILIA PLANO'),
        'NIVEL_PLANO': safe_get('NIVEL_PLANO') or safe_get('Nível Plano') or safe_get('NIVEL PLANO'),
        'CANAL_N0': safe_get('CANAL_N0') or safe_get('Canal N0') or safe_get('CANAL N0'),
        'CANAL_N1': safe_get('CANAL_N1') or safe_get('Canal N1') or safe_get('CANAL N1'),
        'CANAL_N2': safe_get('CANAL_N2') or safe_get('Canal N2') or safe_get('CANAL N2'),
        'CANAL_N3': safe_get('CANAL_N3') or safe_get('Canal N3') or safe_get('CANAL N3'),
        'CANAL_N4': safe_get('CANAL_N4') or safe_get('Canal N4') or safe_get('CANAL N4'),
        'GRUPO_ECONOMICO': safe_get('GRUPO_ECONOMICO') or safe_get('Grupo Econômico') or safe_get('GRUPO ECONOMICO'),
        'CUSTCODE': safe_get('CUSTCODE') or safe_get('Custcode') or safe_get('CUST CODE'),
        'CPF_CNPJ': safe_get('CPF_CNPJ', clean_func=limpar_cpf) or safe_get('CPF/CNPJ', clean_func=limpar_cpf) or safe_get('CPF CNPJ', clean_func=limpar_cpf),
        'PORTABILIDADE': safe_get('PORTABILIDADE') or safe_get('Portabilidade'),
        'SELF_PORTIN': safe_get('SELF_PORTIN') or safe_get('Self Portin') or safe_get('SELF PORTIN'),
        'CANAL_PORTABILIDADE': safe_get('CANAL_PORTABILIDADE') or safe_get('Canal Portabilidade') or safe_get('CANAL PORTABILIDADE'),
        'TENTATIVAS': safe_get('TENTATIVAS') or safe_get('Tentativas'),
        'ID_ISIZE': safe_get('ID_ISIZE') or safe_get('Id iSize') or safe_get('ID ISIZE'),
        'DATA_UPDATE': safe_get('DATA_UPDATE') or safe_get('Data Update') or safe_get('DATA UPDATE'),
        
        # Campos adicionais (se existirem no Excel)
        'cpf': safe_get('CPF', clean_func=limpar_cpf) or safe_get('cpf', clean_func=limpar_cpf),
        'codigo_externo': safe_get('codigo_externo') or safe_get('Código Externo') or safe_get('CODIGO EXTERNO'),
        'numero_ordem': safe_get('numero_ordem') or safe_get('Número Ordem') or safe_get('NUMERO ORDEM') or safe_get('Numero Pedido') or safe_get('NUMERO PEDIDO'),
        'numero_acesso': safe_get('numero_acesso') or safe_get('Número Acesso') or safe_get('NUMERO ACESSO'),
        'numero_linha': safe_get('numero_linha') or safe_get('Número Linha') or safe_get('NUMERO LINHA') or safe_get('Numero Linha Provisoria') or safe_get('NUMERO LINHA PROVISORIA'),
        'remessa_bluechip': safe_get('remessa_bluechip') or safe_get('Remessa Bluechip') or safe_get('REMESSA BLUECHIP') or safe_get('Numero Remessa') or safe_get('NUMERO REMESSA'),
        'pedido_bluechip': safe_get('pedido_bluechip') or safe_get('Pedido Bluechip') or safe_get('PEDIDO BLUECHIP'),
        'cliente_nome': safe_get('cliente_nome') or safe_get('Cliente Nome') or safe_get('CLIENTE NOME'),
        'endereco': safe_get('endereco') or safe_get('Endereço') or safe_get('ENDERECO'),
        'numero': safe_get('numero') or safe_get('Número') or safe_get('NUMERO'),
        'complemento': safe_get('complemento') or safe_get('Complemento'),
        'bairro': safe_get('bairro') or safe_get('Bairro'),
        'cidade': safe_get('cidade') or safe_get('Cidade'),
        'uf': safe_get('uf') or safe_get('UF'),
        'cep': safe_get('cep') or safe_get('CEP'),
        'telefone_portado': safe_get('telefone_portado') or safe_get('Telefone Portado') or safe_get('TELEFONE PORTADO'),
        'plano': safe_get('plano') or safe_get('Plano'),
        'produto_vendido': safe_get('produto_vendido') or safe_get('Produto Vendido') or safe_get('PRODUTO VENDIDO'),
        'data_venda': safe_get('data_venda') or safe_get('Data Venda') or safe_get('DATA VENDA'),
    }
    
    # Garantir que temos pelo menos id_proposta_isize ou ID_ISIZE
    # Prioridade 1: ID_ISIZE do Excel (se existir e for válido - não pode ser CPF)
    if dados.get('ID_ISIZE'):
        id_isize = str(dados['ID_ISIZE']).strip()
        # Verificar se não é um CPF (CPF tem 11 dígitos, id_proposta_isize geralmente tem menos)
        if len(id_isize) <= 15 and not (len(id_isize) == 11 and id_isize.isdigit()):
            dados['id_proposta_isize'] = id_isize
            logger.debug(f"id_proposta_isize do Excel (ID_ISIZE): {id_isize}")
    
    # Prioridade 2: id_proposta_isize direto do Excel (se existir)
    if dados.get('id_proposta_isize'):
        id_proposta = str(dados['id_proposta_isize']).strip()
        # Verificar se não é um CPF
        if len(id_proposta) <= 15 and not (len(id_proposta) == 11 and id_proposta.isdigit()):
            logger.debug(f"id_proposta_isize do Excel: {id_proposta}")
        else:
            # Se parece CPF, limpar para buscar na base_coverte_prop
            dados['id_proposta_isize'] = None
    
    # Prioridade 3: Buscar na base_coverte_prop se db_manager foi fornecido
    if db_manager and not dados.get('id_proposta_isize'):
        id_proposta_encontrado = buscar_id_proposta_isize_base_coverte(db_manager, dados)
        if id_proposta_encontrado:
            # Validar que não é CPF (CPF tem 11 dígitos)
            id_proposta_str = str(id_proposta_encontrado).strip()
            if len(id_proposta_str) <= 15 and not (len(id_proposta_str) == 11 and id_proposta_str.isdigit()):
                dados['id_proposta_isize'] = id_proposta_encontrado
                logger.debug(f"id_proposta_isize encontrado na base_coverte_prop: {id_proposta_encontrado}")
            else:
                logger.warning(f"Valor encontrado parece ser CPF, não id_proposta_isize: {id_proposta_encontrado}")
    
    # Se ainda não tem id_proposta_isize válido, ignorar registro
    if not dados.get('id_proposta_isize'):
        logger.warning("Registro sem id_proposta_isize válido - será ignorado")
        return None
    
    # Remover valores None/vazios
    return {k: v for k, v in dados.items() if v is not None}


def validar_registro(dados: Dict[str, Any], db_manager: DatabaseManager) -> Dict[str, Any]:
    """
    Valida registro com validações cruzadas:
    - STATUS
    - MOTIVO_CONFLITO
    - MOTIVO_CANCELAMENTO
    
    Args:
        dados: Dicionário com dados do registro
        db_manager: Instância do DatabaseManager
        
    Returns:
        Dicionário com dados validados e possíveis conflitos identificados
    """
    validacoes = {
        'tem_conflito': False,
        'conflitos': [],
        'status_valido': True,
        'pode_processar': True
    }
    
    # Validar STATUS
    status = dados.get('STATUS', '').strip().upper() if dados.get('STATUS') else ''
    if not status:
        validacoes['conflitos'].append('STATUS não informado')
        validacoes['status_valido'] = False
    
    # Validar MOTIVO_CONFLITO se STATUS indicar conflito
    motivo_conflito = dados.get('MOTIVO_CONFLITO', '').strip() if dados.get('MOTIVO_CONFLITO') else ''
    if status in ['CONFLITO', 'ERRO', 'PENDENTE', 'BLOQUEADO'] and not motivo_conflito:
        validacoes['conflitos'].append('STATUS indica conflito mas MOTIVO_CONFLITO não informado')
        validacoes['tem_conflito'] = True
    
    # Validar MOTIVO_CANCELAMENTO se STATUS indicar cancelamento
    motivo_cancelamento = dados.get('MOTIVO_CANCELAMENTO', '').strip() if dados.get('MOTIVO_CANCELAMENTO') else ''
    if status in ['CANCELADO', 'CANCELAMENTO'] and not motivo_cancelamento:
        validacoes['conflitos'].append('STATUS indica cancelamento mas MOTIVO_CANCELAMENTO não informado')
        validacoes['tem_conflito'] = True
    
    # Verificar duplicatas no banco (mesmo id_proposta_isize)
    id_proposta = dados.get('id_proposta_isize')
    if id_proposta:
        with db_manager._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, STATUS, MOTIVO_CONFLITO, MOTIVO_CANCELAMENTO, data_importacao
                FROM portabilidade_processamento
                WHERE id_proposta_isize = ?
                ORDER BY data_importacao DESC
                LIMIT 1
            """, (id_proposta,))
            existing = cursor.fetchone()
            
            if existing:
                existing_status = existing['STATUS'] if existing['STATUS'] else ''
                existing_motivo_conflito = existing['MOTIVO_CONFLITO'] if existing['MOTIVO_CONFLITO'] else ''
                existing_motivo_cancelamento = existing['MOTIVO_CANCELAMENTO'] if existing['MOTIVO_CANCELAMENTO'] else ''
                
                # Verificar se há mudança de status que pode indicar conflito
                if existing_status and status and existing_status != status:
                    validacoes['conflitos'].append(
                        f'Mudança de STATUS detectada: {existing_status} -> {status}'
                    )
                    validacoes['tem_conflito'] = True
                
                # Verificar se há conflito não resolvido anteriormente
                if existing_motivo_conflito and not motivo_conflito:
                    validacoes['conflitos'].append(
                        f'Conflito anterior não resolvido: {existing_motivo_conflito}'
                    )
                    validacoes['tem_conflito'] = True
    
    # Atualizar MOTIVO_CONFLITO se houver conflitos identificados
    if validacoes['tem_conflito']:
        conflitos_str = '; '.join(validacoes['conflitos'])
        if not dados.get('MOTIVO_CONFLITO'):
            dados['MOTIVO_CONFLITO'] = conflitos_str
        else:
            dados['MOTIVO_CONFLITO'] = f"{dados['MOTIVO_CONFLITO']}; {conflitos_str}"
    
    # Determinar se pode processar
    validacoes['pode_processar'] = validacoes['status_valido'] and not validacoes['tem_conflito']
    
    return dados, validacoes


def processar_bp_fechados(
    arquivo_excel: Optional[Path] = None,
    db_path: str = DB_PATH
) -> Dict[str, int]:
    """
    Processa arquivo Excel BP_FECHADOS_TIM_OFICIAL.xlsx e insere no banco de dados
    
    Args:
        arquivo_excel: Caminho para o arquivo Excel (se None, busca em /Applications/Documentos/IMPORTACOES_QIGGER)
        db_path: Caminho para o banco de dados
        
    Returns:
        Estatísticas do processamento
    """
    stats = {
        'total_linhas': 0,
        'processados': 0,
        'inseridos': 0,
        'atualizados': 0,
        'erros': 0,
        'ignorados': 0,
        'com_conflitos': 0,
        'validados': 0
    }
    
    # Buscar arquivo se não foi fornecido
    if arquivo_excel is None:
        # Tentar primeiro na pasta de importações
        pasta_importacoes = Path(PASTA_IMPORTACOES) if PASTA_IMPORTACOES else Path("/Applications/Documentos/IMPORTACOES_QIGGER")
        arquivo_excel = pasta_importacoes / "BP_FECHADOS_TIM_OFICIAL.xlsx"
        
        # Se não encontrar, tentar na pasta do projeto
        if not arquivo_excel.exists():
            arquivo_excel = PROJECT_ROOT / "BP_FECHADOS_TIM_OFICIAL.xlsx"
    
    if not arquivo_excel.exists():
        logger.error(f"Arquivo não encontrado: {arquivo_excel}")
        logger.info(f"Procurando em: {Path(PASTA_IMPORTACOES) if PASTA_IMPORTACOES else '/Applications/Documentos/IMPORTACOES_QIGGER'}")
        logger.info(f"E também em: {PROJECT_ROOT}")
        return stats
    
    logger.info(f"Processando arquivo Excel: {arquivo_excel.name}")
    
    # Inicializar banco de dados
    logger.info("="*70)
    logger.info(f"BANCO DE DADOS: {db_path}")
    logger.info("="*70)
    db_manager = DatabaseManager(db_path)
    
    # Verificar se banco existe e tabela portabilidade_processamento foi criada
    with db_manager._get_connection() as conn:
        cursor = conn.cursor()
        
        # Verificar se tabela portabilidade_processamento existe
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='portabilidade_processamento'
        """)
        tem_tabela_processamento = cursor.fetchone() is not None
        
        if tem_tabela_processamento:
            cursor.execute("SELECT COUNT(*) FROM portabilidade_processamento")
            total_processamento = cursor.fetchone()[0]
            logger.info(f"✓ Tabela portabilidade_processamento encontrada com {total_processamento:,} registros")
        else:
            logger.warning("⚠ Tabela portabilidade_processamento não encontrada - será criada automaticamente")
        
        # Verificar se base_coverte_prop existe para busca de associação
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='base_coverte_prop'
        """)
        tem_base_coverte = cursor.fetchone() is not None
        if tem_base_coverte:
            cursor.execute("SELECT COUNT(*) FROM base_coverte_prop")
            total_base = cursor.fetchone()[0]
            logger.info(f"✓ Tabela base_coverte_prop encontrada com {total_base:,} registros - será usada para associação")
        else:
            logger.warning("⚠ Tabela base_coverte_prop não encontrada - associação por id_proposta_isize não será feita")
        
        # Listar todas as tabelas do banco
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name NOT LIKE 'sqlite_%'
            ORDER BY name
        """)
        tabelas = [row[0] for row in cursor.fetchall()]
        logger.info(f"Tabelas disponíveis no banco: {', '.join(tabelas) if tabelas else 'Nenhuma'}")
    
    try:
        # Ler arquivo Excel
        logger.info("Lendo arquivo Excel...")
        df = pd.read_excel(arquivo_excel, engine='openpyxl')
        
        if df.empty:
            logger.warning("Arquivo Excel está vazio")
            return stats
        
        stats['total_linhas'] = len(df)
        logger.info(f"Total de linhas no Excel: {stats['total_linhas']}")
        
        # Processar cada linha com barra de progresso
        print(f"\n[3] Processando {stats['total_linhas']} registros...")
        with db_manager._get_connection() as conn:
            cursor = conn.cursor()
            
            with ProgressBar(
                total=stats['total_linhas'],
                desc="Processando BP_FECHADOS",
                unit="linhas",
                logger=logger,
                log_interval_pct=10.0
            ) as pbar:
                for idx, row in df.iterrows():
                    try:
                        # Mapear campos (passando db_manager para buscar na base_coverte_prop)
                        dados = mapear_campos_excel(row, db_manager)
                        
                        if not dados:
                            stats['ignorados'] += 1
                            pbar.update(1)
                            continue
                        
                        # Validar registro
                        dados_validados, validacoes = validar_registro(dados, db_manager)
                        
                        if validacoes['tem_conflito']:
                            stats['com_conflitos'] += 1
                        
                        if validacoes['pode_processar']:
                            stats['validados'] += 1
                        
                        # Definir data_importacao
                        dados_validados['data_importacao'] = datetime.now().isoformat()
                        
                        # Verificar se registro já existe
                        id_proposta = dados_validados.get('id_proposta_isize')
                        if id_proposta:
                            cursor.execute("""
                                SELECT id FROM portabilidade_processamento
                                WHERE id_proposta_isize = ?
                                ORDER BY data_importacao DESC
                                LIMIT 1
                            """, (id_proposta,))
                            
                            existing = cursor.fetchone()
                            
                            if existing:
                                # Atualizar registro existente
                                campos_update = []
                                valores_update = []
                                
                                for campo, valor in dados_validados.items():
                                    if campo not in ['id', 'id_proposta_isize', 'data_importacao', 'created_at']:
                                        campos_update.append(f"{campo} = ?")
                                        valores_update.append(valor)
                                
                                valores_update.append(datetime.now().isoformat())  # updated_at
                                valores_update.append(existing['id'])  # WHERE id
                                
                                query_update = f"""
                                    UPDATE portabilidade_processamento SET
                                        {', '.join(campos_update)},
                                        updated_at = ?
                                    WHERE id = ?
                                """
                                
                                cursor.execute(query_update, valores_update)
                                stats['atualizados'] += 1
                            else:
                                # Inserir novo registro
                                campos = list(dados_validados.keys())
                                placeholders = ', '.join(['?' for _ in campos])
                                valores = list(dados_validados.values())
                                
                                query_insert = f"""
                                    INSERT INTO portabilidade_processamento ({', '.join(campos)})
                                    VALUES ({placeholders})
                                """
                                
                                cursor.execute(query_insert, valores)
                                stats['inseridos'] += 1
                            
                            stats['processados'] += 1
                        else:
                            stats['ignorados'] += 1
                            logger.warning(f"Registro não pode ser processado devido a validações: {validacoes['conflitos']}")
                        
                        # Atualizar barra de progresso
                        pbar.update(1)
                        pbar.set_postfix(
                            inseridos=stats['inseridos'],
                            atualizados=stats['atualizados'],
                            validados=stats['validados'],
                            erros=stats['erros']
                        )
                        
                    except Exception as e:
                        logger.error(f"Erro ao processar linha {idx + 1}: {e}", exc_info=True)
                        stats['erros'] += 1
                        pbar.update(1)
            
            conn.commit()
        
        # Verificar resultado final na tabela
        with db_manager._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM portabilidade_processamento")
            total_final = cursor.fetchone()[0]
            logger.info(f"✓ Total de registros na tabela portabilidade_processamento: {total_final:,}")
        
        logger.info(
            f"Processamento concluído: {stats['processados']} processados, "
            f"{stats['inseridos']} inseridos, {stats['atualizados']} atualizados, "
            f"{stats['com_conflitos']} com conflitos, {stats['validados']} validados, "
            f"{stats['erros']} erros, {stats['ignorados']} ignorados"
        )
        logger.info(f"Banco de dados: {db_path}")
        
    except Exception as e:
        logger.error(f"Erro ao processar arquivo Excel: {e}", exc_info=True)
        stats['erros'] = stats['total_linhas']
    
    return stats


def main():
    """Função principal"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Processar arquivo Excel BP_FECHADOS_TIM_OFICIAL.xlsx e atualizar tabela portabilidade_processamento"
    )
    parser.add_argument(
        '--arquivo',
        type=str,
        help='Caminho para o arquivo Excel (padrão: BP_FECHADOS_TIM_OFICIAL.xlsx em /Applications/Documentos/IMPORTACOES_QIGGER ou pasta do projeto)'
    )
    parser.add_argument(
        '--db',
        type=str,
        default=DB_PATH,
        help=f'Caminho para o banco de dados (padrão: {DB_PATH})'
    )
    
    args = parser.parse_args()
    
    # Exibir informações do banco de dados
    print("\n" + "="*70)
    print("CONFIGURAÇÃO DO BANCO DE DADOS")
    print("="*70)
    print(f"Banco de dados: {args.db}")
    print(f"Arquivo existe: {'✓ SIM' if Path(args.db).exists() else '✗ NÃO'}")
    if Path(args.db).exists():
        tamanho_mb = Path(args.db).stat().st_size / (1024 * 1024)
        print(f"Tamanho: {tamanho_mb:.2f} MB")
    print("="*70 + "\n")
    
    arquivo = Path(args.arquivo) if args.arquivo else None
    
    stats = processar_bp_fechados(arquivo_excel=arquivo, db_path=args.db)
    
    print("\n" + "="*60)
    print("RESUMO DO PROCESSAMENTO")
    print("="*60)
    print(f"Banco de dados: {args.db}")
    print(f"Total de linhas no Excel: {stats['total_linhas']}")
    print(f"Processados: {stats['processados']}")
    print(f"Inseridos: {stats['inseridos']}")
    print(f"Atualizados: {stats['atualizados']}")
    print(f"Validados: {stats['validados']}")
    print(f"Com conflitos: {stats['com_conflitos']}")
    print(f"Erros: {stats['erros']}")
    print(f"Ignorados: {stats['ignorados']}")
    
    # Verificar tabela final
    try:
        db_manager = DatabaseManager(args.db)
        with db_manager._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM portabilidade_processamento")
            total_final = cursor.fetchone()[0]
            print(f"\nTotal de registros na tabela portabilidade_processamento: {total_final:,}")
    except Exception as e:
        print(f"\n⚠ Erro ao verificar tabela: {e}")
    
    print("="*60)


if __name__ == "__main__":
    main()
