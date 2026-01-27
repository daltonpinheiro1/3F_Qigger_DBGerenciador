"""
Script para corrigir id_proposta_isize incorreto na tabela portabilidade_processamento

Este script:
1. Identifica registros com id_proposta_isize que parece ser CPF (11 dígitos)
2. Busca o id_proposta_isize correto na tabela base_coverte_prop
3. Atualiza os registros com o valor correto
"""
import sys
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List
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
        logging.FileHandler('logs/corrigir_id_proposta_isize.log', encoding='utf-8'),
        console_handler
    ]
)

logger = logging.getLogger(__name__)

from src.database.db_manager import DatabaseManager
from config import DB_PATH


def limpar_cpf(cpf: Any) -> Optional[str]:
    """Limpa CPF removendo caracteres não numéricos"""
    if cpf is None:
        return None
    cpf_str = ''.join(c for c in str(cpf) if c.isdigit())
    return cpf_str if len(cpf_str) >= 11 else None


def buscar_id_proposta_isize_correto(
    db_manager: DatabaseManager,
    registro: Dict[str, Any]
) -> Optional[str]:
    """
    Busca id_proposta_isize correto na tabela base_coverte_prop
    
    Args:
        db_manager: Instância do DatabaseManager
        registro: Dicionário com dados do registro da portabilidade_processamento
        
    Returns:
        id_proposta_isize correto ou None
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
        cpf = registro.get('cpf') or registro.get('CPF_CNPJ')
        numero_ordem = registro.get('numero_ordem') or registro.get('NUMERO_ORDEM')
        numero_acesso = registro.get('numero_acesso') or registro.get('ACESSO') or registro.get('ACESSO_TEMPORARIO')
        numero_linha = registro.get('numero_linha')
        remessa_bluechip = registro.get('remessa_bluechip')
        pedido_bluechip = registro.get('pedido_bluechip')
        codigo_externo = registro.get('codigo_externo')
        telefone_portado = registro.get('telefone_portado')
        
        # Limpar CPF
        if cpf:
            cpf_limpo = limpar_cpf(cpf)
        else:
            cpf_limpo = None
        
        # Estratégia 1: Buscar por CPF/CNPJ
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
                # Validar que não é CPF
                if len(proposta_isize) <= 15 and not (len(proposta_isize) == 11 and proposta_isize.isdigit()):
                    logger.debug(f"Encontrado id_proposta_isize por CPF: {proposta_isize}")
                    return proposta_isize
            if row and row[1]:
                codigo = str(row[1]).strip()
                if len(codigo) <= 15 and not (len(codigo) == 11 and codigo.isdigit()):
                    logger.debug(f"Encontrado codigo_externo por CPF: {codigo}")
                    return codigo
        
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
                if len(proposta_isize) <= 15 and not (len(proposta_isize) == 11 and proposta_isize.isdigit()):
                    logger.debug(f"Encontrado id_proposta_isize por número de ordem: {proposta_isize}")
                    return proposta_isize
            if row and row[1]:
                codigo = str(row[1]).strip()
                if len(codigo) <= 15 and not (len(codigo) == 11 and codigo.isdigit()):
                    logger.debug(f"Encontrado codigo_externo por número de ordem: {codigo}")
                    return codigo
        
        # Estratégia 3: Buscar por acesso provisório
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
                if len(proposta_isize) <= 15 and not (len(proposta_isize) == 11 and proposta_isize.isdigit()):
                    logger.debug(f"Encontrado id_proposta_isize por acesso provisório: {proposta_isize}")
                    return proposta_isize
            if row and row[1]:
                codigo = str(row[1]).strip()
                if len(codigo) <= 15 and not (len(codigo) == 11 and codigo.isdigit()):
                    logger.debug(f"Encontrado codigo_externo por acesso provisório: {codigo}")
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
                proposta_isize = str(row[0]).strip()
                if len(proposta_isize) <= 15 and not (len(proposta_isize) == 11 and proposta_isize.isdigit()):
                    logger.debug(f"Encontrado id_proposta_isize por número de linha: {proposta_isize}")
                    return proposta_isize
            if row and row[1]:
                codigo = str(row[1]).strip()
                if len(codigo) <= 15 and not (len(codigo) == 11 and codigo.isdigit()):
                    logger.debug(f"Encontrado codigo_externo por número de linha: {codigo}")
                    return codigo
        
        # Estratégia 4: Buscar por número de remessa
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
                if len(proposta_isize) <= 15 and not (len(proposta_isize) == 11 and proposta_isize.isdigit()):
                    logger.debug(f"Encontrado id_proposta_isize por remessa: {proposta_isize}")
                    return proposta_isize
            if row and row[1]:
                codigo = str(row[1]).strip()
                if len(codigo) <= 15 and not (len(codigo) == 11 and codigo.isdigit()):
                    logger.debug(f"Encontrado codigo_externo por remessa: {codigo}")
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
                if len(proposta_isize) <= 15 and not (len(proposta_isize) == 11 and proposta_isize.isdigit()):
                    logger.debug(f"Encontrado id_proposta_isize por pedido bluechip: {proposta_isize}")
                    return proposta_isize
            if row and row[1]:
                codigo = str(row[1]).strip()
                if len(codigo) <= 15 and not (len(codigo) == 11 and codigo.isdigit()):
                    logger.debug(f"Encontrado codigo_externo por pedido bluechip: {codigo}")
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
                proposta_isize = str(row[0]).strip()
                if len(proposta_isize) <= 15 and not (len(proposta_isize) == 11 and proposta_isize.isdigit()):
                    logger.debug(f"Encontrado id_proposta_isize por código externo: {proposta_isize}")
                    return proposta_isize
            if row and row[1]:
                codigo = str(row[1]).strip()
                if len(codigo) <= 15 and not (len(codigo) == 11 and codigo.isdigit()):
                    logger.debug(f"Encontrado codigo_externo: {codigo}")
                    return codigo
        
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
                    if len(proposta_isize) <= 15 and not (len(proposta_isize) == 11 and proposta_isize.isdigit()):
                        logger.debug(f"Encontrado id_proposta_isize por telefone portado: {proposta_isize}")
                        return proposta_isize
                if row and row[1]:
                    codigo = str(row[1]).strip()
                    if len(codigo) <= 15 and not (len(codigo) == 11 and codigo.isdigit()):
                        logger.debug(f"Encontrado codigo_externo por telefone portado: {codigo}")
                        return codigo
        
        return None


def corrigir_id_proposta_isize(
    db_path: str = DB_PATH,
    dry_run: bool = False
) -> Dict[str, int]:
    """
    Corrige id_proposta_isize incorreto na tabela portabilidade_processamento
    
    Args:
        db_path: Caminho para o banco de dados
        dry_run: Se True, apenas identifica problemas sem corrigir
        
    Returns:
        Estatísticas da correção
    """
    stats = {
        'total_registros': 0,
        'com_id_incorreto': 0,
        'corrigidos': 0,
        'nao_encontrados': 0,
        'erros': 0
    }
    
    logger.info("="*70)
    logger.info("CORREÇÃO DE id_proposta_isize")
    logger.info("="*70)
    logger.info(f"Banco de dados: {db_path}")
    logger.info(f"Modo: {'DRY RUN (simulação)' if dry_run else 'CORREÇÃO REAL'}")
    logger.info("="*70)
    
    db_manager = DatabaseManager(db_path)
    
    # Verificar se tabela existe
    with db_manager._get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='portabilidade_processamento'
        """)
        if not cursor.fetchone():
            logger.error("Tabela portabilidade_processamento não existe!")
            return stats
        
        # Buscar todos os registros
        cursor.execute("SELECT * FROM portabilidade_processamento")
        colunas = [desc[0] for desc in cursor.description]
        registros = cursor.fetchall()
        
        stats['total_registros'] = len(registros)
        logger.info(f"Total de registros na tabela: {stats['total_registros']}")
        
        # Identificar registros com id_proposta_isize incorreto (11 dígitos = CPF)
        registros_incorretos = []
        for row in registros:
            registro = dict(zip(colunas, row))
            id_proposta = registro.get('id_proposta_isize')
            
            if id_proposta:
                id_proposta_str = str(id_proposta).strip()
                # Verificar se parece CPF (11 dígitos)
                if len(id_proposta_str) == 11 and id_proposta_str.isdigit():
                    registros_incorretos.append(registro)
        
        stats['com_id_incorreto'] = len(registros_incorretos)
        logger.info(f"Registros com id_proposta_isize incorreto (CPF): {stats['com_id_incorreto']}")
        
        if stats['com_id_incorreto'] == 0:
            logger.info("✓ Nenhum registro com id_proposta_isize incorreto encontrado!")
            return stats
        
        # Processar cada registro incorreto
        logger.info(f"\nProcessando {stats['com_id_incorreto']} registros...")
        
        for idx, registro in enumerate(registros_incorretos, 1):
            try:
                id_proposta_antigo = registro.get('id_proposta_isize')
                
                logger.info(f"\n[{idx}/{stats['com_id_incorreto']}] Registro ID: {registro.get('id')}")
                logger.info(f"  id_proposta_isize atual (incorreto): {id_proposta_antigo}")
                
                # Buscar id_proposta_isize correto
                id_proposta_correto = buscar_id_proposta_isize_correto(db_manager, registro)
                
                if id_proposta_correto:
                    logger.info(f"  id_proposta_isize correto encontrado: {id_proposta_correto}")
                    
                    if not dry_run:
                        # Atualizar registro
                        cursor.execute("""
                            UPDATE portabilidade_processamento
                            SET id_proposta_isize = ?, updated_at = ?
                            WHERE id = ?
                        """, (id_proposta_correto, datetime.now().isoformat(), registro.get('id')))
                        conn.commit()
                        logger.info(f"  ✓ Registro atualizado com sucesso")
                        stats['corrigidos'] += 1
                    else:
                        logger.info(f"  [DRY RUN] Seria atualizado para: {id_proposta_correto}")
                        stats['corrigidos'] += 1
                else:
                    logger.warning(f"  ⚠ Não foi possível encontrar id_proposta_isize correto")
                    stats['nao_encontrados'] += 1
                    
            except Exception as e:
                logger.error(f"  ✗ Erro ao processar registro ID {registro.get('id')}: {e}", exc_info=True)
                stats['erros'] += 1
        
        logger.info("\n" + "="*70)
        logger.info("RESUMO DA CORREÇÃO")
        logger.info("="*70)
        logger.info(f"Total de registros: {stats['total_registros']}")
        logger.info(f"Com id_proposta_isize incorreto: {stats['com_id_incorreto']}")
        logger.info(f"{'Simulados' if dry_run else 'Corrigidos'}: {stats['corrigidos']}")
        logger.info(f"Não encontrados: {stats['nao_encontrados']}")
        logger.info(f"Erros: {stats['erros']}")
        logger.info("="*70)
    
    return stats


def main():
    """Função principal"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Corrigir id_proposta_isize incorreto na tabela portabilidade_processamento"
    )
    parser.add_argument(
        '--db',
        type=str,
        default=DB_PATH,
        help=f'Caminho para o banco de dados (padrão: {DB_PATH})'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Modo simulação (não faz alterações, apenas identifica problemas)'
    )
    
    args = parser.parse_args()
    
    print("\n" + "="*70)
    print("CORREÇÃO DE id_proposta_isize")
    print("="*70)
    print(f"Banco de dados: {args.db}")
    print(f"Modo: {'DRY RUN (simulação)' if args.dry_run else 'CORREÇÃO REAL'}")
    print("="*70 + "\n")
    
    if not args.dry_run:
        resposta = input("⚠️  ATENÇÃO: Isso irá modificar o banco de dados. Deseja continuar? (s/N): ")
        if resposta.lower() != 's':
            print("Operação cancelada.")
            return
    
    stats = corrigir_id_proposta_isize(db_path=args.db, dry_run=args.dry_run)
    
    print("\n" + "="*70)
    print("RESUMO DA CORREÇÃO")
    print("="*70)
    print(f"Total de registros: {stats['total_registros']}")
    print(f"Com id_proposta_isize incorreto: {stats['com_id_incorreto']}")
    print(f"{'Simulados' if args.dry_run else 'Corrigidos'}: {stats['corrigidos']}")
    print(f"Não encontrados: {stats['nao_encontrados']}")
    print(f"Erros: {stats['erros']}")
    print("="*70)
    
    if args.dry_run and stats['corrigidos'] > 0:
        print("\n💡 Dica: Execute sem --dry-run para aplicar as correções")


if __name__ == "__main__":
    main()
