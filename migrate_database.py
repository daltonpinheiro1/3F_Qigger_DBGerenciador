"""
Script de Migração do Banco de Dados
Migra dados existentes para a nova estrutura com suporte a triggers.xlsx
"""
import sys
import logging
from pathlib import Path

# Adicionar src ao path
sys.path.insert(0, str(Path(__file__).parent))

from src.database.db_manager import DatabaseManager
from src.engine.trigger_loader import TriggerLoader
from src.models.portabilidade import PortabilidadeRecord, PortabilidadeStatus, StatusOrdem

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def migrate_existing_records(db_manager: DatabaseManager, trigger_loader: TriggerLoader):
    """
    Migra registros existentes aplicando regras do triggers.xlsx
    """
    logger.info("Iniciando migração de registros existentes...")
    
    # Carregar regras
    rules = trigger_loader.load_rules()
    logger.info(f"Carregadas {len(rules)} regras do triggers.xlsx")
    
    # Sincronizar regras com o banco
    db_manager.sync_triggers_from_loader(rules)
    
    # Buscar todos os registros que ainda não foram mapeados
    records = db_manager.get_all_records()
    logger.info(f"Encontrados {len(records)} registros para processar")
    
    mapped_count = 0
    unmapped_count = 0
    
    for record_data in records:
        try:
            # Pular se já foi mapeado (regra_id não é None)
            if record_data.get('regra_id') is not None:
                continue
            
            # Criar PortabilidadeRecord a partir dos dados
            record = _record_from_dict(record_data)
            
            # Tentar encontrar regra correspondente
            rule = trigger_loader.find_matching_rule(record)
            
            if rule:
                # Aplicar regra ao registro
                record.apply_trigger_rule(rule)
                mapped_count += 1
            else:
                # Marcar como não mapeado
                record.mark_as_unmapped()
                unmapped_count += 1
            
            # Atualizar no banco
            _update_record_in_db(db_manager, record_data['id'], record)
            
        except Exception as e:
            logger.error(f"Erro ao processar registro {record_data.get('id')}: {e}")
            continue
    
    logger.info(f"Migração concluída: {mapped_count} mapeados, {unmapped_count} não mapeados")
    return mapped_count, unmapped_count


def _record_from_dict(data: dict) -> PortabilidadeRecord:
    """Cria um PortabilidadeRecord a partir de um dicionário do banco"""
    
    # Parse status_bilhete
    status_bilhete = None
    if data.get('status_bilhete'):
        for status in PortabilidadeStatus:
            if status.value == data['status_bilhete']:
                status_bilhete = status
                break
    
    # Parse status_ordem
    status_ordem = None
    if data.get('status_ordem'):
        for status in StatusOrdem:
            if status.value == data['status_ordem']:
                status_ordem = status
                break
    
    # Parse ultimo_bilhete
    ultimo_bilhete = None
    if data.get('ultimo_bilhete') is not None:
        ultimo_bilhete = bool(data['ultimo_bilhete'])
    
    return PortabilidadeRecord(
        cpf=data.get('cpf', ''),
        numero_acesso=data.get('numero_acesso', ''),
        numero_ordem=data.get('numero_ordem', ''),
        codigo_externo=data.get('codigo_externo', ''),
        numero_temporario=data.get('numero_temporario'),
        bilhete_temporario=data.get('bilhete_temporario'),
        numero_bilhete=data.get('numero_bilhete'),
        status_bilhete=status_bilhete,
        operadora_doadora=data.get('operadora_doadora'),
        motivo_recusa=data.get('motivo_recusa'),
        motivo_cancelamento=data.get('motivo_cancelamento'),
        ultimo_bilhete=ultimo_bilhete,
        status_ordem=status_ordem,
        preco_ordem=data.get('preco_ordem'),
        motivo_nao_consultado=data.get('motivo_nao_consultado'),
        responsavel_processamento=data.get('responsavel_processamento'),
        registro_valido=bool(data.get('registro_valido')) if data.get('registro_valido') is not None else None,
    )


def _update_record_in_db(db_manager: DatabaseManager, record_id: int, record: PortabilidadeRecord):
    """Atualiza um registro existente no banco de dados"""
    import sqlite3
    
    with db_manager._get_connection() as conn:
        cursor = conn.cursor()
        data = record.to_dict()
        
        cursor.execute("""
            UPDATE portabilidade_records SET
                regra_id = ?,
                o_que_aconteceu = ?,
                acao_a_realizar = ?,
                tipo_mensagem = ?,
                template = ?,
                mapeado = ?,
                novo_status_bilhete_trigger = ?,
                ajustes_numero_acesso_trigger = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (
            data['regra_id'],
            data['o_que_aconteceu'],
            data['acao_a_realizar'],
            data['tipo_mensagem'],
            data['template'],
            data['mapeado'],
            data['novo_status_bilhete_trigger'],
            data['ajustes_numero_acesso_trigger'],
            record_id
        ))
        
        conn.commit()


def print_statistics(db_manager: DatabaseManager):
    """Imprime estatísticas após a migração"""
    stats = db_manager.get_statistics()
    
    print("\n" + "="*60)
    print("ESTATÍSTICAS DO BANCO DE DADOS")
    print("="*60)
    print(f"Total de registros: {stats['total_registros']}")
    print(f"Registros mapeados: {stats['registros_mapeados']}")
    print(f"Registros não mapeados: {stats['registros_nao_mapeados']}")
    
    if stats['por_tipo_mensagem']:
        print("\nPor Tipo de Mensagem:")
        for tipo, count in stats['por_tipo_mensagem'].items():
            print(f"  - {tipo}: {count}")
    
    if stats['por_acao']:
        print("\nPor Ação a Realizar:")
        for acao, count in stats['por_acao'].items():
            print(f"  - {acao}: {count}")
    
    print("="*60 + "\n")


def main():
    """Função principal de migração"""
    print("="*60)
    print("MIGRAÇÃO DO BANCO DE DADOS - Qigger DB Gerenciador")
    print("="*60)
    
    # Verificar se arquivo de triggers existe
    triggers_path = Path("triggers.xlsx")
    if not triggers_path.exists():
        logger.error("Arquivo triggers.xlsx não encontrado!")
        print("ERRO: Arquivo triggers.xlsx não encontrado!")
        return 1
    
    try:
        # Inicializar componentes
        print("\n[1/4] Inicializando banco de dados...")
        db_manager = DatabaseManager("data/portabilidade.db")
        
        print("[2/4] Carregando regras do triggers.xlsx...")
        trigger_loader = TriggerLoader("triggers.xlsx")
        
        print("[3/4] Executando migração de dados...")
        mapped, unmapped = migrate_existing_records(db_manager, trigger_loader)
        
        print("[4/4] Otimizando banco de dados...")
        db_manager.optimize()
        
        print("\n✓ Migração concluída com sucesso!")
        print(f"  - Registros mapeados: {mapped}")
        print(f"  - Registros não mapeados: {unmapped}")
        
        # Exibir estatísticas
        print_statistics(db_manager)
        
        # Verificar registros não mapeados
        unmapped_records = db_manager.get_unmapped_records()
        if unmapped_records:
            print("\n⚠ ATENÇÃO: Existem combinações não mapeadas:")
            print("-"*60)
            for record in unmapped_records[:10]:  # Mostrar apenas top 10
                print(f"  Status: {record.get('status_bilhete')}")
                print(f"  Operadora: {record.get('operadora_doadora')}")
                print(f"  Motivo Recusa: {record.get('motivo_recusa')}")
                print(f"  Ocorrências: {record.get('count')}")
                print("-"*40)
            
            if len(unmapped_records) > 10:
                print(f"  ... e mais {len(unmapped_records) - 10} combinações")
            
            print("\nEssas combinações foram adicionadas ao triggers.xlsx para revisão.")
        
        return 0
        
    except Exception as e:
        logger.error(f"Erro durante migração: {e}", exc_info=True)
        print(f"\n✗ Erro durante migração: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
