"""
Script principal para processar arquivos de importação automaticamente
Processa arquivos da pasta IMPORTACOES_QIGGER e exclui após processamento
"""
import sys
import os
from pathlib import Path
from datetime import datetime

# Configurar encoding UTF-8
from src.utils.console_utils import setup_windows_console
setup_windows_console()

import logging
import io

# Configurar logging
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
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/processamento_importacoes.log', encoding='utf-8'),
        console_handler
    ]
)

logger = logging.getLogger(__name__)

from src.database.db_manager import DatabaseManager
from src.engine.qigger_decision_engine import QiggerDecisionEngine
from src.utils.csv_parser import CSVParser
from src.utils.objects_loader import ObjectsLoader
from src.utils.templates_wpp import TemplateMapper, TEMPLATES
from src.models.portabilidade import PortabilidadeRecord
import csv

# Caminhos
PASTA_IMPORTACAO = Path(r"C:\Users\dspin\OneDrive\Documents\IMPORTACOES_QIGGER")
DB_PATH = "data/portabilidade.db"
TRIGGERS_PATH = "triggers.xlsx"
AMOSTRA_OUTPUT = Path("data/amostra_validacao_templates.csv")


def encontrar_arquivo(pasta: Path, extensao: str) -> Path:
    """Encontra o arquivo mais recente com a extensão especificada"""
    arquivos = list(pasta.glob(f"*{extensao}"))
    if not arquivos:
        return None
    return max(arquivos, key=lambda x: x.stat().st_mtime)


def processar_arquivos():
    """Processa os arquivos de importação"""
    
    print("=" * 70)
    print("PROCESSAMENTO AUTOMÁTICO DE IMPORTAÇÕES")
    print("=" * 70)
    print(f"Data/Hora: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print()
    
    # Verificar se há arquivos para processar
    arquivo_csv = encontrar_arquivo(PASTA_IMPORTACAO, ".csv")
    arquivo_objetos = encontrar_arquivo(PASTA_IMPORTACAO, ".xlsx")
    
    if not arquivo_csv and not arquivo_objetos:
        print("Nenhum arquivo encontrado na pasta de importação.")
        print(f"Pasta: {PASTA_IMPORTACAO}")
        return
    
    # 1. Inicializar banco de dados
    print("[1] Inicializando banco de dados...")
    db_manager = DatabaseManager(DB_PATH)
    
    # Sincronizar templates
    print("[2] Sincronizando templates WPP...")
    db_manager.sync_templates_from_config()
    
    # 2. Carregar Relatório de Objetos
    objects_loader = None
    
    if arquivo_objetos:
        print(f"[3] Carregando Relatório de Objetos: {arquivo_objetos.name}")
        try:
            objects_loader = ObjectsLoader(str(arquivo_objetos))
            print(f"    >> {objects_loader.total_records} registros carregados")
        except Exception as e:
            print(f"    >> ERRO ao carregar: {e}")
            arquivo_objetos = None
    else:
        print("[3] Relatório de Objetos não encontrado")
    
    # 3. Inicializar Engine
    print("[4] Inicializando engine de decisão...")
    engine = QiggerDecisionEngine(
        db_manager=db_manager,
        triggers_path=TRIGGERS_PATH,
        objects_loader=objects_loader
    )
    
    # 4. Processar CSV de Portabilidade
    if not arquivo_csv:
        print("AVISO: Nenhum arquivo CSV encontrado!")
        print("Apenas o Relatório de Objetos será processado.")
        return
    
    print(f"[5] Processando CSV: {arquivo_csv.name}")
    
    try:
        records = CSVParser.parse_file(str(arquivo_csv))
        print(f"    >> {len(records)} registros parseados")
    except Exception as e:
        print(f"ERRO ao parsear CSV: {e}")
        logger.error(f"Erro ao parsear CSV: {e}", exc_info=True)
        return
    
    # 5. Processar registros
    print("[6] Processando registros...")
    
    try:
        results_list = engine.process_records_batch(
            records, 
            generate_wpp_output=False,
            save_to_db=True
        )
        print(f"    >> {len(results_list)} registros processados")
    except Exception as e:
        print(f"ERRO ao processar registros: {e}")
        logger.error(f"Erro ao processar registros: {e}", exc_info=True)
        return
    
    # 6. Gerar amostra de validação
    print("[7] Gerando amostra de validação de templates...")
    
    amostra_data = []
    template_stats = {1: 0, 2: 0, 3: 0, 4: 0, 'sem_template': 0}
    
    for record, results in results_list:
        # Obter informações do template
        template_info = TemplateMapper.get_template_for_record(record)
        
        # Gerar link de rastreio
        cod_rastreio = record.cod_rastreio
        if not cod_rastreio or not str(cod_rastreio).startswith('http'):
            cod_rastreio = PortabilidadeRecord.gerar_link_rastreio(record.codigo_externo) or ''
        
        template_id = template_info.get('template_id')
        if template_id:
            template_stats[template_id] = template_stats.get(template_id, 0) + 1
        else:
            template_stats['sem_template'] += 1
        
        # Adicionar à amostra
        amostra_data.append({
            'CPF': record.cpf,
            'Codigo_Externo': record.codigo_externo,
            'Numero_Acesso': record.numero_acesso,
            'Status_Bilhete': record.status_bilhete.value if record.status_bilhete else '',
            'Status_Ordem': record.status_ordem.value if record.status_ordem else '',
            'Tipo_Mensagem': record.tipo_mensagem or '',
            'Template_Triggers': record.template or '',
            'Template_ID': template_id or '',
            'Template_Nome': template_info.get('nome_modelo') or '',
            'Template_Variaveis': TemplateMapper.format_variables_string(template_info.get('variaveis', {})),
            'Cod_Rastreio': cod_rastreio,
            'Nome_Cliente': record.nome_cliente or '',
            'Telefone': record.telefone_contato or record.numero_acesso,
            'Cidade': record.cidade or '',
            'UF': record.uf or '',
            'Mapeado': 'SIM' if record.mapeado else 'NAO',
            'Regra_ID': record.regra_id or '',
            'O_Que_Aconteceu': record.o_que_aconteceu or '',
            'Acao_Realizar': record.acao_a_realizar or '',
        })
    
    # 7. Salvar amostra
    AMOSTRA_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    
    with open(AMOSTRA_OUTPUT, 'w', newline='', encoding='utf-8-sig') as f:
        if amostra_data:
            writer = csv.DictWriter(f, fieldnames=amostra_data[0].keys(), delimiter=';')
            writer.writeheader()
            writer.writerows(amostra_data)
    
    print(f"    >> Amostra salva em: {AMOSTRA_OUTPUT}")
    
    # 8. Estatísticas
    print()
    print("=" * 70)
    print("ESTATÍSTICAS DE TEMPLATES")
    print("=" * 70)
    
    for template_id, config in TEMPLATES.items():
        count = template_stats.get(template_id, 0)
        pct = (count / len(records) * 100) if records else 0
        print(f"  Template {template_id} ({config.nome_modelo}): {count} ({pct:.1f}%)")
    
    sem_template = template_stats.get('sem_template', 0)
    pct_sem = (sem_template / len(records) * 100) if records else 0
    print(f"  Sem template mapeado: {sem_template} ({pct_sem:.1f}%)")
    
    print()
    print("-" * 70)
    print("RESUMO DO PROCESSAMENTO")
    print("-" * 70)
    print(f"  Total de registros: {len(records)}")
    print(f"  Registros mapeados: {sum(1 for r, _ in results_list if r.mapeado)}")
    print(f"  Registros não mapeados: {sum(1 for r, _ in results_list if not r.mapeado)}")
    print(f"  Com dados de logística: {sum(1 for r, _ in results_list if r.nome_cliente)}")
    print(f"  Com link de rastreio: {sum(1 for r, _ in results_list if r.cod_rastreio)}")
    
    # 9. Excluir arquivos processados
    print()
    print("-" * 70)
    print("LIMPEZA DE ARQUIVOS PROCESSADOS")
    print("-" * 70)
    
    arquivos_excluidos = []
    
    try:
        if arquivo_csv and arquivo_csv.exists():
            arquivo_csv.unlink()
            arquivos_excluidos.append(arquivo_csv.name)
            print(f"  >> Deletado: {arquivo_csv.name}")
    except Exception as e:
        print(f"  >> Erro ao deletar CSV: {e}")
        logger.warning(f"Erro ao deletar CSV {arquivo_csv}: {e}")
    
    try:
        if arquivo_objetos and arquivo_objetos.exists():
            arquivo_objetos.unlink()
            arquivos_excluidos.append(arquivo_objetos.name)
            print(f"  >> Deletado: {arquivo_objetos.name}")
    except Exception as e:
        print(f"  >> Erro ao deletar XLSX: {e}")
        logger.warning(f"Erro ao deletar XLSX {arquivo_objetos}: {e}")
    
    if not arquivos_excluidos:
        print("  >> Nenhum arquivo excluído")
    
    print()
    print("=" * 70)
    print("PROCESSAMENTO CONCLUÍDO!")
    print("=" * 70)
    print(f"Data/Hora: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print(f"Banco de dados: {DB_PATH}")
    print(f"Amostra de validação: {AMOSTRA_OUTPUT}")
    print(f"Arquivos processados e excluídos: {len(arquivos_excluidos)}")
    print("=" * 70)


if __name__ == "__main__":
    try:
        processar_arquivos()
    except KeyboardInterrupt:
        print("\nProcessamento interrompido pelo usuário.")
        sys.exit(1)
    except Exception as e:
        print(f"ERRO FATAL: {e}")
        logger.error(f"Erro fatal: {e}", exc_info=True)
        import traceback
        traceback.print_exc()
        sys.exit(1)

