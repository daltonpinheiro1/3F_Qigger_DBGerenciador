#!/usr/bin/env python3
"""
Script Unificado para Processamento Completo
============================================

Processa todas as bases e gera todos os arquivos de homologação em sequência.

ETAPAS:
1. Processa Excel COVERTE BASE PROP.xlsx → base_coverte_prop
2. Processa arquivos CSV de atualização → portabilidade_records
3. Processa Relatorio_Objetos.xlsx → relatorio_objetos
4. Gera arquivos de homologação:
   - WPP (WhatsApp)
   - Reabertura
   - Aprovisionamento
   - Erro no Aprovisionamento

Uso:
    python3 processar_completo.py                    # Processa tudo
    python3 processar_completo.py --apenas-bases    # Apenas processa bases
    python3 processar_completo.py --apenas-homologacao  # Apenas gera homologação
    python3 processar_completo.py --skip-excel      # Pula processamento do Excel
"""
import sys
import logging
import argparse
from pathlib import Path
from datetime import datetime
import subprocess
import shutil

# Adicionar diretório atual ao PYTHONPATH para imports locais
PROJECT_ROOT = Path(__file__).parent.absolute()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Configurar encoding UTF-8
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
        logging.FileHandler('logs/processar_completo.log', encoding='utf-8'),
        console_handler
    ]
)

logger = logging.getLogger(__name__)

# Configurações - importar do config.py centralizado
try:
    from config import (
        PASTA_IMPORTACOES,
        PASTA_ENTRADA,
        PASTA_BASE_COVERTE_LOCAL,
        PASTA_BASE_COVERTE_NETWORK,
        ARQUIVO_BASE_COVERTE_NETWORK,
        DB_PATH,
        PASTA_SAIDA_HOMOLOGACAO,
    )
    pasta_importacoes = Path(PASTA_IMPORTACOES)
    pasta_entrada = Path(PASTA_ENTRADA)
    pasta_coverte_local = Path(PASTA_BASE_COVERTE_LOCAL)
    pasta_coverte_network = Path(PASTA_BASE_COVERTE_NETWORK)
    arquivo_coverte_network = Path(ARQUIVO_BASE_COVERTE_NETWORK) if ARQUIVO_BASE_COVERTE_NETWORK else None
    db_path = DB_PATH
    logger.info(f"✓ Configurações carregadas de config.py")
    logger.info(f"  DB_PATH: {db_path}")
    logger.info(f"  PASTA_COVERTE_NETWORK: {pasta_coverte_network}")
except ImportError as e:
    logger.warning(f"config.py não encontrado ou incompleto ({e}), usando valores padrão")
    pasta_importacoes = Path("/Applications/Documentos/IMPORTACOES_QIGGER")
    pasta_entrada = Path(__file__).parent / "data" / "entrada"
    pasta_coverte_local = pasta_entrada / "excel"
    pasta_coverte_network = Path("/Volumes/02 Planejamento/02 - Relatórios/08 - Relatorios Cliente")
    arquivo_coverte_network = Path("/Volumes/02 Planejamento/02 - Relatórios/08 - Relatorios Cliente/COVERTE BASE PROP.xlsx")
    db_path = str(Path(__file__).parent / "data" / "portabilidade.db")
    PASTA_SAIDA_HOMOLOGACAO = "/Applications/Documentos/Projetos_python/Retornos do gerenciador"

# Pasta de saída para arquivos de homologação (usar do config ou fallback)
try:
    PASTA_SAIDA = Path(PASTA_SAIDA_HOMOLOGACAO)
except NameError:
    PASTA_SAIDA = Path("/Applications/Documentos/Projetos_python/Retornos do gerenciador")

# Scripts de homologação
SCRIPTS_HOMOLOGACAO = [
    {
        'nome': 'WPP (WhatsApp)',
        'script': 'gerar_homologacao_wpp.py',
        'arquivo_origem': 'data/homologacao_wpp.csv',
        'prefixo_nome': 'homologacao_wpp'
    },
    {
        'nome': 'Reabertura',
        'script': 'gerar_homologacao_reabertura.py',
        'arquivo_origem': 'data/homologacao_reabertura.xlsx',
        'prefixo_nome': 'homologacao_reabertura'
    },
    {
        'nome': 'Aprovisionamento',
        'script': 'gerar_homologacao_aprovisionamento.py',
        'arquivo_origem': 'data/homologacao_aprovisionamento.xlsx',
        'prefixo_nome': 'homologacao_aprovisionamento'
    },
    {
        'nome': 'Erro no Aprovisionamento',
        'script': 'gerar_homologacao_erro_aprovisionamento.py',
        'arquivo_origem': 'data/homologacao_erro_aprovisionamento.xlsx',
        'prefixo_nome': 'homologacao_erro_aprovisionamento'
    }
]


def processar_excel_coverte_prop(usar_smb: bool = True) -> dict:
    """
    Processa Excel COVERTE BASE PROP.xlsx
    
    Args:
        usar_smb: Se True, tenta conectar via SMB automaticamente
    """
    logger.info("=" * 70)
    logger.info("ETAPA 1: PROCESSANDO EXCEL COVERTE BASE PROP.xlsx")
    logger.info("=" * 70)
    
    stats = {
        'sucesso': False,
        'arquivo': None,
        'stats': {}
    }
    
    try:
        from processar_excel_unificado import (
            processar_excel_unificado, 
            encontrar_arquivo_excel,
            obter_arquivo_coverte_smb,
            verificar_conexao_smb,
            montar_compartilhamento_smb
        )
        
        # Procurar arquivo (prioridade: SMB > arquivo específico > rede > local > entrada)
        arquivo_coverte = None
        
        # 1. Tentar SMB primeiro se solicitado
        if usar_smb:
            logger.info("📡 Verificando conexão SMB...")
            if not verificar_conexao_smb():
                logger.info("🔌 Tentando montar compartilhamento SMB automaticamente...")
                if montar_compartilhamento_smb():
                    logger.info("✓ Compartilhamento SMB montado com sucesso!")
                else:
                    logger.warning("⚠️ Não foi possível montar SMB automaticamente")
            
            # Tentar obter arquivo via SMB
            arquivo_coverte = obter_arquivo_coverte_smb()
            if arquivo_coverte:
                logger.info(f"✓ Arquivo encontrado via SMB: {arquivo_coverte.name}")
        
        # 2. Arquivo específico configurado
        if not arquivo_coverte and arquivo_coverte_network and arquivo_coverte_network.exists():
            arquivo_coverte = arquivo_coverte_network
            logger.info(f"✓ Arquivo encontrado (caminho específico): {arquivo_coverte}")
        
        # 3. Pasta de rede (se montada)
        if not arquivo_coverte and pasta_coverte_network.exists():
            arquivo_coverte = encontrar_arquivo_excel(pasta_coverte_network)
            if arquivo_coverte:
                logger.info(f"✓ Arquivo encontrado na rede: {arquivo_coverte.name}")
        
        # 4. Pasta local (cópia)
        if not arquivo_coverte:
            arquivo_coverte = encontrar_arquivo_excel(pasta_coverte_local)
            if arquivo_coverte:
                logger.info(f"✓ Arquivo encontrado localmente: {arquivo_coverte.name}")
        
        # 5. Pasta de entrada
        if not arquivo_coverte:
            arquivo_coverte = encontrar_arquivo_excel(pasta_entrada)
            if arquivo_coverte:
                logger.info(f"✓ Arquivo encontrado em pasta de entrada: {arquivo_coverte.name}")
        
        if arquivo_coverte:
            logger.info(f"Processando: {arquivo_coverte}")
            stats_processamento = processar_excel_unificado(arquivo_coverte, db_path)
            stats['sucesso'] = True
            stats['arquivo'] = str(arquivo_coverte)
            stats['stats'] = stats_processamento
            logger.info(f"✅ Excel processado: {stats_processamento.get('processados', 0)} registros")
        else:
            logger.warning("⚠️ Arquivo COVERTE BASE PROP.xlsx não encontrado")
            logger.info("")
            logger.info("Pastas verificadas (em ordem de prioridade):")
            logger.info(f"  1. SMB: smb://files/02 Planejamento/02 - Relatórios/08 - Relatorios Cliente/")
            logger.info(f"  2. Rede: {pasta_coverte_network}")
            logger.info(f"  3. Local: {pasta_coverte_local}")
            logger.info(f"  4. Entrada: {pasta_entrada}")
            logger.info("")
            logger.info("💡 Dica: Monte o compartilhamento SMB manualmente via Finder > Cmd+K")
    
    except ImportError as e:
        logger.error(f"❌ Erro de importação ao processar Excel: {e}")
        import traceback
        logger.error(traceback.format_exc())
    except FileNotFoundError as e:
        logger.error(f"❌ Arquivo não encontrado: {e}")
    except PermissionError as e:
        logger.error(f"❌ Sem permissão para acessar arquivo: {e}")
    except Exception as e:
        logger.error(f"❌ Erro ao processar Excel COVERTE BASE PROP: {e}")
        import traceback
        logger.error(traceback.format_exc())
    
    logger.info("")
    return stats


def processar_csv_atualizacoes() -> dict:
    """Processa arquivos CSV de atualização"""
    logger.info("=" * 70)
    logger.info("ETAPA 2: PROCESSANDO ARQUIVOS CSV DE ATUALIZAÇÃO")
    logger.info("=" * 70)
    
    stats = {
        'sucesso': False,
        'arquivos_processados': 0,
        'registros_processados': 0,
        'erros': 0
    }
    
    try:
        # Buscar arquivos CSV
        arquivos_csv = []
        
        if pasta_importacoes.exists():
            csvs_importacoes = list(pasta_importacoes.glob("*.csv"))
            arquivos_csv.extend(csvs_importacoes)
            logger.info(f"Encontrados {len(csvs_importacoes)} arquivos CSV em {pasta_importacoes}")
        
        if pasta_entrada.exists():
            csvs_entrada = list(pasta_entrada.glob("*.csv"))
            arquivos_csv.extend(csvs_entrada)
            if csvs_entrada:
                logger.info(f"Encontrados {len(csvs_entrada)} arquivos CSV em {pasta_entrada}")
        
        # Remover duplicados
        arquivos_csv = list(set(arquivos_csv))
        
        if arquivos_csv:
            logger.info(f"Total de arquivos CSV únicos: {len(arquivos_csv)}")
            
            try:
                from processar_atualizacoes_gerar_finais import processar_arquivos_atualizacao
                logger.info("Processando arquivos CSV...")
                stats_csv = processar_arquivos_atualizacao()
                
                stats['sucesso'] = True
                stats['arquivos_processados'] = stats_csv.get('arquivos_processados', 0)
                stats['registros_processados'] = stats_csv.get('registros_processados', 0)
                stats['erros'] = stats_csv.get('erros', 0)
                
                logger.info(f"✅ CSVs processados: {stats['arquivos_processados']} arquivos, "
                          f"{stats['registros_processados']} registros, {stats['erros']} erros")
                          
            except ImportError as e:
                logger.error(f"❌ Erro ao importar processar_atualizacoes_gerar_finais: {e}")
                import traceback
                logger.error(traceback.format_exc())
            except Exception as e:
                logger.error(f"❌ Erro ao processar arquivos CSV: {e}")
                import traceback
                logger.error(traceback.format_exc())
        else:
            logger.warning("⚠️ Nenhum arquivo CSV encontrado para processar")
            
    except Exception as e:
        logger.error(f"❌ Erro ao processar arquivos CSV: {e}")
        import traceback
        logger.error(traceback.format_exc())
    
    logger.info("")
    return stats


def processar_relatorio_objetos() -> dict:
    """Processa Relatorio_Objetos.xlsx"""
    logger.info("=" * 70)
    logger.info("ETAPA 3: PROCESSANDO RELATORIO_OBJETOS.xlsx")
    logger.info("=" * 70)
    
    stats = {
        'sucesso': False,
        'arquivo': None,
        'registros': 0
    }
    
    try:
        # Buscar arquivos Relatorio_Objetos
        arquivos_objetos = []
        
        if pasta_importacoes.exists():
            arquivos_objetos.extend(list(pasta_importacoes.glob("Relatorio_Objetos*.xlsx")))
        
        if pasta_entrada.exists():
            arquivos_objetos.extend(list(pasta_entrada.glob("Relatorio_Objetos*.xlsx")))
        
        # Remover duplicados
        arquivos_objetos = list(set(arquivos_objetos))
        
        if arquivos_objetos:
            logger.info(f"Encontrados {len(arquivos_objetos)} arquivo(s) Relatorio_Objetos")
            
            # Usar o mais recente
            arquivo_objetos = max(arquivos_objetos, key=lambda x: x.stat().st_mtime)
            logger.info(f"Processando: {arquivo_objetos.name}")
            
            try:
                from processar_relatorio_objetos_completo import processar_relatorio_objetos_completo
                logger.info("Processando Relatorio_Objetos com suporte completo...")
                processar_relatorio_objetos_completo(str(arquivo_objetos))
                stats['sucesso'] = True
                stats['arquivo'] = str(arquivo_objetos)
                
            except ImportError:
                # Fallback: usar ObjectsLoader diretamente
                from src.utils import ObjectsLoader
                from src.database import DatabaseManager
                
                logger.info("Carregando Relatorio_Objetos...")
                objects_loader = ObjectsLoader(str(arquivo_objetos))
                logger.info(f"Registros carregados: {objects_loader.total_records}")
                
                logger.info("Sincronizando com banco de dados...")
                db_manager = DatabaseManager(db_path)
                sync_stats = db_manager.sync_relatorio_objetos(objects_loader)
                
                stats['sucesso'] = True
                stats['arquivo'] = str(arquivo_objetos)
                stats['registros'] = objects_loader.total_records
                
                logger.info(f"✅ Sincronizado: {sync_stats.get('inseridos', 0)} novos, "
                          f"{sync_stats.get('novas_versoes', 0)} novas versões")
            
            # Mover arquivo para processados após sucesso
            try:
                pasta_processados = pasta_importacoes / "processados"
                pasta_processados.mkdir(parents=True, exist_ok=True)
                
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                novo_nome = f"{arquivo_objetos.stem}_{timestamp}{arquivo_objetos.suffix}"
                destino = pasta_processados / novo_nome
                
                shutil.move(str(arquivo_objetos), str(destino))
                logger.info(f"✅ Arquivo movido para processados: {destino.name}")
            except Exception as e:
                logger.warning(f"⚠️ Não foi possível mover arquivo para processados: {e}")
        else:
            logger.warning("⚠️ Nenhum arquivo Relatorio_Objetos.xlsx encontrado")
            
    except Exception as e:
        logger.error(f"❌ Erro ao processar Relatorio_Objetos: {e}")
        import traceback
        logger.error(traceback.format_exc())
    
    logger.info("")
    return stats


def executar_script_homologacao(script_path: Path) -> bool:
    """Executa um script de homologação"""
    if not script_path.exists():
        logger.error(f"Script não encontrado: {script_path}")
        return False
    
    try:
        logger.info(f"Executando: {script_path.name}")
        result = subprocess.run(
            [sys.executable, str(script_path)],
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace',
            cwd=Path(__file__).parent
        )
        
        if result.returncode == 0:
            logger.info(f"✓ {script_path.name} executado com sucesso")
            return True
        else:
            logger.error(f"✗ {script_path.name} falhou (código {result.returncode})")
            if result.stderr:
                logger.error(f"Erro: {result.stderr[:500]}")
            return False
            
    except Exception as e:
        logger.error(f"Erro ao executar {script_path.name}: {e}")
        return False


def copiar_arquivo_com_timestamp(arquivo_origem: Path, prefixo_nome: str, pasta_destino: Path) -> Path:
    """Copia arquivo para pasta de destino com timestamp"""
    if not arquivo_origem.exists():
        logger.warning(f"Arquivo não encontrado: {arquivo_origem}")
        return None
    
    try:
        pasta_destino.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        extensao = arquivo_origem.suffix
        nome_arquivo = f"{prefixo_nome}_{timestamp}{extensao}"
        arquivo_destino = pasta_destino / nome_arquivo
        
        shutil.copy2(arquivo_origem, arquivo_destino)
        logger.info(f"  ✓ Arquivo copiado: {nome_arquivo}")
        return arquivo_destino
        
    except Exception as e:
        logger.error(f"Erro ao copiar arquivo {arquivo_origem.name}: {e}")
        return None


def gerar_arquivos_homologacao() -> dict:
    """Gera todos os arquivos de homologação"""
    logger.info("=" * 70)
    logger.info("ETAPA 4: GERANDO ARQUIVOS DE HOMOLOGAÇÃO")
    logger.info("=" * 70)
    
    resultados = {
        'sucesso': [],
        'falha': [],
        'arquivos_copiados': []
    }
    
    # Criar pasta de saída
    try:
        PASTA_SAIDA.mkdir(parents=True, exist_ok=True)
        logger.info(f"Pasta de saída: {PASTA_SAIDA}")
    except Exception as e:
        logger.error(f"Erro ao criar pasta de saída: {e}")
        return resultados
    
    # Executar cada script
    for item in SCRIPTS_HOMOLOGACAO:
        logger.info(f"\n[{item['nome']}]")
        script_path = Path(__file__).parent / item['script']
        
        # Executar script
        sucesso = executar_script_homologacao(script_path)
        
        if sucesso:
            resultados['sucesso'].append(item['nome'])
            
            # Copiar arquivo gerado com timestamp
            arquivo_origem = Path(__file__).parent / item['arquivo_origem']
            if arquivo_origem.exists():
                arquivo_copiado = copiar_arquivo_com_timestamp(
                    arquivo_origem,
                    item['prefixo_nome'],
                    PASTA_SAIDA
                )
                if arquivo_copiado:
                    resultados['arquivos_copiados'].append(arquivo_copiado)
            else:
                logger.warning(f"  Arquivo gerado não encontrado: {arquivo_origem}")
        else:
            resultados['falha'].append(item['nome'])
    
    logger.info("")
    return resultados


def main():
    """Função principal"""
    parser = argparse.ArgumentParser(
        description="Processamento Completo - Processa todas as bases e gera arquivos de homologação",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos:
  python3 processar_completo.py                    # Processa tudo
  python3 processar_completo.py --apenas-bases     # Apenas processa bases
  python3 processar_completo.py --apenas-homologacao  # Apenas gera homologação
  python3 processar_completo.py --skip-excel       # Pula processamento do Excel
        """
    )
    
    parser.add_argument(
        '--apenas-bases',
        action='store_true',
        help='Apenas processa as bases (Excel, CSV, Relatório de Objetos), sem gerar homologação'
    )
    
    parser.add_argument(
        '--apenas-homologacao',
        action='store_true',
        help='Apenas gera arquivos de homologação, sem processar bases'
    )
    
    parser.add_argument(
        '--skip-excel',
        action='store_true',
        help='Pula o processamento do Excel COVERTE BASE PROP'
    )
    
    parser.add_argument(
        '--skip-csv',
        action='store_true',
        help='Pula o processamento dos arquivos CSV'
    )
    
    parser.add_argument(
        '--skip-objetos',
        action='store_true',
        help='Pula o processamento do Relatorio_Objetos'
    )
    
    parser.add_argument(
        '--smb',
        action='store_true',
        default=True,
        help='Conectar automaticamente ao compartilhamento SMB (padrão: ativado)'
    )
    
    parser.add_argument(
        '--no-smb',
        action='store_true',
        help='Desativar conexão automática SMB'
    )
    
    args = parser.parse_args()
    
    # Início
    print("=" * 70)
    print("PROCESSAMENTO COMPLETO - 3F Qigger DB Gerenciador")
    print("=" * 70)
    print(f"Data/Hora: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print(f"Banco de dados: {db_path}")
    print()
    
    # Estatísticas gerais
    stats_geral = {
        'excel': {},
        'csv': {},
        'objetos': {},
        'homologacao': {}
    }
    
    # Processar bases (se não for apenas homologação)
    if not args.apenas_homologacao:
        # 1. Excel COVERTE BASE PROP
        if not args.skip_excel:
            usar_smb = args.smb and not args.no_smb
            stats_geral['excel'] = processar_excel_coverte_prop(usar_smb=usar_smb)
        else:
            logger.info("⏭️ Pulando processamento do Excel (--skip-excel)")
            logger.info("")
        
        # 2. CSV de atualização
        if not args.skip_csv:
            stats_geral['csv'] = processar_csv_atualizacoes()
        else:
            logger.info("⏭️ Pulando processamento de CSV (--skip-csv)")
            logger.info("")
        
        # 3. Relatorio_Objetos
        if not args.skip_objetos:
            stats_geral['objetos'] = processar_relatorio_objetos()
        else:
            logger.info("⏭️ Pulando processamento de Relatorio_Objetos (--skip-objetos)")
            logger.info("")
    
    # Gerar arquivos de homologação (se não for apenas bases)
    if not args.apenas_bases:
        stats_geral['homologacao'] = gerar_arquivos_homologacao()
    
    # Resumo final
    print("=" * 70)
    print("RESUMO DO PROCESSAMENTO")
    print("=" * 70)
    
    if not args.apenas_homologacao:
        print("\n📊 PROCESSAMENTO DE BASES:")
        
        if not args.skip_excel:
            excel_stats = stats_geral['excel']
            if excel_stats.get('sucesso'):
                print(f"  ✅ Excel COVERTE BASE PROP: {excel_stats.get('stats', {}).get('processados', 0)} registros")
            else:
                print(f"  ⚠️ Excel COVERTE BASE PROP: Não processado")
        
        if not args.skip_csv:
            csv_stats = stats_geral['csv']
            if csv_stats.get('sucesso'):
                print(f"  ✅ CSV de atualização: {csv_stats.get('arquivos_processados', 0)} arquivos, "
                      f"{csv_stats.get('registros_processados', 0)} registros")
            else:
                print(f"  ⚠️ CSV de atualização: Não processado")
        
        if not args.skip_objetos:
            objetos_stats = stats_geral['objetos']
            if objetos_stats.get('sucesso'):
                print(f"  ✅ Relatorio_Objetos: {objetos_stats.get('registros', 0)} registros")
            else:
                print(f"  ⚠️ Relatorio_Objetos: Não processado")
    
    if not args.apenas_bases:
        print("\n📄 ARQUIVOS DE HOMOLOGAÇÃO:")
        homologacao_stats = stats_geral['homologacao']
        sucesso = len(homologacao_stats.get('sucesso', []))
        total = len(SCRIPTS_HOMOLOGACAO)
        
        print(f"  Gerados com sucesso: {sucesso}/{total}")
        
        if homologacao_stats.get('sucesso'):
            print("  ✅ Arquivos gerados:")
            for nome in homologacao_stats['sucesso']:
                print(f"    - {nome}")
        
        if homologacao_stats.get('falha'):
            print("  ❌ Arquivos que falharam:")
            for nome in homologacao_stats['falha']:
                print(f"    - {nome}")
        
        if homologacao_stats.get('arquivos_copiados'):
            print(f"\n  📁 Arquivos copiados para: {PASTA_SAIDA}")
            for arquivo in homologacao_stats['arquivos_copiados']:
                print(f"    - {arquivo.name}")
    
    print("\n" + "=" * 70)
    print("PROCESSAMENTO CONCLUÍDO!")
    print("=" * 70)
    print(f"\nBanco de dados: {db_path}")
    if not args.apenas_bases and homologacao_stats.get('arquivos_copiados'):
        print(f"Arquivos de homologação: {PASTA_SAIDA}")
    print()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("\nProcessamento interrompido pelo usuário.")
        print("\n⚠️ Processamento interrompido pelo usuário.")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Erro fatal: {e}", exc_info=True)
        print(f"\n❌ ERRO FATAL: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
