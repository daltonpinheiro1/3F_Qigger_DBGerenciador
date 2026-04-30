#!/usr/bin/env python3
"""
Script Unificado para Processamento Completo
============================================

Processa todas as bases e gera todos os arquivos de homologação em sequência.
Todo o processamento grava no banco local (config.DB_PATH); ao final o banco
é replicado para a rede (07 Backoffice). Arquivos processados são movidos
para a pasta padrão de destino (config.PASTA_PROCESSADOS = data/processados).

ETAPAS (ordem obrigatória para verificações corretas):
1. BS_VENDA_DU (coleta e atualização)
2. Arquivos da pasta (data/entrada + IMPORTACOES_QIGGER) por cabeçalho:
      → CSV portabilidade → portabilidade_records
      → Excel Relatorio_Objetos → relatorio_objetos
      → Excel COVERTE/3F_GROSS (se na pasta) → base_coverte_prop (legado) + V2
      → CSV relatorio*.csv → relatorio_faturamento
3. Atualizar Cache Unificada (V2)
4. Gera arquivos de homologação (WPP, Reabertura, Aprovisionamento, Erro, Entrega/Baixa, Consulta)
   → Todos usam filtro de dias (90 ou 180 conforme gerador)
5. Reprocessamento de endereços inválidos
6. Replica portabilidade.db para rede (SMB 07 Backoffice)

Destinos:
- Arquivos processados (entrada): data/processados (PASTA_PROCESSADOS)
- Arquivos de homologação (saída): PASTA_SAIDA_HOMOLOGACAO (Retornos do gerenciador)

Uso:
    python3 processar_completo.py                    # Processa tudo
    python3 processar_completo.py --apenas-bases    # Apenas processa bases
    python3 processar_completo.py --apenas-homologacao  # Apenas gera homologação
    python3 processar_completo.py --apenas-d1-entrega   # Apenas sincroniza D1 Entrega (Cloudflare→SQLite)
    python3 processar_completo.py --skip-excel      # Pula processamento do Excel

IMPORTANTE: Use o venv para ter pandas/openpyxl instalados:
    ./run_processar_completo.sh --cafinate
    ou: source .venv/bin/activate && python3 processar_completo.py
"""
import sys
from pathlib import Path

# Se não está em venv, reexecutar com o Python do venv do projeto (evita "não gerou arquivos")
_SCRIPT_DIR = Path(__file__).resolve().parent
def _ensure_venv():
    in_venv = getattr(sys, 'prefix', None) != getattr(sys, 'base_prefix', None)
    if in_venv:
        return
    for venv_py in (_SCRIPT_DIR / '.venv' / 'bin' / 'python', _SCRIPT_DIR / 'venv' / 'bin' / 'python'):
        if venv_py.exists() and venv_py.is_file():
            import os
            os.execv(str(venv_py), [str(venv_py)] + sys.argv)
            return  # execv não retorna
_ensure_venv()

# Verificar dependências ANTES de qualquer outro import (evita ModuleNotFoundError obscuro)
def _check_deps():
    for mod in ('pandas', 'openpyxl'):
        try:
            __import__(mod)
        except ImportError:
            print("=" * 70)
            print("ERRO: Módulo '%s' não encontrado." % mod)
            print("Os scripts de homologação precisam de pandas e openpyxl.")
            print("")
            print("Instale: pip install -r requirements.txt")
            print("Ou use: source .venv/bin/activate && python3 processar_completo.py")
            print("=" * 70)
            sys.exit(1)
_check_deps()
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
        DB_V2_PATH,
        PASTA_SAIDA_HOMOLOGACAO,
        PASTA_PROCESSADOS,
    )
    pasta_importacoes = Path(PASTA_IMPORTACOES)
    pasta_entrada = Path(PASTA_ENTRADA)
    pasta_coverte_local = Path(PASTA_BASE_COVERTE_LOCAL)
    pasta_coverte_network = Path(PASTA_BASE_COVERTE_NETWORK)
    arquivo_coverte_network = Path(ARQUIVO_BASE_COVERTE_NETWORK) if ARQUIVO_BASE_COVERTE_NETWORK else None
    pasta_processados = Path(PASTA_PROCESSADOS)
    db_path = DB_PATH
    db_v2_path = DB_V2_PATH
    logger.info(f"✓ Configurações carregadas de config.py")
    logger.info(f"  DB_PATH: {db_path}")
    logger.info(f"  DB_V2_PATH: {db_v2_path}")
    logger.info(f"  PASTA_COVERTE_NETWORK: {pasta_coverte_network}")
except ImportError as e:
    logger.warning(f"config.py não encontrado ou incompleto ({e}), usando valores padrão")
    pasta_importacoes = Path("/Applications/Documentos/IMPORTACOES_QIGGER")
    pasta_entrada = Path(__file__).parent / "data" / "entrada"
    pasta_coverte_local = pasta_entrada / "excel"
    pasta_coverte_network = Path("/Volumes/02 Planejamento/02 - Relatórios/08 - Relatorios Cliente")
    arquivo_coverte_network = Path("/Volumes/02 Planejamento/02 - Relatórios/08 - Relatorios Cliente/COVERTE BASE PROP.xlsx")
    pasta_processados = Path(__file__).parent / "data" / "processados"
    db_v2_path = str(Path(__file__).parent / "data" / "portabilidade_v2.db")
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
        'arquivo_origem': 'data/homologacao_wpp.xlsx',
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
    },
    {
        'nome': 'Entrega/Baixa',
        'script': 'gerar_homologacao_entrega_baixa.py',
        'arquivo_origem': 'data/homologacao_entrega_baixa.xlsx',
        'prefixo_nome': 'homologacao_entrega_baixa'
    },
    {
        'nome': 'Consulta',
        'script': 'gerar_homologacao_consulta.py',
        'arquivo_origem': 'data/homologacao_consulta.xlsx',
        'prefixo_nome': 'homologacao_consulta'
    }
]


# --- Detecção de tipo por cabeçalho ---

def _normalizar_coluna(texto: str) -> str:
    """Normaliza nome de coluna para comparação (sem acentos, uppercase)."""
    import unicodedata
    if not texto:
        return ""
    t = str(texto).strip().upper()
    t = unicodedata.normalize('NFD', t)
    t = ''.join(c for c in t if unicodedata.category(c) != 'Mn')
    return ' '.join(t.split())


def detectar_tipo_arquivo_por_cabecalho(arquivo: Path) -> str:
    """
    Detecta o tipo de arquivo pelo cabeçalho (não pelo nome).
    Retorna: 'csv_portabilidade', 'excel_portabilidade', 'csv_relatorio_faturamento',
             'excel_base_coverte', 'excel_relatorio_objetos', 'excel_gross',
             'telegram', 'desconhecido'
    """
    # Detecção rápida por nome para Telegram (antes de abrir o arquivo)
    nome_lower = arquivo.name.lower()
    if 'telegram' in nome_lower:
        return 'telegram'

    try:
        if arquivo.suffix.lower() == '.csv':
            from src.utils.csv_parser import CSVParser
            from processar_relatorio_faturamento import tem_estrutura_faturamento
            encodings = ['utf-8', 'utf-8-sig', 'latin-1', 'cp1252']
            for enc in encodings:
                try:
                    with open(arquivo, 'r', encoding=enc, errors='replace') as f:
                        import csv
                        sample = f.read(8192)
                        f.seek(0)
                        # Detectar delimitador
                        try:
                            dialect = csv.Sniffer().sniff(sample, delimiters=',;\t|')
                        except csv.Error:
                            dialect = csv.excel
                        reader = csv.reader(f, dialect)
                        headers = next(reader, [])
                        if not headers:
                            continue
                        if CSVParser.tem_estrutura_portabilidade(headers):
                            return 'csv_portabilidade'
                        # TIM PRE - CONTROLE: Migrar plano Pre x Controle
                        from processar_tim_pre_controle import tem_estrutura_tim_pre_controle
                        if tem_estrutura_tim_pre_controle(headers) or \
                           ('migrar' in arquivo.name.lower() and 'plano' in arquivo.name.lower()) or \
                           ('pre' in arquivo.name.lower() and 'controle' in arquivo.name.lower()):
                            return 'csv_tim_pre_controle'
                        # Relatório faturamento: nome relatorio*.csv ou cabeçalho com colunas de faturamento
                        if 'relatorio' in arquivo.name.lower() or tem_estrutura_faturamento(headers):
                            return 'csv_relatorio_faturamento'
                        return 'csv_portabilidade'  # Fallback: tentar como portabilidade
                except (UnicodeDecodeError, StopIteration):
                    continue
            return 'desconhecido'

        if arquivo.suffix.lower() in ('.xlsx', '.xls'):
            import pandas as pd
            df = pd.read_excel(arquivo, engine='openpyxl', nrows=1)
            colunas_norm = [_normalizar_coluna(c) for c in df.columns]
            cols_set = set(colunas_norm)
            headers_excel = [str(c) for c in df.columns]

            # Relatorio_Objetos: verificar ANTES de portabilidade para evitar falso positivo.
            # Arquivos "Relatorio_Objetos_*.xlsx" têm Nu Pedido, Destinatário, Rastreio/Objeto/ID ERP.
            tem_nu_pedido = any('NU PEDIDO' in c or 'PEDIDO' in c for c in colunas_norm)
            tem_destinatario = any('DESTINATARIO' in c for c in colunas_norm)
            tem_objeto_ou_rastreio = any('OBJETO' in c or 'RASTREIO' in c or 'ID ERP' in c for c in colunas_norm)
            nome_upper = arquivo.name.upper()
            if (tem_nu_pedido and (tem_destinatario or tem_objeto_ou_rastreio)) or \
               ('RELATORIO_OBJETOS' in nome_upper or 'RELATORIO OBJETOS' in nome_upper):
                return 'excel_relatorio_objetos'

            # TIM Portabilidade/GROSS: ACESSO + OPERADORA_N1 + CLASSIFICACAO_CR ou DATA_SOLICITACAO
            # Detectar ANTES de portabilidade Siebel para evitar falso positivo
            cols_tim = {'ACESSO', 'OPERADORA N1', 'OPERADORA_N1', 'DDD', 'CUSTCODE'}
            cols_tim_encontradas = sum(1 for c in cols_tim if any(c in col for col in colunas_norm))
            tem_classificacao_cr = any('CLASSIFICACAO' in c and 'CR' in c for c in colunas_norm)
            tem_data_solicitacao = any('DATA SOLICITACAO' in c or 'DATA_SOLICITACAO' in c for c in colunas_norm)
            if cols_tim_encontradas >= 3 or (cols_tim_encontradas >= 2 and (tem_classificacao_cr or tem_data_solicitacao)):
                # É um arquivo TIM — classificar como GROSS (vai para base_coverte_prop + V2)
                if tem_classificacao_cr or '3F_GROSS' in nome_upper or 'GROSS' in nome_upper:
                    return 'excel_gross'
                # Portabilidade TIM (DATA_SOLICITACAO, ACESSO_TEMPORARIO, etc.)
                if tem_data_solicitacao or any('ACESSO TEMPORARIO' in c or 'ACESSO_TEMPORARIO' in c for c in colunas_norm):
                    return 'csv_tim_pre_controle'  # Reutilizar tipo existente

            # Excel Siebel/Portabilidade: Cpf, Número de acesso, Código externo (mesma estrutura do CSV)
            from src.utils.csv_parser import CSVParser
            if CSVParser.tem_estrutura_portabilidade(headers_excel):
                return 'excel_portabilidade'

            # COVERTE / 3F_GROSS: CRIVO VENDAS, DATA VENDA, PROPOSTA ISIZE, STATUS VENDA
            cols_coverte = {'CRIVO VENDAS', 'DATA VENDA', 'PROPOSTA ISIZE', 'STATUS VENDA', 'PORTABILIDADE'}
            cols_coverte_encontradas = sum(1 for c in cols_coverte if any(c in col for col in colunas_norm))
            if cols_coverte_encontradas >= 2:
                # 3F_GROSS tem estrutura similar
                if '3F_GROSS' in arquivo.name.upper() or 'GROSS' in arquivo.name.upper():
                    return 'excel_gross'
                return 'excel_base_coverte'

            # GROSS por nome mas sem colunas coverte: pode ser variante
            if '3F_GROSS' in arquivo.name.upper() or 'GROSS' in arquivo.name.upper():
                if cols_coverte_encontradas >= 1 or len(colunas_norm) > 5:
                    return 'excel_gross'

            # Estornadas: gsm como primeira coluna ou no nome do arquivo
            if 'estorno' in arquivo.name.lower() or (len(colunas_norm) <= 2 and any('gsm' in c for c in colunas_norm)):
                return 'excel_estornos'

            # Fallback: se tem PROPOSTA ISIZE ou NUMERO OS ou estrutura similar a base
            if any('PROPOSTA' in c or 'NUMERO OS' in c or 'CPF' in c for c in colunas_norm):
                return 'excel_base_coverte'

            return 'desconhecido'

    except Exception as e:
        logger.debug(f"Erro ao detectar tipo de {arquivo.name}: {e}")
    return 'desconhecido'


def processar_arquivos_pasta_entrada_unificado(
    skip_csv: bool = False,
    skip_excel: bool = False,
    skip_objetos: bool = False,
    skip_faturamento: bool = False,
    db_v2=None,
    importador_v2=None,
) -> dict:
    """
    Processa TODOS os arquivos das pastas de entrada (data/entrada e IMPORTACOES_QIGGER).
    Usa o CABEÇALHO para definir a tabela de destino, não o nome do arquivo.
    
    Args:
        skip_csv: Se True, não processa CSVs de portabilidade
        skip_excel: Se True, não processa Excels (coverte, gross)
        skip_objetos: Se True, não processa Relatorio_Objetos
        skip_faturamento: Se True, não processa Relatorio Faturamento
    """
    logger.info("=" * 70)
    logger.info("PROCESSANDO ARQUIVOS DA PASTA DE ENTRADA (por cabeçalho)")
    logger.info("=" * 70)

    stats = {
        'csv_processados': 0,
        'excel_coverte': 0,
        'excel_gross': 0,
        'excel_objetos': 0,
        'relatorio_faturamento': 0,
        'tim_pre_controle': 0,
        'estornos': 0,
        'ignorados': 0,
        'erros': 0,
    }

    pastas = [pasta_entrada]
    if pasta_importacoes.exists() and pasta_importacoes != pasta_entrada:
        pastas.append(pasta_importacoes)

    arquivos = []
    for pasta in pastas:
        if pasta.exists():
            arquivos.extend(list(pasta.glob("*.csv")))
            arquivos.extend(list(pasta.glob("*.xlsx")))
            arquivos.extend(list(pasta.glob("*.xls")))

    arquivos = list(set(arquivos))
    if not arquivos:
        logger.info("Nenhum arquivo CSV ou Excel encontrado nas pastas de entrada.")
        return stats

    logger.info(f"Encontrados {len(arquivos)} arquivo(s) para processar")
    for arq in sorted(arquivos, key=lambda x: x.stat().st_mtime, reverse=True):
        logger.info(f"  - {arq.name}")

    for arquivo in arquivos:
        try:
            tipo = detectar_tipo_arquivo_por_cabecalho(arquivo)
            logger.info(f"\n[{arquivo.name}] → Tipo detectado pelo cabeçalho: {tipo}")

            if tipo in ('csv_portabilidade', 'excel_portabilidade'):
                if skip_csv:
                    logger.info("  ⏭️ Pulando (--skip-csv)")
                    continue
                try:
                    from processar_atualizacoes_gerar_finais import processar_arquivos_atualizacao
                    _stats = processar_arquivos_atualizacao(arquivos_especificos=[str(arquivo)])
                    if _stats.get('arquivos_processados', 0) > 0:
                        stats['csv_processados'] += 1
                        logger.info(f"  ✓ Portabilidade processado: {_stats.get('registros_processados', 0)} registros")
                except Exception as e:
                    logger.error(f"  Erro ao processar: {e}")
                    stats['erros'] += 1
                # processar_arquivos_atualizacao já move o arquivo para processados
                # v2: importar no novo banco
                if db_v2 and importador_v2 and arquivo.exists():
                    _processar_arquivo_v2(arquivo, db_v2, importador_v2)

            elif tipo == 'excel_base_coverte':
                if skip_excel:
                    logger.info("  ⏭️ Pulando (--skip-excel)")
                    continue
                try:
                    from processar_excel_unificado import processar_excel_unificado
                    st = processar_excel_unificado(arquivo, db_path, forcar_processamento=True)
                    if st.get('processados', 0) > 0:
                        stats['excel_coverte'] += 1
                        logger.info(f"  ✓ Excel COVERTE processado: {st.get('processados', 0)} registros")
                except Exception as e:
                    logger.error(f"  Erro: {e}")
                    stats['erros'] += 1
                # v2: importar no novo banco antes de mover
                if db_v2 and importador_v2 and arquivo.exists():
                    _processar_arquivo_v2(arquivo, db_v2, importador_v2)
                _mover_para_processados(arquivo)

            elif tipo == 'excel_gross':
                if skip_excel:
                    logger.info("  ⏭️ Pulando (--skip-excel)")
                    continue
                try:
                    from processar_excel_unificado import processar_excel_unificado
                    st = processar_excel_unificado(arquivo, db_path, forcar_processamento=True)
                    if st.get('processados', 0) > 0:
                        stats['excel_gross'] += 1
                        logger.info(f"  ✓ Excel GROSS processado: {st.get('processados', 0)} registros")
                except Exception as e:
                    logger.error(f"  Erro: {e}")
                    stats['erros'] += 1
                # v2: importar no novo banco antes de mover
                if db_v2 and importador_v2 and arquivo.exists():
                    _processar_arquivo_v2(arquivo, db_v2, importador_v2)
                _mover_para_processados(arquivo)

            elif tipo == 'csv_relatorio_faturamento':
                if skip_faturamento:
                    logger.info("  ⏭️ Pulando (--skip-faturamento)")
                    continue
                try:
                    from processar_relatorio_faturamento import processar_relatorio_faturamento
                    st = processar_relatorio_faturamento(str(arquivo), db_path)
                    if st.get('inseridos', 0) > 0:
                        stats['relatorio_faturamento'] += 1
                        logger.info(f"  ✓ Relatório faturamento processado: {st.get('inseridos', 0)} registros → relatorio_faturamento")
                except Exception as e:
                    logger.error(f"  Erro: {e}")
                    stats['erros'] += 1
                _mover_para_processados(arquivo)

            elif tipo == 'csv_tim_pre_controle':
                try:
                    # Para Excel, converter para CSV temporário primeiro
                    arquivo_processar = str(arquivo)
                    if arquivo.suffix.lower() in ('.xlsx', '.xls'):
                        import pandas as _pd_tim
                        _df_tim = _pd_tim.read_excel(arquivo, engine='openpyxl')
                        _csv_temp = arquivo.parent / f"{arquivo.stem}_temp.csv"
                        _df_tim.to_csv(_csv_temp, index=False, sep=';', encoding='utf-8-sig')
                        arquivo_processar = str(_csv_temp)
                    from processar_tim_pre_controle import processar_tim_pre_controle
                    st = processar_tim_pre_controle(arquivo_processar, db_path)
                    if st.get('inseridos', 0) > 0:
                        stats['tim_pre_controle'] += 1
                        logger.info(f"  ✓ TIM PRE CONTROLE processado: {st.get('inseridos', 0)} registros → tim_pre_controle")
                    # Limpar CSV temporário
                    if arquivo.suffix.lower() in ('.xlsx', '.xls'):
                        _csv_temp = arquivo.parent / f"{arquivo.stem}_temp.csv"
                        if _csv_temp.exists():
                            _csv_temp.unlink()
                except Exception as e:
                    logger.error(f"  Erro: {e}")
                    stats['erros'] += 1
                # Auditoria RPA: processar pelo pipeline de auditoria ANTES de mover
                if db_v2 and arquivo.suffix.lower() == '.csv' and arquivo.exists():
                    try:
                        from src.pipeline_auditoria.classificador_status import ClassificadorStatus
                        from src.pipeline_auditoria.processador_retorno_rpa import ProcessadorRetornoRPA
                        _classificador = ClassificadorStatus()
                        _proc_rpa = ProcessadorRetornoRPA(_classificador)
                        _rpa_stats = _proc_rpa.processar_arquivo(str(arquivo), db_v2)
                        if _rpa_stats.get('inseridos', 0) > 0:
                            stats.setdefault('auditoria_rpa_inseridos', 0)
                            stats['auditoria_rpa_inseridos'] += _rpa_stats['inseridos']
                            logger.info(
                                "  ✓ Auditoria RPA: %d registros → retornos_rpa_tim",
                                _rpa_stats['inseridos'],
                            )
                    except Exception as e:
                        logger.warning(f"  ⚠ Auditoria RPA: {e}")
                # v2: importar no novo banco
                if db_v2 and importador_v2 and arquivo.exists():
                    _processar_arquivo_v2(arquivo, db_v2, importador_v2)
                _mover_para_processados(arquivo)

            elif tipo == 'excel_estornos':
                try:
                    from processar_estornos import processar_estornos
                    st = processar_estornos(str(arquivo), db_path)
                    if st.get('inseridos', 0) > 0:
                        stats['estornos'] += 1
                        logger.info(f"  ✓ Estornos processado: {st.get('inseridos', 0)} registros → estornos")
                except Exception as e:
                    logger.error(f"  Erro: {e}")
                    stats['erros'] += 1
                _mover_para_processados(arquivo)

            elif tipo == 'excel_relatorio_objetos':
                if skip_objetos:
                    logger.info("  ⏭️ Pulando (--skip-objetos)")
                    continue
                try:
                    from src.utils import ObjectsLoader
                    from src.database import DatabaseManager
                    loader = ObjectsLoader(str(arquivo))
                    if loader.total_records > 0:
                        db_manager = DatabaseManager(db_path)
                        db_manager.sync_relatorio_objetos(loader)
                        stats['excel_objetos'] += 1
                        logger.info(f"  ✓ Relatório Objetos processado: {loader.total_records} registros")
                except Exception as e:
                    logger.error(f"  Erro: {e}")
                    stats['erros'] += 1
                # v2: importar no novo banco antes de mover
                if db_v2 and importador_v2 and arquivo.exists():
                    _processar_arquivo_v2(arquivo, db_v2, importador_v2)
                _mover_para_processados(arquivo)

            elif tipo == 'telegram':
                try:
                    from processar_dados_cadastrais_telegram import processar_dados_cadastrais_telegram
                    st = processar_dados_cadastrais_telegram(
                        arquivo=arquivo, db_path=db_path, mover_para_processados=False
                    )
                    if st.get('processados', 0) > 0:
                        stats.setdefault('telegram', 0)
                        stats['telegram'] += 1
                        logger.info(
                            f"  ✓ Telegram processado: {st.get('inseridos', 0)} inseridos, "
                            f"{st.get('atualizados', 0)} atualizados"
                        )
                except Exception as e:
                    logger.error(f"  Erro Telegram: {e}")
                    stats['erros'] += 1
                _mover_para_processados(arquivo)

            else:
                logger.warning(f"  ⚠ Tipo não identificado pelo cabeçalho - tentando como portabilidade")
                if arquivo.suffix.lower() in ('.csv', '.xlsx', '.xls'):
                    try:
                        from processar_atualizacoes_gerar_finais import processar_arquivos_atualizacao
                        _stats = processar_arquivos_atualizacao(arquivos_especificos=[str(arquivo)])
                        if _stats.get('arquivos_processados', 0) > 0:
                            stats['csv_processados'] += 1
                        else:
                            stats['ignorados'] += 1
                        # processar_arquivos_atualizacao já move o arquivo para processados
                    except Exception:
                        stats['ignorados'] += 1
                        _mover_para_processados(arquivo)  # Move em caso de exceção
                else:
                    stats['ignorados'] += 1
                    _mover_para_processados(arquivo)

        except Exception as e:
            logger.error(f"Erro ao processar {arquivo.name}: {e}", exc_info=True)
            stats['erros'] += 1

    logger.info("")
    return stats


def _processar_arquivo_v2(arquivo: Path, db_v2, importador) -> dict:
    """
    Importa um arquivo para o banco de dados v2 usando o Importador.

    Args:
        arquivo: Caminho do arquivo CSV/Excel.
        db_v2: Instância de DatabaseManagerV2.
        importador: Instância de Importador.

    Returns:
        Dicionário com estatísticas da importação v2.
    """
    stats_v2 = {'inseridos': 0, 'erros': 0, 'status': 'ignorado'}
    try:
        if not arquivo.exists():
            return stats_v2
        resultado = importador.importar_arquivo(str(arquivo), db_v2)
        stats_v2['inseridos'] = resultado.get('inseridos', 0)
        stats_v2['erros'] = resultado.get('erros', 0)
        stats_v2['status'] = resultado.get('status', 'erro')
        if stats_v2['inseridos'] > 0:
            logger.info(f"  ✓ v2: {arquivo.name} importado — inseridos={stats_v2['inseridos']}, erros={stats_v2['erros']}")
    except Exception as e:
        logger.warning(f"  ⚠ v2: Erro ao importar {arquivo.name}: {e}")
        stats_v2['status'] = 'erro'
    return stats_v2


def verificar_integridade_banco(db_path: str) -> dict:
    """
    Executa PRAGMA integrity_check e quick_check no banco.
    Retorna {'ok': bool, 'quick_check': str, 'integrity_check': str, 'errors': list}.
    """
    try:
        from revisar_tabelas_db import verificar_integridade
        r = verificar_integridade(db_path)
        ic = r.get('integrity_check', [])
        ic_str = ic[0] if (isinstance(ic, list) and len(ic) == 1) else str(ic)
        return {
            'ok': r.get('ok', False),
            'quick_check': r.get('quick_check', '?'),
            'integrity_check': ic_str,
            'errors': [] if r.get('ok') else [ic_str]
        }
    except ImportError:
        pass
    try:
        from src.database import DatabaseManager
        dm = DatabaseManager(db_path)
        r = dm.validate_database_integrity()
        return {
            'ok': r.get('integrity_check') == 'OK' and r.get('foreign_keys') == 'OK',
            'quick_check': r.get('integrity_check', '?'),
            'integrity_check': r.get('integrity_check', '?'),
            'errors': r.get('errors', [])
        }
    except Exception as e:
        logger.warning(f"Verificação de integridade não disponível: {e}")
        return {'ok': False, 'quick_check': 'erro', 'integrity_check': str(e), 'errors': [str(e)]}


def contagens_tabelas_principais(db_path: str) -> dict:
    """Retorna contagem de linhas das tabelas principais (auditoria pós-carga)."""
    import sqlite3
    counts = {}
    tabelas = ('base_coverte_prop', 'portabilidade_records', 'relatorio_objetos', 'relatorio_faturamento', 'portabilidade_processamento', 'bs_venda_du', 'tim_pre_controle', 'estornos')
    try:
        conn = sqlite3.connect(db_path)
        for t in tabelas:
            try:
                cur = conn.execute(f"SELECT COUNT(*) FROM [{t}]")
                counts[t] = cur.fetchone()[0]
            except sqlite3.OperationalError:
                counts[t] = None
        conn.close()
    except Exception as e:
        logger.debug(f"Contagens: {e}")
    return counts


def _mover_para_processados(arquivo: Path) -> None:
    """Envia arquivo para a Lixeira do macOS (ou deleta em outros SOs)."""
    try:
        if not arquivo.exists():
            return
        if sys.platform == 'darwin':
            # macOS: usar osascript para mover para Lixeira
            import subprocess as _sp
            result = _sp.run(
                ['osascript', '-e',
                 f'tell application "Finder" to delete POSIX file "{arquivo.resolve()}"'],
                capture_output=True, text=True, timeout=10
            )
            if result.returncode == 0:
                logger.info(f"  ✓ Enviado para Lixeira: {arquivo.name}")
            else:
                # Fallback: deletar direto
                arquivo.unlink()
                logger.info(f"  ✓ Deletado: {arquivo.name}")
        else:
            arquivo.unlink()
            logger.info(f"  ✓ Deletado: {arquivo.name}")
    except Exception as e:
        logger.warning(f"  ⚠ Não foi possível remover {arquivo.name}: {e}")


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
            # Mover para pasta padrão de destino quando for arquivo local (não rede/SMB)
            try:
                arq = Path(arquivo_coverte)
                em_rede = (arquivo_coverte_network and arq.resolve() == Path(arquivo_coverte_network).resolve()) or (
                    str(arq).startswith("/Volumes/") or "Volumes" in str(arq)
                )
                if not em_rede and arq.exists():
                    pasta_processados.mkdir(parents=True, exist_ok=True)
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    novo_nome = f"{arq.stem}_{timestamp}{arq.suffix}"
                    destino = pasta_processados / novo_nome
                    shutil.move(str(arq), str(destino))
                    logger.info(f"✅ COVERTE movido para pasta de destino: {destino.name}")
            except Exception as e:
                logger.warning(f"⚠️ Não foi possível mover COVERTE para processados: {e}")
        else:
            logger.warning("⚠️ Arquivo COVERTE BASE PROP.xlsx não encontrado")
            logger.info("")
            logger.info("Pastas verificadas (em ordem de prioridade):")
            logger.info(f"  1. SMB: smb://files/02 Planejamento/02 - Relatórios/08 - Relatorios Cliente/")
            logger.info(f"  2. Rede: {pasta_coverte_network}")
            logger.info(f"  3. Local: {pasta_coverte_local}")
            logger.info(f"  4. Entrada: {pasta_entrada}")
            logger.info("")
            logger.info("💡 Possíveis soluções:")
            logger.info("   • Monte o SMB: Finder > Cmd+K > smb://files/02 Planejamento")
            logger.info("   • Ou copie COVERTE BASE PROP.xlsx para: data/entrada/ ou data/entrada/excel/")
            logger.info("   • Teste a rede: python test_conexao_rede.py")
    
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


# Nome do arquivo Excel GROSS (Pre Fechamento) na pasta de importações
ARQUIVO_GROSS_NOME = "3F_GROSS_Pre_Fechamento.xlsx"


def processar_excel_gross() -> dict:
    """
    Processa o arquivo 3F_GROSS_Pre_Fechamento.xlsx da pasta IMPORTACOES_QIGGER
    e insere na tabela base_coverte_prop do portabilidade.db
    """
    logger.info("=" * 70)
    logger.info("ETAPA 1b: PROCESSANDO EXCEL 3F_GROSS_Pre_Fechamento.xlsx")
    logger.info("=" * 70)
    
    stats = {
        'sucesso': False,
        'arquivo': None,
        'stats': {}
    }
    
    try:
        # Procurar arquivo na pasta de importações
        arquivo_gross = None
        
        if pasta_importacoes.exists():
            # Nome exato
            candidato = pasta_importacoes / ARQUIVO_GROSS_NOME
            if candidato.exists():
                arquivo_gross = candidato
            # Também aceitar variações (ex: 3F_GROSS_03_02.xlsx) — usar o mais recente
            if not arquivo_gross:
                candidatos = list(pasta_importacoes.glob("3F_GROSS*.xlsx"))
                if candidatos:
                    arquivo_gross = max(candidatos, key=lambda p: p.stat().st_mtime)
            if not arquivo_gross:
                candidatos = list(pasta_importacoes.glob("*GROSS*Pre*Fechamento*.xlsx"))
                if candidatos:
                    arquivo_gross = max(candidatos, key=lambda p: p.stat().st_mtime)
        
        if not arquivo_gross:
            logger.info(f"⚠️ Arquivo {ARQUIVO_GROSS_NOME} não encontrado em {pasta_importacoes}")
            logger.info("")
            return stats
        
        logger.info(f"✓ Arquivo encontrado: {arquivo_gross.name}")
        
        try:
            from processar_excel_unificado import processar_excel_unificado
            
            logger.info(f"Processando: {arquivo_gross}")
            stats_processamento = processar_excel_unificado(arquivo_gross, db_path)
            
            stats['sucesso'] = True
            stats['arquivo'] = str(arquivo_gross)
            stats['stats'] = stats_processamento
            logger.info(f"✅ Excel GROSS processado: {stats_processamento.get('processados', 0)} registros na base_coverte_prop (portabilidade.db)")
                
        except ImportError as e:
            logger.error(f"❌ Erro ao importar processar_excel_unificado: {e}")
            import traceback
            logger.error(traceback.format_exc())
        except Exception as e:
            logger.error(f"❌ Erro ao processar Excel GROSS: {e}")
            import traceback
            logger.error(traceback.format_exc())
        finally:
            # Sempre mover arquivo para pasta padrão de destino (data/processados)
            if arquivo_gross and arquivo_gross.exists():
                try:
                    pasta_processados.mkdir(parents=True, exist_ok=True)
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    novo_nome = f"{arquivo_gross.stem}_{timestamp}{arquivo_gross.suffix}"
                    destino = pasta_processados / novo_nome
                    shutil.move(str(arquivo_gross), str(destino))
                    logger.info(f"✅ Arquivo movido para pasta de destino: {destino.name}")
                except Exception as e:
                    logger.warning(f"⚠️ Não foi possível mover arquivo para processados: {e}")
            
    except Exception as e:
        logger.error(f"❌ Erro na etapa Excel GROSS: {e}")
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
            finally:
                # Sempre mover arquivo para pasta padrão de destino (data/processados)
                if arquivo_objetos and arquivo_objetos.exists():
                    try:
                        pasta_processados.mkdir(parents=True, exist_ok=True)
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        novo_nome = f"{arquivo_objetos.stem}_{timestamp}{arquivo_objetos.suffix}"
                        destino = pasta_processados / novo_nome
                        shutil.move(str(arquivo_objetos), str(destino))
                        logger.info(f"✅ Arquivo movido para pasta de destino: {destino.name}")
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


# Pasta e comandos para sincronizar D1 Entrega (Cloudflare Worker)
PASTA_D1_ENTREGA = Path("/Applications/Documentos/Projetos_python/3F_Mensagens/cloudflare-worker-entrega")


def sincronizar_d1_entrega() -> bool:
    """
    Sincroniza banco D1 do Cloudflare (3f-entrega) para SQLite local.
    Executa: wrangler d1 export + sqlite3 import.
    Pode ser rodado independentemente com: --apenas-d1-entrega
    """
    logger.info("=" * 70)
    logger.info("SINCRONIZAÇÃO D1 ENTREGA (Cloudflare → SQLite local)")
    logger.info("=" * 70)
    if not PASTA_D1_ENTREGA.exists():
        logger.error(f"Pasta não encontrada: {PASTA_D1_ENTREGA}")
        return False
    db_local = PASTA_D1_ENTREGA / "3f-entrega-local.sqlite"
    backup_sql = PASTA_D1_ENTREGA / "3f-entrega-backup.sql"
    try:
        logger.info(f"Diretório: {PASTA_D1_ENTREGA}")
        # 1. wrangler d1 export 3f-entrega-db --remote --output=3f-entrega-backup.sql
        logger.info("Exportando D1 remoto (wrangler d1 export)...")
        r1 = subprocess.run(
            ["wrangler", "d1", "export", "3f-entrega-db", "--remote", "--output=3f-entrega-backup.sql"],
            cwd=PASTA_D1_ENTREGA,
            capture_output=True,
            text=True
        )
        if r1.returncode != 0:
            logger.error("wrangler d1 export falhou")
            if r1.stderr:
                logger.error(f"  stderr: {r1.stderr.strip()}")
            return False
        if not backup_sql.exists():
            logger.error(f"Arquivo de backup não gerado: {backup_sql}")
            return False
        # 2. Remover banco local antigo para import limpo (evita conflitos)
        if db_local.exists():
            db_local.unlink()
            logger.info("  Banco local anterior removido para import limpo")
        # 3. sqlite3 3f-entrega-local.sqlite < 3f-entrega-backup.sql
        logger.info("Importando para SQLite local...")
        with open(backup_sql, "rb") as f:
            r2 = subprocess.run(
                ["sqlite3", str(db_local)],
                stdin=f,
                cwd=PASTA_D1_ENTREGA,
                capture_output=True,
                text=False
            )
        if r2.returncode != 0:
            logger.error("sqlite3 import falhou")
            err = (r2.stderr or b"").decode("utf-8", errors="replace").strip()
            if err:
                logger.error(f"  stderr: {err}")
            return False
        if not db_local.exists() or db_local.stat().st_size == 0:
            logger.error("Banco local não foi criado ou está vazio")
            return False
        logger.info(f"✓ D1 Entrega sincronizado: {db_local.name} ({db_local.stat().st_size / 1024:.1f} KB)")
        return True
    except Exception as e:
        logger.error(f"Erro ao sincronizar D1 entrega: {e}")
        import traceback
        traceback.print_exc()
        return False


def executar_script_homologacao(script_path: Path) -> bool:
    """
    Executa um script de homologação.
    stdout vai ao terminal em tempo real (via Tee interno).
    stderr é capturado e logado em caso de falha.
    Propaga decisão V2/legado via variáveis de ambiente.
    """
    if not script_path.exists():
        logger.error(f"Script não encontrado: {script_path}")
        return False

    try:
        logger.info(f"Executando: {script_path.name}")
        # stdout: herda do processo pai → saída em tempo real no terminal
        # stderr: PIPE para capturar e logar em caso de falha
        import os as _os
        env = _os.environ.copy()
        # Propagar decisão V2/legado do processar_completo para os geradores
        if _os.environ.get('QIGGER_FORCAR_LEGADO'):
            env['QIGGER_FORCAR_LEGADO'] = '1'
        if _os.environ.get('QIGGER_FORCAR_V2'):
            env['QIGGER_FORCAR_V2'] = '1'
        result = subprocess.run(
            [sys.executable, str(script_path)],
            cwd=Path(__file__).parent,
            stderr=subprocess.PIPE,
            text=True,
            encoding='utf-8',
            errors='replace',
            env=env
        )

        if result.returncode == 0:
            logger.info(f"✓ {script_path.name} executado com sucesso (código 0)")
            if result.stderr and result.stderr.strip():
                logger.debug(f"  stderr: {result.stderr.strip()[:500]}")
            return True
        else:
            logger.error(f"✗ {script_path.name} falhou (código {result.returncode})")
            if result.stderr:
                for linha in result.stderr.strip().splitlines()[-30:]:
                    logger.error(f"  STDERR: {linha}")
            return False

    except subprocess.TimeoutExpired:
        logger.error(f"✗ {script_path.name}: timeout")
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


def _verificar_arquivo_gerado(arquivo: Path) -> tuple:
    """
    Verifica se o arquivo gerado tem dados reais (ao menos uma linha de dados além do cabeçalho).
    Returns: (tem_dados: bool, tamanho_bytes: int, num_linhas_dados: int)
    """
    if not arquivo.exists():
        return False, 0, 0
    tamanho = arquivo.stat().st_size
    if tamanho == 0:
        return False, 0, 0
    try:
        import pandas as pd
        ext = arquivo.suffix.lower()
        if ext == '.xlsx':
            df = pd.read_excel(arquivo, engine='openpyxl', nrows=3)
        elif ext == '.csv':
            df = pd.read_csv(arquivo, nrows=3, sep=None, engine='python', on_bad_lines='skip')
        else:
            return tamanho > 0, tamanho, 0
        num_linhas = len(df)
        return num_linhas > 0, tamanho, num_linhas
    except Exception:
        return tamanho > 0, tamanho, 0


def _run_script_subprocess(item: dict) -> tuple:
    """
    Roda um script de homologação em subprocess (para ProcessPoolExecutor).
    Propaga decisão V2/legado via variáveis de ambiente.
    Returns: (sucesso: bool, stdout: str, stderr: str)
    """
    root = Path(__file__).parent
    script_path = root / item['script']
    if not script_path.exists():
        return False, '', f"Script não encontrado: {script_path}"
    try:
        import os as _os
        env = _os.environ.copy()
        # Propagar decisão V2/legado do processar_completo para os geradores
        if _os.environ.get('QIGGER_FORCAR_LEGADO'):
            env['QIGGER_FORCAR_LEGADO'] = '1'
        if _os.environ.get('QIGGER_FORCAR_V2'):
            env['QIGGER_FORCAR_V2'] = '1'
        r = subprocess.run(
            [sys.executable, str(script_path)],
            cwd=root,
            capture_output=True,
            text=True,
            timeout=3600,
            encoding='utf-8',
            errors='replace',
            env=env
        )
        return r.returncode == 0, r.stdout or '', r.stderr or ''
    except subprocess.TimeoutExpired:
        return False, '', f"Timeout ao executar {item['script']}"
    except Exception as exc:
        return False, '', str(exc)


def _processar_resultado_homologacao(
    item: dict,
    sucesso_script: bool,
    stdout_script: str = '',
    stderr_script: str = ''
) -> tuple:
    """
    Valida, copia e move o arquivo gerado por um script de homologação.
    Returns: (nome, arquivo_copiado_ou_None, status_str)
    Status: 'sucesso' | 'sem_dados' | 'nao_gerado' | 'falha_script' | 'falha_copia'
    """
    nome = item['nome']
    arquivo_origem = Path(__file__).parent / item['arquivo_origem']

    # Log da saída do subprocesso (modo paralelo)
    if not sucesso_script:
        logger.error(f"  ✗ [{nome}] Script falhou")
        if stderr_script:
            for linha in stderr_script.strip().splitlines()[-30:]:
                logger.error(f"    STDERR: {linha}")
        return nome, None, 'falha_script'

    # Log stderr opcional (avisos do script mesmo em sucesso)
    if stderr_script and stderr_script.strip():
        for linha in stderr_script.strip().splitlines()[-10:]:
            logger.debug(f"  [{nome}] stderr: {linha}")

    if not arquivo_origem.exists():
        logger.warning(f"  ⚠ [{nome}] Arquivo esperado não gerado: {arquivo_origem}")
        return nome, None, 'nao_gerado'

    tem_dados, tamanho, num_linhas = _verificar_arquivo_gerado(arquivo_origem)
    tamanho_kb = tamanho / 1024

    if not tem_dados:
        # Aprovisionamento: copiar mesmo vazio para garantir que o arquivo esteja na pasta de retornos
        if nome == 'Aprovisionamento':
            arquivo_copiado = copiar_arquivo_com_timestamp(arquivo_origem, item['prefixo_nome'], PASTA_SAIDA)
            if arquivo_copiado:
                logger.info(
                    f"  ✓ [{nome}] Arquivo copiado (sem dados, apenas cabeçalho) — {tamanho_kb:.1f} KB → {arquivo_copiado}"
                )
                _mover_para_processados(arquivo_origem)
                return nome, arquivo_copiado, 'sem_dados'
        logger.warning(
            f"  ⚠ [{nome}] Arquivo gerado sem dados (apenas cabeçalho ou vazio) "
            f"({tamanho_kb:.1f} KB) → não será copiado para saída"
        )
        # Mesmo sem dados, mover para processados para não acumular em data/
        _mover_para_processados(arquivo_origem)
        return nome, None, 'sem_dados'

    logger.info(f"  ✓ [{nome}] {arquivo_origem.name} — {tamanho_kb:.1f} KB, ~{num_linhas}+ linhas de dados")

    # Copiar para PASTA_SAIDA com timestamp
    arquivo_copiado = copiar_arquivo_com_timestamp(arquivo_origem, item['prefixo_nome'], PASTA_SAIDA)
    if not arquivo_copiado:
        logger.error(f"  ✗ [{nome}] Falha ao copiar para {PASTA_SAIDA}")
        return nome, None, 'falha_copia'

    logger.info(f"  ✓ [{nome}] Salvo em: {arquivo_copiado}")

    # Mover original de data/ para pasta_processados
    _mover_para_processados(arquivo_origem)
    return nome, arquivo_copiado, 'sucesso'


def gerar_arquivos_homologacao(workers: int = 1) -> dict:
    """Gera todos os arquivos de homologação (workers=1 sequencial; workers>1 em paralelo)."""
    logger.info("=" * 70)
    logger.info("ETAPA 4: GERANDO ARQUIVOS DE HOMOLOGAÇÃO")
    logger.info("=" * 70)

    resultados = {
        'sucesso': [],
        'sem_dados': [],
        'falha': [],
        'arquivos_copiados': []
    }

    # Verificar e criar pasta de saída ANTES de executar os scripts
    try:
        PASTA_SAIDA.mkdir(parents=True, exist_ok=True)
        logger.info(f"Pasta de saída (retornos): {PASTA_SAIDA}")
    except Exception as e:
        logger.error(f"Não foi possível criar/acessar pasta de saída '{PASTA_SAIDA}': {e}")
        logger.error("Verifique se o caminho existe e se você tem permissão de escrita.")
        return resultados

    # Testar escrita na pasta de saída
    try:
        teste = PASTA_SAIDA / '.write_test'
        teste.touch()
        teste.unlink()
    except Exception as e:
        logger.error(f"Sem permissão de escrita na pasta de saída '{PASTA_SAIDA}': {e}")
        return resultados

    total_scripts = len(SCRIPTS_HOMOLOGACAO)
    logger.info(f"Scripts a executar: {total_scripts}")
    if workers > 1:
        logger.info(f"Modo: paralelo ({workers} workers)")
    else:
        logger.info("Modo: sequencial")

    # ── MODO PARALELO ────────────────────────────────────────────────────────
    if workers > 1:
        from concurrent.futures import ProcessPoolExecutor, as_completed
        script_list = list(SCRIPTS_HOMOLOGACAO)
        resultados_raw = {}  # script → (sucesso, stdout, stderr)
        concluidos = 0

        with ProcessPoolExecutor(max_workers=min(workers, len(script_list))) as executor:
            futures = {executor.submit(_run_script_subprocess, item): item for item in script_list}
            for fut in as_completed(futures):
                item = futures[fut]
                concluidos += 1
                try:
                    ok, stdout, stderr = fut.result()
                except Exception as exc:
                    ok, stdout, stderr = False, '', str(exc)
                resultados_raw[item['script']] = (ok, stdout, stderr)
                status_icon = '✓' if ok else '✗'
                logger.info(f"  [{concluidos}/{total_scripts}] {item['nome']} {status_icon}")
                if not ok and stderr:
                    for linha in stderr.strip().splitlines()[-10:]:
                        logger.error(f"    STDERR: {linha}")

        # Processar resultados na ordem original
        for item in SCRIPTS_HOMOLOGACAO:
            ok, stdout, stderr = resultados_raw.get(item['script'], (False, '', 'não executado'))
            nome, arq_copiado, status = _processar_resultado_homologacao(item, ok, stdout, stderr)
            if status == 'sucesso':
                resultados['sucesso'].append(nome)
                resultados['arquivos_copiados'].append(arq_copiado)
            elif status == 'sem_dados':
                resultados['sem_dados'].append(nome)
                if arq_copiado:
                    resultados['arquivos_copiados'].append(arq_copiado)
            else:
                resultados['falha'].append(nome)

        logger.info("")
        return resultados

    # ── MODO SEQUENCIAL ───────────────────────────────────────────────────────
    for idx, item in enumerate(SCRIPTS_HOMOLOGACAO, 1):
        logger.info(f"\n── [{idx}/{total_scripts}] {item['nome']} ──")
        script_path = Path(__file__).parent / item['script']
        sucesso_script = executar_script_homologacao(script_path)

        nome, arq_copiado, status = _processar_resultado_homologacao(item, sucesso_script)
        if status == 'sucesso':
            resultados['sucesso'].append(nome)
            resultados['arquivos_copiados'].append(arq_copiado)
        elif status == 'sem_dados':
            resultados['sem_dados'].append(nome)
            if arq_copiado:
                resultados['arquivos_copiados'].append(arq_copiado)
        else:
            resultados['falha'].append(nome)

    logger.info("")
    return resultados


def usar_v2(db_manager_v2, args) -> bool:
    """Decide se usa V2 ou fallback para legado."""
    if getattr(args, 'forcar_legado', False):
        logger.info("Flag --forcar-legado ativa, usando banco legado")
        return False
    if getattr(args, 'forcar_v2', False):
        return True
    if db_manager_v2 is None:
        return False
    try:
        resultado = db_manager_v2.validar_integridade()
        if resultado.get('ok'):
            return True
        logger.warning("V2 com problemas de integridade, usando legado")
        return False
    except Exception as e:
        logger.warning("V2 indisponível (%s), usando legado", e)
        return False


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
  python3 processar_completo.py --apenas-d1-entrega   # Sincroniza D1 Entrega (Cloudflare→SQLite)
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
        '--skip-faturamento',
        action='store_true',
        help='Pula o processamento do Relatorio Faturamento (relatorio*.csv)'
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
    
    parser.add_argument(
        '--cafinate', '--caffeinate',
        dest='cafinate',
        action='store_true',
        help='(macOS) Impede o sistema de dormir durante o processamento (usa caffeinate -i)'
    )
    parser.add_argument(
        '--no-caffeinate',
        dest='no_caffeinate',
        action='store_true',
        help='(macOS) Desativa caffeinate mesmo quando ativado por padrão'
    )
    parser.add_argument(
        '--workers',
        type=int,
        default=1,
        metavar='N',
        help='Número de workers para gerar homologação em paralelo (default: 1). Ex.: --workers 3'
    )
    parser.add_argument(
        '--apenas-d1-entrega',
        action='store_true',
        help='Apenas sincroniza D1 Entrega (Cloudflare → SQLite local). Independente do fluxo principal.'
    )
    parser.add_argument(
        '--apenas-reprocessamento',
        action='store_true',
        help='Apenas executa reprocessamento de endereços (ETAPA 5)'
    )
    parser.add_argument(
        '--skip-reprocessamento',
        action='store_true',
        help='Pula o reprocessamento de endereços (ETAPA 5)'
    )
    parser.add_argument(
        '--skip-auditoria',
        action='store_true',
        help='Pula a auditoria de vendas TIM Pré/Controle (ETAPA 2b)'
    )
    parser.add_argument(
        '--forcar-legado',
        action='store_true',
        help='Forçar geração de homologação a partir do banco legado'
    )
    parser.add_argument(
        '--forcar-v2',
        action='store_true',
        help='Forçar geração de homologação exclusivamente a partir do banco V2'
    )
    
    args = parser.parse_args()
    
    # Reexecutar sob caffeinate no macOS somente se --cafinate foi passado explicitamente.
    # (Se ativássemos por padrão no macOS, o processo filho também ativaria e entraria em loop.)
    usar_caffeinate = getattr(args, 'cafinate', False)
    if usar_caffeinate and sys.platform == 'darwin':
        argv_sem_cafinate = [a for a in sys.argv if a not in ('--cafinate', '--caffeinate')]
        script_path = str(Path(__file__).resolve())
        cmd = ['caffeinate', '-i', sys.executable, script_path] + argv_sem_cafinate[1:]
        logger.info("Iniciando sob caffeinate (sistema não irá dormir)")
        result = subprocess.run(cmd)
        sys.exit(result.returncode)
    
    # Execução independente: apenas sincronizar D1 Entrega
    if getattr(args, 'apenas_d1_entrega', False):
        ok = sincronizar_d1_entrega()
        sys.exit(0 if ok else 1)
    
    # Execução independente: apenas reprocessamento de endereços
    if getattr(args, 'apenas_reprocessamento', False):
        logger.info("Modo: apenas reprocessamento de endereços")
        try:
            from src.reprocessamento import ReprocessadorEndereco
            from config import PROXY_FILE
            _proxy_cfg = PROXY_FILE if Path(PROXY_FILE).exists() else None
            _workers = max(1, int(getattr(args, 'workers', 1) or 1))
            reprocessador = ReprocessadorEndereco(
                db_v2_path=db_v2_path,
                periodo_dias=90,
                diretorio_saida=str(PASTA_SAIDA),
                config_proxies=_proxy_cfg,
                workers=_workers,
            )
            resultado = reprocessador.executar()
            print(f"Reprocessamento concluído: {resultado}")
        except Exception as e:
            print(f"Erro: {e}")
            logger.error("Erro no reprocessamento: %s", e, exc_info=True)
        sys.exit(0)
    
    # Início
    print("=" * 70)
    print("PROCESSAMENTO COMPLETO - 3F Qigger DB Gerenciador")
    print("=" * 70)
    print(f"Data/Hora: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print(f"Banco de dados: {db_path}")
    print(f"Banco de dados v2: {db_v2_path}")
    print()
    
    # Inicializar DatabaseManagerV2 e Importador para integração paralela
    db_manager_v2 = None
    importador_v2 = None
    exec_id_v2 = None
    registros_v2_processados = 0
    registros_v2_erros = 0
    try:
        from src.database.db_manager_v2 import DatabaseManagerV2
        from src.database.importador import Importador
        db_manager_v2 = DatabaseManagerV2(db_v2_path)
        importador_v2 = Importador()
        exec_id_v2 = db_manager_v2.registrar_execucao('processamento_completo', {
            'db_path_v1': db_path,
            'db_v2_path': db_v2_path,
            'args': {k: str(v) for k, v in vars(args).items()},
        })
        logger.info(f"✓ DatabaseManagerV2 inicializado (execução #{exec_id_v2})")
    except Exception as e:
        logger.warning(f"⚠ DatabaseManagerV2 não disponível, continuando apenas com v1: {e}")
    
    # Estatísticas gerais
    stats_geral = {
        'excel': {},
        'excel_gross': {},
        'csv': {},
        'objetos': {},
        'homologacao': {},
        'auditoria': {},
    }
    
    # Processar bases (se não for apenas homologação)
    # ORDEM OBRIGATÓRIA: atualizar portabilidade.db ANTES de gerar homologação

    # ETAPA 0: Sincronizar bot_processamento da Oracle Cloud
    logger.info("")
    logger.info(">>> ETAPA 0: Sincronizar Bot Processamento (Oracle Cloud → Local)")
    try:
        from sincronizar_bot_oracle import sincronizar_bot_oracle
        stats_oracle = sincronizar_bot_oracle(local_db_path=db_v2_path)
        _oracle_novos = stats_oracle.get("novos_inseridos", 0)
        _oracle_total = stats_oracle.get("total_oracle", 0)
        if _oracle_novos > 0:
            logger.info("  ✓ Oracle sync: %d novos de %d total", _oracle_novos, _oracle_total)
        else:
            logger.info("  ✓ Oracle sync: nenhum registro novo (total oracle: %d)", _oracle_total)
    except Exception as e:
        logger.warning("  ⚠ Erro ao sincronizar Oracle: %s", e)

    if not args.apenas_homologacao:

        # ETAPA 1: BS_VENDA_DU (coleta e atualização — independente da COVERTE)
        if not args.skip_excel:
            logger.info("")
            logger.info(">>> ETAPA 1: BS_VENDA_DU (coleta e atualização)")
            # v2: Atualizar etapa_atual
            if db_manager_v2 and exec_id_v2:
                try:
                    with db_manager_v2._get_connection() as conn:
                        conn.execute(
                            "UPDATE execucoes_processamento SET etapa_atual = ? WHERE id = ?",
                            ('importacao_bs_venda_du', exec_id_v2)
                        )
                        conn.commit()
                except Exception:
                    pass
            try:
                from processar_bs_venda_du import processar_bs_venda_du
                stats_bs = processar_bs_venda_du(db_path=db_path)
                if stats_bs.get('sucesso'):
                    logger.info(f"  ✓ BS_VENDA_DU: {stats_bs.get('linhas', 0)} registros → tabela bs_venda_du")
                else:
                    logger.warning("  ⚠ BS_VENDA_DU: arquivo não encontrado ou erro")
            except Exception as e:
                logger.warning(f"  ⚠ BS_VENDA_DU: {e}")
            # Verificação de integridade pós Etapa 1
            integridade = verificar_integridade_banco(db_path)
            if integridade.get('ok'):
                logger.info("  ✓ Integridade do banco: OK (pós BS_VENDA_DU)")
            else:
                logger.warning(f"  ⚠ Integridade do banco: {integridade.get('integrity_check', 'erro')}")

        # 2) Arquivos da pasta (CSV, Relatorio_Objetos, GROSS, faturamento)
        logger.info("")
        logger.info(">>> ETAPA 2: Arquivos da pasta de entrada (portabilidade_records, relatorio_objetos, etc.)")
        # v2: Atualizar etapa_atual
        if db_manager_v2 and exec_id_v2:
            try:
                with db_manager_v2._get_connection() as conn:
                    conn.execute(
                        "UPDATE execucoes_processamento SET etapa_atual = ? WHERE id = ?",
                        ('importacao_arquivos', exec_id_v2)
                    )
                    conn.commit()
            except Exception:
                pass
        stats_unificado = processar_arquivos_pasta_entrada_unificado(
            skip_csv=args.skip_csv,
            skip_excel=args.skip_excel,
            skip_objetos=args.skip_objetos,
            skip_faturamento=args.skip_faturamento,
            db_v2=db_manager_v2,
            importador_v2=importador_v2,
        )
        if not args.skip_excel:
            stats_geral['excel'] = {'sucesso': stats_unificado.get('excel_coverte', 0) > 0}
        stats_geral['excel_gross'] = {'sucesso': stats_unificado.get('excel_gross', 0) > 0}
        stats_geral['csv'] = {
            'sucesso': stats_unificado.get('csv_processados', 0) > 0,
            'arquivos_processados': stats_unificado.get('csv_processados', 0),
            'registros_processados': 0
        }
        stats_geral['objetos'] = {'sucesso': stats_unificado.get('excel_objetos', 0) > 0}

        # Atualizar contadores V2 para finalização da execução
        registros_v2_processados = (
            stats_unificado.get('csv_processados', 0)
            + stats_unificado.get('excel_coverte', 0)
            + stats_unificado.get('excel_gross', 0)
            + stats_unificado.get('excel_objetos', 0)
            + stats_unificado.get('tim_pre_controle', 0)
            + stats_unificado.get('relatorio_faturamento', 0)
            + stats_unificado.get('estornos', 0)
            + stats_unificado.get('auditoria_rpa_inseridos', 0)
        )
        registros_v2_erros = stats_unificado.get('erros', 0)

        # Verificação de integridade e contagens pós Etapa 2 (DBA/MIS)
        integridade2 = verificar_integridade_banco(db_path)
        if integridade2.get('ok'):
            logger.info("  ✓ Integridade do banco: OK (pós arquivos da pasta)")
        else:
            logger.warning(f"  ⚠ Integridade do banco: {integridade2.get('integrity_check', 'erro')}")
        contagens = contagens_tabelas_principais(db_path)
        if contagens:
            logger.info("  Contagens (auditoria): " + ", ".join(f"{t}={c}" for t, c in contagens.items() if c is not None))

        # v2: Validar integridade do banco v2 após Etapa 2
        if db_manager_v2:
            try:
                integridade_v2 = db_manager_v2.validar_integridade()
                logger.info(f"  ✓ v2: Integridade pós Etapa 2: ok={integridade_v2.get('ok')}")
            except Exception as e:
                logger.warning(f"  ⚠ v2: Erro na validação de integridade: {e}")

        # v2: Atualizar etapa_atual após processamento de arquivos
        if db_manager_v2 and exec_id_v2:
            try:
                with db_manager_v2._get_connection() as conn:
                    conn.execute(
                        "UPDATE execucoes_processamento SET etapa_atual = ? WHERE id = ?",
                        ('pos_importacao_arquivos', exec_id_v2)
                    )
                    conn.commit()
            except Exception:
                pass

        # ETAPA 2b: Auditoria de vendas TIM Pré/Controle (EVA + Cruzamento)
        if db_manager_v2 and not getattr(args, 'skip_auditoria', False):
            logger.info("")
            logger.info(">>> ETAPA 2b: Auditoria de vendas TIM Pré/Controle (EVA + Cruzamento)")
            if exec_id_v2:
                try:
                    with db_manager_v2._get_connection() as conn:
                        conn.execute(
                            "UPDATE execucoes_processamento SET etapa_atual = ? WHERE id = ?",
                            ('auditoria_vendas_tim', exec_id_v2)
                        )
                        conn.commit()
                except Exception:
                    pass
            try:
                from src.pipeline_auditoria.pipeline import PipelineAuditoria
                pipeline_auditoria = PipelineAuditoria(db_manager_v2)

                # RPA já processado na ETAPA 2 (inline), aqui só EVA + cruzamento
                stats_auditoria = pipeline_auditoria.executar(arquivos_rpa=[])
                stats_geral['auditoria'] = stats_auditoria

                _eva_ins = stats_auditoria.get('eva', {}).get('inseridos', 0)
                _cruz = stats_auditoria.get('cruzamento', {})
                _cruz_total = _cruz.get('total', 0)
                _cruz_match = _cruz.get('com_match', 0)

                logger.info(
                    "  ✓ Auditoria: EVA=%d, Cruzamento=%d (match=%d)",
                    _eva_ins, _cruz_total, _cruz_match,
                )
            except Exception as e:
                logger.warning(f"  ⚠ Erro na auditoria de vendas: {e}")
                stats_geral['auditoria'] = {}

    # ETAPA 3b: Atualizar Cache Unificada no Banco V2
    # BUG 4 FIX: Também atualizar cache quando --apenas-homologacao para garantir dados frescos
    if db_manager_v2:
        logger.info("")
        logger.info(">>> ETAPA 3b: Atualizar Cache Unificada no Banco V2")
        try:
            from src.database.data_unifier import DataUnifier
            import time as _time
            _cache_inicio = _time.time()
            unifier = DataUnifier(db_manager_v2)
            resultado_cache = unifier.reconstruir_cache_completo()
            _cache_tempo = round(_time.time() - _cache_inicio, 2)
            logger.info(
                "  ✓ Cache atualizado: %d inseridos, %d erros, %.2fs",
                resultado_cache.get('inseridos', 0),
                resultado_cache.get('erros', 0),
                _cache_tempo,
            )
        except Exception as e:
            logger.warning("  ⚠ Erro ao atualizar cache: %s", e)
        # v2: Atualizar etapa_atual após cache
        if exec_id_v2:
            try:
                with db_manager_v2._get_connection() as conn:
                    conn.execute(
                        "UPDATE execucoes_processamento SET etapa_atual = ? WHERE id = ?",
                        ('cache_unificada', exec_id_v2)
                    )
                    conn.commit()
            except Exception:
                pass

    # ETAPA 3c: Resolver registros pendentes (proposta_isize não resolvido)
    # Após todas as bases importadas + cache reconstruído, tenta resolver pendências
    if db_manager_v2 and not args.apenas_homologacao:
        logger.info("")
        logger.info(">>> ETAPA 3c: Resolver registros pendentes (proposta_isize)")
        if exec_id_v2:
            try:
                with db_manager_v2._get_connection() as conn:
                    conn.execute(
                        "UPDATE execucoes_processamento SET etapa_atual = ? WHERE id = ?",
                        ('resolver_pendentes', exec_id_v2)
                    )
                    conn.commit()
            except Exception:
                pass
        try:
            from corrigir_id_proposta_isize import corrigir_id_proposta_isize
            stats_correcao = corrigir_id_proposta_isize(db_path=db_path, dry_run=False)
            _corrigidos = stats_correcao.get('corrigidos', 0)
            _nao_encontrados = stats_correcao.get('nao_encontrados', 0)
            _erros_correcao = stats_correcao.get('erros', 0)
            logger.info(
                "  ✓ Pendentes: corrigidos=%d, não resolvidos=%d, erros=%d",
                _corrigidos, _nao_encontrados, _erros_correcao,
            )
            # Se houve correções, reconstruir cache para refletir dados corrigidos
            if _corrigidos > 0:
                logger.info("  ↻ Reconstruindo cache após correções...")
                try:
                    from src.database.data_unifier import DataUnifier
                    import time as _time
                    _rc_inicio = _time.time()
                    unifier = DataUnifier(db_manager_v2)
                    resultado_rc = unifier.reconstruir_cache_completo()
                    _rc_tempo = round(_time.time() - _rc_inicio, 2)
                    logger.info(
                        "  ✓ Cache reconstruído: %d inseridos, %d erros, %.2fs",
                        resultado_rc.get('inseridos', 0),
                        resultado_rc.get('erros', 0),
                        _rc_tempo,
                    )
                except Exception as e:
                    logger.warning("  ⚠ Erro ao reconstruir cache pós-correção: %s", e)
        except Exception as e:
            logger.warning("  ⚠ Erro ao resolver pendentes: %s", e)

        # ETAPA 3d: Reprocessar pendentes GROSS/TIM (telefone com decimal)
        logger.info("")
        logger.info(">>> ETAPA 3d: Reprocessar pendentes GROSS/TIM (normalização telefone)")
        try:
            from src.database.importador import Importador
            _imp_repro = Importador()
            _stats_repro = _imp_repro.reprocessar_pendentes_gross(db_manager_v2)
            _resolvidos = _stats_repro.get('resolvidos', 0)
            _falhas_repro = _stats_repro.get('falhas', 0)
            _total_repro = _stats_repro.get('total', 0)
            logger.info(
                "  ✓ Pendentes GROSS/TIM: resolvidos=%d, falhas=%d, total=%d",
                _resolvidos, _falhas_repro, _total_repro,
            )
            # Se houve resoluções, reconstruir cache
            if _resolvidos > 0:
                logger.info("  ↻ Reconstruindo cache após resolução de pendentes GROSS...")
                try:
                    from src.database.data_unifier import DataUnifier
                    import time as _time
                    _rg_inicio = _time.time()
                    unifier = DataUnifier(db_manager_v2)
                    resultado_rg = unifier.reconstruir_cache_completo()
                    _rg_tempo = round(_time.time() - _rg_inicio, 2)
                    logger.info(
                        "  ✓ Cache reconstruído: %d inseridos, %d erros, %.2fs",
                        resultado_rg.get('inseridos', 0),
                        resultado_rg.get('erros', 0),
                        _rg_tempo,
                    )
                except Exception as e:
                    logger.warning("  ⚠ Erro ao reconstruir cache pós-GROSS: %s", e)
        except Exception as e:
            logger.warning("  ⚠ Erro ao reprocessar pendentes GROSS/TIM: %s", e)

    # Gerar arquivos de homologação (se não for apenas bases)
    if not args.apenas_bases:
        _v2_ativo = usar_v2(db_manager_v2, args)
        # BUG 1 FIX: Propagar decisão V2/legado para subprocessos dos geradores via env vars
        import os as _os
        if getattr(args, 'forcar_legado', False):
            _os.environ['QIGGER_FORCAR_LEGADO'] = '1'
            _os.environ.pop('QIGGER_FORCAR_V2', None)
        elif getattr(args, 'forcar_v2', False):
            _os.environ['QIGGER_FORCAR_V2'] = '1'
            _os.environ.pop('QIGGER_FORCAR_LEGADO', None)
        else:
            _os.environ.pop('QIGGER_FORCAR_LEGADO', None)
            _os.environ.pop('QIGGER_FORCAR_V2', None)
        if _v2_ativo:
            logger.info(">>> ETAPA 4: Geração de homologação via Banco V2")
            if exec_id_v2:
                try:
                    with db_manager_v2._get_connection() as conn:
                        conn.execute(
                            "UPDATE execucoes_processamento SET etapa_atual = ? WHERE id = ?",
                            ('geracao_homologacao_v2', exec_id_v2)
                        )
                        conn.commit()
                except Exception:
                    pass
        else:
            logger.info(">>> ETAPA 4: Geração de homologação via Banco Legado (fallback)")
        workers = max(1, int(getattr(args, 'workers', 1)))
        stats_geral['homologacao'] = gerar_arquivos_homologacao(workers=workers)

    # ETAPA 5: Reprocessamento de endereços inválidos
    if not getattr(args, 'skip_reprocessamento', False) and not args.apenas_bases:
        logger.info("")
        logger.info(">>> ETAPA 5: Reprocessamento de endereços inválidos")
        # BUG 2 FIX: Garantir que workers está definido mesmo se ETAPA 4 não rodou
        _reproc_workers = max(1, int(getattr(args, 'workers', 1)))
        try:
            from src.reprocessamento import ReprocessadorEndereco
            from config import PROXY_FILE
            _proxy_cfg = PROXY_FILE if Path(PROXY_FILE).exists() else None
            reprocessador = ReprocessadorEndereco(
                db_v2_path=db_v2_path,
                periodo_dias=90,
                diretorio_saida=str(PASTA_SAIDA),
                config_proxies=_proxy_cfg,
                workers=_reproc_workers,
            )
            resultado_reproc = reprocessador.executar()
            logger.info(
                "  ✓ Reprocessamento: total=%d, corrigidos=%d, erros=%d, tempo=%.2fs",
                resultado_reproc.get('total', 0),
                resultado_reproc.get('corrigidos', 0),
                resultado_reproc.get('erros', 0),
                resultado_reproc.get('tempo_execucao', 0),
            )
            if resultado_reproc.get('arquivo_saida'):
                logger.info("  ✓ Arquivo: %s", resultado_reproc['arquivo_saida'])
        except Exception as e:
            logger.warning("  ⚠ Erro no reprocessamento de endereços: %s", e)
        # v2: Atualizar etapa_atual após reprocessamento
        if db_manager_v2 and exec_id_v2:
            try:
                with db_manager_v2._get_connection() as conn:
                    conn.execute(
                        "UPDATE execucoes_processamento SET etapa_atual = ? WHERE id = ?",
                        ('reprocessamento_enderecos', exec_id_v2)
                    )
                    conn.commit()
            except Exception:
                pass
    
    # Resumo final
    print("=" * 70)
    print("RESUMO DO PROCESSAMENTO")
    print("=" * 70)
    
    if not args.apenas_homologacao:
        print("\n📊 PROCESSAMENTO DE BASES:")
        
        if not args.skip_excel:
            excel_stats = stats_geral.get('excel', {})
            if excel_stats.get('sucesso'):
                print(f"  ✅ Excel COVERTE/Base (via pasta entrada): processado")
            else:
                print(f"  ⚠️ Excel COVERTE/Base: Nenhum arquivo encontrado na pasta de entrada")
        
        excel_gross_stats = stats_geral.get('excel_gross', {})
        if excel_gross_stats.get('sucesso'):
            print(f"  ✅ Excel 3F_GROSS_Pre_Fechamento: processado")
        elif excel_gross_stats:
            print(f"  ⚠️ Excel 3F_GROSS_Pre_Fechamento: Não encontrado")
        
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
        
        if not args.skip_faturamento:
            _su = locals().get('stats_unificado') or {}
            fat_stats = _su.get('relatorio_faturamento', 0)
            if fat_stats > 0:
                print(f"  ✅ Relatorio Faturamento: {fat_stats} arquivo(s) → relatorio_faturamento")

        # Auditoria de vendas TIM Pré/Controle
        if not getattr(args, 'skip_auditoria', False):
            _aud = stats_geral.get('auditoria', {})
            _aud_rpa_inline = locals().get('stats_unificado', {}).get('auditoria_rpa_inseridos', 0)
            if _aud or _aud_rpa_inline:
                _aud_eva = _aud.get('eva', {}).get('inseridos', 0)
                _aud_cruz = _aud.get('cruzamento', {})
                _aud_total = _aud_cruz.get('total', 0)
                _aud_match = _aud_cruz.get('com_match', 0)
                print(f"  ✅ Auditoria TIM Pré/Ctrl: EVA={_aud_eva}, RPA={_aud_rpa_inline}, "
                      f"Cruzamento={_aud_total} (match={_aud_match})")
            else:
                print(f"  ⚠️ Auditoria TIM Pré/Ctrl: Não executada")
    
    if not args.apenas_bases:
        print("\n📄 ARQUIVOS DE HOMOLOGAÇÃO:")
        homologacao_stats = stats_geral.get('homologacao', {})
        sucesso_lista = homologacao_stats.get('sucesso', [])
        sem_dados_lista = homologacao_stats.get('sem_dados', [])
        falha_lista = homologacao_stats.get('falha', [])
        total = len(SCRIPTS_HOMOLOGACAO)

        print(f"  Com dados gerados : {len(sucesso_lista)}/{total}")
        print(f"  Sem dados (vazio)  : {len(sem_dados_lista)}/{total}")
        print(f"  Falha de script    : {len(falha_lista)}/{total}")

        if sucesso_lista:
            print("\n  ✅ Arquivos gerados com dados:")
            for nome in sucesso_lista:
                print(f"    - {nome}")

        if sem_dados_lista:
            print("\n  ⚠️  Gerados sem dados (apenas cabeçalho — nenhum registro passou nos filtros):")
            for nome in sem_dados_lista:
                print(f"    - {nome}")

        if falha_lista:
            print("\n  ❌ Falha ao gerar (ver log para detalhes):")
            for nome in falha_lista:
                print(f"    - {nome}")

        arquivos_copiados = [a for a in homologacao_stats.get('arquivos_copiados', []) if a]
        if arquivos_copiados:
            print(f"\n  📁 Arquivos salvos em: {PASTA_SAIDA}")
            for arquivo in arquivos_copiados:
                try:
                    kb = arquivo.stat().st_size / 1024
                    print(f"    - {arquivo.name}  ({kb:.1f} KB)")
                except Exception:
                    print(f"    - {arquivo.name}")
    
    # [5] Backup e replicação para rede
    print("\n" + "=" * 70)
    print("ETAPA 6: BACKUP E REPLICAÇÃO PARA REDE")
    print("=" * 70)
    # v2: Atualizar etapa_atual
    if db_manager_v2 and exec_id_v2:
        try:
            with db_manager_v2._get_connection() as conn:
                conn.execute(
                    "UPDATE execucoes_processamento SET etapa_atual = ? WHERE id = ?",
                    ('backup_replicacao', exec_id_v2)
                )
                conn.commit()
        except Exception:
            pass
    
    try:
        from backup_database import replicar_para_rede, BACKUP_REDE_PATH, SMB_URL_BACKOFFICE_LOG
        
        print("\n[5.1] Replicando banco de dados para rede SMB (07 Backoffice)...")
        print(f"    Banco usado no processamento: {db_path}")
        print(f"    Destino rede: smb://files/07 Backoffice/RETORNOS RPA - QIGGER/db.Portabilidade/portabilidade.db")
        if replicar_para_rede(db_path):
            print(f"    ✅ Banco replicado para: {BACKUP_REDE_PATH}")
        else:
            print("    ⚠️ Não foi possível replicar para rede (volume pode não estar montado)")
            print(f"    💡 Monte o SMB: Finder > Cmd+K > {SMB_URL_BACKOFFICE_LOG}")
    except ImportError as e:
        logger.warning(f"Módulo de backup não encontrado: {e}")
        print("    ⚠️ Módulo de backup não disponível")
    except Exception as e:
        logger.warning(f"Erro ao replicar para rede: {e}")
        print(f"    ⚠️ Erro na replicação: {e}")
    
    # Finalizar execução v2
    if db_manager_v2 and exec_id_v2:
        try:
            db_manager_v2.finalizar_execucao(
                exec_id_v2,
                status='concluido',
                registros_processados=registros_v2_processados,
                registros_erro=registros_v2_erros,
            )
            logger.info(f"✓ v2: Execução #{exec_id_v2} finalizada — processados={registros_v2_processados}, erros={registros_v2_erros}")
        except Exception as e:
            logger.warning(f"⚠ v2: Erro ao finalizar execução: {e}")
    
    print("\n" + "=" * 70)
    print("PROCESSAMENTO CONCLUÍDO!")
    print("=" * 70)
    print(f"\nBanco de dados: {db_path}")
    if not args.apenas_bases and stats_geral.get('homologacao', {}).get('arquivos_copiados'):
        print(f"Arquivos de homologação: {PASTA_SAIDA}\n")
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
