"""
Script para gerar todos os arquivos de homologação
Executa todos os scripts de homologação e salva com data/hora no nome
"""
import sys
from pathlib import Path
from datetime import datetime
import subprocess
import shutil

# Configurar encoding UTF-8
from src.utils.console_utils import setup_windows_console
setup_windows_console()

import logging

# Configurar logging
Path('logs').mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/gerar_todos_homologacao.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# Pasta de saída
PASTA_SAIDA = Path("/Applications/Documentos/Projetos_python/Retornos do gerenciador")

# Scripts de homologação a executar
SCRIPTS_HOMOLOGACAO = [
    {
        'nome': 'WPP',
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


def criar_pasta_saida():
    """Cria a pasta de saída se não existir"""
    try:
        PASTA_SAIDA.mkdir(parents=True, exist_ok=True)
        logger.info(f"Pasta de saída criada/verificada: {PASTA_SAIDA}")
        return True
    except Exception as e:
        logger.error(f"Erro ao criar pasta de saída: {e}")
        return False


def executar_script(script_path: Path) -> bool:
    """
    Executa um script Python e retorna True se executou com sucesso
    
    Args:
        script_path: Caminho do script a executar
        
    Returns:
        True se executou com sucesso, False caso contrário
    """
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
            errors='replace'
        )
        
        if result.returncode == 0:
            logger.info(f"✓ {script_path.name} executado com sucesso")
            if result.stdout:
                logger.debug(f"Saída: {result.stdout[:500]}")  # Primeiros 500 caracteres
            return True
        else:
            logger.error(f"✗ {script_path.name} falhou com código {result.returncode}")
            if result.stderr:
                logger.error(f"Erro: {result.stderr[:500]}")
            return False
            
    except Exception as e:
        logger.error(f"Erro ao executar {script_path.name}: {e}")
        return False


def copiar_arquivo_com_timestamp(arquivo_origem: Path, prefixo_nome: str, pasta_destino: Path) -> Path:
    """
    Copia arquivo para pasta de destino com data/hora no nome
    
    Args:
        arquivo_origem: Caminho do arquivo original
        prefixo_nome: Prefixo do nome do arquivo (sem extensão)
        pasta_destino: Pasta de destino
        
    Returns:
        Caminho do arquivo copiado ou None se falhou
    """
    if not arquivo_origem.exists():
        logger.warning(f"Arquivo não encontrado: {arquivo_origem}")
        return None
    
    try:
        # Gerar timestamp no formato YYYYMMDD_HHMMSS
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Obter extensão do arquivo original
        extensao = arquivo_origem.suffix
        
        # Nome do arquivo com timestamp
        nome_arquivo = f"{prefixo_nome}_{timestamp}{extensao}"
        arquivo_destino = pasta_destino / nome_arquivo
        
        # Copiar arquivo
        shutil.copy2(arquivo_origem, arquivo_destino)
        
        logger.info(f"  ✓ Arquivo copiado: {nome_arquivo}")
        return arquivo_destino
        
    except Exception as e:
        logger.error(f"Erro ao copiar arquivo {arquivo_origem.name}: {e}")
        return None


def main():
    """Função principal"""
    print("=" * 70)
    print("GERAÇÃO DE TODOS OS ARQUIVOS DE HOMOLOGAÇÃO")
    print("=" * 70)
    print(f"Data/Hora: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print(f"Pasta de saída: {PASTA_SAIDA}")
    print()
    
    # Criar pasta de saída
    if not criar_pasta_saida():
        print("\n✗ ERRO: Não foi possível criar a pasta de saída!")
        return
    
    # Estatísticas
    resultados = {
        'sucesso': [],
        'falha': [],
        'arquivos_copiados': []
    }
    
    # Executar cada script
    print("[1] Executando scripts de homologação...")
    print()
    
    for item in SCRIPTS_HOMOLOGACAO:
        print(f"  [{item['nome']}]")
        script_path = Path(item['script'])
        
        # Executar script
        sucesso = executar_script(script_path)
        
        if sucesso:
            resultados['sucesso'].append(item['nome'])
            
            # Copiar arquivo gerado com timestamp
            arquivo_origem = Path(item['arquivo_origem'])
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
        
        print()
    
    # Resumo final
    print("=" * 70)
    print("RESUMO DA EXECUÇÃO")
    print("=" * 70)
    print(f"  Scripts executados com sucesso: {len(resultados['sucesso'])}/{len(SCRIPTS_HOMOLOGACAO)}")
    print(f"  Arquivos copiados: {len(resultados['arquivos_copiados'])}")
    print()
    
    if resultados['sucesso']:
        print("  ✓ Scripts executados com sucesso:")
        for nome in resultados['sucesso']:
            print(f"    - {nome}")
        print()
    
    if resultados['falha']:
        print("  ✗ Scripts que falharam:")
        for nome in resultados['falha']:
            print(f"    - {nome}")
        print()
    
    if resultados['arquivos_copiados']:
        print("  Arquivos gerados na pasta de saída:")
        for arquivo in resultados['arquivos_copiados']:
            print(f"    - {arquivo.name}")
        print()
    
    print(f"  Pasta de saída: {PASTA_SAIDA}")
    print()
    print("=" * 70)
    
    if len(resultados['sucesso']) == len(SCRIPTS_HOMOLOGACAO):
        print("TODOS OS ARQUIVOS GERADOS COM SUCESSO!")
    else:
        print("ATENÇÃO: Alguns arquivos não foram gerados. Verifique os logs.")
    print("=" * 70)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nProcessamento interrompido pelo usuário.")
        sys.exit(1)
    except Exception as e:
        print(f"\nERRO FATAL: {e}")
        logger.error(f"Erro fatal: {e}", exc_info=True)
        import traceback
        traceback.print_exc()
        sys.exit(1)
