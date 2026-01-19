"""
Script para processar arquivo Excel COVERTE BASE PROP
e inserir na tabela base_coverte_prop do banco portabilidade.db

Esta tabela é separada da base_unificada (que é para relatório de objetos e gerenciador).
O Excel é a fonte principal e atualiza automaticamente quando o arquivo na rede é modificado.
"""
import sys
import logging
from pathlib import Path
from typing import Dict, Any, Optional
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
        logging.FileHandler('logs/processar_excel_unificado.log', encoding='utf-8'),
        console_handler
    ]
)

logger = logging.getLogger(__name__)

from src.database import DatabaseManager

# Caminhos de configuração (usar config centralizado)
try:
    from config import (
        DB_PATH, PASTA_IMPORTACOES, PASTA_ENTRADA,
        PASTA_BASE_COVERTE_NETWORK, PASTA_BASE_COVERTE_LOCAL, ARQUIVO_BASE_COVERTE_NETWORK
    )
    PASTA_IMPORTACOES_PATH = Path(PASTA_IMPORTACOES)
    PASTA_ENTRADA_PATH = Path(PASTA_ENTRADA)
    CAMINHO_BASE_NETWORK = Path(PASTA_BASE_COVERTE_NETWORK)
    ARQUIVO_BASE_COVERTE = Path(ARQUIVO_BASE_COVERTE_NETWORK) if ARQUIVO_BASE_COVERTE_NETWORK else None
    CAMINHO_BASE_LOCAL = Path(PASTA_BASE_COVERTE_LOCAL)
except ImportError:
    # Fallback se config.py não existir
    PASTA_IMPORTACOES_PATH = Path("/Applications/Documentos/IMPORTACOES_QIGGER")
    PASTA_ENTRADA_PATH = Path(__file__).parent / "data" / "entrada"
    CAMINHO_BASE_NETWORK = Path("/Volumes/02 Planejamento/02 - Relatórios/08 - Relatorios Cliente")
    ARQUIVO_BASE_COVERTE = Path("/Volumes/02 Planejamento/02 - Relatórios/08 - Relatorios Cliente/COVERTE BASE PROP.xlsx")
    CAMINHO_BASE_LOCAL = PASTA_ENTRADA_PATH / "excel"
    
    DB_PATH_RELATIVO = Path(__file__).parent / "data" / "portabilidade.db"
    DB_PATH = str(DB_PATH_RELATIVO)

# Configuração do compartilhamento SMB
SMB_SERVER = "files"
SMB_SHARE = "02 Planejamento"
SMB_PATH = "02 - Relatórios/08 - Relatorios Cliente"
SMB_FILE = "COVERTE BASE PROP.xlsx"
SMB_MOUNT_POINT = f"/Volumes/{SMB_SHARE}"


def verificar_conexao_smb() -> bool:
    """
    Verifica se o compartilhamento SMB está montado.
    
    Returns:
        True se o compartilhamento está acessível
    """
    mount_point = Path(SMB_MOUNT_POINT)
    if mount_point.exists() and mount_point.is_dir():
        # Verificar se realmente é um mount point ativo
        try:
            list(mount_point.iterdir())
            return True
        except (PermissionError, OSError):
            return False
    return False


def montar_compartilhamento_smb(usuario: str = None, senha: str = None) -> bool:
    """
    Monta o compartilhamento SMB no macOS.
    
    Args:
        usuario: Nome de usuário para autenticação (opcional, usa credenciais do Keychain)
        senha: Senha para autenticação (opcional)
        
    Returns:
        True se montou com sucesso ou já estava montado
    """
    import subprocess
    import platform
    
    if platform.system() != 'Darwin':
        logger.warning("Montagem SMB automática disponível apenas no macOS")
        return False
    
    # Verificar se já está montado
    if verificar_conexao_smb():
        logger.info(f"✓ Compartilhamento SMB já está montado: {SMB_MOUNT_POINT}")
        return True
    
    logger.info(f"Tentando montar compartilhamento SMB: smb://{SMB_SERVER}/{SMB_SHARE}")
    
    try:
        # Criar ponto de montagem se não existir
        mount_point = Path(SMB_MOUNT_POINT)
        if not mount_point.exists():
            mount_point.mkdir(parents=True, exist_ok=True)
        
        # Construir URL SMB
        if usuario and senha:
            # Com credenciais explícitas
            smb_url = f"smb://{usuario}:{senha}@{SMB_SERVER}/{SMB_SHARE}"
        elif usuario:
            # Apenas usuário (senha será solicitada ou do Keychain)
            smb_url = f"smb://{usuario}@{SMB_SERVER}/{SMB_SHARE}"
        else:
            # Usar credenciais do Keychain (mais seguro)
            smb_url = f"smb://{SMB_SERVER}/{SMB_SHARE}"
        
        # Método 1: Tentar com mount_smbfs (silencioso, usa Keychain)
        result = subprocess.run(
            ['mount', '-t', 'smbfs', smb_url, str(mount_point)],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode == 0:
            logger.info(f"✓ Compartilhamento SMB montado com sucesso: {SMB_MOUNT_POINT}")
            return True
        
        # Método 2: Tentar com open (abre o Finder para montar interativamente)
        logger.info("Tentando método alternativo via Finder...")
        result = subprocess.run(
            ['open', f'smb://{SMB_SERVER}/{SMB_SHARE}'],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        # Aguardar um pouco para o Finder montar
        import time
        for i in range(10):
            time.sleep(1)
            if verificar_conexao_smb():
                logger.info(f"✓ Compartilhamento SMB montado via Finder: {SMB_MOUNT_POINT}")
                return True
        
        logger.warning(f"⚠️ Não foi possível montar automaticamente. Monte manualmente:")
        logger.warning(f"   1. Finder > Cmd+K (Conectar ao Servidor)")
        logger.warning(f"   2. Digite: smb://{SMB_SERVER}/{SMB_SHARE}")
        logger.warning(f"   3. Autentique com suas credenciais")
        return False
        
    except subprocess.TimeoutExpired:
        logger.warning("Timeout ao tentar montar compartilhamento SMB")
        return False
    except Exception as e:
        logger.error(f"Erro ao montar compartilhamento SMB: {e}")
        return False


def obter_arquivo_coverte_smb() -> Optional[Path]:
    """
    Obtém o caminho do arquivo COVERTE BASE PROP.xlsx no compartilhamento SMB.
    Tenta montar automaticamente se não estiver montado.
    
    Returns:
        Caminho do arquivo (resolvido se era alias) ou None se não encontrado/inválido
    """
    # Verificar se já existe localmente na pasta de entrada
    arquivo_local = PASTA_ENTRADA_PATH / "COVERTE BASE PROP.xlsx"
    if arquivo_local.exists():
        # Resolver alias se for o caso
        arquivo_resolvido = resolver_alias_macos(arquivo_local)
        
        if arquivo_resolvido is None:
            logger.warning(f"⚠️ Arquivo local é alias quebrado: {arquivo_local}")
        elif validar_arquivo_excel(arquivo_resolvido, resolver_alias=False):
            logger.info(f"✓ Usando arquivo local: {arquivo_resolvido}")
            return arquivo_resolvido
        else:
            logger.warning(f"⚠️ Arquivo local inválido ou corrompido: {arquivo_local}")
    
    # Tentar montar o compartilhamento SMB
    if not verificar_conexao_smb():
        logger.info("Compartilhamento SMB não montado. Tentando montar...")
        if not montar_compartilhamento_smb():
            return None
    
    # Construir caminho completo do arquivo
    arquivo_rede = Path(SMB_MOUNT_POINT) / SMB_PATH / SMB_FILE
    
    if arquivo_rede.exists():
        # Resolver alias se necessário
        arquivo_resolvido = resolver_alias_macos(arquivo_rede)
        
        if arquivo_resolvido is None:
            logger.warning(f"⚠️ Arquivo da rede é alias quebrado: {arquivo_rede}")
            return None
        
        if validar_arquivo_excel(arquivo_resolvido, resolver_alias=False):
            logger.info(f"✓ Arquivo encontrado na rede: {arquivo_resolvido}")
            return arquivo_resolvido
        else:
            logger.warning(f"⚠️ Arquivo na rede não é válido: {arquivo_resolvido}")
    else:
        logger.warning(f"⚠️ Arquivo não encontrado na rede: {arquivo_rede}")
        
        # Listar arquivos disponíveis para debug
        pasta_rede = Path(SMB_MOUNT_POINT) / SMB_PATH
        if pasta_rede.exists():
            logger.info(f"Arquivos disponíveis em {pasta_rede}:")
            try:
                for f in pasta_rede.glob("*.xlsx"):
                    logger.info(f"  - {f.name}")
            except PermissionError:
                logger.warning("  (sem permissão para listar)")
    
    return None


def copiar_arquivo_para_local(arquivo_origem: Path) -> Optional[Path]:
    """
    Copia arquivo da rede para pasta local (para processamento mais rápido).
    
    Args:
        arquivo_origem: Caminho do arquivo na rede
        
    Returns:
        Caminho do arquivo local ou None se falhou
    """
    import shutil
    
    if not arquivo_origem or not arquivo_origem.exists():
        return None
    
    try:
        # Criar pasta de entrada se não existir
        PASTA_ENTRADA_PATH.mkdir(parents=True, exist_ok=True)
        
        arquivo_destino = PASTA_ENTRADA_PATH / arquivo_origem.name
        
        # Copiar arquivo
        logger.info(f"Copiando arquivo da rede para local...")
        shutil.copy2(arquivo_origem, arquivo_destino)
        
        if arquivo_destino.exists() and validar_arquivo_excel(arquivo_destino):
            logger.info(f"✓ Arquivo copiado: {arquivo_destino}")
            return arquivo_destino
        else:
            logger.warning(f"⚠️ Falha ao copiar arquivo")
            return None
            
    except PermissionError as e:
        logger.error(f"Sem permissão para copiar arquivo: {e}")
        return None
    except Exception as e:
        logger.error(f"Erro ao copiar arquivo: {e}")
        return None


def resolver_alias_macos(caminho: Path) -> Optional[Path]:
    """
    Resolve aliases do macOS para o caminho real do arquivo.
    No macOS, arquivos podem ser aliases (atalhos) que apontam para outros locais.
    
    Args:
        caminho: Caminho potencialmente um alias
        
    Returns:
        Caminho real do arquivo, ou None se o alias estiver quebrado
    """
    import subprocess
    import platform
    
    if platform.system() != 'Darwin':
        return caminho
    
    if not caminho.exists():
        return caminho
    
    # Verificar se é um alias usando o comando 'file'
    try:
        result = subprocess.run(
            ['file', str(caminho)],
            capture_output=True,
            text=True,
            timeout=5
        )
        
        is_alias = 'MacOS Alias' in result.stdout or 'alias' in result.stdout.lower()
        
        if not is_alias:
            # Não é alias, retornar caminho original
            return caminho
            
        logger.info(f"🔗 Detectado alias macOS: {caminho.name}")
        
    except Exception:
        pass  # Continuar tentando resolver
    
    try:
        # Usar osascript para resolver alias do macOS
        script = f'''
        tell application "Finder"
            set theItem to (POSIX file "{caminho}" as alias)
            set theRealPath to POSIX path of (original item of theItem as alias)
            return theRealPath
        end tell
        '''
        result = subprocess.run(
            ['osascript', '-e', script],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        if result.returncode == 0 and result.stdout.strip():
            caminho_real = Path(result.stdout.strip())
            if caminho_real.exists():
                if caminho_real != caminho:
                    logger.info(f"✓ Alias resolvido: {caminho.name} -> {caminho_real}")
                return caminho_real
            else:
                logger.warning(f"⚠️ Alias aponta para arquivo inexistente: {caminho_real}")
                return None
        else:
            # osascript falhou - provavelmente alias quebrado
            logger.warning(f"⚠️ Não foi possível resolver alias: {caminho.name}")
            if result.stderr:
                logger.debug(f"Erro osascript: {result.stderr.strip()}")
            return None
                
    except subprocess.TimeoutExpired:
        logger.warning(f"Timeout ao resolver alias: {caminho}")
        return None
    except Exception as e:
        logger.debug(f"Erro ao resolver alias: {e}")
        return None
    

def validar_arquivo_excel(caminho: Path, resolver_alias: bool = True) -> bool:
    """
    Valida se o arquivo é um Excel válido (não é alias corrompido ou arquivo vazio).
    
    Args:
        caminho: Caminho do arquivo
        resolver_alias: Se True, tenta resolver alias do macOS antes de validar
        
    Returns:
        True se arquivo parece válido
    """
    if not caminho.exists():
        return False
    
    # Resolver alias do macOS se necessário
    if resolver_alias:
        caminho_resolvido = resolver_alias_macos(caminho)
        if caminho_resolvido is None:
            logger.warning(f"Alias macOS quebrado ou destino inexistente: {caminho}")
            return False
        if caminho_resolvido != caminho:
            # É um alias, validar o arquivo real
            caminho = caminho_resolvido
    
    # Verificar tamanho mínimo (Excel vazio tem pelo menos alguns KB)
    tamanho = caminho.stat().st_size
    if tamanho < 1024:  # Menos de 1KB
        logger.warning(f"Arquivo muito pequeno ({tamanho} bytes), pode ser alias ou corrompido: {caminho}")
        return False
    
    # Verificar se começa com assinatura de ZIP (Excel .xlsx é um ZIP)
    try:
        with open(caminho, 'rb') as f:
            header = f.read(4)
            # Assinatura ZIP: PK\x03\x04
            if header[:2] != b'PK':
                logger.warning(f"Arquivo não tem assinatura ZIP válida: {caminho}")
                return False
    except Exception as e:
        logger.warning(f"Erro ao verificar assinatura do arquivo: {e}")
        return False
    
    return True


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
    return cpf_str if len(cpf_str) == 11 else None


def limpar_telefone(tel: Any) -> Optional[str]:
    """Limpa telefone removendo caracteres não numéricos"""
    if pd.isna(tel) or tel is None:
        return None
    tel_str = ''.join(c for c in str(tel) if c.isdigit())
    return tel_str if len(tel_str) >= 10 else None


def mapear_campos_excel(row: pd.Series) -> Dict[str, Any]:
    """
    Mapeia TODAS as colunas do Excel COVERTE BASE PROP para a tabela base_coverte_prop
    Garante mapeamento correto mesmo com colunas duplicadas (DDD, Telefone)
    """
    def safe_get(key, default=None, clean_func=None, index=0):
        """Busca valor do Excel com tratamento de erros - busca exata primeiro"""
        try:
            # Buscar todas as colunas que contêm a chave
            matching_cols = [col for col in row.index if str(key).lower() in str(col).lower()]
            
            if matching_cols and len(matching_cols) > index:
                valor = row[matching_cols[index]]
                if clean_func:
                    return clean_func(valor)
                return limpar_valor(valor)
            
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
        except (KeyError, IndexError, AttributeError, TypeError):
            pass
        return default
    
    # Detectar colunas DDD e Telefone (podem aparecer múltiplas vezes)
    # Pandas renomeia colunas duplicadas como DDD, DDD.1, DDD.2, etc.
    ddd_cols = [col for col in row.index if str(col).strip().upper().startswith('DDD') and '.' not in str(col).upper() or str(col).strip().upper() == 'DDD' or str(col).strip().upper().startswith('DDD.')]
    # Ordenar para garantir ordem: DDD, DDD.1, DDD.2, etc.
    ddd_cols = sorted([col for col in row.index if 'DDD' in str(col).upper()], key=lambda x: (str(x).upper().replace('DDD', '').replace('.', '0') or '0'))
    
    # Telefone (excluir Telefone Portabilidade e Numero linha)
    telefone_cols = [col for col in row.index 
                     if 'Telefone' in str(col) 
                     and 'Portabilidade' not in str(col) 
                     and 'Numero linha' not in str(col)
                     and str(col).strip() != 'Telefone Portabilidade']
    # Ordenar para garantir ordem: Telefone, Telefone.1, Telefone.2, etc.
    telefone_cols = sorted(telefone_cols, key=lambda x: (str(x).upper().replace('TELEFONE', '').replace('.', '0') or '0'))
    
    # Função para combinar DDD + Telefone no formato "31988776655"
    def combinar_ddd_telefone(ddd, telefone):
        """Combina DDD e Telefone em um único número no formato 31988776655"""
        # Tratar valores None, nan, '-', vazios
        if pd.isna(telefone) or telefone is None or str(telefone).strip() in ['', '-', 'nan', 'None', 'NULL', 'null']:
            return None
        
        # Limpar telefone (remover caracteres não numéricos)
        tel_limpo = ''.join(c for c in str(telefone) if c.isdigit())
        
        if not tel_limpo:
            return None
        
        # Se já tem DDD no início (11 dígitos), retornar direto
        if len(tel_limpo) >= 11:
            return tel_limpo[:11]  # Garantir máximo 11 dígitos
        
        # Se tem DDD separado, combinar
        if ddd and not pd.isna(ddd) and str(ddd).strip() not in ['', '-', 'nan', 'None', 'NULL', 'null']:
            # Tratar float (ex: 65.0 -> 65)
            if isinstance(ddd, float):
                ddd_str = str(int(ddd)) if ddd == int(ddd) else str(ddd)
            else:
                ddd_str = str(ddd)
            ddd_limpo = ''.join(c for c in ddd_str if c.isdigit())
            if ddd_limpo and tel_limpo:
                # Combinar DDD + Telefone
                telefone_completo = ddd_limpo + tel_limpo
                # Garantir máximo 11 dígitos (DDD 2 dígitos + 9 dígitos do telefone)
                if len(telefone_completo) >= 11:
                    return telefone_completo[:11]
                elif len(telefone_completo) >= 10:
                    return telefone_completo
                else:
                    return telefone_completo  # Retornar mesmo se incompleto
        
        # Se não tem DDD mas tem telefone, retornar o telefone limpo
        return tel_limpo if tel_limpo else None
    
    # Capturar DDD e Telefone (primeiro e segundo par) e combinar
    # Usar try/except para evitar erros se as colunas não existirem
    try:
        ddd_1 = row[ddd_cols[0]] if len(ddd_cols) > 0 and ddd_cols[0] in row.index else None
    except (IndexError, KeyError):
        ddd_1 = None
    
    try:
        telefone_raw_1 = row[telefone_cols[0]] if len(telefone_cols) > 0 and telefone_cols[0] in row.index else None
    except (IndexError, KeyError):
        telefone_raw_1 = None
    
    telefone_1 = combinar_ddd_telefone(ddd_1, telefone_raw_1)
    
    try:
        ddd_2 = row[ddd_cols[1]] if len(ddd_cols) > 1 and ddd_cols[1] in row.index else None
    except (IndexError, KeyError):
        ddd_2 = None
    
    try:
        telefone_raw_2 = row[telefone_cols[1]] if len(telefone_cols) > 1 and telefone_cols[1] in row.index else None
    except (IndexError, KeyError):
        telefone_raw_2 = None
    
    telefone_2 = combinar_ddd_telefone(ddd_2, telefone_raw_2)
    
    # Identificadores principais
    cpf = safe_get('CPF', clean_func=limpar_cpf) or safe_get('Documento', clean_func=limpar_cpf)
    codigo_externo = safe_get('Login Externo') or safe_get('Id Auxiliar1')
    proposta_isize = safe_get('Proposta iSize')
    numero_ordem = safe_get('Numero OS') or safe_get('ID ERP')
    numero_acesso = safe_get('Telefone Portabilidade', clean_func=limpar_telefone) or safe_get('Numero linha', clean_func=limpar_telefone)
    
    # Mapear TODAS as colunas do Excel COVERTE BASE PROP
    dados = {
        # Identificadores principais
        'cpf': cpf,
        'codigo_externo': codigo_externo,
        'proposta_isize': proposta_isize,
        'numero_ordem': numero_ordem,
        'numero_acesso': numero_acesso,
        
        # Dados básicos
        'data_venda': safe_get('Data venda'),
        'cliente_nome': safe_get('Cliente'),
        'troca_titularidade': safe_get('Troca Titularidade'),
        'nascimento': safe_get('Nascimento'),
        'mae': safe_get('Mae'),
        
        # Endereço completo
        'endereco': safe_get('Endereco'),
        'numero': safe_get('Numero'),
        'complemento': safe_get('Complemento'),
        'bairro': safe_get('Bairro'),
        'cidade': safe_get('Cidade'),
        'uf': safe_get('UF'),
        'cep': safe_get('Cep'),
        'ponto_referencia': safe_get('Ponto Referencia'),
        
        # Telefones (DDD + Telefone combinados no formato "31988776655")
        'telefone_1': telefone_1,
        'telefone_2': telefone_2,
        'telefone_portado': safe_get('Telefone Portabilidade', clean_func=limpar_telefone),
        'numero_linha': safe_get('Numero linha'),
        
        # Vendedor e Supervisor
        'nome_vendedor': safe_get('Nome vendedor'),
        'login_externo': safe_get('Login Externo'),
        'nome_supervisor': safe_get('Nome Supervisor'),
        
        # Produto e Plano
        'produto_vendido': safe_get('Produto'),
        'forma_pagamento': safe_get('Forma Pagamento'),
        'plano': safe_get('Plano'),
        'vencimento': safe_get('Vencimento'),
        
        # Status Venda
        'status_venda': safe_get('Status venda'),
        'motivo_rejeicao_cancelamento': safe_get('Motivo Rejeicao Cancelamento'),
        'flag': safe_get('Flag'),
        'auditoria': safe_get('Auditoria'),
        'qualidade': safe_get('Qualidade'),
        'conectada': safe_get('Conectada'),
        'data_conectada': safe_get('Data Conectada'),
        'plataforma': safe_get('Plataforma'),
        
        # RPA
        'status_rpa': safe_get('Status RPA'),
        'data_process_rpa': safe_get('Data Process RPA'),
        'data_gross': safe_get('Data Gross'),
        
        # Vivo/Bluechip
        'id_play_vivo': safe_get('ID PLAY Vivo'),
        'matricula_discador': safe_get('Matricula Discador'),
        'conta_online': safe_get('Conta Online'),
        'email': safe_get('Email'),
        'vivo_pay': safe_get('Vivo Pay'),
        'bluechip_status': safe_get('Bluechip Status'),
        'bluechip_data_status': safe_get('Bluechip Data Status'),
        'vivo_internet': safe_get('Vivo Internet'),
        'vivo_tv': safe_get('Vivo TV'),
        'resposta_envio_pedido': safe_get('Resposta Envio Pedido'),
        'pedido_bluechip': safe_get('Pedido Bluechip'),
        'bluechip_data_enviado': safe_get('Bluechip Data enviado'),
        'cd_bluechip': safe_get('CD Bluechip'),
        'nome_equipe': safe_get('Nome Equipe'),
        
        # Logística - Correios
        'rastreio_correios': safe_get('Rastreio Correios'),
        'data_status_correios': safe_get('Data Status Correios'),
        'status_correios': safe_get('Status Correios'),
        
        # Logística - Loggi
        'rastreio_loggi': safe_get('Rastreio Loggi'),
        'data_status_loggi': safe_get('Data Status Loggi'),
        'status_loggi': safe_get('Status Loggi'),
        
        # Entrega
        'data_maxima_prevista_entrega': safe_get('Data Maxima Prevista Entrega'),
        'status_entrega_prevista': safe_get('Status Entrega Prevista'),
        
        # Portabilidade
        'portabilidade': safe_get('Portabilidade'),
        'complemento_portabilidade': safe_get('Complemento Portabilidade'),
        'portabilidade_antecipada': safe_get('Portabilidade Antecipada'),
        'data_marcacao_port_antecipada': safe_get('Data marcacao Port. Antecipada'),
        'quem_marcou_port_antecipada': safe_get('Quem marcou Port. Antecipada'),
        
        # Adicionais
        'app_adicional': safe_get('App Adicional'),
        'observacoes': safe_get('Observacoes'),
        'sms_previo': safe_get('SMS Previo'),
        'avulsa': safe_get('Avulsa'),
        'remessa_bluechip': safe_get('Remessa Bluechip'),
        'qtd_remessas': safe_get('Qtd Remessas'),
        'score': safe_get('Score'),
        'robo_inicio_proc': safe_get('Robo Inicio Proc.'),
        'robo_fim_proc': safe_get('Robo Fim Proc.'),
        'crivo_vendas': safe_get('CRIVO VENDAS'),
    }
    
    # Garantir que temos pelo menos um identificador
    if not dados['cpf'] and not dados['codigo_externo'] and not dados['numero_ordem'] and not dados['proposta_isize']:
        return None
    
    # Fallbacks para campos obrigatórios
    if not dados['numero_ordem']:
        dados['numero_ordem'] = (safe_get('Numero OS') or 
                                 safe_get('ID ERP') or 
                                 safe_get('Nu Pedido') or 
                                 dados['codigo_externo'] or 
                                 dados['proposta_isize'] or 'SEM_ORDEM')
    
    if not dados['numero_acesso']:
        dados['numero_acesso'] = (dados.get('telefone_portado') or 
                                  dados.get('telefone_1') or 
                                  dados.get('numero_linha') or 
                                  '00000000000')
    
    # Remover valores None/vazios
    return {k: v for k, v in dados.items() if v is not None}


def criar_tabela_base_coverte_prop(db_manager: DatabaseManager):
    """Cria tabela para armazenar dados do Excel COVERTE BASE PROP com TODAS as colunas"""
    with db_manager._get_connection() as conn:
        cursor = conn.cursor()
        
        # Criar tabela base_coverte_prop se não existir com TODAS as colunas do Excel
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS base_coverte_prop (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                
                -- Identificadores principais
                cpf TEXT,
                codigo_externo TEXT,
                proposta_isize TEXT,
                numero_ordem TEXT NOT NULL,
                numero_acesso TEXT,
                
                -- Dados básicos
                data_venda TEXT,
                cliente_nome TEXT,
                troca_titularidade TEXT,
                nascimento TEXT,
                mae TEXT,
                
                -- Endereço completo
                endereco TEXT,
                numero TEXT,
                complemento TEXT,
                bairro TEXT,
                cidade TEXT,
                uf TEXT,
                cep TEXT,
                ponto_referencia TEXT,
                
                -- Telefones (DDD + Telefone combinados no formato "31988776655")
                telefone_1 TEXT,
                telefone_2 TEXT,
                telefone_portado TEXT,
                numero_linha TEXT,
                
                -- Vendedor e Supervisor
                nome_vendedor TEXT,
                login_externo TEXT,
                nome_supervisor TEXT,
                
                -- Produto e Plano
                produto_vendido TEXT,
                forma_pagamento TEXT,
                plano TEXT,
                vencimento TEXT,
                
                -- Status Venda
                status_venda TEXT,
                motivo_rejeicao_cancelamento TEXT,
                flag TEXT,
                auditoria TEXT,
                qualidade TEXT,
                conectada TEXT,
                data_conectada TEXT,
                plataforma TEXT,
                
                -- RPA
                status_rpa TEXT,
                data_process_rpa TEXT,
                data_gross TEXT,
                
                -- Vivo/Bluechip
                id_play_vivo TEXT,
                matricula_discador TEXT,
                conta_online TEXT,
                email TEXT,
                vivo_pay TEXT,
                bluechip_status TEXT,
                bluechip_data_status TEXT,
                vivo_internet TEXT,
                vivo_tv TEXT,
                resposta_envio_pedido TEXT,
                pedido_bluechip TEXT,
                bluechip_data_enviado TEXT,
                cd_bluechip TEXT,
                nome_equipe TEXT,
                
                -- Logística - Correios
                rastreio_correios TEXT,
                data_status_correios TEXT,
                status_correios TEXT,
                
                -- Logística - Loggi
                rastreio_loggi TEXT,
                data_status_loggi TEXT,
                status_loggi TEXT,
                
                -- Entrega
                data_maxima_prevista_entrega TEXT,
                status_entrega_prevista TEXT,
                
                -- Portabilidade
                portabilidade TEXT,
                complemento_portabilidade TEXT,
                portabilidade_antecipada TEXT,
                data_marcacao_port_antecipada TEXT,
                quem_marcou_port_antecipada TEXT,
                
                -- Adicionais
                app_adicional TEXT,
                observacoes TEXT,
                sms_previo TEXT,
                avulsa TEXT,
                remessa_bluechip TEXT,
                qtd_remessas TEXT,
                score TEXT,
                robo_inicio_proc TEXT,
                robo_fim_proc TEXT,
                crivo_vendas TEXT,
                
                -- Metadados
                origem_arquivo TEXT,
                data_importacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                UNIQUE(cpf, numero_ordem, codigo_externo)
            )
        """)
        
        # Criar índices
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_coverte_cpf ON base_coverte_prop(cpf)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_coverte_codigo_externo ON base_coverte_prop(codigo_externo)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_coverte_numero_ordem ON base_coverte_prop(numero_ordem)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_coverte_proposta_isize ON base_coverte_prop(proposta_isize)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_coverte_data_importacao ON base_coverte_prop(data_importacao)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_coverte_updated_at ON base_coverte_prop(updated_at)")
        
        conn.commit()
        # Migrar tabela se necessário (remover colunas ddd_telefone_1 e ddd_telefone_2)
        cursor.execute("PRAGMA table_info(base_coverte_prop)")
        colunas_existentes = [col[1] for col in cursor.fetchall()]
        
        if 'ddd_telefone_1' in colunas_existentes or 'ddd_telefone_2' in colunas_existentes:
            logger.info("Migrando tabela base_coverte_prop: removendo colunas ddd_telefone_1 e ddd_telefone_2...")
            
            # Função para combinar DDD + Telefone
            def combinar_ddd_tel(ddd, tel):
                if not tel:
                    return None
                tel_limpo = ''.join(c for c in str(tel) if c.isdigit())
                if len(tel_limpo) >= 11:
                    return tel_limpo[:11]
                if ddd:
                    ddd_limpo = ''.join(c for c in str(ddd) if c.isdigit())
                    if ddd_limpo and tel_limpo:
                        return (ddd_limpo + tel_limpo)[:11]
                return tel_limpo if tel_limpo else None
            
            # Ler todos os registros
            cursor.execute("SELECT * FROM base_coverte_prop")
            registros = cursor.fetchall()
            nomes_colunas = [desc[0] for desc in cursor.description]
            
            # Criar tabela temporária
            cursor.execute("DROP TABLE IF EXISTS base_coverte_prop_temp")
            cursor.execute("""
                CREATE TABLE base_coverte_prop_temp (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    cpf TEXT, codigo_externo TEXT, proposta_isize TEXT, numero_ordem TEXT NOT NULL, numero_acesso TEXT,
                    data_venda TEXT, cliente_nome TEXT, troca_titularidade TEXT, nascimento TEXT, mae TEXT,
                    endereco TEXT, numero TEXT, complemento TEXT, bairro TEXT, cidade TEXT, uf TEXT, cep TEXT, ponto_referencia TEXT,
                    telefone_1 TEXT, telefone_2 TEXT, telefone_portado TEXT, numero_linha TEXT,
                    nome_vendedor TEXT, login_externo TEXT, nome_supervisor TEXT,
                    produto_vendido TEXT, forma_pagamento TEXT, plano TEXT, vencimento TEXT,
                    status_venda TEXT, motivo_rejeicao_cancelamento TEXT, flag TEXT, auditoria TEXT, qualidade TEXT,
                    conectada TEXT, data_conectada TEXT, plataforma TEXT,
                    status_rpa TEXT, data_process_rpa TEXT, data_gross TEXT,
                    id_play_vivo TEXT, matricula_discador TEXT, conta_online TEXT, email TEXT, vivo_pay TEXT,
                    bluechip_status TEXT, bluechip_data_status TEXT, vivo_internet TEXT, vivo_tv TEXT,
                    resposta_envio_pedido TEXT, pedido_bluechip TEXT, bluechip_data_enviado TEXT, cd_bluechip TEXT, nome_equipe TEXT,
                    rastreio_correios TEXT, data_status_correios TEXT, status_correios TEXT,
                    rastreio_loggi TEXT, data_status_loggi TEXT, status_loggi TEXT,
                    data_maxima_prevista_entrega TEXT, status_entrega_prevista TEXT,
                    portabilidade TEXT, complemento_portabilidade TEXT, portabilidade_antecipada TEXT,
                    data_marcacao_port_antecipada TEXT, quem_marcou_port_antecipada TEXT,
                    app_adicional TEXT, observacoes TEXT, sms_previo TEXT, avulsa TEXT,
                    remessa_bluechip TEXT, qtd_remessas TEXT, score TEXT, robo_inicio_proc TEXT, robo_fim_proc TEXT, crivo_vendas TEXT,
                    origem_arquivo TEXT, data_importacao TIMESTAMP, updated_at TIMESTAMP,
                    UNIQUE(cpf, numero_ordem, codigo_externo)
                )
            """)
            
            # Migrar dados combinando DDD + Telefone
            idx_ddd_1 = nomes_colunas.index('ddd_telefone_1') if 'ddd_telefone_1' in nomes_colunas else None
            idx_tel_1 = nomes_colunas.index('telefone_1') if 'telefone_1' in nomes_colunas else None
            idx_ddd_2 = nomes_colunas.index('ddd_telefone_2') if 'ddd_telefone_2' in nomes_colunas else None
            idx_tel_2 = nomes_colunas.index('telefone_2') if 'telefone_2' in nomes_colunas else None
            
            for registro in registros:
                registro_dict = dict(zip(nomes_colunas, registro))
                
                # Combinar telefones
                if idx_ddd_1 is not None and idx_tel_1 is not None:
                    registro_dict['telefone_1'] = combinar_ddd_tel(registro[idx_ddd_1], registro[idx_tel_1])
                if idx_ddd_2 is not None and idx_tel_2 is not None:
                    registro_dict['telefone_2'] = combinar_ddd_tel(registro[idx_ddd_2], registro[idx_tel_2])
                
                # Remover colunas antigas do dict
                registro_dict.pop('ddd_telefone_1', None)
                registro_dict.pop('ddd_telefone_2', None)
                
                # Inserir na nova tabela
                campos = [k for k in registro_dict.keys() if k not in ['ddd_telefone_1', 'ddd_telefone_2']]
                valores = [registro_dict[k] for k in campos]
                placeholders = ','.join(['?' for _ in campos])
                cursor.execute(f"INSERT INTO base_coverte_prop_temp ({','.join(campos)}) VALUES ({placeholders})", valores)
            
            # Substituir tabela antiga pela nova
            cursor.execute("DROP TABLE base_coverte_prop")
            cursor.execute("ALTER TABLE base_coverte_prop_temp RENAME TO base_coverte_prop")
            
            # Recriar índices
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_coverte_cpf ON base_coverte_prop(cpf)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_coverte_codigo_externo ON base_coverte_prop(codigo_externo)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_coverte_numero_ordem ON base_coverte_prop(numero_ordem)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_coverte_proposta_isize ON base_coverte_prop(proposta_isize)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_coverte_data_importacao ON base_coverte_prop(data_importacao)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_coverte_updated_at ON base_coverte_prop(updated_at)")
            
            conn.commit()
            logger.info(f"Migração concluída: {len(registros)} registros migrados")
        
        logger.info("Tabela base_coverte_prop criada/verificada com sucesso")


def processar_excel_unificado(
    arquivo_excel: Path,
    db_path: str = DB_PATH
) -> Dict[str, int]:
    """
    Processa arquivo Excel unificado e insere no banco de dados
    
    Args:
        arquivo_excel: Caminho para o arquivo Excel
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
        'ignorados': 0
    }
    
    if not arquivo_excel.exists():
        logger.error(f"Arquivo não encontrado: {arquivo_excel}")
        return stats
    
    # Resolver alias do macOS se necessário
    arquivo_excel = resolver_alias_macos(arquivo_excel)
    
    # Validar arquivo antes de processar
    if not validar_arquivo_excel(arquivo_excel):
        logger.error(f"Arquivo Excel inválido ou corrompido: {arquivo_excel}")
        logger.error("Verifique se o arquivo não é um alias do macOS ou se está corrompido.")
        return stats
    
    logger.info(f"Processando arquivo Excel: {arquivo_excel.name}")
    
    # Inicializar banco de dados
    db_manager = DatabaseManager(db_path)
    criar_tabela_base_coverte_prop(db_manager)
    
    try:
        # Ler arquivo Excel
        logger.info("Lendo arquivo Excel...")
        df = pd.read_excel(arquivo_excel, engine='openpyxl', dtype=str)
        
        stats['total_linhas'] = len(df)
        logger.info(f"Total de linhas no Excel: {stats['total_linhas']}")
        
        # Detectar tipo de arquivo (normalizar nomes de colunas para comparação)
        colunas_normalizadas = [str(c).strip().upper() for c in df.columns]
        colunas_originais = list(df.columns)
        
        # Verificar colunas características de COVERTE BASE PROP
        colunas_coverte = ['CRIVO VENDAS', 'DATA VENDA', 'PROPOSTA ISIZE', 'STATUS VENDA', 'PORTABILIDADE']
        colunas_encontradas_coverte = [c for c in colunas_coverte if c in colunas_normalizadas]
        
        # Verificar colunas características de Relatorio_Objetos
        colunas_objetos = ['NU PEDIDO', 'ID DO OBJETO', 'DESTINATARIO']
        colunas_encontradas_objetos = [c for c in colunas_objetos if c in colunas_normalizadas]
        
        is_coverte_prop = len(colunas_encontradas_coverte) >= 2  # Pelo menos 2 colunas características
        is_relatorio_objetos = len(colunas_encontradas_objetos) >= 2 and not is_coverte_prop
        
        if is_coverte_prop:
            logger.info("✅ Arquivo detectado: COVERTE BASE PROP")
            logger.info(f"   Colunas identificadas: {', '.join(colunas_encontradas_coverte)}")
        elif is_relatorio_objetos:
            logger.warning("⚠️ Arquivo detectado: Relatorio_Objetos (estrutura diferente do COVERTE BASE PROP)")
            logger.warning("   Este script é para processar COVERTE BASE PROP!")
            logger.warning("   Use processar_atualizacoes_gerar_finais.py para Relatorio_Objetos")
        else:
            logger.warning("⚠️ Tipo de arquivo não identificado")
            logger.warning(f"   Colunas COVERTE encontradas: {colunas_encontradas_coverte}")
            logger.warning(f"   Colunas Objetos encontradas: {colunas_encontradas_objetos}")
        
        # Exibir colunas disponíveis para debug
        logger.info(f"Colunas encontradas no Excel ({len(df.columns)}):")
        for i, col in enumerate(df.columns[:20], 1):  # Mostrar primeiras 20
            logger.info(f"  {i}. {col}")
        if len(df.columns) > 20:
            logger.info(f"  ... e mais {len(df.columns) - 20} colunas")
        
        # Se não for COVERTE BASE PROP, PARAR o processamento
        if not is_coverte_prop:
            logger.error("=" * 70)
            logger.error("ERRO: Este arquivo NÃO é COVERTE BASE PROP!")
            logger.error(f"Arquivo processado: {arquivo_excel.name}")
            logger.error("")
            logger.error("Este script processa APENAS arquivos COVERTE BASE PROP.")
            logger.error("Para processar Relatorio_Objetos, use:")
            logger.error("  - processar_atualizacoes_gerar_finais.py (para CSV)")
            logger.error("  - ObjectsLoader (para Excel de objetos)")
            logger.error("")
            logger.error("O arquivo COVERTE BASE PROP deve estar em:")
            logger.error(f"  {CAMINHO_BASE_NETWORK}")
            logger.error("  ou")
            logger.error(f"  {CAMINHO_BASE_LOCAL}")
            logger.error("=" * 70)
            return stats
        
        # Processar cada linha
        with db_manager._get_connection() as conn:
            cursor = conn.cursor()
            
            for idx, row in df.iterrows():
                try:
                    # Mapear campos
                    dados = mapear_campos_excel(row)
                    
                    if not dados:
                        stats['ignorados'] += 1
                        continue
                    
                    # Verificar se registro já existe
                    cursor.execute("""
                        SELECT id FROM base_coverte_prop
                        WHERE (cpf = ? OR cpf IS NULL)
                        AND numero_ordem = ?
                        AND (codigo_externo = ? OR codigo_externo IS NULL)
                    """, (
                        dados.get('cpf'),
                        dados.get('numero_ordem'),
                        dados.get('codigo_externo')
                    ))
                    
                    existing = cursor.fetchone()
                    
                    if existing:
                        # Atualizar registro existente
                        campos_update = []
                        valores_update = []
                        
                        for campo, valor in dados.items():
                            if campo not in ['cpf', 'numero_ordem', 'codigo_externo']:
                                campos_update.append(f"{campo} = ?")
                                valores_update.append(valor)
                        
                        valores_update.append(existing[0])
                        
                        cursor.execute(f"""
                            UPDATE base_coverte_prop SET
                                {', '.join(campos_update)},
                                updated_at = CURRENT_TIMESTAMP
                            WHERE id = ?
                        """, valores_update)
                        
                        stats['atualizados'] += 1
                    else:
                        # Inserir novo registro
                        campos = list(dados.keys()) + ['origem_arquivo']
                        valores = list(dados.values()) + [arquivo_excel.name]
                        placeholders = ', '.join(['?'] * len(campos))
                        
                        cursor.execute(f"""
                            INSERT INTO base_coverte_prop ({', '.join(campos)})
                            VALUES ({placeholders})
                        """, valores)
                        
                        stats['inseridos'] += 1
                    
                    stats['processados'] += 1
                    
                    if (idx + 1) % 100 == 0:
                        conn.commit()
                        logger.info(f"  Progresso: {idx + 1}/{stats['total_linhas']} linhas processadas...")
                    
                except Exception as e:
                    logger.error(f"Erro ao processar linha {idx + 1}: {e}")
                    stats['erros'] += 1
                    continue
            
            conn.commit()
        
        logger.info("Processamento concluído!")
        logger.info(f"  Total de linhas: {stats['total_linhas']}")
        logger.info(f"  Processados: {stats['processados']}")
        logger.info(f"  Inseridos: {stats['inseridos']}")
        logger.info(f"  Atualizados: {stats['atualizados']}")
        logger.info(f"  Ignorados: {stats['ignorados']}")
        logger.info(f"  Erros: {stats['erros']}")
        
    except Exception as e:
        logger.error(f"Erro ao processar arquivo Excel: {e}", exc_info=True)
        stats['erros'] = stats['total_linhas']
    
    return stats


def encontrar_arquivo_excel(pasta: Path) -> Optional[Path]:
    """
    Encontra o arquivo Excel mais recente na pasta.
    Valida arquivos e resolve aliases do macOS.
    
    Returns:
        Caminho do arquivo Excel válido (resolvido se era alias) ou None
    """
    arquivos = list(pasta.glob("*.xlsx")) + list(pasta.glob("*.xls"))
    if not arquivos:
        return None
    
    # Filtrar arquivos válidos (resolver aliases e validar)
    arquivos_validos = []
    aliases_quebrados = []
    
    for arq in arquivos:
        # Resolver alias primeiro
        arq_resolvido = resolver_alias_macos(arq)
        
        if arq_resolvido is None:
            # Alias quebrado
            aliases_quebrados.append(arq)
            continue
            
        # Validar arquivo resolvido (sem tentar resolver novamente)
        if validar_arquivo_excel(arq_resolvido, resolver_alias=False):
            arquivos_validos.append(arq_resolvido)
    
    if aliases_quebrados:
        logger.warning(f"⚠️ {len(aliases_quebrados)} alias(es) quebrado(s) encontrado(s) em {pasta}:")
        for alias in aliases_quebrados:
            logger.warning(f"   - {alias.name}")
    
    if not arquivos_validos:
        logger.warning(f"Nenhum arquivo Excel válido encontrado em: {pasta}")
        return None
    
    return max(arquivos_validos, key=lambda x: x.stat().st_mtime)


def main():
    """Função principal"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Processar arquivo Excel COVERTE BASE PROP e atualizar tabela base_coverte_prop"
    )
    
    parser.add_argument(
        '--arquivo',
        type=str,
        help='Caminho para o arquivo Excel a ser processado'
    )
    
    parser.add_argument(
        '--pasta',
        type=str,
        help='Pasta para buscar arquivos Excel (processa o mais recente)'
    )
    
    parser.add_argument(
        '--db',
        type=str,
        default=DB_PATH,
        help=f'Caminho para o banco de dados (padrão: {DB_PATH})'
    )
    
    parser.add_argument(
        '--smb',
        action='store_true',
        help='Conectar automaticamente ao compartilhamento SMB (smb://files/02 Planejamento)'
    )
    
    parser.add_argument(
        '--copiar-local',
        action='store_true',
        dest='copiar_local',
        help='Copiar arquivo da rede para pasta local antes de processar (mais rápido)'
    )
    
    args = parser.parse_args()
    
    arquivo_excel = None
    
    if args.arquivo:
        arquivo_excel = Path(args.arquivo)
    elif args.pasta:
        pasta = Path(args.pasta)
        arquivo_excel = encontrar_arquivo_excel(pasta)
        if not arquivo_excel:
            logger.error(f"Nenhum arquivo Excel encontrado em: {pasta}")
            return
    else:
        # Tentar múltiplas fontes (prioridade: COVERTE BASE PROP, depois outras)
        arquivo_excel = None
        
        # 1. PRIORIDADE: Tentar via SMB se solicitado ou se rede não estiver montada
        if args.smb or not CAMINHO_BASE_NETWORK.exists():
            logger.info("=" * 70)
            logger.info("🔌 CONEXÃO SMB")
            logger.info("=" * 70)
            arquivo_excel = obter_arquivo_coverte_smb()
            
            # Se encontrou e solicitou cópia local, copiar
            if arquivo_excel and args.copiar_local:
                arquivo_local = copiar_arquivo_para_local(arquivo_excel)
                if arquivo_local:
                    arquivo_excel = arquivo_local
                    logger.info(f"✓ Usando cópia local para processamento mais rápido")
        
        # 2. Tentar arquivo específico se configurado
        if not arquivo_excel:
            if 'ARQUIVO_BASE_COVERTE' in globals() and ARQUIVO_BASE_COVERTE and ARQUIVO_BASE_COVERTE.exists():
                arquivo_excel = ARQUIVO_BASE_COVERTE
                logger.info(f"✓ Arquivo COVERTE BASE PROP encontrado (caminho específico): {arquivo_excel}")
        
        # 3. Tentar pasta da base unificada (COVERTE BASE PROP) na rede
        if not arquivo_excel and CAMINHO_BASE_NETWORK.exists():
            # Buscar especificamente por "COVERTE BASE PROP.xlsx" primeiro
            arquivos_coverte = list(CAMINHO_BASE_NETWORK.glob("COVERTE BASE PROP*.xlsx"))
            if not arquivos_coverte:
                # Se não encontrar, buscar qualquer .xlsx na pasta
                arquivos_coverte = list(CAMINHO_BASE_NETWORK.glob("*.xlsx"))
            
            if arquivos_coverte:
                arquivo_excel = max(arquivos_coverte, key=lambda x: x.stat().st_mtime)
                logger.info(f"✓ Arquivo COVERTE BASE PROP encontrado na rede: {arquivo_excel.name}")
                logger.info(f"  Caminho: {arquivo_excel}")
        
        # 4. Se não encontrou na rede, verificar se há arquivo local copiado
        if not arquivo_excel and CAMINHO_BASE_LOCAL.exists():
            arquivos_coverte_local = list(CAMINHO_BASE_LOCAL.glob("COVERTE BASE PROP*.xlsx"))
            if arquivos_coverte_local:
                arquivo_excel = max(arquivos_coverte_local, key=lambda x: x.stat().st_mtime)
                logger.info(f"✓ Arquivo COVERTE BASE PROP encontrado localmente: {arquivo_excel.name}")
        
        # 5. Tentar pasta de importações
        if not arquivo_excel and PASTA_IMPORTACOES_PATH.exists():
            arquivo_excel = encontrar_arquivo_excel(PASTA_IMPORTACOES_PATH)
            if arquivo_excel:
                logger.info(f"Arquivo Excel encontrado em pasta de importações: {arquivo_excel.name}")
        
        # 6. Tentar pasta local se não encontrou
        if not arquivo_excel:
            CAMINHO_BASE_LOCAL.mkdir(parents=True, exist_ok=True)
            arquivo_excel = encontrar_arquivo_excel(CAMINHO_BASE_LOCAL)
            if arquivo_excel:
                logger.info(f"Arquivo Excel encontrado em pasta local: {arquivo_excel.name}")
        
        # 7. Tentar pasta de entrada geral
        if not arquivo_excel and PASTA_ENTRADA_PATH.exists():
            arquivo_excel = encontrar_arquivo_excel(PASTA_ENTRADA_PATH)
            if arquivo_excel:
                logger.info(f"Arquivo Excel encontrado em pasta de entrada: {arquivo_excel.name}")
        
        # 8. Última tentativa: tentar SMB automaticamente
        if not arquivo_excel:
            logger.info("")
            logger.info("📡 Tentando conexão SMB automática...")
            arquivo_excel = obter_arquivo_coverte_smb()
        
        if not arquivo_excel:
            logger.error("=" * 70)
            logger.error("❌ Nenhum arquivo Excel especificado ou encontrado.")
            logger.error("=" * 70)
            logger.info("")
            logger.info("Opções disponíveis:")
            logger.info("  --arquivo <caminho>  : Especificar arquivo diretamente")
            logger.info("  --pasta <pasta>      : Buscar em pasta específica")
            logger.info("  --smb                : Conectar via SMB automaticamente")
            logger.info("  --copiar-local       : Copiar arquivo da rede para local")
            logger.info("")
            logger.info(f"Pastas verificadas (em ordem de prioridade):")
            logger.info(f"  1. SMB: smb://{SMB_SERVER}/{SMB_SHARE}/{SMB_PATH}/{SMB_FILE}")
            logger.info(f"  2. Arquivo config: {ARQUIVO_BASE_COVERTE if 'ARQUIVO_BASE_COVERTE' in globals() else 'N/A'}")
            logger.info(f"  3. Base Unificada (rede): {CAMINHO_BASE_NETWORK}")
            logger.info(f"  4. Pasta local: {CAMINHO_BASE_LOCAL}")
            logger.info(f"  5. Pasta de importações: {PASTA_IMPORTACOES_PATH}")
            logger.info(f"  6. Pasta de entrada: {PASTA_ENTRADA_PATH}")
            logger.info("")
            logger.info("Para montar manualmente o caminho de rede no Mac:")
            logger.info("  1. Finder > Cmd+K (Connect to Server)")
            logger.info(f"  2. Digite: smb://{SMB_SERVER}/{SMB_SHARE}")
            logger.info(f"  3. Navegue até: {SMB_PATH}/{SMB_FILE}")
            return
    
    logger.info("=" * 70)
    logger.info("Processador de Excel COVERTE BASE PROP")
    logger.info("Tabela: base_coverte_prop")
    logger.info("=" * 70)
    logger.info("")
    
    stats = processar_excel_unificado(arquivo_excel, args.db)
    
    logger.info("")
    logger.info("=" * 70)
    logger.info("Processamento concluído!")
    logger.info("=" * 70)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("\nProcessamento interrompido pelo usuário.")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Erro fatal: {e}", exc_info=True)
        sys.exit(1)

