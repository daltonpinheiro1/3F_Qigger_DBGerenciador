"""
Script para gerar arquivo de homologação WPP
Mostra como os dados serão enviados ao WhatsApp sem fazer o envio real

REGRAS DE NEGÓCIO:
- Apenas clientes com crivo_vendas = "APROVADA" recebem mensagens
- Coluna Numero deve conter apenas números (letras vão para Complemento)
- Verificação de histórico para controle de tentativas
- Se já enviou nas últimas 24h, não envia novamente
- Se já tem template 1, retorna template 2
- Máximo 5 tentativas de templates 1 ou 2, depois sai da fila
"""
import sys
from pathlib import Path
from datetime import datetime, timedelta
import re

# Configurar encoding UTF-8
from src.utils.console_utils import setup_windows_console
setup_windows_console()

import logging
import io
import csv

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
        logging.FileHandler('logs/homologacao_wpp.log', encoding='utf-8'),
        console_handler
    ]
)

logger = logging.getLogger(__name__)

from src.database.db_manager import DatabaseManager
from src.utils.templates_wpp import TemplateMapper, TEMPLATES
from src.utils.objects_loader import ObjectsLoader
from src.models.portabilidade import PortabilidadeRecord
from src.utils.validar_processamento import filtrar_registros_validos, obter_estatisticas_validacao
from src.utils.progress_bar import ProgressBar, log_progress
from src.utils.data_integrity import sanitizar_valor, validar_integridade_linha
from typing import Dict, Optional
import pandas as pd

# Caminhos (usar config centralizado - data/ para processar_completo encontrar)
try:
    from config import DB_PATH, OUTPUT_WPP, PASTA_SAIDA_HOMOLOGACAO
    OUTPUT_HOMOLOGACAO = Path(OUTPUT_WPP)
    # Base analítica agora vem do banco unificado (base_unificada) ou Excel processado
    BASE_ANALITICA_PATH = Path("/dev/null")  # Placeholder que nunca existe
except ImportError:
    # Fallback: sempre data/ para consistência com processar_completo
    DB_PATH = str(Path(__file__).parent / "data" / "portabilidade.db")
    OUTPUT_HOMOLOGACAO = Path(__file__).parent / "data" / "homologacao_wpp.xlsx"
    BASE_ANALITICA_PATH = Path("/dev/null")  # Placeholder que nunca existe

# Importar QueriesV2 para código path V2 (primário) com fallback legado
try:
    from config import DB_V2_PATH
    from src.database.queries_v2 import QueriesV2
    _DB_V2_AVAILABLE = bool(DB_V2_PATH) and Path(DB_V2_PATH).exists()
except (ImportError, Exception) as _v2_import_err:
    DB_V2_PATH = None
    _DB_V2_AVAILABLE = False

# Respeitar flags --forcar-legado / --forcar-v2 propagadas via env vars
import os as _os
if _os.environ.get('QIGGER_FORCAR_LEGADO') == '1':
    _DB_V2_AVAILABLE = False
elif _os.environ.get('QIGGER_FORCAR_V2') == '1' and DB_V2_PATH:
    _DB_V2_AVAILABLE = True

# Garantir que pasta de saída existe
OUTPUT_HOMOLOGACAO.parent.mkdir(parents=True, exist_ok=True)

# Limite de dias para geração (apenas últimos N dias)
DIAS_LIMITE_HOMOLOGACAO = 55

# Palavras a ignorar ao extrair primeiro e último nome
PALAVRAS_IGNORAR = {'e', 'de', 'da', 'do', 'das', 'dos', 'em', 'na', 'no', 'nas', 'nos'}

# =============================================================================
# CONFIGURAÇÃO DO GOOGLE SHEETS - HISTÓRICO DE ENVIOS
# =============================================================================
GOOGLE_SHEET_ID = '13qXylcL-wYbB4vDouI4d2rRazYQvaPEZneEmLx-lVtk'
GOOGLE_SHEET_EXPORT_URL = f'https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/export?format=csv'
MAX_ENVIOS_POR_CLIENTE = 3  # Máximo de envios tipo 1 ou 2 antes de bloquear

# =============================================================================
# CONFIGURAÇÃO DE CONTROLE DE ENVIOS
# =============================================================================
MAX_TENTATIVAS_TEMPLATE = 3  # Máximo de tentativas por template 1 ou 2
HORAS_ENTRE_ENVIOS = 48  # Horas mínimas entre envios para o mesmo cliente (48h conforme solicitado)

# =============================================================================
# ARQUIVO DE IDs PARA FORÇAR INCLUSÃO NO WPP
# =============================================================================
IDS_FORCAR_WPP_PATH = Path(__file__).parent / "data" / "ids_forcar_wpp.txt"


def carregar_ids_forcar_wpp() -> set:
    """
    Carrega lista de IDs (proposta_isize) que devem ser forçados no disparo WPP.
    O arquivo deve ter um ID por linha. Linhas começando com # são ignoradas.
    
    Returns:
        Set de IDs (strings) para forçar inclusão
    """
    ids_forcar = set()
    
    if not IDS_FORCAR_WPP_PATH.exists():
        logger.info(f"[IDS_FORCAR] Arquivo não encontrado: {IDS_FORCAR_WPP_PATH}")
        return ids_forcar
    
    try:
        with open(IDS_FORCAR_WPP_PATH, 'r', encoding='utf-8') as f:
            for linha in f:
                linha = linha.strip()
                # Ignorar linhas vazias e comentários
                if not linha or linha.startswith('#'):
                    continue
                # Extrair apenas números (remover espaços e vírgulas)
                id_limpo = ''.join(filter(str.isdigit, linha))
                if id_limpo:
                    ids_forcar.add(id_limpo)
        
        logger.info(f"[IDS_FORCAR] {len(ids_forcar)} IDs carregados de {IDS_FORCAR_WPP_PATH.name}")
    except Exception as e:
        logger.error(f"[IDS_FORCAR] Erro ao carregar arquivo: {e}")
    
    return ids_forcar


def extrair_numero_e_complemento(numero_original: str, complemento_original: str = "") -> tuple:
    """
    Extrai apenas números do campo Numero e move letras/texto para Complemento.
    
    Args:
        numero_original: Valor original do campo Numero (pode conter letras)
        complemento_original: Valor original do campo Complemento
        
    Returns:
        Tupla (numero_limpo, complemento_atualizado)
    """
    if not numero_original:
        return "", complemento_original or ""
    
    numero_str = str(numero_original).strip()
    
    # Extrair apenas dígitos do número
    apenas_numeros = ''.join(filter(str.isdigit, numero_str))
    
    # Extrair parte não-numérica (letras e caracteres especiais)
    parte_texto = re.sub(r'[\d\s]+', ' ', numero_str).strip()
    
    # Se houver parte texto, adicionar ao complemento
    if parte_texto:
        complemento_atualizado = complemento_original or ""
        if complemento_atualizado:
            # Evitar duplicação
            if parte_texto.lower() not in complemento_atualizado.lower():
                complemento_atualizado = f"{parte_texto} {complemento_atualizado}".strip()
        else:
            complemento_atualizado = parte_texto
        return apenas_numeros or "0", complemento_atualizado
    
    return apenas_numeros or numero_str, complemento_original or ""


def carregar_historico_envios_gsheet() -> pd.DataFrame:
    """
    Carrega o histórico de envios do Google Sheets compartilhado na nuvem.
    
    Returns:
        DataFrame com histórico de envios ou DataFrame vazio se falhar
    """
    try:
        import requests
        from io import StringIO
        
        logger.info("[GSHEET] Carregando histórico de envios do Google Sheets...")
        
        response = requests.get(GOOGLE_SHEET_EXPORT_URL, timeout=30)
        
        if response.status_code == 200 and 'text/csv' in response.headers.get('content-type', ''):
            df = pd.read_csv(StringIO(response.text), dtype=str)
            logger.info(f"[GSHEET] ✅ {len(df)} registros carregados do histórico")
            return df
        else:
            logger.warning(f"[GSHEET] ⚠️ Não foi possível acessar Google Sheets (status: {response.status_code})")
            return pd.DataFrame()
            
    except ImportError:
        logger.warning("[GSHEET] ⚠️ Módulo 'requests' não instalado. Histórico não será verificado.")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"[GSHEET] ❌ Erro ao carregar histórico: {e}")
        return pd.DataFrame()


def verificar_historico_cliente(
    proposta_isize: str, 
    cpf: str, 
    tipo_comunicacao_novo: int,
    historico_df: pd.DataFrame
) -> int:
    """
    Verifica o histórico de envios de um cliente e determina o Tipo_Comunicacao correto.
    
    Regras:
    1. Se enviou nas últimas 48 horas → retorna 0 (não enviar, aguardar próxima atualização)
    2. Se último Tipo_Comunicacao = 1 e novo = 1 → retorna 2
    3. Se total de envios com tipo 1 ou 2 >= 5 → retorna 0 (não enviar)
    4. Caso contrário, mantém o tipo original
    
    Args:
        proposta_isize: Código da proposta
        cpf: CPF do cliente
        tipo_comunicacao_novo: Tipo de comunicação da nova atualização
        historico_df: DataFrame com histórico de envios
        
    Returns:
        Tipo de comunicação ajustado (0, 1 ou 2)
    """
    if historico_df.empty:
        return tipo_comunicacao_novo
    
    # Normalizar valores para busca
    proposta_str = str(proposta_isize).strip() if proposta_isize else ''
    cpf_str = str(cpf).strip() if cpf else ''
    
    # Buscar registros do cliente no histórico
    mask = pd.Series([False] * len(historico_df))
    
    if proposta_str and 'Proposta_iSize' in historico_df.columns:
        mask = mask | (historico_df['Proposta_iSize'].astype(str).str.strip() == proposta_str)
    
    if cpf_str and 'Cpf' in historico_df.columns:
        mask = mask | (historico_df['Cpf'].astype(str).str.strip() == cpf_str)
    
    registros_cliente = historico_df[mask].copy()
    
    if registros_cliente.empty:
        # Cliente nunca recebeu mensagem
        return tipo_comunicacao_novo
    
    # Regra 1: Verificar se houve envio nas últimas 48 horas (considerando data e hora do disparo)
    agora = datetime.now()
    if 'DataHora_Disparo' in registros_cliente.columns:
        # Tentar parsear data/hora do disparo
        for idx, row in registros_cliente.iterrows():
            data_disparo_str = str(row.get('DataHora_Disparo', '')).strip()
            if not data_disparo_str or data_disparo_str in ['', 'None', 'nan']:
                continue
            
            try:
                # Tentar diferentes formatos de data/hora
                for fmt in [
                    '%Y-%m-%d %H:%M:%S',
                    '%d/%m/%Y %H:%M:%S',
                    '%Y-%m-%d',
                    '%d/%m/%Y',
                    '%Y-%m-%d %H:%M',
                    '%d/%m/%Y %H:%M'
                ]:
                    try:
                        data_disparo = datetime.strptime(data_disparo_str[:19] if len(data_disparo_str) > 19 else data_disparo_str, fmt)
                        horas_decorridas = (agora - data_disparo).total_seconds() / 3600
                        
                        # Se enviou nas últimas 48 horas, não enviar novamente
                        if horas_decorridas < HORAS_ENTRE_ENVIOS:
                            logger.debug(f"[GSHEET] Cliente {proposta_str or cpf_str}: enviado há {horas_decorridas:.1f}h → Tipo 0 (aguardar 48h)")
                            return 0
                        break
                    except ValueError:
                        continue
            except Exception as e:
                logger.debug(f"[GSHEET] Erro ao parsear data_disparo '{data_disparo_str}': {e}")
    
    # Contar envios com tipo 1 ou 2
    if 'Tipo_Comunicacao' in registros_cliente.columns:
        # Converter para numérico, tratando erros
        tipos = pd.to_numeric(registros_cliente['Tipo_Comunicacao'], errors='coerce').fillna(0)
        
        # Contar envios efetivos (tipo 1 ou 2)
        total_envios_efetivos = ((tipos == 1) | (tipos == 2)).sum()
        
        # Regra 3: Se já enviamos 5 vezes, não enviar mais
        if total_envios_efetivos >= MAX_ENVIOS_POR_CLIENTE:
            logger.debug(f"[GSHEET] Cliente {proposta_str or cpf_str}: {total_envios_efetivos} envios → Tipo 0 (bloqueado)")
            return 0
        
        # Pegar último tipo de comunicação
        ultimo_tipo = tipos.iloc[-1] if len(tipos) > 0 else 0
        
        # Regra 2: Se último foi 1 e novo é 1, classificar como 2
        if ultimo_tipo == 1 and tipo_comunicacao_novo == 1:
            logger.debug(f"[GSHEET] Cliente {proposta_str or cpf_str}: último=1, novo=1 → Tipo 2")
            return 2
    
    return tipo_comunicacao_novo


def processar_tipo_comunicacao_com_historico(
    registros: list,
    historico_df: pd.DataFrame
) -> dict:
    """
    Processa todos os registros e ajusta o Tipo_Comunicacao baseado no histórico.
    
    Args:
        registros: Lista de registros a processar
        historico_df: DataFrame com histórico de envios
        
    Returns:
        Dicionário com estatísticas {total, mantidos, alterados_para_2, bloqueados}
    """
    stats = {
        'total': len(registros),
        'mantidos': 0,
        'alterados_para_2': 0,
        'bloqueados': 0
    }
    
    for registro in registros:
        tipo_original = registro.get('tipo_comunicacao', 1)
        if isinstance(tipo_original, str):
            try:
                tipo_original = int(tipo_original) if tipo_original.isdigit() else 1
            except (ValueError, AttributeError):
                tipo_original = 1
        
        tipo_ajustado = verificar_historico_cliente(
            proposta_isize=registro.get('codigo_externo', ''),
            cpf=registro.get('cpf', ''),
            tipo_comunicacao_novo=tipo_original,
            historico_df=historico_df
        )
        
        # Atualizar registro com tipo ajustado
        registro['tipo_comunicacao'] = tipo_ajustado
        
        # Contabilizar
        if tipo_ajustado == 0:
            stats['bloqueados'] += 1
        elif tipo_ajustado == 2 and tipo_original == 1:
            stats['alterados_para_2'] += 1
        else:
            stats['mantidos'] += 1
    
    return stats


def normalizar_telefone(telefone: str) -> str:
    """
    Normaliza telefone para formato brasileiro: 11 dígitos (DDD + nono dígito + número)
    Exemplo: "31999887766"
    
    Args:
        telefone: Telefone em qualquer formato
        
    Returns:
        Telefone normalizado com 11 dígitos ou string vazia
    """
    if not telefone:
        return ""
    
    # Remover todos os caracteres não numéricos
    telefone_limpo = ''.join(filter(str.isdigit, str(telefone)))
    
    # Se já tem 11 dígitos, retornar
    if len(telefone_limpo) == 11:
        return telefone_limpo
    
    # Se tem 10 dígitos (DDD + número sem nono dígito), adicionar 9
    if len(telefone_limpo) == 10:
        return telefone_limpo[:2] + '9' + telefone_limpo[2:]
    
    # Se tem menos de 10 dígitos, não é válido
    if len(telefone_limpo) < 10:
        return ""
    
    # Se tem mais de 11 dígitos, pegar os últimos 11
    if len(telefone_limpo) > 11:
        return telefone_limpo[-11:]
    
    return telefone_limpo


def normalizar_cep(cep: str) -> str:
    """
    Normaliza CEP para formato brasileiro: 8 dígitos com zeros à esquerda
    Exemplo: "30620090"
    
    Args:
        cep: CEP em qualquer formato
        
    Returns:
        CEP normalizado com 8 dígitos ou string vazia
    """
    if not cep:
        return ""
    
    # Remover todos os caracteres não numéricos
    cep_limpo = ''.join(filter(str.isdigit, str(cep)))
    
    # Se vazio, retornar vazio
    if not cep_limpo:
        return ""
    
    # Preencher com zeros à esquerda até 8 dígitos
    cep_normalizado = cep_limpo.zfill(8)
    
    # Se tiver mais de 8 dígitos, pegar apenas os primeiros 8
    if len(cep_normalizado) > 8:
        cep_normalizado = cep_normalizado[:8]
    
    return cep_normalizado


# Padrões de valores inválidos para número de endereço
VALORES_NUMERO_INVALIDO = {
    'sn', 's/n', 's.n', 's.n.', 'n/a', 'na', 'n.a', 'n.a.',
    'nao tem', 'não tem', 'sem numero', 'sem número', 
    'sem', 'nenhum', 'null', 'none', '-', '--', '.',
    '0', '00', '000', 'zero'
}

# Padrões de complementos a extrair do número
PADROES_COMPLEMENTO = [
    r'\s*-?\s*bl\.?\s*(\w+)',           # BL A, BL. A, - BL A
    r'\s*-?\s*bloco\.?\s*(\w+)',         # Bloco A
    r'\s*-?\s*apto?\.?\s*(\d+\w*)',       # Apt 101, Apto. 101A
    r'\s*-?\s*apartamento\.?\s*(\d+\w*)', # Apartamento 101
    r'\s*-?\s*sala\.?\s*(\d+\w*)',        # Sala 101
    r'\s*-?\s*loja\.?\s*(\d+\w*)',        # Loja 01
    r'\s*-?\s*casa\.?\s*(\d+\w*)',        # Casa 2
    r'\s*-?\s*fundos',                    # Fundos
    r'\s*-?\s*frente',                    # Frente
    r'\s*-?\s*lado\.?\s*(\w+)',           # Lado A
    r'\s*-?\s*(\d+)\s*andar',             # 2 andar
    r'\s*-?\s*andar\.?\s*(\d+)',          # Andar 2
    r'\s+([A-Za-z])$',                    # Termina com letra (150 B -> numero=150, complemento=B)
]

import re

def normalizar_numero_endereco(numero: str, complemento_existente: str = "") -> tuple:
    """
    Normaliza número de endereço:
    - Se for sn, n/a, S/N, não tem, etc. -> retorna "0"
    - Se tiver complementos como "B", "fundos", extrai e adiciona ao complemento
    
    Args:
        numero: Número do endereço (pode conter complementos)
        complemento_existente: Complemento já existente
        
    Returns:
        Tupla (numero_normalizado, complemento_atualizado)
    """
    if not numero:
        return "0", complemento_existente
    
    numero_str = str(numero).strip()
    
    # Verificar se é valor inválido (sn, n/a, etc.)
    numero_lower = numero_str.lower().strip()
    if numero_lower in VALORES_NUMERO_INVALIDO:
        return "0", complemento_existente
    
    # Se não tem dígitos, é inválido
    if not any(c.isdigit() for c in numero_str):
        # Pode ser só complemento (ex: "Fundos")
        complemento_novo = numero_str
        if complemento_existente:
            complemento_novo = f"{complemento_existente} - {numero_str}"
        return "0", complemento_novo
    
    # Extrair número e possíveis complementos
    numero_limpo = numero_str
    complemento_extraido = []
    
    # Verificar cada padrão de complemento
    for padrao in PADROES_COMPLEMENTO:
        match = re.search(padrao, numero_str, re.IGNORECASE)
        if match:
            # Extrair o que foi encontrado
            texto_match = match.group(0).strip()
            complemento_extraido.append(texto_match.strip(' -'))
            # Remover do número
            numero_limpo = re.sub(padrao, '', numero_limpo, flags=re.IGNORECASE).strip()
    
    # Se o número restante tem letra isolada no final (ex: "150 B")
    match_letra = re.match(r'^(\d+)\s*([A-Za-z])$', numero_limpo)
    if match_letra:
        numero_limpo = match_letra.group(1)
        complemento_extraido.append(match_letra.group(2).upper())
    
    # Limpar número - pegar só os dígitos iniciais
    numero_final = ''.join(c for c in numero_limpo if c.isdigit())
    
    # Se não sobrou número válido
    if not numero_final:
        numero_final = "0"
    
    # Montar complemento final
    complemento_final = complemento_existente or ""
    if complemento_extraido:
        novo_complemento = " ".join(complemento_extraido)
        if complemento_final:
            # Não duplicar se já existir
            if novo_complemento.lower() not in complemento_final.lower():
                complemento_final = f"{complemento_final} - {novo_complemento}"
        else:
            complemento_final = novo_complemento
    
    return numero_final, complemento_final.strip(' -')


def normalizar_data_venda(data) -> str:
    """
    Normaliza data de venda para formato DD/MM/AAAA
    
    Args:
        data: Data em qualquer formato (datetime, string, etc)
        
    Returns:
        Data formatada como DD/MM/AAAA ou string vazia
    """
    from datetime import datetime as dt_class
    
    if not data:
        return ""
    
    # Se já é string no formato correto, retornar
    if isinstance(data, str):
        # Tentar parsear e reformatar
        try:
            # Tentar formatos comuns
            for fmt in ['%d/%m/%Y', '%Y-%m-%d', '%d/%m/%Y %H:%M:%S', '%Y-%m-%d %H:%M:%S']:
                try:
                    dt = dt_class.strptime(data.strip(), fmt)
                    return dt.strftime('%d/%m/%Y')
                except ValueError:
                    continue
        except (AttributeError, TypeError):
            pass
        return data.strip()
    
    # Se é datetime, formatar
    if isinstance(data, dt_class):
        return data.strftime('%d/%m/%Y')
    
    return str(data)


class BaseAnaliticaLoader:
    """Carrega e busca dados da base analítica final"""
    
    def __init__(self, file_path: str):
        self.file_path = file_path
        self._data = None
        self._index_by_codigo = {}
        self._index_by_cpf = {}
        self._loaded = False
        
    def load(self) -> int:
        """Carrega dados da base analítica"""
        if self._loaded:
            return len(self._data) if self._data is not None else 0
        
        if not Path(self.file_path).exists():
            logger.warning(f"Arquivo base analítica não encontrado: {self.file_path}")
            return 0
        
        try:
            # Tentar diferentes encodings
            encodings = ['utf-8-sig', 'utf-8', 'latin-1', 'cp1252']
            df = None
            encoding_usado = None
            
            for encoding in encodings:
                try:
                    df = pd.read_csv(self.file_path, encoding=encoding, delimiter=';', low_memory=False)
                    encoding_usado = encoding
                    break
                except UnicodeDecodeError:
                    continue
            
            if df is None:
                logger.error(f"Não foi possível ler base analítica: {self.file_path}")
                return 0
            
            self._data = df
            
            # Criar índices por código externo e CPF
            for _, row in df.iterrows():
                # Buscar código externo (pode ser 'Proposta iSize' ou variações)
                codigo_externo = str(row.get('Proposta iSize', '') or row.get('Proposta_iSize', '') or 
                                     row.get('Código externo', '') or row.get('Codigo externo', '') or 
                                     row.get('Código Externo', '') or '').strip()
                cpf = str(row.get('CPF', '') or row.get('Cpf', '') or '').strip()
                
                # Limpar CPF (remover pontos e hífens)
                if cpf:
                    cpf_limpo = cpf.replace('.', '').replace('-', '').strip()
                else:
                    cpf_limpo = ''
                
                if codigo_externo:
                    self._index_by_codigo[codigo_externo] = row
                
                if cpf_limpo:
                    if cpf_limpo not in self._index_by_cpf:
                        self._index_by_cpf[cpf_limpo] = []
                    self._index_by_cpf[cpf_limpo].append(row)
            
            self._loaded = True
            logger.info(f"Base analítica carregada: {len(df)} registros (encoding: {encoding_usado})")
            logger.info(f"  - Índice por código externo: {len(self._index_by_codigo)} códigos únicos")
            logger.info(f"  - Índice por CPF: {len(self._index_by_cpf)} CPFs únicos")
            
            return len(df)
            
        except Exception as e:
            logger.error(f"Erro ao carregar base analítica: {e}")
            return 0
    
    def find_by_codigo_externo(self, codigo_externo: str) -> Optional[pd.Series]:
        """Busca registro por código externo"""
        if not self._loaded:
            self.load()
        
        if not codigo_externo:
            return None
        
        codigo_limpo = str(codigo_externo).strip().lstrip('0')
        
        # Tentar busca direta
        result = self._index_by_codigo.get(codigo_externo)
        if result is None:
            result = self._index_by_codigo.get(codigo_limpo)
        
        if result is not None:
            return result
        
        # Tentar variações
        codigo_variacoes = [
            codigo_externo.zfill(8),
            codigo_externo.zfill(9),
            codigo_limpo,
            codigo_limpo.zfill(8),
        ]
        
        for codigo_var in codigo_variacoes:
            if codigo_var != codigo_externo and codigo_var != codigo_limpo:
                result = self._index_by_codigo.get(codigo_var)
                if result is not None:
                    return result
        
        return None
    
    def find_by_cpf(self, cpf: str) -> Optional[pd.Series]:
        """Busca registro por CPF (retorna o mais recente se houver múltiplos)"""
        if not self._loaded:
            self.load()
        
        if not cpf:
            return None
        
        cpf_limpo = str(cpf).strip().replace('.', '').replace('-', '')
        matches = self._index_by_cpf.get(cpf_limpo, [])
        
        if matches:
            return matches[-1]  # Retorna o mais recente (último)
        
        return None
    
    def find_best_match(self, codigo_externo: str = None, cpf: str = None) -> Optional[pd.Series]:
        """Busca melhor match usando código externo ou CPF"""
        # Prioridade: código externo > CPF
        if codigo_externo:
            result = self.find_by_codigo_externo(codigo_externo)
            if result is not None:
                return result
        
        if cpf:
            result = self.find_by_cpf(cpf)
            if result is not None:
                return result
        
        return None
    
    @property
    def is_loaded(self) -> bool:
        return self._loaded


def extrair_primeiro_ultimo_nome(nome_completo: str) -> str:
    """
    Extrai primeiro e último nome, ignorando palavras de ligação
    
    Args:
        nome_completo: Nome completo do cliente
        
    Returns:
        Primeiro e último nome
    """
    if not nome_completo:
        return ""
    
    # Limpar e dividir
    partes = nome_completo.strip().split()
    
    if not partes:
        return ""
    
    if len(partes) == 1:
        return partes[0]
    
    # Pegar primeiro nome
    primeiro = partes[0]
    
    # Pegar último nome (ignorando palavras de ligação)
    ultimo = None
    for i in range(len(partes) - 1, 0, -1):
        if partes[i].lower() not in PALAVRAS_IGNORAR:
            ultimo = partes[i]
            break
    
    if ultimo:
        return f"{primeiro} {ultimo}"
    else:
        return primeiro


def formatar_link_rastreio(codigo_externo: str, objects_loader: ObjectsLoader = None) -> str:
    """
    Formata link de rastreio completo: https://tim.trakin.co/o/{nu_pedido}
    
    O nu_pedido deve estar no formato: 26-0250016438 (com prefixo 26-)
    
    Args:
        codigo_externo: Código externo (iSize)
        objects_loader: Loader de objetos para buscar número de pedido
        
    Returns:
        Link completo de rastreio no formato: https://tim.trakin.co/o/26-0250016438
    """
    if not codigo_externo:
        return ""
    
    # Tentar buscar número de pedido do Relatório de Objetos
    nu_pedido_completo = None
    
    if objects_loader:
        # Usar find_best_match que busca em múltiplos índices
        obj_match = objects_loader.find_best_match(codigo_externo)
        
        if obj_match:
            # Buscar número de pedido (nu_pedido já vem no formato 26-0250016438)
            nu_pedido = getattr(obj_match, 'nu_pedido', None)
            if nu_pedido:
                nu_pedido_str = str(nu_pedido).strip()
                if nu_pedido_str and not nu_pedido_str.startswith('http'):
                    # Se já tem formato 26-XXXXX, usar direto
                    if '-' in nu_pedido_str and nu_pedido_str.startswith('26-'):
                        nu_pedido_completo = nu_pedido_str
                    # Se tem hífen mas não começa com 26-, verificar se precisa adicionar prefixo
                    elif '-' in nu_pedido_str:
                        partes = nu_pedido_str.split('-', 1)
                        if len(partes) > 1:
                            # Se a primeira parte não é 26, adicionar prefixo 26-
                            if partes[0].strip() != '26':
                                numero = partes[1].strip().zfill(8)
                                nu_pedido_completo = f"26-{numero}"
                            else:
                                nu_pedido_completo = nu_pedido_str
                    # Se não tem hífen, adicionar prefixo 26-
                    else:
                        numero = nu_pedido_str.zfill(8)
                        nu_pedido_completo = f"26-{numero}"
    
    # Se não encontrou, usar código externo como fallback (formatar como 26-XXXXXXXX)
    if not nu_pedido_completo:
        # Garantir que o código externo tenha 8 dígitos com zeros à esquerda
        codigo_limpo = str(codigo_externo).strip().lstrip('0')  # Remover zeros à esquerda
        if not codigo_limpo:
            codigo_limpo = "0"
        numero_formatado = codigo_limpo.zfill(8)  # Preencher com zeros à esquerda até 8 dígitos
        nu_pedido_completo = f"26-{numero_formatado}"
    
    # Garantir que o formato está correto antes de retornar
    if nu_pedido_completo and not nu_pedido_completo.startswith('26-'):
        # Se por algum motivo não começou com 26-, corrigir
        if '-' in nu_pedido_completo:
            partes = nu_pedido_completo.split('-', 1)
            if len(partes) > 1:
                nu_pedido_completo = f"26-{partes[1].zfill(8)}"
        else:
            nu_pedido_completo = f"26-{nu_pedido_completo.zfill(8)}"
    
    # Retornar link completo
    return f"https://tim.trakin.co/o/{nu_pedido_completo}"


def substituir_variaveis_mensagem(corpo_mensagem: str, variaveis: Dict[str, str]) -> str:
    """
    Substitui variáveis {{1}}, {{2}}, etc. na mensagem
    
    Args:
        corpo_mensagem: Texto da mensagem com variáveis
        variaveis: Dicionário com variáveis {"1": "valor1", "2": "valor2"}
        
    Returns:
        Mensagem com variáveis substituídas
    """
    if not corpo_mensagem:
        return ""
    
    mensagem = corpo_mensagem
    for num, valor in variaveis.items():
        mensagem = mensagem.replace(f"{{{{{num}}}}}", str(valor) if valor else "")
    
    return mensagem


# =============================================================================
# DERIVAÇÃO DE TEMPLATE PARA DADOS V2
# (V2 não tem tipo_mensagem — derivar a partir de acao_a_realizar + status)
# =============================================================================

def derivar_template_v2(row_dict: dict) -> int:
    """
    Deriva o IDCorpo (1-8) a partir dos campos do V2.

    Regras (em ordem de prioridade):
    1. status_logistica contém 'AGUARDANDO RETIRADA' / 'AG RETIRADA'  → 3
    2. status_logistica contém 'EM ROTA' / 'SAIU ENTREGA'             → 8
    3. status_logistica contém 'ENTREGUE' sem ativação TIM            → 7
    4. status_tim preenchido (linha ativa)                            → 6
    5. acao_a_realizar = 'REABERTURA' / 'ENVIO GERENCIADOR'           → 1
    6. acao_a_realizar = 'POS VENDA PARABENIZAÇÃO' / 'BOAS VINDAS'    → 6
    7. acao_a_realizar = 'PARABENIZAÇÃO BP'                           → 1
    8. acao_a_realizar = 'INCENTIVO NOVA LINHA'                       → 1
    9. acao_a_realizar = 'VALIDAR GROSS'                              → 1
    10. status_bilhete contém 'PENDENTE' / 'AGUARDANDO'               → 2
    11. status_venda = 'APROVADA' (fallback)                          → 1
    Retorna 0 se não deve enviar (cancelado, sem ação, etc.)
    """
    acao = str(row_dict.get('acao_a_realizar') or '').strip().upper()
    status_log = str(row_dict.get('status_logistica') or '').strip().upper()
    status_venda = str(row_dict.get('status_venda') or '').strip().upper()
    status_tim = str(row_dict.get('status_tim') or '').strip().upper()
    status_bilhete = str(row_dict.get('status_bilhete') or '').strip().upper()
    status_pedido = str(row_dict.get('status_pedido') or '').strip().upper()

    # Não enviar para cancelados
    if status_venda in ('CANCELADA', 'CANCELADO', 'REJEITADA', 'REJEITADO'):
        return 0
    if acao in ('CANCELADO A PEDIDO CLIENTE', 'NAO ENVIAR', 'NÃO ENVIAR', 'DEFINIR AÇÃO'):
        return 0

    # Logística: aguardando retirada nos Correios
    if any(x in status_log for x in ('AG RETIRADA', 'AGUARDANDO RETIRADA', 'OBJETO AGUARDANDO')):
        return 3

    # Logística: chip saiu para entrega
    if any(x in status_log for x in ('EM ROTA', 'SAIU ENTREGA', 'EM TRANSITO', 'ENVIADO')):
        return 8

    # Logística: entregue mas sem ativação TIM
    if 'ENTREGUE' in status_log and not status_tim:
        return 7

    # TIM ativa: portabilidade concluída — apenas status que confirmam linha ativa
    if status_tim in (
        'ATIVA', 'ATIVO', 'ATIVADA', 'ATIVADO',
        'APROVISIONADO',
        'CONFIRMADO PELA DOADORA',
    ):
        return 6

    # Ações que mapeiam para template 1 (confirmação/envio)
    if acao in (
        'REABERTURA', 'ENVIO GERENCIADOR', 'PARABENIZAÇÃO BP',
        'INCENTIVO NOVA LINHA', 'VALIDAR GROSS', 'BOAS VINDAS',
    ):
        return 1

    # Pós-venda / parabenização → concluída
    if acao in ('POS VENDA PARABENIZAÇÃO', 'PARABENIZACAO', 'PARABENIZAÇÃO'):
        return 6

    # Bilhete pendente → pendência SMS (alterna entre 2 e 5)
    if any(x in status_bilhete for x in ('PENDENTE', 'AGUARDANDO', 'VALIDACAO')):
        # Usar template 5 se já houve tentativa anterior (status_venda APROVADA),
        # senão template 2
        return 5 if status_venda == 'APROVADA' else 2

    # Aprovada sem ação específica → template 1 (confirmação)
    if status_venda == 'APROVADA':
        return 1

    return 0


# Mensagens padrão dos templates (caso não estejam no banco)
MENSAGENS_PADRAO = {
    1: """Olá! A sua solicitação de portabilidade para a TIM foi processada com sucesso.  Para autorizar o envio do chip e a continuidade do processo, é necessária a confirmação do titular.  Realize a validação de uma das formas abaixo:  Toque no botão Confirmar Solicitação; ou Envie SMS com a palavra SIM para o número 7678.  Dados da Entrega:  Prazo estimado: Até 10 dias úteis. Recebimento: Necessário maior de 18 anos com documento. Observação: O chip será entregue com número provisório até a conclusão da portabilidade.  Status: Aguardando confirmação.""",
    2: """Olá. Verificamos uma pendência na etapa de validação da sua portabilidade numérica. Para concluir o processo técnico de transferência da linha, é necessário o envio do comando de confirmação via SMS a partir do seu chip atual. Instruções para regularização: 1. Envie a palavra PORTABILIDADE para o número 7678; ou 2. Utilize o atalho no botão abaixo para gerar o SMS automaticamente. O não envio do comando pode ocasionar a suspensão da solicitação. Status: Aguardando validação via SMS.""",
    3: """Olá, {{1}}. O seu pedido encontra-se disponível para retirada.  Para concluir a entrega, compareça à agência dos Correios indicada portando documento de identificação original com foto.  Status Atual: Objeto aguardando retirada Código de Rastreio: {{2}}  Utilize o botão abaixo para consultar o endereço exato da agência.""",
    4: """Olá, {{1}}. A portabilidade da sua linha foi processada.  Para iniciarmos a logística de entrega do chip, valide se o endereço cadastrado está atualizado:  Endereço de Destino: Rua: {{2}}, Nº {{3}}; Complemento: {{4}}; Bairro: {{5}} Cidade: {{6}} UF: {{7}}; CEP: {{8}}. Ponto de Referência: {{9}};  A exatidão dos dados é essencial para evitar devoluções. O endereço acima está correto?""",
    5: """Olá, {{1}}. Identificamos uma pendência na validação da sua portabilidade numérica. Para continuar o processo, confirme a solicitação pelo chip atual:  Instruções para regularização: 1. Envie a palavra SIM para o número 7678; ou 2. Utilize o atalho no botão abaixo para gerar o SMS automaticamente.  Status atual: aguardando validação via SMS.""",
    6: """Olá, {{1}}.  Sua portabilidade para a TIM foi concluída com sucesso e sua linha já está ativa. Se precisar de suporte ou consultar informações da sua linha, responda essa mensagem e fale com um de nossos especialistas.""",
    7: """Olá, {{1}}. Identificamos que o seu chip TIM já foi entregue, mas ainda não houve ativação.  Para concluir o processo: 1. insira o chip no celular; 2. desligue e ligue o aparelho; 3. aguarde a rede TIM aparecer.  Se precisar de ajuda, responda essa mensagem e fale com nosso especialistas.""",
    8: """Olá, {{1}}.  Seu chip TIM saiu para entrega. Acompanhe o envio pelo link abaixo: {{2}}  Quando receber o chip, insira-o no aparelho e reinicie o celular. A ativação da rede pode levar até 24 horas. Para realizar a confirmação 7678 Click no botão abaixo, e siga as instruções.""",
}

def obter_corpo_mensagem_template(db_manager: DatabaseManager, template_id: int) -> str:
    """
    Obtém o corpo da mensagem do template do banco de dados ou usa mensagem padrão
    
    Args:
        db_manager: Gerenciador do banco de dados
        template_id: ID do template
        
    Returns:
        Corpo da mensagem ou mensagem padrão
    """
    try:
        template = db_manager.get_template_by_id(template_id)
        if template:
            corpo = template.get('corpo_mensagem', '') or ''
            if corpo:
                return corpo
    except Exception as e:
        logger.warning(f"Erro ao buscar corpo da mensagem: {e}")
    
    # Usar mensagem padrão se não encontrar no banco
    return MENSAGENS_PADRAO.get(template_id, "")


def gerar_arquivo_homologacao():
    """Gera arquivo de homologação WPP"""
    
    print("=" * 70)
    print("GERAÇÃO DE ARQUIVO DE HOMOLOGAÇÃO WPP")
    print("=" * 70)
    print(f"Data/Hora: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print()
    
    # 1. Conectar ao banco
    print("[1] Conectando ao banco de dados...")
    db_manager = DatabaseManager(DB_PATH)
    
    # 1.1 Carregar IDs para forçar inclusão no WPP
    print("[1.1] Carregando IDs para forçar inclusão no WPP...")
    ids_forcar_wpp = carregar_ids_forcar_wpp()
    if ids_forcar_wpp:
        print(f"    >> {len(ids_forcar_wpp)} IDs carregados para forçar inclusão")
    else:
        print("    >> Nenhum ID para forçar (arquivo vazio ou não encontrado)")
    
    # 2. Buscar registros com template sincronizando todas as tabelas do portabilidade.db
    print("[2] Buscando registros com template mapeado (sincronizando todas as tabelas)...")
    
    # Data limite: apenas últimos 180 dias (ordenar do mais recente para mais antigo)
    data_limite = (datetime.now() - timedelta(days=DIAS_LIMITE_HOMOLOGACAO)).strftime('%Y-%m-%d')
    filtro_data_sql = ">= '" + data_limite + "'"
    print(f"    >> Filtro: últimos {DIAS_LIMITE_HOMOLOGACAO} dias (a partir de {data_limite}) | Ordenação: mais recente primeiro")
    
    # --- V2 como fonte PRIMÁRIA, legado como fallback ---
    rows = None
    columns = None
    _usou_v2 = False

    if _DB_V2_AVAILABLE:
        try:
            import sqlite3 as _sqlite3
            # Verificar existência da view vw_base_unificada antes de consultar
            _conn_check = _sqlite3.connect(DB_V2_PATH)
            try:
                _view_ok = _conn_check.execute(
                    "SELECT name FROM sqlite_master WHERE type='view' AND name='vw_base_unificada'"
                ).fetchone() is not None
            finally:
                _conn_check.close()

            if not _view_ok:
                logger.error(
                    "[V2] View vw_base_unificada não encontrada em %s — fallback para legado",
                    DB_V2_PATH,
                )
                print("    >> [V2] ❌ View vw_base_unificada ausente no banco v2, usando fallback legado")
            else:
                print("    >> [V2] Banco v2 detectado, usando QueriesV2 como fonte primária...")
                queries_v2 = QueriesV2(DB_V2_PATH)
                registros_v2 = queries_v2.buscar_registros_wpp(dias_limite=DIAS_LIMITE_HOMOLOGACAO)
                if registros_v2:
                    columns = list(registros_v2[0].keys())
                    rows = [tuple(r[c] for c in columns) for r in registros_v2]
                    _usou_v2 = True
                    logger.info("[V2] ✅ %d registros obtidos via QueriesV2.buscar_registros_wpp()", len(rows))
                    print(f"    >> [V2] ✅ {len(rows)} registros obtidos via QueriesV2")
                else:
                    logger.warning("[V2] ⚠ QueriesV2.buscar_registros_wpp() retornou 0 registros — ATENÇÃO: V2 pode estar com dados desatualizados ou cache vazia. Usando fallback legado.")
                    print("    >> [V2] ⚠ 0 registros retornados (cache desatualizada?), usando fallback legado")
        except Exception as e:
            logger.error("[V2] Erro ao usar QueriesV2, usando fallback legado: %s", e, exc_info=True)
            print(f"    >> [V2] ❌ Fallback para legado: {e}")
    else:
        logger.info("[V2] Banco v2 não disponível (%s), usando legado", DB_V2_PATH)
        print("    >> [V2] Banco v2 não disponível, usando legado")

    # --- Fallback legado: só executa se V2 não retornou resultados ---
    if not _usou_v2:
        with db_manager._get_connection() as conn:
            cursor = conn.cursor()

            # Verificar quais tabelas existem
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tabelas_existentes = [row[0] for row in cursor.fetchall()]
            tem_base_coverte = 'base_coverte_prop' in tabelas_existentes
            tem_relatorio_objetos = 'relatorio_objetos' in tabelas_existentes

            if tem_base_coverte:
                print("    >> [LEGADO] Usando base_coverte_prop + portabilidade_records" + (" + relatorio_objetos" if tem_relatorio_objetos else ""))
            else:
                print("    >> [LEGADO] Tabela base_coverte_prop não encontrada, usando apenas portabilidade_records")

            # Buscar registros ÚNICOS por codigo_externo E telefone_portado
            # Remove duplicados garantindo um único registro por (codigo_externo, telefone_portado)
            # FILTROS:
            # - Apenas vendas com telefone_portado (não nova linha)
            # - Excluir entregas canceladas/extraviadas
            # - Apenas clientes com crivo_vendas = "APROVADA" (ou IDs forçados)
            if tem_base_coverte:
                query = """
            WITH registros_filtrados AS (
                SELECT 
                -- Dados de portabilidade_records
                    pr.id AS pr_id,
                COALESCE(bc.cpf, pr.cpf""" + (", ro.documento" if tem_relatorio_objetos else "") + """, '') AS cpf,
                COALESCE(pr.numero_acesso, '') AS numero_acesso,
                    COALESCE(bc.numero_ordem, pr.numero_ordem, '') AS numero_ordem,
                COALESCE(bc.proposta_isize, bc.codigo_externo, pr.codigo_externo""" + (", ro.codigo_externo" if tem_relatorio_objetos else "") + """, '') AS codigo_externo,
                
                    -- Template e regras (priorizar dados existentes)
                    COALESCE(pr.tipo_mensagem, '') AS tipo_mensagem,
                    COALESCE(pr.template, '1') AS template,
                    pr.regra_id AS regra_id,
                    COALESCE(pr.o_que_aconteceu, '') AS o_que_aconteceu,
                    COALESCE(pr.acao_a_realizar, '') AS acao_a_realizar,
                    
                    -- Dados adicionais de base_coverte_prop (FONTE PRINCIPAL)
                    bc.cliente_nome AS cliente_nome,
                    bc.telefone_portado AS telefone_portado,
                    bc.data_venda AS data_venda,
                    bc.data_conectada AS data_conectada,
                    bc.plano AS plano,
                    bc.endereco AS endereco,
                    bc.numero AS numero,
                    bc.complemento AS complemento,
                    bc.bairro AS bairro,
                    bc.cidade AS cidade,
                    bc.uf AS uf,
                    bc.cep AS cep,
                    bc.ponto_referencia AS ponto_referencia,
                    bc.crivo_vendas AS crivo_vendas,
                    
                    -- Status de entrega (para filtrar canceladas/extraviadas)
                    COALESCE(bc.status_correios, bc.status_loggi, bc.status_entrega_prevista, '') AS status_entrega,
                
                -- Dados de logística (relatorio_objetos)
                """ + ("""
                    ro.nu_pedido AS nu_pedido,
                    ro.rastreio AS rastreio,
                    ro.status AS ro_status,
                """ if tem_relatorio_objetos else """
                    NULL AS nu_pedido,
                    NULL AS rastreio,
                    NULL AS ro_status,
                """) + """
                
                    -- Contagem de classificações por codigo_externo (calculado depois)
                    COUNT(pr.id) OVER (PARTITION BY COALESCE(bc.proposta_isize, bc.codigo_externo, pr.codigo_externo""" + (", ro.codigo_externo" if tem_relatorio_objetos else "") + """, '')) AS total_classificacoes_raw,
                    ROW_NUMBER() OVER (
                        PARTITION BY 
                            COALESCE(bc.proposta_isize, bc.codigo_externo, pr.codigo_externo""" + (", ro.codigo_externo" if tem_relatorio_objetos else "") + """, ''),
                            COALESCE(bc.telefone_portado, '')
                        ORDER BY 
                            CASE 
                                WHEN bc.data_conectada IS NOT NULL 
                                     AND TRIM(COALESCE(CAST(bc.data_conectada AS TEXT), '')) != ''
                                     AND (SUBSTR(TRIM(CAST(bc.data_conectada AS TEXT)), 5, 1) = '-' OR LENGTH(TRIM(CAST(bc.data_conectada AS TEXT))) >= 10)
                                     AND SUBSTR(TRIM(CAST(bc.data_conectada AS TEXT)), 1, 4) GLOB '[0-9][0-9][0-9][0-9]'
                                THEN date(SUBSTR(TRIM(CAST(bc.data_conectada AS TEXT)), 1, 10))
                                WHEN bc.data_conectada IS NOT NULL 
                                     AND TRIM(COALESCE(CAST(bc.data_conectada AS TEXT), '')) != ''
                                     AND LENGTH(TRIM(CAST(bc.data_conectada AS TEXT))) >= 10
                                     AND SUBSTR(TRIM(CAST(bc.data_conectada AS TEXT)), 3, 1) = '/'
                                     AND SUBSTR(TRIM(CAST(bc.data_conectada AS TEXT)), 6, 1) = '/'
                                THEN date(
                                    SUBSTR(TRIM(CAST(bc.data_conectada AS TEXT)), 7, 4) || '-' || 
                                    SUBSTR(TRIM(CAST(bc.data_conectada AS TEXT)), 4, 2) || '-' || 
                                    SUBSTR(TRIM(CAST(bc.data_conectada AS TEXT)), 1, 2)
                                )
                                ELSE date('1900-01-01')
                            END DESC,
                            pr.id DESC
                    ) AS rn
                    
                FROM base_coverte_prop bc
                LEFT JOIN portabilidade_records pr ON (
                TRIM(COALESCE(CAST(bc.proposta_isize AS TEXT), CAST(bc.codigo_externo AS TEXT), '')) = 
                TRIM(COALESCE(CAST(pr.codigo_externo AS TEXT), ''))
            )
            """ + ("""
            LEFT JOIN relatorio_objetos ro ON (
                    TRIM(COALESCE(CAST(bc.proposta_isize AS TEXT), CAST(bc.codigo_externo AS TEXT), '')) = 
                TRIM(COALESCE(CAST(ro.codigo_externo AS TEXT), ''))
            )
                """ if tem_relatorio_objetos else "") + """
                WHERE bc.proposta_isize IS NOT NULL 
                  AND TRIM(COALESCE(bc.proposta_isize, bc.codigo_externo, '')) != ''
                  -- FILTRO: Apenas últimos 180 dias (data_conectada ou data_venda)
                  AND (
                    (bc.data_conectada IS NULL AND bc.data_venda IS NULL)
                    OR (COALESCE(SUBSTR(TRIM(CAST(bc.data_conectada AS TEXT)), 1, 10), SUBSTR(TRIM(CAST(bc.data_venda AS TEXT)), 1, 10), '9999-12-31')) """ + filtro_data_sql + """
                  )
                  -- FILTRO: Não exibir vendas com status_bilhete like rejeicao sms
                  AND (pr.status_bilhete IS NULL OR pr.status_bilhete NOT LIKE '%rejeicao sms%')
                  -- FILTRO: Apenas vendas com telefone_portado (não nova linha)
                  AND bc.telefone_portado IS NOT NULL 
                  AND TRIM(COALESCE(bc.telefone_portado, '')) != ''
                  -- FILTRO: Excluir entregas canceladas ou extraviadas
                  AND UPPER(TRIM(COALESCE(bc.status_correios, bc.status_loggi, bc.status_entrega_prevista, ''))) NOT IN ('CANCELADA', 'CANCELADO', 'EXTRAVIADA', 'EXTRAVIADO', 'EXTRAVIO', 'BAIXA', 'REMETENTE')
                  AND (
                      -- Registros normais: crivo_vendas = APROVADA com template
                      (
                          UPPER(TRIM(COALESCE(CAST(bc.crivo_vendas AS TEXT), ''))) = 'APROVADA'
                          AND (pr.template IS NOT NULL OR (COALESCE(SUBSTR(TRIM(CAST(bc.data_venda AS TEXT)), 1, 10), '0000-00-00') """ + filtro_data_sql + """))
                      )
                      """ + (f"""
                      -- IDs forçados: incluir independente de crivo_vendas ou template
                      OR TRIM(COALESCE(CAST(bc.proposta_isize AS TEXT), CAST(bc.codigo_externo AS TEXT), '')) IN ({','.join([repr(id) for id in ids_forcar_wpp])})
                      """ if ids_forcar_wpp else "") + """
                  )
            )
            SELECT 
                pr_id AS id,
                cpf,
                numero_acesso,
                numero_ordem,
                codigo_externo,
                tipo_mensagem,
                template,
                regra_id,
                o_que_aconteceu,
                acao_a_realizar,
                cliente_nome,
                telefone_portado,
                data_venda,
                data_conectada,
                plano,
                endereco,
                numero,
                complemento,
                bairro,
                cidade,
                uf,
                cep,
                ponto_referencia,
                crivo_vendas,
                status_entrega,
                nu_pedido,
                rastreio,
                ro_status,
                total_classificacoes_raw AS total_classificacoes,
                CASE WHEN total_classificacoes_raw > 1 THEN 'SIM' ELSE 'NAO' END AS houve_reclassificacao
            FROM registros_filtrados
            WHERE rn = 1
            ORDER BY 
                CASE 
                    WHEN data_conectada IS NOT NULL 
                         AND TRIM(COALESCE(CAST(data_conectada AS TEXT), '')) != ''
                         AND (SUBSTR(TRIM(CAST(data_conectada AS TEXT)), 5, 1) = '-' OR LENGTH(TRIM(CAST(data_conectada AS TEXT))) >= 10)
                         AND SUBSTR(TRIM(CAST(data_conectada AS TEXT)), 1, 4) GLOB '[0-9][0-9][0-9][0-9]'
                    THEN date(SUBSTR(TRIM(CAST(data_conectada AS TEXT)), 1, 10))
                    WHEN data_conectada IS NOT NULL 
                         AND TRIM(COALESCE(CAST(data_conectada AS TEXT), '')) != ''
                         AND LENGTH(TRIM(CAST(data_conectada AS TEXT))) >= 10
                         AND SUBSTR(TRIM(CAST(data_conectada AS TEXT)), 3, 1) = '/'
                         AND SUBSTR(TRIM(CAST(data_conectada AS TEXT)), 6, 1) = '/'
                    THEN date(
                        SUBSTR(TRIM(CAST(data_conectada AS TEXT)), 7, 4) || '-' || 
                        SUBSTR(TRIM(CAST(data_conectada AS TEXT)), 4, 2) || '-' || 
                        SUBSTR(TRIM(CAST(data_conectada AS TEXT)), 1, 2)
                    )
                    ELSE date('1900-01-01')
                END DESC,
                id DESC
            LIMIT 10000
                """
            else:
                # Fallback: usar apenas portabilidade_records (com deduplicação)
                query = """
            SELECT 
                MAX(id) AS id, 
                MAX(cpf) AS cpf, 
                MAX(numero_acesso) AS numero_acesso, 
                MAX(numero_ordem) AS numero_ordem, 
                codigo_externo,
                MAX(tipo_mensagem) AS tipo_mensagem, 
                MAX(template) AS template, 
                MAX(regra_id) AS regra_id, 
                MAX(o_que_aconteceu) AS o_que_aconteceu, 
                MAX(acao_a_realizar) AS acao_a_realizar,
                NULL AS cliente_nome,
                NULL AS telefone_portado,
                NULL AS data_venda,
                NULL AS plano,
                NULL AS endereco,
                NULL AS numero,
                NULL AS complemento,
                NULL AS bairro,
                NULL AS cidade,
                NULL AS uf,
                NULL AS cep,
                NULL AS ponto_referencia,
                NULL AS crivo_vendas,
                NULL AS ro_nu_pedido,
                NULL AS ro_rastreio,
                NULL AS ro_status,
                COUNT(*) AS total_classificacoes,
                CASE WHEN COUNT(*) > 1 THEN 'SIM' ELSE 'NAO' END AS houve_reclassificacao
            FROM portabilidade_records
            WHERE template IS NOT NULL 
              AND template != ''
              AND template != '-'
              AND mapeado = 1
              AND (status_bilhete IS NULL OR status_bilhete NOT LIKE '%rejeicao sms%')
            GROUP BY codigo_externo
            ORDER BY id DESC
            LIMIT 1000
                """

            cursor.execute(query)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            print(f"    >> [LEGADO] {len(rows)} registros encontrados")
    
    if not rows:
        print("Nenhum registro com template encontrado!")
        return
    
    # [1.1] DEDUPLICAÇÃO: Remover duplicatas por codigo_externo (manter o primeiro)
    print("[1.1] Removendo duplicatas por codigo_externo...")
    registros_unicos = {}
    duplicatas_removidas = 0
    for row in rows:
        row_dict = dict(zip(columns, row))
        codigo_externo = str(row_dict.get('codigo_externo', '')).strip()
        if codigo_externo and codigo_externo not in registros_unicos:
            registros_unicos[codigo_externo] = row
        elif codigo_externo:
            duplicatas_removidas += 1
    
    rows = list(registros_unicos.values())
    if duplicatas_removidas > 0:
        print(f"    >> {duplicatas_removidas} duplicatas removidas")
    print(f"    >> {len(rows)} registros únicos para processamento")
    
    # 2.0 Carregar histórico de envios do Google Sheets
    print("[2.0] Carregando histórico de envios do Google Sheets...")
    historico_envios_df = carregar_historico_envios_gsheet()
    if not historico_envios_df.empty:
        print(f"    >> {len(historico_envios_df)} registros no histórico de envios")
    else:
        print("    >> Histórico de envios não disponível (será ignorado)")
    
    # 3. Processar registros e gerar dados de homologação
    print("[3] Processando registros e gerando preview de mensagens...")
    
    homologacao_data = []
    template_stats = {}
    historico_stats = {'mantidos': 0, 'alterados_para_2': 0, 'bloqueados': 0}
    
    # Tentar carregar Relatório de Objetos para enriquecimento
    print("[2.1] Tentando carregar Relatório de Objetos para enriquecimento...")
    objects_loader = None
    # Usar caminho do config ou caminho local do Mac
    try:
        from config import PASTA_IMPORTACOES
        pasta_importacao = Path(PASTA_IMPORTACOES)
    except ImportError:
        pasta_importacao = Path("/Applications/Documentos/IMPORTACOES_QIGGER")
    arquivo_objetos = None
    if pasta_importacao.exists():
        arquivos_xlsx = list(pasta_importacao.glob("*.xlsx"))
        if arquivos_xlsx:
            arquivo_objetos = max(arquivos_xlsx, key=lambda x: x.stat().st_mtime)
            try:
                objects_loader = ObjectsLoader(str(arquivo_objetos))
                print(f"    >> {objects_loader.total_records} registros de logística carregados")
            except Exception as e:
                print(f"    >> Erro ao carregar: {e}")
    
    # Carregar Base Analítica Final como fonte adicional
    print("[2.2] Tentando carregar Base Analítica Final...")
    base_analitica_loader = None
    # Base analítica agora vem do banco unificado (base_unificada) ou base_coverte_prop
    # Verificar se há dados nas tabelas
    with db_manager._get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM base_unificada")
        total_base = cursor.fetchone()[0]
        tem_base_unificada = total_base > 0
        
        cursor.execute("SELECT COUNT(*) FROM base_coverte_prop")
        total_coverte = cursor.fetchone()[0]
        tem_base_coverte = total_coverte > 0
        
        if tem_base_coverte:
            print(f"    >> {total_coverte:,} registros encontrados na base_coverte_prop (COVERTE BASE PROP)")
    
    if BASE_ANALITICA_PATH and BASE_ANALITICA_PATH.exists() and BASE_ANALITICA_PATH != Path("/dev/null"):
        try:
            base_analitica_loader = BaseAnaliticaLoader(str(BASE_ANALITICA_PATH))
            count = base_analitica_loader.load()
            if count > 0:
                print(f"    >> {count} registros da base analítica carregados")
        except Exception as e:
            print(f"    >> Erro ao carregar base analítica: {e}")
    elif not tem_base_unificada:
        print(f"    >> Base analítica não encontrada (nem no banco nem em arquivo)")
    
    # [2.3] Validar registros usando tabela portabilidade_processamento
    print("[2.3] Validando registros com tabela portabilidade_processamento...")
    try:
        # Converter rows para lista de dicionários
        registros_para_validar = []
        for row in rows:
            row_dict = dict(zip(columns, row))
            registros_para_validar.append(row_dict)
        
        # Filtrar registros válidos
        registros_validos, registros_invalidos = filtrar_registros_validos(
            db_manager, registros_para_validar
        )
        
        # Estatísticas de validação
        stats_validacao = obter_estatisticas_validacao(db_manager)
        print(f"    >> {len(registros_validos)} registros válidos para processamento")
        print(f"    >> {len(registros_invalidos)} registros inválidos (serão ignorados)")
        if stats_validacao['total_registros'] > 0:
            print(f"    >> Estatísticas da tabela portabilidade_processamento:")
            print(f"       - Total: {stats_validacao['total_registros']}")
            print(f"       - Válidos: {stats_validacao['validos']}")
            print(f"       - Com conflito: {stats_validacao['com_conflito']}")
            print(f"       - Com cancelamento: {stats_validacao['com_cancelamento']}")
        
        # Usar apenas registros válidos
        rows_validos = []
        for registro in registros_validos:
            # Reconstruir row no formato original
            row_reconstruido = [registro.get(col, None) for col in columns]
            rows_validos.append(row_reconstruido)
        
        rows = rows_validos
        print(f"    >> Processando {len(rows)} registros válidos")
    except Exception as e:
        logger.warning(f"Erro ao validar registros (continuando sem validação): {e}")
        print(f"    >> Aviso: Validação não pôde ser executada, processando todos os registros")
    
    # [4] Processar registros com barra de progresso
    print("[4] Processando registros e gerando arquivo...")
    total_registros = len(rows)
    
    with ProgressBar(
        total=total_registros,
        desc="Gerando homologação WPP",
        unit="registros",
        logger=logger,
        log_interval_pct=10.0
    ) as pbar:
        for row_idx, row in enumerate(rows, 1):
            # Log de evolução a cada 100 registros (visualização do progresso)
            if row_idx % 100 == 0:
                pct = (row_idx / total_registros * 100) if total_registros > 0 else 0
                logger.info(f"  WPP: {row_idx}/{total_registros} ({pct:.1f}%) | homologados: {len(homologacao_data)} | {datetime.now().strftime('%H:%M:%S')}")
            
            # Converter row para dict usando colunas
            row_dict = dict(zip(columns, row))
            
            # Criar registro básico usando dados sincronizados
            record = PortabilidadeRecord(
                cpf=str(row_dict.get('cpf', '') or '').strip(),
                numero_acesso=str(row_dict.get('numero_acesso', '') or '').strip(),
                numero_ordem=str(row_dict.get('numero_ordem', '') or '').strip(),
                codigo_externo=str(row_dict.get('codigo_externo', '') or '').strip(),
                tipo_mensagem=str(row_dict.get('tipo_mensagem', '') or '').strip(),
                template=str(row_dict.get('template', '') or '').strip(),
                regra_id=row_dict.get('regra_id'),
                o_que_aconteceu=str(row_dict.get('o_que_aconteceu', '') or '').strip(),
                acao_a_realizar=str(row_dict.get('acao_a_realizar', '') or '').strip(),
            )
            
            # Preencher dados adicionais — suporta tanto legado (cliente_nome/telefone_portado)
            # quanto V2 (nome_cliente/numero_acesso/telefone_portabilidade)
            nome_raw = (
                row_dict.get('nome_cliente')
                or row_dict.get('cliente_nome')
                or ''
            )
            if nome_raw:
                record.nome_cliente = str(nome_raw).strip()

            telefone_raw = (
                row_dict.get('telefone_portabilidade')
                or row_dict.get('telefone_portado')
                or row_dict.get('numero_acesso')
                or ''
            )
            if telefone_raw:
                record.telefone_contato = str(telefone_raw).strip()
            if row_dict.get('data_venda'):
                try:
                    from datetime import datetime as dt_parser
                    data_venda_str = str(row_dict['data_venda']).strip()
                    if data_venda_str:
                        # Tentar parsear diferentes formatos
                        for fmt in ['%Y-%m-%d', '%Y-%m-%d %H:%M:%S', '%d/%m/%Y', '%d/%m/%Y %H:%M:%S']:
                            try:
                                record.data_venda = dt_parser.strptime(data_venda_str[:19] if len(data_venda_str) > 19 else data_venda_str, fmt)
                                break
                            except ValueError:
                                continue
                except (ValueError, TypeError, AttributeError):
                    pass
            
            # Preencher dados de endereço diretamente da query SQL (base_coverte_prop)
            numero_bruto = str(row_dict.get('numero', '')).strip() if row_dict.get('numero') else ''
            complemento_bruto = str(row_dict.get('complemento', '')).strip() if row_dict.get('complemento') else ''
            
            # Normalizar número e extrair complementos (sn, n/a, S/N -> 0, "150 B" -> numero=150, complemento=B)
            numero_normalizado, complemento_atualizado = normalizar_numero_endereco(numero_bruto, complemento_bruto)
            
            endereco_data = {
                'endereco': str(row_dict.get('endereco', '')).strip() if row_dict.get('endereco') else '',
                'numero': numero_normalizado,
                'complemento': complemento_atualizado,
                'bairro': str(row_dict.get('bairro', '')).strip() if row_dict.get('bairro') else '',
                'ponto_referencia': str(row_dict.get('ponto_referencia', '')).strip() if row_dict.get('ponto_referencia') else ''
            }
            
            # Preencher cidade, UF e CEP diretamente da query SQL
            if row_dict.get('cidade'):
                record.cidade = str(row_dict['cidade']).strip()
            if row_dict.get('uf'):
                record.uf = str(row_dict['uf']).strip()
            if row_dict.get('cep'):
                record.cep = str(row_dict['cep']).strip()
            
            # Enriquecer com dados de logística - buscar TODOS os matches para garantir endereço completo (se não tiver na query)
            
            obj_match = None
            nu_pedido_encontrado = None
            
            # Buscar dados de logística - usar find_best_match que busca em múltiplas fontes
            if objects_loader:
                # Tentar múltiplas estratégias de busca
                
                # 1. Buscar por código externo direto
                obj_match = objects_loader.find_best_match(
                    codigo_externo=record.codigo_externo,
                    cpf=record.cpf
                )
                
                # 2. Se não encontrou, tentar variações do código externo (lookup O(1) via índice)
                if not obj_match and record.codigo_externo:
                    codigo_variacoes = [
                        record.codigo_externo,
                        record.codigo_externo.zfill(8),
                        record.codigo_externo.zfill(9),
                        record.codigo_externo.lstrip('0'),
                    ]
                    for codigo_var in codigo_variacoes:
                        if codigo_var != record.codigo_externo:
                            obj_match = objects_loader.find_best_match(codigo_externo=codigo_var)
                            if obj_match:
                                break
                    
                    # 3. Buscar por CPF (lista pequena, limitada) - preferir lookup direto
                    if not obj_match and record.cpf and hasattr(objects_loader, '_index_by_cpf'):
                        matches = objects_loader._index_by_cpf.get(record.cpf, [])
                        if matches:
                            codigo_target = str(record.codigo_externo).strip().lstrip('0')
                            for match in matches[:20]:  # Limitar a 20 primeiros (mais recentes)
                                codigo_match = str(getattr(match, 'codigo_externo', '')).strip().lstrip('0')
                                if codigo_match == codigo_target or (
                                    codigo_match and codigo_target and
                                    (codigo_match.endswith(codigo_target[-6:]) or codigo_target.endswith(codigo_match[-6:]))
                                ):
                                    obj_match = match
                                    break
                            if not obj_match and matches:
                                obj_match = matches[0]
                
                # 5. Se ainda não encontrou e temos número de acesso, tentar buscar por ID ERP
                if not obj_match and record.numero_acesso:
                    obj_match = objects_loader.find_by_id_erp(record.numero_acesso)
                
                # Se encontrou, preencher TODOS os dados
                if obj_match:
                    # ObjectRecord usa 'destinatario' não 'nome_cliente'
                    record.nome_cliente = getattr(obj_match, 'destinatario', None) or getattr(obj_match, 'nome_cliente', None) or record.nome_cliente or ""
                    record.telefone_contato = getattr(obj_match, 'telefone', None) or getattr(obj_match, 'telefone_contato', None) or record.telefone_contato or ""
                    record.cidade = getattr(obj_match, 'cidade', None) or record.cidade or ""
                    record.uf = getattr(obj_match, 'uf', None) or record.uf or ""
                    record.cep = getattr(obj_match, 'cep', None) or record.cep or ""
                    record.data_venda = getattr(obj_match, 'data_criacao_pedido', None) or getattr(obj_match, 'data_venda', None) or record.data_venda
                    record.status_logistica = getattr(obj_match, 'status', None) or getattr(obj_match, 'status_logistica', None) or record.status_logistica or ""
                    
                    # Dados de endereço do ObjectRecord (normalizar número)
                    endereco_data['endereco'] = getattr(obj_match, 'endereco', '') or endereco_data['endereco'] or ''
                    
                    # Normalizar número do ObjectRecord
                    numero_obj = getattr(obj_match, 'numero', '') or ''
                    complemento_obj = getattr(obj_match, 'complemento', '') or ''
                    if numero_obj and not endereco_data['numero']:
                        numero_norm, compl_norm = normalizar_numero_endereco(numero_obj, complemento_obj)
                        endereco_data['numero'] = numero_norm
                        if compl_norm and not endereco_data['complemento']:
                            endereco_data['complemento'] = compl_norm
                    elif complemento_obj and not endereco_data['complemento']:
                        endereco_data['complemento'] = complemento_obj
                    
                    endereco_data['bairro'] = getattr(obj_match, 'bairro', '') or endereco_data['bairro'] or ''
                    endereco_data['ponto_referencia'] = getattr(obj_match, 'ponto_referencia', '') or endereco_data['ponto_referencia'] or ''
                    
                    # Buscar nu_pedido para usar no link de rastreio
                    nu_pedido = getattr(obj_match, 'nu_pedido', None)
                    if nu_pedido:
                        nu_pedido_str = str(nu_pedido).strip()
                        if nu_pedido_str and not nu_pedido_str.startswith('http'):
                            if '-' in nu_pedido_str and nu_pedido_str.startswith('26-'):
                                nu_pedido_encontrado = nu_pedido_str
                            elif '-' in nu_pedido_str:
                                partes = nu_pedido_str.split('-', 1)
                                if len(partes) > 1:
                                    nu_pedido_encontrado = f"26-{partes[1].zfill(8)}"
                            else:
                                nu_pedido_encontrado = f"26-{nu_pedido_str.zfill(8)}"
            
            # Buscar data de conexão no banco de dados (data_inicial_processamento ou Data Conectada)
            data_conexao = None
            if not record.data_venda:
                with db_manager._get_connection() as conn:
                    cursor = conn.cursor()
                    # Buscar data_inicial_processamento (Data Conectada)
                    cursor.execute("""
                        SELECT data_inicial_processamento, data_portabilidade 
                        FROM portabilidade_records 
                        WHERE codigo_externo = ? 
                        LIMIT 1
                    """, (record.codigo_externo,))
                    result = cursor.fetchone()
                    if result:
                        data_conexao = result[0] or result[1]  # data_inicial_processamento (Data Conectada) ou data_portabilidade
                        if data_conexao:
                            from datetime import datetime as dt_parser
                            if isinstance(data_conexao, str):
                                try:
                                    data_conexao = dt_parser.strptime(data_conexao, '%Y-%m-%d %H:%M:%S')
                                except ValueError:
                                    try:
                                        data_conexao = dt_parser.strptime(data_conexao, '%Y-%m-%d')
                                    except ValueError:
                                        data_conexao = None
                            record.data_venda = data_conexao
            
            # Buscar dados na base_coverte_prop (COVERTE BASE PROP) se disponível
            base_coverte_match = None
            if tem_base_coverte:
                with db_manager._get_connection() as conn:
                    cursor = conn.cursor()
                    # Buscar por CPF, codigo_externo ou numero_ordem
                    cursor.execute("""
                        SELECT * FROM base_coverte_prop
                        WHERE (cpf = ? OR codigo_externo = ? OR numero_ordem = ?)
                        LIMIT 1
                    """, (record.cpf or '', record.codigo_externo or '', record.numero_ordem or ''))
                    row = cursor.fetchone()
                    if row:
                        # Converter para dict usando nomes das colunas
                        col_names = [desc[0] for desc in cursor.description]
                        base_coverte_match = dict(zip(col_names, row))
        
            # Sempre buscar na Base Analítica Final para preencher endereços e dados faltantes
            base_match = None
            if base_analitica_loader and base_analitica_loader.is_loaded:
                # Buscar sempre (mesmo que já tenha alguns dados, pode ter endereço completo)
                base_match = base_analitica_loader.find_best_match(
                    codigo_externo=record.codigo_externo,
                    cpf=record.cpf
                )
            
            # Priorizar base_coverte_prop se disponível (é a fonte mais atual)
            if base_coverte_match:
                base_match = base_coverte_match
            
            if base_match:
                # Preencher dados que estão faltando
                # Mapear colunas da base analítica ou base_coverte_prop
                if not record.nome_cliente:
                    # Tentar diferentes nomes de coluna
                    nome = (base_match.get('Cliente') or 
                           base_match.get('cliente_nome') or
                           base_match.get('Destinatário') or
                           base_match.get('destinatario'))
                    if nome and (not isinstance(nome, float) or not pd.isna(nome)):
                        record.nome_cliente = str(nome).strip()
                
                # Preencher telefone da Base Analítica com PRIORIDADE ESPECÍFICA:
                # 1. PRIMEIRO: "Telefone Portabilidade" (se não vazio)
                # 2. SE VAZIO: DDD + Telefone normalizado (31988776655)
                
                telefone_final = None
                
                # PRIORIDADE 1: Telefone Portabilidade
                telefone_portabilidade = (base_match.get('Telefone Portabilidade') or 
                                         base_match.get('telefone_portado') or
                                         base_match.get('telefone_portabilidade'))
                if telefone_portabilidade and (not isinstance(telefone_portabilidade, float) or not pd.isna(telefone_portabilidade)):
                    telefone_str = str(telefone_portabilidade).strip()
                    # Remover ponto decimal se for número float
                    if telefone_str.endswith('.0'):
                        telefone_str = telefone_str[:-2]
                    if telefone_str:
                        telefone_final = telefone_str
                
                # PRIORIDADE 2: Se Telefone Portabilidade estiver vazio, usar DDD + Telefone
                if not telefone_final:
                    # Buscar DDD
                    ddd = None
                    for col_name in ['DDD', 'DDD.1']:
                        ddd_val = base_match.get(col_name)
                        if pd.notna(ddd_val) and ddd_val:
                            ddd_str = str(ddd_val).strip()
                            # Remover ponto decimal se for número float
                            if ddd_str.endswith('.0'):
                                ddd_str = ddd_str[:-2]
                            if ddd_str:
                                ddd = ddd_str
                                break
                    
                    # Buscar Telefone (não portabilidade)
                    telefone_normal = None
                    for col_name in ['Telefone', 'Telefone.1']:
                        telefone_val = base_match.get(col_name)
                        if pd.notna(telefone_val) and telefone_val:
                            telefone_str = str(telefone_val).strip()
                            # Remover ponto decimal se for número float
                            if telefone_str.endswith('.0'):
                                telefone_str = telefone_str[:-2]
                            if telefone_str:
                                telefone_normal = telefone_str
                                break
                    
                    # Combinar DDD + Telefone se ambos existirem
                    if ddd and telefone_normal:
                        # Limpar caracteres não numéricos
                        ddd_digitos = ''.join(filter(str.isdigit, ddd))
                        telefone_digitos = ''.join(filter(str.isdigit, telefone_normal))
                        
                        # Combinar: DDD + Telefone
                        telefone_combinado = ddd_digitos + telefone_digitos
                        telefone_final = telefone_combinado
                    elif telefone_normal:
                        # Se só tem telefone sem DDD, usar apenas o telefone
                        telefone_final = ''.join(filter(str.isdigit, telefone_normal))
                
                # Se encontrou telefone, normalizar e atribuir
                if telefone_final:
                    # Limpar caracteres não numéricos
                    telefone_limpo = ''.join(filter(str.isdigit, telefone_final))
                    # Normalizar telefone (garantir 11 dígitos)
                    record.telefone_contato = normalizar_telefone(telefone_limpo)
                
                if not record.cidade:
                    cidade = base_match.get('Cidade')
                    if pd.notna(cidade) and cidade:
                        record.cidade = str(cidade).strip()
                
                if not record.uf:
                    uf = base_match.get('UF')
                    if pd.notna(uf) and uf:
                        record.uf = str(uf).strip()
                
                if not record.cep:
                    cep = base_match.get('Cep') or base_match.get('CEP') or base_match.get('Cep')
                    if pd.notna(cep) and cep:
                        record.cep = str(cep).strip()
                
                # Buscar Data Conectada da base analítica
                if not record.data_venda:
                    data_conectada = base_match.get('Data Conectada') or base_match.get('Data_Conectada') or base_match.get('Data Conectada')
                    if pd.notna(data_conectada) and data_conectada:
                        try:
                            from datetime import datetime as dt_parser
                            if isinstance(data_conectada, str):
                                try:
                                    record.data_venda = dt_parser.strptime(data_conectada, '%d/%m/%Y')
                                except ValueError:
                                    try:
                                        record.data_venda = dt_parser.strptime(data_conectada, '%Y-%m-%d')
                                    except ValueError:
                                        pass
                            elif hasattr(data_conectada, 'to_pydatetime'):
                                record.data_venda = data_conectada.to_pydatetime()
                        except (ValueError, TypeError, AttributeError):
                            pass
                
                # Preencher dados de endereço da Base Analítica (sempre, mesmo se já tiver algum dado)
                # Endereco
                endereco = base_match.get('Endereco') or base_match.get('Endereço') or base_match.get('Endereco')
                if pd.notna(endereco) and endereco and str(endereco).strip():
                    endereco_data['endereco'] = str(endereco).strip()
                
                # Numero e Complemento (normalizar)
                numero = base_match.get('Numero') or base_match.get('Número') or base_match.get('Numero')
                complemento = base_match.get('Complemento')
                complemento_str = str(complemento).strip() if pd.notna(complemento) and complemento else ''
                
                if pd.notna(numero) and numero and str(numero).strip():
                    numero_str = str(numero).strip()
                    # Normalizar número e extrair complementos
                    numero_norm, compl_norm = normalizar_numero_endereco(numero_str, complemento_str)
                    endereco_data['numero'] = numero_norm
                    if compl_norm:
                        endereco_data['complemento'] = compl_norm
                elif complemento_str:
                    endereco_data['complemento'] = complemento_str
                
                # Bairro
                bairro = base_match.get('Bairro')
                if pd.notna(bairro) and bairro and str(bairro).strip():
                    endereco_data['bairro'] = str(bairro).strip()
                
                # Ponto_Referencia
                ponto_ref = base_match.get('Ponto Referencia') or base_match.get('Ponto_Referencia') or base_match.get('Ponto Referência')
                if pd.notna(ponto_ref) and ponto_ref and str(ponto_ref).strip():
                    endereco_data['ponto_referencia'] = str(ponto_ref).strip()
                
                if not record.data_venda:
                    data = base_match.get('Data venda') or base_match.get('Data Conectada')
                    if pd.notna(data) and data:
                        try:
                            from datetime import datetime as dt_parser
                            if isinstance(data, str):
                                try:
                                    record.data_venda = dt_parser.strptime(data, '%d/%m/%Y')
                                except ValueError:
                                    try:
                                        record.data_venda = dt_parser.strptime(data, '%Y-%m-%d')
                                    except ValueError:
                                        pass
                            elif hasattr(data, 'to_pydatetime'):
                                record.data_venda = data.to_pydatetime()
                        except (ValueError, TypeError, AttributeError):
                            pass
                
                # Buscar nu_pedido na base analítica se ainda não encontramos
                # A base analítica não tem nu_pedido diretamente, mas podemos usar o código externo
                # O nu_pedido já foi buscado do ObjectsLoader se disponível
            
            # Formatar link de rastreio completo
            if nu_pedido_encontrado:
                # Usar o nu_pedido que já encontramos
                link_rastreio = f"https://tim.trakin.co/o/{nu_pedido_encontrado}"
            else:
                # Se não encontrou, formatar usando código externo
                codigo_limpo = str(record.codigo_externo).strip().lstrip('0')
                if not codigo_limpo:
                    codigo_limpo = "0"
                numero_formatado = codigo_limpo.zfill(8)
                nu_pedido_fallback = f"26-{numero_formatado}"
                link_rastreio = f"https://tim.trakin.co/o/{nu_pedido_fallback}"
            
            # Preparar dados completos para o template (incluindo endereço)
            record_data_completo = {
            "nome_cliente": extrair_primeiro_ultimo_nome(record.nome_cliente or ""),
            "cod_rastreio": link_rastreio,  # Link completo
            "endereco": endereco_data['endereco'],
            "numero": endereco_data['numero'],
            "complemento": endereco_data['complemento'],
            "bairro": endereco_data['bairro'],
            "cidade": record.cidade or "",
            "uf": record.uf or "",
            "cep": record.cep or "",
                "ponto_referencia": endereco_data['ponto_referencia'],
            }
            
            # Obter informações do template
            # Para V2: tipo_mensagem é NULL → derivar a partir de acao_a_realizar + status
            template_info = TemplateMapper.get_template_for_record(record)
            template_id = template_info.get('template_id')

            if not template_id and _usou_v2:
                # Derivar IDCorpo a partir dos campos V2
                template_id = derivar_template_v2(row_dict)
                if template_id == 0:
                    pbar.update(1)
                    continue
                # WPP apenas para clientes com número portado
                # No V2: numero_acesso = bu.telefone_portabilidade (número portado)
                tel_port = str(row_dict.get('numero_acesso') or '').strip()
                if not tel_port or tel_port in ('', '-', '00000000000', 'None', 'null'):
                    pbar.update(1)
                    continue
                # Atualizar record com o template derivado
                record.tipo_mensagem = str(template_id)
                record.template = str(template_id)

            if not template_id:
                pbar.update(1)
                continue
            
            # Estatísticas
            template_stats[template_id] = template_stats.get(template_id, 0) + 1
            
            # Obter configuração do template
            template_config = TEMPLATES.get(template_id)
            if not template_config:
                pbar.update(1)
                continue
        
            # Gerar variáveis com dados completos
            variaveis_dict = TemplateMapper.generate_variables(template_id, record_data_completo)
            
            # Obter corpo da mensagem do banco
            corpo_mensagem = obter_corpo_mensagem_template(db_manager, template_id)
            
            # Substituir variáveis na mensagem
            mensagem_preview = substituir_variaveis_mensagem(corpo_mensagem, variaveis_dict)
            
            # Formatar variáveis para exibição
            variaveis_str = TemplateMapper.format_variables_string(variaveis_dict)
            
            # Extrair primeiro e último nome
            nome_completo = record.nome_cliente or ''
            nome_cliente_formatado = extrair_primeiro_ultimo_nome(nome_completo)
            
            # Normalizar telefone (11 dígitos)
            # Buscar telefone de múltiplas fontes
            telefone_origem = record.telefone_contato or record.numero_acesso or ""
            telefone_contato = normalizar_telefone(telefone_origem)
            
            # Normalizar CEP (8 dígitos)
            cep_normalizado = normalizar_cep(record.cep or "")
            
            # Normalizar Data_Venda (DD/MM/AAAA) - usar Data Conectada
            data_venda_formatada = normalizar_data_venda(record.data_venda)
            
            # Tipo_Comunicacao: usar Template_Triggers, substituir "EM CRIAÇÃO" por "1"
            # Para V2: record.template já foi setado com o template_id derivado
            template_triggers = record.template or ''
            tipo_comunicacao = template_triggers
            if not tipo_comunicacao and _usou_v2:
                # V2 sem template original — usar o template_id derivado
                tipo_comunicacao = str(template_id)
                template_triggers = str(template_id)
            if tipo_comunicacao.upper() in ['EM CRIAÇÃO', 'EM CRIACAO', 'EM_CRIACAO']:
                tipo_comunicacao = '1'
            
            # Converter tipo_comunicacao para int para verificação de histórico
            try:
                tipo_comunicacao_int = int(tipo_comunicacao) if str(tipo_comunicacao).isdigit() else 1
            except (ValueError, TypeError):
                tipo_comunicacao_int = 1
            
            # Verificar histórico de envios e ajustar Tipo_Comunicacao
            if not historico_envios_df.empty:
                tipo_comunicacao_ajustado = verificar_historico_cliente(
                    proposta_isize=record.codigo_externo,
                    cpf=record.cpf,
                    tipo_comunicacao_novo=tipo_comunicacao_int,
                    historico_df=historico_envios_df
                )
                
                # Contabilizar alterações
                if tipo_comunicacao_ajustado == 0:
                    historico_stats['bloqueados'] += 1
                elif tipo_comunicacao_ajustado == 2 and tipo_comunicacao_int == 1:
                    historico_stats['alterados_para_2'] += 1
                else:
                    historico_stats['mantidos'] += 1
                
                tipo_comunicacao = str(tipo_comunicacao_ajustado)
            
            # Extrair contagens e flags de reclassificação da query
            total_classificacoes = row_dict.get('total_classificacoes', 1)
            try:
                total_classificacoes = int(total_classificacoes) if total_classificacoes else 1
            except (ValueError, TypeError):
                total_classificacoes = 1
            
            houve_reclassificacao = row_dict.get('houve_reclassificacao', 'NAO')
            if not houve_reclassificacao or houve_reclassificacao == 'NAO':
                houve_reclassificacao = 'NAO'
            else:
                houve_reclassificacao = 'SIM'
            
            # Contagem de tentativas (baseado no histórico de envios - apenas envios efetivos tipo 1 ou 2)
            tentativas = 0
            if not historico_envios_df.empty and record.codigo_externo:
                # Buscar registros do cliente
                mask = (
                    (historico_envios_df.get('Proposta_iSize', pd.Series()).astype(str).str.strip() == str(record.codigo_externo).strip()) |
                    (historico_envios_df.get('Cpf', pd.Series()).astype(str).str.strip() == str(record.cpf).strip())
                )
                registros_cliente = historico_envios_df[mask].copy()
                
                if not registros_cliente.empty:
                    # Contar apenas envios efetivos (tipo 1 ou 2)
                    if 'Tipo_Comunicacao' in registros_cliente.columns:
                        tipos = pd.to_numeric(registros_cliente['Tipo_Comunicacao'], errors='coerce').fillna(0)
                        tentativas = ((tipos == 1) | (tipos == 2)).sum()
            
            # Normalizar Data_Conectada
            data_conectada_formatada = ''
            if row_dict.get('data_conectada'):
                try:
                    data_conectada_raw = str(row_dict['data_conectada']).strip()
                    if data_conectada_raw:
                        # Tentar parsear diferentes formatos
                        for fmt in ['%Y-%m-%d', '%Y-%m-%d %H:%M:%S', '%d/%m/%Y', '%d/%m/%Y %H:%M:%S']:
                            try:
                                dt = datetime.strptime(data_conectada_raw[:19] if len(data_conectada_raw) > 19 else data_conectada_raw, fmt)
                                data_conectada_formatada = dt.strftime('%d/%m/%Y')
                                break
                            except ValueError:
                                continue
                        if not data_conectada_formatada:
                            data_conectada_formatada = data_conectada_raw[:10]  # Usar primeiros 10 caracteres
                except Exception:
                    pass
            
            # Ordem das colunas principais (conforme especificado para Google Sheets)
            row_data = {
                'Proposta_iSize': record.codigo_externo or '',
                'Cpf': record.cpf or '',
                'NomeCliente': nome_cliente_formatado,
                'Telefone_Contato': telefone_contato,
                'Endereco': endereco_data['endereco'] or '',
                'Numero': endereco_data['numero'] or '',
                'Complemento': endereco_data['complemento'] or '',
                'Bairro': endereco_data['bairro'] or '',
                'Cidade': record.cidade or '',
                'UF': record.uf or '',
                'Cep': cep_normalizado,
                'Ponto_Referencia': endereco_data['ponto_referencia'] or '',
                'Cod_Rastreio': link_rastreio or '',
                'Data_Venda': data_venda_formatada,
                'Tipo_Comunicacao': tipo_comunicacao,
                'Status_Disparo': 'FALSE',
                'DataHora_Disparo': '',
            }
            
            # Colunas de homologação ao final (conforme solicitado)
            row_data.update({
                'Template_Triggers': template_triggers,
                'O_Que_Aconteceu': record.o_que_aconteceu or '',
                'Tentativas': tentativas,
                'Total_Classificacoes': total_classificacoes,
                'Houve_Reclassificacao': houve_reclassificacao,
                'Acao_Realizar': record.acao_a_realizar or '',
            })
            
            homologacao_data.append(row_data)
            
            # Atualizar barra de progresso
            pbar.update(1)
            pbar.set_postfix(
                processados=row_idx,
                total=total_registros,
                homologacao=len(homologacao_data)
            )
    
    # Deduplicação por (cpf, telefone): manter apenas o mais recente (já vem ordenado pela query)
    vistos_cpf_tel = set()
    homologacao_dedup = []
    for row_data in homologacao_data:
        chave = (str(row_data.get('Cpf') or '').strip(), str(row_data.get('Telefone_Contato') or '').strip())
        if chave in vistos_cpf_tel:
            continue
        vistos_cpf_tel.add(chave)
        homologacao_dedup.append(row_data)
    if len(homologacao_data) != len(homologacao_dedup):
        print(f"    >> Deduplicação (cpf + telefone): {len(homologacao_data)} → {len(homologacao_dedup)} registros")
    homologacao_data = homologacao_dedup

    # [4.1] Sanitizar valores (None/NULL/NaN → string vazia)
    for row_data in homologacao_data:
        for key in row_data:
            if isinstance(row_data[key], str):
                row_data[key] = sanitizar_valor(row_data[key])
            elif row_data[key] is None:
                row_data[key] = ''

    # 5. Salvar arquivo de homologação
    print("\n[5] Salvando arquivo de homologação...")
    
    OUTPUT_HOMOLOGACAO.parent.mkdir(parents=True, exist_ok=True)
    
    colunas_principais = [
        'Proposta_iSize', 'Cpf', 'NomeCliente', 'Telefone_Contato',
        'Endereco', 'Numero', 'Complemento', 'Bairro', 'Cidade', 'UF', 'Cep', 'Ponto_Referencia',
        'Cod_Rastreio', 'Data_Venda', 'Tipo_Comunicacao',
        'Status_Disparo', 'DataHora_Disparo'
    ]
    colunas_homologacao = [
        'Template_Triggers', 'O_Que_Aconteceu', 'Tentativas', 'Total_Classificacoes',
        'Houve_Reclassificacao', 'Acao_Realizar'
    ]
    fieldnames = colunas_principais + colunas_homologacao

    output_path = OUTPUT_HOMOLOGACAO
    try:
        df = pd.DataFrame(homologacao_data, columns=fieldnames) if homologacao_data else pd.DataFrame(columns=fieldnames)
        df.to_excel(output_path, index=False, engine='openpyxl', sheet_name='WPP')
    except PermissionError:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = OUTPUT_HOMOLOGACAO.parent / f"homologacao_wpp_{timestamp}.xlsx"
        print(f"    >> Arquivo original está aberto, salvando como: {output_path.name}")
        df = pd.DataFrame(homologacao_data, columns=fieldnames) if homologacao_data else pd.DataFrame(columns=fieldnames)
        df.to_excel(output_path, index=False, engine='openpyxl', sheet_name='WPP')

    print(f"    >> Arquivo salvo em: {output_path}")
    
    # 5. Estatísticas
    print()
    print("=" * 70)
    print("ESTATÍSTICAS DE HOMOLOGAÇÃO")
    print("=" * 70)
    print(f"  Total de registros: {len(homologacao_data)}")
    print()
    print("  Por Template:")
    for template_id, count in sorted(template_stats.items()):
        config = TEMPLATES.get(template_id)
        nome = config.nome_modelo if config else f"Template {template_id}"
        print(f"    Template {template_id} ({nome}): {count} registros")
    
    # Estatísticas de verificação de histórico
    if not historico_envios_df.empty:
        print()
        print("  Verificação de Histórico (Google Sheets):")
        print(f"    Registros mantidos: {historico_stats['mantidos']}")
        print(f"    Alterados de 1→2 (reenvio): {historico_stats['alterados_para_2']}")
        print(f"    Bloqueados (≥{MAX_ENVIOS_POR_CLIENTE} envios): {historico_stats['bloqueados']}")
        
        # Contar quantos registros por tipo final
        tipos_finais = {}
        for row in homologacao_data:
            tipo = row.get('Tipo_Comunicacao', '1')
            tipos_finais[tipo] = tipos_finais.get(tipo, 0) + 1
        
        print()
        print("  Por Tipo_Comunicacao final:")
        for tipo, count in sorted(tipos_finais.items()):
            status = ""
            if tipo == '0':
                status = "(BLOQUEADO - não enviar)"
            elif tipo == '1':
                status = "(primeira comunicação)"
            elif tipo == '2':
                status = "(segunda comunicação)"
            print(f"    Tipo {tipo}: {count} registros {status}")
    
    print()
    print("-" * 70)
    print("INFORMAÇÕES DO ARQUIVO")
    print("-" * 70)
    print(f"  Arquivo: {output_path}")
    print(f"  Total de linhas: {len(homologacao_data) + 1} (incluindo cabeçalho)")
    print(f"  Formato: CSV com delimitador ';'")
    print(f"  Encoding: UTF-8 com BOM (utf-8-sig)")
    print()
    print("  Colunas incluídas:")
    print("    - Dados do Cliente (CPF, Nome, Telefone, Endereço)")
    print("    - Dados da Proposta (Proposta_iSize, Cod_Rastreio)")
    print("    - Template (ID, Nome, Categoria, Cabeçalho)")
    print("    - Variáveis do Template (formatadas)")
    print("    - Preview da Mensagem (com variáveis substituídas)")
    print("    - Botão (se houver)")
    print("    - Status de Disparo (sempre FALSE)")
    print()
    print("=" * 70)
    print("HOMOLOGAÇÃO GERADA COM SUCESSO!")
    print("=" * 70)
    print()
    print("PRÓXIMOS PASSOS:")
    print("  1. Abra o arquivo CSV gerado")
    print("  2. Revise a coluna 'Mensagem_Preview' para validar as mensagens")
    print("  3. Verifique se as variáveis foram substituídas corretamente")
    print("  4. Valide os dados do cliente e links de rastreio")
    print("  5. Após homologação, o arquivo pode ser usado para envio real")
    print("=" * 70)


if __name__ == "__main__":
    try:
        gerar_arquivo_homologacao()
    except KeyboardInterrupt:
        print("\nProcessamento interrompido pelo usuário.")
        sys.exit(1)
    except Exception as e:
        print(f"ERRO FATAL: {e}")
        logger.error(f"Erro fatal: {e}", exc_info=True)
        import traceback
        traceback.print_exc()
        sys.exit(1)

