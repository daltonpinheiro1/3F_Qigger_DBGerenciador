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
from typing import Dict, Optional
import pandas as pd

# Caminhos (usar config centralizado)
try:
    from config import DB_PATH, OUTPUT_WPP, PASTA_SAIDA_HOMOLOGACAO
    OUTPUT_HOMOLOGACAO = Path(OUTPUT_WPP)
    # Base analítica agora vem do banco unificado (base_unificada) ou Excel processado
    # Não precisa mais do arquivo CSV separado
    BASE_ANALITICA_PATH = Path("/dev/null")  # Placeholder que nunca existe
except ImportError:
    # Fallback se config.py não existir - usar caminho absoluto
    DB_PATH = str(Path(__file__).parent / "data" / "portabilidade.db")
    OUTPUT_HOMOLOGACAO = Path("/Applications/Documentos/Projetos_python/Retornos do gerenciador/homologacao_wpp.csv")
    BASE_ANALITICA_PATH = Path("/dev/null")  # Placeholder que nunca existe

# Garantir que pasta de saída existe
OUTPUT_HOMOLOGACAO.parent.mkdir(parents=True, exist_ok=True)

# Palavras a ignorar ao extrair primeiro e último nome
PALAVRAS_IGNORAR = {'e', 'de', 'da', 'do', 'das', 'dos', 'em', 'na', 'no', 'nas', 'nos'}

# =============================================================================
# CONFIGURAÇÃO DO GOOGLE SHEETS - HISTÓRICO DE ENVIOS
# =============================================================================
GOOGLE_SHEET_ID = '13qXylcL-wYbB4vDouI4d2rRazYQvaPEZneEmLx-lVtk'
GOOGLE_SHEET_EXPORT_URL = f'https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/export?format=csv'
MAX_ENVIOS_POR_CLIENTE = 5  # Máximo de envios tipo 1 ou 2 antes de bloquear

# =============================================================================
# CONFIGURAÇÃO DE CONTROLE DE ENVIOS
# =============================================================================
MAX_TENTATIVAS_TEMPLATE = 5  # Máximo de tentativas por template 1 ou 2
HORAS_ENTRE_ENVIOS = 24  # Horas mínimas entre envios para o mesmo cliente


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
    1. Se último Tipo_Comunicacao = 1 e novo = 1 → retorna 2
    2. Se total de envios com tipo 1 ou 2 >= 5 → retorna 0 (não enviar)
    3. Caso contrário, mantém o tipo original
    
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
    
    registros_cliente = historico_df[mask]
    
    if registros_cliente.empty:
        # Cliente nunca recebeu mensagem
        return tipo_comunicacao_novo
    
    # Contar envios com tipo 1 ou 2
    if 'Tipo_Comunicacao' in registros_cliente.columns:
        # Converter para numérico, tratando erros
        tipos = pd.to_numeric(registros_cliente['Tipo_Comunicacao'], errors='coerce').fillna(0)
        
        # Contar envios efetivos (tipo 1 ou 2)
        total_envios_efetivos = ((tipos == 1) | (tipos == 2)).sum()
        
        # Regra 2: Se já enviamos 5 vezes, não enviar mais
        if total_envios_efetivos >= MAX_ENVIOS_POR_CLIENTE:
            logger.debug(f"[GSHEET] Cliente {proposta_str or cpf_str}: {total_envios_efetivos} envios → Tipo 0 (bloqueado)")
            return 0
        
        # Pegar último tipo de comunicação
        ultimo_tipo = tipos.iloc[-1] if len(tipos) > 0 else 0
        
        # Regra 1: Se último foi 1 e novo é 1, classificar como 2
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


# Mensagens padrão dos templates (caso não estejam no banco)
MENSAGENS_PADRAO = {
    1: """Olá. A sua solicitação de portabilidade para a TIM foi processada com sucesso.
Para autorizar o envio do chip e a continuidade do processo, é necessária a confirmação do titular.
Realize a validação de uma das formas abaixo:
1. Toque no botão Confirmar Solicitação; ou
2. Envie SMS com a palavra SIM para o número 7678.
Dados da Entrega:
* Prazo estimado: Até 10 dias úteis.
* Recebimento: Necessário maior de 18 anos com documento.
* Observação: O chip será entregue com número provisório até a conclusão da portabilidade.
Status: Aguardando confirmação.""",
    2: """Olá. Verificamos uma pendência na etapa de validação da sua portabilidade numérica.
Para concluir o processo técnico de transferência da linha, é necessário o envio do comando de confirmação via SMS a partir do seu chip atual.
Instruções para regularização:
1. Envie a palavra PORTABILIDADE para o número 7678; ou
2. Utilize o atalho no botão abaixo para gerar o SMS automaticamente.
O não envio do comando pode ocasionar a suspensão da solicitação.
Status: Aguardando validação via SMS.""",
    3: """Olá, {{1}}. O seu pedido encontra-se disponível para retirada.
Para concluir a entrega, compareça à agência dos Correios indicada portando documento de identificação original com foto.
Status Atual: Objeto aguardando retirada
Código de Rastreio: {{2}}
Utilize o botão abaixo para consultar o endereço exato da agência.""",
    4: """Olá, {{1}}. A portabilidade da sua linha foi processada.
Para iniciarmos a logística de entrega do chip, valide se o endereço cadastrado está atualizado:
Endereço de Destino:
Rua: {{2}}, Nº {{3}}; Complemento: {{4}};
Bairro: {{5}}
Cidade: {{6}}
UF: {{7}};
CEP: {{8}}.
Ponto de Referência: {{9}};
A exatidão dos dados é essencial para evitar devoluções. O endereço acima está correto?""",
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
    
    # 2. Buscar registros com template sincronizando todas as tabelas do portabilidade.db
    print("[2] Buscando registros com template mapeado (sincronizando todas as tabelas)...")
    
    with db_manager._get_connection() as conn:
        cursor = conn.cursor()
        
        # Verificar quais tabelas existem
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tabelas_existentes = [row[0] for row in cursor.fetchall()]
        tem_base_coverte = 'base_coverte_prop' in tabelas_existentes
        tem_relatorio_objetos = 'relatorio_objetos' in tabelas_existentes
        
        if tem_base_coverte:
            print("    >> Usando base_coverte_prop + portabilidade_records" + (" + relatorio_objetos" if tem_relatorio_objetos else ""))
        else:
            print("    >> Tabela base_coverte_prop não encontrada, usando apenas portabilidade_records")
        
        # Buscar registros ÚNICOS por codigo_externo, priorizando base_coverte_prop como fonte principal
        # Remove duplicados usando GROUP BY garantindo um único registro por codigo_externo
        # FILTRO: Apenas clientes com crivo_vendas = "APROVADA"
        if tem_base_coverte:
            query = """
            SELECT 
                -- Dados de portabilidade_records
                MAX(pr.id) AS id, 
                COALESCE(MAX(bc.cpf), MAX(pr.cpf)""" + (", MAX(ro.documento)" if tem_relatorio_objetos else "") + """, '') AS cpf,
                COALESCE(MAX(pr.numero_acesso), '') AS numero_acesso,
                COALESCE(MAX(bc.numero_ordem), MAX(pr.numero_ordem), '') AS numero_ordem,
                COALESCE(bc.proposta_isize, bc.codigo_externo, pr.codigo_externo""" + (", ro.codigo_externo" if tem_relatorio_objetos else "") + """, '') AS codigo_externo,
                
                -- Template e regras (priorizar dados existentes)
                COALESCE(MAX(pr.tipo_mensagem), '') AS tipo_mensagem,
                COALESCE(MAX(pr.template), '1') AS template,
                MAX(pr.regra_id) AS regra_id,
                COALESCE(MAX(pr.o_que_aconteceu), '') AS o_que_aconteceu,
                COALESCE(MAX(pr.acao_a_realizar), '') AS acao_a_realizar,
                
                -- Dados adicionais de base_coverte_prop (FONTE PRINCIPAL)
                MAX(bc.cliente_nome) AS cliente_nome,
                MAX(bc.telefone_portado) AS telefone_portado,
                MAX(bc.data_venda) AS data_venda,
                MAX(bc.plano) AS plano,
                MAX(bc.endereco) AS endereco,
                MAX(bc.numero) AS numero,
                MAX(bc.complemento) AS complemento,
                MAX(bc.bairro) AS bairro,
                MAX(bc.cidade) AS cidade,
                MAX(bc.uf) AS uf,
                MAX(bc.cep) AS cep,
                MAX(bc.ponto_referencia) AS ponto_referencia,
                MAX(bc.crivo_vendas) AS crivo_vendas,
                
                -- Dados de logística (relatorio_objetos)
                """ + ("""
                MAX(ro.nu_pedido) AS nu_pedido,
                MAX(ro.rastreio) AS rastreio,
                MAX(ro.status) AS ro_status,
                """ if tem_relatorio_objetos else """
                NULL AS nu_pedido,
                NULL AS rastreio,
                NULL AS ro_status,
                """) + """
                
                -- Contadores para controle de duplicação/reclassificação
                COUNT(DISTINCT pr.id) AS total_classificacoes,
                CASE WHEN COUNT(DISTINCT pr.id) > 1 THEN 'SIM' ELSE 'NAO' END AS houve_reclassificacao
                
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
              AND (pr.template IS NOT NULL OR bc.data_venda >= '2026-01-01')
              -- FILTRO CRÍTICO: Apenas clientes com crivo_vendas = "APROVADA"
              AND UPPER(TRIM(COALESCE(CAST(bc.crivo_vendas AS TEXT), ''))) = 'APROVADA'
            GROUP BY COALESCE(bc.proposta_isize, bc.codigo_externo, pr.codigo_externo""" + (", ro.codigo_externo" if tem_relatorio_objetos else "") + """, '')
            ORDER BY MAX(bc.data_venda) DESC NULLS LAST
            LIMIT 5000
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
            GROUP BY codigo_externo
            ORDER BY id DESC
            LIMIT 1000
            """
        
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        print(f"    >> {len(rows)} registros encontrados")
    
    if not rows:
        print("Nenhum registro com template encontrado!")
        return
    
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
    
    for row in rows:
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
        
        # Preencher dados adicionais de base_coverte_prop se disponíveis
        if row_dict.get('cliente_nome'):
            record.nome_cliente = str(row_dict['cliente_nome']).strip() if row_dict.get('cliente_nome') else ''
        if row_dict.get('telefone_portado'):
            record.telefone_contato = str(row_dict['telefone_portado']).strip()
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
            
            # 2. Se não encontrou, tentar variações do código externo
            if not obj_match and record.codigo_externo:
                # Tentar com zeros à esquerda
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
                
                # 3. Tentar buscar por nu_pedido usando o código externo
                # IMPORTANTE: Se houver múltiplos pedidos, usar sempre o mais recente
                if not obj_match and hasattr(objects_loader, '_index_by_nu_pedido'):
                    # Coletar todos os matches primeiro
                    matches_por_codigo = []
                    codigo_target = str(record.codigo_externo).strip().lstrip('0')
                    
                    for nu_ped, obj in objects_loader._index_by_nu_pedido.items():
                        codigo_obj = str(getattr(obj, 'codigo_externo', '')).strip().lstrip('0')
                        # Verificar se o código externo do objeto corresponde
                        if codigo_obj == codigo_target or \
                           str(record.codigo_externo) in str(nu_ped) or \
                           codigo_target in str(nu_ped):
                            matches_por_codigo.append(obj)
                    
                    # Se encontrou múltiplos, escolher o mais recente por data
                    if matches_por_codigo:
                        # Ordenar por data de inserção ou criação (mais recente primeiro)
                        matches_por_codigo.sort(
                            key=lambda x: (
                                x.data_insercao or x.data_criacao_pedido or datetime.min
                            ),
                            reverse=True
                        )
                        obj_match = matches_por_codigo[0]  # Pegar o mais recente
                
                # 4. Tentar buscar todos os registros por CPF e encontrar o que tem código externo próximo
                # IMPORTANTE: Se houver múltiplos, usar sempre o mais recente
                if not obj_match and record.cpf and hasattr(objects_loader, '_index_by_cpf'):
                    matches = objects_loader._index_by_cpf.get(record.cpf, [])
                    if matches:
                        # Tentar encontrar o mais próximo do código externo
                        codigo_target = str(record.codigo_externo).strip().lstrip('0')
                        matches_com_codigo = []
                        
                        for match in matches:
                            codigo_match = str(getattr(match, 'codigo_externo', '')).strip().lstrip('0')
                            if codigo_match:
                                # Verificar se códigos são exatamente iguais ou muito próximos
                                if codigo_match == codigo_target or \
                                   codigo_match.endswith(codigo_target[-6:]) or \
                                   codigo_target.endswith(codigo_match[-6:]):
                                    matches_com_codigo.append(match)
                        
                        # Se encontrou matches com código similar, escolher o mais recente
                        if matches_com_codigo:
                            # Ordenar por data (mais recente primeiro)
                            matches_com_codigo.sort(
                                key=lambda x: (
                                    x.data_insercao or x.data_criacao_pedido or datetime.min
                                ),
                                reverse=True
                            )
                            obj_match = matches_com_codigo[0]  # Pegar o mais recente
                        # Se não encontrou similar, usar o mais recente de todos (primeiro da lista, já ordenado)
                        elif matches:
                            obj_match = matches[0]  # Primeiro já é o mais recente (ordenado no load)
            
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
        template_info = TemplateMapper.get_template_for_record(record)
        template_id = template_info.get('template_id')
        
        if not template_id:
            continue
        
        # Estatísticas
        template_stats[template_id] = template_stats.get(template_id, 0) + 1
        
        # Obter configuração do template
        template_config = TEMPLATES.get(template_id)
        if not template_config:
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
        template_triggers = record.template or ''
        tipo_comunicacao = template_triggers
        if template_triggers.upper() in ['EM CRIAÇÃO', 'EM CRIACAO', 'EM_CRIACAO']:
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
        
        # Contagem de tentativas (baseado no histórico de envios)
        tentativas = 0
        if not historico_envios_df.empty and record.codigo_externo:
            # Contar quantas vezes este cliente já recebeu mensagens
            mask = (
                (historico_envios_df.get('proposta_isize', pd.Series()).astype(str).str.strip() == str(record.codigo_externo).strip()) |
                (historico_envios_df.get('cpf', pd.Series()).astype(str).str.strip() == str(record.cpf).strip())
            )
            tentativas = mask.sum() if hasattr(mask, 'sum') else 0
        
        # Ordem IMUTÁVEL das colunas principais (conforme especificado para Google Sheets)
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
            'Tentativas': tentativas,
            'Total_Classificacoes': total_classificacoes,
            'Houve_Reclassificacao': houve_reclassificacao,
            'Status_Disparo': 'FALSE',  # Sempre FALSE
            'DataHora_Disparo': '',  # Sempre vazio
        }
        
        # Colunas apenas para homologação (não se aplica à produção) - adicionadas no final
        row_data.update({
            'Template_Triggers': template_triggers,
            'O_Que_Aconteceu': record.o_que_aconteceu or '',
            'Acao_Realizar': record.acao_a_realizar or '',
        })
        
        homologacao_data.append(row_data)
    
    # 4. Salvar arquivo de homologação
    print("[4] Salvando arquivo de homologação...")
    
    OUTPUT_HOMOLOGACAO.parent.mkdir(parents=True, exist_ok=True)
    
    # Tentar salvar, se arquivo estiver aberto, usar nome temporário
    output_path = OUTPUT_HOMOLOGACAO
    try:
        # Testar se consegue escrever
        with open(output_path, 'w', newline='', encoding='utf-8-sig') as f:
            pass
    except PermissionError:
        # Arquivo está aberto, usar nome temporário
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = OUTPUT_HOMOLOGACAO.parent / f"homologacao_wpp_{timestamp}.csv"
        print(f"    >> Arquivo original está aberto, salvando como: {output_path.name}")
    
    with open(output_path, 'w', newline='', encoding='utf-8-sig') as f:
        if homologacao_data:
            # Ordem IMUTÁVEL das colunas principais (para Google Sheets)
            # Removida coluna vazia após NomeCliente (conforme solicitado)
            colunas_principais = [
                'Proposta_iSize', 'Cpf', 'NomeCliente', 'Telefone_Contato',
                'Endereco', 'Numero', 'Complemento', 'Bairro', 'Cidade', 'UF', 'Cep', 'Ponto_Referencia',
                'Cod_Rastreio', 'Data_Venda', 'Tipo_Comunicacao', 
                'Tentativas', 'Total_Classificacoes', 'Houve_Reclassificacao',
                'Status_Disparo', 'DataHora_Disparo'
            ]
            
            # Colunas apenas para homologação (não se aplica à produção) - adicionadas no final
            colunas_homologacao = [
                'Template_Triggers',
                'O_Que_Aconteceu',
                'Acao_Realizar'
            ]
            
            # Ordem completa
            fieldnames = colunas_principais + colunas_homologacao
            
            # Escrever cabeçalho
            f.write(';'.join(fieldnames) + '\n')
            
            # Escrever dados
            writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=';', extrasaction='ignore')
            for row in homologacao_data:
                writer.writerow(row)
    
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

