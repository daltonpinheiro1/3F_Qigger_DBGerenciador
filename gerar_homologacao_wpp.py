"""
Script para gerar arquivo de homologação WPP
Mostra como os dados serão enviados ao WhatsApp sem fazer o envio real

REGRAS DE NEGÓCIO:
- Apenas clientes com crivo_vendas = "APROVADA" recebem mensagens
- Coluna Numero deve conter apenas números (letras vão para Complemento)
- Verificação no Google Sheets para controle de tentativas
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
import json

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
    from config import DB_PATH, OUTPUT_WPP
    DB_PATH = DB_PATH
    OUTPUT_HOMOLOGACAO = Path(OUTPUT_WPP)
    # Base analítica agora vem do banco unificado (base_unificada) ou Excel processado
    # Não precisa mais do arquivo CSV separado
    BASE_ANALITICA_PATH = Path("/dev/null")  # Placeholder que nunca existe
except ImportError:
    # Fallback se config.py não existir
    DB_PATH = "data/portabilidade.db"
    OUTPUT_HOMOLOGACAO = Path("data/homologacao_wpp.csv")
    BASE_ANALITICA_PATH = Path("/dev/null")  # Placeholder que nunca existe

# Configuração do Google Sheets para controle de disparos
GSHEET_ID = "13qXylcL-wYbB4vDouI4d2rRazYQvaPEZneEmLx-lVtk"
GSHEET_PATH = Path("/Users/mac/Library/CloudStorage/GoogleDrive-dspinheiro31@gmail.com/.shortcut-targets-by-id/1Go5-kLQhwQ1l2nZpQGVQg_gNi2l-fsES/Portabilidade TIM - Régua de Comunicação")

# Palavras a ignorar ao extrair primeiro e último nome
PALAVRAS_IGNORAR = {'e', 'de', 'da', 'do', 'das', 'dos', 'em', 'na', 'no', 'nas', 'nos'}

# Limite de tentativas por template
MAX_TENTATIVAS_TEMPLATE = 5
HORAS_ENTRE_ENVIOS = 24


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


def carregar_historico_gsheets() -> dict:
    """
    Carrega histórico de disparos do Google Sheets.
    Retorna dicionário com {proposta_isize: {template, data_disparo, tentativas}}
    """
    historico = {}
    
    try:
        # Tentar usar gspread se disponível
        try:
            import gspread
            from google.oauth2.service_account import Credentials
            
            # Verificar se há credenciais configuradas
            creds_path = Path.home() / ".config" / "gspread" / "service_account.json"
            if not creds_path.exists():
                creds_path = Path(__file__).parent / "credentials" / "service_account.json"
            
            if creds_path.exists():
                scopes = [
                    'https://www.googleapis.com/auth/spreadsheets',
                    'https://www.googleapis.com/auth/drive'
                ]
                creds = Credentials.from_service_account_file(str(creds_path), scopes=scopes)
                gc = gspread.authorize(creds)
                
                # Abrir planilha
                sh = gc.open_by_key(GSHEET_ID)
                worksheet = sh.sheet1
                
                # Obter todos os dados
                records = worksheet.get_all_records()
                
                for row in records:
                    proposta = str(row.get('Proposta_iSize', '')).strip()
                    if proposta:
                        template = str(row.get('Tipo_Comunicacao', '')).strip()
                        status = str(row.get('Status_Disparo', '')).strip().upper()
                        data_str = str(row.get('DataHora_Disparo', '')).strip()
                        tentativas = int(row.get('Tentativas', 0) or 0)
                        
                        # Parsear data do disparo
                        data_disparo = None
                        if data_str:
                            for fmt in ['%d/%m/%Y %H:%M:%S', '%d/%m/%Y %H:%M', '%d/%m/%Y', '%Y-%m-%d %H:%M:%S']:
                                try:
                                    data_disparo = datetime.strptime(data_str, fmt)
                                    break
                                except ValueError:
                                    continue
                        
                        if proposta not in historico:
                            historico[proposta] = {
                                'template': template,
                                'data_disparo': data_disparo,
                                'tentativas': tentativas,
                                'status': status
                            }
                        else:
                            # Atualizar se encontrar registro mais recente
                            if data_disparo and (not historico[proposta]['data_disparo'] or 
                                                 data_disparo > historico[proposta]['data_disparo']):
                                historico[proposta]['data_disparo'] = data_disparo
                                historico[proposta]['template'] = template
                            historico[proposta]['tentativas'] = max(historico[proposta]['tentativas'], tentativas)
                
                logger.info(f"✓ Histórico carregado do Google Sheets: {len(historico)} registros")
                return historico
                
        except ImportError:
            logger.warning("gspread não instalado, tentando método alternativo...")
        except Exception as e:
            logger.warning(f"Erro ao acessar Google Sheets via API: {e}")
        
        # Método alternativo: carregar do banco local (tabela disparos_wpp)
        logger.info("Usando histórico do banco de dados local...")
        
    except Exception as e:
        logger.warning(f"Erro ao carregar histórico: {e}")
    
    return historico


def verificar_pode_enviar(proposta_isize: str, template_atual: str, historico: dict, db_manager) -> tuple:
    """
    Verifica se pode enviar mensagem para o cliente baseado nas regras:
    - Se já enviou nas últimas 24h, não envia
    - Se já tem template 1, retorna template 2
    - Se tem 5 tentativas de templates 1 ou 2, não entra mais na fila
    
    Args:
        proposta_isize: Código da proposta
        template_atual: Template sugerido pelo sistema
        historico: Histórico de disparos do Google Sheets
        db_manager: Gerenciador do banco de dados
        
    Returns:
        Tupla (pode_enviar: bool, template_final: str, tentativas: int, motivo: str)
    """
    tentativas = 0
    agora = datetime.now()
    
    # Verificar no histórico do Google Sheets
    if proposta_isize in historico:
        info = historico[proposta_isize]
        tentativas = info.get('tentativas', 0)
        ultimo_template = info.get('template', '')
        data_ultimo = info.get('data_disparo')
        
        # Regra 1: Se tem 5+ tentativas de template 1 ou 2, não envia mais
        if tentativas >= MAX_TENTATIVAS_TEMPLATE and ultimo_template in ['1', '2']:
            return False, template_atual, tentativas, f"Limite de {MAX_TENTATIVAS_TEMPLATE} tentativas atingido"
        
        # Regra 2: Se já enviou nas últimas 24h, não envia
        if data_ultimo:
            horas_desde_ultimo = (agora - data_ultimo).total_seconds() / 3600
            if horas_desde_ultimo < HORAS_ENTRE_ENVIOS:
                horas_restantes = HORAS_ENTRE_ENVIOS - horas_desde_ultimo
                return False, template_atual, tentativas, f"Aguardar {horas_restantes:.1f}h para próximo envio"
        
        # Regra 3: Se já tem template 1, deve usar template 2
        if ultimo_template == '1' and template_atual == '1':
            template_atual = '2'
    
    # Verificar também no banco local
    try:
        with db_manager._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT template_id, data_disparo, COUNT(*) as total
                FROM disparos_wpp
                WHERE codigo_externo = ?
                GROUP BY codigo_externo
                ORDER BY data_disparo DESC
                LIMIT 1
            """, (proposta_isize,))
            row = cursor.fetchone()
            
            if row:
                ultimo_template_db = str(row[0]) if row[0] else ''
                data_ultimo_db = row[1]
                total_db = row[2] or 0
                
                tentativas = max(tentativas, total_db)
                
                # Verificar 24h no banco local também
                if data_ultimo_db:
                    if isinstance(data_ultimo_db, str):
                        try:
                            data_ultimo_db = datetime.strptime(data_ultimo_db, '%Y-%m-%d %H:%M:%S')
                        except ValueError:
                            data_ultimo_db = None
                    
                    if data_ultimo_db:
                        horas_desde_ultimo = (agora - data_ultimo_db).total_seconds() / 3600
                        if horas_desde_ultimo < HORAS_ENTRE_ENVIOS:
                            horas_restantes = HORAS_ENTRE_ENVIOS - horas_desde_ultimo
                            return False, template_atual, tentativas, f"Aguardar {horas_restantes:.1f}h (banco local)"
                
                # Regra de alternância de template
                if ultimo_template_db == '1' and template_atual == '1':
                    template_atual = '2'
                    
    except Exception as e:
        logger.debug(f"Erro ao verificar banco local: {e}")
    
    return True, template_atual, tentativas + 1, "OK"


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
        tem_disparos_wpp = 'disparos_wpp' in tabelas_existentes
        
        # Criar tabela de controle de disparos se não existir
        if not tem_disparos_wpp:
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS disparos_wpp (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                codigo_externo TEXT NOT NULL,
                cpf TEXT,
                template_id INTEGER,
                data_disparo DATETIME DEFAULT CURRENT_TIMESTAMP,
                status_disparo TEXT DEFAULT 'ENVIADO',
                observacao TEXT,
                UNIQUE(codigo_externo, template_id)
            )
            """)
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_disparos_codigo ON disparos_wpp(codigo_externo)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_disparos_cpf ON disparos_wpp(cpf)")
            conn.commit()
            tem_disparos_wpp = True
            print("    >> Tabela disparos_wpp criada para controle de disparos")
        
        if tem_base_coverte:
            print("    >> Usando base_coverte_prop + portabilidade_records" + (" + relatorio_objetos" if tem_relatorio_objetos else ""))
        else:
            print("    >> Tabela base_coverte_prop não encontrada, usando apenas portabilidade_records")
        
        # Buscar registros que têm template e estão mapeados, sincronizando com base_coverte_prop e relatorio_objetos
        if tem_base_coverte:
            query = """
            SELECT DISTINCT
                -- Dados de portabilidade_records
                pr.id, 
                COALESCE(bc.cpf, pr.cpf""" + (", ro.documento" if tem_relatorio_objetos else "") + """, '') AS cpf,
                COALESCE(pr.numero_acesso, '') AS numero_acesso,
                COALESCE(pr.numero_ordem, bc.numero_ordem, '') AS numero_ordem,
                COALESCE(bc.proposta_isize, bc.codigo_externo, pr.codigo_externo""" + (", ro.codigo_externo" if tem_relatorio_objetos else "") + """, '') AS codigo_externo,
                
                -- Template e regras
                pr.tipo_mensagem,
                pr.template,
                pr.regra_id,
                pr.o_que_aconteceu,
                pr.acao_a_realizar,
                
                -- Dados adicionais de base_coverte_prop
                bc.cliente_nome,
                bc.telefone_portado,
                bc.data_venda,
                bc.plano,
                bc.endereco,
                bc.numero,
                bc.complemento,
                bc.bairro,
                bc.cidade,
                bc.uf,
                bc.cep,
                bc.ponto_referencia,
                bc.crivo_vendas,
                
                -- Dados de logística (relatorio_objetos)
                """ + ("""
                ro.nu_pedido,
                ro.rastreio,
                ro.status AS ro_status
                """ if tem_relatorio_objetos else """
                NULL AS ro_nu_pedido,
                NULL AS ro_rastreio,
                NULL AS ro_status
                """) + """
                
            FROM portabilidade_records pr
            LEFT JOIN base_coverte_prop bc ON (
                TRIM(COALESCE(CAST(bc.proposta_isize AS TEXT), CAST(bc.codigo_externo AS TEXT), '')) = 
                TRIM(COALESCE(CAST(pr.codigo_externo AS TEXT), ''))
            )
            """ + ("""
            LEFT JOIN relatorio_objetos ro ON (
                TRIM(COALESCE(CAST(bc.proposta_isize AS TEXT), CAST(bc.codigo_externo AS TEXT), CAST(pr.codigo_externo AS TEXT), '')) = 
                TRIM(COALESCE(CAST(ro.codigo_externo AS TEXT), ''))
            )
            """ if tem_relatorio_objetos else "") + ("""
            LEFT JOIN disparos_wpp dw ON (
                TRIM(COALESCE(CAST(bc.proposta_isize AS TEXT), CAST(bc.codigo_externo AS TEXT), CAST(pr.codigo_externo AS TEXT), '')) = 
                TRIM(COALESCE(CAST(dw.codigo_externo AS TEXT), ''))
            )
            """ if tem_disparos_wpp else "") + """
            WHERE pr.template IS NOT NULL 
              AND pr.template != ''
              AND pr.template != '-'
              AND pr.mapeado = 1
              -- FILTRO CRÍTICO: Apenas vendas APROVADAS no CRIVO recebem mensagem WPP
              AND UPPER(TRIM(COALESCE(CAST(bc.crivo_vendas AS TEXT), ''))) = 'APROVADA'
              """ + ("""
              -- FILTRO: Excluir registros que já tiveram disparo realizado
              AND dw.id IS NULL
              """ if tem_disparos_wpp else "") + """
            ORDER BY 
                CASE 
                    WHEN bc.data_venda IS NOT NULL 
                         AND TRIM(CAST(bc.data_venda AS TEXT)) != ''
                         AND (SUBSTR(TRIM(CAST(bc.data_venda AS TEXT)), 5, 1) = '-' OR LENGTH(TRIM(CAST(bc.data_venda AS TEXT))) >= 10)
                         AND SUBSTR(TRIM(CAST(bc.data_venda AS TEXT)), 1, 4) GLOB '[0-9][0-9][0-9][0-9]'
                    THEN date(SUBSTR(TRIM(CAST(bc.data_venda AS TEXT)), 1, 10))
                    WHEN bc.data_venda IS NOT NULL 
                         AND TRIM(CAST(bc.data_venda AS TEXT)) != ''
                         AND LENGTH(TRIM(CAST(bc.data_venda AS TEXT))) >= 10
                         AND SUBSTR(TRIM(CAST(bc.data_venda AS TEXT)), 3, 1) = '/'
                         AND SUBSTR(TRIM(CAST(bc.data_venda AS TEXT)), 6, 1) = '/'
                    THEN date(
                        SUBSTR(TRIM(CAST(bc.data_venda AS TEXT)), 7, 4) || '-' || 
                        SUBSTR(TRIM(CAST(bc.data_venda AS TEXT)), 4, 2) || '-' || 
                        SUBSTR(TRIM(CAST(bc.data_venda AS TEXT)), 1, 2)
                    )
                    ELSE date('1900-01-01')
                END DESC,
                pr.id DESC
            LIMIT 5000
            """
        else:
            # Fallback: usar apenas portabilidade_records
            query = """
            SELECT DISTINCT
                id, cpf, numero_acesso, numero_ordem, codigo_externo,
                tipo_mensagem, template, regra_id, o_que_aconteceu, acao_a_realizar,
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
                NULL AS ro_status
            FROM portabilidade_records
            WHERE template IS NOT NULL 
              AND template != ''
              AND template != '-'
              AND mapeado = 1
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
    
    # 3. Processar registros e gerar dados de homologação
    print("[3] Processando registros e gerando preview de mensagens...")
    
    homologacao_data = []
    template_stats = {}
    
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
    
    registros_filtrados_crivo = 0
    registros_filtrados_24h = 0
    registros_filtrados_tentativas = 0
    
    # Carregar histórico do Google Sheets para controle de tentativas
    print("[2.3] Carregando histórico de disparos para controle...")
    historico_disparos = carregar_historico_gsheets()
    
    for row in rows:
        # Converter row para dict usando colunas
        row_dict = dict(zip(columns, row))
        
        # VERIFICAÇÃO CRÍTICA: Apenas APROVADA no CRIVO pode receber mensagem
        crivo_vendas = str(row_dict.get('crivo_vendas', '') or '').strip().upper()
        if crivo_vendas != 'APROVADA':
            registros_filtrados_crivo += 1
            continue  # Não processar este registro - APENAS APROVADA
        
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
        endereco_data = {
            'endereco': str(row_dict.get('endereco', '')).strip() if row_dict.get('endereco') else '',
            'numero': str(row_dict.get('numero', '')).strip() if row_dict.get('numero') else '',
            'complemento': str(row_dict.get('complemento', '')).strip() if row_dict.get('complemento') else '',
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
                
                # Dados de endereço do ObjectRecord
                endereco_data['endereco'] = getattr(obj_match, 'endereco', '') or endereco_data['endereco'] or ''
                endereco_data['numero'] = getattr(obj_match, 'numero', '') or endereco_data['numero'] or ''
                endereco_data['complemento'] = getattr(obj_match, 'complemento', '') or endereco_data['complemento'] or ''
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
                    
                    # VERIFICAÇÃO CRÍTICA: Se encontrou match no base_coverte_prop, verificar CRIVO
                    crivo_match = str(base_coverte_match.get('crivo_vendas', '') or '').strip().upper()
                    if crivo_match in ['REPROVADA CRIVO', 'REPROVADA', 'REPROVADO', 'REPROVADO CRIVO']:
                        registros_filtrados_crivo += 1
                        continue  # Pular este registro - CRIVO REPROVADO
        
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
                    
                    # Numero
                    numero = base_match.get('Numero') or base_match.get('Número') or base_match.get('Numero')
                    if pd.notna(numero) and numero and str(numero).strip():
                        endereco_data['numero'] = str(numero).strip()
                    
                    # Complemento
                    complemento = base_match.get('Complemento')
                    if pd.notna(complemento) and complemento and str(complemento).strip():
                        endereco_data['complemento'] = str(complemento).strip()
                    
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
        
        # Tipo_Comunicacao: usar Template_Triggers, substituir "EM CRIAÇÃO" por "1"
        template_triggers = record.template or ''
        tipo_comunicacao = template_triggers
        if template_triggers.upper() in ['EM CRIAÇÃO', 'EM CRIACAO', 'EM_CRIACAO']:
            tipo_comunicacao = '1'
        
        # VERIFICAÇÃO DE TENTATIVAS E REGRAS DE ENVIO
        proposta_isize = record.codigo_externo or ''
        pode_enviar, tipo_comunicacao_final, tentativas, motivo = verificar_pode_enviar(
            proposta_isize, 
            tipo_comunicacao, 
            historico_disparos,
            db_manager
        )
        
        if not pode_enviar:
            if 'Aguardar' in motivo:
                registros_filtrados_24h += 1
            elif 'Limite' in motivo:
                registros_filtrados_tentativas += 1
            continue  # Não incluir na fila de disparos
        
        # Usar o tipo de comunicação ajustado (pode ter mudado de 1 para 2)
        tipo_comunicacao = tipo_comunicacao_final
        
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
        
        # EXTRAIR LETRAS DO NÚMERO E MOVER PARA COMPLEMENTO
        numero_original = endereco_data['numero'] or ''
        complemento_original = endereco_data['complemento'] or ''
        numero_limpo, complemento_atualizado = extrair_numero_e_complemento(numero_original, complemento_original)
        
        # Ordem IMUTÁVEL das colunas principais (conforme especificado para Google Sheets)
        # REMOVIDA coluna vazia (_vazia) conforme solicitado
        row_data = {
            'Proposta_iSize': record.codigo_externo or '',
            'Cpf': record.cpf or '',
            'NomeCliente': nome_cliente_formatado,
            'Telefone_Contato': telefone_contato,
            'Endereco': endereco_data['endereco'] or '',
            'Numero': numero_limpo,  # Apenas números
            'Complemento': complemento_atualizado,  # Inclui letras extraídas do número
            'Bairro': endereco_data['bairro'] or '',
            'Cidade': record.cidade or '',
            'UF': record.uf or '',
            'Cep': cep_normalizado,
            'Ponto_Referencia': endereco_data['ponto_referencia'] or '',
            'Cod_Rastreio': link_rastreio or '',
            'Data_Venda': data_venda_formatada,
            'Tipo_Comunicacao': tipo_comunicacao,
            'Tentativas': tentativas,  # Nova coluna com contagem de tentativas
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
            # Ordem das colunas principais (para Google Sheets)
            # REMOVIDA coluna vazia (_vazia) conforme solicitado
            # ADICIONADA coluna Tentativas para controle
            colunas_principais = [
                'Proposta_iSize', 'Cpf', 'NomeCliente', 'Telefone_Contato',
                'Endereco', 'Numero', 'Complemento', 'Bairro', 'Cidade', 'UF', 'Cep', 'Ponto_Referencia',
                'Cod_Rastreio', 'Data_Venda', 'Tipo_Comunicacao', 'Tentativas', 'Status_Disparo', 'DataHora_Disparo'
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
    
    # 4.1. Registrar disparos na tabela de controle (para evitar envios duplicados)
    print("[4.1] Registrando disparos na tabela de controle...")
    disparos_registrados = 0
    with db_manager._get_connection() as conn:
        cursor = conn.cursor()
        for row in homologacao_data:
            codigo_externo = row.get('Proposta_iSize', '')
            cpf = row.get('Cpf', '')
            template_triggers = row.get('Template_Triggers', '')
            
            # Tentar extrair template_id do template_triggers
            template_id = None
            try:
                if template_triggers and template_triggers.isdigit():
                    template_id = int(template_triggers)
            except:
                pass
            
            if codigo_externo:
                try:
                    cursor.execute("""
                        INSERT OR IGNORE INTO disparos_wpp (codigo_externo, cpf, template_id, observacao)
                        VALUES (?, ?, ?, 'Gerado via homologacao_wpp')
                    """, (codigo_externo, cpf, template_id))
                    if cursor.rowcount > 0:
                        disparos_registrados += 1
                except Exception as e:
                    logger.debug(f"Erro ao registrar disparo para {codigo_externo}: {e}")
        
        conn.commit()
    
    print(f"    >> {disparos_registrados} novos disparos registrados na tabela de controle")
    
    # 5. Estatísticas
    print()
    print("=" * 70)
    print("ESTATÍSTICAS DE HOMOLOGAÇÃO")
    print("=" * 70)
    print(f"  Total de registros gerados: {len(homologacao_data)}")
    print()
    print("  FILTROS APLICADOS:")
    if registros_filtrados_crivo > 0:
        print(f"    ⚠️  CRIVO não APROVADA: {registros_filtrados_crivo}")
    if registros_filtrados_24h > 0:
        print(f"    ⏱️  Aguardando 24h: {registros_filtrados_24h}")
    if registros_filtrados_tentativas > 0:
        print(f"    🚫 Limite de tentativas ({MAX_TENTATIVAS_TEMPLATE}): {registros_filtrados_tentativas}")
    print()
    print("  Por Template:")
    for template_id, count in sorted(template_stats.items()):
        config = TEMPLATES.get(template_id)
        nome = config.nome_modelo if config else f"Template {template_id}"
        print(f"    Template {template_id} ({nome}): {count} registros")
    
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

