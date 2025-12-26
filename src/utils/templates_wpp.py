"""
Templates de Mensagens WhatsApp - Mapeamento e Geração de Variáveis
Versão 1.0 - Integração com Régua de Comunicação

Referência dos Templates:
ID | Nome_modelo                    | Uso
1  | confirma_portabilidade_v1      | Confirmação de portabilidade processada
2  | pendencia_sms_portabilidade    | Pendência de validação SMS
3  | aviso_retirada_correios_v1     | Chip aguardando retirada nos Correios
4  | confirmacao_endereco_v1        | Confirmação de endereço de entrega
"""
import logging
from typing import Optional, Dict, List, Any
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class TemplateID(Enum):
    """IDs dos templates disponíveis"""
    CONFIRMA_PORTABILIDADE = 1
    PENDENCIA_SMS_PORTABILIDADE = 2
    AVISO_RETIRADA_CORREIOS = 3
    CONFIRMACAO_ENDERECO = 4


@dataclass
class TemplateConfig:
    """Configuração de um template"""
    id: int
    nome_modelo: str
    categoria: str
    cabecalho: str
    variaveis: List[str]  # Lista de nomes das variáveis {{1}}, {{2}}, etc.
    tem_botao: bool
    botao_url: Optional[str] = None
    botao_texto: Optional[str] = None


# Configuração completa dos templates
TEMPLATES: Dict[int, TemplateConfig] = {
    1: TemplateConfig(
        id=1,
        nome_modelo="confirma_portabilidade_v1",
        categoria="Utilidade / Atualização de Pedido",
        cabecalho="Atualização de Solicitação",
        variaveis=[],  # Sem variáveis dinâmicas
        tem_botao=True,
        botao_texto="Confirmar Solicitação",
        botao_url="https://tinyurl.com/portsim"
    ),
    2: TemplateConfig(
        id=2,
        nome_modelo="pendencia_sms_portabilidade",
        categoria="Utilidade / Atualização de Conta",
        cabecalho="Aviso de Pendência Técnica",
        variaveis=[],  # Sem variáveis dinâmicas
        tem_botao=True,
        botao_texto="Gerar SMS de Validação",
        botao_url="https://tinyurl.com/portsim"
    ),
    3: TemplateConfig(
        id=3,
        nome_modelo="aviso_retirada_correios_v1",
        categoria="Utilidade / Atualização de Conta",
        cabecalho="Atualização Logística",
        variaveis=["nome_cliente", "cod_rastreio"],  # {{1}} = nome, {{2}} = rastreio
        tem_botao=True,
        botao_texto="Ver Endereço da Agência",
        botao_url="https://rastreamento.correios.com.br/app/index.php"
    ),
    4: TemplateConfig(
        id=4,
        nome_modelo="confirmacao_endereco_v1",
        categoria="Utilidade / Atualização de Conta",
        cabecalho="Conferência de Dados de Entrega",
        variaveis=[
            "nome_cliente",      # {{1}} = nome
            "endereco",          # {{2}} = rua
            "numero",            # {{3}} = número
            "complemento",       # {{4}} = complemento
            "bairro",            # {{5}} = bairro
            "cidade",            # {{6}} = cidade
            "uf",                # {{7}} = UF
            "cep",               # {{8}} = CEP
            "ponto_referencia"   # {{9}} = ponto de referência
        ],
        tem_botao=False
    ),
}


# Mapeamento de Tipo de Comunicação/Template para Template WPP
# Baseado nos valores do triggers.xlsx
TIPO_COMUNICACAO_PARA_TEMPLATE: Dict[str, int] = {
    # Por Tipo de Mensagem (campo Tipo_Mensagem do triggers)
    "CONFIRMACAO BP": 1,           # -> confirma_portabilidade_v1
    "LIBERACAO BONUS": 1,          # -> confirma_portabilidade_v1
    "PENDENTE": 2,                 # -> pendencia_sms_portabilidade
    
    # Por Template do triggers.xlsx (campo Template)
    "1": 1,   # Template 1 do triggers -> confirma_portabilidade_v1
    "2": 1,   # Template 2 do triggers -> confirma_portabilidade_v1
    
    # Tipos de comunicação numéricos (sistema anterior)
    "3": 1,   # Portabilidade Concluída -> confirma_portabilidade_v1
    "5": 2,   # Reagendar Portabilidade -> pendencia_sms_portabilidade
    "6": 2,   # Portabilidade Pendente -> pendencia_sms_portabilidade
    
    # Entrega
    "14": 3,  # Aguardando Retirada -> aviso_retirada_correios_v1
    "RETIRADA CORREIOS": 3,        # -> aviso_retirada_correios_v1
    "AGUARDANDO RETIRADA": 3,      # -> aviso_retirada_correios_v1
    
    # Confirmação de endereço
    "43": 4,  # Endereço Incorreto -> confirmacao_endereco_v1
    "ENDERECO INCORRETO": 4,       # -> confirmacao_endereco_v1
    
    # Não enviar - sem template
    "NÃO ENVIAR": None,
    "NAO ENVIAR": None,
    "-": None,
    "": None,
}


class TemplateMapper:
    """
    Classe para mapear e gerar dados de templates WPP
    """
    
    @staticmethod
    def get_template_id(tipo_comunicacao: str) -> Optional[int]:
        """
        Retorna o ID do template para um tipo de comunicação
        
        Args:
            tipo_comunicacao: Código do tipo de comunicação
            
        Returns:
            ID do template ou None se não mapeado
        """
        if not tipo_comunicacao:
            return None
        
        tipo_str = str(tipo_comunicacao).strip().upper()
        
        # Tentar buscar diretamente
        result = TIPO_COMUNICACAO_PARA_TEMPLATE.get(tipo_str)
        if result is not None:
            return result
        
        # Tentar buscar com valor original (case-sensitive)
        result = TIPO_COMUNICACAO_PARA_TEMPLATE.get(str(tipo_comunicacao).strip())
        if result is not None:
            return result
        
        return None
    
    @staticmethod
    def get_template_config(template_id: int) -> Optional[TemplateConfig]:
        """
        Retorna a configuração de um template
        
        Args:
            template_id: ID do template
            
        Returns:
            TemplateConfig ou None
        """
        return TEMPLATES.get(template_id)
    
    @staticmethod
    def get_template_name(template_id: int) -> Optional[str]:
        """
        Retorna o nome do modelo do template
        
        Args:
            template_id: ID do template
            
        Returns:
            Nome do modelo ou None
        """
        config = TEMPLATES.get(template_id)
        return config.nome_modelo if config else None
    
    @classmethod
    def generate_variables(cls, template_id: int, record_data: Dict[str, Any]) -> Dict[str, str]:
        """
        Gera as variáveis para um template a partir dos dados do registro
        
        Args:
            template_id: ID do template
            record_data: Dicionário com dados do registro
            
        Returns:
            Dicionário com variáveis numeradas {{"1": valor, "2": valor, ...}}
        """
        config = TEMPLATES.get(template_id)
        if not config:
            return {}
        
        variables = {}
        for i, var_name in enumerate(config.variaveis, start=1):
            value = record_data.get(var_name, "")
            # Garantir que não seja None
            variables[str(i)] = str(value) if value else ""
        
        return variables
    
    @classmethod
    def get_template_for_record(cls, record) -> Dict[str, Any]:
        """
        Determina o template e variáveis para um registro de portabilidade
        
        Args:
            record: PortabilidadeRecord ou dict com dados
            
        Returns:
            Dicionário com template_id, nome_modelo, variaveis
        """
        from src.models.portabilidade import PortabilidadeRecord
        
        # Converter para dict se necessário
        if isinstance(record, PortabilidadeRecord):
            record_data = {
                "nome_cliente": record.nome_cliente or "",
                "cod_rastreio": record.cod_rastreio or PortabilidadeRecord.gerar_link_rastreio(record.codigo_externo) or "",
                "endereco": "",  # Não disponível diretamente
                "numero": "",
                "complemento": "",
                "bairro": "",
                "cidade": record.cidade or "",
                "uf": record.uf or "",
                "cep": record.cep or "",
                "ponto_referencia": "",
            }
            # Tentar template primeiro, depois tipo_mensagem
            template_field = record.template
            tipo_msg_field = record.tipo_mensagem
        else:
            record_data = record
            template_field = record.get("template")
            tipo_msg_field = record.get("tipo_comunicacao") or record.get("tipo_mensagem")
        
        # Tentar obter template ID - primeiro pelo template, depois pelo tipo_mensagem
        template_id = cls.get_template_id(template_field)
        if template_id is None:
            template_id = cls.get_template_id(tipo_msg_field)
        
        if not template_id:
            return {
                "template_id": None,
                "nome_modelo": None,
                "variaveis": {},
                "mapeado": False
            }
        
        # Gerar variáveis
        variables = cls.generate_variables(template_id, record_data)
        
        return {
            "template_id": template_id,
            "nome_modelo": cls.get_template_name(template_id),
            "variaveis": variables,
            "mapeado": True
        }
    
    @staticmethod
    def format_variables_string(variables: Dict[str, str]) -> str:
        """
        Formata variáveis como string para uso em planilhas
        Ex: "{{1}}=João;{{2}}=ABC123"
        
        Args:
            variables: Dicionário de variáveis
            
        Returns:
            String formatada
        """
        if not variables:
            return ""
        
        parts = [f"{{{{{k}}}}}={v}" for k, v in sorted(variables.items())]
        return ";".join(parts)
    
    @classmethod
    def enrich_wpp_data(cls, wpp_dict: Dict[str, Any], record) -> Dict[str, Any]:
        """
        Enriquece os dados de saída WPP com informações do template
        
        Args:
            wpp_dict: Dicionário de saída WPP
            record: Registro de portabilidade
            
        Returns:
            Dicionário enriquecido
        """
        template_info = cls.get_template_for_record(record)
        
        # Adicionar informações do template
        wpp_dict["Template_ID"] = template_info.get("template_id") or ""
        wpp_dict["Template_Nome"] = template_info.get("nome_modelo") or ""
        wpp_dict["Template_Variaveis"] = cls.format_variables_string(
            template_info.get("variaveis", {})
        )
        
        return wpp_dict


def get_all_templates() -> List[Dict[str, Any]]:
    """
    Retorna lista de todos os templates disponíveis
    
    Returns:
        Lista de dicionários com informações dos templates
    """
    result = []
    for template_id, config in TEMPLATES.items():
        result.append({
            "id": config.id,
            "nome_modelo": config.nome_modelo,
            "categoria": config.categoria,
            "cabecalho": config.cabecalho,
            "variaveis": config.variaveis,
            "tem_botao": config.tem_botao,
            "botao_texto": config.botao_texto,
            "botao_url": config.botao_url,
        })
    return result


def print_templates_info():
    """Imprime informações sobre os templates disponíveis"""
    print("\n" + "=" * 70)
    print("TEMPLATES DE MENSAGENS WHATSAPP DISPONÍVEIS")
    print("=" * 70)
    
    for template_id, config in TEMPLATES.items():
        print(f"\n[Template {config.id}] {config.nome_modelo}")
        print(f"  Categoria: {config.categoria}")
        print(f"  Cabeçalho: {config.cabecalho}")
        if config.variaveis:
            print(f"  Variáveis: {', '.join(config.variaveis)}")
        else:
            print("  Variáveis: Nenhuma")
        if config.tem_botao:
            print(f"  Botão: {config.botao_texto} -> {config.botao_url}")
    
    print("\n" + "-" * 70)
    print("MAPEAMENTO TIPO_COMUNICACAO -> TEMPLATE")
    print("-" * 70)
    
    tipo_nomes = {
        "1": "Portabilidade Agendada",
        "2": "Portabilidade Antecipada",
        "3": "Portabilidade Concluída",
        "5": "Reagendar Portabilidade",
        "6": "Portabilidade Pendente",
        "14": "Aguardando Retirada",
        "43": "Endereço Incorreto",
    }
    
    for tipo, template_id in TIPO_COMUNICACAO_PARA_TEMPLATE.items():
        nome_tipo = tipo_nomes.get(tipo, f"Tipo {tipo}")
        template_nome = TEMPLATES[template_id].nome_modelo
        print(f"  {tipo} ({nome_tipo}) -> {template_id} ({template_nome})")
    
    print("=" * 70 + "\n")


if __name__ == "__main__":
    print_templates_info()

