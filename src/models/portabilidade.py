"""
Modelos de dados para registros de portabilidade
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List
from enum import Enum


class PortabilidadeStatus(Enum):
    """Status possíveis de uma portabilidade"""
    PENDENTE = "Portabilidade Pendente"
    CANCELADA = "Portabilidade Cancelada"
    CONCLUIDA = "Portado"
    CONFLITO = "Conflito"
    FALHA_PARCIAL = "Falha Parcial"
    SUSPENSA = "Portabilidade Suspensa"
    SEM_BILHETE = "Nenhum bilhete de portabilidade encontrado"
    ERRO_APROVISIONAMENTO = "Erro no Aprovisionamento"
    EM_APROVISIONAMENTO = "Em Aprovisionamento"
    PENDENTE_PORTABILIDADE = "Pendente Portabilidade"
    ERRO_SISTEMA = "[Sistema] Não foi possível processar esse registro."
    CLIENTE_SEM_CADASTRO = "Cliente sem cadastro"
    CANCELAMENTO_PENDENTE = "Cancelamento Pendente"


class StatusOrdem(Enum):
    """Status da ordem"""
    CONCLUIDO = "Concluído"
    PENDENTE = "Pendente Portabilidade"
    EM_APROVISIONAMENTO = "Em Aprovisionamento"
    ERRO_APROVISIONAMENTO = "Erro no Aprovisionamento"


@dataclass
class TriggerRule:
    """Modelo de dados para uma regra do triggers.xlsx"""
    regra_id: int
    status_bilhete: Optional[str] = None
    operadora_doadora: Optional[str] = None
    motivo_recusa: Optional[str] = None
    motivo_cancelamento: Optional[str] = None
    ultimo_bilhete: Optional[bool] = None
    motivo_nao_consultado: Optional[str] = None
    novo_status_bilhete: Optional[str] = None
    ajustes_numero_acesso: Optional[str] = None
    o_que_aconteceu: Optional[str] = None
    acao_a_realizar: Optional[str] = None
    tipo_mensagem: Optional[str] = None
    template: Optional[str] = None
    
    def to_dict(self) -> dict:
        """Converte a regra para dicionário"""
        return {
            'regra_id': self.regra_id,
            'status_bilhete': self.status_bilhete,
            'operadora_doadora': self.operadora_doadora,
            'motivo_recusa': self.motivo_recusa,
            'motivo_cancelamento': self.motivo_cancelamento,
            'ultimo_bilhete': self.ultimo_bilhete,
            'motivo_nao_consultado': self.motivo_nao_consultado,
            'novo_status_bilhete': self.novo_status_bilhete,
            'ajustes_numero_acesso': self.ajustes_numero_acesso,
            'o_que_aconteceu': self.o_que_aconteceu,
            'acao_a_realizar': self.acao_a_realizar,
            'tipo_mensagem': self.tipo_mensagem,
            'template': self.template,
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'TriggerRule':
        """Cria uma regra a partir de um dicionário"""
        return cls(
            regra_id=cls._clean_value(data.get('REGRA_ID') or data.get('regra_id')),
            status_bilhete=cls._clean_value(data.get('Status do bilhete') or data.get('status_bilhete')),
            operadora_doadora=cls._clean_value(data.get('Operadora doadora') or data.get('operadora_doadora')),
            motivo_recusa=cls._clean_value(data.get('Motivo da recusa') or data.get('motivo_recusa')),
            motivo_cancelamento=cls._clean_value(data.get('Motivo do cancelamento') or data.get('motivo_cancelamento')),
            ultimo_bilhete=cls._parse_ultimo_bilhete(
                data.get('Último bilhete de portabilidade?') or data.get('ultimo_bilhete')
            ),
            motivo_nao_consultado=cls._clean_value(data.get('Motivo de não ter sido consultado') or data.get('motivo_nao_consultado')),
            novo_status_bilhete=cls._clean_value(data.get('Novo status do bilhete') or data.get('novo_status_bilhete')),
            ajustes_numero_acesso=cls._clean_value(data.get('Ajustes número de acesso') or data.get('ajustes_numero_acesso')),
            o_que_aconteceu=cls._clean_value(data.get('O que aconteceu') or data.get('o_que_aconteceu')),
            acao_a_realizar=cls._clean_value(data.get('Ação a ser realizada') or data.get('acao_a_realizar')),
            tipo_mensagem=cls._clean_value(data.get('Tipo de mensagem') or data.get('tipo_mensagem')),
            template=cls._clean_value(data.get('Templete') or data.get('template')),  # Note: typo in xlsx
        )
    
    @staticmethod
    def _clean_value(value):
        """Limpa valores NaN e retorna None para valores vazios"""
        if value is None:
            return None
        if isinstance(value, float):
            import math
            if math.isnan(value):
                return None
        if isinstance(value, str) and value.strip() == '':
            return None
        return value
    
    @staticmethod
    def _parse_ultimo_bilhete(value) -> Optional[bool]:
        """Parse do valor de último bilhete"""
        if value is None:
            return None
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        value_str = str(value).strip().lower()
        if value_str in ['sim', 'yes', 'true', '1', 's']:
            return True
        elif value_str in ['não', 'nao', 'no', 'false', '0', 'n']:
            return False
        return None


@dataclass
class PortabilidadeRecord:
    """Modelo de dados para um registro de portabilidade"""
    
    # Dados básicos (obrigatórios)
    cpf: str
    numero_acesso: str
    numero_ordem: str
    codigo_externo: str
    
    # Bilhetes
    numero_temporario: Optional[str] = None
    bilhete_temporario: Optional[str] = None
    numero_bilhete: Optional[str] = None
    status_bilhete: Optional[PortabilidadeStatus] = None
    
    # Operadora e datas
    operadora_doadora: Optional[str] = None
    data_portabilidade: Optional[datetime] = None
    
    # Motivos
    motivo_recusa: Optional[str] = None
    motivo_cancelamento: Optional[str] = None
    ultimo_bilhete: Optional[bool] = None
    
    # Status da ordem
    status_ordem: Optional[StatusOrdem] = None
    preco_ordem: Optional[str] = None
    data_conclusao_ordem: Optional[datetime] = None
    
    # Motivo de não ter sido consultado (mantido para matching com triggers)
    motivo_nao_consultado: Optional[str] = None
    
    # Motivos adicionais para tratativas específicas
    motivo_nao_cancelado: Optional[str] = None
    motivo_nao_aberto: Optional[str] = None
    motivo_nao_reagendado: Optional[str] = None
    
    # Processamento
    responsavel_processamento: Optional[str] = None
    data_inicial_processamento: Optional[datetime] = None
    data_final_processamento: Optional[datetime] = None
    
    # Validações básicas
    registro_valido: Optional[bool] = None
    numero_acesso_valido: Optional[bool] = None
    ajustes_registro: Optional[str] = None
    ajustes_numero_acesso: Optional[str] = None
    
    # === NOVOS CAMPOS (triggers.xlsx) ===
    regra_id: Optional[int] = None
    o_que_aconteceu: Optional[str] = None
    acao_a_realizar: Optional[str] = None
    tipo_mensagem: Optional[str] = None
    template: Optional[str] = None
    mapeado: bool = True  # True = mapeado, False = não mapeado
    
    # Campos de resultado do matching
    novo_status_bilhete_trigger: Optional[str] = None
    ajustes_numero_acesso_trigger: Optional[str] = None
    novo_status_bilhete: Optional[str] = None
    nova_data_portabilidade: Optional[datetime] = None
    
    # === CAMPOS DE LOGÍSTICA (Relatório de Objetos) ===
    nome_cliente: Optional[str] = None
    telefone_contato: Optional[str] = None
    cidade: Optional[str] = None
    uf: Optional[str] = None
    cep: Optional[str] = None
    cod_rastreio: Optional[str] = None
    data_venda: Optional[datetime] = None
    status_logistica: Optional[str] = None
    
    def to_dict(self) -> dict:
        """Converte o registro para dicionário"""
        return {
            'cpf': self.cpf,
            'numero_acesso': self.numero_acesso,
            'numero_ordem': self.numero_ordem,
            'codigo_externo': self.codigo_externo,
            'numero_temporario': self.numero_temporario,
            'bilhete_temporario': self.bilhete_temporario,
            'numero_bilhete': self.numero_bilhete,
            'status_bilhete': self.status_bilhete.value if self.status_bilhete else None,
            'operadora_doadora': self.operadora_doadora,
            'data_portabilidade': self.data_portabilidade.isoformat() if self.data_portabilidade else None,
            'motivo_recusa': self.motivo_recusa,
            'motivo_cancelamento': self.motivo_cancelamento,
            'ultimo_bilhete': self.ultimo_bilhete,
            'status_ordem': self.status_ordem.value if self.status_ordem else None,
            'preco_ordem': self.preco_ordem,
            'data_conclusao_ordem': self.data_conclusao_ordem.isoformat() if self.data_conclusao_ordem else None,
            'motivo_nao_consultado': self.motivo_nao_consultado,
            'motivo_nao_cancelado': self.motivo_nao_cancelado,
            'motivo_nao_aberto': self.motivo_nao_aberto,
            'motivo_nao_reagendado': self.motivo_nao_reagendado,
            'responsavel_processamento': self.responsavel_processamento,
            'data_inicial_processamento': self.data_inicial_processamento.isoformat() if self.data_inicial_processamento else None,
            'data_final_processamento': self.data_final_processamento.isoformat() if self.data_final_processamento else None,
            'registro_valido': self.registro_valido,
            'numero_acesso_valido': self.numero_acesso_valido,
            'ajustes_registro': self.ajustes_registro,
            'ajustes_numero_acesso': self.ajustes_numero_acesso,
            # Novos campos (triggers.xlsx)
            'regra_id': self.regra_id,
            'o_que_aconteceu': self.o_que_aconteceu,
            'acao_a_realizar': self.acao_a_realizar,
            'tipo_mensagem': self.tipo_mensagem,
            'template': self.template,
            'mapeado': 1 if self.mapeado else 0,
            'novo_status_bilhete_trigger': self.novo_status_bilhete_trigger,
            'ajustes_numero_acesso_trigger': self.ajustes_numero_acesso_trigger,
            'novo_status_bilhete': self.novo_status_bilhete,
            'nova_data_portabilidade': self.nova_data_portabilidade.isoformat() if self.nova_data_portabilidade else None,
            # Campos de logística
            'nome_cliente': self.nome_cliente,
            'telefone_contato': self.telefone_contato,
            'cidade': self.cidade,
            'uf': self.uf,
            'cep': self.cep,
            'cod_rastreio': self.cod_rastreio,
            'data_venda': self.data_venda.isoformat() if self.data_venda else None,
            'status_logistica': self.status_logistica,
        }
    
    def apply_trigger_rule(self, rule: TriggerRule) -> None:
        """Aplica uma regra de trigger ao registro"""
        self.regra_id = rule.regra_id
        self.o_que_aconteceu = rule.o_que_aconteceu
        self.acao_a_realizar = rule.acao_a_realizar
        self.tipo_mensagem = rule.tipo_mensagem
        self.template = rule.template
        self.novo_status_bilhete_trigger = rule.novo_status_bilhete
        self.ajustes_numero_acesso_trigger = rule.ajustes_numero_acesso
        self.mapeado = True
    
    def mark_as_unmapped(self) -> None:
        """Marca o registro como não mapeado"""
        self.mapeado = False
        self.o_que_aconteceu = "NÃO MAPEADO"
        self.acao_a_realizar = "REVISAR REGRAS"
        self.tipo_mensagem = "PENDENTE"
        self.template = None
    
    def get_matching_keys(self) -> dict:
        """Retorna as chaves usadas para matching com triggers"""
        return {
            'status_bilhete': self.status_bilhete.value if self.status_bilhete else None,
            'operadora_doadora': self.operadora_doadora,
            'motivo_recusa': self.motivo_recusa,
            'motivo_cancelamento': self.motivo_cancelamento,
            'ultimo_bilhete': self.ultimo_bilhete,
            'motivo_nao_consultado': self.motivo_nao_consultado,
        }
    
    @staticmethod
    def gerar_link_rastreio(codigo_pedido: Optional[str]) -> Optional[str]:
        """
        Gera o link de rastreio no formato https://tim.trakin.co/o/{numero_pedido}
        
        Args:
            codigo_pedido: Número do pedido ou código externo
            
        Returns:
            Link de rastreio formatado ou None se não houver código
        """
        if not codigo_pedido:
            return None
        
        # Limpar o código (remover espaços e caracteres especiais)
        codigo_limpo = str(codigo_pedido).strip()
        if not codigo_limpo:
            return None
        
        return f"https://tim.trakin.co/o/{codigo_limpo}"
    
    def enrich_with_logistics(self, object_record) -> None:
        """
        Enriquece o registro com dados de logística do Relatório de Objetos
        
        Args:
            object_record: ObjectRecord com dados de logística
        """
        if object_record is None:
            return
        
        self.nome_cliente = object_record.destinatario
        self.telefone_contato = object_record.telefone or self.numero_acesso
        self.cidade = object_record.cidade
        self.uf = object_record.uf
        self.cep = object_record.cep
        self.data_venda = object_record.data_criacao_pedido
        self.status_logistica = object_record.status
        
        # Gerar link de rastreio:
        # Prioridade 1: Nu Pedido do relatório de objetos (mais atualizado)
        # Prioridade 2: Código externo do registro
        if object_record.nu_pedido:
            self.cod_rastreio = self.gerar_link_rastreio(object_record.nu_pedido)
        elif self.codigo_externo:
            self.cod_rastreio = self.gerar_link_rastreio(self.codigo_externo)
        else:
            self.cod_rastreio = object_record.rastreio  # Fallback para rastreio original
    
    def to_wpp_dict(self) -> dict:
        """
        Converte para formato da Régua de Comunicação WPP
        
        Returns:
            Dicionário com campos para planilha WPP
        """
        # Garantir que cod_rastreio seja um link válido
        cod_rastreio = self.cod_rastreio
        if not cod_rastreio or not cod_rastreio.startswith('http'):
            # Gerar link se ainda não for um link válido
            cod_rastreio = self.gerar_link_rastreio(self.codigo_externo) or ''
        
        return {
            'Proposta_iSize': self.codigo_externo,
            'Cpf': self.cpf,
            'NomeCliente': self.nome_cliente or '',
            'Telefone_Contato': self.telefone_contato or self.numero_acesso,
            'Endereco': '',  # Não disponível no relatório de objetos
            'Numero': '',
            'Complemento': '',
            'Bairro': '',
            'Cidade': self.cidade or '',
            'UF': self.uf or '',
            'Cep': self.cep or '',
            'Ponto_Referencia': '',
            'Cod_Rastreio': cod_rastreio,
            'Data_Venda': self.data_venda.strftime('%Y-%m-%d %H:%M:%S') if self.data_venda else '',
            'Tipo_Comunicacao': self.template or '',
            'Status_Disparo': 'FALSE',
            'DataHora_Disparo': '',
        }
