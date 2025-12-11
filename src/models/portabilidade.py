"""
Modelos de dados para registros de portabilidade
"""
from dataclasses import dataclass
from datetime import datetime
from typing import Optional
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


class StatusOrdem(Enum):
    """Status da ordem"""
    CONCLUIDO = "Concluído"
    PENDENTE = "Pendente Portabilidade"
    EM_APROVISIONAMENTO = "Em Aprovisionamento"
    ERRO_APROVISIONAMENTO = "Erro no Aprovisionamento"


@dataclass
class PortabilidadeRecord:
    """Modelo de dados para um registro de portabilidade"""
    
    # Dados básicos
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
    
    # Motivos de não ação
    motivo_nao_consultado: Optional[str] = None
    motivo_nao_cancelado: Optional[str] = None
    motivo_nao_aberto: Optional[str] = None
    motivo_nao_reagendado: Optional[str] = None
    
    # Novos status
    novo_status_bilhete: Optional[PortabilidadeStatus] = None
    nova_data_portabilidade: Optional[datetime] = None
    
    # Processamento
    responsavel_processamento: Optional[str] = None
    data_inicial_processamento: Optional[datetime] = None
    data_final_processamento: Optional[datetime] = None
    
    # Validações
    registro_valido: Optional[bool] = None
    ajustes_registro: Optional[str] = None
    numero_acesso_valido: Optional[bool] = None
    ajustes_numero_acesso: Optional[str] = None
    
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
            'novo_status_bilhete': self.novo_status_bilhete.value if self.novo_status_bilhete else None,
            'nova_data_portabilidade': self.nova_data_portabilidade.isoformat() if self.nova_data_portabilidade else None,
            'responsavel_processamento': self.responsavel_processamento,
            'data_inicial_processamento': self.data_inicial_processamento.isoformat() if self.data_inicial_processamento else None,
            'data_final_processamento': self.data_final_processamento.isoformat() if self.data_final_processamento else None,
            'registro_valido': self.registro_valido,
            'ajustes_registro': self.ajustes_registro,
            'numero_acesso_valido': self.numero_acesso_valido,
            'ajustes_numero_acesso': self.ajustes_numero_acesso,
        }

