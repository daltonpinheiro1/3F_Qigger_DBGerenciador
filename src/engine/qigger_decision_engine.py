"""
Qigger Decision Engine - Motor de decisão para gerenciamento de portabilidade
Implementa 23 regras de negócio para processamento de portabilidade
"""
import logging
import time
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass

from src.models.portabilidade import PortabilidadeRecord, PortabilidadeStatus, StatusOrdem
from src.database.db_manager import DatabaseManager

logger = logging.getLogger(__name__)


@dataclass
class DecisionResult:
    """Resultado de uma decisão da engine"""
    rule_name: str
    decision: str
    action: str
    details: str
    priority: int
    execution_time_ms: float = 0.0


class QiggerDecisionEngine:
    """
    Motor de decisão para processamento de portabilidade
    Implementa 23 regras de negócio baseadas nas políticas da empresa
    """
    
    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        """
        Inicializa a engine de decisão
        
        Args:
            db_manager: Gerenciador de banco de dados (opcional)
        """
        self.db_manager = db_manager
        self.rules_registry: Dict[str, callable] = {}
        self._register_all_rules()
    
    def _register_all_rules(self):
        """Registra todas as 23 regras de negócio"""
        self.rules_registry = {
            'rule_01_validar_cpf': self._rule_01_validar_cpf,
            'rule_02_validar_numero_acesso': self._rule_02_validar_numero_acesso,
            'rule_03_validar_campos_obrigatorios': self._rule_03_validar_campos_obrigatorios,
            'rule_04_cliente_sem_cadastro': self._rule_04_cliente_sem_cadastro,
            'rule_05_portabilidade_cancelada': self._rule_05_portabilidade_cancelada,
            'rule_06_portabilidade_pendente': self._rule_06_portabilidade_pendente,
            'rule_07_portabilidade_concluida': self._rule_07_portabilidade_concluida,
            'rule_08_conflito_detectado': self._rule_08_conflito_detectado,
            'rule_09_falha_parcial': self._rule_09_falha_parcial,
            'rule_10_erro_aprovisionamento': self._rule_10_erro_aprovisionamento,
            'rule_11_erro_sistema': self._rule_11_erro_sistema,
            'rule_12_sem_bilhete_portabilidade': self._rule_12_sem_bilhete_portabilidade,
            'rule_13_motivo_recusa_cliente': self._rule_13_motivo_recusa_cliente,
            'rule_14_motivo_cancelamento_automatico': self._rule_14_motivo_cancelamento_automatico,
            'rule_15_cpf_invalido': self._rule_15_cpf_invalido,
            'rule_16_numero_vago': self._rule_16_numero_vago,
            'rule_17_sem_resposta_sms': self._rule_17_sem_resposta_sms,
            'rule_18_portabilidade_suspensa': self._rule_18_portabilidade_suspensa,
            'rule_19_ordem_concluida': self._rule_19_ordem_concluida,
            'rule_20_ordem_pendente': self._rule_20_ordem_pendente,
            'rule_21_em_aprovisionamento': self._rule_21_em_aprovisionamento,
            'rule_22_validar_datas': self._rule_22_validar_datas,
            'rule_23_priorizar_ultimo_bilhete': self._rule_23_priorizar_ultimo_bilhete,
        }
    
    def process_record(self, record: PortabilidadeRecord) -> List[DecisionResult]:
        """
        Processa um registro aplicando todas as regras relevantes
        
        Args:
            record: Registro de portabilidade a ser processado
            
        Returns:
            Lista de resultados de decisão
        """
        results = []
        
        for rule_name, rule_func in self.rules_registry.items():
            try:
                start_time = time.time()
                result = rule_func(record)
                execution_time = (time.time() - start_time) * 1000
                
                if result:
                    result.execution_time_ms = execution_time
                    results.append(result)
                    
                    # Log no banco de dados se disponível
                    if self.db_manager:
                        # Primeiro, inserir o registro se necessário
                        record_id = self.db_manager.insert_record(record)
                        self.db_manager.log_rule_execution(
                            record_id, rule_name, result.decision, execution_time
                        )
                        self.db_manager.log_decision(
                            record_id, rule_name, result.decision, result.details
                        )
            except Exception as e:
                logger.error(f"Erro ao executar regra {rule_name}: {e}")
        
        # Ordenar por prioridade (menor número = maior prioridade)
        results.sort(key=lambda x: x.priority)
        
        return results
    
    # ========== REGRAS DE VALIDAÇÃO ==========
    
    def _rule_01_validar_cpf(self, record: PortabilidadeRecord) -> Optional[DecisionResult]:
        """Regra 1: Validar formato e consistência do CPF"""
        if not record.cpf or len(record.cpf) != 11 or not record.cpf.isdigit():
            return DecisionResult(
                rule_name="rule_01_validar_cpf",
                decision="REJEITAR",
                action="Marcar registro como inválido",
                details=f"CPF inválido: {record.cpf}. Deve conter 11 dígitos numéricos.",
                priority=1
            )
        return None
    
    def _rule_02_validar_numero_acesso(self, record: PortabilidadeRecord) -> Optional[DecisionResult]:
        """Regra 2: Validar número de acesso (mínimo 11 caracteres)"""
        if not record.numero_acesso:
            return DecisionResult(
                rule_name="rule_02_validar_numero_acesso",
                decision="REJEITAR",
                action="Marcar número de acesso como inválido",
                details="Número de acesso é obrigatório",
                priority=1
            )
        
        if len(record.numero_acesso) < 11:
            return DecisionResult(
                rule_name="rule_02_validar_numero_acesso",
                decision="REJEITAR",
                action="Marcar número de acesso como inválido",
                details=f"Número de acesso deve conter no mínimo 11 caracteres. Atual: {len(record.numero_acesso)}",
                priority=1
            )
        return None
    
    def _rule_03_validar_campos_obrigatorios(self, record: PortabilidadeRecord) -> Optional[DecisionResult]:
        """Regra 3: Validar campos obrigatórios"""
        missing_fields = []
        
        if not record.cpf:
            missing_fields.append("CPF")
        if not record.numero_acesso:
            missing_fields.append("Número de acesso")
        if not record.numero_ordem:
            missing_fields.append("Número da ordem")
        if not record.codigo_externo:
            missing_fields.append("Código externo")
        
        if missing_fields:
            return DecisionResult(
                rule_name="rule_03_validar_campos_obrigatorios",
                decision="REJEITAR",
                action="Marcar registro como inválido",
                details=f"Campos obrigatórios ausentes: {', '.join(missing_fields)}",
                priority=1
            )
        return None
    
    def _rule_22_validar_datas(self, record: PortabilidadeRecord) -> Optional[DecisionResult]:
        """Regra 22: Validar consistência de datas"""
        issues = []
        
        if record.data_portabilidade and record.data_final_processamento:
            if record.data_portabilidade > record.data_final_processamento:
                issues.append("Data de portabilidade posterior à data de processamento")
        
        if record.data_inicial_processamento and record.data_final_processamento:
            if record.data_inicial_processamento > record.data_final_processamento:
                issues.append("Data inicial de processamento posterior à data final")
        
        if issues:
            return DecisionResult(
                rule_name="rule_22_validar_datas",
                decision="AVISAR",
                action="Verificar inconsistências de data",
                details="; ".join(issues),
                priority=5
            )
        return None
    
    # ========== REGRAS DE STATUS ==========
    
    def _rule_04_cliente_sem_cadastro(self, record: PortabilidadeRecord) -> Optional[DecisionResult]:
        """Regra 4: Cliente sem cadastro no sistema"""
        if (record.motivo_nao_consultado and 
            "Cliente sem cadastro" in record.motivo_nao_consultado):
            return DecisionResult(
                rule_name="rule_04_cliente_sem_cadastro",
                decision="PENDENTE",
                action="Criar cadastro do cliente",
                details="Cliente não possui cadastro no sistema. Necessário criar cadastro antes de processar portabilidade.",
                priority=3
            )
        return None
    
    def _rule_05_portabilidade_cancelada(self, record: PortabilidadeRecord) -> Optional[DecisionResult]:
        """Regra 5: Portabilidade cancelada"""
        if record.status_bilhete == PortabilidadeStatus.CANCELADA:
            motivo = record.motivo_cancelamento or record.motivo_recusa or "Não especificado"
            return DecisionResult(
                rule_name="rule_05_portabilidade_cancelada",
                decision="CANCELAR",
                action="Registrar cancelamento e arquivar",
                details=f"Portabilidade cancelada. Motivo: {motivo}",
                priority=2
            )
        return None
    
    def _rule_06_portabilidade_pendente(self, record: PortabilidadeRecord) -> Optional[DecisionResult]:
        """Regra 6: Portabilidade pendente"""
        if record.status_bilhete == PortabilidadeStatus.PENDENTE:
            return DecisionResult(
                rule_name="rule_06_portabilidade_pendente",
                decision="MONITORAR",
                action="Acompanhar status da portabilidade",
                details=f"Portabilidade pendente. Data prevista: {record.data_portabilidade}",
                priority=4
            )
        return None
    
    def _rule_07_portabilidade_concluida(self, record: PortabilidadeRecord) -> Optional[DecisionResult]:
        """Regra 7: Portabilidade concluída com sucesso"""
        if record.status_bilhete == PortabilidadeStatus.CONCLUIDA:
            return DecisionResult(
                rule_name="rule_07_portabilidade_concluida",
                decision="CONCLUIR",
                action="Finalizar processo e atualizar status",
                details=f"Portabilidade concluída com sucesso. Data: {record.data_portabilidade}",
                priority=2
            )
        return None
    
    def _rule_08_conflito_detectado(self, record: PortabilidadeRecord) -> Optional[DecisionResult]:
        """Regra 8: Conflito detectado na portabilidade"""
        if record.status_bilhete == PortabilidadeStatus.CONFLITO:
            return DecisionResult(
                rule_name="rule_08_conflito_detectado",
                decision="RESOLVER_CONFLITO",
                action="Investigar e resolver conflito",
                details=f"Conflito detectado na portabilidade. Operadora: {record.operadora_doadora}",
                priority=2
            )
        return None
    
    def _rule_09_falha_parcial(self, record: PortabilidadeRecord) -> Optional[DecisionResult]:
        """Regra 9: Falha parcial na portabilidade"""
        if record.status_bilhete == PortabilidadeStatus.FALHA_PARCIAL:
            return DecisionResult(
                rule_name="rule_09_falha_parcial",
                decision="REPROCESSAR",
                action="Tentar reprocessar portabilidade",
                details=f"Falha parcial detectada. Data de conclusão: {record.data_conclusao_ordem}",
                priority=3
            )
        return None
    
    def _rule_10_erro_aprovisionamento(self, record: PortabilidadeRecord) -> Optional[DecisionResult]:
        """Regra 10: Erro no aprovisionamento"""
        if (record.status_ordem == StatusOrdem.ERRO_APROVISIONAMENTO or
            record.status_bilhete == PortabilidadeStatus.ERRO_APROVISIONAMENTO):
            return DecisionResult(
                rule_name="rule_10_erro_aprovisionamento",
                decision="CORRIGIR_APROVISIONAMENTO",
                action="Corrigir erro de aprovisionamento",
                details="Erro detectado no processo de aprovisionamento. Necessário intervenção técnica.",
                priority=2
            )
        return None
    
    def _rule_11_erro_sistema(self, record: PortabilidadeRecord) -> Optional[DecisionResult]:
        """Regra 11: Erro do sistema"""
        if (record.motivo_nao_consultado and 
            "[Sistema] Não foi possível processar esse registro" in record.motivo_nao_consultado):
            return DecisionResult(
                rule_name="rule_11_erro_sistema",
                decision="REPROCESSAR",
                action="Tentar reprocessar após correção do sistema",
                details="Erro do sistema impediu o processamento. Necessário reprocessar.",
                priority=2
            )
        return None
    
    def _rule_12_sem_bilhete_portabilidade(self, record: PortabilidadeRecord) -> Optional[DecisionResult]:
        """Regra 12: Nenhum bilhete de portabilidade encontrado"""
        if (record.motivo_nao_consultado and 
            "Nenhum bilhete de portabilidade encontrado" in record.motivo_nao_consultado):
            return DecisionResult(
                rule_name="rule_12_sem_bilhete_portabilidade",
                decision="PESQUISAR",
                action="Pesquisar bilhetes de portabilidade",
                details="Nenhum bilhete de portabilidade foi encontrado para este registro.",
                priority=4
            )
        return None
    
    def _rule_18_portabilidade_suspensa(self, record: PortabilidadeRecord) -> Optional[DecisionResult]:
        """Regra 18: Portabilidade suspensa"""
        if record.status_bilhete == PortabilidadeStatus.SUSPENSA:
            return DecisionResult(
                rule_name="rule_18_portabilidade_suspensa",
                decision="INVESTIGAR",
                action="Investigar motivo da suspensão",
                details=f"Portabilidade suspensa. Data: {record.data_portabilidade}",
                priority=3
            )
        return None
    
    def _rule_19_ordem_concluida(self, record: PortabilidadeRecord) -> Optional[DecisionResult]:
        """Regra 19: Ordem concluída"""
        if record.status_ordem == StatusOrdem.CONCLUIDO:
            return DecisionResult(
                rule_name="rule_19_ordem_concluida",
                decision="ARQUIVAR",
                action="Arquivar ordem concluída",
                details=f"Ordem concluída. Preço: {record.preco_ordem}. Data: {record.data_conclusao_ordem}",
                priority=5
            )
        return None
    
    def _rule_20_ordem_pendente(self, record: PortabilidadeRecord) -> Optional[DecisionResult]:
        """Regra 20: Ordem pendente"""
        if record.status_ordem == StatusOrdem.PENDENTE:
            return DecisionResult(
                rule_name="rule_20_ordem_pendente",
                decision="AGUARDAR",
                action="Aguardar processamento da ordem",
                details="Ordem pendente de processamento",
                priority=4
            )
        return None
    
    def _rule_21_em_aprovisionamento(self, record: PortabilidadeRecord) -> Optional[DecisionResult]:
        """Regra 21: Em aprovisionamento"""
        if (record.status_ordem == StatusOrdem.EM_APROVISIONAMENTO or
            record.status_bilhete == PortabilidadeStatus.EM_APROVISIONAMENTO):
            return DecisionResult(
                rule_name="rule_21_em_aprovisionamento",
                decision="MONITORAR",
                action="Monitorar processo de aprovisionamento",
                details="Registro em processo de aprovisionamento",
                priority=4
            )
        return None
    
    # ========== REGRAS DE MOTIVOS ==========
    
    def _rule_13_motivo_recusa_cliente(self, record: PortabilidadeRecord) -> Optional[DecisionResult]:
        """Regra 13: Rejeição do cliente via SMS"""
        if record.motivo_recusa and "Rejeição do Cliente via SMS" in record.motivo_recusa:
            return DecisionResult(
                rule_name="rule_13_motivo_recusa_cliente",
                decision="CANCELAR",
                action="Cancelar portabilidade por rejeição do cliente",
                details="Cliente rejeitou a portabilidade via SMS",
                priority=2
            )
        return None
    
    def _rule_14_motivo_cancelamento_automatico(self, record: PortabilidadeRecord) -> Optional[DecisionResult]:
        """Regra 14: Cancelamento automático pela BDR"""
        if record.motivo_cancelamento and "Cancelamento Automático pela BDR" in record.motivo_cancelamento:
            return DecisionResult(
                rule_name="rule_14_motivo_cancelamento_automatico",
                decision="CANCELAR",
                action="Registrar cancelamento automático",
                details="Portabilidade cancelada automaticamente pela BDR",
                priority=2
            )
        return None
    
    def _rule_15_cpf_invalido(self, record: PortabilidadeRecord) -> Optional[DecisionResult]:
        """Regra 15: CPF inválido"""
        if record.motivo_recusa and "CPF Inválido" in record.motivo_recusa:
            return DecisionResult(
                rule_name="rule_15_cpf_invalido",
                decision="CORRIGIR",
                action="Corrigir CPF do cliente",
                details="CPF inválido detectado. Necessário correção dos dados.",
                priority=2
            )
        return None
    
    def _rule_16_numero_vago(self, record: PortabilidadeRecord) -> Optional[DecisionResult]:
        """Regra 16: Portabilidade de número vago"""
        if record.motivo_recusa and "Portabillidade de Número Vago" in record.motivo_recusa:
            return DecisionResult(
                rule_name="rule_16_numero_vago",
                decision="REJEITAR",
                action="Rejeitar portabilidade de número vago",
                details="Número está vago e não pode ser portado",
                priority=2
            )
        return None
    
    def _rule_17_sem_resposta_sms(self, record: PortabilidadeRecord) -> Optional[DecisionResult]:
        """Regra 17: Sem resposta do SMS do cliente"""
        if record.motivo_recusa and "Sem Resposta do SMS do Cliente" in record.motivo_recusa:
            return DecisionResult(
                rule_name="rule_17_sem_resposta_sms",
                decision="REAGENDAR",
                action="Reagendar envio de SMS",
                details="Cliente não respondeu ao SMS. Tentar novamente.",
                priority=3
            )
        return None
    
    # ========== REGRAS ESPECIAIS ==========
    
    def _rule_23_priorizar_ultimo_bilhete(self, record: PortabilidadeRecord) -> Optional[DecisionResult]:
        """Regra 23: Priorizar último bilhete de portabilidade"""
        if record.ultimo_bilhete is True:
            return DecisionResult(
                rule_name="rule_23_priorizar_ultimo_bilhete",
                decision="PRIORIZAR",
                action="Processar com prioridade",
                details="Este é o último bilhete de portabilidade. Processar com prioridade.",
                priority=1
            )
        return None
    
    def get_applicable_rules(self, record: PortabilidadeRecord) -> List[str]:
        """
        Retorna lista de regras aplicáveis para um registro
        
        Args:
            record: Registro de portabilidade
            
        Returns:
            Lista de nomes de regras aplicáveis
        """
        applicable = []
        for rule_name, rule_func in self.rules_registry.items():
            try:
                result = rule_func(record)
                if result:
                    applicable.append(rule_name)
            except Exception:
                pass
        return applicable

