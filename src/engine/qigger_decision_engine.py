"""
Qigger Decision Engine - Motor de decisão para gerenciamento de portabilidade
Versão 3.1 - Usa regras dinâmicas do triggers.xlsx + integração com logística
- Processamento batch otimizado com paralelização conceitual
- Cache de regras e resultados
- Geração automática de links de rastreio
"""
import logging
import time
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from dataclasses import dataclass
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.models.portabilidade import PortabilidadeRecord, PortabilidadeStatus, StatusOrdem, TriggerRule
from src.database.db_manager import DatabaseManager
from src.engine.trigger_loader import TriggerLoader
from src.utils.objects_loader import ObjectsLoader
from src.utils.wpp_output_generator import WPPOutputGenerator

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
    regra_id: Optional[int] = None
    o_que_aconteceu: Optional[str] = None
    acao_a_realizar: Optional[str] = None
    tipo_mensagem: Optional[str] = None
    template: Optional[str] = None
    mapeado: bool = True


class QiggerDecisionEngine:
    """
    Motor de decisão para processamento de portabilidade
    Usa regras dinâmicas do triggers.xlsx + integração com logística
    """
    
    def __init__(self, db_manager: Optional[DatabaseManager] = None, 
                 triggers_path: str = "triggers.xlsx",
                 objects_loader: Optional[ObjectsLoader] = None,
                 wpp_output_path: Optional[str] = None):
        """
        Inicializa a engine de decisão
        
        Args:
            db_manager: Gerenciador de banco de dados (opcional)
            triggers_path: Caminho para o arquivo triggers.xlsx
            objects_loader: Loader para Relatório de Objetos (logística)
            wpp_output_path: Caminho para saída da Régua de Comunicação WPP
        """
        self.db_manager = db_manager
        self.trigger_loader = TriggerLoader(triggers_path)
        self.objects_loader = objects_loader
        self.wpp_generator = WPPOutputGenerator(wpp_output_path) if wpp_output_path else None
        self._load_triggers()
    
    def set_objects_loader(self, objects_loader: ObjectsLoader):
        """
        Define o loader de objetos para enriquecimento de dados
        
        Args:
            objects_loader: Loader já carregado
        """
        self.objects_loader = objects_loader
        logger.info(f"ObjectsLoader configurado: {objects_loader.total_records} registros")
    
    def set_wpp_output(self, wpp_output_path: str):
        """
        Define o caminho de saída para Régua de Comunicação WPP
        
        Args:
            wpp_output_path: Caminho para arquivo CSV de saída
        """
        self.wpp_generator = WPPOutputGenerator(wpp_output_path)
        logger.info(f"Saída WPP configurada: {wpp_output_path}")
    
    def _load_triggers(self):
        """Carrega as regras do triggers.xlsx"""
        try:
            self.trigger_loader.load_rules()
            
            # Sincronizar com banco se disponível
            if self.db_manager:
                rules = self.trigger_loader.get_all_rules()
                self.db_manager.sync_triggers_from_loader(rules)
                
            logger.info(f"Carregadas {len(self.trigger_loader.get_all_rules())} regras do triggers.xlsx")
        except FileNotFoundError:
            logger.warning("Arquivo triggers.xlsx não encontrado. Usando regras de validação básicas apenas.")
        except Exception as e:
            logger.error(f"Erro ao carregar triggers: {e}")
    
    def reload_triggers(self):
        """Recarrega as regras do triggers.xlsx"""
        self.trigger_loader.load_rules(force_reload=True)
        if self.db_manager:
            rules = self.trigger_loader.get_all_rules()
            self.db_manager.sync_triggers_from_loader(rules)
        logger.info("Regras recarregadas do triggers.xlsx")
    
    def process_record(self, record: PortabilidadeRecord, save_to_db: bool = True, 
                       enrich_logistics: bool = True) -> List[DecisionResult]:
        """
        Processa um registro aplicando regras do triggers.xlsx
        
        Args:
            record: Registro de portabilidade a ser processado
            save_to_db: Se True, salva no banco de dados
            enrich_logistics: Se True, enriquece com dados de logística
            
        Returns:
            Lista de resultados de decisão
        """
        results = []
        record_id = None
        start_time = time.time()
        
        # Enriquecer com dados de logística se disponível
        if enrich_logistics and self.objects_loader and self.objects_loader.is_loaded:
            obj_record = self.objects_loader.find_best_match(
                codigo_externo=record.codigo_externo,
                id_erp=record.numero_ordem,
                cpf=record.cpf
            )
            if obj_record:
                record.enrich_with_logistics(obj_record)
                logger.debug(f"Registro {record.codigo_externo} enriquecido com dados de logística")
            else:
                # Garantir link de rastreio mesmo sem dados de logística
                if not record.cod_rastreio and record.codigo_externo:
                    record.cod_rastreio = PortabilidadeRecord.gerar_link_rastreio(record.codigo_externo)
        elif not record.cod_rastreio and record.codigo_externo:
            # Garantir link de rastreio quando logística não está habilitada
            record.cod_rastreio = PortabilidadeRecord.gerar_link_rastreio(record.codigo_externo)
        
        # Validações básicas primeiro
        validation_results = self._run_validations(record)
        results.extend(validation_results)
        
        # Buscar regra correspondente no triggers.xlsx
        matched_rule = self.trigger_loader.find_matching_rule(record)
        
        if matched_rule:
            # Aplicar regra ao registro
            record.apply_trigger_rule(matched_rule)
            
            result = DecisionResult(
                rule_name=f"trigger_rule_{matched_rule.regra_id}",
                decision=matched_rule.o_que_aconteceu or "PROCESSADO",
                action=matched_rule.acao_a_realizar or "NENHUMA",
                details=f"Regra {matched_rule.regra_id} aplicada",
                priority=2,
                execution_time_ms=(time.time() - start_time) * 1000,
                regra_id=matched_rule.regra_id,
                o_que_aconteceu=matched_rule.o_que_aconteceu,
                acao_a_realizar=matched_rule.acao_a_realizar,
                tipo_mensagem=matched_rule.tipo_mensagem,
                template=matched_rule.template,
                mapeado=True
            )
            results.append(result)
        else:
            # Registro não mapeado
            record.mark_as_unmapped()
            
            # Adicionar ao triggers.xlsx
            try:
                new_regra_id = self.trigger_loader.add_unmapped_rule(record)
                details = f"Nova regra {new_regra_id} criada no triggers.xlsx para revisão"
            except Exception as e:
                logger.error(f"Erro ao adicionar regra não mapeada: {e}")
                details = "Registro não mapeado - erro ao adicionar ao xlsx"
                new_regra_id = None
            
            result = DecisionResult(
                rule_name="unmapped",
                decision="NÃO MAPEADO",
                action="REVISAR REGRAS",
                details=details,
                priority=10,
                execution_time_ms=(time.time() - start_time) * 1000,
                regra_id=new_regra_id,
                o_que_aconteceu="NÃO MAPEADO",
                acao_a_realizar="REVISAR REGRAS",
                tipo_mensagem="PENDENTE",
                template=None,
                mapeado=False
            )
            results.append(result)
        
        # Salvar no banco
        if save_to_db and self.db_manager:
            try:
                record_id = self.db_manager.insert_record(record)
                
                # Logar decisões
                for result in results:
                    self.db_manager.log_decision(
                        record_id=record_id,
                        rule_name=result.rule_name,
                        decision=result.decision,
                        details=result.details,
                        regra_id=result.regra_id,
                        o_que_aconteceu=result.o_que_aconteceu,
                        acao_a_realizar=result.acao_a_realizar
                    )
                    self.db_manager.log_rule_execution(
                        record_id=record_id,
                        rule_name=result.rule_name,
                        result=result.decision,
                        execution_time_ms=result.execution_time_ms,
                        regra_id=result.regra_id
                    )
                
                # Se não mapeado, registrar para análise
                if not record.mapeado:
                    self.db_manager.log_unmapped_record(record, record_id)
                    
            except Exception as e:
                logger.error(f"Erro ao salvar registro no banco: {e}")
        
        # Ordenar por prioridade
        results.sort(key=lambda x: x.priority)
        
        return results
    
    def process_records_batch(self, records: List[PortabilidadeRecord], 
                               generate_wpp_output: bool = True,
                               save_to_db: bool = True,
                               parallel: bool = False,
                               max_workers: int = 4) -> List[Tuple[PortabilidadeRecord, List[DecisionResult]]]:
        """
        Processa múltiplos registros de forma otimizada
        
        Args:
            records: Lista de registros a serem processados
            generate_wpp_output: Se True, gera saída para Régua de Comunicação WPP
            save_to_db: Se True, salva resultados no banco de dados
            parallel: Se True, processa em paralelo (útil para grandes lotes)
            max_workers: Número máximo de workers para processamento paralelo
            
        Returns:
            Lista de tuplas (registro, resultados)
        """
        if not records:
            return []
        
        start_time = time.time()
        results_list = []
        
        # Verificar se arquivo de triggers foi modificado
        self.trigger_loader.reload_if_modified()
        
        # Pré-enriquecer todos os registros de uma vez se ObjectsLoader disponível
        if self.objects_loader and self.objects_loader.is_loaded:
            self._batch_enrich_logistics(records)
        
        # Processar registros
        if parallel and len(records) > 10:
            # Processamento paralelo para lotes grandes (sem salvar no banco em paralelo)
            results_list = self._process_parallel(records, max_workers)
            
            # Salvar no banco de forma sequencial (SQLite não suporta bem escrita paralela)
            if save_to_db and self.db_manager:
                self._batch_save_to_db(results_list)
        else:
            # Processamento sequencial otimizado
            for record in records:
                results = self.process_record(
                    record, 
                    save_to_db=save_to_db, 
                    enrich_logistics=False  # Já enriquecido em batch
                )
                results_list.append((record, results))
        
        elapsed = time.time() - start_time
        logger.info(f"Batch processado: {len(records)} registros em {elapsed:.2f}s ({len(records)/elapsed:.1f} reg/s)")
        
        # Gerar saída WPP se configurado
        if generate_wpp_output and self.wpp_generator:
            processed_records = [r for r, _ in results_list]
            wpp_file = self.wpp_generator.generate_for_batch(
                processed_records, 
                append=True
            )
            if wpp_file:
                logger.info(f"Gerado arquivo WPP: {wpp_file}")
        
        return results_list
    
    def _batch_enrich_logistics(self, records: List[PortabilidadeRecord]) -> int:
        """
        Enriquece múltiplos registros com dados de logística de uma vez
        
        Args:
            records: Lista de registros
            
        Returns:
            Número de registros enriquecidos
        """
        enriched_count = 0
        
        for record in records:
            if not self.objects_loader:
                continue
                
            obj_record = self.objects_loader.find_best_match(
                codigo_externo=record.codigo_externo,
                id_erp=record.numero_ordem,
                cpf=record.cpf
            )
            
            if obj_record:
                record.enrich_with_logistics(obj_record)
                enriched_count += 1
            else:
                # Garantir que mesmo sem logística, o link de rastreio seja gerado
                if not record.cod_rastreio and record.codigo_externo:
                    record.cod_rastreio = PortabilidadeRecord.gerar_link_rastreio(record.codigo_externo)
        
        logger.debug(f"Enriquecidos {enriched_count}/{len(records)} registros com dados de logística")
        return enriched_count
    
    def _process_parallel(self, records: List[PortabilidadeRecord], max_workers: int) -> List[Tuple[PortabilidadeRecord, List[DecisionResult]]]:
        """
        Processa registros em paralelo (para matching de regras apenas, sem DB)
        
        Args:
            records: Lista de registros
            max_workers: Número máximo de workers
            
        Returns:
            Lista de tuplas (registro, resultados)
        """
        results_dict = {}
        
        def process_single(idx_record):
            idx, record = idx_record
            results = self._process_record_rules_only(record)
            return (idx, record, results)
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(process_single, (i, r)): i 
                for i, r in enumerate(records)
            }
            
            for future in as_completed(futures):
                try:
                    idx, record, results = future.result()
                    results_dict[idx] = (record, results)
                except Exception as e:
                    logger.error(f"Erro no processamento paralelo: {e}")
        
        # Ordenar resultados pela ordem original
        return [results_dict[i] for i in range(len(records)) if i in results_dict]
    
    def _process_record_rules_only(self, record: PortabilidadeRecord) -> List[DecisionResult]:
        """
        Processa um registro aplicando apenas regras (sem salvar no banco)
        
        Args:
            record: Registro a processar
            
        Returns:
            Lista de resultados
        """
        results = []
        start_time = time.time()
        
        # Validações básicas
        validation_results = self._run_validations(record)
        results.extend(validation_results)
        
        # Buscar regra correspondente
        matched_rule = self.trigger_loader.find_matching_rule(record)
        
        if matched_rule:
            record.apply_trigger_rule(matched_rule)
            
            result = DecisionResult(
                rule_name=f"trigger_rule_{matched_rule.regra_id}",
                decision=matched_rule.o_que_aconteceu or "PROCESSADO",
                action=matched_rule.acao_a_realizar or "NENHUMA",
                details=f"Regra {matched_rule.regra_id} aplicada",
                priority=2,
                execution_time_ms=(time.time() - start_time) * 1000,
                regra_id=matched_rule.regra_id,
                o_que_aconteceu=matched_rule.o_que_aconteceu,
                acao_a_realizar=matched_rule.acao_a_realizar,
                tipo_mensagem=matched_rule.tipo_mensagem,
                template=matched_rule.template,
                mapeado=True
            )
            results.append(result)
        else:
            record.mark_as_unmapped()
            
            result = DecisionResult(
                rule_name="unmapped",
                decision="NÃO MAPEADO",
                action="REVISAR REGRAS",
                details="Registro não mapeado",
                priority=10,
                execution_time_ms=(time.time() - start_time) * 1000,
                o_que_aconteceu="NÃO MAPEADO",
                acao_a_realizar="REVISAR REGRAS",
                tipo_mensagem="PENDENTE",
                mapeado=False
            )
            results.append(result)
        
        results.sort(key=lambda x: x.priority)
        return results
    
    def _batch_save_to_db(self, results_list: List[Tuple[PortabilidadeRecord, List[DecisionResult]]]) -> int:
        """
        Salva múltiplos resultados no banco de dados
        
        Args:
            results_list: Lista de tuplas (registro, resultados)
            
        Returns:
            Número de registros salvos
        """
        if not self.db_manager:
            return 0
        
        saved_count = 0
        
        # Usar insert em batch quando possível
        records_to_save = [r for r, _ in results_list]
        
        try:
            record_ids = self.db_manager.insert_records_batch(records_to_save)
            
            # Logar decisões para cada registro
            for (record, results), record_id in zip(results_list, record_ids):
                for result in results:
                    try:
                        self.db_manager.log_decision(
                            record_id=record_id,
                            rule_name=result.rule_name,
                            decision=result.decision,
                            details=result.details,
                            regra_id=result.regra_id,
                            o_que_aconteceu=result.o_que_aconteceu,
                            acao_a_realizar=result.acao_a_realizar
                        )
                    except Exception as e:
                        logger.error(f"Erro ao logar decisão: {e}")
                
                # Registrar não mapeados
                if not record.mapeado:
                    try:
                        self.db_manager.log_unmapped_record(record, record_id)
                    except Exception as e:
                        logger.error(f"Erro ao logar registro não mapeado: {e}")
                
                saved_count += 1
                
        except Exception as e:
            logger.error(f"Erro ao salvar batch no banco: {e}")
        
        return saved_count
    
    def generate_wpp_output(self, records: List[PortabilidadeRecord], 
                           output_path: Optional[str] = None,
                           timestamped: bool = False) -> Optional[str]:
        """
        Gera arquivo de saída para Régua de Comunicação WPP
        
        Args:
            records: Lista de registros processados
            output_path: Caminho para saída (opcional, usa padrão se não especificado)
            timestamped: Se True, adiciona timestamp ao nome do arquivo
            
        Returns:
            Caminho do arquivo gerado ou None
        """
        generator = self.wpp_generator or WPPOutputGenerator(output_path)
        
        if timestamped:
            output_dir = Path(output_path).parent if output_path else None
            return generator.generate_timestamped(records, output_dir=str(output_dir) if output_dir else None)
        else:
            return generator.generate_for_batch(records, output_path=output_path)
    
    def _run_validations(self, record: PortabilidadeRecord) -> List[DecisionResult]:
        """
        Executa validações básicas no registro
        
        Args:
            record: Registro a validar
            
        Returns:
            Lista de resultados de validação
        """
        results = []
        
        # Validar CPF
        cpf_result = self._validate_cpf(record)
        if cpf_result:
            results.append(cpf_result)
        
        # Validar número de acesso
        acesso_result = self._validate_numero_acesso(record)
        if acesso_result:
            results.append(acesso_result)
        
        # Validar campos obrigatórios
        campos_result = self._validate_campos_obrigatorios(record)
        if campos_result:
            results.append(campos_result)
        
        return results
    
    @staticmethod
    def _validar_digitos_verificadores_cpf(cpf: str) -> bool:
        """
        Valida os dígitos verificadores do CPF usando o algoritmo oficial
        """
        if len(cpf) != 11:
            return False
        
        if cpf == cpf[0] * 11:
            return False
        
        soma = sum(int(cpf[i]) * (10 - i) for i in range(9))
        digito1 = 11 - (soma % 11)
        if digito1 >= 10:
            digito1 = 0
        
        soma = sum(int(cpf[i]) * (11 - i) for i in range(10))
        digito2 = 11 - (soma % 11)
        if digito2 >= 10:
            digito2 = 0
        
        return int(cpf[9]) == digito1 and int(cpf[10]) == digito2
    
    def _validate_cpf(self, record: PortabilidadeRecord) -> Optional[DecisionResult]:
        """Valida formato e consistência do CPF"""
        if not record.cpf:
            return DecisionResult(
                rule_name="validation_cpf",
                decision="REJEITAR",
                action="Marcar registro como inválido",
                details="CPF não fornecido",
                priority=1
            )
        
        cpf_limpo = ''.join(filter(str.isdigit, record.cpf))
        
        if len(cpf_limpo) != 11:
            return DecisionResult(
                rule_name="validation_cpf",
                decision="REJEITAR",
                action="Marcar registro como inválido",
                details=f"CPF inválido: {record.cpf}. Deve conter 11 dígitos numéricos.",
                priority=1
            )
        
        if not self._validar_digitos_verificadores_cpf(cpf_limpo):
            return DecisionResult(
                rule_name="validation_cpf",
                decision="REJEITAR",
                action="Marcar registro como inválido",
                details=f"CPF inválido: {record.cpf}. Dígitos verificadores incorretos.",
                priority=1
            )
        
        return None
    
    def _validate_numero_acesso(self, record: PortabilidadeRecord) -> Optional[DecisionResult]:
        """Valida número de acesso"""
        if not record.numero_acesso:
            return DecisionResult(
                rule_name="validation_numero_acesso",
                decision="REJEITAR",
                action="Marcar número de acesso como inválido",
                details="Número de acesso é obrigatório",
                priority=1
            )
        
        if len(record.numero_acesso) < 11:
            return DecisionResult(
                rule_name="validation_numero_acesso",
                decision="REJEITAR",
                action="Marcar número de acesso como inválido",
                details=f"Número de acesso deve conter no mínimo 11 caracteres. Atual: {len(record.numero_acesso)}",
                priority=1
            )
        
        return None
    
    def _validate_campos_obrigatorios(self, record: PortabilidadeRecord) -> Optional[DecisionResult]:
        """Valida campos obrigatórios"""
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
                rule_name="validation_campos_obrigatorios",
                decision="REJEITAR",
                action="Marcar registro como inválido",
                details=f"Campos obrigatórios ausentes: {', '.join(missing_fields)}",
                priority=1
            )
        
        return None
    
    def get_rules_stats(self) -> Dict:
        """Retorna estatísticas das regras"""
        return self.trigger_loader.get_rules_stats()
    
    def get_applicable_rules_preview(self, record: PortabilidadeRecord) -> Optional[TriggerRule]:
        """
        Retorna a regra que seria aplicada a um registro (preview sem salvar)
        
        Args:
            record: Registro de portabilidade
            
        Returns:
            Regra que seria aplicada ou None
        """
        return self.trigger_loader.find_matching_rule(record)
    
    def get_logistics_stats(self) -> dict:
        """Retorna estatísticas do loader de objetos"""
        if self.objects_loader:
            return self.objects_loader.get_stats()
        return {'loaded': False, 'total_records': 0}
    
    def get_wpp_stats(self) -> dict:
        """Retorna estatísticas do gerador WPP"""
        if self.wpp_generator:
            return self.wpp_generator.get_stats()
        return {'total_pending': 0, 'output_path': None}
    
    def get_full_stats(self) -> dict:
        """Retorna estatísticas completas do engine"""
        return {
            'triggers': self.get_rules_stats(),
            'logistics': self.get_logistics_stats(),
            'wpp': self.get_wpp_stats(),
        }
