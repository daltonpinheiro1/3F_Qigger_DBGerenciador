"""
Gerador de planilhas CSV específicas para Google Drive e Backoffice
"""
import csv
import logging
from pathlib import Path
from typing import List, Dict, Optional
from datetime import datetime
import uuid

from src.models.portabilidade import PortabilidadeRecord, PortabilidadeStatus, StatusOrdem
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.engine.qigger_decision_engine import DecisionResult

logger = logging.getLogger(__name__)


class CSVGenerator:
    """Gerador de planilhas CSV formatadas"""
    
    @staticmethod
    def generate_retornos_qigger_csv(
        records: List[PortabilidadeRecord],
        results_map: Dict[str, List['DecisionResult']],
        output_path: Path
    ) -> bool:
        """
        Gera planilha para Google Drive (Retornos_Qigger) com ID e data_atualizacao
        
        Args:
            records: Lista de registros processados
            results_map: Dicionário mapeando CPF+Ordem para resultados
            output_path: Caminho do arquivo de saída
            
        Returns:
            True se gerado com sucesso
        """
        try:
            # Criar pasta se não existir
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Sempre usar modo append para acumular dados de todas as planilhas
            file_exists = output_path.exists()
            mode = 'a' if file_exists else 'w'
            
            with open(output_path, mode, newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f, delimiter=';')
                
                # Se arquivo novo, escrever cabeçalho
                if not file_exists:
                    headers = [
                        'ID',
                        'Data_Atualizacao',
                        'CPF',
                        'Numero_Acesso',
                        'Numero_Ordem',
                        'Codigo_Externo',
                        'Cod_Rastreio',  # Link de rastreio https://tim.trakin.co/o/{pedido}
                        'Numero_Temporario',
                        'Bilhete_Temporario',
                        'Numero_Bilhete',
                        'Status_Bilhete',
                        'Operadora_Doadora',
                        'Data_Portabilidade',
                        'Motivo_Recusa',
                        'Motivo_Cancelamento',
                        'Ultimo_Bilhete',
                        'Status_Ordem',
                        'Preco_Ordem',
                        'Data_Conclusao_Ordem',
                        'Motivo_Nao_Consultado',
                        'Motivo_Nao_Cancelado',
                        'Motivo_Nao_Aberto',
                        'Motivo_Nao_Reagendado',
                        'Novo_Status_Bilhete',
                        'Nova_Data_Portabilidade',
                        'Responsavel_Processamento',
                        'Data_Inicial_Processamento',
                        'Data_Final_Processamento',
                        'Registro_Valido',
                        'Ajustes_Registro',
                        'Numero_Acesso_Valido',
                        'Ajustes_Numero_Acesso',
                        'Decisoes_Aplicadas',
                        'Acoes_Recomendadas'
                    ]
                    writer.writerow(headers)
                
                # Adicionar registros
                data_atualizacao = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                for record in records:
                    try:
                        # Gerar ID único
                        record_id = str(uuid.uuid4())
                        
                        # Buscar resultados para este registro
                        key = f"{record.cpf}_{record.numero_ordem}"
                        results = results_map.get(key, [])
                        
                        # Formatar decisões e ações (tratar valores None)
                        decisoes = "; ".join([r.decision for r in results if r and r.decision]) if results else ''
                        acoes = "; ".join([r.action for r in results if r and r.action]) if results else ''
                        
                        # Tratar valores None e formatar dados
                        def safe_str(value, default=''):
                            """Converte valor para string de forma segura"""
                            if value is None:
                                return default
                            return str(value)
                        
                        def safe_date(value, default=''):
                            """Formata data de forma segura"""
                            if value is None:
                                return default
                            try:
                                if isinstance(value, datetime):
                                    return value.strftime("%Y-%m-%d %H:%M:%S")
                                return str(value)
                            except:
                                return default
                        
                        def safe_bool(value, default='Não'):
                            """Converte boolean para Sim/Não"""
                            if value is None:
                                return default
                            return 'Sim' if value else 'Não'
                        
                        def safe_enum(value, default=''):
                            """Extrai valor de enum de forma segura"""
                            if value is None:
                                return default
                            try:
                                return value.value if hasattr(value, 'value') else str(value)
                            except:
                                return default
                        
                        # Gerar link de rastreio se não existir
                        cod_rastreio = safe_str(record.cod_rastreio)
                        if not cod_rastreio or not cod_rastreio.startswith('http'):
                            cod_rastreio = PortabilidadeRecord.gerar_link_rastreio(record.codigo_externo) or ''
                        
                        # Montar linha com dados tratados
                        row = [
                            record_id,
                            data_atualizacao,
                            safe_str(record.cpf),
                            safe_str(record.numero_acesso),
                            safe_str(record.numero_ordem),
                            safe_str(record.codigo_externo),
                            cod_rastreio,  # Link de rastreio https://tim.trakin.co/o/{pedido}
                            safe_str(record.numero_temporario),
                            safe_str(record.bilhete_temporario),
                            safe_str(record.numero_bilhete),
                            safe_enum(record.status_bilhete),
                            safe_str(record.operadora_doadora),
                            safe_date(record.data_portabilidade),
                            safe_str(record.motivo_recusa),
                            safe_str(record.motivo_cancelamento),
                            safe_bool(record.ultimo_bilhete),
                            safe_enum(record.status_ordem),
                            safe_str(record.preco_ordem),
                            safe_date(record.data_conclusao_ordem),
                            safe_str(record.motivo_nao_consultado),
                            safe_str(record.motivo_nao_cancelado),
                            safe_str(record.motivo_nao_aberto),
                            safe_str(record.motivo_nao_reagendado),
                            safe_enum(record.novo_status_bilhete),
                            safe_date(record.nova_data_portabilidade),
                            safe_str(record.responsavel_processamento),
                            safe_date(record.data_inicial_processamento),
                            safe_date(record.data_final_processamento),
                            safe_bool(record.registro_valido),
                            safe_str(record.ajustes_registro),
                            safe_bool(record.numero_acesso_valido),
                            safe_str(record.ajustes_numero_acesso),
                            decisoes,
                            acoes
                        ]
                        writer.writerow(row)
                    except Exception as e:
                        logger.error(f"Erro ao processar registro para Retornos_Qigger: {e}")
                        continue
            
            logger.info(f"Planilha Retornos_Qigger gerada: {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Erro ao gerar planilha Retornos_Qigger: {e}")
            return False
    
    @staticmethod
    def generate_aprovisionamentos_csv(
        records: List[PortabilidadeRecord],
        results_map: Dict[str, List['DecisionResult']],
        output_path: Path
    ) -> bool:
        """
        Gera planilha de Aprovisionamentos para Backoffice
        Contém apenas casos aprovados/provisionados
        
        Args:
            records: Lista de registros processados
            results_map: Dicionário mapeando CPF+Ordem para resultados
            output_path: Caminho do arquivo de saída
            
        Returns:
            True se gerado com sucesso
        """
        try:
            # Filtrar apenas casos aprovados/provisionados
            aprovisionados = []
            
            for record in records:
                # Verificar se é caso de aprovisionamento
                is_aprovisionado = False
                
                # Status de ordem em aprovisionamento
                if record.status_ordem == StatusOrdem.EM_APROVISIONAMENTO:
                    is_aprovisionado = True
                
                # Status de bilhete em aprovisionamento
                if record.status_bilhete == PortabilidadeStatus.EM_APROVISIONAMENTO:
                    is_aprovisionado = True
                
                # Verificar resultados de decisão
                key = f"{record.cpf}_{record.numero_ordem}"
                results = results_map.get(key, [])
                
                for result in results:
                    # Decisões que indicam aprovisionamento
                    if result.decision in ['APROVISIONAR', 'CORRIGIR_APROVISIONAMENTO', 'REPROCESSAR']:
                        is_aprovisionado = True
                        break
                    
                    # Regras específicas de aprovisionamento
                    if 'rule_10_erro_aprovisionamento' in result.rule_name:
                        is_aprovisionado = True
                        break
                    
                    if 'rule_21_em_aprovisionamento' in result.rule_name:
                        is_aprovisionado = True
                        break
                
                if is_aprovisionado:
                    aprovisionados.append(record)
            
            if not aprovisionados:
                logger.info("Nenhum caso de aprovisionamento encontrado")
                return False
            
            # Gerar CSV
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_path, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f, delimiter=';')
                
                # Cabeçalho
                headers = [
                    'CPF',
                    'Numero_Acesso',
                    'Numero_Ordem',
                    'Codigo_Externo',
                    'Cod_Rastreio',  # Link de rastreio https://tim.trakin.co/o/{pedido}
                    'Status_Bilhete',
                    'Status_Ordem',
                    'Operadora_Doadora',
                    'Data_Portabilidade',
                    'Preco_Ordem',
                    'Motivo_Recusa',
                    'Motivo_Cancelamento',
                    'Decisoes_Aplicadas',
                    'Acoes_Recomendadas'
                ]
                writer.writerow(headers)
                
                # Funções auxiliares para tratamento seguro
                def safe_str(value, default=''):
                    return str(value) if value is not None else default
                
                def safe_date(value, default=''):
                    if value is None:
                        return default
                    try:
                        if isinstance(value, datetime):
                            return value.strftime("%Y-%m-%d %H:%M:%S")
                        return str(value)
                    except:
                        return default
                
                def safe_enum(value, default=''):
                    if value is None:
                        return default
                    try:
                        return value.value if hasattr(value, 'value') else str(value)
                    except:
                        return default
                
                # Dados
                for record in aprovisionados:
                    try:
                        key = f"{record.cpf}_{record.numero_ordem}"
                        results = results_map.get(key, [])
                        
                        # Formatar decisões e ações (tratar valores None)
                        decisoes = "; ".join([r.decision for r in results if r and r.decision]) if results else ''
                        acoes = "; ".join([r.action for r in results if r and r.action]) if results else ''
                        
                        # Gerar link de rastreio
                        cod_rastreio = safe_str(record.cod_rastreio)
                        if not cod_rastreio or not cod_rastreio.startswith('http'):
                            cod_rastreio = PortabilidadeRecord.gerar_link_rastreio(record.codigo_externo) or ''
                        
                        row = [
                            safe_str(record.cpf),
                            safe_str(record.numero_acesso),
                            safe_str(record.numero_ordem),
                            safe_str(record.codigo_externo),
                            cod_rastreio,  # Link de rastreio
                            safe_enum(record.status_bilhete),
                            safe_enum(record.status_ordem),
                            safe_str(record.operadora_doadora),
                            safe_date(record.data_portabilidade),
                            safe_str(record.preco_ordem),
                            safe_str(record.motivo_recusa),
                            safe_str(record.motivo_cancelamento),
                            decisoes,
                            acoes
                        ]
                        writer.writerow(row)
                    except Exception as e:
                        logger.error(f"Erro ao processar registro de aprovisionamento: {e}")
                        continue
            
            logger.info(f"Planilha Aprovisionamentos gerada: {output_path} ({len(aprovisionados)} registros)")
            return True
            
        except Exception as e:
            logger.error(f"Erro ao gerar planilha Aprovisionamentos: {e}")
            return False
    
    @staticmethod
    def generate_reabertura_csv(
        records: List[PortabilidadeRecord],
        results_map: Dict[str, List['DecisionResult']],
        output_path: Path
    ) -> bool:
        """
        Gera planilha de Reabertura para Backoffice
        Contém casos com status Cancelado ou Pendente Cancelamento
        
        Args:
            records: Lista de registros processados
            results_map: Dicionário mapeando CPF+Ordem para resultados
            output_path: Caminho do arquivo de saída
            
        Returns:
            True se gerado com sucesso
        """
        try:
            # Filtrar casos de cancelamento ou pendente cancelamento
            reabertura = []
            
            for record in records:
                is_reabertura = False
                
                # Status cancelado
                if record.status_bilhete == PortabilidadeStatus.CANCELADA:
                    is_reabertura = True
                
                # Verificar motivos que indicam cancelamento pendente
                if record.motivo_cancelamento:
                    if any(termo in record.motivo_cancelamento.lower() for termo in ['cancelamento', 'cancelado', 'pendente']):
                        is_reabertura = True
                
                # Verificar resultados de decisão
                key = f"{record.cpf}_{record.numero_ordem}"
                results = results_map.get(key, [])
                
                for result in results:
                    # Decisões que indicam reabertura
                    if result.decision in ['CANCELAR', 'REABRIR', 'REAGENDAR']:
                        is_reabertura = True
                        break
                    
                    # Regras específicas de cancelamento
                    if 'rule_05_portabilidade_cancelada' in result.rule_name:
                        is_reabertura = True
                        break
                    
                    if 'rule_14_motivo_cancelamento' in result.rule_name:
                        is_reabertura = True
                        break
                
                if is_reabertura:
                    reabertura.append(record)
            
            if not reabertura:
                logger.info("Nenhum caso de reabertura encontrado")
                return False
            
            # Gerar CSV
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_path, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f, delimiter=';')
                
                # Cabeçalho
                headers = [
                    'CPF',
                    'Numero_Acesso',
                    'Numero_Ordem',
                    'Codigo_Externo',
                    'Cod_Rastreio',  # Link de rastreio https://tim.trakin.co/o/{pedido}
                    'Status_Bilhete',
                    'Status_Ordem',
                    'Operadora_Doadora',
                    'Data_Portabilidade',
                    'Motivo_Cancelamento',
                    'Motivo_Recusa',
                    'Preco_Ordem',
                    'Decisoes_Aplicadas',
                    'Acoes_Recomendadas'
                ]
                writer.writerow(headers)
                
                # Funções auxiliares para tratamento seguro
                def safe_str(value, default=''):
                    return str(value) if value is not None else default
                
                def safe_date(value, default=''):
                    if value is None:
                        return default
                    try:
                        if isinstance(value, datetime):
                            return value.strftime("%Y-%m-%d %H:%M:%S")
                        return str(value)
                    except:
                        return default
                
                def safe_enum(value, default=''):
                    if value is None:
                        return default
                    try:
                        return value.value if hasattr(value, 'value') else str(value)
                    except:
                        return default
                
                # Dados
                for record in reabertura:
                    try:
                        key = f"{record.cpf}_{record.numero_ordem}"
                        results = results_map.get(key, [])
                        
                        # Formatar decisões e ações (tratar valores None)
                        decisoes = "; ".join([r.decision for r in results if r and r.decision]) if results else ''
                        acoes = "; ".join([r.action for r in results if r and r.action]) if results else ''
                        
                        # Gerar link de rastreio
                        cod_rastreio = safe_str(record.cod_rastreio)
                        if not cod_rastreio or not cod_rastreio.startswith('http'):
                            cod_rastreio = PortabilidadeRecord.gerar_link_rastreio(record.codigo_externo) or ''
                        
                        row = [
                            safe_str(record.cpf),
                            safe_str(record.numero_acesso),
                            safe_str(record.numero_ordem),
                            safe_str(record.codigo_externo),
                            cod_rastreio,  # Link de rastreio
                            safe_enum(record.status_bilhete),
                            safe_enum(record.status_ordem),
                            safe_str(record.operadora_doadora),
                            safe_date(record.data_portabilidade),
                            safe_str(record.motivo_cancelamento),
                            safe_str(record.motivo_recusa),
                            safe_str(record.preco_ordem),
                            decisoes,
                            acoes
                        ]
                        writer.writerow(row)
                    except Exception as e:
                        logger.error(f"Erro ao processar registro de reabertura: {e}")
                        continue
            
            logger.info(f"Planilha Reabertura gerada: {output_path} ({len(reabertura)} registros)")
            return True
            
        except Exception as e:
            logger.error(f"Erro ao gerar planilha Reabertura: {e}")
            return False

