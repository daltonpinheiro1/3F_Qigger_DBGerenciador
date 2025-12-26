"""
Gerador de planilhas CSV específicas para Google Drive e Backoffice
"""
import csv
import logging
from pathlib import Path
from typing import List, Dict, Optional
from datetime import datetime
from collections import defaultdict
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
        output_path: Path,
        objects_loader=None
    ) -> bool:
        """
        Gera planilha de Aprovisionamentos para Backoffice
        Formato completo com todas as colunas do CSV original
        Filtro: Em aprovisionamento E (entregue OU status 6)
        
        Args:
            records: Lista de registros processados
            results_map: Dicionário mapeando CPF+Ordem para resultados
            output_path: Caminho do arquivo de saída
            objects_loader: Loader de objetos para verificar status de entrega (opcional)
            
        Returns:
            True se gerado com sucesso
        """
        try:
            # Filtrar casos de aprovisionamento E entregue
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
                    # Regras específicas de aprovisionamento
                    if 'rule_10_erro_aprovisionamento' in result.rule_name:
                        is_aprovisionado = True
                        break
                    
                    if 'rule_21_em_aprovisionamento' in result.rule_name:
                        is_aprovisionado = True
                        break
                
                if not is_aprovisionado:
                    continue
                
                # Verificar se está entregue (regra: entregue OU status 6)
                is_entregue = False
                
                # Verificar status de logística (status_logistica pode conter "6" ou "Pedido entregue")
                if record.status_logistica:
                    status_str = str(record.status_logistica).lower()
                    if '6' in status_str or 'entregue' in status_str or 'entreg' in status_str:
                        is_entregue = True
                
                # Verificar no ObjectsLoader se disponível
                if not is_entregue and objects_loader:
                    obj_match = objects_loader.find_best_match(
                        codigo_externo=record.codigo_externo,
                        cpf=record.cpf
                    )
                    if obj_match:
                        # Verificar data de entrega
                        if hasattr(obj_match, 'data_entrega') and obj_match.data_entrega:
                            is_entregue = True
                        
                        # Verificar status (6 ou "Pedido entregue")
                        if hasattr(obj_match, 'status') and obj_match.status:
                            status_str = str(obj_match.status).lower()
                            if '6' in status_str or 'entregue' in status_str or 'entreg' in status_str:
                                is_entregue = True
                
                # Aplicar filtro: aprovisionamento E entregue
                if is_aprovisionado and is_entregue:
                    aprovisionados.append(record)
            
            if not aprovisionados:
                logger.info("Nenhum caso de aprovisionamento com entrega encontrado")
                return False
            
            # Gerar CSV com todas as colunas do modelo original
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_path, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f, delimiter=';')
                
                # Cabeçalho completo conforme modelo
                headers = [
                    'Cpf',
                    'Número de acesso',
                    'Número da ordem',
                    'Código externo',
                    'ToutBox',
                    'Número do bilhete',
                    'Status do bilhete',
                    'Operadora doadora',
                    'Data da portabilidade',
                    'Motivo da recusa',
                    'Motivo do cancelamento',
                    'Último bilhete de portabilidade?',
                    'Status da ordem',
                    'Preço da ordem',
                    'Data da conclusão da ordem',
                    'Motivo de não ter sido consultado',
                    'Motivo de não ter sido cancelado',
                    'Motivo de não ter sido aberto',
                    'Motivo de não ter sido reagendado',
                    'Novo status do bilhete',
                    'Nova data da portabilidade',
                    'Responsável pelo processamento',
                    'Data inicial do processamento',
                    'Data final do processamento',
                    'Registro válido?',
                    'Ajustes registro',
                    'Número de acesso válido?',
                    'Ajustes número de acesso'
                ]
                writer.writerow(headers)
                
                # Funções auxiliares
                def safe_str(value, default=''):
                    return str(value) if value is not None else default
                
                def safe_date(value, default=''):
                    if value is None:
                        return default
                    try:
                        if isinstance(value, datetime):
                            return value.strftime("%d/%m/%Y")
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
                
                def safe_bool(value, default=''):
                    if value is None:
                        return default
                    return 'Sim' if value else 'Não'
                
                # Dados
                for record in aprovisionados:
                    try:
                        row = [
                            safe_str(record.cpf),
                            safe_str(record.numero_acesso),
                            safe_str(record.numero_ordem),
                            safe_str(record.codigo_externo),
                            '',  # ToutBox (não temos no modelo)
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
                            safe_str(record.novo_status_bilhete),
                            safe_date(record.nova_data_portabilidade),
                            safe_str(record.responsavel_processamento),
                            safe_date(record.data_inicial_processamento),
                            safe_date(record.data_final_processamento),
                            safe_bool(record.registro_valido),
                            safe_str(record.ajustes_registro),
                            safe_bool(record.numero_acesso_valido),
                            safe_str(record.ajustes_numero_acesso)
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
        Formato: Agrupa por CPF com múltiplos números de acesso, ordens e códigos externos (até 5)
        
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
            
            # Agrupar por CPF
            from collections import defaultdict
            grupos_cpf = defaultdict(list)
            
            for record in reabertura:
                grupos_cpf[record.cpf].append(record)
            
            # Gerar CSV
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_path, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f, delimiter='\t')  # Usar TAB como delimitador (conforme modelo)
                
                # Cabeçalho conforme modelo
                headers = [
                    'Cpf',
                    'Plano',
                    'Preço',
                    'Número de acesso 1',
                    'Número de acesso 2',
                    'Número de acesso 3',
                    'Número de acesso 4',
                    'Número de acesso 5',
                    'Número da ordem 1',
                    'Número da ordem 2',
                    'Número da ordem 3',
                    'Número da ordem 4',
                    'Número da ordem 5',
                    'Código externo 1',
                    'Código externo 2',
                    'Código externo 3',
                    'Código externo 4',
                    'Código externo 5'
                ]
                writer.writerow(headers)
                
                # Funções auxiliares
                def safe_str(value, default=''):
                    return str(value) if value is not None else default
                
                # Processar cada grupo de CPF
                for cpf, registros_cpf in grupos_cpf.items():
                    # Limitar a 5 registros por CPF
                    registros_cpf = registros_cpf[:5]
                    
                    # Preencher arrays (máximo 5)
                    numeros_acesso = [safe_str(r.numero_acesso) for r in registros_cpf] + [''] * (5 - len(registros_cpf))
                    numeros_ordem = [safe_str(r.numero_ordem) for r in registros_cpf] + [''] * (5 - len(registros_cpf))
                    codigos_externo = [safe_str(r.codigo_externo) for r in registros_cpf] + [''] * (5 - len(registros_cpf))
                    
                    # Pegar Plano e Preço do primeiro registro (se disponível)
                    primeiro = registros_cpf[0]
                    plano = ''  # Não temos campo Plano no modelo atual
                    preco = safe_str(primeiro.preco_ordem, '').replace('R$', '').replace(',', '.').strip()
                    
                    # Montar linha
                    row = [
                        safe_str(cpf),
                        plano,
                        preco,
                        numeros_acesso[0],
                        numeros_acesso[1],
                        numeros_acesso[2],
                        numeros_acesso[3],
                        numeros_acesso[4],
                        numeros_ordem[0],
                        numeros_ordem[1],
                        numeros_ordem[2],
                        numeros_ordem[3],
                        numeros_ordem[4],
                        codigos_externo[0],
                        codigos_externo[1],
                        codigos_externo[2],
                        codigos_externo[3],
                        codigos_externo[4]
                    ]
                    writer.writerow(row)
            
            logger.info(f"Planilha Reabertura gerada: {output_path} ({len(grupos_cpf)} CPFs, {len(reabertura)} registros)")
            return True
            
        except Exception as e:
            logger.error(f"Erro ao gerar planilha Reabertura: {e}")
            return False

