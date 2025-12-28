"""
Gerador de planilhas CSV específicas para Google Drive e Backoffice
"""
import csv
import logging
from pathlib import Path
from typing import List, Dict, Optional, TYPE_CHECKING
from datetime import datetime
from collections import defaultdict
import uuid

import pandas as pd

from src.models.portabilidade import PortabilidadeRecord, PortabilidadeStatus, StatusOrdem

if TYPE_CHECKING:
    from src.engine.qigger_decision_engine import DecisionResult

logger = logging.getLogger(__name__)


def sintetizar_texto(texto: str, max_caracteres: int = 80) -> str:
    """
    Sintetiza texto longo para melhor visualização no Excel
    Remove quebras de linha, espaços múltiplos e torna o texto mais objetivo
    
    Args:
        texto: Texto a ser sintetizado
        max_caracteres: Número máximo de caracteres (padrão: 80)
        
    Returns:
        Texto sintetizado e limpo
    """
    if not texto:
        return ''
    
    texto_str = str(texto).strip()
    
    # Remover quebras de linha e caracteres de controle
    texto_str = texto_str.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
    
    # Remover espaços múltiplos
    while '  ' in texto_str:
        texto_str = texto_str.replace('  ', ' ')
    
    # Se houver múltiplas entradas numeradas [1], [2], etc., pegar apenas a primeira
    if texto_str.count('[') > 1 and texto_str.count(']') > 1:
        # Dividir por [ e pegar apenas a primeira parte significativa
        partes = texto_str.split('[')
        if len(partes) >= 2:
            # Pegar a primeira entrada numerada completa
            primeira_entrada = partes[1].split(']', 1)
            if len(primeira_entrada) > 1:
                texto_str = primeira_entrada[1].strip()
            else:
                # Se não tem ], pegar tudo até o próximo [
                texto_str = primeira_entrada[0].strip()
    
    # Remover prefixos [1], [2] do início se existirem
    texto_str = texto_str.strip()
    while texto_str.startswith('[') and ']' in texto_str:
        pos_fecha = texto_str.find(']')
        if pos_fecha > 0 and pos_fecha < 5:  # Prefixo curto como [1], [2]
            texto_str = texto_str[pos_fecha + 1:].strip()
        else:
            break
    
    # Limpar novamente espaços múltiplos
    while '  ' in texto_str:
        texto_str = texto_str.replace('  ', ' ')
    
    # Extrair apenas a parte mais importante (antes de ponto final ou vírgula se muito longo)
    # Se o texto tiver mais de max_caracteres, tentar pegar apenas a primeira frase
    if len(texto_str) > max_caracteres:
        # Procurar primeiro ponto final, ponto e vírgula ou vírgula
        for separador in ['. ', '; ', ', ']:
            pos_sep = texto_str.find(separador)
            if pos_sep > 0 and pos_sep <= max_caracteres:
                texto_str = texto_str[:pos_sep + 1].strip()
                break
        
        # Se ainda estiver muito longo, truncar de forma inteligente
        if len(texto_str) > max_caracteres:
            texto_truncado = texto_str[:max_caracteres - 3]
            # Procurar último espaço antes do limite para não cortar palavras
            ultimo_espaco = texto_truncado.rfind(' ')
            if ultimo_espaco > max_caracteres * 0.6:  # Pelo menos 60% do texto
                texto_str = texto_truncado[:ultimo_espaco].strip()
            else:
                texto_str = texto_truncado.strip()
            
            # Adicionar "..." apenas se realmente foi truncado
            if len(texto_str) < len(texto):
                texto_str = texto_str + '...'
    
    return texto_str


def formatar_iccid_como_texto(iccid: any) -> str:
    """
    Formata ICCID como texto para preservar todos os dígitos no Excel
    
    Args:
        iccid: Valor do ICCID (pode ser string, int, float)
        
    Returns:
        ICCID formatado como string completa
    """
    if not iccid:
        return ''
    
    # Converter para string preservando todos os caracteres
    iccid_str = str(iccid).strip()
    
    # Remover espaços e caracteres especiais que possam causar problemas
    iccid_str = ''.join(c for c in iccid_str if c.isdigit() or c.isalnum())
    
    # Garantir que seja tratado como texto no Excel (adicionar prefixo TAB)
    # O Excel interpreta valores que começam com TAB como texto
    # Mas como estamos usando CSV, vamos garantir que seja string completa
    return iccid_str


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
        objects_loader=None,
        base_analitica_loader=None
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
                # Status da ordem deve ser "Em Aprovisionamento" ou "Erro no Aprovisionamento"
                is_aprovisionado = False
                
                # Status de ordem em aprovisionamento ou erro no aprovisionamento
                if record.status_ordem == StatusOrdem.EM_APROVISIONAMENTO:
                    is_aprovisionado = True
                elif record.status_ordem == StatusOrdem.ERRO_APROVISIONAMENTO:
                    is_aprovisionado = True
                
                # Status de bilhete em aprovisionamento (opcional, mas mantém compatibilidade)
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
                
                # EXCLUIR registros com motivos específicos
                # Verificar Motivo da recusa
                motivo_recusa = str(record.motivo_recusa or '').strip()
                motivo_cancelamento = str(record.motivo_cancelamento or '').strip()
                
                motivos_excluir = [
                    'Rejeição do Cliente via SMS',
                    'CPF Inválido',
                    'Portabilidade de Número Vago',
                    'Portabillidade de Número Vago',  # Com erro de digitação
                    'Tipo de cliente inválido'
                ]
                
                # Verificar se algum motivo de exclusão está presente
                deve_excluir = False
                for motivo in motivos_excluir:
                    if motivo.lower() in motivo_recusa.lower() or motivo.lower() in motivo_cancelamento.lower():
                        deve_excluir = True
                        break
                
                if deve_excluir:
                    continue
                
                # Verificar se está entregue
                # PRIORIDADE: Última Ocorrência (Relatório de Objetos) > Base Analítica > Status/Data Entrega
                is_entregue = False
                
                # PRIORIDADE 1: Verificar Última Ocorrência no ObjectsLoader (Relatório de Objetos)
                if objects_loader:
                    obj_match = objects_loader.find_best_match(
                        codigo_externo=record.codigo_externo,
                        cpf=record.cpf
                    )
                    if obj_match:
                        # Verificar Última Ocorrência (prioridade máxima)
                        # Excluir "Entrega Cancelada" da contabilização
                        if hasattr(obj_match, 'ultima_ocorrencia') and obj_match.ultima_ocorrencia:
                            ultima_ocorrencia_str = str(obj_match.ultima_ocorrencia).lower()
                            # Excluir entrega cancelada
                            if 'entrega cancelada' not in ultima_ocorrencia_str and 'cancelada' not in ultima_ocorrencia_str:
                                if any(termo in ultima_ocorrencia_str for termo in ['pedido entregue', 'entregue', '6']):
                                    is_entregue = True
                        
                        # Se não encontrou em Última Ocorrência, verificar Status
                        if not is_entregue and hasattr(obj_match, 'status') and obj_match.status:
                            status_str = str(obj_match.status).lower()
                            if any(termo in status_str for termo in ['pedido entregue', 'entregue', '6']):
                                is_entregue = True
                        
                        # Se não encontrou, verificar data de entrega
                        if not is_entregue and hasattr(obj_match, 'data_entrega') and obj_match.data_entrega:
                            is_entregue = True
                        
                        # Se não encontrou, verificar ICCID (se possui ICCID, considera entregue)
                        if not is_entregue:
                            if hasattr(obj_match, 'iccid') and obj_match.iccid:
                                iccid_str = str(obj_match.iccid).strip()
                                if iccid_str and iccid_str.lower() != 'nan':
                                    is_entregue = True
                            elif hasattr(obj_match, 'chip_id') and obj_match.chip_id:
                                chip_id_str = str(obj_match.chip_id).strip()
                                if chip_id_str and chip_id_str.lower() != 'nan':
                                    is_entregue = True
                
                # PRIORIDADE 2: Verificar na Base Analítica (se disponível e não encontrou ainda)
                # Nota: Base Analítica será verificada no script de homologação se necessário
                
                # PRIORIDADE 3: Verificar ICCID na Base Analítica (se possui ICCID, considera entregue)
                if not is_entregue and base_analitica_loader and hasattr(base_analitica_loader, 'is_loaded') and base_analitica_loader.is_loaded:
                    base_match = base_analitica_loader.find_by_codigo_externo(record.codigo_externo)
                    if base_match is None and record.cpf:
                        if hasattr(base_analitica_loader, 'find_by_cpf'):
                            base_match = base_analitica_loader.find_by_cpf(record.cpf)
                    
                    if base_match is not None and isinstance(base_match, pd.Series):
                        # Verificar ICCID na Base Analítica
                        for col_name in ['ICCID', 'Chip ID', 'chip_id', 'Chip_ID', 'ICCID/Chip']:
                            if col_name in base_match.index:
                                iccid_val = base_match[col_name]
                                if pd.notna(iccid_val):
                                    iccid_str = str(iccid_val).strip()
                                    if iccid_str and iccid_str.lower() != 'nan':
                                        is_entregue = True
                                        break
                
                # PRIORIDADE 4: Verificar status de logística do record (fallback)
                if not is_entregue and record.status_logistica:
                    status_str = str(record.status_logistica).lower()
                    if any(termo in status_str for termo in ['pedido entregue', 'entregue', '6']):
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
                    'ICCID',  # Coluna E - ICCID ou chip_id
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
                    'Ajustes número de acesso',
                    'Status da entrega',
                    'Data da entrega',
                    'Parâmetro de Identificação',
                    'Data Última Atualização Coleta',
                    'Tipo de Venda'  # Nova coluna: Portabilidade ou Nova Linha
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
                        # Classificar tipo de venda: Portabilidade ou Nova Linha
                        # Portabilidade: tem operadora doadora OU data de portabilidade
                        tipo_venda = 'Nova Linha'
                        if record.operadora_doadora and str(record.operadora_doadora).strip():
                            tipo_venda = 'Portabilidade'
                        elif record.data_portabilidade:
                            tipo_venda = 'Portabilidade'
                        
                        # Buscar dados de entrega do Relatório de Objetos
                        # PRIORIDADE: Última Ocorrência > Bluechip Status > Data Entrega
                        status_entrega = ''
                        data_entrega = ''
                        iccid = ''  # ICCID ou chip_id (como texto)
                        parametro_identificacao = ''  # Parâmetro de identificação
                        data_ultima_atualizacao = ''  # Data da última atualização da coleta
                        
                        if objects_loader:
                            obj_match = objects_loader.find_best_match(
                                codigo_externo=record.codigo_externo,
                                cpf=record.cpf
                            )
                            if obj_match:
                                # PRIORIDADE 1: Última Ocorrência (excluir "Entrega Cancelada")
                                # Montar status completo com todos os detalhes disponíveis
                                status_parts = []
                                
                                # Última Ocorrência (principal)
                                if hasattr(obj_match, 'ultima_ocorrencia') and obj_match.ultima_ocorrencia:
                                    ultima_ocorrencia_str = str(obj_match.ultima_ocorrencia).lower()
                                    # Excluir entrega cancelada
                                    if 'entrega cancelada' not in ultima_ocorrencia_str and 'cancelada' not in ultima_ocorrencia_str:
                                        status_parts.append(safe_str(obj_match.ultima_ocorrencia))
                                
                                # Se não encontrou na Última Ocorrência, tentar Última Ocorrência Cronológica
                                if not status_parts and hasattr(obj_match, 'ultima_ocorrencia_cronologica') and obj_match.ultima_ocorrencia_cronologica:
                                    ultima_ocorrencia_cron_str = str(obj_match.ultima_ocorrencia_cronologica).lower()
                                    if 'entrega cancelada' not in ultima_ocorrencia_cron_str and 'cancelada' not in ultima_ocorrencia_cron_str:
                                        status_parts.append(safe_str(obj_match.ultima_ocorrencia_cronologica))
                                
                                # Adicionar detalhes adicionais se disponíveis
                                detalhes = []
                                if hasattr(obj_match, 'local_ultima_ocorrencia') and obj_match.local_ultima_ocorrencia:
                                    detalhes.append(f"Local: {safe_str(obj_match.local_ultima_ocorrencia)}")
                                if hasattr(obj_match, 'cidade_ultima_ocorrencia') and obj_match.cidade_ultima_ocorrencia:
                                    cidade = safe_str(obj_match.cidade_ultima_ocorrencia)
                                    estado = safe_str(obj_match.estado_ultima_ocorrencia) if hasattr(obj_match, 'estado_ultima_ocorrencia') and obj_match.estado_ultima_ocorrencia else ''
                                    if estado:
                                        detalhes.append(f"{cidade}/{estado}")
                                    else:
                                        detalhes.append(cidade)
                                
                                # Montar status completo
                                if status_parts:
                                    status_entrega = status_parts[0]  # Status principal
                                    if detalhes:
                                        status_entrega += f" - {', '.join(detalhes)}"
                                
                                # Data da entrega
                                if hasattr(obj_match, 'data_entrega') and obj_match.data_entrega:
                                    data_entrega = safe_date(obj_match.data_entrega)
                                
                                # ICCID ou chip_id (buscar no Relatório de Objetos)
                                # Garantir que seja texto para preservar todos os dígitos
                                if hasattr(obj_match, 'iccid') and obj_match.iccid:
                                    iccid = formatar_iccid_como_texto(obj_match.iccid)
                                elif hasattr(obj_match, 'chip_id') and obj_match.chip_id:
                                    iccid = formatar_iccid_como_texto(obj_match.chip_id)
                                
                                # Parâmetro de identificação e data da última atualização
                                # Usar data_insercao como data da última atualização da coleta
                                if hasattr(obj_match, 'data_insercao') and obj_match.data_insercao:
                                    data_ultima_atualizacao = safe_date(obj_match.data_insercao)
                                
                                # Parâmetro de identificação pode ser o código externo ou nu_pedido
                                if hasattr(obj_match, 'nu_pedido') and obj_match.nu_pedido:
                                    parametro_identificacao = safe_str(obj_match.nu_pedido)
                                elif record.codigo_externo:
                                    parametro_identificacao = safe_str(record.codigo_externo)
                        
                        # PRIORIDADE 2: Bluechip Status da Base Analítica (se não encontrou Última Ocorrência)
                        # FALLBACK: Usar id_isize (código externo) para buscar na Base Analítica
                        if not status_entrega and base_analitica_loader and hasattr(base_analitica_loader, 'is_loaded') and base_analitica_loader.is_loaded:
                            # Buscar por código externo (id_isize) primeiro
                            base_match = base_analitica_loader.find_by_codigo_externo(record.codigo_externo)
                            if base_match is None and record.cpf:
                                if hasattr(base_analitica_loader, 'find_by_cpf'):
                                    base_match = base_analitica_loader.find_by_cpf(record.cpf)
                            
                            if base_match is not None and isinstance(base_match, pd.Series):
                                # Buscar Bluechip Status (status principal)
                                bluechip_status = None
                                for col_name in ['Bluechip Status_Padronizado', 'Bluechip Status', 'Status Entrega', 'Status_Entrega']:
                                    if col_name in base_match.index:
                                        bluechip_status_val = base_match[col_name]
                                        if pd.notna(bluechip_status_val):
                                            bluechip_status_str = str(bluechip_status_val).lower()
                                            # Excluir entrega cancelada
                                            if 'entrega cancelada' not in bluechip_status_str and 'cancelada' not in bluechip_status_str:
                                                bluechip_status = safe_str(bluechip_status_val)
                                                break
                                
                                # Se encontrou Bluechip Status, montar status completo com detalhes adicionais
                                if bluechip_status:
                                    status_parts_ba = [bluechip_status]
                                    detalhes_ba = []
                                    
                                    # Buscar detalhes adicionais na Base Analítica
                                    # Endereço/Local (se disponível)
                                    for col_name in ['Endereco', 'Endereço', 'Logradouro', 'Rua', 'Local Entrega', 'Local_Entrega']:
                                        if col_name in base_match.index:
                                            local_val = base_match[col_name]
                                            if pd.notna(local_val):
                                                detalhes_ba.append(f"Local: {safe_str(local_val)}")
                                                break
                                    
                                    # Cidade e Estado
                                    cidade_ba = None
                                    estado_ba = None
                                    for col_name in ['Cidade', 'Municipio', 'Município']:
                                        if col_name in base_match.index:
                                            cidade_val = base_match[col_name]
                                            if pd.notna(cidade_val):
                                                cidade_ba = safe_str(cidade_val)
                                                break
                                    
                                    for col_name in ['UF', 'Estado']:
                                        if col_name in base_match.index:
                                            estado_val = base_match[col_name]
                                            if pd.notna(estado_val):
                                                estado_ba = safe_str(estado_val)
                                                break
                                    
                                    # Adicionar Cidade/Estado se disponível
                                    if cidade_ba:
                                        if estado_ba:
                                            detalhes_ba.append(f"{cidade_ba}/{estado_ba}")
                                        else:
                                            detalhes_ba.append(cidade_ba)
                                    
                                    # Montar status completo da Base Analítica
                                    status_entrega = status_parts_ba[0]
                                    if detalhes_ba:
                                        status_entrega += f" - {', '.join(detalhes_ba)}"
                        
                        # Garantir que ICCID seja tratado como texto no Excel
                        # Adicionar prefixo ' para forçar Excel a tratar como texto
                        iccid_formatado = f"'{iccid}" if iccid else ''
                        
                        row = [
                            safe_str(record.cpf),
                            safe_str(record.numero_acesso),
                            safe_str(record.numero_ordem),
                            safe_str(record.codigo_externo),
                            iccid_formatado,  # Coluna E - ICCID ou chip_id (forçado como texto com prefixo ')
                            '',  # ToutBox (não temos no modelo)
                            safe_str(record.numero_bilhete),
                            safe_enum(record.status_bilhete),
                            safe_str(record.operadora_doadora),
                            safe_date(record.data_portabilidade),
                            safe_str(record.motivo_recusa),
                            safe_str(record.motivo_cancelamento),
                            'Sim',  # Último bilhete de portabilidade? sempre Sim
                            safe_enum(record.status_ordem),
                            safe_str(record.preco_ordem),
                            safe_date(record.data_conclusao_ordem),
                            sintetizar_texto(safe_str(record.motivo_nao_consultado), max_caracteres=80),
                            sintetizar_texto(safe_str(record.motivo_nao_cancelado), max_caracteres=80),
                            sintetizar_texto(safe_str(record.motivo_nao_aberto), max_caracteres=80),
                            sintetizar_texto(safe_str(record.motivo_nao_reagendado), max_caracteres=80),
                            safe_str(record.novo_status_bilhete),
                            safe_date(record.nova_data_portabilidade),
                            safe_str(record.responsavel_processamento),
                            safe_date(record.data_inicial_processamento),
                            safe_date(record.data_final_processamento),
                            safe_bool(record.registro_valido),
                            safe_str(record.ajustes_registro),
                            safe_bool(record.numero_acesso_valido),
                            safe_str(record.ajustes_numero_acesso),
                            status_entrega,  # Status da entrega do Relatório de Objetos (Última Ocorrência)
                            data_entrega,     # Data da entrega do Relatório de Objetos
                            parametro_identificacao,  # Parâmetro de identificação
                            data_ultima_atualizacao,   # Data da última atualização da coleta
                            tipo_venda  # Tipo de Venda: Portabilidade ou Nova Linha
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
        output_path: Path,
        base_analitica_loader=None
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
            
            # Agrupar por CPF e capturar regra aplicada
            from collections import defaultdict
            grupos_cpf = defaultdict(list)
            regras_aplicadas = {}  # CPF -> regra aplicada
            
            for record in reabertura:
                grupos_cpf[record.cpf].append(record)
                
                # Capturar qual regra foi aplicada para este registro
                regra_aplicada = ''
                key = f"{record.cpf}_{record.numero_ordem}"
                results = results_map.get(key, [])
                
                # Verificar status cancelado
                if record.status_bilhete == PortabilidadeStatus.CANCELADA:
                    regra_aplicada = 'Status Cancelado'
                
                # Verificar motivos de cancelamento
                if not regra_aplicada and record.motivo_cancelamento:
                    if any(termo in str(record.motivo_cancelamento).lower() for termo in ['cancelamento', 'cancelado', 'pendente']):
                        regra_aplicada = 'Motivo Cancelamento'
                
                # Verificar resultados de decisão
                if not regra_aplicada:
                    for result in results:
                        # Decisões que indicam reabertura
                        if result.decision in ['CANCELAR', 'REABRIR', 'REAGENDAR']:
                            regra_aplicada = f'Decisão: {result.decision}'
                            break
                        
                        # Regras específicas de cancelamento
                        if 'rule_05_portabilidade_cancelada' in result.rule_name:
                            regra_aplicada = 'Regra 05: Portabilidade Cancelada'
                            break
                        
                        if 'rule_14_motivo_cancelamento' in result.rule_name:
                            regra_aplicada = 'Regra 14: Motivo Cancelamento'
                            break
                
                # Se não encontrou regra específica, usar a primeira regra encontrada
                if not regra_aplicada and results:
                    primeira_regra = results[0]
                    regra_aplicada = primeira_regra.rule_name if hasattr(primeira_regra, 'rule_name') else 'Regra não identificada'
                
                # Armazenar regra aplicada (usar a primeira encontrada para o CPF)
                if record.cpf not in regras_aplicadas:
                    regras_aplicadas[record.cpf] = regra_aplicada
            
            # Função para extrair valor final do plano/preço
            def extrair_valor_plano(texto_plano: str) -> str:
                """
                Extrai apenas o valor final do texto do plano/preço
                Exemplos:
                - "TIM CONTROLE A PLUS - 31,99" -> "31,99"
                - "SP 24,99" -> "24,99"
                - "R$ 29,99" -> "29,99"
                """
                if not texto_plano:
                    return ''
                
                texto = str(texto_plano).strip()
                
                # Procurar por padrão " - " seguido de número
                if ' - ' in texto:
                    partes = texto.split(' - ')
                    if len(partes) > 1:
                        valor = partes[-1].strip()
                        # Remover prefixos comuns (SP, R$, etc.)
                        valor = valor.replace('SP', '').replace('R$', '').replace('$', '').strip()
                        # Verificar se é um valor numérico (pode ter vírgula ou ponto)
                        if any(c.isdigit() for c in valor):
                            return valor
                
                # Se não encontrou padrão " - ", tentar remover prefixos diretamente
                # Remover prefixos comuns do início (com espaço ou sem)
                valor_limpo = texto
                prefixos = ['SP ', 'SP', 'R$ ', 'R$', '$ ', '$', 'RS ', 'RS']
                for prefixo in prefixos:
                    if valor_limpo.upper().startswith(prefixo.upper()):
                        valor_limpo = valor_limpo[len(prefixo):].strip()
                        break  # Remover apenas o primeiro prefixo encontrado
                
                # Verificar se restou um valor numérico
                if any(c.isdigit() for c in valor_limpo):
                    return valor_limpo
                
                # Se não encontrou padrão, retornar o texto original
                return texto
            
            # Gerar XLSX (Excel)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            import pandas as pd
            
            # Preparar dados
            dados = []
            
            # Funções auxiliares
            def safe_str(value, default=''):
                return str(value) if value is not None else default
            
            # Processar cada grupo de CPF
            for cpf, registros_cpf in grupos_cpf.items():
                # Limitar a 5 registros por CPF
                registros_cpf = registros_cpf[:5]
                
                # Preencher arrays (máximo 5) com lógica especial para Número de acesso 1 e 2
                numeros_acesso_1 = []
                numeros_acesso_2 = []
                numeros_acesso_3_5 = []
                
                for r in registros_cpf:
                    # Buscar dados da Base Analítica para este registro específico
                    telefone_portabilidade = ''
                    numero_linha = ''
                    
                    if base_analitica_loader and hasattr(base_analitica_loader, 'is_loaded') and base_analitica_loader.is_loaded:
                        # Tentar buscar por código externo primeiro
                        base_match = base_analitica_loader.find_by_codigo_externo(r.codigo_externo)
                        if base_match is None and cpf:
                            # Se não encontrou, tentar por CPF
                            if hasattr(base_analitica_loader, 'find_by_cpf'):
                                base_match_cpf = base_analitica_loader.find_by_cpf(cpf)
                                if isinstance(base_match_cpf, list) and len(base_match_cpf) > 0:
                                    base_match = base_match_cpf[0]
                                elif base_match_cpf is not None:
                                    base_match = base_match_cpf
                        
                        if base_match is not None:
                            # Buscar "Telefone Portabilidade" da Base Analítica
                            if isinstance(base_match, pd.Series):
                                telefone_port_val = base_match.get('Telefone Portabilidade', '')
                                if pd.notna(telefone_port_val) and str(telefone_port_val).strip() and str(telefone_port_val).strip() != '-':
                                    telefone_portabilidade = str(telefone_port_val).strip()
                                
                                # Buscar "Numero linha" (com variações do nome da coluna)
                                for col_name in ['Numero linha', 'numero linha', 'Numero Linha', 'Número Linha', 'Numero_linha', 'Número_linha']:
                                    if col_name in base_match.index:
                                        numero_linha_val = base_match[col_name]
                                        if pd.notna(numero_linha_val):
                                            numero_linha_str = str(numero_linha_val).strip()
                                            # Remover .0 se for float
                                            if numero_linha_str.endswith('.0'):
                                                numero_linha_str = numero_linha_str[:-2]
                                            if numero_linha_str:
                                                numero_linha = numero_linha_str
                                                break
                            elif isinstance(base_match, dict):
                                telefone_port_val = base_match.get('Telefone Portabilidade', '')
                                if telefone_port_val and str(telefone_port_val).strip() != '-':
                                    telefone_portabilidade = str(telefone_port_val).strip()
                                
                                # Buscar numero linha
                                for col_name in ['Numero linha', 'numero linha', 'Numero Linha', 'Número Linha', 'Numero_linha', 'Número_linha']:
                                    if col_name in base_match:
                                        numero_linha_val = base_match[col_name]
                                        if numero_linha_val:
                                            numero_linha_str = str(numero_linha_val).strip()
                                            if numero_linha_str.endswith('.0'):
                                                numero_linha_str = numero_linha_str[:-2]
                                            if numero_linha_str:
                                                numero_linha = numero_linha_str
                                                break
                    
                    # Verificar se é portabilidade
                    is_portabilidade = False
                    if r.operadora_doadora and str(r.operadora_doadora).strip():
                        is_portabilidade = True
                    elif r.data_portabilidade:
                        is_portabilidade = True
                    
                    # Obter valores - PRIORIDADE: Base Analítica > Record
                    # Número portado: usar "Telefone Portabilidade" da Base Analítica se disponível
                    numero_portado = telefone_portabilidade if telefone_portabilidade else safe_str(r.numero_acesso)
                    
                    # Número provisório: usar "Numero linha" da Base Analítica se disponível
                    numero_provisorio = numero_linha if numero_linha else (safe_str(r.numero_temporario) if r.numero_temporario else '')
                    
                    # Número de acesso 1: número portado (se portabilidade) ou número provisório (se não houver portado)
                    if is_portabilidade:
                        # Se é portabilidade, número portado vem da Base Analítica ("Telefone Portabilidade") ou record
                        # Se não houver número portado, usar número provisório
                        numero_acesso_1 = numero_portado if numero_portado else numero_provisorio
                    else:
                        # Se não é portabilidade, usar número provisório se existir, senão numero_acesso
                        numero_acesso_1 = numero_provisorio if numero_provisorio else safe_str(r.numero_acesso)
                    
                    # Número de acesso 2: se for portabilidade, inserir número provisório ("Numero linha")
                    # Se não tiver número provisório, estará idêntico nas 2 colunas
                    if is_portabilidade and numero_provisorio:
                        numero_acesso_2 = numero_provisorio
                    else:
                        # Se não for portabilidade ou não tiver provisório, usar o mesmo de acesso 1
                        numero_acesso_2 = numero_acesso_1
                    
                    numeros_acesso_1.append(numero_acesso_1)
                    numeros_acesso_2.append(numero_acesso_2)
                
                # Preencher até 5 registros (apenas para arrays, mas não usar acesso 3-5)
                while len(numeros_acesso_1) < 5:
                    numeros_acesso_1.append('')
                    numeros_acesso_2.append('')
                
                # Arrays finais - apenas acesso 1 e 2, 3-5 ficam vazios
                numeros_acesso = [
                    numeros_acesso_1[0],
                    numeros_acesso_2[0],
                    '',  # Número de acesso 3 - não preencher
                    '',  # Número de acesso 4 - não preencher
                    ''   # Número de acesso 5 - não preencher
                ]
                
                # Número da ordem: usar sempre o primeiro registro e repetir na ordem 2
                # Validar formato: deve ser "1-XXXXXXXXXXXXX" (não usar id_isize se não estiver nesse formato)
                primeiro = registros_cpf[0] if registros_cpf else None
                primeiro_numero_ordem_raw = safe_str(primeiro.numero_ordem) if primeiro else ''
                primeiro_codigo_externo = safe_str(primeiro.codigo_externo) if primeiro else ''
                
                # Validar se numero_ordem está no formato correto (começa com "1-")
                primeiro_numero_ordem = ''
                if primeiro_numero_ordem_raw:
                    # Verificar se está no formato "1-XXXXXXXXXXXXX"
                    if primeiro_numero_ordem_raw.startswith('1-') and len(primeiro_numero_ordem_raw) > 2:
                        primeiro_numero_ordem = primeiro_numero_ordem_raw
                    # Se não estiver no formato correto e for igual ao código externo (id_isize), não usar
                    elif primeiro_numero_ordem_raw == primeiro_codigo_externo:
                        # Não usar id_isize, deixar vazio (será usado fallback da Base Analítica)
                        primeiro_numero_ordem = ''
                    # Se não estiver no formato mas não for id_isize, usar apenas se começar com "1-"
                    elif primeiro_numero_ordem_raw.startswith('1-'):
                        primeiro_numero_ordem = primeiro_numero_ordem_raw
                
                # FALLBACK: Se não encontrou número da ordem válido, buscar "Numero OS" da Base Analítica
                if not primeiro_numero_ordem and base_analitica_loader and hasattr(base_analitica_loader, 'is_loaded') and base_analitica_loader.is_loaded:
                    # Tentar buscar por código externo primeiro
                    base_match = base_analitica_loader.find_by_codigo_externo(primeiro_codigo_externo)
                    if base_match is None and cpf:
                        # Se não encontrou, tentar por CPF
                        if hasattr(base_analitica_loader, 'find_by_cpf'):
                            base_match_cpf = base_analitica_loader.find_by_cpf(cpf)
                            if isinstance(base_match_cpf, list) and len(base_match_cpf) > 0:
                                base_match = base_match_cpf[0]
                            elif base_match_cpf is not None:
                                base_match = base_match_cpf
                    
                    if base_match is not None:
                        # Buscar "Numero OS" ou variações do nome da coluna
                        if isinstance(base_match, pd.Series):
                            for col_name in ['Numero OS', 'Numero_OS', 'Número OS', 'Número_OS', 'numero os', 'Numero Os']:
                                if col_name in base_match.index:
                                    numero_os_val = base_match[col_name]
                                    if pd.notna(numero_os_val):
                                        numero_os_str = str(numero_os_val).strip()
                                        # Não usar se for "0-00" ou vazio
                                        if numero_os_str and numero_os_str != '0-00' and numero_os_str.lower() != 'nan':
                                            primeiro_numero_ordem = numero_os_str
                                            break
                        elif isinstance(base_match, dict):
                            for col_name in ['Numero OS', 'Numero_OS', 'Número OS', 'Número_OS', 'numero os', 'Numero Os']:
                                if col_name in base_match:
                                    numero_os_val = base_match[col_name]
                                    if numero_os_val:
                                        numero_os_str = str(numero_os_val).strip()
                                        if numero_os_str and numero_os_str != '0-00':
                                            primeiro_numero_ordem = numero_os_str
                                            break
                
                numeros_ordem = [
                    primeiro_numero_ordem,  # Número da ordem 1 - sempre usar a existente (formato "1-XXXXXXXXXXXXX")
                    primeiro_numero_ordem,  # Número da ordem 2 - repetir ordem 1 (não usar id_isize)
                    '',  # Número da ordem 3 - não preencher
                    '',  # Número da ordem 4 - não preencher
                    ''   # Número da ordem 5 - não preencher
                ]
                
                # Código externo: usar sempre o primeiro registro e repetir no código 2
                # (já foi definido acima para validação do número da ordem)
                codigos_externo = [
                    primeiro_codigo_externo,  # Código externo 1
                    primeiro_codigo_externo,  # Código externo 2 - repetir código 1
                    '',  # Código externo 3 - não preencher
                    '',  # Código externo 4 - não preencher
                    ''   # Código externo 5 - não preencher
                ]
                
                # Pegar Plano e Preço da Base Analítica
                primeiro = registros_cpf[0]
                plano = ''  # Nome completo do plano (ex: "TIM CONTROLE A PLUS - 31,99")
                preco_raw = safe_str(primeiro.preco_ordem, '').replace('R$', '').replace(',', '.').strip()
                # Limpar preço removendo prefixos (SP, R$, etc.)
                preco = extrair_valor_plano(preco_raw) if preco_raw else ''
                
                # Buscar Plano na Base Analítica
                if base_analitica_loader and hasattr(base_analitica_loader, 'is_loaded') and base_analitica_loader.is_loaded:
                    # Tentar buscar por código externo primeiro
                    base_match = base_analitica_loader.find_by_codigo_externo(primeiro.codigo_externo)
                    if base_match is None and cpf:
                        # Se não encontrou, tentar por CPF
                        if hasattr(base_analitica_loader, 'find_by_cpf'):
                            base_match = base_analitica_loader.find_by_cpf(cpf)
                    
                    if base_match is not None and isinstance(base_match, pd.Series):
                        # Buscar coluna Plano (pode ter vários nomes)
                        for col_name in ['Plano', 'Plano_', 'Plano Cliente', 'Plano_Cliente', 'Nome do Plano']:
                            if col_name in base_match.index:
                                plano_valor = base_match[col_name]
                                if pd.notna(plano_valor):
                                    plano_texto = str(plano_valor)
                                    if plano_texto and plano_texto.lower() != 'nan':
                                        # Coluna Plano: manter o texto completo
                                        plano = plano_texto.strip()
                                        
                                        # Coluna Preço: extrair apenas o valor final
                                        preco_extraido = extrair_valor_plano(plano_texto)
                                        if preco_extraido:
                                            preco = preco_extraido
                                        break
                
                # Obter regra aplicada para este CPF
                regra_aplicada = regras_aplicadas.get(cpf, 'Regra não identificada')
                
                # Limpar preço removendo prefixos (SP, R$, etc.) - garantir apenas valor
                preco_limpo = preco
                if preco_limpo:
                    preco_limpo = str(preco_limpo).strip()
                    # Remover prefixos comuns que possam ter sobrado (com espaço ou sem)
                    prefixos = ['SP ', 'SP', 'R$ ', 'R$', '$ ', '$', 'RS ', 'RS']
                    for prefixo in prefixos:
                        if preco_limpo.upper().startswith(prefixo.upper()):
                            preco_limpo = preco_limpo[len(prefixo):].strip()
                            break
                    # Remover espaços extras
                    preco_limpo = preco_limpo.strip()
                
                # Montar linha
                row = {
                    'Cpf': safe_str(cpf),
                    'Plano': plano,
                    'Preço': preco_limpo,
                    'Número de acesso 1': numeros_acesso[0],
                    'Número de acesso 2': numeros_acesso[1],
                    'Número de acesso 3': numeros_acesso[2],
                    'Número de acesso 4': numeros_acesso[3],
                    'Número de acesso 5': numeros_acesso[4],
                    'Número da ordem 1': numeros_ordem[0],
                    'Número da ordem 2': numeros_ordem[1],
                    'Número da ordem 3': numeros_ordem[2],
                    'Número da ordem 4': numeros_ordem[3],
                    'Número da ordem 5': numeros_ordem[4],
                    'Código externo 1': codigos_externo[0],
                    'Código externo 2': codigos_externo[1],
                    'Código externo 3': codigos_externo[2],
                    'Código externo 4': codigos_externo[3],
                    'Código externo 5': codigos_externo[4],
                    'Regra Aplicada': regra_aplicada  # Coluna no final com a regra aplicada
                }
                dados.append(row)
            
            # Criar DataFrame e salvar como XLSX
            df = pd.DataFrame(dados)
            
            # Ordenar colunas conforme modelo
            colunas_ordem = [
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
                'Código externo 5',
                'Regra Aplicada'  # Coluna no final com a regra aplicada
            ]
            
            df = df[colunas_ordem]
            
            # Salvar como XLSX
            df.to_excel(output_path, index=False, engine='openpyxl')
            
            logger.info(f"Planilha Reabertura gerada: {output_path} ({len(grupos_cpf)} CPFs, {len(reabertura)} registros)")
            return True
            
        except Exception as e:
            logger.error(f"Erro ao gerar planilha Reabertura: {e}")
            return False

