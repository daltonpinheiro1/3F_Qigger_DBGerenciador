"""
Unificador de Dados - Integra múltiplas fontes no banco unificado
Versão 1.0

Este módulo integra dados de:
- Base Analítica Final (CSV)
- Relatório de Objetos (XLSX)
- Gerenciador/Siebel (CSV de portabilidade)
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
from pathlib import Path

import pandas as pd

from src.database.unified_db import UnifiedDatabaseManager
from src.utils.objects_loader import ObjectsLoader
from src.models.portabilidade import PortabilidadeRecord

logger = logging.getLogger(__name__)


class DataUnifier:
    """
    Unifica dados de múltiplas fontes no banco de dados unificado
    """
    
    def __init__(self, db_manager: UnifiedDatabaseManager):
        """
        Inicializa o unificador
        
        Args:
            db_manager: Gerenciador do banco unificado
        """
        self.db_manager = db_manager
    
    def unify_from_base_analitica(
        self,
        file_path: str,
        batch_size: int = 1000
    ) -> Dict[str, int]:
        """
        Unifica dados da Base Analítica Final
        
        Args:
            file_path: Caminho para o arquivo base_analitica_final.csv
            batch_size: Tamanho do lote para processamento
            
        Returns:
            Estatísticas do processamento
        """
        if not Path(file_path).exists():
            logger.warning(f"Arquivo base analítica não encontrado: {file_path}")
            return {'processados': 0, 'novos': 0, 'atualizados': 0, 'erros': 0}
        
        stats = {'processados': 0, 'novos': 0, 'atualizados': 0, 'erros': 0}
        
        try:
            # Tentar diferentes encodings
            encodings = ['utf-8-sig', 'utf-8', 'latin-1', 'cp1252']
            df = None
            
            for encoding in encodings:
                try:
                    df = pd.read_csv(file_path, encoding=encoding, delimiter=';', low_memory=False)
                    logger.info(f"Base analítica carregada com encoding {encoding}: {len(df)} registros")
                    break
                except (UnicodeDecodeError, Exception) as e:
                    continue
            
            if df is None:
                logger.error("Não foi possível ler a base analítica com nenhum encoding")
                return stats
            
            # Processar em lotes
            for i in range(0, len(df), batch_size):
                batch = df.iloc[i:i+batch_size]
                
                for _, row in batch.iterrows():
                    try:
                        dados = self._extract_base_analitica_data(row)
                        
                        if not dados.get('proposta_isize') and not dados.get('numero_ordem'):
                            continue
                        
                        id_isize = dados.get('proposta_isize') or dados.get('numero_ordem') or str(row.get('Login Externo', ''))
                        numero_ordem = dados.get('numero_ordem') or id_isize
                        
                        versao, is_nova = self.db_manager.insert_or_update_record(
                            id_isize=str(id_isize),
                            numero_ordem=str(numero_ordem),
                            dados=dados,
                            origem_dados='base_analitica'
                        )
                        
                        stats['processados'] += 1
                        if is_nova:
                            stats['novos'] += 1
                        else:
                            stats['atualizados'] += 1
                    
                    except Exception as e:
                        logger.error(f"Erro ao processar linha da base analítica: {e}")
                        stats['erros'] += 1
            
            logger.info(f"Base analítica processada: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"Erro ao processar base analítica: {e}")
            stats['erros'] = 1
            return stats
    
    def _extract_base_analitica_data(self, row: pd.Series) -> Dict[str, Any]:
        """
        Extrai dados de uma linha da base analítica
        """
        def safe_get(key, default=None):
            value = row.get(key)
            if pd.isna(value):
                return default
            return str(value).strip() if value is not None else default
        
        # Mapeamento de colunas da base analítica
        dados = {
            # Identificadores
            'proposta_isize': safe_get('Proposta iSize'),
            'codigo_externo': safe_get('Login Externo'),
            'cpf': safe_get('CPF'),
            
            # Dados do cliente
            'cliente_nome': safe_get('Cliente'),
            'cliente_telefone': safe_get('Telefone'),
            'telefone_portado': safe_get('Telefone Portabilidade'),
            
            # Endereço
            'endereco': safe_get('Endereco'),
            'numero': safe_get('Numero'),
            'complemento': safe_get('Complemento'),
            'bairro': safe_get('Bairro'),
            'cidade': safe_get('Cidade'),
            'uf': safe_get('UF'),
            'cep': safe_get('Cep'),
            'ponto_referencia': safe_get('Ponto Referencia'),
            
            # Datas
            'data_venda': safe_get('Data venda'),
            'data_conectada': safe_get('Data Conectada'),
            
            # Produto
            'produto_vendido': safe_get('Produto'),
            'plano': safe_get('Plano'),
            
            # Status e logística
            'status_venda': safe_get('Status venda'),
            'rastreio_correios': safe_get('Rastreio Correios'),
            'rastreio_loggi': safe_get('Rastreio Loggi'),
            
            # Portabilidade
            'portabilidade': safe_get('Portabilidade'),
            'complemento_portabilidade': safe_get('Complemento Portabilidade'),
            'portabilidade_antecipada': safe_get('Portabilidade Antecipada'),
            
            # Outros
            'numero_os': safe_get('Numero OS'),
            'pedido_bluechip': safe_get('Pedido Bluechip'),
        }
        
        # Limpar valores vazios
        return {k: v for k, v in dados.items() if v and v not in ['', 'nan', 'None']}
    
    def unify_from_relatorio_objetos(
        self,
        file_path: str
    ) -> Dict[str, int]:
        """
        Unifica dados do Relatório de Objetos
        
        Args:
            file_path: Caminho para o arquivo XLSX do Relatório de Objetos
            
        Returns:
            Estatísticas do processamento
        """
        if not Path(file_path).exists():
            logger.warning(f"Arquivo relatório de objetos não encontrado: {file_path}")
            return {'processados': 0, 'novos': 0, 'atualizados': 0, 'erros': 0}
        
        stats = {'processados': 0, 'novos': 0, 'atualizados': 0, 'erros': 0}
        
        try:
            objects_loader = ObjectsLoader(file_path)
            
            for obj_record in objects_loader._records:
                try:
                    dados = self._extract_objects_data(obj_record)
                    
                    if not dados.get('codigo_externo'):
                        continue
                    
                    id_isize = dados['codigo_externo']
                    numero_ordem = dados.get('numero_ordem') or id_isize
                    
                    versao, is_nova = self.db_manager.insert_or_update_record(
                        id_isize=str(id_isize),
                        numero_ordem=str(numero_ordem),
                        dados=dados,
                        origem_dados='relatorio_objetos'
                    )
                    
                    stats['processados'] += 1
                    if is_nova:
                        stats['novos'] += 1
                    else:
                        stats['atualizados'] += 1
                
                except Exception as e:
                    logger.error(f"Erro ao processar registro do relatório de objetos: {e}")
                    stats['erros'] += 1
            
            logger.info(f"Relatório de objetos processado: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"Erro ao processar relatório de objetos: {e}")
            stats['erros'] = 1
            return stats
    
    def _extract_objects_data(self, obj_record) -> Dict[str, Any]:
        """
        Extrai dados de um ObjectRecord
        """
        dados = {
            # Identificadores
            'codigo_externo': obj_record.codigo_externo,
            'numero_ordem': obj_record.codigo_externo,  # Usar código externo como número da ordem
            'nu_pedido': obj_record.nu_pedido,
            'id_erp': obj_record.id_erp,
            
            # Dados do cliente
            'cliente_nome': obj_record.destinatario,
            'telefone_contato': obj_record.telefone,
            'documento': obj_record.documento,
            
            # Endereço
            'cidade': obj_record.cidade,
            'uf': obj_record.uf,
            'cep': obj_record.cep,
            
            # Logística
            'status_logistica': obj_record.status,
            'rastreio': obj_record.rastreio,
            'transportadora': obj_record.transportadora,
            'data_criacao_pedido': obj_record.data_criacao_pedido.isoformat() if obj_record.data_criacao_pedido else None,
            'previsao_entrega': obj_record.previsao_entrega.isoformat() if obj_record.previsao_entrega else None,
            'data_entrega': obj_record.data_entrega.isoformat() if obj_record.data_entrega else None,
            
            # Formatar link de rastreio
            'cod_rastreio': f"https://tim.trakin.co/o/{obj_record.nu_pedido}" if obj_record.nu_pedido else None,
        }
        
        # Limpar valores None
        return {k: v for k, v in dados.items() if v is not None}
    
    def unify_from_portabilidade_records(
        self,
        records: List[PortabilidadeRecord]
    ) -> Dict[str, int]:
        """
        Unifica dados de registros de portabilidade (Siebel/Gerenciador)
        
        Args:
            records: Lista de PortabilidadeRecord
            
        Returns:
            Estatísticas do processamento
        """
        stats = {'processados': 0, 'novos': 0, 'atualizados': 0, 'erros': 0}
        
        for record in records:
            try:
                dados = self._extract_portabilidade_data(record)
                
                if not record.codigo_externo:
                    continue
                
                id_isize = record.codigo_externo
                numero_ordem = record.numero_ordem or id_isize
                
                versao, is_nova = self.db_manager.insert_or_update_record(
                    id_isize=str(id_isize),
                    numero_ordem=str(numero_ordem),
                    dados=dados,
                    origem_dados='gerenciador'
                )
                
                stats['processados'] += 1
                if is_nova:
                    stats['novos'] += 1
                else:
                    stats['atualizados'] += 1
            
            except Exception as e:
                logger.error(f"Erro ao processar registro de portabilidade: {e}")
                stats['erros'] += 1
        
        logger.info(f"Registros de portabilidade processados: {stats}")
        return stats
    
    def _extract_portabilidade_data(self, record: PortabilidadeRecord) -> Dict[str, Any]:
        """
        Extrai dados de um PortabilidadeRecord
        """
        dados = {
            # Identificadores
            'codigo_externo': record.codigo_externo,
            'numero_ordem': record.numero_ordem,
            'cpf': record.cpf,
            'numero_acesso': record.numero_acesso,
            
            # Bilhetes e status
            'status_bilhete': record.status_bilhete.value if record.status_bilhete else None,
            'numero_bilhete': record.numero_bilhete,
            'operadora_doadora': record.operadora_doadora,
            'data_portabilidade': record.data_portabilidade.isoformat() if record.data_portabilidade else None,
            
            # Motivos (FOCO PRINCIPAL)
            'motivo_recusa': record.motivo_recusa,
            'motivo_cancelamento': record.motivo_cancelamento,
            'motivo_nao_consultado': record.motivo_nao_consultado,
            'motivo_nao_cancelado': record.motivo_nao_cancelado,
            'motivo_nao_aberto': record.motivo_nao_aberto,
            'motivo_nao_reagendado': record.motivo_nao_reagendado,
            
            # Status da ordem (FOCO PRINCIPAL)
            'status_ordem': record.status_ordem.value if record.status_ordem else None,
            'data_conclusao_ordem': record.data_conclusao_ordem.isoformat() if record.data_conclusao_ordem else None,
            
            # Triggers e regras
            'regra_id': record.regra_id,
            'o_que_aconteceu': record.o_que_aconteceu,
            'acao_a_realizar': record.acao_a_realizar,
            'tipo_mensagem': record.tipo_mensagem,
            'template': record.template,
            'mapeado': 1 if record.mapeado else 0,
            
            # Logística (se já foi enriquecido)
            'cliente_nome': record.nome_cliente,
            'telefone_contato': record.telefone_contato,
            'cidade': record.cidade,
            'uf': record.uf,
            'cep': record.cep,
            'status_logistica': record.status_logistica,
            'cod_rastreio': record.cod_rastreio,
            'data_venda': record.data_venda.isoformat() if record.data_venda else None,
            
            # Processamento
            'data_inicial_processamento': record.data_inicial_processamento.isoformat() if record.data_inicial_processamento else None,
            'data_final_processamento': record.data_final_processamento.isoformat() if record.data_final_processamento else None,
        }
        
        # Limpar valores None
        return {k: v for k, v in dados.items() if v is not None}
    
    def synchronize_all_sources(
        self,
        base_analitica_path: Optional[str] = None,
        relatorio_objetos_path: Optional[str] = None,
        portabilidade_records: Optional[List[PortabilidadeRecord]] = None
    ) -> Dict[str, Any]:
        """
        Sincroniza todas as fontes de dados disponíveis
        
        Args:
            base_analitica_path: Caminho para base analítica (opcional)
            relatorio_objetos_path: Caminho para relatório de objetos (opcional)
            portabilidade_records: Lista de registros de portabilidade (opcional)
            
        Returns:
            Estatísticas completas da sincronização
        """
        stats_total = {
            'base_analitica': {},
            'relatorio_objetos': {},
            'portabilidade': {},
            'total_processados': 0,
            'total_novos': 0,
            'total_atualizados': 0,
            'total_erros': 0
        }
        
        # Sincronizar base analítica
        if base_analitica_path:
            logger.info("Sincronizando Base Analítica...")
            stats = self.unify_from_base_analitica(base_analitica_path)
            stats_total['base_analitica'] = stats
            stats_total['total_processados'] += stats['processados']
            stats_total['total_novos'] += stats['novos']
            stats_total['total_atualizados'] += stats['atualizados']
            stats_total['total_erros'] += stats['erros']
        
        # Sincronizar relatório de objetos
        if relatorio_objetos_path:
            logger.info("Sincronizando Relatório de Objetos...")
            stats = self.unify_from_relatorio_objetos(relatorio_objetos_path)
            stats_total['relatorio_objetos'] = stats
            stats_total['total_processados'] += stats['processados']
            stats_total['total_novos'] += stats['novos']
            stats_total['total_atualizados'] += stats['atualizados']
            stats_total['total_erros'] += stats['erros']
        
        # Sincronizar registros de portabilidade
        if portabilidade_records:
            logger.info("Sincronizando Registros de Portabilidade...")
            stats = self.unify_from_portabilidade_records(portabilidade_records)
            stats_total['portabilidade'] = stats
            stats_total['total_processados'] += stats['processados']
            stats_total['total_novos'] += stats['novos']
            stats_total['total_atualizados'] += stats['atualizados']
            stats_total['total_erros'] += stats['erros']
        
        logger.info(f"Sincronização completa: {stats_total}")
        return stats_total

