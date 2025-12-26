"""
Régua de Comunicação WhatsApp - Versão DINÂMICA
Cruza múltiplas fontes de dados para determinar status atual:
- Base Analítica (dados de venda/cliente)
- Relatório de Objetos (status de logística em tempo real)
- CSV Siebel (status de portabilidade em tempo real)
"""
import logging
import re
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum

import pandas as pd

from src.utils.objects_loader import ObjectsLoader, ObjectRecord
from src.models.portabilidade import PortabilidadeRecord

logger = logging.getLogger(__name__)


class TipoComunicacao(Enum):
    """Tipos de comunicação da régua"""
    # Portabilidade
    PORTABILIDADE_AGENDADA = "1"
    PORTABILIDADE_ANTECIPADA = "2"
    PORTABILIDADE_CONCLUIDA = "3"
    PORTABILIDADE_CANCELADA = "4"
    PORTABILIDADE_REAGENDAR = "5"
    PORTABILIDADE_PENDENTE = "6"
    
    # Entrega
    CHIP_DESPACHADO = "10"
    CHIP_EM_ROTA = "11"
    CHIP_ENTREGUE = "12"
    CHIP_ENTREGA_FALHOU = "13"
    CHIP_AGUARDANDO_RETIRADA = "14"
    CHIP_DEVOLVIDO = "15"
    
    # Ativação
    ATIVACAO_PENDENTE = "20"
    ATIVACAO_CONCLUIDA = "21"
    
    # Boas vindas / Follow-up
    BOAS_VINDAS = "30"
    FOLLOW_UP_7_DIAS = "31"
    FOLLOW_UP_30_DIAS = "32"
    
    # Problemas
    PROBLEMA_ENTREGA = "40"
    PROBLEMA_ATIVACAO = "41"
    AREA_RISCO = "42"
    ENDERECO_INCORRETO = "43"
    CLIENTE_DESCONHECE = "44"
    
    # Não mapeado
    NAO_MAPEADO = "99"


@dataclass
class StatusConsolidado:
    """Status consolidado de múltiplas fontes"""
    proposta_isize: str
    
    # Dados do cliente (base analítica)
    cpf: Optional[str] = None
    nome_cliente: Optional[str] = None
    telefone: Optional[str] = None
    email: Optional[str] = None
    endereco: Optional[str] = None
    numero: Optional[str] = None
    complemento: Optional[str] = None
    bairro: Optional[str] = None
    cidade: Optional[str] = None
    uf: Optional[str] = None
    cep: Optional[str] = None
    ponto_referencia: Optional[str] = None
    data_venda: Optional[datetime] = None
    
    # Status de logística (Relatório de Objetos - mais recente)
    status_logistica: Optional[str] = None
    data_status_logistica: Optional[datetime] = None
    cod_rastreio: Optional[str] = None
    previsao_entrega: Optional[datetime] = None
    data_entrega: Optional[datetime] = None
    
    # Status de portabilidade (CSV Siebel - mais recente)
    status_bilhete: Optional[str] = None
    status_ordem: Optional[str] = None
    data_portabilidade: Optional[datetime] = None
    motivo_cancelamento: Optional[str] = None
    motivo_recusa: Optional[str] = None
    motivo_nao_consultado: Optional[str] = None
    conectada: bool = False
    
    # Metadados
    fonte_logistica: bool = False
    fonte_portabilidade: bool = False
    fonte_analitica: bool = False
    data_ultima_atualizacao: Optional[datetime] = None


@dataclass
class DisparoDinamico:
    """Registro para disparo de comunicação (versão dinâmica)"""
    proposta_isize: str
    cpf: str
    nome_cliente: str
    telefone_contato: str
    endereco: str
    numero: str
    complemento: str
    bairro: str
    cidade: str
    uf: str
    cep: str
    ponto_referencia: str
    cod_rastreio: str
    data_venda: str
    tipo_comunicacao: str
    status_disparo: str = "FALSE"
    datahora_disparo: str = ""
    
    # Campos de rastreabilidade
    status_logistica: Optional[str] = None
    status_portabilidade: Optional[str] = None
    data_status: Optional[str] = None
    fontes_utilizadas: str = ""
    
    def to_dict(self) -> dict:
        """Converte para dicionário no formato da régua WPP"""
        return {
            'Proposta_iSize': self.proposta_isize,
            'Cpf': self.cpf,
            'NomeCliente': self.nome_cliente,
            'Telefone_Contato': self.telefone_contato,
            'Endereco': self.endereco,
            'Numero': self.numero,
            'Complemento': self.complemento,
            'Bairro': self.bairro,
            'Cidade': self.cidade,
            'UF': self.uf,
            'Cep': self.cep,
            'Ponto_Referencia': self.ponto_referencia,
            'Cod_Rastreio': self.cod_rastreio,
            'Data_Venda': self.data_venda,
            'Tipo_Comunicacao': self.tipo_comunicacao,
            'Status_Disparo': self.status_disparo,
            'DataHora_Disparo': self.datahora_disparo,
        }


class ReguaComunicacaoDinamica:
    """
    Motor da Régua de Comunicação - Versão DINÂMICA
    
    Cruza múltiplas fontes de dados para determinar o status atual de cada proposta:
    1. Base Analítica (dados de venda/cliente)
    2. Relatório de Objetos (status de logística em tempo real)
    3. CSV Siebel (status de portabilidade em tempo real)
    
    Usa o ID iSize (Proposta iSize / Código Externo) como chave de cruzamento.
    """
    
    def __init__(self):
        """Inicializa a régua de comunicação dinâmica"""
        self.df_analitica: Optional[pd.DataFrame] = None
        self.df_logistica: Optional[pd.DataFrame] = None
        self.df_portabilidade: Optional[pd.DataFrame] = None
        
        # Índices para busca rápida
        self._idx_analitica: Dict[str, pd.Series] = {}
        self._idx_logistica: Dict[str, pd.Series] = {}
        self._idx_portabilidade: Dict[str, pd.Series] = {}
        
        self._disparos: List[DisparoDinamico] = []
        self._status_consolidados: Dict[str, StatusConsolidado] = {}
    
    def carregar_base_analitica(self, file_path: str) -> int:
        """Carrega a base analítica (dados de venda/cliente)"""
        if not Path(file_path).exists():
            logger.warning(f"Base analítica não encontrada: {file_path}")
            return 0
        
        try:
            self.df_analitica = pd.read_csv(file_path, sep=';', encoding='utf-8', low_memory=False)
            
            # Criar índice por Proposta iSize
            for idx, row in self.df_analitica.iterrows():
                proposta = self._clean_value(row.get('Proposta iSize'))
                if proposta:
                    self._idx_analitica[str(proposta)] = row
            
            logger.info(f"Base analítica carregada: {len(self._idx_analitica)} propostas")
            return len(self._idx_analitica)
        except Exception as e:
            logger.error(f"Erro ao carregar base analítica: {e}")
            return 0
    
    def carregar_relatorio_objetos(self, file_path: str) -> int:
        """Carrega o Relatório de Objetos (logística)"""
        if not Path(file_path).exists():
            logger.warning(f"Relatório de Objetos não encontrado: {file_path}")
            return 0
        
        try:
            self.df_logistica = pd.read_excel(file_path)
            
            # Criar índice por código externo (extraído do Nu Pedido)
            for idx, row in self.df_logistica.iterrows():
                nu_pedido = self._clean_value(row.get('Nu Pedido'))
                if nu_pedido:
                    codigo = self._extrair_codigo_externo(nu_pedido)
                    if codigo:
                        # Manter o mais recente por Data Inserção
                        existing = self._idx_logistica.get(codigo)
                        if not existing is None:
                            data_existing = self._parse_date(existing.get('Data Inserção'))
                            data_new = self._parse_date(row.get('Data Inserção'))
                            if data_new and data_existing and data_new <= data_existing:
                                continue
                        self._idx_logistica[codigo] = row
            
            logger.info(f"Relatório de Objetos carregado: {len(self._idx_logistica)} propostas")
            return len(self._idx_logistica)
        except Exception as e:
            logger.error(f"Erro ao carregar Relatório de Objetos: {e}")
            return 0
    
    def carregar_csv_portabilidade(self, file_path: str) -> int:
        """Carrega o CSV de gerenciamento de portabilidade (Siebel)"""
        if not Path(file_path).exists():
            logger.warning(f"CSV Portabilidade não encontrado: {file_path}")
            return 0
        
        try:
            # Tentar diferentes encodings
            for encoding in ['utf-8-sig', 'utf-8', 'latin-1', 'cp1252']:
                try:
                    self.df_portabilidade = pd.read_csv(file_path, encoding=encoding)
                    break
                except:
                    continue
            
            if self.df_portabilidade is None:
                logger.error("Não foi possível ler o CSV de portabilidade")
                return 0
            
            # Criar índice por Código externo
            for idx, row in self.df_portabilidade.iterrows():
                codigo = self._clean_value(row.get('Código externo'))
                if codigo:
                    # Manter o mais recente por Data final do processamento
                    existing = self._idx_portabilidade.get(str(codigo))
                    if not existing is None:
                        data_existing = self._parse_date(existing.get('Data final do processamento'))
                        data_new = self._parse_date(row.get('Data final do processamento'))
                        if data_new and data_existing and data_new <= data_existing:
                            continue
                    self._idx_portabilidade[str(codigo)] = row
            
            logger.info(f"CSV Portabilidade carregado: {len(self._idx_portabilidade)} propostas")
            return len(self._idx_portabilidade)
        except Exception as e:
            logger.error(f"Erro ao carregar CSV Portabilidade: {e}")
            return 0
    
    def consolidar_status(self, proposta_isize: str) -> Optional[StatusConsolidado]:
        """
        Consolida o status de uma proposta de todas as fontes
        Prioriza dados mais recentes e do Relatório de Objetos para envio
        
        Args:
            proposta_isize: ID da proposta
            
        Returns:
            StatusConsolidado com dados de todas as fontes
        """
        status = StatusConsolidado(proposta_isize=proposta_isize)
        
        # === DADOS DA BASE ANALÍTICA (dados base do cliente) ===
        row_analitica = self._idx_analitica.get(proposta_isize)
        if row_analitica is not None:
            status.fonte_analitica = True
            status.cpf = self._clean_cpf(row_analitica.get('CPF'))
            status.nome_cliente = self._clean_value(row_analitica.get('Cliente'))
            status.email = self._clean_value(row_analitica.get('Email'))
            status.endereco = self._clean_value(row_analitica.get('Endereco'))
            status.numero = self._clean_value(row_analitica.get('Numero'))
            status.complemento = self._clean_value(row_analitica.get('Complemento'))
            status.bairro = self._clean_value(row_analitica.get('Bairro'))
            status.cidade = self._clean_value(row_analitica.get('Cidade'))
            status.uf = self._clean_value(row_analitica.get('UF'))
            status.cep = self._clean_value(row_analitica.get('Cep'))
            status.ponto_referencia = self._clean_value(row_analitica.get('Ponto Referencia'))
            status.data_venda = self._parse_date(row_analitica.get('Data venda'))
            
            # Telefone (prioridade: Portabilidade > Principal)
            tel_port = self._clean_phone(row_analitica.get('Telefone Portabilidade'))
            if tel_port:
                status.telefone = tel_port
            else:
                ddd = self._clean_value(row_analitica.get('DDD'))
                tel = self._clean_value(row_analitica.get('Telefone'))
                if ddd and tel:
                    status.telefone = f"{ddd}{tel}".replace('-', '')
            
            # Status de conexão da base analítica
            conectada = self._clean_value(row_analitica.get('Conectada'))
            if conectada and conectada.upper() == 'CONECTADA':
                status.conectada = True
        
        # === DADOS DO RELATÓRIO DE OBJETOS (LOGÍSTICA - PRIORIDADE PARA ENVIO) ===
        # Este é o mais recente e atualizado para dados de envio/rastreio
        row_logistica = self._idx_logistica.get(proposta_isize)
        if row_logistica is not None:
            status.fonte_logistica = True
            status.status_logistica = self._clean_value(row_logistica.get('Status'))
            status.data_status_logistica = self._parse_date(row_logistica.get('Data Inserção'))
            status.previsao_entrega = self._parse_date(row_logistica.get('Previsão Entrega'))
            status.data_entrega = self._parse_date(row_logistica.get('Data Entrega'))
            
            # Atualizar dados de contato do relatório de objetos (mais atualizados para envio)
            nome_logistica = self._clean_value(row_logistica.get('Destinatário'))
            if nome_logistica:
                status.nome_cliente = nome_logistica  # Prioriza nome do relatório de objetos
            
            tel_logistica = self._clean_phone(row_logistica.get('Telefone'))
            if tel_logistica:
                status.telefone = tel_logistica  # Prioriza telefone do relatório de objetos
            
            cidade_logistica = self._clean_value(row_logistica.get('Cidade'))
            if cidade_logistica:
                status.cidade = cidade_logistica
            
            uf_logistica = self._clean_value(row_logistica.get('UF'))
            if uf_logistica:
                status.uf = uf_logistica
                
            cep_logistica = self._clean_value(row_logistica.get('CEP'))
            if cep_logistica:
                status.cep = cep_logistica
        
        # === DADOS DO CSV PORTABILIDADE (SIEBEL - STATUS DE PORTABILIDADE) ===
        row_port = self._idx_portabilidade.get(proposta_isize)
        if row_port is not None:
            status.fonte_portabilidade = True
            status.status_bilhete = self._clean_value(row_port.get('Status do bilhete'))
            status.status_ordem = self._clean_value(row_port.get('Status da ordem'))
            status.data_portabilidade = self._parse_date(row_port.get('Data da portabilidade'))
            status.motivo_cancelamento = self._clean_value(row_port.get('Motivo do cancelamento'))
            status.motivo_recusa = self._clean_value(row_port.get('Motivo da recusa'))
            status.motivo_nao_consultado = self._clean_value(row_port.get('Motivo de não ter sido consultado'))
            
            # Atualizar telefone e CPF se não tiver de outras fontes
            if not status.cpf:
                status.cpf = self._clean_cpf(row_port.get('Cpf'))
            if not status.telefone:
                status.telefone = self._clean_phone(row_port.get('Número de acesso'))
        
        # Determinar data de última atualização (mais recente de todas as fontes)
        datas = [d for d in [status.data_status_logistica, status.data_portabilidade, status.data_venda] if d]
        if datas:
            status.data_ultima_atualizacao = max(datas)
        
        return status
    
    def determinar_tipo_comunicacao(self, status: StatusConsolidado) -> Optional[str]:
        """
        Determina o tipo de comunicação baseado no status consolidado
        
        Prioridade:
        1. Problemas críticos (entrega cancelada, área de risco)
        2. Status de portabilidade atual
        3. Status de logística atual
        4. Status de ativação
        5. Follow-up
        """
        
        # === 1. PROBLEMAS DE LOGÍSTICA (PRIORIDADE MÁXIMA) ===
        if status.status_logistica:
            sl = status.status_logistica.lower()
            
            # Entrega cancelada / com problema
            if 'cancelada' in sl or 'cancelado' in sl:
                if 'área de risco' in sl or 'area de risco' in sl:
                    return TipoComunicacao.AREA_RISCO.value
                elif 'não retirada' in sl or 'nao retirada' in sl:
                    return TipoComunicacao.CHIP_AGUARDANDO_RETIRADA.value
                elif 'desconhece' in sl or 'desconhecido' in sl:
                    return TipoComunicacao.CLIENTE_DESCONHECE.value
                elif 'endereço' in sl or 'endereco' in sl:
                    return TipoComunicacao.ENDERECO_INCORRETO.value
                else:
                    return TipoComunicacao.CHIP_ENTREGA_FALHOU.value
            
            # Devolvido
            if 'devolvido' in sl or 'devolução' in sl or 'devolvida' in sl:
                return TipoComunicacao.CHIP_DEVOLVIDO.value
        
        # === 2. STATUS DE PORTABILIDADE (PRIORIDADE ALTA) ===
        if status.status_bilhete:
            sb = status.status_bilhete.lower()
            
            # Portabilidade cancelada
            if 'cancelad' in sb:
                return TipoComunicacao.PORTABILIDADE_CANCELADA.value
            
            # Portabilidade pendente
            if 'pendente' in sb:
                # Verificar motivo
                if status.motivo_nao_consultado:
                    return TipoComunicacao.PORTABILIDADE_REAGENDAR.value
                return TipoComunicacao.PORTABILIDADE_PENDENTE.value
            
            # Portabilidade concluída
            if 'portado' in sb or 'concluíd' in sb or 'concluido' in sb:
                if status.conectada:
                    return TipoComunicacao.ATIVACAO_CONCLUIDA.value
                return TipoComunicacao.PORTABILIDADE_CONCLUIDA.value
            
            # Conflito ou erro
            if 'conflito' in sb or 'erro' in sb or 'falha' in sb:
                return TipoComunicacao.PORTABILIDADE_REAGENDAR.value
        
        # === 3. STATUS DE LOGÍSTICA NORMAL ===
        if status.status_logistica:
            sl = status.status_logistica.lower()
            
            # Entregue
            if 'entregue' in sl or 'finalizada' in sl:
                if status.conectada:
                    return TipoComunicacao.ATIVACAO_CONCLUIDA.value
                else:
                    return TipoComunicacao.ATIVACAO_PENDENTE.value
            
            # Em rota / trânsito
            if 'em rota' in sl or 'trânsito' in sl or 'transito' in sl:
                return TipoComunicacao.CHIP_EM_ROTA.value
            
            # Integrado (despachado)
            if 'integrado' in sl or 'despachado' in sl:
                return TipoComunicacao.CHIP_DESPACHADO.value
        
        # === 4. CLIENTE JÁ CONECTADO - BOAS VINDAS / FOLLOW-UP ===
        if status.conectada:
            return TipoComunicacao.BOAS_VINDAS.value
        
        return None
    
    def processar_todas_propostas(self) -> List[DisparoDinamico]:
        """
        Processa todas as propostas de todas as fontes
        
        Returns:
            Lista de disparos
        """
        self._disparos = []
        self._status_consolidados = {}
        
        # Coletar todos os IDs de proposta de todas as fontes
        todos_ids = set()
        todos_ids.update(self._idx_analitica.keys())
        todos_ids.update(self._idx_logistica.keys())
        todos_ids.update(self._idx_portabilidade.keys())
        
        logger.info(f"Total de propostas únicas: {len(todos_ids)}")
        
        processados = 0
        for proposta_id in todos_ids:
            # Consolidar status
            status = self.consolidar_status(proposta_id)
            if not status:
                continue
            
            self._status_consolidados[proposta_id] = status
            
            # Determinar tipo de comunicação
            tipo = self.determinar_tipo_comunicacao(status)
            if not tipo:
                continue
            
            # Verificar se tem telefone
            if not status.telefone:
                continue
            
            # Criar disparo
            fontes = []
            if status.fonte_analitica:
                fontes.append("Analítica")
            if status.fonte_logistica:
                fontes.append("Logística")
            if status.fonte_portabilidade:
                fontes.append("Portabilidade")
            
            # Gerar link de rastreio usando a proposta como identificador
            # Prioridade: link existente > gerar novo link a partir da proposta
            cod_rastreio = status.cod_rastreio
            if not cod_rastreio or not str(cod_rastreio).startswith('http'):
                cod_rastreio = PortabilidadeRecord.gerar_link_rastreio(proposta_id) or ''
            
            disparo = DisparoDinamico(
                proposta_isize=proposta_id,
                cpf=status.cpf or '',
                nome_cliente=status.nome_cliente or '',
                telefone_contato=status.telefone,
                endereco=status.endereco or '',
                numero=status.numero or '',
                complemento=status.complemento or '',
                bairro=status.bairro or '',
                cidade=status.cidade or '',
                uf=status.uf or '',
                cep=status.cep or '',
                ponto_referencia=status.ponto_referencia or '',
                cod_rastreio=cod_rastreio,
                data_venda=status.data_venda.strftime('%Y-%m-%d %H:%M:%S') if status.data_venda else '',
                tipo_comunicacao=tipo,
                status_logistica=status.status_logistica,
                status_portabilidade=status.status_bilhete,
                data_status=status.data_ultima_atualizacao.strftime('%Y-%m-%d %H:%M:%S') if status.data_ultima_atualizacao else '',
                fontes_utilizadas=', '.join(fontes),
            )
            
            self._disparos.append(disparo)
            processados += 1
            
            if processados % 1000 == 0:
                logger.info(f"Processadas {processados} propostas...")
        
        logger.info(f"Processamento concluído: {len(self._disparos)} disparos de {len(todos_ids)} propostas")
        return self._disparos
    
    def gerar_csv_disparos(self, output_path: str) -> Optional[str]:
        """Gera CSV com os disparos"""
        if not self._disparos:
            logger.warning("Nenhum disparo para exportar")
            return None
        
        try:
            data = [d.to_dict() for d in self._disparos]
            df = pd.DataFrame(data)
            
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(output_path, index=False, encoding='utf-8-sig')
            
            logger.info(f"Arquivo gerado: {output_path} ({len(df)} registros)")
            return output_path
        except Exception as e:
            logger.error(f"Erro ao gerar CSV: {e}")
            return None
    
    def get_estatisticas(self) -> Dict[str, Any]:
        """Retorna estatísticas detalhadas"""
        tipo_nomes = {
            '1': 'Portabilidade Agendada',
            '2': 'Portabilidade Antecipada',
            '3': 'Portabilidade Concluída',
            '4': 'Portabilidade Cancelada',
            '5': 'Reagendar Portabilidade',
            '6': 'Portabilidade Pendente',
            '10': 'Chip Despachado',
            '11': 'Chip em Rota',
            '12': 'Chip Entregue',
            '13': 'Falha na Entrega',
            '14': 'Aguardando Retirada',
            '15': 'Chip Devolvido',
            '20': 'Ativação Pendente',
            '21': 'Ativação Concluída',
            '30': 'Boas Vindas',
            '31': 'Follow-up 7 dias',
            '32': 'Follow-up 30 dias',
            '40': 'Problema Entrega',
            '41': 'Problema Ativação',
            '42': 'Área de Risco',
            '43': 'Endereço Incorreto',
            '44': 'Cliente Desconhece',
            '99': 'Não Mapeado',
        }
        
        por_tipo = {}
        for d in self._disparos:
            tipo = d.tipo_comunicacao
            nome = tipo_nomes.get(tipo, tipo)
            por_tipo[nome] = por_tipo.get(nome, 0) + 1
        
        # Contagem por fonte
        por_fonte = {'Apenas Analítica': 0, 'Apenas Logística': 0, 'Apenas Portabilidade': 0, 'Múltiplas': 0}
        for status in self._status_consolidados.values():
            fontes = sum([status.fonte_analitica, status.fonte_logistica, status.fonte_portabilidade])
            if fontes > 1:
                por_fonte['Múltiplas'] += 1
            elif status.fonte_analitica:
                por_fonte['Apenas Analítica'] += 1
            elif status.fonte_logistica:
                por_fonte['Apenas Logística'] += 1
            elif status.fonte_portabilidade:
                por_fonte['Apenas Portabilidade'] += 1
        
        return {
            'total_disparos': len(self._disparos),
            'total_propostas': len(self._status_consolidados),
            'propostas_analitica': len(self._idx_analitica),
            'propostas_logistica': len(self._idx_logistica),
            'propostas_portabilidade': len(self._idx_portabilidade),
            'por_tipo': dict(sorted(por_tipo.items(), key=lambda x: -x[1])),
            'por_fonte': por_fonte,
        }
    
    # === MÉTODOS AUXILIARES ===
    
    def _extrair_codigo_externo(self, nu_pedido: str) -> Optional[str]:
        """Extrai código externo do Nu Pedido (ex: 26-0250015976 -> 250015976)"""
        if not nu_pedido:
            return None
        
        # Remover sufixos como "-01", "-02"
        parts = str(nu_pedido).split('-')
        if len(parts) >= 2:
            codigo = parts[1]
            if codigo.startswith('0'):
                codigo = codigo[1:]
            return re.sub(r'[^0-9]', '', codigo)
        
        return re.sub(r'[^0-9]', '', str(nu_pedido))
    
    @staticmethod
    def _clean_value(value) -> Optional[str]:
        """Limpa valor removendo NaN e espaços"""
        if value is None:
            return None
        if isinstance(value, float):
            import math
            if math.isnan(value):
                return None
        value_str = str(value).strip()
        if value_str.lower() in ['nan', 'none', '', '-']:
            return None
        return value_str
    
    @staticmethod
    def _clean_cpf(value) -> Optional[str]:
        """Limpa CPF"""
        cleaned = ReguaComunicacaoDinamica._clean_value(value)
        if not cleaned:
            return None
        cpf = re.sub(r'[^0-9]', '', cleaned.split('.')[0])
        if len(cpf) >= 11:
            return cpf[:11]
        return cpf.zfill(11) if cpf else None
    
    @staticmethod
    def _clean_phone(value) -> Optional[str]:
        """Limpa telefone"""
        cleaned = ReguaComunicacaoDinamica._clean_value(value)
        if not cleaned:
            return None
        phone = re.sub(r'[^0-9]', '', cleaned.split('.')[0])
        if len(phone) >= 10:
            return phone
        return None
    
    @staticmethod
    def _parse_date(value) -> Optional[datetime]:
        """Parse de data"""
        if value is None:
            return None
        
        if isinstance(value, datetime):
            return value
        
        if isinstance(value, pd.Timestamp):
            return value.to_pydatetime()
        
        cleaned = ReguaComunicacaoDinamica._clean_value(value)
        if not cleaned:
            return None
        
        formats = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d",
            "%d/%m/%Y %H:%M:%S",
            "%d/%m/%Y",
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(cleaned, fmt)
            except:
                continue
        
        return None
