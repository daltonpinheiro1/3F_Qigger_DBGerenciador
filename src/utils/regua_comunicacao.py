"""
Régua de Comunicação WhatsApp - Algoritmo de Disparo
Baseado na base_analitica_final.csv
"""
import logging
import re
from pathlib import Path
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum

import pandas as pd

from src.models.portabilidade import PortabilidadeRecord

logger = logging.getLogger(__name__)


class TipoComunicacao(Enum):
    """Tipos de comunicação da régua"""
    # Portabilidade
    PORTABILIDADE_AGENDADA = "1"           # Portabilidade agendada com sucesso
    PORTABILIDADE_ANTECIPADA = "2"         # Portabilidade antecipada disponível
    PORTABILIDADE_CONCLUIDA = "3"          # Portabilidade concluída
    PORTABILIDADE_CANCELADA = "4"          # Portabilidade cancelada
    PORTABILIDADE_REAGENDAR = "5"          # Necessário reagendar portabilidade
    
    # Entrega
    CHIP_DESPACHADO = "10"                 # Chip despachado para entrega
    CHIP_EM_ROTA = "11"                    # Chip em rota de entrega
    CHIP_ENTREGUE = "12"                   # Chip entregue com sucesso
    CHIP_ENTREGA_FALHOU = "13"             # Falha na entrega do chip
    CHIP_AGUARDANDO_RETIRADA = "14"        # Aguardando retirada nos Correios
    CHIP_DEVOLVIDO = "15"                  # Chip devolvido ao remetente
    
    # Ativação
    ATIVACAO_PENDENTE = "20"               # Chip entregue, aguardando ativação
    ATIVACAO_CONCLUIDA = "21"              # Linha ativada com sucesso
    
    # Boas vindas / Follow-up
    BOAS_VINDAS = "30"                     # Mensagem de boas-vindas
    FOLLOW_UP_7_DIAS = "31"                # Follow-up após 7 dias
    FOLLOW_UP_30_DIAS = "32"               # Follow-up após 30 dias
    
    # Problemas
    PROBLEMA_ENTREGA = "40"                # Problema com entrega
    PROBLEMA_ATIVACAO = "41"               # Problema com ativação
    AREA_RISCO = "42"                      # Entrega em área de risco
    ENDERECO_INCORRETO = "43"              # Endereço incorreto
    
    # Não mapeado
    NAO_MAPEADO = "99"


@dataclass
class DisparoComunicacao:
    """Registro para disparo de comunicação"""
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
    
    # Campos adicionais para análise
    status_funil: Optional[str] = None
    status_entrega: Optional[str] = None
    status_portabilidade: Optional[str] = None
    email: Optional[str] = None
    
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


class ReguaComunicacao:
    """
    Motor da Régua de Comunicação
    
    Analisa a base analítica e determina quais comunicações devem ser disparadas
    baseado no status do funil, entrega e portabilidade.
    """
    
    def __init__(self, base_analitica_path: Optional[str] = None):
        """
        Inicializa a régua de comunicação
        
        Args:
            base_analitica_path: Caminho para base_analitica_final.csv
        """
        self.base_path = base_analitica_path
        self.df: Optional[pd.DataFrame] = None
        self._disparos: List[DisparoComunicacao] = []
        
        if base_analitica_path:
            self.load_base(base_analitica_path)
    
    def load_base(self, file_path: str) -> int:
        """
        Carrega a base analítica
        
        Args:
            file_path: Caminho para o arquivo CSV
            
        Returns:
            Número de registros carregados
        """
        self.base_path = file_path
        
        if not Path(file_path).exists():
            logger.error(f"Arquivo não encontrado: {file_path}")
            return 0
        
        try:
            # Carregar com separador correto (;)
            self.df = pd.read_csv(file_path, sep=';', encoding='utf-8', low_memory=False)
            logger.info(f"Base analítica carregada: {len(self.df)} registros")
            return len(self.df)
        except Exception as e:
            logger.error(f"Erro ao carregar base analítica: {e}")
            return 0
    
    def analisar_registro(self, row: pd.Series) -> Optional[DisparoComunicacao]:
        """
        Analisa um registro e determina a comunicação apropriada
        
        Args:
            row: Linha do DataFrame
            
        Returns:
            DisparoComunicacao ou None se não houver comunicação a fazer
        """
        try:
            # Extrair dados básicos
            proposta = self._clean_value(row.get('Proposta iSize'))
            cpf = self._clean_cpf(row.get('CPF'))
            nome = self._clean_value(row.get('Cliente'))
            
            if not proposta or not cpf:
                return None
            
            # Extrair telefone (prioridade: Telefone Portabilidade > Telefone principal)
            telefone = self._clean_phone(row.get('Telefone Portabilidade'))
            if not telefone:
                ddd = self._clean_value(row.get('DDD'))
                tel = self._clean_value(row.get('Telefone'))
                if ddd and tel:
                    telefone = f"{ddd}{tel}".replace('-', '')
            
            if not telefone:
                return None
            
            # Status importantes
            status_funil = self._clean_value(row.get('Status_Funil'))
            status_entrega = self._clean_value(row.get('Bluechip Status_Padronizado'))
            status_venda = self._clean_value(row.get('Status venda'))
            conectada = self._clean_value(row.get('Conectada'))
            portabilidade = self._clean_value(row.get('Portabilidade'))
            
            # Determinar tipo de comunicação
            tipo_comunicacao = self._determinar_tipo_comunicacao(
                status_funil=status_funil,
                status_entrega=status_entrega,
                status_venda=status_venda,
                conectada=conectada,
                portabilidade=portabilidade,
                row=row
            )
            
            if not tipo_comunicacao:
                return None
            
            # Gerar link de rastreio
            # Prioridade: usar proposta iSize como identificador do pedido
            cod_rastreio_original = self._clean_value(row.get('Rastreio Correios')) or self._clean_value(row.get('Rastreio Loggi')) or ''
            cod_rastreio = PortabilidadeRecord.gerar_link_rastreio(str(proposta)) or cod_rastreio_original
            
            # Criar registro de disparo
            return DisparoComunicacao(
                proposta_isize=str(proposta),
                cpf=cpf,
                nome_cliente=nome or '',
                telefone_contato=telefone,
                endereco=self._clean_value(row.get('Endereco')) or '',
                numero=self._clean_value(row.get('Numero')) or '',
                complemento=self._clean_value(row.get('Complemento')) or '',
                bairro=self._clean_value(row.get('Bairro')) or '',
                cidade=self._clean_value(row.get('Cidade')) or '',
                uf=self._clean_value(row.get('UF')) or '',
                cep=self._clean_value(row.get('Cep')) or '',
                ponto_referencia=self._clean_value(row.get('Ponto Referencia')) or '',
                cod_rastreio=cod_rastreio,
                data_venda=self._format_date(row.get('Data venda')),
                tipo_comunicacao=tipo_comunicacao,
                status_funil=status_funil,
                status_entrega=status_entrega,
                status_portabilidade=portabilidade,
                email=self._clean_value(row.get('Email')),
            )
            
        except Exception as e:
            logger.debug(f"Erro ao analisar registro: {e}")
            return None
    
    def _determinar_tipo_comunicacao(
        self,
        status_funil: Optional[str],
        status_entrega: Optional[str],
        status_venda: Optional[str],
        conectada: Optional[str],
        portabilidade: Optional[str],
        row: pd.Series
    ) -> Optional[str]:
        """
        Determina o tipo de comunicação baseado nos status
        
        Régua de prioridade:
        1. Problemas de entrega (cancelada, área de risco, etc.)
        2. Status de portabilidade
        3. Status de entrega
        4. Status de ativação
        5. Follow-up
        """
        
        # === PROBLEMAS DE ENTREGA (PRIORIDADE ALTA) ===
        if status_entrega:
            status_lower = status_entrega.lower()
            
            # Entrega cancelada
            if 'cancelada' in status_lower or 'cancelado' in status_lower:
                if 'área de risco' in status_lower or 'area de risco' in status_lower:
                    return TipoComunicacao.AREA_RISCO.value
                elif 'não retirada' in status_lower or 'nao retirada' in status_lower:
                    return TipoComunicacao.CHIP_AGUARDANDO_RETIRADA.value
                elif 'desconhecido' in status_lower:
                    return TipoComunicacao.ENDERECO_INCORRETO.value
                else:
                    return TipoComunicacao.CHIP_ENTREGA_FALHOU.value
            
            # Devolvido
            if 'devolvido' in status_lower or 'devolução' in status_lower:
                return TipoComunicacao.CHIP_DEVOLVIDO.value
        
        # === STATUS DE ENTREGA NORMAL ===
        if status_entrega:
            status_lower = status_entrega.lower()
            
            # Entregue
            if 'entregue' in status_lower or 'finalizada' in status_lower:
                # Se entregue mas não conectada, lembrar de ativar
                if conectada and conectada.upper() != 'CONECTADA':
                    return TipoComunicacao.ATIVACAO_PENDENTE.value
                elif conectada and conectada.upper() == 'CONECTADA':
                    return TipoComunicacao.ATIVACAO_CONCLUIDA.value
                else:
                    return TipoComunicacao.CHIP_ENTREGUE.value
            
            # Em rota / trânsito
            if 'em rota' in status_lower or 'trânsito' in status_lower or 'transito' in status_lower:
                return TipoComunicacao.CHIP_EM_ROTA.value
            
            # Integrado (despachado)
            if 'integrado' in status_lower:
                return TipoComunicacao.CHIP_DESPACHADO.value
        
        # === PORTABILIDADE ===
        if portabilidade:
            port_lower = portabilidade.lower()
            
            if 'sim' in port_lower or port_lower == 'portabilidade':
                # Verificar se portabilidade antecipada está marcada
                port_antecipada = self._clean_value(row.get('Portabilidade Antecipada'))
                if port_antecipada and port_antecipada.lower() == 'sim':
                    return TipoComunicacao.PORTABILIDADE_ANTECIPADA.value
                
                # Verificar status de conexão
                if conectada and conectada.upper() == 'CONECTADA':
                    return TipoComunicacao.PORTABILIDADE_CONCLUIDA.value
                else:
                    return TipoComunicacao.PORTABILIDADE_AGENDADA.value
        
        # === STATUS DO FUNIL ===
        if status_funil:
            funil_lower = status_funil.lower()
            
            if 'faturado' in funil_lower or 'gross' in funil_lower:
                if conectada and conectada.upper() == 'CONECTADA':
                    return TipoComunicacao.BOAS_VINDAS.value
            
            if 'despachado' in funil_lower:
                return TipoComunicacao.CHIP_DESPACHADO.value
            
            if 'entregue' in funil_lower:
                return TipoComunicacao.CHIP_ENTREGUE.value
        
        # === FOLLOW-UP (verificar data) ===
        data_conectada = self._parse_date(row.get('Data Conectada'))
        if data_conectada:
            dias_desde_conexao = (datetime.now() - data_conectada).days
            
            if dias_desde_conexao >= 30:
                return TipoComunicacao.FOLLOW_UP_30_DIAS.value
            elif dias_desde_conexao >= 7:
                return TipoComunicacao.FOLLOW_UP_7_DIAS.value
        
        return None
    
    def processar_base(self, filtros: Optional[Dict[str, Any]] = None) -> List[DisparoComunicacao]:
        """
        Processa toda a base analítica e gera lista de disparos
        
        Args:
            filtros: Filtros opcionais (ex: {'Status venda': 'APROVADA'})
            
        Returns:
            Lista de disparos a serem feitos
        """
        if self.df is None:
            logger.error("Base analítica não carregada")
            return []
        
        self._disparos = []
        df_filtrado = self.df.copy()
        
        # Aplicar filtros
        if filtros:
            for coluna, valor in filtros.items():
                if coluna in df_filtrado.columns:
                    df_filtrado = df_filtrado[df_filtrado[coluna] == valor]
        
        # Processar cada registro
        total = len(df_filtrado)
        processados = 0
        
        for idx, row in df_filtrado.iterrows():
            disparo = self.analisar_registro(row)
            if disparo:
                self._disparos.append(disparo)
                processados += 1
            
            if (idx + 1) % 1000 == 0:
                logger.info(f"Processados {idx + 1}/{total} registros...")
        
        logger.info(f"Processamento concluído: {processados} disparos identificados de {total} registros")
        return self._disparos
    
    def gerar_csv_disparos(self, output_path: str, append: bool = False) -> Optional[str]:
        """
        Gera CSV com os disparos para a régua de comunicação
        
        Args:
            output_path: Caminho para arquivo de saída
            append: Se True, adiciona ao arquivo existente
            
        Returns:
            Caminho do arquivo gerado
        """
        if not self._disparos:
            logger.warning("Nenhum disparo para exportar")
            return None
        
        try:
            # Converter para DataFrame
            data = [d.to_dict() for d in self._disparos]
            df_output = pd.DataFrame(data)
            
            # Verificar se deve adicionar a arquivo existente
            path_obj = Path(output_path)
            path_obj.parent.mkdir(parents=True, exist_ok=True)
            
            if append and path_obj.exists():
                df_existing = pd.read_csv(output_path, encoding='utf-8-sig')
                df_output = pd.concat([df_existing, df_output], ignore_index=True)
                # Remover duplicatas por Proposta_iSize + Tipo_Comunicacao
                df_output = df_output.drop_duplicates(
                    subset=['Proposta_iSize', 'Tipo_Comunicacao'], 
                    keep='last'
                )
            
            df_output.to_csv(output_path, index=False, encoding='utf-8-sig')
            logger.info(f"Arquivo de disparos gerado: {output_path} ({len(df_output)} registros)")
            return output_path
            
        except Exception as e:
            logger.error(f"Erro ao gerar CSV de disparos: {e}")
            return None
    
    def get_estatisticas(self) -> Dict[str, Any]:
        """Retorna estatísticas dos disparos"""
        if not self._disparos:
            return {'total': 0, 'por_tipo': {}}
        
        por_tipo = {}
        for d in self._disparos:
            tipo = d.tipo_comunicacao
            por_tipo[tipo] = por_tipo.get(tipo, 0) + 1
        
        # Mapear códigos para nomes
        tipo_nomes = {
            '1': 'Portabilidade Agendada',
            '2': 'Portabilidade Antecipada',
            '3': 'Portabilidade Concluída',
            '4': 'Portabilidade Cancelada',
            '5': 'Reagendar Portabilidade',
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
            '99': 'Não Mapeado',
        }
        
        por_tipo_nomeado = {
            tipo_nomes.get(k, k): v for k, v in sorted(por_tipo.items())
        }
        
        return {
            'total': len(self._disparos),
            'por_tipo': por_tipo_nomeado,
            'base_registros': len(self.df) if self.df is not None else 0,
        }
    
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
        """Limpa CPF mantendo apenas dígitos"""
        cleaned = ReguaComunicacao._clean_value(value)
        if not cleaned:
            return None
        # Remover pontos e traços, manter apenas números
        cpf = re.sub(r'[^0-9]', '', cleaned.split('.')[0])
        if len(cpf) >= 11:
            return cpf[:11]
        return cpf.zfill(11) if cpf else None
    
    @staticmethod
    def _clean_phone(value) -> Optional[str]:
        """Limpa telefone mantendo apenas dígitos"""
        cleaned = ReguaComunicacao._clean_value(value)
        if not cleaned:
            return None
        phone = re.sub(r'[^0-9]', '', cleaned.split('.')[0])
        if len(phone) >= 10:
            return phone
        return None
    
    @staticmethod
    def _format_date(value) -> str:
        """Formata data para string"""
        if value is None:
            return ''
        
        if isinstance(value, datetime):
            return value.strftime('%Y-%m-%d %H:%M:%S')
        
        cleaned = ReguaComunicacao._clean_value(value)
        if not cleaned:
            return ''
        
        return cleaned
    
    @staticmethod
    def _parse_date(value) -> Optional[datetime]:
        """Parse de data"""
        cleaned = ReguaComunicacao._clean_value(value)
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
            except ValueError:
                continue
        
        return None
