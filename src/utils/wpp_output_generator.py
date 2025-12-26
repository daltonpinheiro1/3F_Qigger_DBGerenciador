"""
Gerador de saída para Régua de Comunicação WhatsApp
Versão 2.0 - Com mapeamento de templates e variáveis

Templates Disponíveis:
1 - confirma_portabilidade_v1 (Confirmação de portabilidade)
2 - pendencia_sms_portabilidade (Pendência de validação SMS)
3 - aviso_retirada_correios_v1 (Aguardando retirada nos Correios)
4 - confirmacao_endereco_v1 (Confirmação de endereço)
"""
import csv
import logging
from pathlib import Path
from typing import List, Optional, Dict, Any
from datetime import datetime

from src.models.portabilidade import PortabilidadeRecord
from src.utils.templates_wpp import TemplateMapper, TEMPLATES

logger = logging.getLogger(__name__)


class WPPOutputGenerator:
    """
    Gera arquivos de saída compatíveis com a Régua de Comunicação WhatsApp
    
    A saída é um CSV/Excel com os seguintes campos:
    - Proposta_iSize
    - Cpf
    - NomeCliente
    - Telefone_Contato
    - Endereco, Numero, Complemento, Bairro
    - Cidade, UF, Cep
    - Ponto_Referencia
    - Cod_Rastreio (Link https://tim.trakin.co/o/{numero_pedido})
    - Data_Venda
    - Tipo_Comunicacao (Template do triggers.xlsx)
    - Template_ID (ID do template WPP)
    - Template_Nome (Nome do modelo do template)
    - Template_Variaveis (Variáveis formatadas para o template)
    - Status_Disparo (sempre FALSE)
    - DataHora_Disparo (vazio)
    """
    
    # Headers da planilha WPP (atualizado com campos de template)
    WPP_HEADERS = [
        'Proposta_iSize',
        'Cpf',
        'NomeCliente',
        'Telefone_Contato',
        'Endereco',
        'Numero',
        'Complemento',
        'Bairro',
        'Cidade',
        'UF',
        'Cep',
        'Ponto_Referencia',
        'Cod_Rastreio',
        'Data_Venda',
        'Tipo_Comunicacao',
        'Template_ID',
        'Template_Nome',
        'Template_Variaveis',
        'Status_Disparo',
        'DataHora_Disparo',
    ]
    
    def __init__(self, output_path: Optional[str] = None):
        """
        Inicializa o gerador
        
        Args:
            output_path: Caminho padrão para saída dos arquivos
        """
        self.output_path = output_path
        self._records_to_export: List[PortabilidadeRecord] = []
    
    def add_record(self, record: PortabilidadeRecord) -> bool:
        """
        Adiciona um registro para exportação
        Apenas registros com Template definido são adicionados
        
        Args:
            record: Registro de portabilidade
            
        Returns:
            True se registro foi adicionado
        """
        # Só adicionar se tiver Template definido
        if not record.template:
            return False
        
        self._records_to_export.append(record)
        return True
    
    def add_records(self, records: List[PortabilidadeRecord]) -> int:
        """
        Adiciona múltiplos registros para exportação
        
        Args:
            records: Lista de registros
            
        Returns:
            Número de registros adicionados
        """
        count = 0
        for record in records:
            if self.add_record(record):
                count += 1
        return count
    
    def clear(self):
        """Limpa a lista de registros para exportação"""
        self._records_to_export = []
    
    def generate_csv(self, output_path: Optional[str] = None, append: bool = False) -> Optional[str]:
        """
        Gera arquivo CSV com os registros
        
        Args:
            output_path: Caminho para o arquivo de saída
            append: Se True, adiciona ao arquivo existente
            
        Returns:
            Caminho do arquivo gerado ou None se erro
        """
        path = output_path or self.output_path
        
        if not path:
            logger.error("Caminho de saída não especificado")
            return None
        
        if not self._records_to_export:
            logger.warning("Nenhum registro para exportar (sem Template definido)")
            return None
        
        try:
            path_obj = Path(path)
            path_obj.parent.mkdir(parents=True, exist_ok=True)
            
            # Verificar se arquivo existe e se deve adicionar header
            file_exists = path_obj.exists() and path_obj.stat().st_size > 0
            write_header = not append or not file_exists
            
            mode = 'a' if append and file_exists else 'w'
            
            with open(path, mode, newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=self.WPP_HEADERS)
                
                if write_header:
                    writer.writeheader()
                
                for record in self._records_to_export:
                    # Obter dados base do registro
                    row = record.to_wpp_dict()
                    
                    # Enriquecer com informações do template
                    row = self._enrich_with_template_info(row, record)
                    
                    writer.writerow(row)
            
            logger.info(f"Gerado arquivo WPP: {path} ({len(self._records_to_export)} registros)")
            return str(path)
            
        except Exception as e:
            logger.error(f"Erro ao gerar CSV WPP: {e}")
            return None
    
    def _enrich_with_template_info(self, row: Dict[str, Any], record: PortabilidadeRecord) -> Dict[str, Any]:
        """
        Enriquece a linha com informações do template WPP
        
        Args:
            row: Dicionário com dados da linha
            record: Registro de portabilidade
            
        Returns:
            Dicionário enriquecido
        """
        try:
            # Obter informações do template
            template_info = TemplateMapper.get_template_for_record(record)
            
            # Adicionar campos do template
            row['Template_ID'] = template_info.get('template_id') or ''
            row['Template_Nome'] = template_info.get('nome_modelo') or ''
            row['Template_Variaveis'] = TemplateMapper.format_variables_string(
                template_info.get('variaveis', {})
            )
            
        except Exception as e:
            logger.warning(f"Erro ao enriquecer com template: {e}")
            row['Template_ID'] = ''
            row['Template_Nome'] = ''
            row['Template_Variaveis'] = ''
        
        return row
    
    def generate_for_batch(
        self, 
        records: List[PortabilidadeRecord], 
        output_path: Optional[str] = None,
        append: bool = True
    ) -> Optional[str]:
        """
        Gera arquivo CSV para um lote de registros
        
        Args:
            records: Lista de registros processados
            output_path: Caminho para o arquivo de saída
            append: Se True, adiciona ao arquivo existente
            
        Returns:
            Caminho do arquivo gerado ou None
        """
        self.clear()
        added = self.add_records(records)
        
        if added == 0:
            logger.info("Nenhum registro com Template para exportar ao WPP")
            return None
        
        logger.info(f"Exportando {added} registros para Régua de Comunicação WPP")
        return self.generate_csv(output_path, append=append)
    
    def generate_timestamped(
        self, 
        records: List[PortabilidadeRecord],
        output_dir: Optional[str] = None,
        prefix: str = "WPP_Regua"
    ) -> Optional[str]:
        """
        Gera arquivo CSV com timestamp no nome
        
        Args:
            records: Lista de registros processados
            output_dir: Diretório de saída
            prefix: Prefixo do nome do arquivo
            
        Returns:
            Caminho do arquivo gerado ou None
        """
        self.clear()
        added = self.add_records(records)
        
        if added == 0:
            logger.info("Nenhum registro com Template para exportar ao WPP")
            return None
        
        output_dir = output_dir or (Path(self.output_path).parent if self.output_path else ".")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{prefix}_{timestamp}.csv"
        output_path = Path(output_dir) / filename
        
        return self.generate_csv(str(output_path), append=False)
    
    @property
    def pending_count(self) -> int:
        """Retorna número de registros pendentes para exportação"""
        return len(self._records_to_export)
    
    def get_stats(self) -> dict:
        """Retorna estatísticas do gerador"""
        templates = {}
        for record in self._records_to_export:
            template = record.template or 'Sem Template'
            templates[template] = templates.get(template, 0) + 1
        
        return {
            'total_pending': len(self._records_to_export),
            'by_template': templates,
            'output_path': self.output_path,
        }
    
    @staticmethod
    def filter_records_with_template(records: List[PortabilidadeRecord]) -> List[PortabilidadeRecord]:
        """
        Filtra registros que têm Template definido
        
        Args:
            records: Lista de registros
            
        Returns:
            Lista filtrada
        """
        return [r for r in records if r.template]
