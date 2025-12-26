"""
Gerenciador de saída de arquivos processados
Copia arquivos para Google Drive e Backoffice após processamento
"""
import logging
import shutil
from pathlib import Path
from typing import Optional, List, Dict
from datetime import datetime

from src.models.portabilidade import PortabilidadeRecord
from src.utils.csv_generator import CSVGenerator

logger = logging.getLogger(__name__)


class FileOutputManager:
    """Gerenciador de saída de arquivos processados"""
    
    def __init__(
        self,
        google_drive_path: Optional[str] = None,
        backoffice_path: Optional[str] = None
    ):
        """
        Inicializa o gerenciador de saída
        
        Args:
            google_drive_path: Caminho para Google Drive (ex: G:\\Meu Drive\\Retornos_Qigger)
            backoffice_path: Caminho para Backoffice (ex: \\\\files\\07 Backoffice\\RETORNOS RPA - QIGGER\\GERENCIAMENTO)
        """
        self.google_drive_path = Path(google_drive_path) if google_drive_path else None
        self.backoffice_path = Path(backoffice_path) if backoffice_path else None
        
        # Verificar e criar pastas se necessário
        self._ensure_paths_exist()
    
    def _ensure_paths_exist(self):
        """Verifica e cria pastas de destino se necessário"""
        if self.google_drive_path:
            try:
                if not self.google_drive_path.exists():
                    logger.warning(f"Pasta Google Drive não existe: {self.google_drive_path}")
                    logger.info(f"Tentando criar pasta: {self.google_drive_path}")
                    self.google_drive_path.mkdir(parents=True, exist_ok=True)
                    logger.info(f"Pasta Google Drive criada: {self.google_drive_path}")
                else:
                    logger.debug(f"Pasta Google Drive existe: {self.google_drive_path}")
            except Exception as e:
                logger.error(f"Erro ao verificar/criar pasta Google Drive: {e}")
        
        if self.backoffice_path:
            try:
                if not self.backoffice_path.exists():
                    logger.warning(f"Pasta Backoffice não existe: {self.backoffice_path}")
                    logger.info(f"Tentando criar pasta: {self.backoffice_path}")
                    self.backoffice_path.mkdir(parents=True, exist_ok=True)
                    logger.info(f"Pasta Backoffice criada: {self.backoffice_path}")
                else:
                    logger.debug(f"Pasta Backoffice existe: {self.backoffice_path}")
            except Exception as e:
                logger.error(f"Erro ao verificar/criar pasta Backoffice: {e}")
    
    def copy_to_outputs(
        self,
        source_file: Path,
        success: bool = True,
        records: Optional[List[PortabilidadeRecord]] = None,
        results_map: Optional[Dict[str, List['DecisionResult']]] = None,
        objects_loader=None
    ) -> List[str]:
        """
        Copia arquivo processado para os destinos configurados
        Gera planilhas específicas para Google Drive e Backoffice
        
        Args:
            source_file: Arquivo fonte a ser copiado
            success: Se True, arquivo foi processado com sucesso; se False, houve erro
            records: Lista de registros processados (opcional, para gerar planilhas específicas)
            results_map: Dicionário mapeando CPF+Ordem para resultados (opcional)
            objects_loader: Loader de objetos para verificar status de entrega (opcional)
            
        Returns:
            Lista de caminhos onde o arquivo foi copiado/gerado com sucesso
        """
        copied_paths = []
        
        if not source_file.exists():
            logger.error(f"Arquivo fonte não existe: {source_file}")
            return copied_paths
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        source_name = source_file.stem
        
        # Google Drive: Gerar planilha Retornos_Qigger com ID e data_atualizacao
        # Arquivo único e acumulativo (sem timestamp no nome)
        if self.google_drive_path and records and results_map:
            try:
                # Nome do arquivo fixo: Retornos_Qigger.csv (acumula dados de todas as planilhas)
                retornos_file = self.google_drive_path / "Retornos_Qigger.csv"
                
                # Gerar planilha com todos os dados tratados (append se já existir)
                if CSVGenerator.generate_retornos_qigger_csv(records, results_map, retornos_file):
                    copied_paths.append(str(retornos_file))
                    logger.info(f"Planilha Retornos_Qigger atualizada: {retornos_file} ({len(records)} registros adicionados)")
            except Exception as e:
                logger.error(f"Erro ao gerar planilha Retornos_Qigger: {e}")
        
        # Backoffice: Gerar planilhas Aprovisionamentos e Reabertura
        if self.backoffice_path and records and results_map:
            try:
                # Planilha Aprovisionamentos
                aprovisionamentos_file = self.backoffice_path / f"Aprovisionamentos_{timestamp}_{source_name}.csv"
                if CSVGenerator.generate_aprovisionamentos_csv(records, results_map, aprovisionamentos_file, objects_loader):
                    copied_paths.append(str(aprovisionamentos_file))
                    logger.info(f"Planilha Aprovisionamentos gerada: {aprovisionamentos_file}")
                
                # Planilha Reabertura
                reabertura_file = self.backoffice_path / f"Reabertura_{timestamp}_{source_name}.csv"
                if CSVGenerator.generate_reabertura_csv(records, results_map, reabertura_file):
                    copied_paths.append(str(reabertura_file))
                    logger.info(f"Planilha Reabertura gerada: {reabertura_file}")
                    
            except Exception as e:
                logger.error(f"Erro ao gerar planilhas Backoffice: {e}")
        
        # Fallback: Se não tiver records/results_map, copiar arquivo original
        if not (records and results_map):
            status = "PROCESSADO" if success else "ERRO"
            new_name = f"{source_name}_{status}_{timestamp}{source_file.suffix}"
            
            # Copiar para Google Drive
            if self.google_drive_path:
                try:
                    dest_path = self.google_drive_path / new_name
                    shutil.copy2(source_file, dest_path)
                    copied_paths.append(str(dest_path))
                    logger.info(f"Arquivo copiado para Google Drive: {dest_path}")
                except Exception as e:
                    logger.error(f"Erro ao copiar para Google Drive: {e}")
            
            # Copiar para Backoffice
            if self.backoffice_path:
                try:
                    dest_path = self.backoffice_path / new_name
                    shutil.copy2(source_file, dest_path)
                    copied_paths.append(str(dest_path))
                    logger.info(f"Arquivo copiado para Backoffice: {dest_path}")
                except Exception as e:
                    logger.error(f"Erro ao copiar para Backoffice: {e}")
        
        return copied_paths
    
    def delete_source_file(self, source_file: Path) -> bool:
        """
        Deleta arquivo fonte após processamento e cópia
        
        Args:
            source_file: Arquivo a ser deletado
            
        Returns:
            True se deletado com sucesso, False caso contrário
        """
        try:
            if source_file.exists():
                source_file.unlink()
                logger.info(f"Arquivo fonte deletado: {source_file}")
                return True
            else:
                logger.warning(f"Arquivo não existe para deletar: {source_file}")
                return False
        except Exception as e:
            logger.error(f"Erro ao deletar arquivo fonte {source_file}: {e}")
            return False
    
    def process_and_cleanup(
        self,
        source_file: Path,
        success: bool = True,
        records: Optional[List[PortabilidadeRecord]] = None,
        results_map: Optional[Dict[str, List['DecisionResult']]] = None,
        objects_loader=None
    ) -> dict:
        """
        Processa arquivo completo: copia para outputs e deleta fonte
        
        Args:
            source_file: Arquivo fonte
            success: Se processamento foi bem-sucedido
            records: Lista de registros processados (opcional, para gerar planilhas específicas)
            results_map: Dicionário mapeando CPF+Ordem para resultados (opcional)
            objects_loader: Loader de objetos para verificar status de entrega (opcional)
            
        Returns:
            Dicionário com resultados da operação
        """
        result = {
            'source_file': str(source_file),
            'success': success,
            'copied_to': [],
            'deleted': False,
            'errors': []
        }
        
        # Copiar para outputs (gera planilhas específicas se records/results_map fornecidos)
        try:
            copied_paths = self.copy_to_outputs(source_file, success, records, results_map, objects_loader)
            result['copied_to'] = copied_paths
        except Exception as e:
            error_msg = f"Erro ao copiar para outputs: {e}"
            logger.error(error_msg)
            result['errors'].append(error_msg)
        
        # NÃO deletar arquivo fonte por padrão (comentado para manter arquivos)
        # Descomente a linha abaixo se quiser deletar após processar
        # try:
        #     result['deleted'] = self.delete_source_file(source_file)
        # except Exception as e:
        #     error_msg = f"Erro ao deletar arquivo fonte: {e}"
        #     logger.error(error_msg)
        #     result['errors'].append(error_msg)
        
        return result

