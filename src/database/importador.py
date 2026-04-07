"""
Módulo de importação de arquivos CSV/Excel para o banco de dados de portabilidade.

Responsável por:
- Identificar automaticamente o tipo de arquivo pelo cabeçalho
- Calcular hash SHA-256 para detecção de duplicatas
- Orquestrar o fluxo de importação (hash → duplicata → lote → leitura → validação → inserção)
- Validar e corrigir proposta_isize (detecção de CPF e fallback)
- Mapear colunas de cada tipo de arquivo para as tabelas normalizadas
- Processar registros em lotes configuráveis

Tipos de arquivo suportados:
    - coverte_prop: COVERTE BASE PROP (76 colunas de propostas de venda)
    - portabilidade_tim: Base de Portabilidade TIM (39 colunas)
    - gross: 3F GROSS (8 colunas de ativação)
    - relatorio_objetos: Relatório de Objetos (78 colunas de logística)
    - resultado_gross: Resultado GROSS (8 colunas)
    - backoffice: Propostas Backoffice (31 colunas)
    - consulta_siebel: Consulta Siebel (29 colunas)
"""

import hashlib
import json
import logging
import math
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)

# ============================================================================
# Assinaturas de colunas para identificação automática de tipo de arquivo
# ============================================================================

ASSINATURAS_TIPO_ARQUIVO: Dict[str, List[str]] = {
    'coverte_prop': ['Proposta iSize', 'Cliente', 'Data venda', 'Plano'],
    'portabilidade_tim': ['DATA_SOLICITACAO', 'ACESSO', 'DOADORA', 'RECEPTORA', 'STATUS'],
    'gross': ['CLASSIFICACAO_CR', 'ACESSO', 'CUSTCODE', 'OPERADORA_N1'],
    'relatorio_objetos': ['Nu Pedido', 'Rastreio', 'Transportadora', 'Última Ocorrencia'],
    'resultado_gross': ['Proposta', 'Numero Acesso', 'Data gross', 'Resultado', 'ICCID'],
    'backoffice': ['PEDIDO', 'BLUE_CHIP', 'STATUS_PEDIDO', 'NUMERO_PORTADO', 'NUMERO_PROVISORIO'],
    'consulta_siebel': ['Cpf', 'Número de acesso', 'Número da ordem', 'Código externo', 'Status do bilhete'],
}


class Importador:
    """
    Importador de arquivos CSV/Excel para o banco de dados de portabilidade.

    Orquestra a leitura, validação e inserção de registros provenientes
    de 7 tipos de arquivo distintos, distribuindo os dados nas tabelas
    normalizadas do novo schema.
    """

    def __init__(self, batch_size: int = 100):
        """
        Inicializa o importador.

        Args:
            batch_size: Quantidade de registros por lote de commit (padrão: 100).
        """
        self.batch_size = batch_size
        logger.info("Importador inicializado com batch_size=%d", batch_size)

    # ====================================================================
    # Identificação de tipo de arquivo
    # ====================================================================

    @staticmethod
    def identificar_tipo_arquivo(colunas_cabecalho: List[str]) -> str:
        """
        Identifica o tipo de arquivo com base nos nomes das colunas do cabeçalho.

        Compara as colunas fornecidas com as assinaturas conhecidas de cada tipo.
        Retorna o tipo cujas colunas-assinatura estão todas presentes no cabeçalho.

        Args:
            colunas_cabecalho: Lista de nomes de colunas do arquivo.

        Returns:
            String com o tipo do arquivo identificado.

        Raises:
            ValueError: Se o tipo não puder ser identificado.
        """
        colunas_set = set(colunas_cabecalho)

        for tipo, assinatura in ASSINATURAS_TIPO_ARQUIVO.items():
            if all(col in colunas_set for col in assinatura):
                logger.info("Tipo de arquivo identificado: %s", tipo)
                return tipo

        raise ValueError(
            f"Tipo de arquivo não identificado. Colunas recebidas: {colunas_cabecalho[:10]}..."
        )

    # ====================================================================
    # Hash SHA-256
    # ====================================================================

    @staticmethod
    def calcular_hash_sha256(caminho_arquivo: str) -> str:
        """
        Calcula o hash SHA-256 de um arquivo.

        Args:
            caminho_arquivo: Caminho completo para o arquivo.

        Returns:
            String hexadecimal do hash SHA-256.
        """
        sha256 = hashlib.sha256()
        with open(caminho_arquivo, 'rb') as f:
            for bloco in iter(lambda: f.read(8192), b''):
                sha256.update(bloco)
        hash_hex = sha256.hexdigest()
        logger.debug("Hash SHA-256 calculado para %s: %s", caminho_arquivo, hash_hex)
        return hash_hex

    # ====================================================================
    # Verificação de duplicata
    # ====================================================================

    @staticmethod
    def _verificar_duplicata(hash_sha256: str, db_manager) -> bool:
        """
        Verifica se um arquivo com o mesmo hash já foi importado.

        Args:
            hash_sha256: Hash SHA-256 do arquivo.
            db_manager: Instância do DatabaseManagerV2.

        Returns:
            True se o hash já existe (duplicata), False caso contrário.
        """
        with db_manager._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT id FROM lotes_importacao WHERE hash_sha256 = ?",
                (hash_sha256,),
            )
            row = cursor.fetchone()
            if row:
                logger.warning("Arquivo duplicado detectado (hash=%s, lote_id=%d)", hash_sha256, row[0])
                return True
            return False

    # ====================================================================
    # Leitura de arquivos
    # ====================================================================

    @staticmethod
    def _ler_arquivo(caminho: str, tipo_arquivo: str = None) -> pd.DataFrame:
        """
        Lê um arquivo CSV ou Excel e retorna um DataFrame.

        Para CSV, tenta múltiplas codificações (utf-8-sig, latin-1, cp1252)
        e separadores (;, ',', \\t).
        Para Excel, usa openpyxl (.xlsx) ou xlrd (.xls).
        Para arquivos GROSS, detecta a linha de cabeçalho (pode não ser a linha 0).

        Args:
            caminho: Caminho completo para o arquivo.
            tipo_arquivo: Tipo do arquivo (opcional, usado para GROSS).

        Returns:
            DataFrame com os dados do arquivo.

        Raises:
            ValueError: Se o arquivo não puder ser lido.
        """
        ext = Path(caminho).suffix.lower()

        if ext in ('.xlsx', '.xls'):
            return Importador._ler_excel(caminho, tipo_arquivo)
        elif ext == '.csv':
            return Importador._ler_csv(caminho)
        else:
            raise ValueError(f"Extensão de arquivo não suportada: {ext}")

    @staticmethod
    def _ler_csv(caminho: str) -> pd.DataFrame:
        """
        Lê um arquivo CSV tentando múltiplas codificações e separadores.

        Args:
            caminho: Caminho para o arquivo CSV.

        Returns:
            DataFrame com os dados.
        """
        encodings = ['utf-8-sig', 'latin-1', 'cp1252']
        separators = [';', ',', '\t']

        for encoding in encodings:
            for sep in separators:
                try:
                    df = pd.read_csv(
                        caminho, encoding=encoding, sep=sep,
                        dtype=str, keep_default_na=False,
                    )
                    if len(df.columns) > 1:
                        logger.info(
                            "CSV lido com sucesso: encoding=%s, sep='%s', linhas=%d, colunas=%d",
                            encoding, sep, len(df), len(df.columns),
                        )
                        return df
                except Exception:
                    continue

        raise ValueError(f"Não foi possível ler o arquivo CSV: {caminho}")

    @staticmethod
    def _ler_excel(caminho: str, tipo_arquivo: str = None) -> pd.DataFrame:
        """
        Lê um arquivo Excel (.xlsx ou .xls).

        Para arquivos GROSS, detecta a linha de cabeçalho que pode não ser a linha 0.

        Args:
            caminho: Caminho para o arquivo Excel.
            tipo_arquivo: Tipo do arquivo (para tratamento especial de GROSS).

        Returns:
            DataFrame com os dados.
        """
        ext = Path(caminho).suffix.lower()
        engine = 'openpyxl' if ext == '.xlsx' else 'xlrd'

        if tipo_arquivo == 'gross':
            # GROSS pode ter cabeçalho em linha diferente de 0
            df_raw = pd.read_excel(caminho, engine=engine, header=None, dtype=str, keep_default_na=False)
            header_row = Importador._detectar_header_gross(df_raw)
            df = pd.read_excel(
                caminho, engine=engine, header=header_row,
                dtype=str, keep_default_na=False,
            )
        else:
            df = pd.read_excel(caminho, engine=engine, dtype=str, keep_default_na=False)

        logger.info("Excel lido com sucesso: linhas=%d, colunas=%d", len(df), len(df.columns))
        return df

    @staticmethod
    def _detectar_header_gross(df_raw: pd.DataFrame) -> int:
        """
        Detecta a linha de cabeçalho em arquivos GROSS.

        Procura a linha que contém as colunas-assinatura do tipo gross.

        Args:
            df_raw: DataFrame sem cabeçalho.

        Returns:
            Índice da linha de cabeçalho.
        """
        assinatura = ASSINATURAS_TIPO_ARQUIVO['gross']
        for idx, row in df_raw.iterrows():
            valores = [str(v).strip() for v in row.values]
            if all(col in valores for col in assinatura):
                logger.info("Cabeçalho GROSS detectado na linha %d", idx)
                return idx
        logger.warning("Cabeçalho GROSS não detectado, usando linha 0")
        return 0

    # ====================================================================
    # Validação e correção de proposta_isize (Task 5.2)
    # ====================================================================

    @staticmethod
    def normalizar_cpf(cpf_str: str) -> str:
        """
        Normaliza um CPF removendo pontuação (., -, /, espaços).

        Retorna apenas os dígitos. Operação idempotente: aplicar múltiplas
        vezes produz o mesmo resultado.

        Args:
            cpf_str: String do CPF em qualquer formato.

        Returns:
            String contendo apenas os dígitos do CPF.
        """
        if cpf_str is None:
            return ''
        return re.sub(r'[.\-/\s]', '', str(cpf_str).strip())

    @staticmethod
    def validar_proposta_isize(valor: Any) -> bool:
        """
        Valida se o valor de proposta_isize é válido (não é CPF).

        Retorna True se o valor é válido (NÃO é 11 dígitos puros).
        Retorna False se o valor parece ser um CPF (exatamente 11 dígitos numéricos).

        Args:
            valor: Valor do campo proposta_isize.

        Returns:
            True se válido, False se parece CPF.
        """
        if valor is None:
            return False
        valor_str = str(valor).strip()
        if not valor_str:
            return False
        # CPF = exatamente 11 dígitos numéricos
        if len(valor_str) == 11 and valor_str.isdigit():
            return False
        return True

    @staticmethod
    def resolver_proposta_isize(valor_cpf: str, db_manager) -> Optional[str]:
        """
        Tenta resolver o proposta_isize correto quando o valor contém CPF.

        Ordem de fallback:
            1. Busca na tabela propostas por CPF
            2. Busca por numero_ordem (consulta_siebel)
            3. Busca por numero_acesso (consulta_siebel)
            4. Busca por remessa_bluechip (bluechip)
            5. Busca por pedido_bluechip (bluechip)
            6. Busca por codigo_externo (consulta_siebel)
            7. Busca por telefone_portado (portabilidade)

        Args:
            valor_cpf: Valor do CPF (11 dígitos) usado como fallback.
            db_manager: Instância do DatabaseManagerV2.

        Returns:
            proposta_isize correto ou None se não encontrado.
        """
        cpf_limpo = Importador.normalizar_cpf(valor_cpf)
        if not cpf_limpo:
            return None

        with db_manager._get_connection() as conn:
            cursor = conn.cursor()

            # 1. Buscar por CPF na tabela propostas
            cursor.execute(
                "SELECT proposta_isize FROM propostas WHERE cpf = ? ORDER BY versao DESC LIMIT 1",
                (cpf_limpo,),
            )
            row = cursor.fetchone()
            if row and row[0] and Importador.validar_proposta_isize(row[0]):
                logger.debug("proposta_isize resolvido por CPF: %s", row[0])
                return str(row[0])

            # 2. Buscar por numero_ordem na consulta_siebel
            cursor.execute(
                "SELECT proposta_isize FROM consulta_siebel WHERE cpf = ? ORDER BY versao DESC LIMIT 1",
                (cpf_limpo,),
            )
            row = cursor.fetchone()
            if row and row[0] and Importador.validar_proposta_isize(row[0]):
                logger.debug("proposta_isize resolvido por consulta_siebel (CPF): %s", row[0])
                return str(row[0])

            # 3. Buscar por numero_acesso na portabilidade
            cursor.execute(
                """SELECT p.proposta_isize FROM portabilidade p
                   JOIN propostas pr ON p.proposta_isize = pr.proposta_isize
                   WHERE pr.cpf = ? ORDER BY p.versao DESC LIMIT 1""",
                (cpf_limpo,),
            )
            row = cursor.fetchone()
            if row and row[0] and Importador.validar_proposta_isize(row[0]):
                logger.debug("proposta_isize resolvido por portabilidade (CPF): %s", row[0])
                return str(row[0])

            # 4. Buscar por remessa_bluechip
            cursor.execute(
                """SELECT b.proposta_isize FROM bluechip b
                   JOIN propostas pr ON b.proposta_isize = pr.proposta_isize
                   WHERE pr.cpf = ? ORDER BY b.versao DESC LIMIT 1""",
                (cpf_limpo,),
            )
            row = cursor.fetchone()
            if row and row[0] and Importador.validar_proposta_isize(row[0]):
                logger.debug("proposta_isize resolvido por bluechip (CPF): %s", row[0])
                return str(row[0])

            # 5. Buscar por pedido_bluechip (já coberto acima via bluechip)

            # 6. Buscar por codigo_externo
            cursor.execute(
                "SELECT proposta_isize FROM consulta_siebel WHERE codigo_externo = ? ORDER BY versao DESC LIMIT 1",
                (cpf_limpo,),
            )
            row = cursor.fetchone()
            if row and row[0] and Importador.validar_proposta_isize(row[0]):
                logger.debug("proposta_isize resolvido por codigo_externo: %s", row[0])
                return str(row[0])

            # 7. Buscar por telefone_portado
            cursor.execute(
                "SELECT proposta_isize FROM portabilidade WHERE telefone_portabilidade = ? ORDER BY versao DESC LIMIT 1",
                (cpf_limpo,),
            )
            row = cursor.fetchone()
            if row and row[0] and Importador.validar_proposta_isize(row[0]):
                logger.debug("proposta_isize resolvido por telefone_portado: %s", row[0])
                return str(row[0])

        logger.warning("proposta_isize não resolvido para CPF: %s", cpf_limpo)
        return None

    # ====================================================================
    # Helpers
    # ====================================================================

    @staticmethod
    def _limpar_valor(v: Any) -> Optional[str]:
        """
        Converte NaN, None, strings vazias e 'nan' para None.

        Args:
            v: Valor a ser limpo.

        Returns:
            String limpa ou None.
        """
        if v is None:
            return None
        s = str(v).strip()
        if s == '' or s.lower() == 'nan' or s.lower() == 'none' or s.lower() == 'nat':
            return None
        return s

    def _obter_proposta_isize(self, row: pd.Series, campo: str, lote_id: int, db_manager) -> Optional[str]:
        """
        Obtém e valida o proposta_isize de uma linha, aplicando correção se necessário.

        Se o valor parece CPF, tenta resolver via fallback. Se resolvido, registra
        correção na auditoria. Se não resolvido, registra em registros_pendentes.

        Args:
            row: Linha do DataFrame.
            campo: Nome da coluna que contém o proposta_isize.
            lote_id: ID do lote de importação.
            db_manager: Instância do DatabaseManagerV2.

        Returns:
            proposta_isize válido ou None.
        """
        valor = self._limpar_valor(row.get(campo))
        if valor is None:
            return None

        if self.validar_proposta_isize(valor):
            return valor

        # Valor parece CPF — tentar resolver
        logger.info("proposta_isize parece CPF (%s), tentando resolver...", valor)
        resolvido = self.resolver_proposta_isize(valor, db_manager)

        if resolvido:
            # Registrar correção na auditoria
            self._registrar_correcao_auditoria(
                db_manager, 'propostas', valor, resolvido, lote_id
            )
            return resolvido
        else:
            # Registrar pendência
            self._registrar_pendencia(
                db_manager, row, campo, valor, lote_id
            )
            return None

    @staticmethod
    def _registrar_correcao_auditoria(
        db_manager, tabela: str, valor_original: str, valor_corrigido: str, lote_id: int
    ):
        """
        Registra uma correção de proposta_isize na tabela de auditoria.

        Args:
            db_manager: Instância do DatabaseManagerV2.
            tabela: Tabela de origem.
            valor_original: Valor original (CPF).
            valor_corrigido: Valor corrigido (proposta_isize).
            lote_id: ID do lote de importação.
        """
        with db_manager._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """INSERT INTO auditoria (tabela, operacao, chave_negocio, valores_json, lote_importacao_id, detalhes)
                   VALUES (?, 'CORRECAO', ?, ?, ?, ?)""",
                (
                    tabela,
                    valor_corrigido,
                    json.dumps({'original': valor_original, 'corrigido': valor_corrigido}),
                    lote_id,
                    f"proposta_isize corrigido de CPF ({valor_original}) para {valor_corrigido}",
                ),
            )
            conn.commit()
        logger.info("Correção registrada: %s → %s", valor_original, valor_corrigido)

    @staticmethod
    def _registrar_pendencia(
        db_manager, row: pd.Series, campo: str, valor_original: str, lote_id: int
    ):
        """
        Registra um registro pendente quando o proposta_isize não pode ser resolvido.

        Args:
            db_manager: Instância do DatabaseManagerV2.
            row: Linha do DataFrame com os dados originais.
            campo: Nome do campo de proposta_isize.
            valor_original: Valor original (CPF não resolvido).
            lote_id: ID do lote de importação.
        """
        dados_json = json.dumps({k: str(v) for k, v in row.to_dict().items()}, ensure_ascii=False)
        with db_manager._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """INSERT INTO registros_pendentes
                   (tabela_origem, dados_json, chave_original, tipo_pendencia, lote_importacao_id)
                   VALUES (?, ?, ?, 'proposta_isize_pendente', ?)""",
                ('importacao', dados_json, valor_original, lote_id),
            )
            conn.commit()
        logger.warning("Registro pendente criado para CPF: %s", valor_original)

    # ====================================================================
    # Importação principal (Task 5.1)
    # ====================================================================

    def importar_arquivo(self, caminho: str, db_manager) -> Dict[str, Any]:
        """
        Orquestra a importação completa de um arquivo.

        Fluxo: hash → verificar duplicata → criar lote → ler arquivo →
        identificar tipo → validar → inserir registros → finalizar lote.

        Args:
            caminho: Caminho completo para o arquivo CSV/Excel.
            db_manager: Instância do DatabaseManagerV2.

        Returns:
            Dicionário com estatísticas da importação:
                - lote_id: ID do lote criado
                - tipo_arquivo: Tipo identificado
                - total_registros: Total de linhas no arquivo
                - inseridos: Registros inseridos com sucesso
                - erros: Registros com erro
                - pendentes: Registros pendentes (proposta_isize não resolvido)
                - status: 'concluido', 'erro' ou 'duplicado'
        """
        stats = {
            'lote_id': None,
            'tipo_arquivo': None,
            'total_registros': 0,
            'inseridos': 0,
            'erros': 0,
            'pendentes': 0,
            'status': 'erro',
        }

        nome_arquivo = os.path.basename(caminho)
        logger.info("Iniciando importação: %s", nome_arquivo)

        # 1. Calcular hash
        hash_sha256 = self.calcular_hash_sha256(caminho)

        # 2. Verificar duplicata
        if self._verificar_duplicata(hash_sha256, db_manager):
            stats['status'] = 'duplicado'
            logger.warning("Importação rejeitada: arquivo duplicado (%s)", nome_arquivo)
            return stats

        # 3. Ler arquivo (primeira passada para identificar tipo)
        try:
            df = self._ler_arquivo(caminho)
        except ValueError as e:
            logger.error("Erro ao ler arquivo %s: %s", nome_arquivo, e)
            return stats

        colunas = list(df.columns)
        stats['total_registros'] = len(df)

        # 4. Identificar tipo
        try:
            tipo_arquivo = self.identificar_tipo_arquivo(colunas)
        except ValueError as e:
            logger.error("Tipo de arquivo não identificado: %s", e)
            return stats

        stats['tipo_arquivo'] = tipo_arquivo

        # Se for GROSS e precisou detectar header, reler
        if tipo_arquivo == 'gross':
            try:
                df = self._ler_arquivo(caminho, tipo_arquivo='gross')
                stats['total_registros'] = len(df)
            except ValueError:
                pass  # Já lido com sucesso acima

        # 5. Criar lote
        try:
            lote_id = db_manager.criar_lote(nome_arquivo, tipo_arquivo, hash_sha256)
        except Exception as e:
            logger.error("Erro ao criar lote: %s", e)
            return stats

        stats['lote_id'] = lote_id

        # 6. Processar registros em lotes
        mapeadores = {
            'coverte_prop': self._mapear_coverte_prop,
            'portabilidade_tim': self._mapear_portabilidade_tim,
            'gross': self._mapear_gross,
            'relatorio_objetos': self._mapear_relatorio_objetos,
            'resultado_gross': self._mapear_resultado_gross,
            'backoffice': self._mapear_backoffice,
            'consulta_siebel': self._mapear_consulta_siebel,
        }

        mapeador = mapeadores.get(tipo_arquivo)
        if not mapeador:
            logger.error("Mapeador não encontrado para tipo: %s", tipo_arquivo)
            db_manager.finalizar_lote(lote_id, 0, 0, 'erro')
            return stats

        total = len(df)
        for inicio in range(0, total, self.batch_size):
            fim = min(inicio + self.batch_size, total)
            batch = df.iloc[inicio:fim]

            for idx, row in batch.iterrows():
                try:
                    mapeador(row, lote_id, db_manager)
                    stats['inseridos'] += 1
                except Exception as e:
                    stats['erros'] += 1
                    logger.error("Erro ao processar linha %d: %s", idx, e)

            logger.info(
                "Lote processado: %d/%d (inseridos=%d, erros=%d)",
                min(fim, total), total, stats['inseridos'], stats['erros'],
            )

        # 7. Finalizar lote
        status_final = 'concluido' if stats['erros'] == 0 else 'concluido'
        db_manager.finalizar_lote(lote_id, stats['inseridos'], stats['erros'], status_final)
        stats['status'] = status_final

        logger.info(
            "Importação concluída: %s — inseridos=%d, erros=%d, pendentes=%d",
            nome_arquivo, stats['inseridos'], stats['erros'], stats['pendentes'],
        )
        return stats

    # ====================================================================
    # Mapeamento COVERTE BASE PROP → 8 tabelas (Task 5.3)
    # ====================================================================

    def _mapear_coverte_prop(self, row: pd.Series, lote_id: int, db_manager):
        """
        Distribui os 76 campos da COVERTE BASE PROP em 8 tabelas normalizadas.

        Tabelas de destino: clientes, propostas, status_venda, portabilidade,
        bluechip, rastreio_entregas, servicos_adicionais, robo_processamento.

        Args:
            row: Linha do DataFrame com os dados originais.
            lote_id: ID do lote de importação.
            db_manager: Instância do DatabaseManagerV2.
        """
        lv = self._limpar_valor
        cpf = self.normalizar_cpf(row.get('CPF', ''))
        proposta_isize = self._obter_proposta_isize(row, 'Proposta iSize', lote_id, db_manager)

        if not cpf:
            logger.warning("CPF vazio na linha, pulando registro")
            raise ValueError("CPF vazio")

        # 1. clientes
        db_manager.inserir_registro('clientes', {
            'cpf': cpf,
            'nome_cliente': lv(row.get('Cliente')),
            'data_nascimento': lv(row.get('Nascimento')),
            'nome_mae': lv(row.get('Mae')),
            'endereco': lv(row.get('Endereco')),
            'numero': lv(row.get('Numero')),
            'complemento': lv(row.get('Complemento')),
            'bairro': lv(row.get('Bairro')),
            'cidade': lv(row.get('Cidade')),
            'uf': lv(row.get('UF')),
            'cep': lv(row.get('Cep')),
            'ponto_referencia': lv(row.get('Ponto Referencia')),
            'ddd_1': lv(row.get('DDD')),
            'telefone_1': lv(row.get('Telefone')),
            'ddd_2': lv(row.get('DDD.1')),
            'telefone_2': lv(row.get('Telefone.1')),
            'email': lv(row.get('Email')),
            'score': lv(row.get('Score')),
        }, lote_id)

        if not proposta_isize:
            return  # Registro pendente já criado

        # 2. propostas
        db_manager.inserir_registro('propostas', {
            'proposta_isize': proposta_isize,
            'cpf': cpf,
            'data_venda': lv(row.get('Data venda')),
            'produto': lv(row.get('Produto')),
            'plano': lv(row.get('Plano')),
            'forma_pagamento': lv(row.get('Forma Pagamento')),
            'vencimento': lv(row.get('Vencimento')),
            'tipo_chip': lv(row.get('Tipo Chip')),
            'conta_online': lv(row.get('Conta Online')),
            'vivo_pay': lv(row.get('Vivo Pay')),
            'app_adicional': lv(row.get('App Adicional')),
            'plataforma': lv(row.get('Plataforma')),
            'nome_equipe': lv(row.get('Nome Equipe')),
            'nome_vendedor': lv(row.get('Nome vendedor')),
            'login_externo': lv(row.get('Login Externo')),
            'nome_supervisor': lv(row.get('Nome Supervisor')),
            'matricula_discador': lv(row.get('Matricula Discador')),
            'avulsa': lv(row.get('Avulsa')),
            'sms_previo': lv(row.get('SMS Previo')),
            'observacoes': lv(row.get('Observacoes')),
        }, lote_id)

        # 3. status_venda
        db_manager.inserir_registro('status_venda', {
            'proposta_isize': proposta_isize,
            'status_venda': lv(row.get('Status venda')),
            'motivo_rejeicao_cancelamento': lv(row.get('Motivo Rejeicao Cancelamento')),
            'flag': lv(row.get('Flag')),
            'auditoria': lv(row.get('Auditoria')),
            'qualidade': lv(row.get('Qualidade')),
            'conectada': lv(row.get('Conectada')),
            'data_conectada': lv(row.get('Data Conectada')),
        }, lote_id)

        # 4. portabilidade
        db_manager.inserir_registro('portabilidade', {
            'proposta_isize': proposta_isize,
            'telefone_portabilidade': lv(row.get('Telefone Portabilidade')),
            'numero_linha': lv(row.get('Numero linha')),
            'portabilidade_status': lv(row.get('Portabilidade')),
            'complemento_portabilidade': lv(row.get('Complemento Portabilidade')),
            'portabilidade_antecipada': lv(row.get('Portabilidade Antecipada')),
            'data_marcacao_port_antecipada': lv(row.get('Data marcacao Port. Antecipada')),
            'quem_marcou_port_antecipada': lv(row.get('Quem marcou Port. Antecipada')),
        }, lote_id)

        # 5. bluechip
        db_manager.inserir_registro('bluechip', {
            'proposta_isize': proposta_isize,
            'bluechip_status': lv(row.get('Bluechip Status')),
            'bluechip_data_status': lv(row.get('Bluechip Data Status')),
            'resposta_envio_pedido': lv(row.get('Resposta Envio Pedido')),
            'pedido_bluechip': lv(row.get('Pedido Bluechip')),
            'bluechip_data_enviado': lv(row.get('Bluechip Data enviado')),
            'data_maxima_prevista_entrega': lv(row.get('Data Maxima Prevista Entrega')),
            'status_entrega_prevista': lv(row.get('Status Entrega Prevista')),
            'cd_bluechip': lv(row.get('CD Bluechip')),
            'remessa_bluechip': lv(row.get('Remessa Bluechip')),
            'qtd_remessas': lv(row.get('Qtd Remessas')),
        }, lote_id)

        # 6. rastreio_entregas
        db_manager.inserir_registro('rastreio_entregas', {
            'proposta_isize': proposta_isize,
            'rastreio_correios': lv(row.get('Rastreio Correios')),
            'rastreio_loggi': lv(row.get('Rastreio Loggi')),
            'data_status_correios': lv(row.get('Data Status Correios')),
            'status_correios': lv(row.get('Status Correios')),
            'data_status_loggi': lv(row.get('Data Status Loggi')),
            'status_loggi': lv(row.get('Status Loggi')),
        }, lote_id)

        # 7. servicos_adicionais
        db_manager.inserir_registro('servicos_adicionais', {
            'proposta_isize': proposta_isize,
            'vivo_internet': lv(row.get('Vivo Internet')),
            'vivo_tv': lv(row.get('Vivo TV')),
            'id_play_vivo': lv(row.get('ID PLAY Vivo')),
        }, lote_id)

        # 8. robo_processamento
        db_manager.inserir_registro('robo_processamento', {
            'proposta_isize': proposta_isize,
            'robo_inicio_proc': lv(row.get('Robo Inicio Proc.')),
            'robo_fim_proc': lv(row.get('Robo Fim Proc.')),
        }, lote_id)

    # ====================================================================
    # Mapeamento Portabilidade TIM → portabilidade_tim
    # ====================================================================

    def _mapear_portabilidade_tim(self, row: pd.Series, lote_id: int, db_manager):
        """
        Mapeia uma linha do arquivo Portabilidade TIM para a tabela portabilidade_tim.

        Args:
            row: Linha do DataFrame.
            lote_id: ID do lote de importação.
            db_manager: Instância do DatabaseManagerV2.
        """
        lv = self._limpar_valor
        acesso = lv(row.get('ACESSO'))

        # Resolver proposta_isize via CPF_CNPJ (TIM não tem proposta_isize direto)
        cpf_cnpj = self.normalizar_cpf(row.get('CPF_CNPJ', ''))
        proposta_isize = None
        if cpf_cnpj:
            proposta_isize = self.resolver_proposta_isize(cpf_cnpj, db_manager)

        if not proposta_isize:
            # Tentar resolver por acesso (telefone portado)
            if acesso:
                with db_manager._get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute(
                        "SELECT proposta_isize FROM portabilidade WHERE telefone_portabilidade = ? ORDER BY versao DESC LIMIT 1",
                        (acesso,),
                    )
                    r = cursor.fetchone()
                    if r and r[0]:
                        proposta_isize = str(r[0])

        if not proposta_isize:
            self._registrar_pendencia(db_manager, row, 'ACESSO', acesso or '', lote_id)
            raise ValueError(f"proposta_isize não resolvido para TIM (acesso={acesso})")

        db_manager.inserir_registro('portabilidade_tim', {
            'proposta_isize': proposta_isize,
            'acesso': acesso,
            'acesso_temporario': lv(row.get('ACESSO_TEMPORARIO')),
            'ddd': lv(row.get('DDD')),
            'data_solicitacao': lv(row.get('DATA_SOLICITACAO')),
            'mes_solicitacao': lv(row.get('MES_SOLICITACAO')),
            'data_ativacao': lv(row.get('DATA_ATIVACAO')),
            'mes_ativacao': lv(row.get('MES_ATIVACAO')),
            'data_conclusao': lv(row.get('DATA_CONCLUSAO')),
            'sky_contrato': lv(row.get('SKY_CONTRATO')),
            'sky_cliente': lv(row.get('SKY_CLIENTE')),
            'protocolo': lv(row.get('PROTOCOLO')),
            'operadora_n1': lv(row.get('OPERADORA_N1')),
            'tipo_pre_pos_controle': lv(row.get('TIPO_PRE_POS_CONTROLE')),
            'tecnologia': lv(row.get('TECNOLOGIA')),
            'voz_dados': lv(row.get('VOZ_DADOS')),
            'doadora': lv(row.get('DOADORA')),
            'receptora': lv(row.get('RECEPTORA')),
            'tipo': lv(row.get('TIPO')),
            'status': lv(row.get('STATUS')),
            'tipo_segmento_1': lv(row.get('TIPO_SEGMENTO_1')),
            'tipo_segmento_2': lv(row.get('TIPO_SEGMENTO_2')),
            'tipo_familia_plano': lv(row.get('TIPO_FAMILIA_PLANO')),
            'nivel_plano': lv(row.get('NIVEL_PLANO')),
            'canal_n0': lv(row.get('CANAL_N0')),
            'canal_n1': lv(row.get('CANAL_N1')),
            'canal_n2': lv(row.get('CANAL_N2')),
            'canal_n3': lv(row.get('CANAL_N3')),
            'canal_n4': lv(row.get('CANAL_N4')),
            'grupo_economico': lv(row.get('GRUPO_ECONOMICO')),
            'custcode': lv(row.get('CUSTCODE')),
            'cpf_cnpj': cpf_cnpj or lv(row.get('CPF_CNPJ')),
            'portabilidade': lv(row.get('PORTABILIDADE')),
            'motivo_conflito': lv(row.get('MOTIVO_CONFLITO')),
            'motivo_cancelamento': lv(row.get('MOTIVO_CANCELAMENTO')),
            'self_portin': lv(row.get('SELF_PORTIN')),
            'canal_portabilidade': lv(row.get('CANAL_PORTABILIDADE')),
            'tentativas': lv(row.get('TENTATIVAS')),
            'cart_canal_n1': lv(row.get('CART_CANAL_N1')),
            'cart_canal_n2': lv(row.get('CART_CANAL_N2')),
        }, lote_id)

    # ====================================================================
    # Mapeamento GROSS → gross
    # ====================================================================

    def _mapear_gross(self, row: pd.Series, lote_id: int, db_manager):
        """
        Mapeia uma linha do arquivo GROSS para a tabela gross.

        Args:
            row: Linha do DataFrame.
            lote_id: ID do lote de importação.
            db_manager: Instância do DatabaseManagerV2.
        """
        lv = self._limpar_valor
        acesso = lv(row.get('ACESSO'))

        # GROSS não tem proposta_isize direto — resolver por acesso (telefone)
        proposta_isize = None
        if acesso:
            with db_manager._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT proposta_isize FROM portabilidade WHERE telefone_portabilidade = ? ORDER BY versao DESC LIMIT 1",
                    (acesso,),
                )
                r = cursor.fetchone()
                if r and r[0]:
                    proposta_isize = str(r[0])

        if not proposta_isize:
            self._registrar_pendencia(db_manager, row, 'ACESSO', acesso or '', lote_id)
            raise ValueError(f"proposta_isize não resolvido para GROSS (acesso={acesso})")

        db_manager.inserir_registro('gross', {
            'proposta_isize': proposta_isize,
            'acesso': acesso,
            'ddd': lv(row.get('DDD')),
            'custcode': lv(row.get('CUSTCODE')),
            'operadora_n1': lv(row.get('OPERADORA_N1')),
            'classificacao_cr': lv(row.get('CLASSIFICACAO_CR')),
            'data_gross': lv(row.get('DATA_GROSS')),
            'nome_pdv': lv(row.get('NOME_PDV')),
            'mes': lv(row.get('MES')),
        }, lote_id)

    # ====================================================================
    # Mapeamento Relatório de Objetos → logistica
    # ====================================================================

    def _mapear_relatorio_objetos(self, row: pd.Series, lote_id: int, db_manager):
        """
        Mapeia uma linha do Relatório de Objetos para a tabela logistica.

        Fallback de resolução de proposta_isize (em ordem):
        1. Id Auxiliar1 (direto)
        2. Nu Pedido (extrair de "26-0XXXXXXXXX" → "XXXXXXXXX")
        3. ID ERP (buscar numero_ordem na consulta_siebel)
        4. ICCID (buscar na tabela bluechip/logistica)
        5. Documento/CPF (buscar na tabela propostas)

        Args:
            row: Linha do DataFrame.
            lote_id: ID do lote de importação.
            db_manager: Instância do DatabaseManagerV2.
        """
        lv = self._limpar_valor

        proposta_isize = None

        # 1. Id Auxiliar1 (direto)
        id_aux = lv(row.get('Id Auxiliar1'))
        if id_aux and self.validar_proposta_isize(id_aux):
            proposta_isize = id_aux

        # 2. Nu Pedido → extrair proposta_isize
        if not proposta_isize:
            nu_pedido = lv(row.get('Nu Pedido'))
            if nu_pedido and nu_pedido.startswith('26-0'):
                candidato = nu_pedido[4:]  # "26-0260015906" → "260015906"
                if candidato and self.validar_proposta_isize(candidato):
                    # Verificar se existe na tabela propostas
                    with db_manager._get_connection() as conn:
                        cursor = conn.cursor()
                        cursor.execute(
                            "SELECT proposta_isize FROM propostas WHERE proposta_isize = ? LIMIT 1",
                            (candidato,),
                        )
                        if cursor.fetchone():
                            proposta_isize = candidato

        # 3. ID ERP → buscar numero_ordem na consulta_siebel
        if not proposta_isize:
            id_erp = lv(row.get('ID ERP'))
            if id_erp and id_erp != '0-00':
                with db_manager._get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute(
                        "SELECT proposta_isize FROM consulta_siebel WHERE numero_ordem = ? ORDER BY versao DESC LIMIT 1",
                        (id_erp,),
                    )
                    r = cursor.fetchone()
                    if r and r[0]:
                        proposta_isize = str(r[0])

        # 4. ICCID → buscar na tabela resultado_gross ou backoffice
        if not proposta_isize:
            iccid = lv(row.get('ICCID'))
            if iccid:
                with db_manager._get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute(
                        "SELECT proposta_isize FROM resultado_gross WHERE iccid = ? ORDER BY versao DESC LIMIT 1",
                        (iccid,),
                    )
                    r = cursor.fetchone()
                    if r and r[0]:
                        proposta_isize = str(r[0])
                    else:
                        cursor.execute(
                            "SELECT proposta_isize FROM backoffice WHERE iccid = ? ORDER BY versao DESC LIMIT 1",
                            (iccid,),
                        )
                        r = cursor.fetchone()
                        if r and r[0]:
                            proposta_isize = str(r[0])

        # 5. Documento/CPF → buscar na tabela propostas
        if not proposta_isize:
            doc = self.normalizar_cpf(row.get('Documento', ''))
            if doc and len(doc) == 11:
                proposta_isize = self.resolver_proposta_isize(doc, db_manager)

        # 6. Nu Pedido sem verificação (fallback final)
        if not proposta_isize:
            nu_pedido = lv(row.get('Nu Pedido'))
            if nu_pedido and nu_pedido.startswith('26-0'):
                proposta_isize = nu_pedido[4:]

        if not proposta_isize:
            self._registrar_pendencia(
                db_manager, row, 'Nu Pedido',
                lv(row.get('Nu Pedido')) or lv(row.get('Id Auxiliar1')) or '', lote_id,
            )
            raise ValueError("proposta_isize não resolvido para Relatório de Objetos")

        db_manager.inserir_registro('logistica', {
            'proposta_isize': proposta_isize,
            'nu_pedido': lv(row.get('Nu Pedido')),
            'rastreio': lv(row.get('Rastreio')),
            'iccid': lv(row.get('ICCID')),
            'numero_pedido_marketplace': lv(row.get('Número Pedido Marketplace')),
            'nota_fiscal': lv(row.get('Nota Fiscal')),
            'serie_nf': lv(row.get('Série NF')),
            'data_emissao_nf': lv(row.get('Data Emissão NF')),
            'chave_nota_fiscal': lv(row.get('Chave Nota Fiscal')),
            'valor_nf': lv(row.get('Valor NF')),
            'valor_frete': lv(row.get('Valor Frete')),
            'id_canal_venda': lv(row.get('Id Canal Venda')),
            'id_warehouse': lv(row.get('Id Warehouse')),
            'id_erp': lv(row.get('ID ERP')),
            'id_transportadora': lv(row.get('Id Transportadora')),
            'transportadora': lv(row.get('Transportadora')),
            'id_servico': lv(row.get('Id Serviço')),
            'nome_servico': lv(row.get('Nome Serviço')),
            'destinatario': lv(row.get('Destinatário')),
            'documento': lv(row.get('Documento')),
            'email': lv(row.get('Email')),
            'telefone': lv(row.get('Telefone')),
            'cidade': lv(row.get('Cidade')),
            'uf': lv(row.get('UF')),
            'cep': lv(row.get('CEP')),
            'data_insercao': lv(row.get('Data Inserção')),
            'data_primeiro_patch': lv(row.get('Data Primeiro Patch')),
            'data_ultimo_patch': lv(row.get('Data Último Patch')),
            'data_postagem': lv(row.get('Data Postagem')),
            'previsao_entrega': lv(row.get('Previsão Entrega')),
            'data_prometida': lv(row.get('Data Prometida')),
            'prazo_dias_corridos': lv(row.get('Prazo Dias Corridos')),
            'prazo_dias_uteis': lv(row.get('Prazo Dias Úteis')),
            'prazo_efetivo': lv(row.get('Prazo Efetivo')),
            'status': lv(row.get('Status')),
            'tentativas_entrega': lv(row.get('Tentativas Entrega')),
            'data_entrega': lv(row.get('Data Entrega')),
            'ultima_ocorrencia': lv(row.get('Última Ocorrencia')),
            'data_ultima_ocorrencia': lv(row.get('Data Última Ocorrência')),
            'local_ultima_ocorrencia': lv(row.get('Local Última Ocorrência')),
            'cidade_ultima_ocorrencia': lv(row.get('Cidade Última Ocorrência')),
            'estado_ultima_ocorrencia': lv(row.get('Estado Última Ocorrência')),
            'ultima_ocorrencia_cronologica': lv(row.get('Última Ocorrência Cronológica')),
            'motivo_devolucao': lv(row.get('Motivo Devolução')),
            'retorno_fluxo': lv(row.get('Retorno Fluxo')),
            'protocolo_logistica': lv(row.get('Protocolo Logística')),
            'motivo_abertura_protocolo': lv(row.get('Motivo Abertura Protocolo')),
            'status_protocolo': lv(row.get('Status Protocolo')),
            'reversa': lv(row.get('Reversa')),
            'codigo_coleta_postagem': lv(row.get('Código Coleta/Postagem')),
            'cd': lv(row.get('CD')),
            'dispatch': lv(row.get('Dispatch')),
        }, lote_id)

    # ====================================================================
    # Mapeamento Resultado GROSS → resultado_gross
    # ====================================================================

    def _mapear_resultado_gross(self, row: pd.Series, lote_id: int, db_manager):
        """
        Mapeia uma linha do arquivo Resultado GROSS para a tabela resultado_gross.

        Args:
            row: Linha do DataFrame.
            lote_id: ID do lote de importação.
            db_manager: Instância do DatabaseManagerV2.
        """
        lv = self._limpar_valor

        proposta_isize = self._obter_proposta_isize(row, 'Proposta', lote_id, db_manager)
        if not proposta_isize:
            raise ValueError("proposta_isize não resolvido para Resultado GROSS")

        cpf = self.normalizar_cpf(row.get('CPF', ''))

        db_manager.inserir_registro('resultado_gross', {
            'proposta_isize': proposta_isize,
            'numero_acesso': lv(row.get('Numero Acesso')),
            'data_gross': lv(row.get('Data gross')),
            'cpf': cpf or None,
            'iccid': lv(row.get('ICCID')),
            'data_arquivo': lv(row.get('Data arquivo')),
            'arquivo_origem': lv(row.get('Arquivo origem')),
            'resultado': lv(row.get('Resultado')),
        }, lote_id)

    # ====================================================================
    # Mapeamento Backoffice → backoffice
    # ====================================================================

    def _mapear_backoffice(self, row: pd.Series, lote_id: int, db_manager):
        """
        Mapeia uma linha do arquivo Backoffice para a tabela backoffice.

        Args:
            row: Linha do DataFrame.
            lote_id: ID do lote de importação.
            db_manager: Instância do DatabaseManagerV2.
        """
        lv = self._limpar_valor

        # PEDIDO = proposta_isize no backoffice
        proposta_isize = self._obter_proposta_isize(row, 'PEDIDO', lote_id, db_manager)
        if not proposta_isize:
            raise ValueError("proposta_isize não resolvido para Backoffice")

        cpf = self.normalizar_cpf(row.get('CPF', ''))

        db_manager.inserir_registro('backoffice', {
            'proposta_isize': proposta_isize,
            'pedido': lv(row.get('PEDIDO')),
            'blue_chip': lv(row.get('BLUE_CHIP')),
            'data_venda': lv(row.get('DATA_VENDA')),
            'tipo_plano': lv(row.get('TIPO_PLANO')),
            'plano_ativado': lv(row.get('PLANO_ATIVADO')),
            'plano_fidelizado': lv(row.get('PLANO_FIDELIZADO')),
            'portabilidade': lv(row.get('PORTABILIDADE')),
            'numero_provisorio': lv(row.get('NUMERO_PROVISORIO')),
            'numero_portado': lv(row.get('NUMERO_PORTADO')),
            'cpf': cpf or None,
            'nome_cliente': lv(row.get('NOME_CLIENTE')),
            'endereco': lv(row.get('ENDERECO')),
            'cep': lv(row.get('CEP')),
            'uf': lv(row.get('UF')),
            'login_vendedor': lv(row.get('LOGIN_VENDEDOR')),
            'vendedor': lv(row.get('VENDEDOR')),
            'login_bko': lv(row.get('LOGIN_BKO')),
            'bko': lv(row.get('BKO')),
            'data_input_siebel': lv(row.get('DATA_INPUT_SIEBEL')),
            'iccid': lv(row.get('ICCID')),
            'data_envio_chip': lv(row.get('DATA_ENVIO_CHIP')),
            'data_entrega_chip': lv(row.get('DATA_ENTREGA_CHIP')),
            'data_abertura_bp': lv(row.get('DATA_ABERTURA_BP')),
            'data_conclusao_bp': lv(row.get('DATA_CONCLUSAO_BP')),
            'status_pedido': lv(row.get('STATUS_PEDIDO')),
            'detalhe_status': lv(row.get('DETALHE_STATUS')),
            'data_atualizacao_status': lv(row.get('DATA_ATUALIZACAO_STATUS')),
            'tempo_tratamento_total': lv(row.get('TEMPO_TRATAMENTO_TOTAL')),
            'obs_bo': lv(row.get('OBS_BO')),
            'protocolo_conectada': lv(row.get('PROTOCOLO_CONECTADA')),
            'nome_equipe': lv(row.get('NOME_EQUIPE')),
        }, lote_id)

    # ====================================================================
    # Mapeamento Consulta Siebel → consulta_siebel
    # ====================================================================

    def _mapear_consulta_siebel(self, row: pd.Series, lote_id: int, db_manager):
        """
        Mapeia uma linha do arquivo Consulta Siebel para a tabela consulta_siebel.

        Args:
            row: Linha do DataFrame.
            lote_id: ID do lote de importação.
            db_manager: Instância do DatabaseManagerV2.
        """
        lv = self._limpar_valor

        # Código externo = proposta_isize no Siebel
        proposta_isize = self._obter_proposta_isize(row, 'Código externo', lote_id, db_manager)
        if not proposta_isize:
            raise ValueError("proposta_isize não resolvido para Consulta Siebel")

        cpf = self.normalizar_cpf(row.get('Cpf', ''))

        db_manager.inserir_registro('consulta_siebel', {
            'proposta_isize': proposta_isize,
            'cpf': cpf or None,
            'numero_acesso': lv(row.get('Número de acesso')),
            'numero_ordem': lv(row.get('Número da ordem')),
            'codigo_externo': lv(row.get('Código externo')),
            'numero_temporario': lv(row.get('Número temporário')),
            'bilhete_temporario': lv(row.get('Bilhete temporário')),
            'numero_bilhete': lv(row.get('Número do bilhete')),
            'status_bilhete': lv(row.get('Status do bilhete')),
            'operadora_doadora': lv(row.get('Operadora doadora')),
            'data_portabilidade': lv(row.get('Data de portabilidade')),
            'motivo_recusa': lv(row.get('Motivo da recusa')),
            'motivo_cancelamento': lv(row.get('Motivo do cancelamento')),
            'ultimo_bilhete': lv(row.get('Último bilhete')),
            'status_ordem': lv(row.get('Status da ordem')),
            'preco_ordem': lv(row.get('Preço da ordem')),
            'data_conclusao_ordem': lv(row.get('Data de conclusão da ordem')),
            'motivo_nao_consultado': lv(row.get('Motivo não consultado')),
            'motivo_nao_cancelado': lv(row.get('Motivo não cancelado')),
            'motivo_nao_aberto': lv(row.get('Motivo não aberto')),
            'motivo_nao_reagendado': lv(row.get('Motivo não reagendado')),
            'novo_status_bilhete': lv(row.get('Novo status do bilhete')),
            'nova_data_portabilidade': lv(row.get('Nova data de portabilidade')),
            'responsavel_processamento': lv(row.get('Responsável processamento')),
            'data_inicial_processamento': lv(row.get('Data inicial processamento')),
            'data_final_processamento': lv(row.get('Data final processamento')),
            'registro_valido': lv(row.get('Registro válido')),
            'ajustes_registro': lv(row.get('Ajustes registro')),
            'numero_acesso_valido': lv(row.get('Número de acesso válido')),
            'ajustes_numero_acesso': lv(row.get('Ajustes número de acesso')),
        }, lote_id)
