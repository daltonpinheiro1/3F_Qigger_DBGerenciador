"""
Schema SQL do novo banco de dados de Portabilidade.

Define todo o DDL (Data Definition Language) para o banco SQLite redesenhado,
incluindo tabelas de controle, tabelas de dados (INSERT-only, versionadas),
índices, triggers de bloqueio de UPDATE, triggers de auditoria automática,
views de registro corrente e a view unificada principal.

Versão: 1 — Schema inicial (redesign completo)
"""

# =============================================================================
# Versão do Schema
# =============================================================================

SCHEMA_VERSION = 1

# =============================================================================
# PRAGMAs de Performance
# =============================================================================

PRAGMAS = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA cache_size = -128000;
PRAGMA temp_store = MEMORY;
PRAGMA mmap_size = 536870912;
PRAGMA auto_vacuum = INCREMENTAL;
PRAGMA foreign_keys = ON;
"""

# =============================================================================
# 1. Tabelas de Controle (UPDATE permitido)
# =============================================================================

# 1.1 schema_versao
SQL_SCHEMA_VERSAO = """
CREATE TABLE IF NOT EXISTS schema_versao (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    versao INTEGER NOT NULL UNIQUE,
    descricao TEXT NOT NULL,
    script_sql TEXT,
    aplicado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

SQL_SCHEMA_VERSAO_INICIAL = """
INSERT OR IGNORE INTO schema_versao (versao, descricao) VALUES (1, 'Schema inicial - redesign completo');
"""

# 1.2 lotes_importacao
SQL_LOTES_IMPORTACAO = """
CREATE TABLE IF NOT EXISTS lotes_importacao (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nome_arquivo TEXT NOT NULL,
    caminho_origem TEXT,
    tipo_arquivo TEXT NOT NULL CHECK (tipo_arquivo IN (
        'coverte_prop', 'portabilidade_tim', 'gross',
        'relatorio_objetos', 'resultado_gross', 'backoffice',
        'consulta_siebel', 'migracao', 'reprocessamento',
        'vendas_eva', 'retorno_rpa_tim', 'auditoria_vendas'
    )),
    hash_sha256 TEXT NOT NULL,
    qtd_registros INTEGER DEFAULT 0,
    qtd_inseridos INTEGER DEFAULT 0,
    qtd_erros INTEGER DEFAULT 0,
    status TEXT DEFAULT 'em_andamento' CHECK (status IN (
        'em_andamento', 'concluido', 'erro', 'duplicado'
    )),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    finalizado_em TIMESTAMP,
    UNIQUE(hash_sha256)
);

CREATE INDEX IF NOT EXISTS idx_lotes_hash ON lotes_importacao(hash_sha256);
CREATE INDEX IF NOT EXISTS idx_lotes_tipo ON lotes_importacao(tipo_arquivo);
CREATE INDEX IF NOT EXISTS idx_lotes_created ON lotes_importacao(created_at DESC);
"""

# 1.3 arquivos_importados
SQL_ARQUIVOS_IMPORTADOS = """
CREATE TABLE IF NOT EXISTS arquivos_importados (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    lote_importacao_id INTEGER NOT NULL,
    nome_arquivo TEXT NOT NULL,
    caminho_completo TEXT,
    tamanho_bytes INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (lote_importacao_id) REFERENCES lotes_importacao(id)
);

CREATE INDEX IF NOT EXISTS idx_arquivos_lote ON arquivos_importados(lote_importacao_id);
"""

# 1.4 execucoes_processamento
SQL_EXECUCOES_PROCESSAMENTO = """
CREATE TABLE IF NOT EXISTS execucoes_processamento (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tipo TEXT NOT NULL CHECK (tipo IN (
        'processamento_completo', 'importacao', 'reprocessamento',
        'geracao_homologacao', 'backup', 'migracao'
    )),
    parametros TEXT,
    status TEXT DEFAULT 'em_andamento' CHECK (status IN (
        'em_andamento', 'concluido', 'erro', 'cancelado'
    )),
    etapa_atual TEXT,
    registros_processados INTEGER DEFAULT 0,
    registros_erro INTEGER DEFAULT 0,
    detalhes_erro TEXT,
    inicio_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    fim_em TIMESTAMP,
    duracao_segundos REAL
);

CREATE INDEX IF NOT EXISTS idx_execucoes_status ON execucoes_processamento(status);
CREATE INDEX IF NOT EXISTS idx_execucoes_tipo ON execucoes_processamento(tipo);
CREATE INDEX IF NOT EXISTS idx_execucoes_inicio ON execucoes_processamento(inicio_em DESC);
"""

# 1.5 auditoria
SQL_AUDITORIA = """
CREATE TABLE IF NOT EXISTS auditoria (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tabela TEXT NOT NULL,
    operacao TEXT NOT NULL CHECK (operacao IN ('INSERT', 'CORRECAO', 'MIGRACAO', 'REGRA_APLICADA')),
    registro_id INTEGER,
    chave_negocio TEXT,
    versao_registro INTEGER,
    valores_json TEXT,
    lote_importacao_id INTEGER,
    execucao_id INTEGER,
    detalhes TEXT,
    tempo_execucao_ms REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (lote_importacao_id) REFERENCES lotes_importacao(id),
    FOREIGN KEY (execucao_id) REFERENCES execucoes_processamento(id)
);

CREATE INDEX IF NOT EXISTS idx_auditoria_tabela ON auditoria(tabela);
CREATE INDEX IF NOT EXISTS idx_auditoria_operacao ON auditoria(operacao);
CREATE INDEX IF NOT EXISTS idx_auditoria_chave ON auditoria(chave_negocio);
CREATE INDEX IF NOT EXISTS idx_auditoria_created ON auditoria(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_auditoria_lote ON auditoria(lote_importacao_id);
CREATE INDEX IF NOT EXISTS idx_auditoria_periodo ON auditoria(created_at, tabela);
"""

# 1.6 historico_backups
SQL_HISTORICO_BACKUPS = """
CREATE TABLE IF NOT EXISTS historico_backups (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nome_arquivo TEXT NOT NULL,
    caminho_destino TEXT NOT NULL,
    destino_tipo TEXT NOT NULL CHECK (destino_tipo IN ('local', 'rede', 'smb')),
    tamanho_bytes INTEGER,
    status TEXT NOT NULL CHECK (status IN ('sucesso', 'falha', 'parcial')),
    detalhes_erro TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_backups_created ON historico_backups(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_backups_status ON historico_backups(status);
"""

# 1.7 metricas_processamento
SQL_METRICAS_PROCESSAMENTO = """
CREATE TABLE IF NOT EXISTS metricas_processamento (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    execucao_id INTEGER,
    etapa TEXT NOT NULL,
    registros_total INTEGER DEFAULT 0,
    registros_por_segundo REAL,
    tempo_execucao_ms REAL,
    memoria_utilizada_mb REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (execucao_id) REFERENCES execucoes_processamento(id)
);

CREATE INDEX IF NOT EXISTS idx_metricas_execucao ON metricas_processamento(execucao_id);
CREATE INDEX IF NOT EXISTS idx_metricas_etapa ON metricas_processamento(etapa);
"""

# 1.8 registros_pendentes
SQL_REGISTROS_PENDENTES = """
CREATE TABLE IF NOT EXISTS registros_pendentes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tabela_origem TEXT NOT NULL,
    dados_json TEXT NOT NULL,
    chave_original TEXT,
    tipo_pendencia TEXT NOT NULL CHECK (tipo_pendencia IN (
        'proposta_isize_pendente', 'chave_duplicada', 'dados_invalidos'
    )),
    tentativas_resolucao INTEGER DEFAULT 0,
    resolvido INTEGER DEFAULT 0,
    proposta_isize_resolvido TEXT,
    lote_importacao_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    resolvido_em TIMESTAMP,
    FOREIGN KEY (lote_importacao_id) REFERENCES lotes_importacao(id)
);

CREATE INDEX IF NOT EXISTS idx_pendentes_tipo ON registros_pendentes(tipo_pendencia);
CREATE INDEX IF NOT EXISTS idx_pendentes_resolvido ON registros_pendentes(resolvido);
CREATE INDEX IF NOT EXISTS idx_pendentes_chave ON registros_pendentes(chave_original);
"""

# 1.9 cache_base_unificada
SQL_CACHE_BASE_UNIFICADA = """
CREATE TABLE IF NOT EXISTS cache_base_unificada (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    proposta_isize TEXT NOT NULL UNIQUE,
    cpf TEXT,
    nome_cliente TEXT,
    telefone_portabilidade TEXT,
    numero_linha TEXT,
    numero_ordem TEXT,
    -- Dados de proposta
    data_venda TEXT,
    produto TEXT,
    plano TEXT,
    forma_pagamento TEXT,
    nome_equipe TEXT,
    nome_vendedor TEXT,
    -- Dados de cliente
    data_nascimento TEXT,
    endereco TEXT,
    endereco_numero TEXT,
    complemento TEXT,
    bairro TEXT,
    cidade_cliente TEXT,
    uf_cliente TEXT,
    cep_cliente TEXT,
    ddd_1 TEXT,
    telefone_1 TEXT,
    email TEXT,
    score TEXT,
    -- Status venda
    status_venda TEXT,
    motivo_rejeicao_cancelamento TEXT,
    conectada TEXT,
    data_conectada TEXT,
    -- Dados de portabilidade
    portabilidade_status TEXT,
    complemento_portabilidade TEXT,
    -- Dados TIM
    status_tim TEXT,
    data_ativacao_tim TEXT,
    acesso_tim TEXT,
    -- Dados logística
    status_logistica TEXT,
    rastreio TEXT,
    rastreio_logistica TEXT,
    data_entrega TEXT,
    nu_pedido TEXT,
    transportadora TEXT,
    previsao_entrega TEXT,
    -- Dados GROSS
    data_gross TEXT,
    classificacao_cr TEXT,
    -- Dados resultado GROSS
    resultado_gross TEXT,
    -- Dados backoffice
    status_pedido TEXT,
    detalhe_status TEXT,
    data_atualizacao_status TEXT,
    -- Dados Siebel
    numero_acesso TEXT,
    codigo_externo TEXT,
    status_bilhete TEXT,
    operadora_doadora TEXT,
    data_portabilidade TEXT,
    motivo_recusa TEXT,
    motivo_cancelamento TEXT,
    status_ordem TEXT,
    novo_status_bilhete TEXT,
    -- Dados Bluechip
    bluechip_status TEXT,
    pedido_bluechip TEXT,
    remessa_bluechip TEXT,
    data_maxima_prevista_entrega TEXT,
    -- Decisão
    regra_id INTEGER,
    decisao TEXT,
    acao_a_realizar TEXT,
    tipo_mensagem TEXT,
    -- Controle
    hash_dados TEXT,
    atualizado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_cache_proposta ON cache_base_unificada(proposta_isize);
CREATE INDEX IF NOT EXISTS idx_cache_cpf ON cache_base_unificada(cpf);
CREATE INDEX IF NOT EXISTS idx_cache_telefone ON cache_base_unificada(telefone_portabilidade);
CREATE INDEX IF NOT EXISTS idx_cache_numero_linha ON cache_base_unificada(numero_linha);
CREATE INDEX IF NOT EXISTS idx_cache_numero_acesso ON cache_base_unificada(numero_acesso);
CREATE INDEX IF NOT EXISTS idx_cache_atualizado ON cache_base_unificada(atualizado_em DESC);
"""

# =============================================================================
# 2. Tabelas de Dados (INSERT-only, versionadas)
# =============================================================================

# 2.1 clientes
SQL_CLIENTES = """
CREATE TABLE IF NOT EXISTS clientes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    cpf TEXT NOT NULL,
    nome_cliente TEXT,
    data_nascimento TEXT,
    nome_mae TEXT,
    endereco TEXT,
    numero TEXT,
    complemento TEXT,
    bairro TEXT,
    cidade TEXT,
    uf TEXT,
    cep TEXT,
    ponto_referencia TEXT,
    ddd_1 TEXT,
    telefone_1 TEXT,
    ddd_2 TEXT,
    telefone_2 TEXT,
    email TEXT,
    score TEXT,
    versao INTEGER NOT NULL DEFAULT 1,
    lote_importacao_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(cpf, versao),
    FOREIGN KEY (lote_importacao_id) REFERENCES lotes_importacao(id)
);

CREATE INDEX IF NOT EXISTS idx_clientes_cpf ON clientes(cpf);
CREATE INDEX IF NOT EXISTS idx_clientes_cpf_versao ON clientes(cpf, versao DESC);
CREATE INDEX IF NOT EXISTS idx_clientes_created ON clientes(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_clientes_lote ON clientes(lote_importacao_id);
"""

# 2.2 propostas
SQL_PROPOSTAS = """
CREATE TABLE IF NOT EXISTS propostas (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    proposta_isize TEXT NOT NULL,
    cpf TEXT NOT NULL,
    data_venda TEXT,
    produto TEXT,
    plano TEXT,
    forma_pagamento TEXT,
    vencimento TEXT,
    tipo_chip TEXT,
    conta_online TEXT,
    vivo_pay TEXT,
    app_adicional TEXT,
    plataforma TEXT,
    nome_equipe TEXT,
    nome_vendedor TEXT,
    login_externo TEXT,
    nome_supervisor TEXT,
    matricula_discador TEXT,
    avulsa TEXT,
    sms_previo TEXT,
    observacoes TEXT,
    versao INTEGER NOT NULL DEFAULT 1,
    lote_importacao_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(proposta_isize, versao),
    FOREIGN KEY (lote_importacao_id) REFERENCES lotes_importacao(id)
);

CREATE INDEX IF NOT EXISTS idx_propostas_isize_versao ON propostas(proposta_isize, versao DESC);
CREATE INDEX IF NOT EXISTS idx_propostas_cpf ON propostas(cpf);
CREATE INDEX IF NOT EXISTS idx_propostas_created ON propostas(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_propostas_lote ON propostas(lote_importacao_id);
"""

# 2.3 status_venda
SQL_STATUS_VENDA = """
CREATE TABLE IF NOT EXISTS status_venda (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    proposta_isize TEXT NOT NULL,
    status_venda TEXT,
    motivo_rejeicao_cancelamento TEXT,
    flag TEXT,
    auditoria TEXT,
    qualidade TEXT,
    conectada TEXT,
    data_conectada TEXT,
    versao INTEGER NOT NULL DEFAULT 1,
    lote_importacao_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(proposta_isize, versao),
    FOREIGN KEY (lote_importacao_id) REFERENCES lotes_importacao(id)
);

CREATE INDEX IF NOT EXISTS idx_status_venda_proposta ON status_venda(proposta_isize);
CREATE INDEX IF NOT EXISTS idx_status_venda_proposta_versao ON status_venda(proposta_isize, versao DESC);
CREATE INDEX IF NOT EXISTS idx_status_venda_status ON status_venda(status_venda);
CREATE INDEX IF NOT EXISTS idx_status_venda_lote ON status_venda(lote_importacao_id);
"""

# 2.4 portabilidade
SQL_PORTABILIDADE = """
CREATE TABLE IF NOT EXISTS portabilidade (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    proposta_isize TEXT NOT NULL,
    telefone_portabilidade TEXT,
    numero_linha TEXT,
    portabilidade_status TEXT,
    complemento_portabilidade TEXT,
    portabilidade_antecipada TEXT,
    data_marcacao_port_antecipada TEXT,
    quem_marcou_port_antecipada TEXT,
    versao INTEGER NOT NULL DEFAULT 1,
    lote_importacao_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(proposta_isize, versao),
    FOREIGN KEY (lote_importacao_id) REFERENCES lotes_importacao(id)
);

CREATE INDEX IF NOT EXISTS idx_portabilidade_proposta ON portabilidade(proposta_isize);
CREATE INDEX IF NOT EXISTS idx_portabilidade_proposta_versao ON portabilidade(proposta_isize, versao DESC);
CREATE INDEX IF NOT EXISTS idx_portabilidade_telefone ON portabilidade(telefone_portabilidade);
CREATE INDEX IF NOT EXISTS idx_portabilidade_lote ON portabilidade(lote_importacao_id);
"""

# 2.5 portabilidade_tim
SQL_PORTABILIDADE_TIM = """
CREATE TABLE IF NOT EXISTS portabilidade_tim (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    proposta_isize TEXT NOT NULL,
    acesso TEXT,
    acesso_temporario TEXT,
    ddd TEXT,
    data_solicitacao TEXT,
    mes_solicitacao TEXT,
    data_ativacao TEXT,
    mes_ativacao TEXT,
    data_conclusao TEXT,
    sky_contrato TEXT,
    sky_cliente TEXT,
    protocolo TEXT,
    operadora_n1 TEXT,
    tipo_pre_pos_controle TEXT,
    tecnologia TEXT,
    voz_dados TEXT,
    doadora TEXT,
    receptora TEXT,
    tipo TEXT,
    status TEXT,
    tipo_segmento_1 TEXT,
    tipo_segmento_2 TEXT,
    tipo_familia_plano TEXT,
    nivel_plano TEXT,
    canal_n0 TEXT,
    canal_n1 TEXT,
    canal_n2 TEXT,
    canal_n3 TEXT,
    canal_n4 TEXT,
    grupo_economico TEXT,
    custcode TEXT,
    cpf_cnpj TEXT,
    portabilidade TEXT,
    motivo_conflito TEXT,
    motivo_cancelamento TEXT,
    self_portin TEXT,
    canal_portabilidade TEXT,
    tentativas TEXT,
    cart_canal_n1 TEXT,
    cart_canal_n2 TEXT,
    versao INTEGER NOT NULL DEFAULT 1,
    lote_importacao_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(proposta_isize, acesso, versao),
    FOREIGN KEY (lote_importacao_id) REFERENCES lotes_importacao(id)
);

CREATE INDEX IF NOT EXISTS idx_port_tim_proposta ON portabilidade_tim(proposta_isize);
CREATE INDEX IF NOT EXISTS idx_port_tim_proposta_versao ON portabilidade_tim(proposta_isize, acesso, versao DESC);
CREATE INDEX IF NOT EXISTS idx_port_tim_acesso ON portabilidade_tim(acesso);
CREATE INDEX IF NOT EXISTS idx_port_tim_acesso_temp ON portabilidade_tim(acesso_temporario);
CREATE INDEX IF NOT EXISTS idx_port_tim_cpf ON portabilidade_tim(cpf_cnpj);
CREATE INDEX IF NOT EXISTS idx_port_tim_lote ON portabilidade_tim(lote_importacao_id);
"""

# 2.6 logistica
SQL_LOGISTICA = """
CREATE TABLE IF NOT EXISTS logistica (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    proposta_isize TEXT NOT NULL,
    nu_pedido TEXT,
    rastreio TEXT,
    iccid TEXT,
    numero_pedido_marketplace TEXT,
    nota_fiscal TEXT,
    serie_nf TEXT,
    data_emissao_nf TEXT,
    chave_nota_fiscal TEXT,
    valor_nf TEXT,
    valor_frete TEXT,
    id_canal_venda TEXT,
    id_warehouse TEXT,
    id_erp TEXT,
    id_transportadora TEXT,
    transportadora TEXT,
    id_servico TEXT,
    nome_servico TEXT,
    destinatario TEXT,
    documento TEXT,
    email TEXT,
    telefone TEXT,
    cidade TEXT,
    uf TEXT,
    cep TEXT,
    data_insercao TEXT,
    data_primeiro_patch TEXT,
    data_ultimo_patch TEXT,
    data_postagem TEXT,
    previsao_entrega TEXT,
    data_prometida TEXT,
    prazo_dias_corridos TEXT,
    prazo_dias_uteis TEXT,
    prazo_efetivo TEXT,
    status TEXT,
    tentativas_entrega TEXT,
    data_entrega TEXT,
    ultima_ocorrencia TEXT,
    data_ultima_ocorrencia TEXT,
    local_ultima_ocorrencia TEXT,
    cidade_ultima_ocorrencia TEXT,
    estado_ultima_ocorrencia TEXT,
    ultima_ocorrencia_cronologica TEXT,
    motivo_devolucao TEXT,
    retorno_fluxo TEXT,
    protocolo_logistica TEXT,
    motivo_abertura_protocolo TEXT,
    status_protocolo TEXT,
    reversa TEXT,
    codigo_coleta_postagem TEXT,
    cd TEXT,
    dispatch TEXT,
    versao INTEGER NOT NULL DEFAULT 1,
    lote_importacao_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(proposta_isize, nu_pedido, versao),
    FOREIGN KEY (lote_importacao_id) REFERENCES lotes_importacao(id)
);

CREATE INDEX IF NOT EXISTS idx_logistica_proposta ON logistica(proposta_isize);
CREATE INDEX IF NOT EXISTS idx_logistica_proposta_versao ON logistica(proposta_isize, nu_pedido, versao DESC);
CREATE INDEX IF NOT EXISTS idx_logistica_nu_pedido ON logistica(nu_pedido);
CREATE INDEX IF NOT EXISTS idx_logistica_id_erp ON logistica(id_erp);
CREATE INDEX IF NOT EXISTS idx_logistica_rastreio ON logistica(rastreio);
CREATE INDEX IF NOT EXISTS idx_logistica_documento ON logistica(documento);
CREATE INDEX IF NOT EXISTS idx_logistica_status ON logistica(status);
CREATE INDEX IF NOT EXISTS idx_logistica_lote ON logistica(lote_importacao_id);
"""

# 2.7 gross
SQL_GROSS = """
CREATE TABLE IF NOT EXISTS gross (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    proposta_isize TEXT,
    acesso TEXT,
    ddd TEXT,
    custcode TEXT,
    operadora_n1 TEXT,
    classificacao_cr TEXT,
    data_gross TEXT,
    nome_pdv TEXT,
    mes TEXT,
    versao INTEGER NOT NULL DEFAULT 1,
    lote_importacao_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(COALESCE(proposta_isize, ''), COALESCE(acesso, ''), versao),
    FOREIGN KEY (lote_importacao_id) REFERENCES lotes_importacao(id)
);

CREATE INDEX IF NOT EXISTS idx_gross_proposta ON gross(proposta_isize);
CREATE INDEX IF NOT EXISTS idx_gross_proposta_versao ON gross(proposta_isize, acesso, versao DESC);
CREATE INDEX IF NOT EXISTS idx_gross_acesso ON gross(acesso);
CREATE INDEX IF NOT EXISTS idx_gross_lote ON gross(lote_importacao_id);
"""

# 2.8 resultado_gross
SQL_RESULTADO_GROSS = """
CREATE TABLE IF NOT EXISTS resultado_gross (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    proposta_isize TEXT NOT NULL,
    numero_acesso TEXT,
    data_gross TEXT,
    cpf TEXT,
    iccid TEXT,
    data_arquivo TEXT,
    arquivo_origem TEXT,
    resultado TEXT,
    versao INTEGER NOT NULL DEFAULT 1,
    lote_importacao_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(proposta_isize, versao),
    FOREIGN KEY (lote_importacao_id) REFERENCES lotes_importacao(id)
);

CREATE INDEX IF NOT EXISTS idx_res_gross_proposta ON resultado_gross(proposta_isize);
CREATE INDEX IF NOT EXISTS idx_res_gross_proposta_versao ON resultado_gross(proposta_isize, versao DESC);
CREATE INDEX IF NOT EXISTS idx_res_gross_acesso ON resultado_gross(numero_acesso);
CREATE INDEX IF NOT EXISTS idx_res_gross_lote ON resultado_gross(lote_importacao_id);
"""

# 2.9 backoffice
SQL_BACKOFFICE = """
CREATE TABLE IF NOT EXISTS backoffice (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    proposta_isize TEXT NOT NULL,
    pedido TEXT,
    blue_chip TEXT,
    data_venda TEXT,
    tipo_plano TEXT,
    plano_ativado TEXT,
    plano_fidelizado TEXT,
    portabilidade TEXT,
    numero_provisorio TEXT,
    numero_portado TEXT,
    cpf TEXT,
    nome_cliente TEXT,
    endereco TEXT,
    cep TEXT,
    uf TEXT,
    login_vendedor TEXT,
    vendedor TEXT,
    login_bko TEXT,
    bko TEXT,
    data_input_siebel TEXT,
    iccid TEXT,
    data_envio_chip TEXT,
    data_entrega_chip TEXT,
    data_abertura_bp TEXT,
    data_conclusao_bp TEXT,
    status_pedido TEXT,
    detalhe_status TEXT,
    data_atualizacao_status TEXT,
    tempo_tratamento_total TEXT,
    obs_bo TEXT,
    protocolo_conectada TEXT,
    nome_equipe TEXT,
    versao INTEGER NOT NULL DEFAULT 1,
    lote_importacao_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(proposta_isize, versao),
    FOREIGN KEY (lote_importacao_id) REFERENCES lotes_importacao(id)
);

CREATE INDEX IF NOT EXISTS idx_backoffice_proposta ON backoffice(proposta_isize);
CREATE INDEX IF NOT EXISTS idx_backoffice_proposta_versao ON backoffice(proposta_isize, versao DESC);
CREATE INDEX IF NOT EXISTS idx_backoffice_numero_portado ON backoffice(numero_portado);
CREATE INDEX IF NOT EXISTS idx_backoffice_cpf ON backoffice(cpf);
CREATE INDEX IF NOT EXISTS idx_backoffice_status ON backoffice(status_pedido);
CREATE INDEX IF NOT EXISTS idx_backoffice_lote ON backoffice(lote_importacao_id);
"""

# 2.10 consulta_siebel
SQL_CONSULTA_SIEBEL = """
CREATE TABLE IF NOT EXISTS consulta_siebel (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    proposta_isize TEXT NOT NULL,
    cpf TEXT,
    numero_acesso TEXT,
    numero_ordem TEXT,
    codigo_externo TEXT,
    numero_temporario TEXT,
    bilhete_temporario TEXT,
    numero_bilhete TEXT,
    status_bilhete TEXT,
    operadora_doadora TEXT,
    data_portabilidade TEXT,
    motivo_recusa TEXT,
    motivo_cancelamento TEXT,
    ultimo_bilhete TEXT,
    status_ordem TEXT,
    preco_ordem TEXT,
    data_conclusao_ordem TEXT,
    motivo_nao_consultado TEXT,
    motivo_nao_cancelado TEXT,
    motivo_nao_aberto TEXT,
    motivo_nao_reagendado TEXT,
    novo_status_bilhete TEXT,
    nova_data_portabilidade TEXT,
    responsavel_processamento TEXT,
    data_inicial_processamento TEXT,
    data_final_processamento TEXT,
    registro_valido TEXT,
    ajustes_registro TEXT,
    numero_acesso_valido TEXT,
    ajustes_numero_acesso TEXT,
    versao INTEGER NOT NULL DEFAULT 1,
    lote_importacao_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(proposta_isize, numero_acesso, numero_ordem, versao),
    FOREIGN KEY (lote_importacao_id) REFERENCES lotes_importacao(id)
);

CREATE INDEX IF NOT EXISTS idx_siebel_proposta ON consulta_siebel(proposta_isize);
CREATE INDEX IF NOT EXISTS idx_siebel_proposta_versao ON consulta_siebel(proposta_isize, numero_acesso, numero_ordem, versao DESC);
CREATE INDEX IF NOT EXISTS idx_siebel_codigo_externo ON consulta_siebel(codigo_externo);
CREATE INDEX IF NOT EXISTS idx_siebel_numero_acesso ON consulta_siebel(numero_acesso);
CREATE INDEX IF NOT EXISTS idx_siebel_numero_ordem ON consulta_siebel(numero_ordem);
CREATE INDEX IF NOT EXISTS idx_siebel_status_bilhete ON consulta_siebel(status_bilhete);
CREATE INDEX IF NOT EXISTS idx_siebel_matching ON consulta_siebel(status_bilhete, operadora_doadora, motivo_recusa);
CREATE INDEX IF NOT EXISTS idx_siebel_lote ON consulta_siebel(lote_importacao_id);
"""

# 2.11 bluechip
SQL_BLUECHIP = """
CREATE TABLE IF NOT EXISTS bluechip (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    proposta_isize TEXT NOT NULL,
    bluechip_status TEXT,
    bluechip_data_status TEXT,
    resposta_envio_pedido TEXT,
    pedido_bluechip TEXT,
    bluechip_data_enviado TEXT,
    data_maxima_prevista_entrega TEXT,
    status_entrega_prevista TEXT,
    cd_bluechip TEXT,
    remessa_bluechip TEXT,
    qtd_remessas TEXT,
    versao INTEGER NOT NULL DEFAULT 1,
    lote_importacao_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(proposta_isize, versao),
    FOREIGN KEY (lote_importacao_id) REFERENCES lotes_importacao(id)
);

CREATE INDEX IF NOT EXISTS idx_bluechip_proposta ON bluechip(proposta_isize);
CREATE INDEX IF NOT EXISTS idx_bluechip_proposta_versao ON bluechip(proposta_isize, versao DESC);
CREATE INDEX IF NOT EXISTS idx_bluechip_pedido ON bluechip(pedido_bluechip);
CREATE INDEX IF NOT EXISTS idx_bluechip_remessa ON bluechip(remessa_bluechip);
CREATE INDEX IF NOT EXISTS idx_bluechip_lote ON bluechip(lote_importacao_id);
"""

# 2.12 rastreio_entregas
SQL_RASTREIO_ENTREGAS = """
CREATE TABLE IF NOT EXISTS rastreio_entregas (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    proposta_isize TEXT NOT NULL,
    rastreio_correios TEXT,
    rastreio_loggi TEXT,
    data_status_correios TEXT,
    status_correios TEXT,
    data_status_loggi TEXT,
    status_loggi TEXT,
    versao INTEGER NOT NULL DEFAULT 1,
    lote_importacao_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(proposta_isize, versao),
    FOREIGN KEY (lote_importacao_id) REFERENCES lotes_importacao(id)
);

CREATE INDEX IF NOT EXISTS idx_rastreio_proposta ON rastreio_entregas(proposta_isize);
CREATE INDEX IF NOT EXISTS idx_rastreio_proposta_versao ON rastreio_entregas(proposta_isize, versao DESC);
CREATE INDEX IF NOT EXISTS idx_rastreio_correios ON rastreio_entregas(rastreio_correios);
CREATE INDEX IF NOT EXISTS idx_rastreio_loggi ON rastreio_entregas(rastreio_loggi);
CREATE INDEX IF NOT EXISTS idx_rastreio_lote ON rastreio_entregas(lote_importacao_id);
"""

# 2.13 servicos_adicionais
SQL_SERVICOS_ADICIONAIS = """
CREATE TABLE IF NOT EXISTS servicos_adicionais (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    proposta_isize TEXT NOT NULL,
    vivo_internet TEXT,
    vivo_tv TEXT,
    id_play_vivo TEXT,
    versao INTEGER NOT NULL DEFAULT 1,
    lote_importacao_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(proposta_isize, versao),
    FOREIGN KEY (lote_importacao_id) REFERENCES lotes_importacao(id)
);

CREATE INDEX IF NOT EXISTS idx_servicos_proposta ON servicos_adicionais(proposta_isize);
CREATE INDEX IF NOT EXISTS idx_servicos_proposta_versao ON servicos_adicionais(proposta_isize, versao DESC);
CREATE INDEX IF NOT EXISTS idx_servicos_lote ON servicos_adicionais(lote_importacao_id);
"""

# 2.14 robo_processamento
SQL_ROBO_PROCESSAMENTO = """
CREATE TABLE IF NOT EXISTS robo_processamento (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    proposta_isize TEXT NOT NULL,
    robo_inicio_proc TEXT,
    robo_fim_proc TEXT,
    versao INTEGER NOT NULL DEFAULT 1,
    lote_importacao_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(proposta_isize, versao),
    FOREIGN KEY (lote_importacao_id) REFERENCES lotes_importacao(id)
);

CREATE INDEX IF NOT EXISTS idx_robo_proposta ON robo_processamento(proposta_isize);
CREATE INDEX IF NOT EXISTS idx_robo_proposta_versao ON robo_processamento(proposta_isize, versao DESC);
CREATE INDEX IF NOT EXISTS idx_robo_lote ON robo_processamento(lote_importacao_id);
"""

# 2.15 decisoes
SQL_DECISOES = """
CREATE TABLE IF NOT EXISTS decisoes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    proposta_isize TEXT NOT NULL,
    regra_id INTEGER,
    decisao TEXT NOT NULL,
    o_que_aconteceu TEXT,
    acao_a_realizar TEXT,
    tipo_mensagem TEXT,
    template TEXT,
    detalhes TEXT,
    tempo_execucao_ms REAL,
    versao INTEGER NOT NULL DEFAULT 1,
    lote_importacao_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(proposta_isize, regra_id, versao),
    FOREIGN KEY (regra_id) REFERENCES regras_decisao(regra_id),
    FOREIGN KEY (lote_importacao_id) REFERENCES lotes_importacao(id)
);

CREATE INDEX IF NOT EXISTS idx_decisoes_proposta ON decisoes(proposta_isize);
CREATE INDEX IF NOT EXISTS idx_decisoes_proposta_versao ON decisoes(proposta_isize, regra_id, versao DESC);
CREATE INDEX IF NOT EXISTS idx_decisoes_regra ON decisoes(regra_id);
CREATE INDEX IF NOT EXISTS idx_decisoes_created ON decisoes(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_decisoes_lote ON decisoes(lote_importacao_id);
"""

# 2.16 regras_decisao
SQL_REGRAS_DECISAO = """
CREATE TABLE IF NOT EXISTS regras_decisao (
    regra_id INTEGER PRIMARY KEY,
    status_bilhete TEXT,
    operadora_doadora TEXT,
    motivo_recusa TEXT,
    motivo_cancelamento TEXT,
    ultimo_bilhete INTEGER,
    motivo_nao_consultado TEXT,
    novo_status_bilhete TEXT,
    ajustes_numero_acesso TEXT,
    o_que_aconteceu TEXT,
    acao_a_realizar TEXT,
    tipo_mensagem TEXT,
    template TEXT,
    ativo INTEGER DEFAULT 1,
    versao INTEGER NOT NULL DEFAULT 1,
    lote_importacao_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (lote_importacao_id) REFERENCES lotes_importacao(id)
);

CREATE INDEX IF NOT EXISTS idx_regras_status ON regras_decisao(status_bilhete);
CREATE INDEX IF NOT EXISTS idx_regras_ativo ON regras_decisao(ativo);
CREATE INDEX IF NOT EXISTS idx_regras_matching ON regras_decisao(status_bilhete, operadora_doadora, motivo_recusa);
"""

# 2.17 templates_wpp
SQL_TEMPLATES_WPP = """
CREATE TABLE IF NOT EXISTS templates_wpp (
    id INTEGER PRIMARY KEY,
    nome_modelo TEXT NOT NULL UNIQUE,
    categoria TEXT,
    cabecalho_texto TEXT,
    corpo_mensagem TEXT,
    rodape TEXT,
    tipo_botao TEXT,
    botao_texto TEXT,
    botao_url TEXT,
    variaveis TEXT,
    ativo INTEGER DEFAULT 1,
    versao INTEGER NOT NULL DEFAULT 1,
    lote_importacao_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (lote_importacao_id) REFERENCES lotes_importacao(id)
);

CREATE INDEX IF NOT EXISTS idx_templates_nome ON templates_wpp(nome_modelo);
CREATE INDEX IF NOT EXISTS idx_templates_ativo ON templates_wpp(ativo);
"""

# 2.18 tipo_comunicacao_template
SQL_TIPO_COMUNICACAO_TEMPLATE = """
CREATE TABLE IF NOT EXISTS tipo_comunicacao_template (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tipo_comunicacao TEXT NOT NULL UNIQUE,
    tipo_descricao TEXT,
    template_id INTEGER NOT NULL,
    ativo INTEGER DEFAULT 1,
    versao INTEGER NOT NULL DEFAULT 1,
    lote_importacao_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (template_id) REFERENCES templates_wpp(id),
    FOREIGN KEY (lote_importacao_id) REFERENCES lotes_importacao(id)
);

CREATE INDEX IF NOT EXISTS idx_tipo_com_tipo ON tipo_comunicacao_template(tipo_comunicacao);
CREATE INDEX IF NOT EXISTS idx_tipo_com_template ON tipo_comunicacao_template(template_id);
"""

# 2.19 vendas_eva
SQL_VENDAS_EVA = """
CREATE TABLE IF NOT EXISTS vendas_eva (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    numero_acesso TEXT NOT NULL,
    operacao TEXT,
    pedido TEXT,
    id_atendimento TEXT,
    data_hora_gravacao TEXT,
    data_emissao TEXT,
    cod_venda TEXT,
    nome_cliente TEXT,
    cpf TEXT,
    telefone TEXT,
    produto TEXT,
    plano TEXT,
    status_venda TEXT,
    canal TEXT,
    equipe TEXT,
    vendedor TEXT,
    supervisor TEXT,
    dados_json TEXT,
    versao INTEGER NOT NULL DEFAULT 1,
    lote_importacao_id INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(numero_acesso, versao),
    FOREIGN KEY (lote_importacao_id) REFERENCES lotes_importacao(id)
);

CREATE INDEX IF NOT EXISTS idx_vendas_eva_acesso ON vendas_eva(numero_acesso);
CREATE INDEX IF NOT EXISTS idx_vendas_eva_acesso_versao ON vendas_eva(numero_acesso, versao DESC);
CREATE INDEX IF NOT EXISTS idx_vendas_eva_cod ON vendas_eva(cod_venda);
CREATE INDEX IF NOT EXISTS idx_vendas_eva_cpf ON vendas_eva(cpf);
CREATE INDEX IF NOT EXISTS idx_vendas_eva_lote ON vendas_eva(lote_importacao_id);
CREATE INDEX IF NOT EXISTS idx_vendas_eva_created ON vendas_eva(created_at DESC);
"""

# 2.20 retornos_rpa_tim
SQL_RETORNOS_RPA_TIM = """
CREATE TABLE IF NOT EXISTS retornos_rpa_tim (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    numero_acesso TEXT NOT NULL,
    codigo_externo TEXT,
    protocolo TEXT,
    motivo_nao_migrado TEXT,
    data_inicial_processamento TEXT,
    data_final_processamento TEXT,
    data_aprovacao TEXT,
    status_classificado TEXT,
    origem_arquivo TEXT,
    versao INTEGER NOT NULL DEFAULT 1,
    lote_importacao_id INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(numero_acesso, versao),
    FOREIGN KEY (lote_importacao_id) REFERENCES lotes_importacao(id)
);

CREATE INDEX IF NOT EXISTS idx_retornos_rpa_acesso ON retornos_rpa_tim(numero_acesso);
CREATE INDEX IF NOT EXISTS idx_retornos_rpa_acesso_versao ON retornos_rpa_tim(numero_acesso, versao DESC);
CREATE INDEX IF NOT EXISTS idx_retornos_rpa_codigo ON retornos_rpa_tim(codigo_externo);
CREATE INDEX IF NOT EXISTS idx_retornos_rpa_status ON retornos_rpa_tim(status_classificado);
CREATE INDEX IF NOT EXISTS idx_retornos_rpa_lote ON retornos_rpa_tim(lote_importacao_id);
CREATE INDEX IF NOT EXISTS idx_retornos_rpa_created ON retornos_rpa_tim(created_at DESC);
"""

# 2.21 auditoria_vendas_tim
SQL_AUDITORIA_VENDAS_TIM = """
CREATE TABLE IF NOT EXISTS auditoria_vendas_tim (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    numero_acesso TEXT NOT NULL,
    cod_venda TEXT,
    operacao TEXT,
    pedido TEXT,
    id_atendimento TEXT,
    data_hora_gravacao TEXT,
    data_emissao_eva TEXT,
    nome_cliente TEXT,
    cpf TEXT,
    telefone TEXT,
    produto TEXT,
    plano TEXT,
    status_venda_eva TEXT,
    canal TEXT,
    equipe TEXT,
    vendedor TEXT,
    supervisor TEXT,
    codigo_externo TEXT,
    protocolo TEXT,
    motivo_nao_migrado TEXT,
    data_inicial_processamento TEXT,
    data_final_processamento TEXT,
    data_aprovacao TEXT,
    status_classificado TEXT NOT NULL,
    vendas_eva_id INTEGER,
    retornos_rpa_tim_id INTEGER,
    versao INTEGER NOT NULL DEFAULT 1,
    lote_importacao_id INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(numero_acesso, versao),
    FOREIGN KEY (lote_importacao_id) REFERENCES lotes_importacao(id)
);

CREATE INDEX IF NOT EXISTS idx_audit_vendas_acesso ON auditoria_vendas_tim(numero_acesso);
CREATE INDEX IF NOT EXISTS idx_audit_vendas_acesso_versao ON auditoria_vendas_tim(numero_acesso, versao DESC);
CREATE INDEX IF NOT EXISTS idx_audit_vendas_cod ON auditoria_vendas_tim(cod_venda);
CREATE INDEX IF NOT EXISTS idx_audit_vendas_status ON auditoria_vendas_tim(status_classificado);
CREATE INDEX IF NOT EXISTS idx_audit_vendas_cpf ON auditoria_vendas_tim(cpf);
CREATE INDEX IF NOT EXISTS idx_audit_vendas_lote ON auditoria_vendas_tim(lote_importacao_id);
CREATE INDEX IF NOT EXISTS idx_audit_vendas_created ON auditoria_vendas_tim(created_at DESC);
"""

# =============================================================================
# 3. Triggers
# =============================================================================

# 3.1 Triggers BEFORE UPDATE — Bloqueio de UPDATE em tabelas de dados
SQL_TRIGGERS_NO_UPDATE = """
CREATE TRIGGER IF NOT EXISTS trg_clientes_no_update
BEFORE UPDATE ON clientes
BEGIN
    SELECT RAISE(ABORT, 'UPDATE proibido na tabela clientes. Use INSERT com nova versao.');
END;

CREATE TRIGGER IF NOT EXISTS trg_propostas_no_update
BEFORE UPDATE ON propostas
BEGIN
    SELECT RAISE(ABORT, 'UPDATE proibido na tabela propostas. Use INSERT com nova versao.');
END;

CREATE TRIGGER IF NOT EXISTS trg_status_venda_no_update
BEFORE UPDATE ON status_venda
BEGIN
    SELECT RAISE(ABORT, 'UPDATE proibido na tabela status_venda. Use INSERT com nova versao.');
END;

CREATE TRIGGER IF NOT EXISTS trg_portabilidade_no_update
BEFORE UPDATE ON portabilidade
BEGIN
    SELECT RAISE(ABORT, 'UPDATE proibido na tabela portabilidade. Use INSERT com nova versao.');
END;

CREATE TRIGGER IF NOT EXISTS trg_portabilidade_tim_no_update
BEFORE UPDATE ON portabilidade_tim
BEGIN
    SELECT RAISE(ABORT, 'UPDATE proibido na tabela portabilidade_tim. Use INSERT com nova versao.');
END;

CREATE TRIGGER IF NOT EXISTS trg_logistica_no_update
BEFORE UPDATE ON logistica
BEGIN
    SELECT RAISE(ABORT, 'UPDATE proibido na tabela logistica. Use INSERT com nova versao.');
END;

CREATE TRIGGER IF NOT EXISTS trg_gross_no_update
BEFORE UPDATE ON gross
BEGIN
    SELECT RAISE(ABORT, 'UPDATE proibido na tabela gross. Use INSERT com nova versao.');
END;

CREATE TRIGGER IF NOT EXISTS trg_resultado_gross_no_update
BEFORE UPDATE ON resultado_gross
BEGIN
    SELECT RAISE(ABORT, 'UPDATE proibido na tabela resultado_gross. Use INSERT com nova versao.');
END;

CREATE TRIGGER IF NOT EXISTS trg_backoffice_no_update
BEFORE UPDATE ON backoffice
BEGIN
    SELECT RAISE(ABORT, 'UPDATE proibido na tabela backoffice. Use INSERT com nova versao.');
END;

CREATE TRIGGER IF NOT EXISTS trg_consulta_siebel_no_update
BEFORE UPDATE ON consulta_siebel
BEGIN
    SELECT RAISE(ABORT, 'UPDATE proibido na tabela consulta_siebel. Use INSERT com nova versao.');
END;

CREATE TRIGGER IF NOT EXISTS trg_bluechip_no_update
BEFORE UPDATE ON bluechip
BEGIN
    SELECT RAISE(ABORT, 'UPDATE proibido na tabela bluechip. Use INSERT com nova versao.');
END;

CREATE TRIGGER IF NOT EXISTS trg_rastreio_entregas_no_update
BEFORE UPDATE ON rastreio_entregas
BEGIN
    SELECT RAISE(ABORT, 'UPDATE proibido na tabela rastreio_entregas. Use INSERT com nova versao.');
END;

CREATE TRIGGER IF NOT EXISTS trg_servicos_adicionais_no_update
BEFORE UPDATE ON servicos_adicionais
BEGIN
    SELECT RAISE(ABORT, 'UPDATE proibido na tabela servicos_adicionais. Use INSERT com nova versao.');
END;

CREATE TRIGGER IF NOT EXISTS trg_robo_processamento_no_update
BEFORE UPDATE ON robo_processamento
BEGIN
    SELECT RAISE(ABORT, 'UPDATE proibido na tabela robo_processamento. Use INSERT com nova versao.');
END;

CREATE TRIGGER IF NOT EXISTS trg_decisoes_no_update
BEFORE UPDATE ON decisoes
BEGIN
    SELECT RAISE(ABORT, 'UPDATE proibido na tabela decisoes. Use INSERT com nova versao.');
END;

CREATE TRIGGER IF NOT EXISTS trg_vendas_eva_no_update
BEFORE UPDATE ON vendas_eva
BEGIN
    SELECT RAISE(ABORT, 'UPDATE proibido na tabela vendas_eva. Use INSERT com nova versao.');
END;

CREATE TRIGGER IF NOT EXISTS trg_retornos_rpa_tim_no_update
BEFORE UPDATE ON retornos_rpa_tim
BEGIN
    SELECT RAISE(ABORT, 'UPDATE proibido na tabela retornos_rpa_tim. Use INSERT com nova versao.');
END;

CREATE TRIGGER IF NOT EXISTS trg_auditoria_vendas_tim_no_update
BEFORE UPDATE ON auditoria_vendas_tim
BEGIN
    SELECT RAISE(ABORT, 'UPDATE proibido na tabela auditoria_vendas_tim. Use INSERT com nova versao.');
END;
"""

# 3.2 Triggers AFTER INSERT — Auditoria automática
SQL_TRIGGERS_AUDITORIA = """
CREATE TRIGGER IF NOT EXISTS trg_auditoria_clientes
AFTER INSERT ON clientes
BEGIN
    INSERT INTO auditoria (tabela, operacao, registro_id, chave_negocio, versao_registro, lote_importacao_id)
    VALUES ('clientes', 'INSERT', NEW.id, NEW.cpf, NEW.versao, NEW.lote_importacao_id);
END;

CREATE TRIGGER IF NOT EXISTS trg_auditoria_propostas
AFTER INSERT ON propostas
BEGIN
    INSERT INTO auditoria (tabela, operacao, registro_id, chave_negocio, versao_registro, lote_importacao_id)
    VALUES ('propostas', 'INSERT', NEW.id, NEW.proposta_isize, NEW.versao, NEW.lote_importacao_id);
END;

CREATE TRIGGER IF NOT EXISTS trg_auditoria_status_venda
AFTER INSERT ON status_venda
BEGIN
    INSERT INTO auditoria (tabela, operacao, registro_id, chave_negocio, versao_registro, lote_importacao_id)
    VALUES ('status_venda', 'INSERT', NEW.id, NEW.proposta_isize, NEW.versao, NEW.lote_importacao_id);
END;

CREATE TRIGGER IF NOT EXISTS trg_auditoria_portabilidade
AFTER INSERT ON portabilidade
BEGIN
    INSERT INTO auditoria (tabela, operacao, registro_id, chave_negocio, versao_registro, lote_importacao_id)
    VALUES ('portabilidade', 'INSERT', NEW.id, NEW.proposta_isize, NEW.versao, NEW.lote_importacao_id);
END;

CREATE TRIGGER IF NOT EXISTS trg_auditoria_portabilidade_tim
AFTER INSERT ON portabilidade_tim
BEGIN
    INSERT INTO auditoria (tabela, operacao, registro_id, chave_negocio, versao_registro, lote_importacao_id)
    VALUES ('portabilidade_tim', 'INSERT', NEW.id, NEW.proposta_isize, NEW.versao, NEW.lote_importacao_id);
END;

CREATE TRIGGER IF NOT EXISTS trg_auditoria_logistica
AFTER INSERT ON logistica
BEGIN
    INSERT INTO auditoria (tabela, operacao, registro_id, chave_negocio, versao_registro, lote_importacao_id)
    VALUES ('logistica', 'INSERT', NEW.id, NEW.proposta_isize, NEW.versao, NEW.lote_importacao_id);
END;

CREATE TRIGGER IF NOT EXISTS trg_auditoria_gross
AFTER INSERT ON gross
BEGIN
    INSERT INTO auditoria (tabela, operacao, registro_id, chave_negocio, versao_registro, lote_importacao_id)
    VALUES ('gross', 'INSERT', NEW.id, NEW.proposta_isize, NEW.versao, NEW.lote_importacao_id);
END;

CREATE TRIGGER IF NOT EXISTS trg_auditoria_resultado_gross
AFTER INSERT ON resultado_gross
BEGIN
    INSERT INTO auditoria (tabela, operacao, registro_id, chave_negocio, versao_registro, lote_importacao_id)
    VALUES ('resultado_gross', 'INSERT', NEW.id, NEW.proposta_isize, NEW.versao, NEW.lote_importacao_id);
END;

CREATE TRIGGER IF NOT EXISTS trg_auditoria_backoffice
AFTER INSERT ON backoffice
BEGIN
    INSERT INTO auditoria (tabela, operacao, registro_id, chave_negocio, versao_registro, lote_importacao_id)
    VALUES ('backoffice', 'INSERT', NEW.id, NEW.proposta_isize, NEW.versao, NEW.lote_importacao_id);
END;

CREATE TRIGGER IF NOT EXISTS trg_auditoria_consulta_siebel
AFTER INSERT ON consulta_siebel
BEGIN
    INSERT INTO auditoria (tabela, operacao, registro_id, chave_negocio, versao_registro, lote_importacao_id)
    VALUES ('consulta_siebel', 'INSERT', NEW.id, NEW.proposta_isize, NEW.versao, NEW.lote_importacao_id);
END;

CREATE TRIGGER IF NOT EXISTS trg_auditoria_bluechip
AFTER INSERT ON bluechip
BEGIN
    INSERT INTO auditoria (tabela, operacao, registro_id, chave_negocio, versao_registro, lote_importacao_id)
    VALUES ('bluechip', 'INSERT', NEW.id, NEW.proposta_isize, NEW.versao, NEW.lote_importacao_id);
END;

CREATE TRIGGER IF NOT EXISTS trg_auditoria_rastreio_entregas
AFTER INSERT ON rastreio_entregas
BEGIN
    INSERT INTO auditoria (tabela, operacao, registro_id, chave_negocio, versao_registro, lote_importacao_id)
    VALUES ('rastreio_entregas', 'INSERT', NEW.id, NEW.proposta_isize, NEW.versao, NEW.lote_importacao_id);
END;

CREATE TRIGGER IF NOT EXISTS trg_auditoria_servicos_adicionais
AFTER INSERT ON servicos_adicionais
BEGIN
    INSERT INTO auditoria (tabela, operacao, registro_id, chave_negocio, versao_registro, lote_importacao_id)
    VALUES ('servicos_adicionais', 'INSERT', NEW.id, NEW.proposta_isize, NEW.versao, NEW.lote_importacao_id);
END;

CREATE TRIGGER IF NOT EXISTS trg_auditoria_robo_processamento
AFTER INSERT ON robo_processamento
BEGIN
    INSERT INTO auditoria (tabela, operacao, registro_id, chave_negocio, versao_registro, lote_importacao_id)
    VALUES ('robo_processamento', 'INSERT', NEW.id, NEW.proposta_isize, NEW.versao, NEW.lote_importacao_id);
END;

CREATE TRIGGER IF NOT EXISTS trg_auditoria_decisoes
AFTER INSERT ON decisoes
BEGIN
    INSERT INTO auditoria (tabela, operacao, registro_id, chave_negocio, versao_registro, lote_importacao_id)
    VALUES ('decisoes', 'INSERT', NEW.id, NEW.proposta_isize, NEW.versao, NEW.lote_importacao_id);
END;

CREATE TRIGGER IF NOT EXISTS trg_auditoria_vendas_eva
AFTER INSERT ON vendas_eva
BEGIN
    INSERT INTO auditoria (tabela, operacao, registro_id, chave_negocio, versao_registro, lote_importacao_id)
    VALUES ('vendas_eva', 'INSERT', NEW.id, NEW.numero_acesso, NEW.versao, NEW.lote_importacao_id);
END;

CREATE TRIGGER IF NOT EXISTS trg_auditoria_retornos_rpa_tim
AFTER INSERT ON retornos_rpa_tim
BEGIN
    INSERT INTO auditoria (tabela, operacao, registro_id, chave_negocio, versao_registro, lote_importacao_id)
    VALUES ('retornos_rpa_tim', 'INSERT', NEW.id, NEW.numero_acesso, NEW.versao, NEW.lote_importacao_id);
END;

CREATE TRIGGER IF NOT EXISTS trg_auditoria_vendas_tim
AFTER INSERT ON auditoria_vendas_tim
BEGIN
    INSERT INTO auditoria (tabela, operacao, registro_id, chave_negocio, versao_registro, lote_importacao_id)
    VALUES ('auditoria_vendas_tim', 'INSERT', NEW.id, NEW.numero_acesso, NEW.versao, NEW.lote_importacao_id);
END;
"""

# =============================================================================
# 4. Views (Registro Corrente — MAX(versao) por chave de negócio)
# =============================================================================

SQL_VIEWS_CORRENTE = """
CREATE VIEW IF NOT EXISTS vw_clientes_corrente AS
SELECT c.* FROM clientes c
INNER JOIN (
    SELECT cpf, MAX(versao) AS max_versao FROM clientes GROUP BY cpf
) latest ON c.cpf = latest.cpf AND c.versao = latest.max_versao;

CREATE VIEW IF NOT EXISTS vw_propostas_corrente AS
SELECT p.* FROM propostas p
INNER JOIN (
    SELECT proposta_isize, MAX(versao) AS max_versao FROM propostas GROUP BY proposta_isize
) latest ON p.proposta_isize = latest.proposta_isize AND p.versao = latest.max_versao;

CREATE VIEW IF NOT EXISTS vw_status_venda_corrente AS
SELECT sv.* FROM status_venda sv
INNER JOIN (
    SELECT proposta_isize, MAX(versao) AS max_versao FROM status_venda GROUP BY proposta_isize
) latest ON sv.proposta_isize = latest.proposta_isize AND sv.versao = latest.max_versao;

CREATE VIEW IF NOT EXISTS vw_portabilidade_corrente AS
SELECT p.* FROM portabilidade p
INNER JOIN (
    SELECT proposta_isize, MAX(versao) AS max_versao FROM portabilidade GROUP BY proposta_isize
) latest ON p.proposta_isize = latest.proposta_isize AND p.versao = latest.max_versao;

CREATE VIEW IF NOT EXISTS vw_portabilidade_tim_corrente AS
SELECT pt.* FROM portabilidade_tim pt
INNER JOIN (
    SELECT proposta_isize, acesso, MAX(versao) AS max_versao
    FROM portabilidade_tim GROUP BY proposta_isize, acesso
) latest ON pt.proposta_isize = latest.proposta_isize
    AND pt.acesso = latest.acesso
    AND pt.versao = latest.max_versao;

CREATE VIEW IF NOT EXISTS vw_logistica_corrente AS
SELECT l.* FROM logistica l
INNER JOIN (
    SELECT proposta_isize, nu_pedido, MAX(versao) AS max_versao
    FROM logistica GROUP BY proposta_isize, nu_pedido
) latest ON l.proposta_isize = latest.proposta_isize
    AND l.nu_pedido = latest.nu_pedido
    AND l.versao = latest.max_versao;

CREATE VIEW IF NOT EXISTS vw_gross_corrente AS
SELECT g.* FROM gross g
INNER JOIN (
    SELECT COALESCE(proposta_isize, '') AS pi, COALESCE(acesso, '') AS ac, MAX(versao) AS max_versao
    FROM gross GROUP BY COALESCE(proposta_isize, ''), COALESCE(acesso, '')
) latest ON COALESCE(g.proposta_isize, '') = latest.pi
    AND COALESCE(g.acesso, '') = latest.ac
    AND g.versao = latest.max_versao;

CREATE VIEW IF NOT EXISTS vw_resultado_gross_corrente AS
SELECT rg.* FROM resultado_gross rg
INNER JOIN (
    SELECT proposta_isize, MAX(versao) AS max_versao FROM resultado_gross GROUP BY proposta_isize
) latest ON rg.proposta_isize = latest.proposta_isize AND rg.versao = latest.max_versao;

CREATE VIEW IF NOT EXISTS vw_backoffice_corrente AS
SELECT b.* FROM backoffice b
INNER JOIN (
    SELECT proposta_isize, MAX(versao) AS max_versao FROM backoffice GROUP BY proposta_isize
) latest ON b.proposta_isize = latest.proposta_isize AND b.versao = latest.max_versao;

CREATE VIEW IF NOT EXISTS vw_consulta_siebel_corrente AS
SELECT cs.* FROM consulta_siebel cs
INNER JOIN (
    SELECT proposta_isize, numero_acesso, numero_ordem, MAX(versao) AS max_versao
    FROM consulta_siebel GROUP BY proposta_isize, numero_acesso, numero_ordem
) latest ON cs.proposta_isize = latest.proposta_isize
    AND cs.numero_acesso = latest.numero_acesso
    AND cs.numero_ordem = latest.numero_ordem
    AND cs.versao = latest.max_versao;

CREATE VIEW IF NOT EXISTS vw_bluechip_corrente AS
SELECT bc.* FROM bluechip bc
INNER JOIN (
    SELECT proposta_isize, MAX(versao) AS max_versao FROM bluechip GROUP BY proposta_isize
) latest ON bc.proposta_isize = latest.proposta_isize AND bc.versao = latest.max_versao;

CREATE VIEW IF NOT EXISTS vw_rastreio_entregas_corrente AS
SELECT re.* FROM rastreio_entregas re
INNER JOIN (
    SELECT proposta_isize, MAX(versao) AS max_versao FROM rastreio_entregas GROUP BY proposta_isize
) latest ON re.proposta_isize = latest.proposta_isize AND re.versao = latest.max_versao;

CREATE VIEW IF NOT EXISTS vw_servicos_adicionais_corrente AS
SELECT sa.* FROM servicos_adicionais sa
INNER JOIN (
    SELECT proposta_isize, MAX(versao) AS max_versao FROM servicos_adicionais GROUP BY proposta_isize
) latest ON sa.proposta_isize = latest.proposta_isize AND sa.versao = latest.max_versao;

CREATE VIEW IF NOT EXISTS vw_robo_processamento_corrente AS
SELECT rp.* FROM robo_processamento rp
INNER JOIN (
    SELECT proposta_isize, MAX(versao) AS max_versao FROM robo_processamento GROUP BY proposta_isize
) latest ON rp.proposta_isize = latest.proposta_isize AND rp.versao = latest.max_versao;

CREATE VIEW IF NOT EXISTS vw_decisoes_corrente AS
SELECT d.* FROM decisoes d
INNER JOIN (
    SELECT proposta_isize, regra_id, MAX(versao) AS max_versao
    FROM decisoes GROUP BY proposta_isize, regra_id
) latest ON d.proposta_isize = latest.proposta_isize
    AND d.regra_id = latest.regra_id
    AND d.versao = latest.max_versao;

CREATE VIEW IF NOT EXISTS vw_vendas_eva_corrente AS
SELECT ve.* FROM vendas_eva ve
INNER JOIN (
    SELECT numero_acesso, MAX(versao) AS max_versao FROM vendas_eva GROUP BY numero_acesso
) latest ON ve.numero_acesso = latest.numero_acesso AND ve.versao = latest.max_versao;

CREATE VIEW IF NOT EXISTS vw_retornos_rpa_tim_corrente AS
SELECT rr.* FROM retornos_rpa_tim rr
INNER JOIN (
    SELECT numero_acesso, MAX(versao) AS max_versao FROM retornos_rpa_tim GROUP BY numero_acesso
) latest ON rr.numero_acesso = latest.numero_acesso AND rr.versao = latest.max_versao;

CREATE VIEW IF NOT EXISTS vw_auditoria_vendas_tim_corrente AS
SELECT avt.* FROM auditoria_vendas_tim avt
INNER JOIN (
    SELECT numero_acesso, MAX(versao) AS max_versao FROM auditoria_vendas_tim GROUP BY numero_acesso
) latest ON avt.numero_acesso = latest.numero_acesso AND avt.versao = latest.max_versao;
"""

# 4.1 View Unificada Principal
SQL_VIEW_BASE_UNIFICADA = """
CREATE VIEW IF NOT EXISTS vw_base_unificada AS
SELECT
    -- Proposta (chave principal)
    p.proposta_isize,
    p.cpf,
    p.data_venda,
    p.produto,
    p.plano,
    p.forma_pagamento,
    p.nome_equipe,
    p.nome_vendedor,
    -- Cliente
    c.nome_cliente,
    c.data_nascimento,
    c.endereco,
    c.numero AS endereco_numero,
    c.complemento,
    c.bairro,
    c.cidade AS cidade_cliente,
    c.uf AS uf_cliente,
    c.cep AS cep_cliente,
    c.ddd_1,
    c.telefone_1,
    c.email,
    c.score,
    -- Status Venda
    sv.status_venda,
    sv.motivo_rejeicao_cancelamento,
    sv.flag,
    sv.conectada,
    sv.data_conectada,
    -- Portabilidade
    port.telefone_portabilidade,
    port.numero_linha,
    port.portabilidade_status,
    port.complemento_portabilidade,
    -- Bluechip
    bc.bluechip_status,
    bc.pedido_bluechip,
    bc.remessa_bluechip,
    bc.data_maxima_prevista_entrega,
    -- Rastreio
    re.rastreio_correios,
    re.rastreio_loggi,
    re.status_correios,
    re.status_loggi,
    -- Serviços Adicionais
    sa.vivo_internet,
    sa.vivo_tv,
    -- Robô
    rp.robo_inicio_proc,
    rp.robo_fim_proc,
    -- Consulta Siebel (mais recente por proposta)
    cs.numero_acesso,
    cs.numero_ordem,
    cs.codigo_externo,
    cs.status_bilhete,
    cs.operadora_doadora,
    cs.data_portabilidade,
    cs.motivo_recusa,
    cs.motivo_cancelamento,
    cs.status_ordem,
    cs.novo_status_bilhete,
    -- Backoffice
    bo.status_pedido,
    bo.detalhe_status,
    bo.data_atualizacao_status,
    -- Logística (mais recente por proposta)
    lg.nu_pedido,
    lg.rastreio AS rastreio_logistica,
    lg.status AS status_logistica,
    lg.transportadora,
    lg.data_entrega,
    lg.previsao_entrega,
    -- GROSS
    gr.data_gross,
    gr.classificacao_cr,
    -- Resultado GROSS
    rg.resultado AS resultado_gross,
    -- Portabilidade TIM
    pt.acesso AS acesso_tim,
    pt.status AS status_tim,
    pt.data_ativacao AS data_ativacao_tim,
    -- Decisão
    d.regra_id,
    d.decisao,
    d.acao_a_realizar,
    d.tipo_mensagem
FROM vw_propostas_corrente p
LEFT JOIN vw_clientes_corrente c ON p.cpf = c.cpf
LEFT JOIN vw_status_venda_corrente sv ON p.proposta_isize = sv.proposta_isize
LEFT JOIN vw_portabilidade_corrente port ON p.proposta_isize = port.proposta_isize
LEFT JOIN vw_bluechip_corrente bc ON p.proposta_isize = bc.proposta_isize
LEFT JOIN vw_rastreio_entregas_corrente re ON p.proposta_isize = re.proposta_isize
LEFT JOIN vw_servicos_adicionais_corrente sa ON p.proposta_isize = sa.proposta_isize
LEFT JOIN vw_robo_processamento_corrente rp ON p.proposta_isize = rp.proposta_isize
LEFT JOIN vw_consulta_siebel_corrente cs ON p.proposta_isize = cs.proposta_isize
LEFT JOIN vw_backoffice_corrente bo ON p.proposta_isize = bo.proposta_isize
LEFT JOIN vw_logistica_corrente lg ON p.proposta_isize = lg.proposta_isize
LEFT JOIN vw_gross_corrente gr ON p.proposta_isize = gr.proposta_isize
LEFT JOIN vw_resultado_gross_corrente rg ON p.proposta_isize = rg.proposta_isize
LEFT JOIN vw_portabilidade_tim_corrente pt ON p.proposta_isize = pt.proposta_isize
LEFT JOIN vw_decisoes_corrente d ON p.proposta_isize = d.proposta_isize;
"""

# =============================================================================
# Listas ordenadas para criação do schema
# =============================================================================

# Tabelas de controle (ordem de criação respeitando dependências)
TABELAS_CONTROLE = [
    SQL_SCHEMA_VERSAO,
    SQL_LOTES_IMPORTACAO,
    SQL_ARQUIVOS_IMPORTADOS,
    SQL_EXECUCOES_PROCESSAMENTO,
    SQL_AUDITORIA,
    SQL_HISTORICO_BACKUPS,
    SQL_METRICAS_PROCESSAMENTO,
    SQL_REGISTROS_PENDENTES,
    SQL_CACHE_BASE_UNIFICADA,
]

# Tabelas de dados (ordem de criação respeitando dependências FK)
TABELAS_DADOS = [
    SQL_CLIENTES,
    SQL_PROPOSTAS,
    SQL_STATUS_VENDA,
    SQL_PORTABILIDADE,
    SQL_PORTABILIDADE_TIM,
    SQL_LOGISTICA,
    SQL_GROSS,
    SQL_RESULTADO_GROSS,
    SQL_BACKOFFICE,
    SQL_CONSULTA_SIEBEL,
    SQL_BLUECHIP,
    SQL_RASTREIO_ENTREGAS,
    SQL_SERVICOS_ADICIONAIS,
    SQL_ROBO_PROCESSAMENTO,
    SQL_REGRAS_DECISAO,
    SQL_DECISOES,
    SQL_TEMPLATES_WPP,
    SQL_TIPO_COMUNICACAO_TEMPLATE,
    SQL_VENDAS_EVA,
    SQL_RETORNOS_RPA_TIM,
    SQL_AUDITORIA_VENDAS_TIM,
]

# Nomes das 15 tabelas de dados que possuem triggers de bloqueio e auditoria
TABELAS_DADOS_IMUTAVEIS = [
    'clientes',
    'propostas',
    'status_venda',
    'portabilidade',
    'portabilidade_tim',
    'logistica',
    'gross',
    'resultado_gross',
    'backoffice',
    'consulta_siebel',
    'bluechip',
    'rastreio_entregas',
    'servicos_adicionais',
    'robo_processamento',
    'decisoes',
    'vendas_eva',
    'retornos_rpa_tim',
    'auditoria_vendas_tim',
]

# Nomes das tabelas de controle (UPDATE permitido)
TABELAS_CONTROLE_NOMES = [
    'schema_versao',
    'lotes_importacao',
    'arquivos_importados',
    'execucoes_processamento',
    'auditoria',
    'historico_backups',
    'metricas_processamento',
    'registros_pendentes',
    'cache_base_unificada',
]


# =============================================================================
# Função principal de criação do schema
# =============================================================================

def criar_schema(conn):
    """
    Cria todo o schema do banco de dados na conexão fornecida.

    Executa na seguinte ordem:
    1. PRAGMAs de performance
    2. Tabelas de controle (9 tabelas)
    3. Tabelas de dados (18 tabelas)
    4. Triggers BEFORE UPDATE (bloqueio de UPDATE em 15 tabelas)
    5. Triggers AFTER INSERT (auditoria automática em 15 tabelas)
    6. Views de registro corrente (15 views)
    7. View unificada principal
    8. Inserção da versão inicial do schema

    Args:
        conn: Conexão sqlite3 aberta
    """
    cursor = conn.cursor()

    # 1. PRAGMAs de performance
    for pragma in PRAGMAS.strip().split('\n'):
        pragma = pragma.strip()
        if pragma and not pragma.startswith('--'):
            cursor.execute(pragma)

    # 2. Tabelas de controle
    for sql in TABELAS_CONTROLE:
        cursor.executescript(sql)

    # 3. Tabelas de dados
    for sql in TABELAS_DADOS:
        cursor.executescript(sql)

    # 4. Triggers BEFORE UPDATE (bloqueio)
    cursor.executescript(SQL_TRIGGERS_NO_UPDATE)

    # 5. Triggers AFTER INSERT (auditoria)
    cursor.executescript(SQL_TRIGGERS_AUDITORIA)

    # 6. Views de registro corrente
    cursor.executescript(SQL_VIEWS_CORRENTE)

    # 7. View unificada principal
    cursor.executescript(SQL_VIEW_BASE_UNIFICADA)

    # 8. Versão inicial do schema
    cursor.executescript(SQL_SCHEMA_VERSAO_INICIAL)

    conn.commit()


def migrar_lotes_importacao_check(conn):
    """
    Migra a tabela lotes_importacao para incluir os novos tipos de arquivo
    (vendas_eva, retorno_rpa_tim, auditoria_vendas) no CHECK constraint.

    SQLite não suporta ALTER TABLE para modificar CHECK constraints,
    então recria a tabela preservando os dados existentes.
    """
    import sqlite3 as _sqlite3
    cursor = conn.cursor()

    # Verificar se a migração já foi aplicada testando um INSERT com novo tipo
    try:
        cursor.execute(
            "INSERT INTO lotes_importacao (nome_arquivo, tipo_arquivo, hash_sha256) "
            "VALUES ('__migration_test__', 'vendas_eva', '__test__')"
        )
        # Se chegou aqui, o CHECK já aceita o novo tipo — rollback e sair
        conn.rollback()
        cursor.execute(
            "DELETE FROM lotes_importacao WHERE nome_arquivo = '__migration_test__'"
        )
        conn.commit()
        return  # Migração já aplicada
    except _sqlite3.IntegrityError:
        conn.rollback()
        # CHECK constraint antigo — precisa migrar

    # Recriar tabela com CHECK atualizado
    cursor.executescript("""
        PRAGMA foreign_keys = OFF;

        CREATE TABLE IF NOT EXISTS lotes_importacao_new (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nome_arquivo TEXT NOT NULL,
            caminho_origem TEXT,
            tipo_arquivo TEXT NOT NULL CHECK (tipo_arquivo IN (
                'coverte_prop', 'portabilidade_tim', 'gross',
                'relatorio_objetos', 'resultado_gross', 'backoffice',
                'consulta_siebel', 'migracao', 'reprocessamento',
                'vendas_eva', 'retorno_rpa_tim', 'auditoria_vendas'
            )),
            hash_sha256 TEXT NOT NULL,
            qtd_registros INTEGER DEFAULT 0,
            qtd_inseridos INTEGER DEFAULT 0,
            qtd_erros INTEGER DEFAULT 0,
            status TEXT DEFAULT 'em_andamento' CHECK (status IN (
                'em_andamento', 'concluido', 'erro', 'duplicado'
            )),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            finalizado_em TIMESTAMP,
            UNIQUE(hash_sha256)
        );

        INSERT INTO lotes_importacao_new
            SELECT * FROM lotes_importacao;

        DROP TABLE lotes_importacao;

        ALTER TABLE lotes_importacao_new RENAME TO lotes_importacao;

        CREATE INDEX IF NOT EXISTS idx_lotes_hash ON lotes_importacao(hash_sha256);
        CREATE INDEX IF NOT EXISTS idx_lotes_tipo ON lotes_importacao(tipo_arquivo);
        CREATE INDEX IF NOT EXISTS idx_lotes_created ON lotes_importacao(created_at DESC);

        PRAGMA foreign_keys = ON;
    """)
    conn.commit()
