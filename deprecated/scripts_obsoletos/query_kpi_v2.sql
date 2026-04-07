-- =============================================================================
-- Query KPI Funil de Vendas — portabilidade_v2.db
-- Adaptada do schema normalizado v2 (INSERT-only, versionado)
--
-- Usa tabelas físicas com MAX(versao) para registro corrente.
-- Resultados ÚNICOS por proposta_isize, dados mais recentes.
-- Funil completo: Venda → Crivo → Envio → Entrega → GROSS → BP
-- =============================================================================

WITH
-- ============================================================
-- 1. PROPOSTAS CORRENTES (registro mais recente por proposta)
-- ============================================================
prop AS (
    SELECT p.*
    FROM propostas p
    INNER JOIN (
        SELECT proposta_isize, MAX(versao) AS mv
        FROM propostas
        WHERE data_venda >= '2026-01-01'
          AND data_venda < '2027-01-01'
        GROUP BY proposta_isize
    ) pm ON p.proposta_isize = pm.proposta_isize AND p.versao = pm.mv
),

-- ============================================================
-- 2. CLIENTES CORRENTES
-- ============================================================
cli AS (
    SELECT c.*
    FROM clientes c
    INNER JOIN (
        SELECT cpf, MAX(versao) AS mv FROM clientes GROUP BY cpf
    ) cm ON c.cpf = cm.cpf AND c.versao = cm.mv
),

-- ============================================================
-- 3. STATUS VENDA CORRENTE + CRIVO DERIVADO
-- ============================================================
sv AS (
    SELECT
        s.proposta_isize,
        s.status_venda,
        s.motivo_rejeicao_cancelamento,
        s.conectada,
        s.data_conectada,
        CASE
            WHEN UPPER(TRIM(COALESCE(s.status_venda, ''))) IN ('APROVADA', 'APROVADO')
                THEN 'APROVADA'
            WHEN UPPER(TRIM(COALESCE(s.status_venda, ''))) IN ('CANCELADA', 'CANCELADO', 'REJEITADA', 'REJEITADO')
                THEN 'REPROVADA CRIVO'
                    || CASE
                        WHEN NULLIF(TRIM(s.motivo_rejeicao_cancelamento), '') IS NOT NULL
                        THEN ' - ' || TRIM(s.motivo_rejeicao_cancelamento)
                        ELSE ''
                       END
            WHEN UPPER(TRIM(COALESCE(s.status_venda, ''))) IN ('PENDENTE', 'NOVA', 'NOVO')
                THEN 'PENDENTE'
            ELSE ''
        END AS crivo_vendas
    FROM status_venda s
    INNER JOIN (
        SELECT proposta_isize, MAX(versao) AS mv
        FROM status_venda GROUP BY proposta_isize
    ) sm ON s.proposta_isize = sm.proposta_isize AND s.versao = sm.mv
),

-- ============================================================
-- 4. PORTABILIDADE CORRENTE
-- ============================================================
port AS (
    SELECT pt.*
    FROM portabilidade pt
    INNER JOIN (
        SELECT proposta_isize, MAX(versao) AS mv
        FROM portabilidade GROUP BY proposta_isize
    ) pm ON pt.proposta_isize = pm.proposta_isize AND pt.versao = pm.mv
),

-- ============================================================
-- 5. BLUECHIP CORRENTE
-- ============================================================
bc AS (
    SELECT b.*
    FROM bluechip b
    INNER JOIN (
        SELECT proposta_isize, MAX(versao) AS mv
        FROM bluechip GROUP BY proposta_isize
    ) bm ON b.proposta_isize = bm.proposta_isize AND b.versao = bm.mv
),

-- ============================================================
-- 6. RASTREIO ENTREGAS CORRENTE
-- ============================================================
re AS (
    SELECT r.*
    FROM rastreio_entregas r
    INNER JOIN (
        SELECT proposta_isize, MAX(versao) AS mv
        FROM rastreio_entregas GROUP BY proposta_isize
    ) rm ON r.proposta_isize = rm.proposta_isize AND r.versao = rm.mv
),

-- ============================================================
-- 7. LOGÍSTICA CORRENTE (mais recente por proposta — ÚNICO)
-- ============================================================
lg AS (
    SELECT l.*
    FROM logistica l
    INNER JOIN (
        SELECT proposta_isize, MAX(id) AS max_id
        FROM logistica GROUP BY proposta_isize
    ) lm ON l.id = lm.max_id
),

-- ============================================================
-- 8. CONSULTA SIEBEL CORRENTE (mais recente por proposta — ÚNICO)
-- ============================================================
cs AS (
    SELECT s.*
    FROM consulta_siebel s
    INNER JOIN (
        SELECT proposta_isize, MAX(id) AS max_id
        FROM consulta_siebel GROUP BY proposta_isize
    ) sm ON s.id = sm.max_id
),

-- ============================================================
-- 9. PORTABILIDADE TIM CORRENTE (mais recente por proposta — ÚNICO)
-- ============================================================
pt AS (
    SELECT t.*
    FROM portabilidade_tim t
    INNER JOIN (
        SELECT proposta_isize, MAX(id) AS max_id
        FROM portabilidade_tim GROUP BY proposta_isize
    ) tm ON t.id = tm.max_id
),

-- ============================================================
-- 10. DECISÕES CORRENTE (mais recente por proposta — ÚNICO)
-- ============================================================
dec AS (
    SELECT d.*
    FROM decisoes d
    INNER JOIN (
        SELECT proposta_isize, MAX(id) AS max_id
        FROM decisoes GROUP BY proposta_isize
    ) dm ON d.id = dm.max_id
),

-- ============================================================
-- 11. BACKOFFICE CORRENTE
-- ============================================================
bo AS (
    SELECT b.*
    FROM backoffice b
    INNER JOIN (
        SELECT proposta_isize, MAX(versao) AS mv
        FROM backoffice GROUP BY proposta_isize
    ) bm ON b.proposta_isize = bm.proposta_isize AND b.versao = bm.mv
),

-- ============================================================
-- 12. LOGÍSTICA STATS (agregados por proposta)
-- ============================================================
lg_stats AS (
    SELECT
        proposta_isize,
        COUNT(DISTINCT nu_pedido) AS qtd_pedidos,
        COUNT(DISTINCT rastreio) AS qtd_rastreios,
        MAX(data_entrega) AS data_entrega_max,
        MAX(previsao_entrega) AS previsao_entrega_max
    FROM logistica
    GROUP BY proposta_isize
),

-- ============================================================
-- 13. NORMALIZAÇÃO DE STATUS LOGÍSTICA
-- ============================================================
base AS (
    SELECT
        prop.proposta_isize AS id_isize,
        PRINTF('%011d', CAST(
            CASE
                WHEN REPLACE(REPLACE(REPLACE(COALESCE(prop.cpf, ''), '.', ''), '-', ''), '/', '') GLOB '[0-9]*'
                 AND REPLACE(REPLACE(REPLACE(COALESCE(prop.cpf, ''), '.', ''), '-', ''), '/', '') <> ''
                THEN REPLACE(REPLACE(REPLACE(COALESCE(prop.cpf, ''), '.', ''), '-', ''), '/', '')
                ELSE '0'
            END AS INTEGER
        )) AS cpf_11,
        cli.nome_cliente,
        prop.nome_vendedor,
        prop.nome_equipe,
        prop.data_venda,
        DATE(prop.data_venda) AS data_venda_date,
        prop.produto,
        prop.plano,
        cli.email,
        cli.telefone_1,
        cli.cep AS cep_raw,
        UPPER(TRIM(COALESCE(cli.uf, ''))) AS uf_raw,
        cli.endereco,
        cli.cidade,
        cli.score,
        -- Status venda e crivo
        sv.status_venda AS status_funil,
        sv.crivo_vendas,
        sv.conectada,
        sv.data_conectada,
        DATE(sv.data_conectada) AS data_conectada_date,
        -- Portabilidade
        port.telefone_portabilidade AS tel_port_raw,
        port.numero_linha AS num_linha_raw,
        port.portabilidade_status,
        -- Bluechip
        bc.bluechip_status AS bp_status,
        bc.pedido_bluechip AS pedido_bluechip_base,
        bc.remessa_bluechip,
        bc.data_maxima_prevista_entrega AS previsao_entrega_base,
        bc.resposta_envio_pedido,
        bc.qtd_remessas,
        -- Rastreio
        re.rastreio_correios AS rastreio_correios_base,
        re.rastreio_loggi AS rastreio_loggi_base,
        re.status_correios,
        re.status_loggi,
        -- Consulta Siebel
        cs.numero_acesso,
        cs.numero_ordem,
        cs.codigo_externo,
        cs.status_bilhete,
        cs.operadora_doadora,
        cs.data_portabilidade,
        cs.motivo_recusa,
        cs.motivo_cancelamento,
        cs.status_ordem,
        cs.preco_ordem,
        cs.novo_status_bilhete,
        -- Logística
        lg.nu_pedido AS ultimo_pedido,
        lg.id_erp AS id_erp_ult,
        lg.iccid AS iccid_ult,
        lg.rastreio AS rastreio_ult,
        lg.transportadora AS transportadora_ult,
        lg.status AS ro_status_ult,
        lg.ultima_ocorrencia AS ro_ultima_ocorrencia_ult,
        lg.data_entrega,
        lg.previsao_entrega,
        lg.data_insercao AS data_insercao_ult,
        -- Logística extras para tipo_entrega
        CAST(COALESCE(NULLIF(TRIM(lg.prazo_dias_uteis), ''), '-1') AS INTEGER) AS prazo_dias_uteis,
        COALESCE(lg.nome_servico, '') AS nome_servico,
        -- Logística stats
        COALESCE(ls.qtd_pedidos, 0) AS qtd_pedidos,
        COALESCE(ls.qtd_rastreios, 0) AS qtd_rastreios,
        ls.data_entrega_max,
        ls.previsao_entrega_max,
        -- Portabilidade TIM
        pt.acesso AS acesso_tim,
        pt.status AS status_tim,
        pt.doadora,
        pt.data_ativacao AS data_ativacao_tim,
        -- Decisão
        dec.regra_id,
        dec.decisao,
        dec.acao_a_realizar,
        dec.tipo_mensagem,
        -- Backoffice
        bo.status_pedido,
        bo.detalhe_status
    FROM prop
    LEFT JOIN cli ON prop.cpf = cli.cpf
    LEFT JOIN sv ON prop.proposta_isize = sv.proposta_isize
    LEFT JOIN port ON prop.proposta_isize = port.proposta_isize
    LEFT JOIN bc ON prop.proposta_isize = bc.proposta_isize
    LEFT JOIN re ON prop.proposta_isize = re.proposta_isize
    LEFT JOIN lg ON prop.proposta_isize = lg.proposta_isize
    LEFT JOIN cs ON prop.proposta_isize = cs.proposta_isize
    LEFT JOIN pt ON prop.proposta_isize = pt.proposta_isize
    LEFT JOIN dec ON prop.proposta_isize = dec.proposta_isize
    LEFT JOIN bo ON prop.proposta_isize = bo.proposta_isize
    LEFT JOIN lg_stats ls ON prop.proposta_isize = ls.proposta_isize
),

-- ============================================================
-- 14. CÁLCULOS DERIVADOS
-- ============================================================
calc AS (
    SELECT
        b.*,
        -- Tipo de venda
        CASE
            WHEN COALESCE(b.tel_port_raw, '') <> '' AND LENGTH(TRIM(b.tel_port_raw)) >= 10
            THEN 'PORTABILIDADE'
            ELSE 'NOVA LINHA'
        END AS tipo_venda,
        -- Regional SP
        CASE
            WHEN b.uf_raw = 'SP' THEN
                CASE WHEN CAST(SUBSTR(b.cep_raw, 1, 2) AS INTEGER) BETWEEN 1 AND 9 THEN 'SP1' ELSE 'SP2' END
            WHEN b.uf_raw <> '' THEN b.uf_raw
            ELSE ''
        END AS regional_sp,
        -- Email válido
        CASE
            WHEN TRIM(COALESCE(b.email, '')) = '' THEN 'INVALIDO'
            WHEN b.email NOT LIKE '%@%' OR b.email NOT LIKE '%.%' THEN 'INVALIDO'
            WHEN LOWER(b.email) LIKE '%naotem%' OR LOWER(b.email) LIKE '%naopossui%'
              OR LOWER(b.email) LIKE '%sememail%' OR LOWER(b.email) LIKE '%tim@%' THEN 'INVALIDO'
            ELSE 'VALIDO'
        END AS email_valido,
        -- Status resposta envio pedido
        CASE
            WHEN b.resposta_envio_pedido IS NULL OR TRIM(b.resposta_envio_pedido) = '' THEN 'NAO ENVIADO'
            WHEN UPPER(b.resposta_envio_pedido) LIKE '%OK%'
             AND (UPPER(b.resposta_envio_pedido) LIKE '%ENVIADO%'
               OR UPPER(b.resposta_envio_pedido) LIKE '%OK%200%'
               OR UPPER(b.resposta_envio_pedido) LIKE '%OK%201%') THEN 'ENVIADO'
            ELSE 'ERRO'
        END AS status_resposta_envio_pedido,
        -- Normalização status logística
        UPPER(TRIM(COALESCE(b.ro_status_ult, ''))) AS ro_status_n,
        UPPER(TRIM(COALESCE(b.ro_ultima_ocorrencia_ult, ''))) AS ro_ocorr_n,
        -- Tipo entrega: Express (<=2 dias úteis) vs Correios
        -- Fallback 1: prazo_dias_uteis da logística
        -- Fallback 2: transportadora/serviço
        -- Fallback 3: range de CEP (capitais/metrópoles = Express)
        CASE
            -- Nível 1: prazo real em dias úteis
            WHEN b.prazo_dias_uteis >= 0 AND b.prazo_dias_uteis <= 2
                THEN 'EXPRESS'
            WHEN b.prazo_dias_uteis > 2
                THEN 'CORREIOS'
            -- Nível 2: transportadora ou serviço
            WHEN UPPER(COALESCE(b.transportadora_ult, '')) LIKE '%50 MAIS%'
              OR UPPER(COALESCE(b.nome_servico, '')) LIKE '%CHIP EXPRESS%'
              OR UPPER(COALESCE(b.transportadora_ult, '')) LIKE '%CARTEIRO AMIGO%'
              OR UPPER(COALESCE(b.transportadora_ult, '')) LIKE '%LOGGI%'
              OR UPPER(COALESCE(b.transportadora_ult, '')) LIKE '%CSS%'
                THEN 'EXPRESS'
            WHEN UPPER(COALESCE(b.transportadora_ult, '')) LIKE '%ROTALOG%'
              OR UPPER(COALESCE(b.transportadora_ult, '')) LIKE '%CORREIOS%'
                THEN 'CORREIOS'
            -- Nível 3: range de CEP (capitais e regiões metropolitanas = Express)
            WHEN b.cep_raw <> '' AND (
                -- SP Capital + Grande SP
                (CAST(SUBSTR(b.cep_raw, 1, 5) AS INTEGER) BETWEEN 1000 AND 9999)
                OR (CAST(SUBSTR(b.cep_raw, 1, 5) AS INTEGER) BETWEEN 6000 AND 9999)
                -- RJ Capital + Baixada
                OR (CAST(SUBSTR(b.cep_raw, 1, 5) AS INTEGER) BETWEEN 20000 AND 26999)
                -- BH + Região Metropolitana
                OR (CAST(SUBSTR(b.cep_raw, 1, 5) AS INTEGER) BETWEEN 30000 AND 34999)
                -- Brasília
                OR (CAST(SUBSTR(b.cep_raw, 1, 5) AS INTEGER) BETWEEN 70000 AND 72999)
                -- Curitiba
                OR (CAST(SUBSTR(b.cep_raw, 1, 5) AS INTEGER) BETWEEN 80000 AND 83999)
                -- Porto Alegre
                OR (CAST(SUBSTR(b.cep_raw, 1, 5) AS INTEGER) BETWEEN 90000 AND 94999)
                -- Salvador
                OR (CAST(SUBSTR(b.cep_raw, 1, 5) AS INTEGER) BETWEEN 40000 AND 42999)
                -- Recife
                OR (CAST(SUBSTR(b.cep_raw, 1, 5) AS INTEGER) BETWEEN 50000 AND 54999)
                -- Fortaleza
                OR (CAST(SUBSTR(b.cep_raw, 1, 5) AS INTEGER) BETWEEN 60000 AND 63999)
                -- Goiânia
                OR (CAST(SUBSTR(b.cep_raw, 1, 5) AS INTEGER) BETWEEN 74000 AND 74999)
                -- Campinas
                OR (CAST(SUBSTR(b.cep_raw, 1, 5) AS INTEGER) BETWEEN 13000 AND 13139)
                -- Vitória
                OR (CAST(SUBSTR(b.cep_raw, 1, 5) AS INTEGER) BETWEEN 29000 AND 29099)
                -- Manaus
                OR (CAST(SUBSTR(b.cep_raw, 1, 5) AS INTEGER) BETWEEN 69000 AND 69099)
                -- Belém
                OR (CAST(SUBSTR(b.cep_raw, 1, 5) AS INTEGER) BETWEEN 66000 AND 66999)
            ) THEN 'EXPRESS'
            WHEN b.cep_raw <> ''
                THEN 'CORREIOS'
            ELSE 'SEM INFO'
        END AS tipo_entrega
    FROM base b
),

-- ============================================================
-- 15. STATUS ENTREGA DERIVADO
-- ============================================================
status_calc AS (
    SELECT
        c.*,
        -- Status entrega parametrizado
        CASE
            WHEN c.ro_status_n = 'AGUARDANDO RETIRADA' THEN 'AG RETIRADA CORREIOS'
            WHEN c.ro_ocorr_n LIKE '%PEDIDO ENTREGUE%' OR c.ro_ocorr_n = 'ENTREGUE'
              OR c.ro_status_n LIKE 'ENTREGUE%' THEN 'SIM'
            WHEN c.ro_status_n IN ('INSERIDO NO BANCO DE DADOS', 'EM DEVOLUCAO AO REMETENTE',
                'DISTRIBUIDO AO REMETENTE', 'ENTREGA CANCELADA') THEN 'QUEBRA'
            WHEN c.ro_status_n IN ('EM TRANSITO', 'EM TRANSITO ') THEN 'EM ROTA'
            WHEN c.ro_status_n = 'EM ATRASO' THEN
                CASE WHEN c.ro_ocorr_n IN ('DESTINATARIO DESCONHECIDO', 'ENDERECO INCORRETO')
                     THEN 'QUEBRA' ELSE 'EM ROTA' END
            WHEN c.ro_ocorr_n LIKE '%ENTREG%' THEN 'SIM'
            WHEN c.ro_ocorr_n LIKE '%CANCEL%' OR c.ro_ocorr_n LIKE '%DEVOLV%'
              OR c.ro_ocorr_n LIKE '%EXTRAVI%' THEN 'QUEBRA'
            ELSE 'EM ROTA'
        END AS status_entrega_param,
        -- Em rota SIM/NÃO
        CASE
            WHEN c.ro_status_n IN ('EM TRANSITO', 'EM TRANSITO ', 'EM ATRASO', 'AGUARDANDO RETIRADA')
            THEN 'SIM'
            WHEN c.ro_ocorr_n LIKE '%CANCEL%' OR c.ro_ocorr_n LIKE '%DEVOLV%'
              OR c.ro_ocorr_n LIKE '%EXTRAVI%' THEN 'NÃO'
            WHEN c.ro_ocorr_n LIKE '%ENTREG%' THEN 'NÃO'
            ELSE 'SIM'
        END AS em_rota_sim_nao
    FROM calc c
),

-- ============================================================
-- 16. GROSS E BP FLAGS (via consulta_siebel + portabilidade_tim)
-- Regra BP: exclusivo para PORTABILIDADE
--   - status_bilhete IN (Portado, Falha Parcial, Antigo) → SIM
--   - status_bilhete IN (Portabilidade Pendente, Pendente Portabilidade) → PENDENTE
--   - Caso contrário → NÃO
-- ============================================================
gross_bp AS (
    SELECT
        proposta_isize,
        -- GROSS flags (consulta_siebel)
        MAX(CASE WHEN UPPER(TRIM(status_ordem)) IN ('CONCLUÍDO', 'CONCLUIDO',
            'PORTABILIDADE PENDENTE', 'PENDENTE PORTABILIDADE') THEN 1 ELSE 0 END) AS gross_flag,
        MIN(CASE WHEN UPPER(TRIM(status_ordem)) IN ('CONCLUÍDO', 'CONCLUIDO',
            'PORTABILIDADE PENDENTE', 'PENDENTE PORTABILIDADE')
            THEN DATE(COALESCE(data_conclusao_ordem, created_at)) END) AS data_gross,
        -- BP flags (consulta_siebel: Portado, Falha Parcial)
        MAX(CASE WHEN UPPER(TRIM(status_bilhete)) IN ('PORTADO', 'FALHA PARCIAL')
            THEN 1 ELSE 0 END) AS bp_siebel_flag,
        MIN(CASE WHEN UPPER(TRIM(status_bilhete)) IN ('PORTADO', 'FALHA PARCIAL')
            THEN DATE(COALESCE(data_portabilidade, created_at)) END) AS data_bp_siebel,
        -- BP pendente (consulta_siebel)
        MAX(CASE WHEN UPPER(TRIM(status_bilhete)) IN ('PORTABILIDADE PENDENTE', 'PENDENTE PORTABILIDADE')
            THEN 1 ELSE 0 END) AS bp_pendente_siebel_flag,
        -- Não aprovisionamento
        MAX(CASE WHEN UPPER(TRIM(status_ordem)) IN ('EM APROVISIONAMENTO', 'ERRO NO APROVISIONAMENTO')
            THEN 1 ELSE 0 END) AS nao_aprovisionamento_flag
    FROM consulta_siebel
    GROUP BY proposta_isize
),

-- BP flags via portabilidade_tim (ATIVA, FALHA PARCIAL, ANTIGO = fechado)
bp_tim AS (
    SELECT
        proposta_isize,
        MAX(CASE WHEN UPPER(TRIM(status)) IN ('ATIVA', 'FALHA PARCIAL', 'ANTIGO')
            THEN 1 ELSE 0 END) AS bp_tim_flag,
        MIN(CASE WHEN UPPER(TRIM(status)) IN ('ATIVA', 'FALHA PARCIAL', 'ANTIGO')
            THEN DATE(COALESCE(data_conclusao, data_ativacao, created_at)) END) AS data_bp_tim,
        MAX(CASE WHEN UPPER(TRIM(status)) IN ('PENDENTE', 'CONFIRMADO PELA DOADORA', 'REAGENDADO')
            THEN 1 ELSE 0 END) AS bp_pendente_tim_flag
    FROM portabilidade_tim
    GROUP BY proposta_isize
)

-- ============================================================
-- RESULTADO FINAL — UNIQUE por proposta_isize
-- ============================================================
SELECT
    s.id_isize                                              AS "ID_ISIZE",
    s.cpf_11                                               AS "CPF",
    s.nome_cliente                                         AS "Nome_Cliente",
    s.regional_sp                                          AS "Regional_SP",
    COALESCE(s.crivo_vendas, '')                           AS "Crivo_Vendas",
    s.tipo_venda                                           AS "Tipo_Venda",
    -- Numero_OS: priorizar formato 1-XXXXXXXXXXXXX
    -- Fallback: id_erp (logística) → numero_ordem (siebel) → vazio
    CASE
        WHEN s.numero_ordem LIKE '1-1%' AND LENGTH(s.numero_ordem) BETWEEN 14 AND 17
            THEN s.numero_ordem
        WHEN s.id_erp_ult LIKE '1-1%' AND LENGTH(s.id_erp_ult) BETWEEN 14 AND 17
            THEN s.id_erp_ult
        WHEN s.numero_ordem LIKE '1-%' AND LENGTH(s.numero_ordem) >= 14
            THEN s.numero_ordem
        ELSE ''
    END                                                    AS "Numero_OS",
    s.tel_port_raw                                         AS "Numero_Portado",
    s.num_linha_raw                                        AS "Numero_NovaLinha_ou_Provisorio",
    s.nome_vendedor                                        AS "Nome_Vendedor",
    s.nome_equipe                                          AS "Nome_Equipe",
    STRFTIME('%d/%m/%Y', s.data_venda_date)               AS "Data_Venda",
    STRFTIME('%m/%Y', s.data_venda_date)                  AS "Mes_Ano_Venda",
    s.email                                                AS "Email",
    s.email_valido                                         AS "Email_Valido",
    s.tipo_entrega                                         AS "Tipo_Entrega",
    COALESCE(s.ultimo_pedido, s.pedido_bluechip_base, '') AS "Ultimo_Numero_Pedido",
    CASE
        WHEN s.status_entrega_param = 'SIM' AND COALESCE(s.iccid_ult, '') = ''
        THEN 'FALTANDO ICCID'
        ELSE COALESCE(s.iccid_ult, '')
    END                                                    AS "ICCID",
    COALESCE(s.rastreio_ult, s.rastreio_correios_base, s.rastreio_loggi_base, '') AS "Rastreio",
    COALESCE(s.transportadora_ult, '')                     AS "Transportadora",
    STRFTIME('%d/%m/%Y', COALESCE(s.data_conectada_date, DATE(s.data_insercao_ult), s.data_venda_date))
                                                           AS "Data_Conectada",
    STRFTIME('%m/%Y', COALESCE(s.data_conectada_date, DATE(s.data_insercao_ult), s.data_venda_date))
                                                           AS "Mes_Ano_Conexao",
    s.status_resposta_envio_pedido                         AS "Status_Resposta_Envio_Pedido",
    s.resposta_envio_pedido                                AS "Resposta_Envio_Pedido_Original",
    s.qtd_pedidos                                          AS "Tentativas_QTD_Remessas",
    s.qtd_rastreios                                        AS "Tentativas_QTD_OS",
    COALESCE(s.qtd_pedidos, 0) + COALESCE(s.qtd_rastreios, 0) AS "Total_Tratamento_Soma",
    CASE
        WHEN COALESCE(s.qtd_pedidos, 0) >= 2 OR COALESCE(s.qtd_rastreios, 0) >= 2
        THEN 'SIM' ELSE 'NÃO'
    END                                                    AS "Tratado_Bko",
    CASE
        WHEN (COALESCE(s.qtd_pedidos, 0) + COALESCE(s.qtd_rastreios, 0)) >= 1
        THEN (COALESCE(s.qtd_pedidos, 0) + COALESCE(s.qtd_rastreios, 0)) - 1
        ELSE 0
    END                                                    AS "Tentativas_BKO_Qtd",
    s.bp_status                                            AS "BP_Status",
    s.ro_status_ult                                        AS "RO_Status",
    s.ro_ultima_ocorrencia_ult                             AS "RO_Ultima_Ocorrencia",
    s.status_entrega_param                                 AS "Status_Entrega",
    CASE WHEN s.status_entrega_param = 'SIM' THEN 'SIM' ELSE 'NÃO' END
                                                           AS "Entregue_SIM_NAO",
    s.em_rota_sim_nao                                      AS "Em_Rota_SIM_NAO",
    -- Em_Rota_Dentro_Previsao: previsão vs hoje (para quem ainda não entregou)
    CASE
        WHEN s.status_entrega_param = 'SIM' THEN NULL  -- já entregou, não se aplica
        WHEN DATE(COALESCE(s.previsao_entrega_max, s.previsao_entrega)) IS NOT NULL
        THEN CASE
            WHEN DATE('now', 'localtime') <= DATE(COALESCE(s.previsao_entrega_max, s.previsao_entrega))
            THEN 'SIM' ELSE 'NÃO'
            END
        ELSE NULL
    END                                                    AS "Em_Rota_Dentro_Previsao",
    CASE WHEN s.status_entrega_param = 'SIM'
         THEN STRFTIME('%d/%m/%Y', DATE(s.data_entrega_max))
    END                                                    AS "Data_Entrega",
    STRFTIME('%d/%m/%Y', DATE(COALESCE(s.previsao_entrega_max, s.previsao_entrega, s.previsao_entrega_base)))
                                                           AS "Previsao_Entrega",
    -- Dentro_Prazo: aplica para TODOS (entregues = entrega vs previsão, em rota = hoje vs previsão)
    CASE
        WHEN DATE(COALESCE(s.previsao_entrega_max, s.previsao_entrega, s.previsao_entrega_base)) IS NULL
            THEN NULL
        WHEN s.status_entrega_param = 'SIM' AND s.data_entrega_max IS NOT NULL
        THEN CASE
            WHEN DATE(s.data_entrega_max) <= DATE(COALESCE(s.previsao_entrega_max, s.previsao_entrega, s.previsao_entrega_base))
            THEN 'SIM' ELSE 'NÃO' END
        ELSE CASE
            WHEN DATE('now', 'localtime') <= DATE(COALESCE(s.previsao_entrega_max, s.previsao_entrega, s.previsao_entrega_base))
            THEN 'SIM' ELSE 'NÃO' END
    END                                                    AS "Dentro_Prazo",
    s.status_funil                                         AS "Status_Funil_Proposta",
    -- GROSS
    CASE
        WHEN gb.nao_aprovisionamento_flag = 1 THEN 'Não Aprovisionamento'
        WHEN gb.gross_flag = 1 THEN 'Sim'
        ELSE 'Não'
    END                                                    AS "GROSS_Efetivo",
    CASE
        WHEN gb.nao_aprovisionamento_flag = 1 THEN NULL
        WHEN gb.gross_flag = 1 THEN STRFTIME('%d/%m/%Y', gb.data_gross)
        ELSE NULL
    END                                                    AS "Data_GROSS",
    -- BP Fechado (exclusivo PORTABILIDADE)
    -- Fontes: consulta_siebel (Portado, Falha Parcial) + portabilidade_tim (ATIVA, FALHA PARCIAL, ANTIGO)
    CASE
        WHEN s.tipo_venda = 'NOVA LINHA' THEN 'N/A'
        WHEN COALESCE(gb.bp_siebel_flag, 0) = 1 OR COALESCE(bt.bp_tim_flag, 0) = 1 THEN 'Sim'
        WHEN COALESCE(gb.bp_pendente_siebel_flag, 0) = 1 OR COALESCE(bt.bp_pendente_tim_flag, 0) = 1 THEN 'Pendente'
        ELSE 'Não'
    END                                                    AS "BP_Fechado",
    CASE
        WHEN s.tipo_venda = 'NOVA LINHA' THEN NULL
        WHEN COALESCE(gb.bp_siebel_flag, 0) = 1 THEN STRFTIME('%d/%m/%Y', gb.data_bp_siebel)
        WHEN COALESCE(bt.bp_tim_flag, 0) = 1 THEN STRFTIME('%d/%m/%Y', bt.data_bp_tim)
        ELSE NULL
    END                                                    AS "Data_BP_Fechado",
    -- Origem da evidência BP
    CASE
        WHEN s.tipo_venda = 'NOVA LINHA' THEN 'N/A'
        WHEN COALESCE(gb.bp_siebel_flag, 0) = 1 AND COALESCE(bt.bp_tim_flag, 0) = 1
            THEN 'Siebel + TIM'
        WHEN COALESCE(gb.bp_siebel_flag, 0) = 1 THEN 'Consulta Siebel'
        WHEN COALESCE(bt.bp_tim_flag, 0) = 1 THEN 'Portabilidade TIM'
        WHEN COALESCE(gb.bp_pendente_siebel_flag, 0) = 1 THEN 'Siebel (Pendente)'
        WHEN COALESCE(bt.bp_pendente_tim_flag, 0) = 1 THEN 'TIM (Pendente)'
        ELSE 'Sem evidência'
    END                                                    AS "Origem_BP",
    -- Decisão
    s.regra_id                                             AS "Regra_ID",
    s.decisao                                              AS "Decisao",
    s.acao_a_realizar                                      AS "Acao_Realizar",
    -- Backoffice
    s.status_pedido                                        AS "Status_Pedido_BKO",
    s.detalhe_status                                       AS "Detalhe_Status_BKO",
    -- TIM
    s.acesso_tim                                           AS "Acesso_TIM",
    s.status_tim                                           AS "Status_TIM",
    s.doadora                                              AS "Doadora_TIM"

FROM status_calc s
LEFT JOIN gross_bp gb ON gb.proposta_isize = s.id_isize
LEFT JOIN bp_tim bt ON bt.proposta_isize = s.id_isize
ORDER BY s.data_venda_date DESC, s.id_isize;
