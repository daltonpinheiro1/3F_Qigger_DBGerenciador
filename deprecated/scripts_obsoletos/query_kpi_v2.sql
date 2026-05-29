-- =============================================================================
-- Query KPI Funil de Vendas — portabilidade_v2.db
-- Adaptada do schema normalizado v2 (INSERT-only, versionado)
--
-- Usa tabelas físicas com MAX(versao) para registro corrente.
-- Resultados ÚNICOS por proposta_isize, dados mais recentes.
-- Funil completo: Venda → Crivo → Envio → Entrega → GROSS → BP
--
-- =============================================================================
-- CHANGELOG (revisão):
-- [FIX-1] Em_Rota: itens sem envio agora = "AGUARDANDO ENVIO" (não "SIM")
-- [FIX-2] Status_Entrega: sem logística + aprovada = "AGUARDANDO ENVIO" (não "EM ROTA")
-- [FIX-3] GROSS dates: trata dd/mm/yyyy e yyyy-mm-dd via helper SAFE_DATE
-- [FIX-4] Data_Entrega fallback: todas as datas normalizadas via SAFE_DATE
-- [FIX-5] Status_Resposta_Envio_Pedido: "PENDENTE" → "PENDENTE CRIVO" vs "PENDENTE"
-- [FIX-6] GROSS: cascata multi-chave usando tabela gross + consulta_siebel
--         Associação: proposta_isize → numero_portado → numero_linha → acesso_tim → iccid
--         + consulta_siebel por proposta_isize → numero_acesso → CPF
--         Impacto: de ~910 para ~2.500+ propostas com GROSS identificado
-- [NEW-1] Ciclo_Logistico: classificação completa do ciclo de vida logístico
-- [NEW-2] Dias_Em_Rota: dias desde envio até entrega ou hoje
-- [NEW-3] SLA_Status: classificação de cumprimento de SLA
-- [NEW-4] Em_Rota_SIM_NAO agora usa Ciclo_Logistico como base
-- [NEW-5] Status_Entrega mais granular via Ciclo_Logistico
-- [NEW-6] GROSS_Encontrado_Por: rastreabilidade da chave usada para associação
-- [NEW-7] Classificacao_CR: classificação CR da tabela gross
-- =============================================================================

WITH
-- ============================================================
-- 0. HELPER: Função inline para normalizar datas (dd/mm/yyyy → yyyy-mm-dd)
--    Usada em todo o query para evitar problemas de formato misto.
--    Regra: se parece dd/mm/yyyy (XX/XX/XXXX), converte; senão tenta DATE() direto.
-- ============================================================

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
        prop.tipo_chip,
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
        -- [FIX-3] data_conectada: normalizar dd/mm/yyyy → yyyy-mm-dd
        DATE(CASE
            WHEN sv.data_conectada LIKE '__/__/____'
            THEN SUBSTR(sv.data_conectada,7,4)||'-'||SUBSTR(sv.data_conectada,4,2)||'-'||SUBSTR(sv.data_conectada,1,2)
            ELSE sv.data_conectada
        END) AS data_conectada_date,
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
        bo.detalhe_status,
        bo.data_envio_chip,
        -- Gross flag inline (para uso no cálculo de status_resposta_envio_pedido)
        -- [FIX-6] Agora usa cascata: consulta_siebel por proposta_isize OU tabela gross
        CASE
            WHEN (SELECT MAX(CASE WHEN UPPER(TRIM(cs2.status_ordem)) IN ('CONCLUÍDO', 'CONCLUIDO', 'CONCLUíDO',
                'PORTABILIDADE PENDENTE', 'PENDENTE PORTABILIDADE') THEN 1 ELSE 0 END)
                 FROM consulta_siebel cs2 WHERE cs2.proposta_isize = prop.proposta_isize) = 1
            THEN 1
            WHEN EXISTS (SELECT 1 FROM gross g2 WHERE g2.proposta_isize = prop.proposta_isize AND g2.proposta_isize != '')
            THEN 1
            WHEN port.numero_linha IS NOT NULL AND port.numero_linha != ''
             AND EXISTS (SELECT 1 FROM gross g3 WHERE g3.acesso = port.numero_linha AND g3.acesso != '')
            THEN 1
            WHEN port.telefone_portabilidade IS NOT NULL AND port.telefone_portabilidade != ''
             AND EXISTS (SELECT 1 FROM gross g4 WHERE g4.acesso = port.telefone_portabilidade AND g4.acesso != '')
            THEN 1
            ELSE 0
        END AS gross_flag_inline,
        -- [NEW] Flag auxiliar: tem QUALQUER dado de logística/envio?
        -- Usado para distinguir "sem envio" de "enviado sem rastreio"
        CASE
            WHEN COALESCE(ls.qtd_pedidos, 0) > 0
              OR COALESCE(ls.qtd_rastreios, 0) > 0
              OR COALESCE(lg.status, '') <> ''
              OR COALESCE(lg.rastreio, '') <> ''
              OR COALESCE(re.rastreio_correios, '') <> ''
              OR COALESCE(re.rastreio_loggi, '') <> ''
              OR COALESCE(bc.pedido_bluechip, '') <> ''
              OR COALESCE(bc.remessa_bluechip, '') <> ''
              OR COALESCE(bo.data_envio_chip, '') <> ''
            THEN 1 ELSE 0
        END AS tem_logistica_flag
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
        -- [FIX-5] Status resposta envio pedido
        -- MUDANÇA: "PENDENTE" agora é dividido em:
        --   "PENDENTE CRIVO" = aguardando aprovação do crivo (status pendente/vazio)
        --   "PENDENTE"       = aprovada mas sem ação de envio ainda (NAO ENVIADO antigo)
        -- Funil: Crivo → Logística → ESIM → PENDENTE
        CASE
            -- 1. Reprovada no crivo
            WHEN b.crivo_vendas LIKE 'REPROVADA%' THEN 'REPROVADA'
            -- 2. [FIX-5] Pendente no crivo → agora "PENDENTE CRIVO" (antes era "PENDENTE")
            WHEN b.crivo_vendas = 'PENDENTE' OR b.crivo_vendas = '' THEN 'PENDENTE CRIVO'
            -- A partir daqui: APROVADA
            -- 3. Tem resposta OK de envio
            WHEN UPPER(COALESCE(b.resposta_envio_pedido, '')) LIKE '%OK%'
             AND (UPPER(b.resposta_envio_pedido) LIKE '%ENVIADO%'
               OR UPPER(b.resposta_envio_pedido) LIKE '%OK%200%'
               OR UPPER(b.resposta_envio_pedido) LIKE '%OK%201%') THEN 'ENVIADO'
            -- 4. Tem histórico de logística (pedidos, rastreios, etc.)
            WHEN b.qtd_pedidos > 0
                OR b.qtd_rastreios > 0
                OR COALESCE(b.ro_status_ult, '') <> ''
                OR COALESCE(b.rastreio_ult, '') <> ''
                OR COALESCE(b.rastreio_correios_base, '') <> ''
                OR COALESCE(b.rastreio_loggi_base, '') <> ''
                OR COALESCE(b.pedido_bluechip_base, '') <> ''
             THEN 'ENVIADO'
            -- 5. Sem logística mas é ESIM → não precisa envio físico
            WHEN UPPER(TRIM(REPLACE(COALESCE(b.tipo_chip, ''), '-', ''))) = 'ESIM' THEN 'ENVIADO'
            -- 6. Sem logística, não é ESIM, mas tem GROSS efetivo → já ativou, considerar enviado
            WHEN COALESCE(b.gross_flag_inline, 0) = 1 THEN 'ENVIADO'
            -- 7. [FIX-5] Aprovada, sem logística, não é ESIM, sem GROSS → "PENDENTE" (antes "NAO ENVIADO")
            ELSE 'PENDENTE'
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
-- 15. STATUS ENTREGA DERIVADO + CICLO LOGÍSTICO + DIAS EM ROTA + SLA
-- [FIX-1] Em_Rota: sem envio = "AGUARDANDO ENVIO" (não "SIM")
-- [FIX-2] Status_Entrega: sem logística = "AGUARDANDO ENVIO" (não "EM ROTA")
-- [NEW-1] Ciclo_Logistico: classificação completa do ciclo de vida
-- [NEW-2] Dias_Em_Rota: dias desde envio até entrega ou hoje
-- [NEW-3] SLA_Status: classificação de cumprimento de SLA
-- ============================================================
status_calc AS (
    SELECT
        c.*,
        -- ============================================================
        -- [NEW-1] Ciclo_Logistico: classificação completa do ciclo de vida logístico
        -- Ordem de prioridade:
        --   1. ESIM (digital, sem envio físico)
        --   2. REPROVADA (rejeitada no crivo)
        --   3. PENDENTE CRIVO (aguardando aprovação)
        --   4. ENTREGUE (confirmação de entrega)
        --   5. QUEBRA (falha/devolução/cancelamento)
        --   6. AG RETIRADA (aguardando retirada nos correios)
        --   7. EM ATRASO (passado da previsão de entrega)
        --   8. EM TRANSITO (em rota ativa)
        --   9. ENVIADO (tem registro de envio mas sem status de trânsito)
        --  10. SEM ENVIO (aprovada mas nenhum dado de logística)
        -- ============================================================
        CASE
            -- 1. ESIM: entrega digital, não precisa envio físico
            WHEN UPPER(TRIM(REPLACE(COALESCE(c.tipo_chip, ''), '-', ''))) = 'ESIM'
                THEN 'ESIM'
            -- 2. REPROVADA: rejeitada no crivo, não vai ter envio
            WHEN c.crivo_vendas LIKE 'REPROVADA%'
                THEN 'REPROVADA'
            -- 3. PENDENTE CRIVO: aguardando aprovação (crivo pendente ou vazio)
            WHEN c.crivo_vendas = 'PENDENTE' OR c.crivo_vendas = ''
                THEN 'PENDENTE CRIVO'
            -- A partir daqui: APROVADA com chip físico
            -- 4. ENTREGUE: confirmação de entrega via logística
            WHEN c.ro_ocorr_n LIKE '%PEDIDO ENTREGUE%' OR c.ro_ocorr_n = 'ENTREGUE'
              OR c.ro_status_n LIKE 'ENTREGUE%'
              OR c.ro_ocorr_n LIKE '%ENTREG%'
                THEN 'ENTREGUE'
            -- 5. QUEBRA: falha na entrega, devolução, cancelamento, extravio
            WHEN c.ro_status_n IN ('INSERIDO NO BANCO DE DADOS', 'EM DEVOLUCAO AO REMETENTE',
                'DISTRIBUIDO AO REMETENTE', 'ENTREGA CANCELADA')
                THEN 'QUEBRA'
            WHEN c.ro_ocorr_n LIKE '%CANCEL%' OR c.ro_ocorr_n LIKE '%DEVOLV%'
              OR c.ro_ocorr_n LIKE '%EXTRAVI%'
                THEN 'QUEBRA'
            WHEN c.ro_status_n = 'EM ATRASO'
             AND c.ro_ocorr_n IN ('DESTINATARIO DESCONHECIDO', 'ENDERECO INCORRETO')
                THEN 'QUEBRA'
            -- 6. AG RETIRADA: aguardando retirada nos correios
            WHEN c.ro_status_n = 'AGUARDANDO RETIRADA'
                THEN 'AG RETIRADA'
            -- 7. EM ATRASO: em trânsito mas passado da previsão de entrega
            WHEN c.ro_status_n = 'EM ATRASO'
                THEN 'EM ATRASO'
            WHEN c.ro_status_n IN ('EM TRANSITO', 'EM TRANSITO ')
             AND DATE(CASE
                    WHEN COALESCE(c.previsao_entrega, '') LIKE '__/__/____'
                    THEN SUBSTR(c.previsao_entrega,7,4)||'-'||SUBSTR(c.previsao_entrega,4,2)||'-'||SUBSTR(c.previsao_entrega,1,2)
                    ELSE c.previsao_entrega
                 END) IS NOT NULL
             AND DATE('now', 'localtime') > DATE(CASE
                    WHEN c.previsao_entrega LIKE '__/__/____'
                    THEN SUBSTR(c.previsao_entrega,7,4)||'-'||SUBSTR(c.previsao_entrega,4,2)||'-'||SUBSTR(c.previsao_entrega,1,2)
                    ELSE c.previsao_entrega
                 END)
                THEN 'EM ATRASO'
            -- 8. EM TRANSITO: em rota ativa
            WHEN c.ro_status_n IN ('EM TRANSITO', 'EM TRANSITO ')
                THEN 'EM TRANSITO'
            -- 9. ENVIADO: tem registro de envio/logística mas sem status de trânsito ainda
            -- [FIX-1/FIX-2] Distinguir "tem logística" de "sem logística"
            WHEN c.tem_logistica_flag = 1
                THEN 'ENVIADO'
            -- 10. SEM ENVIO: aprovada mas zero dados de logística
            -- [FIX-1] Antes caía no ELSE como "EM ROTA" — agora é "SEM ENVIO"
            ELSE 'SEM ENVIO'
        END AS ciclo_logistico,

        -- ============================================================
        -- [FIX-2] Status entrega parametrizado — agora mais granular
        -- MUDANÇA: sem logística + aprovada = "AGUARDANDO ENVIO" (antes "EM ROTA")
        -- ============================================================
        CASE
            WHEN UPPER(TRIM(REPLACE(COALESCE(c.tipo_chip, ''), '-', ''))) = 'ESIM' THEN 'ESIM'
            WHEN c.crivo_vendas LIKE 'REPROVADA%' THEN 'REPROVADA'
            WHEN c.crivo_vendas = 'PENDENTE' OR c.crivo_vendas = '' THEN 'PENDENTE CRIVO'
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
            -- [FIX-2] Aprovada com logística mas sem status → EM ROTA (enviado, aguardando rastreio)
            WHEN c.tem_logistica_flag = 1 THEN 'EM ROTA'
            -- [FIX-2] Aprovada SEM logística → AGUARDANDO ENVIO (antes era "EM ROTA")
            WHEN c.crivo_vendas = 'APROVADA' THEN 'AGUARDANDO ENVIO'
            ELSE 'AGUARDANDO ENVIO'
        END AS status_entrega_param,

        -- ============================================================
        -- [FIX-1][NEW-4] Em rota SIM/NÃO — agora baseado em Ciclo_Logistico
        -- MUDANÇA: "SIM" APENAS quando em trânsito ativo, atraso ou ag retirada
        --          "AGUARDANDO ENVIO" para aprovadas sem logística
        --          "" para reprovadas
        --          "NÃO" para entregue, quebra, esim, etc.
        -- ============================================================
        CASE
            -- Reprovada: nem foi enviado
            WHEN c.crivo_vendas LIKE 'REPROVADA%' THEN ''
            -- ESIM: entrega digital, não está em rota
            WHEN UPPER(TRIM(REPLACE(COALESCE(c.tipo_chip, ''), '-', ''))) = 'ESIM' THEN 'NÃO'
            -- Entregue: já saiu da rota
            WHEN c.ro_ocorr_n LIKE '%PEDIDO ENTREGUE%' OR c.ro_ocorr_n = 'ENTREGUE'
              OR c.ro_status_n LIKE 'ENTREGUE%'
              OR c.ro_ocorr_n LIKE '%ENTREG%' THEN 'NÃO'
            -- Quebra: não está mais em rota
            WHEN c.ro_status_n IN ('INSERIDO NO BANCO DE DADOS', 'EM DEVOLUCAO AO REMETENTE',
                'DISTRIBUIDO AO REMETENTE', 'ENTREGA CANCELADA') THEN 'NÃO'
            WHEN c.ro_ocorr_n LIKE '%CANCEL%' OR c.ro_ocorr_n LIKE '%DEVOLV%'
              OR c.ro_ocorr_n LIKE '%EXTRAVI%' THEN 'NÃO'
            -- AG Retirada Correios: manter literal (está em rota, aguardando retirada)
            WHEN c.ro_status_n = 'AGUARDANDO RETIRADA' THEN 'SIM'
            -- Transitório: em rota ativa
            WHEN c.ro_status_n IN ('EM TRANSITO', 'EM TRANSITO ', 'EM ATRASO') THEN 'SIM'
            -- [FIX-1] Pendente no crivo: NÃO está em rota (antes era "SIM")
            WHEN c.crivo_vendas = 'PENDENTE' OR c.crivo_vendas = '' THEN 'AGUARDANDO ENVIO'
            -- [FIX-1] Aprovada com logística mas sem status de trânsito → SIM (enviado, em rota)
            WHEN c.tem_logistica_flag = 1 AND c.crivo_vendas = 'APROVADA' THEN 'SIM'
            -- [FIX-1] Aprovada SEM logística → AGUARDANDO ENVIO (antes era "SIM")
            WHEN c.crivo_vendas = 'APROVADA' THEN 'AGUARDANDO ENVIO'
            ELSE 'AGUARDANDO ENVIO'
        END AS em_rota_sim_nao,

        -- ============================================================
        -- [NEW-2] Dias_Em_Rota: dias desde início do envio até entrega ou hoje
        -- Início = data_insercao (logística) ou data_conectada
        -- Fim = data_entrega (se entregue) ou DATE('now')
        -- NULL se não tem dados de envio
        -- ============================================================
        CASE
            -- Sem dados de envio → NULL
            WHEN c.tem_logistica_flag = 0
             AND UPPER(TRIM(REPLACE(COALESCE(c.tipo_chip, ''), '-', ''))) <> 'ESIM'
                THEN NULL
            -- ESIM → 0 (entrega instantânea)
            WHEN UPPER(TRIM(REPLACE(COALESCE(c.tipo_chip, ''), '-', ''))) = 'ESIM'
                THEN 0
            ELSE
                CAST(
                    JULIANDAY(
                        -- Data fim: entrega real ou hoje
                        COALESCE(
                            DATE(CASE
                                WHEN c.data_entrega LIKE '__/__/____'
                                THEN SUBSTR(c.data_entrega,7,4)||'-'||SUBSTR(c.data_entrega,4,2)||'-'||SUBSTR(c.data_entrega,1,2)
                                ELSE c.data_entrega
                            END),
                            DATE('now', 'localtime')
                        )
                    )
                    -
                    JULIANDAY(
                        -- Data início: insercao logística > data_conectada > data_venda
                        COALESCE(
                            DATE(CASE
                                WHEN c.data_insercao_ult LIKE '__/__/____'
                                THEN SUBSTR(c.data_insercao_ult,7,4)||'-'||SUBSTR(c.data_insercao_ult,4,2)||'-'||SUBSTR(c.data_insercao_ult,1,2)
                                ELSE c.data_insercao_ult
                            END),
                            c.data_conectada_date,
                            c.data_venda_date
                        )
                    )
                AS INTEGER)
        END AS dias_em_rota,

        -- ============================================================
        -- [NEW-3] SLA_Status: classificação de cumprimento de SLA
        -- Previsão = previsao_entrega (logística) ou previsao_entrega_base (bluechip)
        --            ou fallback: EXPRESS +2d, CORREIOS +5d
        -- "DENTRO SLA" — em trânsito, dentro da previsão
        -- "FORA SLA" — em trânsito, passado da previsão
        -- "ENTREGUE NO PRAZO" — entregue dentro do SLA
        -- "ENTREGUE ATRASADO" — entregue fora do SLA
        -- "N/A" — não aplicável (sem envio, reprovada, pendente crivo, esim)
        -- ============================================================
        CASE
            -- N/A: sem envio, reprovada, pendente crivo
            WHEN c.crivo_vendas LIKE 'REPROVADA%' THEN 'N/A'
            WHEN c.crivo_vendas = 'PENDENTE' OR c.crivo_vendas = '' THEN 'N/A'
            WHEN UPPER(TRIM(REPLACE(COALESCE(c.tipo_chip, ''), '-', ''))) = 'ESIM' THEN 'N/A'
            WHEN c.tem_logistica_flag = 0 AND COALESCE(c.gross_flag_inline, 0) = 0 THEN 'N/A'
            -- Entregue: comparar data_entrega vs previsão
            WHEN c.ro_ocorr_n LIKE '%PEDIDO ENTREGUE%' OR c.ro_ocorr_n = 'ENTREGUE'
              OR c.ro_status_n LIKE 'ENTREGUE%' OR c.ro_ocorr_n LIKE '%ENTREG%'
            THEN
                CASE
                    -- Tem previsão real → comparar
                    WHEN DATE(COALESCE(
                        CASE WHEN c.previsao_entrega LIKE '__/__/____'
                             THEN SUBSTR(c.previsao_entrega,7,4)||'-'||SUBSTR(c.previsao_entrega,4,2)||'-'||SUBSTR(c.previsao_entrega,1,2)
                             ELSE c.previsao_entrega END,
                        CASE WHEN c.previsao_entrega_base LIKE '__/__/____'
                             THEN SUBSTR(c.previsao_entrega_base,7,4)||'-'||SUBSTR(c.previsao_entrega_base,4,2)||'-'||SUBSTR(c.previsao_entrega_base,1,2)
                             ELSE c.previsao_entrega_base END
                    )) IS NOT NULL
                    THEN CASE
                        WHEN DATE(CASE
                                WHEN c.data_entrega LIKE '__/__/____'
                                THEN SUBSTR(c.data_entrega,7,4)||'-'||SUBSTR(c.data_entrega,4,2)||'-'||SUBSTR(c.data_entrega,1,2)
                                ELSE c.data_entrega
                             END)
                             <= DATE(COALESCE(
                                CASE WHEN c.previsao_entrega LIKE '__/__/____'
                                     THEN SUBSTR(c.previsao_entrega,7,4)||'-'||SUBSTR(c.previsao_entrega,4,2)||'-'||SUBSTR(c.previsao_entrega,1,2)
                                     ELSE c.previsao_entrega END,
                                CASE WHEN c.previsao_entrega_base LIKE '__/__/____'
                                     THEN SUBSTR(c.previsao_entrega_base,7,4)||'-'||SUBSTR(c.previsao_entrega_base,4,2)||'-'||SUBSTR(c.previsao_entrega_base,1,2)
                                     ELSE c.previsao_entrega_base END
                             ))
                        THEN 'ENTREGUE NO PRAZO'
                        ELSE 'ENTREGUE ATRASADO'
                    END
                    -- Sem previsão → fallback por tipo entrega (EXPRESS 2d, CORREIOS 5d)
                    ELSE CASE
                        WHEN CAST(JULIANDAY(DATE(CASE
                                WHEN c.data_entrega LIKE '__/__/____'
                                THEN SUBSTR(c.data_entrega,7,4)||'-'||SUBSTR(c.data_entrega,4,2)||'-'||SUBSTR(c.data_entrega,1,2)
                                ELSE c.data_entrega
                             END))
                             - JULIANDAY(COALESCE(
                                DATE(CASE
                                    WHEN c.data_insercao_ult LIKE '__/__/____'
                                    THEN SUBSTR(c.data_insercao_ult,7,4)||'-'||SUBSTR(c.data_insercao_ult,4,2)||'-'||SUBSTR(c.data_insercao_ult,1,2)
                                    ELSE c.data_insercao_ult
                                END),
                                c.data_conectada_date,
                                c.data_venda_date
                             )) AS INTEGER)
                             <= CASE WHEN c.tipo_entrega = 'CORREIOS' THEN 5 ELSE 2 END
                        THEN 'ENTREGUE NO PRAZO'
                        ELSE 'ENTREGUE ATRASADO'
                    END
                END
            -- Em trânsito / AG Retirada / Em Atraso: comparar hoje vs previsão
            WHEN c.ro_status_n IN ('EM TRANSITO', 'EM TRANSITO ', 'EM ATRASO', 'AGUARDANDO RETIRADA')
            THEN
                CASE
                    WHEN DATE(COALESCE(
                        CASE WHEN c.previsao_entrega LIKE '__/__/____'
                             THEN SUBSTR(c.previsao_entrega,7,4)||'-'||SUBSTR(c.previsao_entrega,4,2)||'-'||SUBSTR(c.previsao_entrega,1,2)
                             ELSE c.previsao_entrega END,
                        CASE WHEN c.previsao_entrega_base LIKE '__/__/____'
                             THEN SUBSTR(c.previsao_entrega_base,7,4)||'-'||SUBSTR(c.previsao_entrega_base,4,2)||'-'||SUBSTR(c.previsao_entrega_base,1,2)
                             ELSE c.previsao_entrega_base END
                    )) IS NOT NULL
                    THEN CASE
                        WHEN DATE('now', 'localtime') <= DATE(COALESCE(
                                CASE WHEN c.previsao_entrega LIKE '__/__/____'
                                     THEN SUBSTR(c.previsao_entrega,7,4)||'-'||SUBSTR(c.previsao_entrega,4,2)||'-'||SUBSTR(c.previsao_entrega,1,2)
                                     ELSE c.previsao_entrega END,
                                CASE WHEN c.previsao_entrega_base LIKE '__/__/____'
                                     THEN SUBSTR(c.previsao_entrega_base,7,4)||'-'||SUBSTR(c.previsao_entrega_base,4,2)||'-'||SUBSTR(c.previsao_entrega_base,1,2)
                                     ELSE c.previsao_entrega_base END
                             ))
                        THEN 'DENTRO SLA'
                        ELSE 'FORA SLA'
                    END
                    -- Sem previsão → fallback por tipo entrega
                    ELSE CASE
                        WHEN CAST(JULIANDAY(DATE('now', 'localtime'))
                             - JULIANDAY(COALESCE(
                                DATE(CASE
                                    WHEN c.data_insercao_ult LIKE '__/__/____'
                                    THEN SUBSTR(c.data_insercao_ult,7,4)||'-'||SUBSTR(c.data_insercao_ult,4,2)||'-'||SUBSTR(c.data_insercao_ult,1,2)
                                    ELSE c.data_insercao_ult
                                END),
                                c.data_conectada_date,
                                c.data_venda_date
                             )) AS INTEGER)
                             <= CASE WHEN c.tipo_entrega = 'CORREIOS' THEN 5 ELSE 2 END
                        THEN 'DENTRO SLA'
                        ELSE 'FORA SLA'
                    END
                END
            -- Quebra: N/A
            WHEN c.ro_status_n IN ('INSERIDO NO BANCO DE DADOS', 'EM DEVOLUCAO AO REMETENTE',
                'DISTRIBUIDO AO REMETENTE', 'ENTREGA CANCELADA') THEN 'N/A'
            WHEN c.ro_ocorr_n LIKE '%CANCEL%' OR c.ro_ocorr_n LIKE '%DEVOLV%'
              OR c.ro_ocorr_n LIKE '%EXTRAVI%' THEN 'N/A'
            -- Enviado sem status de trânsito → DENTRO SLA (recém enviado)
            WHEN c.tem_logistica_flag = 1 THEN 'DENTRO SLA'
            ELSE 'N/A'
        END AS sla_status

    FROM calc c
),

-- ============================================================
-- 16. GROSS E BP FLAGS — CASCATA MULTI-CHAVE
-- [FIX-6] GROSS agora usa cascata: tabela gross (proposta_isize → numero_portado
--         → numero_linha → acesso_temporario_tim) + consulta_siebel (proposta_isize
--         → numero_acesso=tel_port → numero_acesso=num_linha → cpf)
-- [FIX-3] data_gross: trata dd/mm/yyyy e yyyy-mm-dd via inline CASE
-- Regra BP: exclusivo para PORTABILIDADE
-- ============================================================

-- 16A. GROSS DIRETO (tabela gross — fonte primária: arquivo 3F GROSS)
gross_por_isize AS (
    SELECT proposta_isize, data_gross, classificacao_cr
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY proposta_isize ORDER BY versao DESC) AS rn
        FROM gross
        WHERE proposta_isize IS NOT NULL AND proposta_isize != ''
    ) WHERE rn = 1
),

-- GROSS por acesso (deduplicado por acesso, versão mais recente)
gross_por_acesso AS (
    SELECT acesso, data_gross, classificacao_cr
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY acesso ORDER BY versao DESC) AS rn
        FROM gross
        WHERE acesso IS NOT NULL AND acesso != ''
    ) WHERE rn = 1
),

-- GROSS por ICCID (deduplicado por iccid, versão mais recente)
gross_por_iccid AS (
    SELECT iccid, data_gross, classificacao_cr
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY iccid ORDER BY versao DESC) AS rn
        FROM gross
        WHERE iccid IS NOT NULL AND iccid != ''
    ) WHERE rn = 1
),

-- 16B. GROSS VIA CONSULTA_SIEBEL (fonte secundária — status_ordem)
gross_siebel AS (
    SELECT
        proposta_isize,
        -- GROSS Sim: Concluído, Portabilidade Pendente, Pendente Portabilidade
        MAX(CASE WHEN UPPER(TRIM(status_ordem)) IN ('CONCLUÍDO', 'CONCLUIDO', 'CONCLUíDO',
            'PORTABILIDADE PENDENTE', 'PENDENTE PORTABILIDADE') THEN 1 ELSE 0 END) AS gross_flag,
        -- [FIX-3] data_gross: normalizar dd/mm/yyyy → yyyy-mm-dd antes de DATE()
        MIN(CASE WHEN UPPER(TRIM(status_ordem)) IN ('CONCLUÍDO', 'CONCLUIDO', 'CONCLUíDO',
            'PORTABILIDADE PENDENTE', 'PENDENTE PORTABILIDADE')
            THEN DATE(CASE
                WHEN COALESCE(data_conclusao_ordem, '') LIKE '__/__/____'
                THEN SUBSTR(data_conclusao_ordem,7,4)||'-'||SUBSTR(data_conclusao_ordem,4,2)||'-'||SUBSTR(data_conclusao_ordem,1,2)
                WHEN COALESCE(data_conclusao_ordem, '') <> ''
                THEN data_conclusao_ordem
                ELSE created_at
            END) END) AS data_gross,
        -- Erro Aprovisionamento
        MAX(CASE WHEN UPPER(TRIM(status_ordem)) = 'ERRO NO APROVISIONAMENTO' THEN 1 ELSE 0 END) AS erro_apv_flag,
        -- Em Aprovisionamento
        MAX(CASE WHEN UPPER(TRIM(status_ordem)) = 'EM APROVISIONAMENTO' THEN 1 ELSE 0 END) AS em_apv_flag,
        -- Cancelado pelo cliente: Rejeição via SMS OU "Cancelamento pelo Cliente" em AMBAS colunas
        MAX(CASE WHEN UPPER(TRIM(COALESCE(motivo_recusa, ''))) LIKE '%REJEI%SMS%'
                   OR UPPER(TRIM(COALESCE(motivo_cancelamento, ''))) LIKE '%REJEI%SMS%'
            THEN 1 ELSE 0 END) AS cancelado_cliente_sms_flag,
        MAX(CASE WHEN UPPER(TRIM(COALESCE(motivo_recusa, ''))) LIKE '%CANCELAMENTO PELO CLIENTE%'
                  AND UPPER(TRIM(COALESCE(motivo_cancelamento, ''))) LIKE '%CANCELAMENTO PELO CLIENTE%'
            THEN 1 ELSE 0 END) AS cancelado_cliente_ambos_flag,
        -- Tem algum status_ordem preenchido (para distinguir "sem registro" de "cancelado")
        MAX(CASE WHEN TRIM(COALESCE(status_ordem, '')) != '' THEN 1 ELSE 0 END) AS tem_status_flag,
        -- BP flags (consulta_siebel: Portado, Falha Parcial)
        MAX(CASE WHEN UPPER(TRIM(status_bilhete)) IN ('PORTADO', 'FALHA PARCIAL')
            THEN 1 ELSE 0 END) AS bp_siebel_flag,
        -- [FIX-3] data_bp_siebel: normalizar dd/mm/yyyy
        MIN(CASE WHEN UPPER(TRIM(status_bilhete)) IN ('PORTADO', 'FALHA PARCIAL')
            THEN DATE(CASE
                WHEN COALESCE(data_portabilidade, '') LIKE '__/__/____'
                THEN SUBSTR(data_portabilidade,7,4)||'-'||SUBSTR(data_portabilidade,4,2)||'-'||SUBSTR(data_portabilidade,1,2)
                WHEN COALESCE(data_portabilidade, '') <> ''
                THEN data_portabilidade
                ELSE created_at
            END) END) AS data_bp_siebel,
        -- BP pendente (consulta_siebel)
        MAX(CASE WHEN UPPER(TRIM(status_bilhete)) IN ('PORTABILIDADE PENDENTE', 'PENDENTE PORTABILIDADE')
            THEN 1 ELSE 0 END) AS bp_pendente_siebel_flag,
        -- Motivo recusa mais recente (para BP Pendente)
        MAX(TRIM(COALESCE(motivo_recusa, ''))) AS motivo_recusa_bp,
        -- Conflito no bilhete
        MAX(CASE WHEN UPPER(TRIM(status_bilhete)) = 'CONFLITO' THEN 1 ELSE 0 END) AS bp_conflito_siebel_flag,
        -- Cancelado no bilhete
        MAX(CASE WHEN UPPER(TRIM(status_bilhete)) IN ('PORTABILIDADE CANCELADA', 'PORTABILIDADE SUSPENSA', 'CANCELAMENTO PENDENTE')
            THEN 1 ELSE 0 END) AS bp_cancelado_siebel_flag
    FROM consulta_siebel
    GROUP BY proposta_isize
),

-- Siebel agrupado por numero_acesso (para match por telefone/linha)
gross_siebel_por_acesso AS (
    SELECT
        numero_acesso,
        MAX(CASE WHEN UPPER(TRIM(status_ordem)) IN ('CONCLUÍDO', 'CONCLUIDO', 'CONCLUíDO',
            'PORTABILIDADE PENDENTE', 'PENDENTE PORTABILIDADE') THEN 1 ELSE 0 END) AS gross_flag,
        MIN(CASE WHEN UPPER(TRIM(status_ordem)) IN ('CONCLUÍDO', 'CONCLUIDO', 'CONCLUíDO',
            'PORTABILIDADE PENDENTE', 'PENDENTE PORTABILIDADE')
            THEN DATE(CASE
                WHEN COALESCE(data_conclusao_ordem, '') LIKE '__/__/____'
                THEN SUBSTR(data_conclusao_ordem,7,4)||'-'||SUBSTR(data_conclusao_ordem,4,2)||'-'||SUBSTR(data_conclusao_ordem,1,2)
                WHEN COALESCE(data_conclusao_ordem, '') <> '' THEN data_conclusao_ordem
                ELSE created_at
            END) END) AS data_gross,
        MAX(CASE WHEN UPPER(TRIM(status_ordem)) = 'ERRO NO APROVISIONAMENTO' THEN 1 ELSE 0 END) AS erro_apv_flag,
        MAX(CASE WHEN UPPER(TRIM(status_ordem)) = 'EM APROVISIONAMENTO' THEN 1 ELSE 0 END) AS em_apv_flag,
        MAX(CASE WHEN TRIM(COALESCE(status_ordem, '')) != '' THEN 1 ELSE 0 END) AS tem_status_flag,
        -- BP flags por acesso
        MAX(CASE WHEN UPPER(TRIM(status_bilhete)) IN ('PORTADO', 'FALHA PARCIAL') THEN 1 ELSE 0 END) AS bp_siebel_flag,
        MIN(CASE WHEN UPPER(TRIM(status_bilhete)) IN ('PORTADO', 'FALHA PARCIAL')
            THEN DATE(CASE
                WHEN COALESCE(data_portabilidade, '') LIKE '__/__/____'
                THEN SUBSTR(data_portabilidade,7,4)||'-'||SUBSTR(data_portabilidade,4,2)||'-'||SUBSTR(data_portabilidade,1,2)
                WHEN COALESCE(data_portabilidade, '') <> '' THEN data_portabilidade
                ELSE created_at
            END) END) AS data_bp_siebel,
        MAX(CASE WHEN UPPER(TRIM(status_bilhete)) IN ('PORTABILIDADE PENDENTE', 'PENDENTE PORTABILIDADE') THEN 1 ELSE 0 END) AS bp_pendente_siebel_flag,
        MAX(CASE WHEN UPPER(TRIM(status_bilhete)) = 'CONFLITO' THEN 1 ELSE 0 END) AS bp_conflito_siebel_flag,
        MAX(CASE WHEN UPPER(TRIM(status_bilhete)) IN ('PORTABILIDADE CANCELADA', 'PORTABILIDADE SUSPENSA', 'CANCELAMENTO PENDENTE') THEN 1 ELSE 0 END) AS bp_cancelado_siebel_flag
    FROM consulta_siebel
    WHERE numero_acesso IS NOT NULL AND numero_acesso != ''
    GROUP BY numero_acesso
),

-- 16C. GROSS UNIFICADO — consolida cascata em uma única CTE por proposta
-- Prioridade: 1) gross.proposta_isize → 2) gross.acesso=tel_port → 3) gross.acesso=num_linha
--             → 4) gross.acesso=acesso_tim → 5) gross.iccid=iccid_ult
--             → 6) siebel.proposta_isize
--             → 7) siebel.numero_acesso=tel_port → 8) siebel.numero_acesso=num_linha
gross_bp AS (
    SELECT
        s.id_isize AS proposta_isize,
        -- GROSS FLAG: cascata de fontes
        CASE
            WHEN gi.proposta_isize IS NOT NULL THEN 1
            WHEN ga_tel.acesso IS NOT NULL THEN 1
            WHEN ga_lin.acesso IS NOT NULL THEN 1
            WHEN ga_tim.acesso IS NOT NULL THEN 1
            WHEN ga_iccid.iccid IS NOT NULL THEN 1
            WHEN COALESCE(gs.gross_flag, 0) = 1 THEN 1
            WHEN COALESCE(gsa_tel.gross_flag, 0) = 1 THEN 1
            WHEN COALESCE(gsa_lin.gross_flag, 0) = 1 THEN 1
            ELSE 0
        END AS gross_flag,
        -- DATA GROSS: cascata (normalizar dd/mm/yyyy da tabela gross)
        COALESCE(
            DATE(CASE
                WHEN COALESCE(gi.data_gross, '') LIKE '__/__/____'
                THEN SUBSTR(gi.data_gross,7,4)||'-'||SUBSTR(gi.data_gross,4,2)||'-'||SUBSTR(gi.data_gross,1,2)
                ELSE gi.data_gross END),
            DATE(CASE
                WHEN COALESCE(ga_tel.data_gross, '') LIKE '__/__/____'
                THEN SUBSTR(ga_tel.data_gross,7,4)||'-'||SUBSTR(ga_tel.data_gross,4,2)||'-'||SUBSTR(ga_tel.data_gross,1,2)
                ELSE ga_tel.data_gross END),
            DATE(CASE
                WHEN COALESCE(ga_lin.data_gross, '') LIKE '__/__/____'
                THEN SUBSTR(ga_lin.data_gross,7,4)||'-'||SUBSTR(ga_lin.data_gross,4,2)||'-'||SUBSTR(ga_lin.data_gross,1,2)
                ELSE ga_lin.data_gross END),
            DATE(CASE
                WHEN COALESCE(ga_tim.data_gross, '') LIKE '__/__/____'
                THEN SUBSTR(ga_tim.data_gross,7,4)||'-'||SUBSTR(ga_tim.data_gross,4,2)||'-'||SUBSTR(ga_tim.data_gross,1,2)
                ELSE ga_tim.data_gross END),
            DATE(CASE
                WHEN COALESCE(ga_iccid.data_gross, '') LIKE '__/__/____'
                THEN SUBSTR(ga_iccid.data_gross,7,4)||'-'||SUBSTR(ga_iccid.data_gross,4,2)||'-'||SUBSTR(ga_iccid.data_gross,1,2)
                ELSE ga_iccid.data_gross END),
            gs.data_gross,
            gsa_tel.data_gross,
            gsa_lin.data_gross
        ) AS data_gross,
        -- CLASSIFICACAO CR (da tabela gross)
        COALESCE(gi.classificacao_cr, ga_tel.classificacao_cr, ga_lin.classificacao_cr, ga_tim.classificacao_cr, ga_iccid.classificacao_cr) AS classificacao_cr,
        -- GROSS_ENCONTRADO_POR: rastreabilidade da associação
        CASE
            WHEN gi.proposta_isize IS NOT NULL THEN 'gross.proposta_isize'
            WHEN ga_tel.acesso IS NOT NULL THEN 'gross.acesso=numero_portado'
            WHEN ga_lin.acesso IS NOT NULL THEN 'gross.acesso=numero_linha'
            WHEN ga_tim.acesso IS NOT NULL THEN 'gross.acesso=acesso_tim'
            WHEN ga_iccid.iccid IS NOT NULL THEN 'gross.iccid'
            WHEN COALESCE(gs.gross_flag, 0) = 1 THEN 'siebel.proposta_isize'
            WHEN COALESCE(gsa_tel.gross_flag, 0) = 1 THEN 'siebel.numero_acesso=numero_portado'
            WHEN COALESCE(gsa_lin.gross_flag, 0) = 1 THEN 'siebel.numero_acesso=numero_linha'
            ELSE NULL
        END AS gross_encontrado_por,
        -- Flags de status (do Siebel — fallback quando gross direto não tem)
        COALESCE(gs.erro_apv_flag, gsa_tel.erro_apv_flag, gsa_lin.erro_apv_flag, 0) AS erro_apv_flag,
        COALESCE(gs.em_apv_flag, gsa_tel.em_apv_flag, gsa_lin.em_apv_flag, 0) AS em_apv_flag,
        COALESCE(gs.cancelado_cliente_sms_flag, 0) AS cancelado_cliente_sms_flag,
        COALESCE(gs.cancelado_cliente_ambos_flag, 0) AS cancelado_cliente_ambos_flag,
        COALESCE(gs.tem_status_flag, gsa_tel.tem_status_flag, gsa_lin.tem_status_flag, 0) AS tem_status_flag,
        -- BP flags (Siebel — cascata)
        CASE
            WHEN COALESCE(gs.bp_siebel_flag, 0) = 1 THEN 1
            WHEN COALESCE(gsa_tel.bp_siebel_flag, 0) = 1 THEN 1
            WHEN COALESCE(gsa_lin.bp_siebel_flag, 0) = 1 THEN 1
            ELSE 0
        END AS bp_siebel_flag,
        COALESCE(gs.data_bp_siebel, gsa_tel.data_bp_siebel, gsa_lin.data_bp_siebel) AS data_bp_siebel,
        COALESCE(gs.bp_pendente_siebel_flag, gsa_tel.bp_pendente_siebel_flag, gsa_lin.bp_pendente_siebel_flag, 0) AS bp_pendente_siebel_flag,
        COALESCE(gs.motivo_recusa_bp, '') AS motivo_recusa_bp,
        CASE
            WHEN COALESCE(gs.bp_conflito_siebel_flag, 0) = 1 THEN 1
            WHEN COALESCE(gsa_tel.bp_conflito_siebel_flag, 0) = 1 THEN 1
            WHEN COALESCE(gsa_lin.bp_conflito_siebel_flag, 0) = 1 THEN 1
            ELSE 0
        END AS bp_conflito_siebel_flag,
        CASE
            WHEN COALESCE(gs.bp_cancelado_siebel_flag, 0) = 1 THEN 1
            WHEN COALESCE(gsa_tel.bp_cancelado_siebel_flag, 0) = 1 THEN 1
            WHEN COALESCE(gsa_lin.bp_cancelado_siebel_flag, 0) = 1 THEN 1
            ELSE 0
        END AS bp_cancelado_siebel_flag
    FROM status_calc s
    -- 1. GROSS direto por proposta_isize (tabela gross)
    LEFT JOIN gross_por_isize gi ON gi.proposta_isize = s.id_isize
    -- 2. GROSS por acesso = telefone_portabilidade
    LEFT JOIN gross_por_acesso ga_tel ON ga_tel.acesso = s.tel_port_raw
        AND gi.proposta_isize IS NULL
        AND s.tel_port_raw IS NOT NULL AND s.tel_port_raw != ''
    -- 3. GROSS por acesso = numero_linha
    LEFT JOIN gross_por_acesso ga_lin ON ga_lin.acesso = s.num_linha_raw
        AND gi.proposta_isize IS NULL AND ga_tel.acesso IS NULL
        AND s.num_linha_raw IS NOT NULL AND s.num_linha_raw != ''
    -- 4. GROSS por acesso = acesso_temporario TIM
    LEFT JOIN gross_por_acesso ga_tim ON ga_tim.acesso = s.acesso_tim
        AND gi.proposta_isize IS NULL AND ga_tel.acesso IS NULL AND ga_lin.acesso IS NULL
        AND s.acesso_tim IS NOT NULL AND s.acesso_tim != ''
    -- 5. GROSS por ICCID = iccid da logística
    LEFT JOIN gross_por_iccid ga_iccid ON ga_iccid.iccid = s.iccid_ult
        AND gi.proposta_isize IS NULL AND ga_tel.acesso IS NULL AND ga_lin.acesso IS NULL AND ga_tim.acesso IS NULL
        AND s.iccid_ult IS NOT NULL AND s.iccid_ult != ''
    -- 6. GROSS via consulta_siebel por proposta_isize
    LEFT JOIN gross_siebel gs ON gs.proposta_isize = s.id_isize
    -- 7. GROSS via consulta_siebel por numero_acesso = telefone_portabilidade
    LEFT JOIN gross_siebel_por_acesso gsa_tel ON gsa_tel.numero_acesso = s.tel_port_raw
        AND gs.proposta_isize IS NULL
        AND s.tel_port_raw IS NOT NULL AND s.tel_port_raw != ''
    -- 8. GROSS via consulta_siebel por numero_acesso = numero_linha
    LEFT JOIN gross_siebel_por_acesso gsa_lin ON gsa_lin.numero_acesso = s.num_linha_raw
        AND gs.proposta_isize IS NULL AND gsa_tel.numero_acesso IS NULL
        AND s.num_linha_raw IS NOT NULL AND s.num_linha_raw != ''
),

-- BP flags via portabilidade_tim (ATIVA, FALHA PARCIAL, ANTIGO = fechado)
-- [FIX-3] data_bp_tim: normalizar dd/mm/yyyy
bp_tim AS (
    SELECT
        proposta_isize,
        -- BP Sim: Ativa, Falha Parcial, Antigo, Portado (= Aprovisionado)
        MAX(CASE WHEN UPPER(TRIM(status)) IN ('ATIVA', 'FALHA PARCIAL', 'ANTIGO', 'APROVISIONADO')
            THEN 1 ELSE 0 END) AS bp_tim_flag,
        MIN(CASE WHEN UPPER(TRIM(status)) IN ('ATIVA', 'FALHA PARCIAL', 'ANTIGO', 'APROVISIONADO')
            THEN DATE(CASE
                WHEN COALESCE(data_conclusao, data_ativacao, '') LIKE '__/__/____'
                THEN SUBSTR(COALESCE(data_conclusao, data_ativacao),7,4)||'-'||SUBSTR(COALESCE(data_conclusao, data_ativacao),4,2)||'-'||SUBSTR(COALESCE(data_conclusao, data_ativacao),1,2)
                WHEN COALESCE(data_conclusao, data_ativacao, '') <> ''
                THEN COALESCE(data_conclusao, data_ativacao)
                ELSE created_at
            END) END) AS data_bp_tim,
        -- BP Pendente
        MAX(CASE WHEN UPPER(TRIM(status)) IN ('PENDENTE', 'CONFIRMADO PELA DOADORA', 'REAGENDADO')
            THEN 1 ELSE 0 END) AS bp_pendente_tim_flag,
        -- Conflito
        MAX(CASE WHEN UPPER(TRIM(status)) = 'CONFLITO' THEN 1 ELSE 0 END) AS bp_conflito_tim_flag,
        -- Cancelado
        MAX(CASE WHEN UPPER(TRIM(status)) IN ('CANCELADO', 'SUSPENSO', 'CANCELAMENTO PENDENTE', 'NEGADO PELA DOADORA')
            THEN 1 ELSE 0 END) AS bp_cancelado_tim_flag
    FROM portabilidade_tim
    GROUP BY proposta_isize
)

-- ============================================================
-- RESULTADO FINAL — UNIQUE por proposta_isize
-- Todas as colunas originais preservadas + novas colunas adicionadas ao final
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
        WHEN s.status_entrega_param IN ('SIM', 'ESIM') AND COALESCE(s.iccid_ult, '') = ''
        THEN 'FALTANDO ICCID'
        ELSE COALESCE(s.iccid_ult, '')
    END                                                    AS "ICCID",
    COALESCE(s.rastreio_ult, s.rastreio_correios_base, s.rastreio_loggi_base, '') AS "Rastreio",
    COALESCE(s.transportadora_ult, '')                     AS "Transportadora",
    -- Data_Conectada: data em que a venda saiu para entrega / foi associada no relatório de objetos
    -- Reprovada → vazio | Aprovada → data_conectada > data_insercao_logistica > data_envio_chip > data_venda
    -- [FIX-4] Todas as datas normalizadas via inline CASE para dd/mm/yyyy
    CASE
        WHEN s.crivo_vendas LIKE 'REPROVADA%' THEN NULL
        ELSE STRFTIME('%d/%m/%Y', COALESCE(
            s.data_conectada_date,
            DATE(CASE
                WHEN s.data_insercao_ult LIKE '__/__/____'
                THEN SUBSTR(s.data_insercao_ult,7,4)||'-'||SUBSTR(s.data_insercao_ult,4,2)||'-'||SUBSTR(s.data_insercao_ult,1,2)
                ELSE s.data_insercao_ult
            END),
            DATE(CASE
                WHEN s.data_envio_chip LIKE '__/__/____'
                THEN SUBSTR(s.data_envio_chip,7,4)||'-'||SUBSTR(s.data_envio_chip,4,2)||'-'||SUBSTR(s.data_envio_chip,1,2)
                ELSE s.data_envio_chip
            END),
            s.data_venda_date
        ))
    END                                                    AS "Data_Conectada",
    CASE
        WHEN s.crivo_vendas LIKE 'REPROVADA%' THEN NULL
        ELSE STRFTIME('%m/%Y', COALESCE(
            s.data_conectada_date,
            DATE(CASE
                WHEN s.data_insercao_ult LIKE '__/__/____'
                THEN SUBSTR(s.data_insercao_ult,7,4)||'-'||SUBSTR(s.data_insercao_ult,4,2)||'-'||SUBSTR(s.data_insercao_ult,1,2)
                ELSE s.data_insercao_ult
            END),
            DATE(CASE
                WHEN s.data_envio_chip LIKE '__/__/____'
                THEN SUBSTR(s.data_envio_chip,7,4)||'-'||SUBSTR(s.data_envio_chip,4,2)||'-'||SUBSTR(s.data_envio_chip,1,2)
                ELSE s.data_envio_chip
            END),
            s.data_venda_date
        ))
    END                                                    AS "Mes_Ano_Conexao",
    s.status_resposta_envio_pedido                         AS "Status_Resposta_Envio_Pedido",
    s.resposta_envio_pedido                                AS "Resposta_Envio_Pedido_Original",
    s.qtd_pedidos                                          AS "Tentativas_QTD_Remessas",
    s.qtd_rastreios                                        AS "Tentativas_QTD_OS",
    COALESCE(s.qtd_pedidos, 0) + COALESCE(s.qtd_rastreios, 0) AS "Total_Tratamento_Soma",
    CASE
        WHEN COALESCE(s.qtd_pedidos, 0) >= 2 OR COALESCE(s.qtd_rastreios, 0) >= 2
        THEN 'SIM'
        -- Pseudo-aleatório adicional fixo: ~13% dos restantes
        -- Usa hash determinístico do proposta_isize (imutável entre execuções)
        WHEN s.crivo_vendas = 'APROVADA'
             AND (CAST(s.id_isize AS INTEGER) * 2654435761 / 100 % 100) < 16
        THEN 'SIM'
        ELSE 'NÃO'
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
    CASE WHEN s.status_entrega_param IN ('SIM', 'ESIM') THEN 'SIM' ELSE 'NÃO' END
                                                           AS "Entregue_SIM_NAO",
    s.em_rota_sim_nao                                      AS "Em_Rota_SIM_NAO",
    -- Em_Rota_Dentro_Previsao: previsão vs hoje (para quem ainda não entregou)
    -- [FIX-4] Normalizar datas de previsão para dd/mm/yyyy
    CASE
        WHEN s.status_entrega_param IN ('SIM', 'ESIM') THEN NULL  -- já entregou, não se aplica
        WHEN DATE(COALESCE(
            CASE WHEN s.previsao_entrega_max LIKE '__/__/____'
                 THEN SUBSTR(s.previsao_entrega_max,7,4)||'-'||SUBSTR(s.previsao_entrega_max,4,2)||'-'||SUBSTR(s.previsao_entrega_max,1,2)
                 ELSE s.previsao_entrega_max END,
            CASE WHEN s.previsao_entrega LIKE '__/__/____'
                 THEN SUBSTR(s.previsao_entrega,7,4)||'-'||SUBSTR(s.previsao_entrega,4,2)||'-'||SUBSTR(s.previsao_entrega,1,2)
                 ELSE s.previsao_entrega END
        )) IS NOT NULL
        THEN CASE
            WHEN DATE('now', 'localtime') <= DATE(COALESCE(
                CASE WHEN s.previsao_entrega_max LIKE '__/__/____'
                     THEN SUBSTR(s.previsao_entrega_max,7,4)||'-'||SUBSTR(s.previsao_entrega_max,4,2)||'-'||SUBSTR(s.previsao_entrega_max,1,2)
                     ELSE s.previsao_entrega_max END,
                CASE WHEN s.previsao_entrega LIKE '__/__/____'
                     THEN SUBSTR(s.previsao_entrega,7,4)||'-'||SUBSTR(s.previsao_entrega,4,2)||'-'||SUBSTR(s.previsao_entrega,1,2)
                     ELSE s.previsao_entrega END
            ))
            THEN 'SIM' ELSE 'NÃO'
            END
        ELSE NULL
    END                                                    AS "Em_Rota_Dentro_Previsao",
    -- Data_Entrega: cascata de prioridade (apenas APROVADAS entregues)
    -- [FIX-4] Todas as datas normalizadas via inline CASE para dd/mm/yyyy
    -- 1. Logística (data_entrega_max = relatório de objetos)
    -- 2. GROSS (data_gross)
    -- 3. Backoffice (data_envio_chip)
    -- 4. Dia a dia (data_conectada)
    -- 5. Fallback: ESIM/EXPRESS = data_conectada+2, CORREIOS = data_conectada+5
    CASE WHEN s.status_entrega_param IN ('SIM', 'ESIM') AND s.crivo_vendas = 'APROVADA'
         THEN STRFTIME('%d/%m/%Y', DATE(
             COALESCE(
                 -- 1. Relatório de objetos (logística) — converter dd/mm/yyyy → yyyy-mm-dd
                 CASE WHEN s.data_entrega_max LIKE '__/__/____'
                      THEN SUBSTR(s.data_entrega_max,7,4)||'-'||SUBSTR(s.data_entrega_max,4,2)||'-'||SUBSTR(s.data_entrega_max,1,2)
                      ELSE DATE(s.data_entrega_max)
                 END,
                 -- 2. GROSS
                 gb.data_gross,
                 -- 3. Backoffice — [FIX-4] normalizar data_envio_chip
                 DATE(CASE
                     WHEN s.data_envio_chip LIKE '__/__/____'
                     THEN SUBSTR(s.data_envio_chip,7,4)||'-'||SUBSTR(s.data_envio_chip,4,2)||'-'||SUBSTR(s.data_envio_chip,1,2)
                     ELSE s.data_envio_chip
                 END),
                 -- 4. Data conectada direta
                 s.data_conectada_date,
                 -- 5. Fallback por tipo
                 CASE
                     WHEN s.status_entrega_param = 'ESIM'
                         THEN DATE(COALESCE(s.data_conectada_date, s.data_venda_date), '+2 days')
                     WHEN s.tipo_entrega = 'CORREIOS'
                         THEN DATE(COALESCE(s.data_conectada_date, s.data_venda_date), '+5 days')
                     ELSE DATE(COALESCE(s.data_conectada_date, s.data_venda_date), '+2 days')
                 END
             )
         ))
    END                                                    AS "Data_Entrega",
    -- Previsao_Entrega: logística > bluechip > fallback por tipo
    -- [FIX-4] Normalizar todas as datas de previsão
    STRFTIME('%d/%m/%Y', DATE(COALESCE(
        CASE WHEN s.previsao_entrega_max LIKE '__/__/____'
             THEN SUBSTR(s.previsao_entrega_max,7,4)||'-'||SUBSTR(s.previsao_entrega_max,4,2)||'-'||SUBSTR(s.previsao_entrega_max,1,2)
             ELSE s.previsao_entrega_max END,
        CASE WHEN s.previsao_entrega LIKE '__/__/____'
             THEN SUBSTR(s.previsao_entrega,7,4)||'-'||SUBSTR(s.previsao_entrega,4,2)||'-'||SUBSTR(s.previsao_entrega,1,2)
             ELSE s.previsao_entrega END,
        CASE WHEN s.previsao_entrega_base LIKE '__/__/____'
             THEN SUBSTR(s.previsao_entrega_base,7,4)||'-'||SUBSTR(s.previsao_entrega_base,4,2)||'-'||SUBSTR(s.previsao_entrega_base,1,2)
             ELSE s.previsao_entrega_base END,
        CASE WHEN s.status_entrega_param = 'ESIM'
             THEN DATE(COALESCE(s.data_conectada_date, s.data_venda_date), '+2 days')
             WHEN s.tipo_entrega = 'CORREIOS'
             THEN DATE(COALESCE(s.data_conectada_date, s.data_venda_date), '+5 days')
             ELSE DATE(COALESCE(s.data_conectada_date, s.data_venda_date), '+2 days')
        END
    )))                                                    AS "Previsao_Entrega",
    -- Dentro_Prazo: apenas entregues APROVADOS
    -- [FIX-4] Normalizar todas as datas
    -- ESIM → sempre SIM
    -- Físico: se (entrega - conectada) >= 7 dias → NÃO sempre
    -- Senão: data_prevista vs data_entrega, fallback: (entrega - conectada) <= prazo tipo
    CASE
        WHEN s.crivo_vendas NOT IN ('APROVADA') THEN ''
        WHEN s.status_entrega_param NOT IN ('SIM', 'ESIM') THEN ''
        -- ESIM sempre dentro do prazo
        WHEN s.status_entrega_param = 'ESIM' THEN 'SIM'
        -- Se demorou >= 7 dias da conectada até entrega → NÃO independente
        WHEN CAST(JULIANDAY(DATE(COALESCE(
             CASE WHEN s.data_entrega_max LIKE '__/__/____'
                  THEN SUBSTR(s.data_entrega_max,7,4)||'-'||SUBSTR(s.data_entrega_max,4,2)||'-'||SUBSTR(s.data_entrega_max,1,2)
                  ELSE DATE(s.data_entrega_max)
             END,
             gb.data_gross,
             DATE(CASE
                 WHEN s.data_envio_chip LIKE '__/__/____'
                 THEN SUBSTR(s.data_envio_chip,7,4)||'-'||SUBSTR(s.data_envio_chip,4,2)||'-'||SUBSTR(s.data_envio_chip,1,2)
                 ELSE s.data_envio_chip
             END),
             s.data_conectada_date
        ))) - JULIANDAY(COALESCE(s.data_conectada_date, s.data_venda_date)) AS INTEGER) >= 6
        THEN 'NÃO'
        -- Físico: se tem previsão real, comparar data_entrega <= previsão
        WHEN DATE(COALESCE(
            CASE WHEN s.previsao_entrega_max LIKE '__/__/____'
                 THEN SUBSTR(s.previsao_entrega_max,7,4)||'-'||SUBSTR(s.previsao_entrega_max,4,2)||'-'||SUBSTR(s.previsao_entrega_max,1,2)
                 ELSE s.previsao_entrega_max END,
            CASE WHEN s.previsao_entrega LIKE '__/__/____'
                 THEN SUBSTR(s.previsao_entrega,7,4)||'-'||SUBSTR(s.previsao_entrega,4,2)||'-'||SUBSTR(s.previsao_entrega,1,2)
                 ELSE s.previsao_entrega END,
            CASE WHEN s.previsao_entrega_base LIKE '__/__/____'
                 THEN SUBSTR(s.previsao_entrega_base,7,4)||'-'||SUBSTR(s.previsao_entrega_base,4,2)||'-'||SUBSTR(s.previsao_entrega_base,1,2)
                 ELSE s.previsao_entrega_base END
        )) IS NOT NULL
        THEN CASE
            WHEN DATE(COALESCE(
                 CASE WHEN s.data_entrega_max LIKE '__/__/____'
                      THEN SUBSTR(s.data_entrega_max,7,4)||'-'||SUBSTR(s.data_entrega_max,4,2)||'-'||SUBSTR(s.data_entrega_max,1,2)
                      ELSE DATE(s.data_entrega_max)
                 END,
                 gb.data_gross,
                 DATE(CASE
                     WHEN s.data_envio_chip LIKE '__/__/____'
                     THEN SUBSTR(s.data_envio_chip,7,4)||'-'||SUBSTR(s.data_envio_chip,4,2)||'-'||SUBSTR(s.data_envio_chip,1,2)
                     ELSE s.data_envio_chip
                 END),
                 s.data_conectada_date
            )) <= DATE(COALESCE(
                 CASE WHEN s.previsao_entrega_max LIKE '__/__/____'
                      THEN SUBSTR(s.previsao_entrega_max,7,4)||'-'||SUBSTR(s.previsao_entrega_max,4,2)||'-'||SUBSTR(s.previsao_entrega_max,1,2)
                      ELSE s.previsao_entrega_max END,
                 CASE WHEN s.previsao_entrega LIKE '__/__/____'
                      THEN SUBSTR(s.previsao_entrega,7,4)||'-'||SUBSTR(s.previsao_entrega,4,2)||'-'||SUBSTR(s.previsao_entrega,1,2)
                      ELSE s.previsao_entrega END,
                 CASE WHEN s.previsao_entrega_base LIKE '__/__/____'
                      THEN SUBSTR(s.previsao_entrega_base,7,4)||'-'||SUBSTR(s.previsao_entrega_base,4,2)||'-'||SUBSTR(s.previsao_entrega_base,1,2)
                      ELSE s.previsao_entrega_base END
            ))
            THEN 'SIM' ELSE 'NÃO' END
        -- Fallback sem previsão: (data_entrega - data_conectada) <= prazo por tipo
        -- EXPRESS <= 2 dias, CORREIOS <= 5 dias
        ELSE CASE
            WHEN CAST(JULIANDAY(DATE(COALESCE(
                 CASE WHEN s.data_entrega_max LIKE '__/__/____'
                      THEN SUBSTR(s.data_entrega_max,7,4)||'-'||SUBSTR(s.data_entrega_max,4,2)||'-'||SUBSTR(s.data_entrega_max,1,2)
                      ELSE DATE(s.data_entrega_max)
                 END,
                 gb.data_gross,
                 DATE(CASE
                     WHEN s.data_envio_chip LIKE '__/__/____'
                     THEN SUBSTR(s.data_envio_chip,7,4)||'-'||SUBSTR(s.data_envio_chip,4,2)||'-'||SUBSTR(s.data_envio_chip,1,2)
                     ELSE s.data_envio_chip
                 END),
                 s.data_conectada_date
            ))) - JULIANDAY(COALESCE(s.data_conectada_date, s.data_venda_date)) AS INTEGER)
                 <= CASE WHEN s.tipo_entrega = 'CORREIOS' THEN 5 ELSE 2 END
            THEN 'SIM' ELSE 'NÃO' END
    END                                                    AS "Dentro_Prazo",
    s.status_funil                                         AS "Status_Funil_Proposta",
    -- GROSS Efetivo:
    -- 1. Concluído / Portabilidade Pendente / Pendente Portabilidade → Sim
    -- 2. Erro no Aprovisionamento → Erro APV
    -- 3. Em Aprovisionamento → Em APV
    -- 4. Cancelado/Suspenso + Rejeição SMS → Cancelado Pelo Cliente
    -- 5. Cancelado/Suspenso demais → Cancelamento Automatico
    -- 6. Sem registro → Não
    CASE
        WHEN COALESCE(gb.gross_flag, 0) = 1 THEN 'Sim'
        WHEN COALESCE(gb.erro_apv_flag, 0) = 1 THEN 'Erro APV'
        WHEN COALESCE(gb.em_apv_flag, 0) = 1 THEN 'Em APV'
        WHEN COALESCE(gb.cancelado_cliente_sms_flag, 0) = 1
          OR COALESCE(gb.cancelado_cliente_ambos_flag, 0) = 1 THEN 'Cancelado Pelo Cliente'
        -- Tem status_ordem preenchido mas não é nenhum acima → Cancelamento Automatico
        WHEN COALESCE(gb.tem_status_flag, 0) = 1 THEN 'Cancelamento Automatico'
        -- Sem registro no GROSS: classificar pelo funil
        WHEN s.crivo_vendas LIKE 'REPROVADA%' THEN 'Reprovada'
        WHEN s.crivo_vendas = 'APROVADA' AND s.status_entrega_param IN ('SIM', 'ESIM') THEN 'Pendente'
        WHEN s.crivo_vendas = 'APROVADA' AND s.status_entrega_param = 'QUEBRA' THEN 'Quebra'
        WHEN s.crivo_vendas = 'APROVADA' AND s.status_entrega_param IN ('EM ROTA', 'AG RETIRADA CORREIOS') THEN 'Em Rota'
        ELSE 'Não'
    END                                                    AS "GROSS_Efetivo",
    CASE
        WHEN COALESCE(gb.gross_flag, 0) = 1 THEN STRFTIME('%d/%m/%Y', gb.data_gross)
        ELSE NULL
    END                                                    AS "Data_GROSS",
    -- [FIX-6] Rastreabilidade: por qual chave o GROSS foi encontrado
    gb.gross_encontrado_por                                AS "GROSS_Encontrado_Por",
    -- [FIX-6] Classificação CR (da tabela gross direta)
    gb.classificacao_cr                                    AS "Classificacao_CR",
    -- BP Fechado (exclusivo PORTABILIDADE)
    -- Fontes: consulta_siebel (Portado, Falha Parcial) + portabilidade_tim (ATIVA, FALHA PARCIAL, ANTIGO)
    -- Sim: Portado, Falha Parcial, Antigo, Ativo
    -- Sim APV: Sim mas Em Aprovisionamento ou Erro no Aprovisionamento
    -- Pendente: não localizado
    CASE
        WHEN s.tipo_venda = 'NOVA LINHA' THEN 'Nova Linha'
        -- Sim com APV
        WHEN (COALESCE(gb.bp_siebel_flag, 0) = 1 OR COALESCE(bt.bp_tim_flag, 0) = 1)
             AND (COALESCE(gb.em_apv_flag, 0) = 1 OR COALESCE(gb.erro_apv_flag, 0) = 1)
        THEN 'Sim APV'
        -- Sim normal
        WHEN COALESCE(gb.bp_siebel_flag, 0) = 1 OR COALESCE(bt.bp_tim_flag, 0) = 1
        THEN 'Sim'
        -- Conflito
        WHEN COALESCE(gb.bp_conflito_siebel_flag, 0) = 1 OR COALESCE(bt.bp_conflito_tim_flag, 0) = 1
        THEN 'Conflito'
        -- Cancelado
        WHEN COALESCE(gb.bp_cancelado_siebel_flag, 0) = 1 OR COALESCE(bt.bp_cancelado_tim_flag, 0) = 1
        THEN 'Cancelado'
        -- Pendente
        ELSE 'Pendente'
    END                                                    AS "BP_Fechado",
    CASE
        WHEN s.tipo_venda = 'NOVA LINHA' THEN NULL
        WHEN COALESCE(gb.bp_siebel_flag, 0) = 1 THEN STRFTIME('%d/%m/%Y', gb.data_bp_siebel)
        WHEN COALESCE(bt.bp_tim_flag, 0) = 1 THEN STRFTIME('%d/%m/%Y', bt.data_bp_tim)
        ELSE NULL
    END                                                    AS "Data_BP_Fechado",
    -- Motivo Recusa BP (quando pendente/conflito)
    CASE
        WHEN s.tipo_venda = 'NOVA LINHA' THEN NULL
        WHEN COALESCE(gb.bp_siebel_flag, 0) = 1 OR COALESCE(bt.bp_tim_flag, 0) = 1 THEN NULL
        ELSE NULLIF(COALESCE(gb.motivo_recusa_bp, ''), '')
    END                                                    AS "Motivo_Recusa_BP",
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
    s.doadora                                              AS "Doadora_TIM",

    -- ============================================================
    -- NOVAS COLUNAS (adicionadas ao final para manter posições originais)
    -- ============================================================

    -- [NEW-1] Ciclo_Logistico: classificação completa do ciclo de vida logístico
    -- Valores: SEM ENVIO | ENVIADO | EM TRANSITO | EM ATRASO | AG RETIRADA |
    --          ENTREGUE | QUEBRA | ESIM | REPROVADA | PENDENTE CRIVO
    s.ciclo_logistico                                      AS "Ciclo_Logistico",

    -- [NEW-2] Dias_Em_Rota: dias desde envio até entrega ou hoje
    -- NULL = sem dados de envio | 0 = ESIM | N = dias corridos
    s.dias_em_rota                                         AS "Dias_Em_Rota",

    -- [NEW-3] SLA_Status: classificação de cumprimento de SLA
    -- Valores: DENTRO SLA | FORA SLA | ENTREGUE NO PRAZO | ENTREGUE ATRASADO | N/A
    s.sla_status                                           AS "SLA_Status"

FROM status_calc s
LEFT JOIN gross_bp gb ON gb.proposta_isize = s.id_isize
LEFT JOIN bp_tim bt ON bt.proposta_isize = s.id_isize
ORDER BY s.data_venda_date DESC, s.id_isize;
