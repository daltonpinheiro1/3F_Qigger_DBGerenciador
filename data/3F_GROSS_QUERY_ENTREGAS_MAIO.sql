/*
   QUERY: ENTREGAS vs GROSS vs CONSULTA SIEBEL vs PORTABILIDADE TIM
   Base: portabilidade_v2.db
   Revisao DBA: unicidade, ORDER BY id DESC, datas normalizadas dd/mm/aaaa

   Cascata GROSS: proposta_isize > acesso=portado > acesso=linha > acesso=tim > iccid
   Cascata SIEBEL: proposta_isize > numero_acesso=portado > numero_acesso=linha > cpf
   Cascata TIM: proposta_isize > acesso=portado > acesso=linha > cpf

   FILTRO DE DATA: altere as datas em dd/mm/aaaa
*/

WITH filtro_periodo AS (
    SELECT
        SUBSTR('01/05/2026',7,4)||'-'||SUBSTR('01/05/2026',4,2)||'-'||SUBSTR('01/05/2026',1,2) AS data_inicio,
        SUBSTR('31/05/2026',7,4)||'-'||SUBSTR('31/05/2026',4,2)||'-'||SUBSTR('31/05/2026',1,2) AS data_fim
),

entregas_periodo AS (
    SELECT
        l.proposta_isize, l.nu_pedido, l.rastreio, l.iccid,
        l.status AS status_logistica, l.data_entrega, l.transportadora,
        ROW_NUMBER() OVER (PARTITION BY l.proposta_isize ORDER BY
            DATE(CASE WHEN l.data_entrega LIKE '__/__/____'
                 THEN SUBSTR(l.data_entrega,7,4)||'-'||SUBSTR(l.data_entrega,4,2)||'-'||SUBSTR(l.data_entrega,1,2)
                 ELSE l.data_entrega END) DESC,
            l.id DESC
        ) AS rn
    FROM logistica l, filtro_periodo f
    WHERE UPPER(l.status) LIKE '%ENTREGUE%'
      AND DATE(CASE
            WHEN l.data_entrega LIKE '__/__/____'
            THEN SUBSTR(l.data_entrega,7,4)||'-'||SUBSTR(l.data_entrega,4,2)||'-'||SUBSTR(l.data_entrega,1,2)
            ELSE l.data_entrega
          END) BETWEEN f.data_inicio AND f.data_fim
),

prop_atual AS (
    SELECT p.* FROM propostas p
    INNER JOIN (SELECT proposta_isize, MAX(versao) AS mv FROM propostas GROUP BY proposta_isize) pm
        ON p.proposta_isize = pm.proposta_isize AND p.versao = pm.mv
),

port_atual AS (
    SELECT pt.* FROM portabilidade pt
    INNER JOIN (SELECT proposta_isize, MAX(versao) AS mv FROM portabilidade GROUP BY proposta_isize) pm
        ON pt.proposta_isize = pm.proposta_isize AND pt.versao = pm.mv
),

sv_atual AS (
    SELECT s.* FROM status_venda s
    INNER JOIN (SELECT proposta_isize, MAX(versao) AS mv FROM status_venda GROUP BY proposta_isize) sm
        ON s.proposta_isize = sm.proposta_isize AND s.versao = sm.mv
),

tim_por_isize AS (
    SELECT t.* FROM portabilidade_tim t
    INNER JOIN (SELECT proposta_isize, MAX(id) AS max_id FROM portabilidade_tim GROUP BY proposta_isize) tm
        ON t.id = tm.max_id
),

ultima_data_gross AS (
    SELECT MAX(
        CASE WHEN data_gross LIKE '__/__/____'
             THEN SUBSTR(data_gross,7,4)||'-'||SUBSTR(data_gross,4,2)||'-'||SUBSTR(data_gross,1,2)
             ELSE data_gross END
    ) AS max_data_iso
    FROM gross WHERE data_gross IS NOT NULL AND TRIM(data_gross) != ''
),

gross_por_isize AS (
    SELECT proposta_isize, data_gross, classificacao_cr, iccid AS gross_iccid
    FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY proposta_isize ORDER BY versao DESC, id DESC) AS rn
          FROM gross WHERE proposta_isize IS NOT NULL AND TRIM(proposta_isize) != '') WHERE rn = 1
),

gross_por_acesso AS (
    SELECT acesso, data_gross, classificacao_cr, iccid AS gross_iccid
    FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY acesso ORDER BY versao DESC, id DESC) AS rn
          FROM gross WHERE acesso IS NOT NULL AND TRIM(acesso) != '') WHERE rn = 1
),

gross_por_iccid AS (
    SELECT iccid, data_gross, classificacao_cr
    FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY iccid ORDER BY versao DESC, id DESC) AS rn
          FROM gross WHERE iccid IS NOT NULL AND TRIM(iccid) != '') WHERE rn = 1
),

siebel_por_isize AS (
    SELECT cs.* FROM consulta_siebel cs
    INNER JOIN (SELECT proposta_isize, MAX(id) AS max_id FROM consulta_siebel GROUP BY proposta_isize) cm
        ON cs.id = cm.max_id
),

siebel_por_acesso AS (
    SELECT cs.* FROM consulta_siebel cs
    INNER JOIN (SELECT numero_acesso, MAX(id) AS max_id FROM consulta_siebel
                WHERE numero_acesso IS NOT NULL AND TRIM(numero_acesso) != '' GROUP BY numero_acesso) cm
        ON cs.id = cm.max_id
),

siebel_por_cpf AS (
    SELECT cs.* FROM consulta_siebel cs
    INNER JOIN (SELECT cpf, MAX(id) AS max_id FROM consulta_siebel
                WHERE cpf IS NOT NULL AND TRIM(cpf) != '' GROUP BY cpf) cm
        ON cs.id = cm.max_id
),

tim_por_acesso AS (
    SELECT t.* FROM portabilidade_tim t
    INNER JOIN (SELECT acesso, MAX(id) AS max_id FROM portabilidade_tim
                WHERE acesso IS NOT NULL AND TRIM(acesso) != '' GROUP BY acesso) tm
        ON t.id = tm.max_id
),

tim_por_cpf AS (
    SELECT t.* FROM portabilidade_tim t
    INNER JOIN (SELECT cpf_cnpj, MAX(id) AS max_id FROM portabilidade_tim
                WHERE cpf_cnpj IS NOT NULL AND TRIM(cpf_cnpj) != '' GROUP BY cpf_cnpj) tm
        ON t.id = tm.max_id
),

base AS (
    SELECT
        e.proposta_isize, e.nu_pedido, e.rastreio, e.iccid,
        e.status_logistica, e.data_entrega,
        DATE(CASE WHEN e.data_entrega LIKE '__/__/____'
             THEN SUBSTR(e.data_entrega,7,4)||'-'||SUBSTR(e.data_entrega,4,2)||'-'||SUBSTR(e.data_entrega,1,2)
             ELSE e.data_entrega END) AS data_entrega_iso,
        pr.cpf, pr.data_venda, pr.plano,
        po.telefone_portabilidade, po.numero_linha,
        cl.nome_cliente,
        sv.status_venda, sv.motivo_rejeicao_cancelamento,
        tim_base.acesso_temporario
    FROM entregas_periodo e
    LEFT JOIN prop_atual pr ON pr.proposta_isize = e.proposta_isize
    LEFT JOIN port_atual po ON po.proposta_isize = e.proposta_isize
    LEFT JOIN clientes cl ON cl.cpf = pr.cpf
        AND cl.versao = (SELECT MAX(versao) FROM clientes WHERE cpf = pr.cpf)
    LEFT JOIN sv_atual sv ON sv.proposta_isize = e.proposta_isize
    LEFT JOIN tim_por_isize tim_base ON tim_base.proposta_isize = e.proposta_isize
    WHERE e.rn = 1
)

SELECT
    b.proposta_isize AS "ID_ISIZE",
    b.cpf AS "CPF",
    b.nome_cliente AS "Nome_Cliente",
    COALESCE(b.telefone_portabilidade, '') AS "Numero_Portado",
    COALESCE(b.numero_linha, '') AS "Numero_Linha",
    COALESCE(b.iccid, '') AS "ICCID",

    CASE WHEN b.data_venda LIKE '____-__-__%'
         THEN SUBSTR(b.data_venda, 9, 2)||'/'||SUBSTR(b.data_venda, 6, 2)||'/'||SUBSTR(b.data_venda, 1, 4)
         ELSE COALESCE(b.data_venda, '') END AS "Data_Venda",

    b.plano AS "Plano",
    UPPER(TRIM(b.status_logistica)) AS "Status_Logistica",
    COALESCE(b.rastreio, '') AS "Rastreio",

    CASE WHEN b.data_entrega LIKE '__/__/____' THEN b.data_entrega
         WHEN b.data_entrega LIKE '____-__-__%'
         THEN SUBSTR(b.data_entrega, 9, 2)||'/'||SUBSTR(b.data_entrega, 6, 2)||'/'||SUBSTR(b.data_entrega, 1, 4)
         ELSE COALESCE(b.data_entrega, '') END AS "Data_Entrega",

    COALESCE(b.nu_pedido, '') AS "Nu_Pedido",

    CASE
        WHEN b.data_venda >= (SELECT data_inicio FROM filtro_periodo) THEN 'M0'
        WHEN b.data_venda < (SELECT data_inicio FROM filtro_periodo) THEN 'M-1'
        ELSE 'N/A'
    END AS "SAFRA",

    CASE
        WHEN COALESCE(b.telefone_portabilidade, '') != '' THEN 'PORTABILIDADE'
        ELSE 'NOVA LINHA'
    END AS "TIPO",

    CASE WHEN COALESCE(g1.data_gross, g2.data_gross, g3.data_gross, g4.data_gross, g5.data_gross) LIKE '__/__/____'
         THEN COALESCE(g1.data_gross, g2.data_gross, g3.data_gross, g4.data_gross, g5.data_gross)
         WHEN COALESCE(g1.data_gross, g2.data_gross, g3.data_gross, g4.data_gross, g5.data_gross) LIKE '____-__-__%'
         THEN SUBSTR(COALESCE(g1.data_gross, g2.data_gross, g3.data_gross, g4.data_gross, g5.data_gross),9,2)||'/'||SUBSTR(COALESCE(g1.data_gross, g2.data_gross, g3.data_gross, g4.data_gross, g5.data_gross),6,2)||'/'||SUBSTR(COALESCE(g1.data_gross, g2.data_gross, g3.data_gross, g4.data_gross, g5.data_gross),1,4)
         ELSE COALESCE(g1.data_gross, g2.data_gross, g3.data_gross, g4.data_gross, g5.data_gross, '')
    END AS "Data_GROSS",

    UPPER(TRIM(COALESCE(g1.classificacao_cr, g2.classificacao_cr, g3.classificacao_cr, g4.classificacao_cr, g5.classificacao_cr, ''))) AS "Classificacao_CR",

    CASE
        WHEN COALESCE(g1.data_gross, g2.data_gross, g3.data_gross, g4.data_gross, g5.data_gross) IS NOT NULL THEN 'SIM'
        WHEN UPPER(TRIM(COALESCE(s1.status_ordem, s2.status_ordem, s3.status_ordem, s4.status_ordem, '')))
             IN ('CONCLUÍDO','CONCLUIDO','PORTABILIDADE PENDENTE','PENDENTE PORTABILIDADE') THEN 'SIM (SIEBEL)'
        ELSE 'NAO'
    END AS "CONSTA_NO_GROSS",

    CASE
        WHEN g1.proposta_isize IS NOT NULL THEN 'gross.proposta_isize'
        WHEN g2.acesso IS NOT NULL THEN 'gross.acesso=portado'
        WHEN g3.acesso IS NOT NULL THEN 'gross.acesso=linha'
        WHEN g4.acesso IS NOT NULL THEN 'gross.acesso=tim'
        WHEN g5.iccid IS NOT NULL THEN 'gross.iccid'
        WHEN UPPER(TRIM(COALESCE(s1.status_ordem,''))) IN ('CONCLUÍDO','CONCLUIDO','PORTABILIDADE PENDENTE','PENDENTE PORTABILIDADE') THEN 'siebel.proposta_isize'
        WHEN UPPER(TRIM(COALESCE(s2.status_ordem,''))) IN ('CONCLUÍDO','CONCLUIDO','PORTABILIDADE PENDENTE','PENDENTE PORTABILIDADE') THEN 'siebel.acesso=portado'
        WHEN UPPER(TRIM(COALESCE(s3.status_ordem,''))) IN ('CONCLUÍDO','CONCLUIDO','PORTABILIDADE PENDENTE','PENDENTE PORTABILIDADE') THEN 'siebel.acesso=linha'
        WHEN UPPER(TRIM(COALESCE(s4.status_ordem,''))) IN ('CONCLUÍDO','CONCLUIDO','PORTABILIDADE PENDENTE','PENDENTE PORTABILIDADE') THEN 'siebel.cpf'
        ELSE ''
    END AS "GROSS_Encontrado_Por",

    UPPER(TRIM(COALESCE(s1.status_bilhete, s2.status_bilhete, s3.status_bilhete, s4.status_bilhete, ''))) AS "Status_Bilhete",
    UPPER(TRIM(COALESCE(s1.status_ordem, s2.status_ordem, s3.status_ordem, s4.status_ordem, ''))) AS "Status_Ordem",
    UPPER(TRIM(COALESCE(s1.motivo_recusa, s2.motivo_recusa, s3.motivo_recusa, s4.motivo_recusa, ''))) AS "Motivo_Recusa",
    UPPER(TRIM(COALESCE(s1.motivo_cancelamento, s2.motivo_cancelamento, s3.motivo_cancelamento, s4.motivo_cancelamento, ''))) AS "Motivo_Cancelamento",
    UPPER(TRIM(COALESCE(s1.novo_status_bilhete, s2.novo_status_bilhete, s3.novo_status_bilhete, s4.novo_status_bilhete, ''))) AS "Novo_Status_Bilhete",

    CASE
        WHEN s1.proposta_isize IS NOT NULL THEN 'proposta_isize'
        WHEN s2.numero_acesso IS NOT NULL THEN 'acesso=portado'
        WHEN s3.numero_acesso IS NOT NULL THEN 'acesso=linha'
        WHEN s4.cpf IS NOT NULL THEN 'cpf'
        ELSE ''
    END AS "SIEBEL_Encontrado_Por",

    UPPER(TRIM(COALESCE(t1.status, t2.status, t3.status, t4.status, ''))) AS "Status_TIM",

    CASE
        WHEN t1.proposta_isize IS NOT NULL THEN 'proposta_isize'
        WHEN t2.acesso IS NOT NULL THEN 'acesso=portado'
        WHEN t3.acesso IS NOT NULL THEN 'acesso=linha'
        WHEN t4.cpf_cnpj IS NOT NULL THEN 'cpf'
        ELSE ''
    END AS "TIM_Encontrado_Por",

    UPPER(TRIM(COALESCE(t1.motivo_conflito, t2.motivo_conflito, t3.motivo_conflito, t4.motivo_conflito, ''))) AS "Motivo_Conflito_TIM",
    UPPER(TRIM(COALESCE(t1.motivo_cancelamento, t2.motivo_cancelamento, t3.motivo_cancelamento, t4.motivo_cancelamento, ''))) AS "Motivo_Cancelamento_TIM",

    UPPER(TRIM(COALESCE(
        COALESCE(t1.status, t2.status, t3.status, t4.status),
        COALESCE(s1.novo_status_bilhete, s2.novo_status_bilhete, s3.novo_status_bilhete, s4.novo_status_bilhete),
        COALESCE(s1.status_bilhete, s2.status_bilhete, s3.status_bilhete, s4.status_bilhete),
        ''
    ))) AS "STATUS_BP",

    CASE
        WHEN COALESCE(s1.status_ordem, s2.status_ordem, s3.status_ordem, s4.status_ordem) IS NOT NULL
             AND TRIM(COALESCE(s1.status_ordem, s2.status_ordem, s3.status_ordem, s4.status_ordem)) != '' THEN 'SIEBEL'
        WHEN COALESCE(t1.status, t2.status, t3.status, t4.status) IS NOT NULL THEN 'TIM'
        WHEN COALESCE(g1.data_gross, g2.data_gross, g3.data_gross, g4.data_gross, g5.data_gross) IS NOT NULL THEN 'GROSS'
        ELSE 'SEM DADOS'
    END AS "Fonte_Status",

    UPPER(TRIM(COALESCE(b.status_venda, ''))) AS "Status_Venda",
    UPPER(TRIM(COALESCE(b.motivo_rejeicao_cancelamento, ''))) AS "Situacao_Venda",

    CASE
        WHEN COALESCE(g1.data_gross, g2.data_gross, g3.data_gross, g4.data_gross, g5.data_gross) IS NOT NULL
            THEN 'ENTREGUE + GROSS LOCALIZADO'
        WHEN UPPER(TRIM(COALESCE(s1.status_ordem, s2.status_ordem, s3.status_ordem, s4.status_ordem, '')))
             IN ('CONCLUÍDO','CONCLUIDO','PORTABILIDADE PENDENTE','PENDENTE PORTABILIDADE')
            THEN 'ENTREGUE + GROSS VIA SIEBEL'
        WHEN b.data_entrega_iso > (SELECT max_data_iso FROM ultima_data_gross)
             AND UPPER(COALESCE(t1.status, t2.status, t3.status, t4.status, '')) != 'ATIVA'
            THEN 'ENTREGUE - GROSS PENDENTE (ARQUIVO)'
        WHEN COALESCE(t1.status, t2.status, t3.status, t4.status) IS NOT NULL
             OR TRIM(COALESCE(s1.status_bilhete, s2.status_bilhete, s3.status_bilhete, s4.status_bilhete, '')) != ''
            THEN 'ENTREGUE + GROSS PARCIAL'
        ELSE 'ENTREGUE + GROSS NAO LOCALIZADO'
    END AS "RESULTADO_ANALISE"

FROM base b

LEFT JOIN gross_por_isize g1 ON g1.proposta_isize = b.proposta_isize
LEFT JOIN gross_por_acesso g2 ON g2.acesso = b.telefone_portabilidade
    AND g1.proposta_isize IS NULL
    AND b.telefone_portabilidade IS NOT NULL AND TRIM(b.telefone_portabilidade) != ''
LEFT JOIN gross_por_acesso g3 ON g3.acesso = b.numero_linha
    AND g1.proposta_isize IS NULL AND g2.acesso IS NULL
    AND b.numero_linha IS NOT NULL AND TRIM(b.numero_linha) != ''
LEFT JOIN gross_por_acesso g4 ON g4.acesso = b.acesso_temporario
    AND g1.proposta_isize IS NULL AND g2.acesso IS NULL AND g3.acesso IS NULL
    AND b.acesso_temporario IS NOT NULL AND TRIM(b.acesso_temporario) != ''
LEFT JOIN gross_por_iccid g5 ON g5.iccid = b.iccid
    AND g1.proposta_isize IS NULL AND g2.acesso IS NULL AND g3.acesso IS NULL AND g4.acesso IS NULL
    AND b.iccid IS NOT NULL AND TRIM(b.iccid) != ''

LEFT JOIN siebel_por_isize s1 ON s1.proposta_isize = b.proposta_isize
LEFT JOIN siebel_por_acesso s2 ON s2.numero_acesso = b.telefone_portabilidade
    AND s1.proposta_isize IS NULL
    AND b.telefone_portabilidade IS NOT NULL AND TRIM(b.telefone_portabilidade) != ''
LEFT JOIN siebel_por_acesso s3 ON s3.numero_acesso = b.numero_linha
    AND s1.proposta_isize IS NULL AND s2.numero_acesso IS NULL
    AND b.numero_linha IS NOT NULL AND TRIM(b.numero_linha) != ''
LEFT JOIN siebel_por_cpf s4 ON s4.cpf = b.cpf
    AND s1.proposta_isize IS NULL AND s2.numero_acesso IS NULL AND s3.numero_acesso IS NULL
    AND b.cpf IS NOT NULL AND TRIM(b.cpf) != ''

LEFT JOIN tim_por_isize t1 ON t1.proposta_isize = b.proposta_isize
LEFT JOIN tim_por_acesso t2 ON t2.acesso = b.telefone_portabilidade
    AND t1.proposta_isize IS NULL
    AND b.telefone_portabilidade IS NOT NULL AND TRIM(b.telefone_portabilidade) != ''
LEFT JOIN tim_por_acesso t3 ON t3.acesso = b.numero_linha
    AND t1.proposta_isize IS NULL AND t2.acesso IS NULL
    AND b.numero_linha IS NOT NULL AND TRIM(b.numero_linha) != ''
LEFT JOIN tim_por_cpf t4 ON t4.cpf_cnpj = b.cpf
    AND t1.proposta_isize IS NULL AND t2.acesso IS NULL AND t3.acesso IS NULL
    AND b.cpf IS NOT NULL AND TRIM(b.cpf) != ''

ORDER BY
    CASE WHEN COALESCE(g1.data_gross, g2.data_gross, g3.data_gross, g4.data_gross, g5.data_gross) IS NOT NULL THEN 0 ELSE 1 END,
    b.data_entrega_iso DESC,
    b.proposta_isize;
