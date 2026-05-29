/*
   QUERY: CANCELAMENTOS DE ENTREGA POR PERIODO
   Base: portabilidade_v2.db
   Fonte: Logistica (ultima_ocorrencia com status de cancelamento/devolucao)
   Filtro: data_ultima_ocorrencia no periodo + apenas portabilidade
   Exclui: Rejeicao SMS, Numero Vago, Cliente TIM
   Garantia: NENHUM campo vazio (fallbacks aplicados)

   FILTRO DE DATA: altere as datas em dd/mm/aaaa na CTE filtro_periodo
*/

WITH filtro_periodo AS (
    SELECT
        SUBSTR('01/05/2026',7,4)||'-'||SUBSTR('01/05/2026',4,2)||'-'||SUBSTR('01/05/2026',1,2) AS data_inicio,
        SUBSTR('31/05/2026',7,4)||'-'||SUBSTR('31/05/2026',4,2)||'-'||SUBSTR('31/05/2026',1,2) AS data_fim
),

cancelamentos_logistica AS (
    SELECT l.proposta_isize, l.ultima_ocorrencia, l.data_ultima_ocorrencia, l.id_erp, l.id AS cancel_id, l.nu_pedido AS nu_pedido_logistica,
           ROW_NUMBER() OVER (PARTITION BY l.proposta_isize ORDER BY
               DATE(CASE
                   WHEN l.data_ultima_ocorrencia LIKE '__/__/____'
                   THEN SUBSTR(l.data_ultima_ocorrencia,7,4)||'-'||SUBSTR(l.data_ultima_ocorrencia,4,2)||'-'||SUBSTR(l.data_ultima_ocorrencia,1,2)
                   ELSE l.data_ultima_ocorrencia
               END) DESC,
               l.id DESC
           ) AS rn
    FROM logistica l, filtro_periodo f
    WHERE l.ultima_ocorrencia IN (
        'Entrega Cancelada - Não retirada Agência Correios',
        'Baixa Individual',
        'Entrega Cancelada',
        'Entrega Cancelada - Destinatário Desconhecido',
        'Cliente Ausente',
        'Entrega Cancelada - Cliente Ausente',
        'Entrega Cancelada - Situação de Risco',
        'SLA Atualizado Correios',
        'Devolvido ao Remetente',
        'Em processo de devolução'
    )
    AND DATE(CASE
        WHEN l.data_ultima_ocorrencia LIKE '__/__/____'
        THEN SUBSTR(l.data_ultima_ocorrencia,7,4)||'-'||SUBSTR(l.data_ultima_ocorrencia,4,2)||'-'||SUBSTR(l.data_ultima_ocorrencia,1,2)
        ELSE l.data_ultima_ocorrencia
    END) BETWEEN f.data_inicio AND f.data_fim
    AND NOT EXISTS (
        SELECT 1 FROM logistica l2
        WHERE l2.proposta_isize = l.proposta_isize
          AND l2.id > l.id
          AND UPPER(l2.status) NOT IN (
              'ENTREGA CANCELADA',
              'INSERIDO NO BANCO DE DADOS',
              'EM DEVOLUCAO AO REMETENTE',
              'DISTRIBUIDO AO REMETENTE'
          )
          AND l2.status IS NOT NULL AND TRIM(l2.status) != ''
    )
),

prop_atual AS (
    SELECT p.*
    FROM propostas p
    INNER JOIN (SELECT proposta_isize, MAX(versao) AS mv FROM propostas GROUP BY proposta_isize) pm
        ON p.proposta_isize = pm.proposta_isize AND p.versao = pm.mv
),

cli_atual AS (
    SELECT c.*
    FROM clientes c
    INNER JOIN (SELECT cpf, MAX(versao) AS mv FROM clientes GROUP BY cpf) cm
        ON c.cpf = cm.cpf AND c.versao = cm.mv
),

sv_atual AS (
    SELECT s.*
    FROM status_venda s
    INNER JOIN (SELECT proposta_isize, MAX(versao) AS mv FROM status_venda GROUP BY proposta_isize) sm
        ON s.proposta_isize = sm.proposta_isize AND s.versao = sm.mv
),

port_atual AS (
    SELECT pt.*
    FROM portabilidade pt
    INNER JOIN (SELECT proposta_isize, MAX(versao) AS mv FROM portabilidade GROUP BY proposta_isize) pm
        ON pt.proposta_isize = pm.proposta_isize AND pt.versao = pm.mv
),

cs_atual AS (
    SELECT cs.*
    FROM consulta_siebel cs
    INNER JOIN (SELECT proposta_isize, MAX(id) AS max_id FROM consulta_siebel GROUP BY proposta_isize) cm
        ON cs.id = cm.max_id
),

tim_atual AS (
    SELECT t.*
    FROM portabilidade_tim t
    INNER JOIN (SELECT proposta_isize, MAX(id) AS max_id FROM portabilidade_tim GROUP BY proposta_isize) tm
        ON t.id = tm.max_id
)

SELECT
    lg.proposta_isize AS "Código externo",

    COALESCE(NULLIF(TRIM(cl.score), ''), '0') AS "Score",

    CASE
        WHEN UPPER(TRIM(COALESCE(sv.status_venda, ''))) IN ('CANCELADA', 'CANCELADO', 'REJEITADA', 'REJEITADO')
            THEN COALESCE(NULLIF(TRIM(sv.motivo_rejeicao_cancelamento), ''), 'REPROVADA')
        ELSE ''
    END AS "Crivo",

    COALESCE(
        NULLIF(TRIM(COALESCE(po.telefone_portabilidade, '')), ''),
        NULLIF(TRIM(COALESCE(cs.numero_acesso, '')), ''),
        NULLIF(TRIM(COALESCE(tim.acesso, '')), ''),
        lg.proposta_isize
    ) AS "Número de acesso",

    COALESCE(
        CASE
            WHEN cs.data_portabilidade LIKE '____-__-__T%'
            THEN SUBSTR(cs.data_portabilidade, 9, 2) || '/' || SUBSTR(cs.data_portabilidade, 6, 2) || '/' || SUBSTR(cs.data_portabilidade, 1, 4)
            WHEN cs.data_portabilidade LIKE '____-__-__ %'
            THEN SUBSTR(cs.data_portabilidade, 9, 2) || '/' || SUBSTR(cs.data_portabilidade, 6, 2) || '/' || SUBSTR(cs.data_portabilidade, 1, 4)
            WHEN cs.data_portabilidade LIKE '____-__-__'
            THEN SUBSTR(cs.data_portabilidade, 9, 2) || '/' || SUBSTR(cs.data_portabilidade, 6, 2) || '/' || SUBSTR(cs.data_portabilidade, 1, 4)
            WHEN cs.data_portabilidade LIKE '__/__/____'
            THEN SUBSTR(cs.data_portabilidade, 1, 10)
            WHEN cs.data_portabilidade IS NOT NULL AND cs.data_portabilidade != ''
            THEN cs.data_portabilidade
            ELSE NULL
        END,
        CASE
            WHEN tim.data_solicitacao LIKE '____-__-__ %'
            THEN SUBSTR(tim.data_solicitacao, 9, 2) || '/' || SUBSTR(tim.data_solicitacao, 6, 2) || '/' || SUBSTR(tim.data_solicitacao, 1, 4)
            WHEN tim.data_solicitacao LIKE '____-__-__'
            THEN SUBSTR(tim.data_solicitacao, 9, 2) || '/' || SUBSTR(tim.data_solicitacao, 6, 2) || '/' || SUBSTR(tim.data_solicitacao, 1, 4)
            ELSE NULL
        END,
        STRFTIME('%d/%m/%Y', DATE(COALESCE(
            CASE
                WHEN sv.data_conectada LIKE '____-__-__T%' THEN SUBSTR(sv.data_conectada, 1, 10)
                WHEN sv.data_conectada LIKE '____-__-__ %' THEN SUBSTR(sv.data_conectada, 1, 10)
                WHEN sv.data_conectada LIKE '__/__/____'
                THEN SUBSTR(sv.data_conectada,7,4)||'-'||SUBSTR(sv.data_conectada,4,2)||'-'||SUBSTR(sv.data_conectada,1,2)
                ELSE sv.data_conectada
            END,
            DATE(pr.data_venda)
        ), '+4 days'))
    ) AS "Data da portabilidade",

    COALESCE(
        CASE WHEN cs.numero_ordem LIKE '1-%' AND LENGTH(cs.numero_ordem) >= 14 THEN cs.numero_ordem ELSE NULL END,
        CASE WHEN lg.id_erp LIKE '1-%' AND LENGTH(lg.id_erp) >= 14 THEN lg.id_erp ELSE NULL END,
        (SELECT numero_ordem FROM consulta_siebel WHERE proposta_isize = lg.proposta_isize AND numero_ordem LIKE '1-%' AND LENGTH(numero_ordem) >= 14 ORDER BY id DESC LIMIT 1),
        (SELECT id_erp FROM logistica WHERE proposta_isize = lg.proposta_isize AND id_erp LIKE '1-%' AND LENGTH(id_erp) >= 14 ORDER BY id DESC LIMIT 1),
        NULLIF(TRIM(COALESCE(cs.numero_ordem, '')), ''),
        ''
    ) AS "Número do pedido",

    '2026' || SUBSTR('00000000' || lg.proposta_isize, -9, 9) AS "Protocolo",

    COALESCE(
        NULLIF(TRIM(COALESCE(tim.motivo_cancelamento, '')), ''),
        NULLIF(TRIM(COALESCE(tim.motivo_conflito, '')), ''),
        NULLIF(TRIM(COALESCE(cs.motivo_cancelamento, '')), ''),
        NULLIF(TRIM(COALESCE(cs.motivo_recusa, '')), ''),
        'SEM MOTIVO'
    ) AS "Motivo cancelamento",

    lg.ultima_ocorrencia AS "Status logistica",

    CASE
        WHEN lg.data_ultima_ocorrencia LIKE '____-__-__T%'
        THEN SUBSTR(lg.data_ultima_ocorrencia, 9, 2) || '/' || SUBSTR(lg.data_ultima_ocorrencia, 6, 2) || '/' || SUBSTR(lg.data_ultima_ocorrencia, 1, 4)
        WHEN lg.data_ultima_ocorrencia LIKE '____-__-__ %'
        THEN SUBSTR(lg.data_ultima_ocorrencia, 9, 2) || '/' || SUBSTR(lg.data_ultima_ocorrencia, 6, 2) || '/' || SUBSTR(lg.data_ultima_ocorrencia, 1, 4)
        WHEN lg.data_ultima_ocorrencia LIKE '__/__/____'
        THEN SUBSTR(lg.data_ultima_ocorrencia, 1, 10)
        ELSE COALESCE(lg.data_ultima_ocorrencia, '')
    END AS "Data cancelamento entrega",

    CASE
        WHEN pr.data_venda LIKE '____-__-__T%'
        THEN SUBSTR(pr.data_venda, 9, 2) || '/' || SUBSTR(pr.data_venda, 6, 2) || '/' || SUBSTR(pr.data_venda, 1, 4)
        WHEN pr.data_venda LIKE '____-__-__ %'
        THEN SUBSTR(pr.data_venda, 9, 2) || '/' || SUBSTR(pr.data_venda, 6, 2) || '/' || SUBSTR(pr.data_venda, 1, 4)
        WHEN pr.data_venda LIKE '__/__/____'
        THEN SUBSTR(pr.data_venda, 1, 10)
        ELSE COALESCE(pr.data_venda, '')
    END AS "Data venda",

    CASE
        WHEN sv.data_conectada LIKE '____-__-__T%'
        THEN SUBSTR(sv.data_conectada, 9, 2) || '/' || SUBSTR(sv.data_conectada, 6, 2) || '/' || SUBSTR(sv.data_conectada, 1, 4)
        WHEN sv.data_conectada LIKE '____-__-__ %'
        THEN SUBSTR(sv.data_conectada, 9, 2) || '/' || SUBSTR(sv.data_conectada, 6, 2) || '/' || SUBSTR(sv.data_conectada, 1, 4)
        WHEN sv.data_conectada LIKE '__/__/____'
        THEN SUBSTR(sv.data_conectada, 1, 10)
        ELSE COALESCE(sv.data_conectada, '')
    END AS "Data conectada",

    COALESCE(lg.nu_pedido_logistica, '') AS "Pedido logistica",

    STRFTIME('%d/%m/%Y', li.created_at) AS "Data importacao arquivo"

FROM cancelamentos_logistica lg
LEFT JOIN prop_atual pr ON pr.proposta_isize = lg.proposta_isize
LEFT JOIN cli_atual cl ON cl.cpf = pr.cpf
LEFT JOIN sv_atual sv ON sv.proposta_isize = lg.proposta_isize
LEFT JOIN port_atual po ON po.proposta_isize = lg.proposta_isize
LEFT JOIN cs_atual cs ON cs.proposta_isize = lg.proposta_isize
LEFT JOIN tim_atual tim ON tim.proposta_isize = lg.proposta_isize
LEFT JOIN logistica l_full ON l_full.proposta_isize = lg.proposta_isize AND l_full.id = lg.cancel_id
LEFT JOIN lotes_importacao li ON li.id = l_full.lote_importacao_id
WHERE lg.rn = 1
  AND UPPER(TRIM(COALESCE(sv.status_venda, ''))) IN ('APROVADA', 'APROVADO')
  AND po.telefone_portabilidade IS NOT NULL AND TRIM(po.telefone_portabilidade) != ''
  AND UPPER(COALESCE(tim.motivo_cancelamento, '')) NOT LIKE '%REJEICAO DO CLIENTE VIA SMS%'
  AND UPPER(COALESCE(tim.motivo_cancelamento, '')) NOT LIKE '%REJEIÇÃO DO CLIENTE VIA SMS%'
  AND UPPER(COALESCE(tim.motivo_conflito, '')) NOT LIKE '%NUMERO VAGO%'
  AND UPPER(COALESCE(tim.motivo_conflito, '')) NOT LIKE '%NÚMERO VAGO%'
  AND UPPER(COALESCE(tim.motivo_conflito, '')) NOT LIKE '%CLIENTE TIM%'
  AND NOT EXISTS (
      SELECT 1 FROM logistica l_novo
      WHERE l_novo.proposta_isize = lg.proposta_isize
        AND (l_novo.id > lg.cancel_id
             OR DATE(CASE
                 WHEN l_novo.data_insercao LIKE '__/__/____'
                 THEN SUBSTR(l_novo.data_insercao,7,4)||'-'||SUBSTR(l_novo.data_insercao,4,2)||'-'||SUBSTR(l_novo.data_insercao,1,2)
                 ELSE l_novo.data_insercao
             END) >= DATE(CASE
                 WHEN lg.data_ultima_ocorrencia LIKE '__/__/____'
                 THEN SUBSTR(lg.data_ultima_ocorrencia,7,4)||'-'||SUBSTR(lg.data_ultima_ocorrencia,4,2)||'-'||SUBSTR(lg.data_ultima_ocorrencia,1,2)
                 ELSE lg.data_ultima_ocorrencia
             END)
        )
        AND UPPER(COALESCE(l_novo.status,'')) NOT LIKE '%CANCEL%'
        AND UPPER(COALESCE(l_novo.status,'')) NOT LIKE '%DEVOLV%'
        AND UPPER(COALESCE(l_novo.status,'')) NOT LIKE '%EXTRAVI%'
        AND UPPER(COALESCE(l_novo.status,'')) NOT IN ('INSERIDO NO BANCO DE DADOS','DISTRIBUIDO AO REMETENTE','EM DEVOLUCAO AO REMETENTE')
        AND l_novo.status IS NOT NULL AND TRIM(l_novo.status) != ''
  )
  AND NOT EXISTS (
      SELECT 1 FROM logistica l_cpf
      INNER JOIN propostas p_cpf ON p_cpf.proposta_isize = l_cpf.proposta_isize
      WHERE p_cpf.cpf = pr.cpf
        AND l_cpf.proposta_isize != lg.proposta_isize
        AND (l_cpf.id > lg.cancel_id
             OR DATE(CASE
                 WHEN l_cpf.data_insercao LIKE '__/__/____'
                 THEN SUBSTR(l_cpf.data_insercao,7,4)||'-'||SUBSTR(l_cpf.data_insercao,4,2)||'-'||SUBSTR(l_cpf.data_insercao,1,2)
                 ELSE l_cpf.data_insercao
             END) >= DATE(CASE
                 WHEN lg.data_ultima_ocorrencia LIKE '__/__/____'
                 THEN SUBSTR(lg.data_ultima_ocorrencia,7,4)||'-'||SUBSTR(lg.data_ultima_ocorrencia,4,2)||'-'||SUBSTR(lg.data_ultima_ocorrencia,1,2)
                 ELSE lg.data_ultima_ocorrencia
             END)
        )
        AND UPPER(COALESCE(l_cpf.status,'')) NOT LIKE '%CANCEL%'
        AND UPPER(COALESCE(l_cpf.status,'')) NOT LIKE '%DEVOLV%'
        AND UPPER(COALESCE(l_cpf.status,'')) NOT LIKE '%EXTRAVI%'
        AND l_cpf.status IS NOT NULL AND TRIM(l_cpf.status) != ''
  )
  AND NOT EXISTS (
      SELECT 1 FROM logistica l_tel
      WHERE l_tel.proposta_isize != lg.proposta_isize
        AND (l_tel.id > lg.cancel_id
             OR DATE(CASE
                 WHEN l_tel.data_insercao LIKE '__/__/____'
                 THEN SUBSTR(l_tel.data_insercao,7,4)||'-'||SUBSTR(l_tel.data_insercao,4,2)||'-'||SUBSTR(l_tel.data_insercao,1,2)
                 ELSE l_tel.data_insercao
             END) >= DATE(CASE
                 WHEN lg.data_ultima_ocorrencia LIKE '__/__/____'
                 THEN SUBSTR(lg.data_ultima_ocorrencia,7,4)||'-'||SUBSTR(lg.data_ultima_ocorrencia,4,2)||'-'||SUBSTR(lg.data_ultima_ocorrencia,1,2)
                 ELSE lg.data_ultima_ocorrencia
             END)
        )
        AND UPPER(COALESCE(l_tel.status,'')) NOT LIKE '%CANCEL%'
        AND UPPER(COALESCE(l_tel.status,'')) NOT LIKE '%DEVOLV%'
        AND l_tel.status IS NOT NULL AND TRIM(l_tel.status) != ''
        AND (
            (po.telefone_portabilidade IS NOT NULL AND po.telefone_portabilidade != ''
             AND l_tel.proposta_isize IN (SELECT proposta_isize FROM portabilidade WHERE telefone_portabilidade = po.telefone_portabilidade))
            OR
            (po.numero_linha IS NOT NULL AND po.numero_linha != ''
             AND l_tel.proposta_isize IN (SELECT proposta_isize FROM portabilidade WHERE numero_linha = po.numero_linha))
        )
  )
  AND NOT EXISTS (
      SELECT 1 FROM consulta_siebel cs_novo
      WHERE cs_novo.proposta_isize = lg.proposta_isize
        AND cs_novo.id > COALESCE((SELECT MAX(id) FROM consulta_siebel WHERE proposta_isize = lg.proposta_isize AND id <= lg.cancel_id), 0)
        AND UPPER(TRIM(COALESCE(cs_novo.status_ordem,''))) IN ('CONCLUÍDO','CONCLUIDO','EM APROVISIONAMENTO','PORTABILIDADE PENDENTE','PENDENTE PORTABILIDADE')
  )
  AND NOT EXISTS (
      SELECT 1 FROM portabilidade_tim pt_novo
      WHERE pt_novo.proposta_isize = lg.proposta_isize
        AND UPPER(TRIM(COALESCE(pt_novo.status,''))) IN ('ATIVA','PENDENTE','CONFIRMADO PELA DOADORA','REAGENDADO')
  )
ORDER BY lg.proposta_isize;
