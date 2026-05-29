/*
   QUERY 1: CONSOLIDADO - ENVIOS DUPLICADOS INDEVIDOS POR NUMERO DE ACESSO
   Base: portabilidade_v2.db
   Logica:
     - Encontra o ultimo pedido LEGITIMO (primeiro apos ultimo cancelamento)
     - Tudo que veio DEPOIS do legitimo sem cancelamento = INDEVIDO
   Resultado: 1 linha por telefone (consolidado)

   FILTRO DE DATA: altere as datas em dd/mm/aaaa na CTE filtro_periodo
*/

WITH filtro_periodo AS (
    SELECT
        SUBSTR('01/05/2026',7,4)||'-'||SUBSTR('01/05/2026',4,2)||'-'||SUBSTR('01/05/2026',1,2) AS data_inicio,
        SUBSTR('31/05/2026',7,4)||'-'||SUBSTR('31/05/2026',4,2)||'-'||SUBSTR('31/05/2026',1,2) AS data_fim
),

envios_periodo AS (
    SELECT l.proposta_isize, l.nu_pedido, l.id_erp, l.status, l.data_insercao, l.id,
           p.cpf,
           COALESCE(po.telefone_portabilidade, '') AS telefone_portado,
           ROW_NUMBER() OVER (PARTITION BY l.proposta_isize, l.nu_pedido ORDER BY l.id DESC) AS rn_ult
    FROM logistica l, filtro_periodo f
    INNER JOIN propostas p ON p.proposta_isize = l.proposta_isize
        AND p.versao = (SELECT MAX(versao) FROM propostas WHERE proposta_isize = l.proposta_isize)
    LEFT JOIN portabilidade po ON po.proposta_isize = l.proposta_isize
        AND po.versao = (SELECT MAX(versao) FROM portabilidade WHERE proposta_isize = l.proposta_isize)
    WHERE DATE(CASE
        WHEN l.data_insercao LIKE '__/__/____'
        THEN SUBSTR(l.data_insercao,7,4)||'-'||SUBSTR(l.data_insercao,4,2)||'-'||SUBSTR(l.data_insercao,1,2)
        ELSE l.data_insercao
    END) BETWEEN f.data_inicio AND f.data_fim
    AND l.nu_pedido IS NOT NULL AND l.nu_pedido != ''
    AND po.telefone_portabilidade IS NOT NULL AND po.telefone_portabilidade != ''
),

envios_unicos AS (
    SELECT e.*
    FROM envios_periodo e
    WHERE e.rn_ult = 1
      AND e.id = (
          SELECT MIN(e2.id) FROM envios_periodo e2
          WHERE e2.nu_pedido = e.nu_pedido AND e2.telefone_portado = e.telefone_portado AND e2.rn_ult = 1
      )
),

tel_multiplos AS (
    SELECT telefone_portado
    FROM envios_unicos
    GROUP BY telefone_portado
    HAVING COUNT(DISTINCT nu_pedido) > 1
),

pedidos_ordenados AS (
    SELECT e.telefone_portado, e.cpf, e.proposta_isize, e.nu_pedido, e.id_erp,
           e.status, e.data_insercao, e.id,
           ROW_NUMBER() OVER (PARTITION BY e.telefone_portado ORDER BY
               DATE(CASE
                   WHEN e.data_insercao LIKE '__/__/____'
                   THEN SUBSTR(e.data_insercao,7,4)||'-'||SUBSTR(e.data_insercao,4,2)||'-'||SUBSTR(e.data_insercao,1,2)
                   ELSE e.data_insercao
               END) ASC, e.nu_pedido ASC
           ) AS ordem
    FROM envios_unicos e
    INNER JOIN tel_multiplos tm ON tm.telefone_portado = e.telefone_portado
),

ultimo_cancelado AS (
    SELECT telefone_portado,
           MAX(ordem) AS ordem_ultimo_cancel
    FROM pedidos_ordenados
    WHERE UPPER(COALESCE(status, '')) LIKE '%CANCEL%'
       OR UPPER(COALESCE(status, '')) LIKE '%DEVOLV%'
       OR UPPER(COALESCE(status, '')) LIKE '%EXTRAVI%'
    GROUP BY telefone_portado
),

classificacao AS (
    SELECT
        p.telefone_portado, p.cpf, p.proposta_isize, p.nu_pedido,
        p.status, p.data_insercao, p.ordem,
        COALESCE(uc.ordem_ultimo_cancel, 0) AS ordem_ultimo_cancel,
        CASE
            WHEN p.ordem <= COALESCE(uc.ordem_ultimo_cancel, 0) THEN 'CANCELADO'
            WHEN p.ordem = COALESCE(uc.ordem_ultimo_cancel, 0) + 1 THEN 'LEGITIMO'
            ELSE 'INDEVIDO'
        END AS classificacao
    FROM pedidos_ordenados p
    LEFT JOIN ultimo_cancelado uc ON uc.telefone_portado = p.telefone_portado
)

SELECT
    c.telefone_portado AS "Número de acesso",
    MAX(c.cpf) AS "CPF",

    (SELECT c2.nu_pedido FROM classificacao c2
     WHERE c2.telefone_portado = c.telefone_portado AND c2.classificacao = 'LEGITIMO'
     LIMIT 1
    ) AS "Pedido legítimo",

    (SELECT c2.proposta_isize FROM classificacao c2
     WHERE c2.telefone_portado = c.telefone_portado AND c2.classificacao = 'LEGITIMO'
     LIMIT 1
    ) AS "Proposta legítima",

    GROUP_CONCAT(CASE WHEN c.classificacao = 'INDEVIDO' THEN c.nu_pedido END) AS "Pedidos indevidos",

    SUM(CASE WHEN c.classificacao = 'INDEVIDO' THEN 1 ELSE 0 END) AS "Qtd indevidos",

    COUNT(DISTINCT c.nu_pedido) AS "Total pedidos periodo",

    GROUP_CONCAT(c.nu_pedido || '(' || SUBSTR(c.classificacao, 1, 3) || ')') AS "Cronologia"

FROM classificacao c
WHERE c.telefone_portado IN (
    SELECT telefone_portado FROM classificacao WHERE classificacao = 'INDEVIDO'
)
GROUP BY c.telefone_portado
ORDER BY SUM(CASE WHEN c.classificacao = 'INDEVIDO' THEN 1 ELSE 0 END) DESC,
         c.telefone_portado;
