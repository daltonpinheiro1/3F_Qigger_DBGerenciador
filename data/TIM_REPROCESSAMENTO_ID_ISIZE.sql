/*
   QUERY: TIM REPROCESSAMENTO POR ID ISIZE
   Base: portabilidade_v2.db (tabelas normalizadas)
   Objetivo: Gerar arquivo de reprocessamento TIM com dados completos
   Fonte: propostas + clientes + portabilidade + logistica + gross + bluechip

   ICCID cascata: logistica > gross.iccid > bluechip

   Para filtrar por IDs especificos, adicione na lista WHERE ao final
*/

WITH prop_atual AS (
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

port_atual AS (
    SELECT pt.*
    FROM portabilidade pt
    INNER JOIN (SELECT proposta_isize, MAX(versao) AS mv FROM portabilidade GROUP BY proposta_isize) pm
        ON pt.proposta_isize = pm.proposta_isize AND pt.versao = pm.mv
),

bc_atual AS (
    SELECT b.*
    FROM bluechip b
    INNER JOIN (SELECT proposta_isize, MAX(versao) AS mv FROM bluechip GROUP BY proposta_isize) bm
        ON b.proposta_isize = bm.proposta_isize AND b.versao = bm.mv
),

lg_atual AS (
    SELECT l.*
    FROM logistica l
    INNER JOIN (SELECT proposta_isize, MAX(id) AS max_id FROM logistica GROUP BY proposta_isize) lm
        ON l.id = lm.max_id
),

gross_iccid AS (
    SELECT proposta_isize, iccid
    FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY proposta_isize ORDER BY versao DESC) AS rn
          FROM gross WHERE iccid IS NOT NULL AND iccid != '') WHERE rn = 1
),

base AS (
    SELECT
        PRINTF('%011d', CAST(
            REPLACE(REPLACE(REPLACE(REPLACE(COALESCE(p.cpf, bo.cpf, ''),'.',''),'-',''),'/',''),' ','')
        AS INTEGER)) AS cpf,
        COALESCE(cl.nome_cliente, bo.nome_cliente, '') AS nome_cliente,
        CASE
            WHEN cl.data_nascimento LIKE '____-__-__T%'
            THEN SUBSTR(cl.data_nascimento, 9, 2) || '/' || SUBSTR(cl.data_nascimento, 6, 2) || '/' || SUBSTR(cl.data_nascimento, 1, 4)
            WHEN cl.data_nascimento LIKE '____-__-__ %'
            THEN SUBSTR(cl.data_nascimento, 9, 2) || '/' || SUBSTR(cl.data_nascimento, 6, 2) || '/' || SUBSTR(cl.data_nascimento, 1, 4)
            WHEN cl.data_nascimento LIKE '____-__-__'
            THEN SUBSTR(cl.data_nascimento, 9, 2) || '/' || SUBSTR(cl.data_nascimento, 6, 2) || '/' || SUBSTR(cl.data_nascimento, 1, 4)
            WHEN cl.data_nascimento LIKE '__/__/____'
            THEN SUBSTR(cl.data_nascimento, 1, 10)
            ELSE COALESCE(cl.data_nascimento, '')
        END AS data_nascimento,
        COALESCE(cl.nome_mae, '') AS nome_mae,
        CASE
            WHEN LENGTH(REPLACE(REPLACE(REPLACE(COALESCE(cl.cep, bo.cep, ''),'-',''),'.',''),' ','')) < 8
            THEN SUBSTR('00000000' || REPLACE(REPLACE(REPLACE(COALESCE(cl.cep, bo.cep, ''),'-',''),'.',''),' ',''), -8)
            ELSE SUBSTR(REPLACE(REPLACE(REPLACE(COALESCE(cl.cep, bo.cep, ''),'-',''),'.',''),' ',''), 1, 8)
        END AS cep,
        UPPER(COALESCE(cl.uf, bo.uf, '')) AS estado,
        COALESCE(cl.endereco, bo.endereco, '') AS logradouro,
        COALESCE(cl.numero, '') AS numero,
        COALESCE(cl.complemento, '') AS complemento,
        COALESCE(cl.ponto_referencia, '') AS referencia,
        COALESCE(cl.bairro, '') AS bairro,
        COALESCE(cl.cidade, '') AS cidade,
        COALESCE(cl.email, '') AS email,
        COALESCE(po.telefone_portabilidade, bo.numero_portado, '') AS telefone_portado,
        COALESCE(po.numero_linha, bo.numero_provisorio, '') AS numero_linha,
        CASE
            WHEN LENGTH(TRIM(COALESCE(cl.ddd_1,'')) || REPLACE(REPLACE(REPLACE(COALESCE(cl.telefone_1,''),'-',''),'.',''),' ','')) = 11
            THEN TRIM(COALESCE(cl.ddd_1,'')) || REPLACE(REPLACE(REPLACE(COALESCE(cl.telefone_1,''),'-',''),'.',''),' ','')
            WHEN LENGTH(TRIM(COALESCE(cl.ddd_1,'')) || REPLACE(REPLACE(REPLACE(COALESCE(cl.telefone_1,''),'-',''),'.',''),' ','')) = 10
            THEN SUBSTR(TRIM(COALESCE(cl.ddd_1,'')) || REPLACE(REPLACE(REPLACE(COALESCE(cl.telefone_1,''),'-',''),'.',''),' ',''), 1, 2)
                 || '9'
                 || SUBSTR(TRIM(COALESCE(cl.ddd_1,'')) || REPLACE(REPLACE(REPLACE(COALESCE(cl.telefone_1,''),'-',''),'.',''),' ',''), 3)
            ELSE TRIM(COALESCE(cl.ddd_1,'')) || REPLACE(REPLACE(REPLACE(COALESCE(cl.telefone_1,''),'-',''),'.',''),' ','')
        END AS telefone_1,
        CASE
            WHEN LENGTH(TRIM(COALESCE(cl.ddd_2,'')) || REPLACE(REPLACE(REPLACE(COALESCE(cl.telefone_2,''),'-',''),'.',''),' ','')) = 11
            THEN TRIM(COALESCE(cl.ddd_2,'')) || REPLACE(REPLACE(REPLACE(COALESCE(cl.telefone_2,''),'-',''),'.',''),' ','')
            WHEN LENGTH(TRIM(COALESCE(cl.ddd_2,'')) || REPLACE(REPLACE(REPLACE(COALESCE(cl.telefone_2,''),'-',''),'.',''),' ','')) = 10
            THEN SUBSTR(TRIM(COALESCE(cl.ddd_2,'')) || REPLACE(REPLACE(REPLACE(COALESCE(cl.telefone_2,''),'-',''),'.',''),' ',''), 1, 2)
                 || '9'
                 || SUBSTR(TRIM(COALESCE(cl.ddd_2,'')) || REPLACE(REPLACE(REPLACE(COALESCE(cl.telefone_2,''),'-',''),'.',''),' ',''), 3)
            ELSE TRIM(COALESCE(cl.ddd_2,'')) || REPLACE(REPLACE(REPLACE(COALESCE(cl.telefone_2,''),'-',''),'.',''),' ','')
        END AS telefone_2,
        COALESCE(p.plano, '') AS plano,
        SUBSTR(COALESCE(p.plano, ''), -5) AS preco,
        COALESCE(p.vencimento, '') AS data_vencimento,
        REPLACE(REPLACE(REPLACE(REPLACE(COALESCE(po.telefone_portabilidade, bo.numero_portado, ''),'-',''),'.',''),' ',''),'+','') AS numero_portabilidade,
        COALESCE(
            NULLIF(TRIM(COALESCE(lg.iccid, '')), ''),
            NULLIF(TRIM(COALESCE(gi.iccid, '')), ''),
            NULLIF(TRIM(COALESCE(bo.iccid, '')), ''),
            ''
        ) AS tim_chip,
        CASE
            WHEN COALESCE(p.data_venda, bo.data_venda) LIKE '____-__-__T%' THEN SUBSTR(COALESCE(p.data_venda, bo.data_venda), 1, 19)
            WHEN COALESCE(p.data_venda, bo.data_venda) LIKE '____-__-__ %' THEN SUBSTR(COALESCE(p.data_venda, bo.data_venda), 1, 19)
            WHEN COALESCE(p.data_venda, bo.data_venda) LIKE '____-__-__' THEN COALESCE(p.data_venda, bo.data_venda) || ' 00:00:00'
            WHEN COALESCE(p.data_venda, bo.data_venda) LIKE '__/__/____ %'
            THEN SUBSTR(COALESCE(p.data_venda, bo.data_venda), 7, 4) || '-' || SUBSTR(COALESCE(p.data_venda, bo.data_venda), 4, 2) || '-' || SUBSTR(COALESCE(p.data_venda, bo.data_venda), 1, 2) || ' ' || SUBSTR(COALESCE(p.data_venda, bo.data_venda), 12, 8)
            WHEN COALESCE(p.data_venda, bo.data_venda) LIKE '__/__/____'
            THEN SUBSTR(COALESCE(p.data_venda, bo.data_venda), 7, 4) || '-' || SUBSTR(COALESCE(p.data_venda, bo.data_venda), 4, 2) || '-' || SUBSTR(COALESCE(p.data_venda, bo.data_venda), 1, 2) || ' 00:00:00'
            ELSE COALESCE(p.data_venda, bo.data_venda, '')
        END AS data_venda_norm,
        COALESCE(p.plataforma, '') AS canal,
        ids.proposta_isize AS codigo_externo,
        COALESCE(
            NULLIF(TRIM(COALESCE(lg.iccid, '')), ''),
            NULLIF(TRIM(COALESCE(gi.iccid, '')), ''),
            NULLIF(TRIM(COALESCE(bo.iccid, '')), ''),
            ''
        ) AS iccid_final
    FROM (
        SELECT DISTINCT proposta_isize FROM (
            SELECT proposta_isize FROM propostas
            UNION ALL SELECT proposta_isize FROM backoffice
            UNION ALL SELECT proposta_isize FROM logistica
            UNION ALL SELECT proposta_isize FROM consulta_siebel
        ) WHERE proposta_isize IN (

260011250, 260016727, 260020924, 260020668, 260016839, 260011851, 260011528, 260011368, 260016991, 260016989, 260016945, 260016887, 260016835, 260020496, 260018607, 260018367, 260018314, 260018309, 260017901, 260017807, 260016969, 260016777, 260016680, 260016009, 260016008, 260015166, 260014887, 260005853, 260003940, 260012040, 260011719, 260011571, 260011465, 260011156, 260011006, 260014132, 250021154, 260019977, 260018129, 260018043, 260017644, 260017319, 260017001, 260016890, 260016815, 260016771, 260016695, 260016254, 260016201, 260016118, 260016047, 260016027, 260015900, 260015829, 260015656, 260015643, 260015533, 260015445, 260015435, 260014980, 260014905, 260014895, 260014752, 260014366, 260013693, 260013538, 260006728, 260012299, 260011509, 260020817, 260020180, 260018055, 260017181, 260016924, 260016702, 260016439, 260016348, 260016219, 260016191, 260016044, 260015894, 260015634, 260015327, 260015326, 260015288, 260014896, 260014875, 260014773, 260014109, 260006647, 260012087, 260021239, 260021081, 260020987, 260020314, 260016978, 260016582, 260016094, 260015996, 260015112, 260015102, 260013924, 260008580, 260007433, 260017578, 260017338, 260017076, 260015673, 260020653, 260020002, 260016928, 260015479, 260014988, 260014841, 260014625, 260013981, 260013789, 260017797, 260017722, 260017712, 260021294, 260021243, 260020639, 260015118, 250014530, 260007949, 260007301, 260001844, 260012856, 260012734, 260017969, 260017965, 260017916, 260017769, 260017741, 260017713, 260017697, 260017465, 260017432, 260017382, 260013828, 260007849, 260007256, 260013140, 260018248, 260018195, 260018074, 260017936, 260017738, 260017075, 260014000, 260007730, 260007326, 260013383, 260013311, 260013086, 260012720, 260012553, 260011506, 260010348, 250013728, 260018445, 260018417, 260018073, 260021348, 260021295, 260021275, 260008713, 260008667, 260008551, 260007806, 260013261, 260018187, 260018152, 260021956, 260021527, 260020842, 260009053, 260008793, 260008777, 260013221, 260018207, 260022219, 260021606, 260013350, 260018494, 260017832, 260022400, 260022268, 260022034, 260009383, 260019097, 260022356, 260022184, 260021875, 250019443, 260008135, 260019244, 260016127, 260023005, 260022715, 260022609, 260019107, 260018593, 260018001, 260017366, 260019456, 260023198, 260023086, 260022925, 260022921, 260022904, 260022755, 260022663, 260022604, 260021238, 260009791, 260023962, 260023593, 260023558, 260023519, 260023283, 260022841, 260022693, 260022674, 260022405, 260022368, 260021890, 260021821, 260009352, 260008825, 260014701, 260019306, 260019220, 260024137, 260024116, 260024049, 260022480, 250021502, 260019935, 260002892, 260013459, 260019616, 260017918, 250020985, 250017171, 260010384, 260010061, 260009652, 260015761, 250021453, 260010826, 260010751, 260010729, 260009824, 260009600, 250013853, 260015215, 260010698, 250021353, 260011296, 260015573, 260015565, 260011395, 260011121, 260016033, 260020135, 260019829, 260011559, 260011552, 260020664, 260020552, 260012085, 260005828, 260010853, 260016186, 260015079, 260020815, 260020674, 260020659, 260016340

        )
    ) ids
    LEFT JOIN prop_atual p ON p.proposta_isize = ids.proposta_isize
    LEFT JOIN cli_atual cl ON cl.cpf = COALESCE(p.cpf, (SELECT cpf FROM backoffice WHERE proposta_isize = ids.proposta_isize ORDER BY versao DESC LIMIT 1))
    LEFT JOIN port_atual po ON po.proposta_isize = ids.proposta_isize
    LEFT JOIN bc_atual bc ON bc.proposta_isize = ids.proposta_isize
    LEFT JOIN lg_atual lg ON lg.proposta_isize = ids.proposta_isize
    LEFT JOIN gross_iccid gi ON gi.proposta_isize = ids.proposta_isize
    LEFT JOIN (
        SELECT b.* FROM backoffice b
        INNER JOIN (SELECT proposta_isize, MAX(versao) AS mv FROM backoffice GROUP BY proposta_isize) bm
            ON b.proposta_isize = bm.proposta_isize AND b.versao = bm.mv
    ) bo ON bo.proposta_isize = ids.proposta_isize
)

SELECT
    cpf                    AS "Cpf",
    nome_cliente           AS "Nome do cliente",
    data_nascimento        AS "Data de nascimento",
    nome_mae               AS "Nome da mãe",
    cep                    AS "Cep",
    estado                 AS "Estado",
    logradouro             AS "Logradouro",
    numero                 AS "Número",
    complemento            AS "Complemento",
    referencia             AS "Referencia",
    bairro                 AS "Bairro",
    cidade                 AS "Cidade",
    email                  AS "Email",
    CASE
        WHEN LENGTH(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(COALESCE(telefone_portado,''),'-',''),'.',''),' ',''),'(',''),')','')) = 11
        THEN REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(COALESCE(telefone_portado,''),'-',''),'.',''),' ',''),'(',''),')','')
        WHEN LENGTH(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(COALESCE(telefone_portado,''),'-',''),'.',''),' ',''),'(',''),')','')) = 10
        THEN SUBSTR(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(COALESCE(telefone_portado,''),'-',''),'.',''),' ',''),'(',''),')',''), 1, 2) || '9' || SUBSTR(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(COALESCE(telefone_portado,''),'-',''),'.',''),' ',''),'(',''),')',''), 3)
        WHEN LENGTH(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(COALESCE(numero_linha,''),'-',''),'.',''),' ',''),'(',''),')','')) = 11
        THEN REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(COALESCE(numero_linha,''),'-',''),'.',''),' ',''),'(',''),')','')
        WHEN LENGTH(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(COALESCE(numero_linha,''),'-',''),'.',''),' ',''),'(',''),')','')) = 10
        THEN SUBSTR(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(COALESCE(numero_linha,''),'-',''),'.',''),' ',''),'(',''),')',''), 1, 2) || '9' || SUBSTR(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(COALESCE(numero_linha,''),'-',''),'.',''),' ',''),'(',''),')',''), 3)
        WHEN LENGTH(telefone_1) = 11 THEN telefone_1
        WHEN LENGTH(telefone_1) = 10 THEN SUBSTR(telefone_1, 1, 2) || '9' || SUBSTR(telefone_1, 3)
        WHEN LENGTH(telefone_2) = 11 THEN telefone_2
        WHEN LENGTH(telefone_2) = 10 THEN SUBSTR(telefone_2, 1, 2) || '9' || SUBSTR(telefone_2, 3)
        WHEN LENGTH(numero_portabilidade) = 11 THEN numero_portabilidade
        WHEN LENGTH(numero_portabilidade) = 10 THEN SUBSTR(numero_portabilidade, 1, 2) || '9' || SUBSTR(numero_portabilidade, 3)
        ELSE numero_portabilidade
    END AS "Telefone de contato 1",
    CASE
        WHEN LENGTH(telefone_2) >= 10 AND telefone_2 != telefone_1 THEN
            CASE WHEN LENGTH(telefone_2) = 11 THEN telefone_2
                 WHEN LENGTH(telefone_2) = 10 THEN SUBSTR(telefone_2, 1, 2) || '9' || SUBSTR(telefone_2, 3)
                 ELSE telefone_2 END
        ELSE ''
    END AS "Telefone de contato 2",
    ''                     AS "Telefone de contato 3",
    plano                  AS "Plano",
    preco                  AS "Preço",
    'FATURA'               AS "Tipo do pagamento",
    'ONLINE'               AS "Tipo da fatura",
    ''                     AS "Banco",
    ''                     AS "Agência",
    ''                     AS "Conta",
    data_vencimento        AS "Data de vencimento da fatura",
    numero_portabilidade   AS "Número da portabilidade",
    tim_chip               AS "Tim Chip",
    data_venda_norm        AS "Data da venda",
    ''                     AS "Tem número provisório?",
    CASE WHEN numero_portabilidade != '' THEN 'SIM' ELSE 'NÃO' END AS "Tem portabilidade antecipada?",
    canal                  AS "Canal",
    codigo_externo         AS "Código externo",
    iccid_final            AS "ICCID"
FROM base
ORDER BY data_venda_norm DESC;
