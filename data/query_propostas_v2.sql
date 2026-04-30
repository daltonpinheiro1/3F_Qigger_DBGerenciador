-- Query de propostas específicas — portabilidade_v2.db
-- Adaptada do layout base_coverte_prop para schema V2 normalizado
-- ATTACH legado para busca ampla de ICCID em relatorio_objetos
--
-- INSTRUÇÃO: Execute este bloco UMA VEZ por sessão.
-- Se "already in use", pule direto para o SELECT.
-- =====================================================================

-- 1) Desanexar se já existir (ignora erro se não existir)
--    Execute separadamente: DETACH DATABASE legado;
-- 2) Anexar
ATTACH DATABASE '/Applications/Documentos/Projetos_python/3F_Qigger_DBGerenciador/data/portabilidade.db' AS legado;

-- =====================================================================
-- FILTRO: Cole os IDs separados por vírgula dentro do IN()
-- =====================================================================

SELECT
    -- CPF 11 dígitos
    PRINTF('%011d', CAST(
        CASE
            WHEN REPLACE(REPLACE(REPLACE(COALESCE(c.cpf, ''), '.', ''), '-', ''), '/', '') GLOB '[0-9]*'
             AND REPLACE(REPLACE(REPLACE(COALESCE(c.cpf, ''), '.', ''), '-', ''), '/', '') <> ''
            THEN REPLACE(REPLACE(REPLACE(c.cpf, '.', ''), '-', ''), '/', '')
            ELSE '0'
        END AS INTEGER
    )) AS "Cpf",
    c.nome_cliente AS "Nome do cliente",
    -- Data nascimento dd/mm/aaaa
    CASE
        WHEN c.data_nascimento IS NULL OR TRIM(c.data_nascimento) = '' THEN NULL
        WHEN c.data_nascimento LIKE '____-__-__%'
        THEN SUBSTR(c.data_nascimento, 9, 2) || '/' || SUBSTR(c.data_nascimento, 6, 2) || '/' || SUBSTR(c.data_nascimento, 1, 4)
        WHEN c.data_nascimento LIKE '__/__/____' THEN SUBSTR(c.data_nascimento, 1, 10)
        ELSE c.data_nascimento
    END AS "Data de nascimento",
    c.nome_mae AS "Nome da mãe",
    -- CEP 8 dígitos
    CASE
        WHEN LENGTH(REPLACE(REPLACE(COALESCE(c.cep, ''), '-', ''), ' ', '')) < 8
        THEN SUBSTR('00000000' || REPLACE(REPLACE(COALESCE(c.cep, ''), '-', ''), ' ', ''), -8)
        ELSE REPLACE(REPLACE(c.cep, '-', ''), ' ', '')
    END AS "Cep",
    UPPER(TRIM(COALESCE(c.uf, ''))) AS "Estado",
    TRIM(COALESCE(c.endereco, '')) AS "Logradouro",
    REPLACE(REPLACE(REPLACE(COALESCE(c.numero, ''), '.', ''), '-', ''), ' ', '') AS "Número",
    TRIM(COALESCE(c.complemento, '')) AS "Complemento",
    TRIM(COALESCE(c.ponto_referencia, '')) AS "Referencia",
    TRIM(COALESCE(c.bairro, '')) AS "Bairro",
    TRIM(COALESCE(c.cidade, '')) AS "Cidade",
    TRIM(COALESCE(c.email, '')) AS "Email",
    -- Telefone 1: portado ou ddd+tel (11 dígitos)
    CASE
        WHEN COALESCE(TRIM(port.telefone_portabilidade), '') <> ''
         AND TRIM(port.telefone_portabilidade) <> '-'
         AND LENGTH(REPLACE(REPLACE(REPLACE(TRIM(port.telefone_portabilidade), '-', ''), ' ', ''), '(', '')) >= 10
        THEN SUBSTR('00000000000' || REPLACE(REPLACE(REPLACE(REPLACE(TRIM(port.telefone_portabilidade), '-', ''), ' ', ''), '(', ''), ')', ''), -11)
        WHEN COALESCE(TRIM(c.ddd_1), '') <> '' AND COALESCE(TRIM(c.telefone_1), '') <> ''
        THEN SUBSTR('00000000000' || REPLACE(REPLACE(REPLACE(REPLACE(TRIM(c.ddd_1), '-', ''), ' ', ''), '(', ''), ')', '')
             || REPLACE(REPLACE(REPLACE(REPLACE(TRIM(c.telefone_1), '-', ''), ' ', ''), '(', ''), ')', ''), -11)
        ELSE '00000000000'
    END AS "Telefone de contato 1",
    -- Telefone 2
    CASE
        WHEN COALESCE(TRIM(c.telefone_1), '') <> ''
         AND COALESCE(TRIM(c.ddd_1), '') <> ''
        THEN SUBSTR('00000000000' || REPLACE(REPLACE(REPLACE(REPLACE(TRIM(c.ddd_1), '-', ''), ' ', ''), '(', ''), ')', '')
             || REPLACE(REPLACE(REPLACE(REPLACE(TRIM(c.telefone_1), '-', ''), ' ', ''), '(', ''), ')', ''), -11)
        ELSE ''
    END AS "Telefone de contato 2",
    '' AS "Telefone de contato 3",
    TRIM(COALESCE(p.plano, '')) AS "Plano",
    SUBSTR(TRIM(COALESCE(p.plano, '')), -5) AS "Preço",
    'FATURA' AS "Tipo do pagamento",
    'ONLINE' AS "Tipo da fatura",
    '' AS "Banco",
    '' AS "Agência",
    '' AS "Conta",
    COALESCE(p.vencimento, '') AS "Data de vencimento da fatura",
    -- Número portabilidade
    CASE
        WHEN COALESCE(TRIM(port.telefone_portabilidade), '') <> ''
         AND TRIM(port.telefone_portabilidade) <> '-'
        THEN REPLACE(REPLACE(REPLACE(REPLACE(TRIM(port.telefone_portabilidade), '-', ''), ' ', ''), '(', ''), ')', '')
        ELSE ''
    END AS "Número da portabilidade",
    -- Tim chip / ICCID: busca em TODAS as tabelas (logística → bluechip → backoffice → legado)
    COALESCE(
        -- 1. Logística V2 por proposta
        (SELECT l.iccid FROM logistica l
         WHERE l.proposta_isize = p.proposta_isize
           AND COALESCE(TRIM(l.iccid), '') <> '' AND TRIM(l.iccid) <> '-'
         ORDER BY l.id DESC LIMIT 1),
        -- 2. Backoffice V2
        (SELECT bo.iccid FROM backoffice bo
         WHERE bo.proposta_isize = p.proposta_isize
           AND COALESCE(TRIM(bo.iccid), '') <> '' AND TRIM(bo.iccid) <> '-'
         ORDER BY bo.versao DESC LIMIT 1),
        -- 3. Logística V2 via pedido_bluechip
        (SELECT l2.iccid FROM logistica l2
         WHERE COALESCE(TRIM(p2.pedido_bluechip), '') <> ''
           AND l2.nu_pedido = TRIM(p2.pedido_bluechip)
           AND COALESCE(TRIM(l2.iccid), '') <> '' AND TRIM(l2.iccid) <> '-'
         ORDER BY l2.id DESC LIMIT 1),
        -- 4. Legado: relatorio_objetos por codigo_externo (proposta_isize)
        (SELECT ro.iccid FROM legado.relatorio_objetos ro
         WHERE TRIM(CAST(ro.codigo_externo AS TEXT)) = p.proposta_isize
           AND COALESCE(TRIM(CAST(ro.iccid AS TEXT)), '') <> '' AND TRIM(CAST(ro.iccid AS TEXT)) <> '-'
         ORDER BY ro.id DESC LIMIT 1),
        -- 5. Legado: relatorio_objetos por nu_pedido (pedido_bluechip)
        (SELECT ro.iccid FROM legado.relatorio_objetos ro
         WHERE COALESCE(TRIM(p2.pedido_bluechip), '') <> ''
           AND TRIM(CAST(ro.nu_pedido AS TEXT)) = TRIM(p2.pedido_bluechip)
           AND COALESCE(TRIM(CAST(ro.iccid AS TEXT)), '') <> '' AND TRIM(CAST(ro.iccid AS TEXT)) <> '-'
         ORDER BY ro.id DESC LIMIT 1),
        -- 6. Legado: relatorio_objetos por CPF (documento)
        (SELECT ro.iccid FROM legado.relatorio_objetos ro
         WHERE COALESCE(TRIM(p.cpf), '') <> ''
           AND REPLACE(REPLACE(REPLACE(TRIM(CAST(ro.documento AS TEXT)), '.', ''), '-', ''), '/', '')
               = REPLACE(REPLACE(REPLACE(TRIM(p.cpf), '.', ''), '-', ''), '/', '')
           AND COALESCE(TRIM(CAST(ro.iccid AS TEXT)), '') <> '' AND TRIM(CAST(ro.iccid AS TEXT)) <> '-'
         ORDER BY ro.id DESC LIMIT 1),
        -- 7. Legado: relatorio_objetos por telefone (numero portado)
        (SELECT ro.iccid FROM legado.relatorio_objetos ro
         WHERE COALESCE(TRIM(port.telefone_portabilidade), '') <> ''
           AND TRIM(port.telefone_portabilidade) <> '-'
           AND REPLACE(REPLACE(REPLACE(REPLACE(TRIM(CAST(ro.telefone AS TEXT)), '-', ''), ' ', ''), '(', ''), ')', '')
               = REPLACE(REPLACE(REPLACE(REPLACE(TRIM(port.telefone_portabilidade), '-', ''), ' ', ''), '(', ''), ')', '')
           AND COALESCE(TRIM(CAST(ro.iccid AS TEXT)), '') <> '' AND TRIM(CAST(ro.iccid AS TEXT)) <> '-'
         ORDER BY ro.id DESC LIMIT 1),
        ''
    ) AS "Tim Chip",
    -- Data venda dd/mm/aaaa
    STRFTIME('%d/%m/%Y', DATE(p.data_venda)) AS "Data da venda",
    '' AS "Tem número provisório?",
    CASE WHEN COALESCE(TRIM(port.telefone_portabilidade), '') <> ''
          AND TRIM(port.telefone_portabilidade) <> '-'
         THEN 'SIM' ELSE 'NÃO'
    END AS "Tem portabilidade antecipada?",
    COALESCE(p.plataforma, '') AS "Canal",
    p.proposta_isize AS "Código externo",
    -- ICCID: mesma busca ampla (V2 + legado por proposta, pedido, CPF, telefone)
    COALESCE(
        (SELECT l.iccid FROM logistica l
         WHERE l.proposta_isize = p.proposta_isize
           AND COALESCE(TRIM(l.iccid), '') <> '' AND TRIM(l.iccid) <> '-'
         ORDER BY l.id DESC LIMIT 1),
        (SELECT bo.iccid FROM backoffice bo
         WHERE bo.proposta_isize = p.proposta_isize
           AND COALESCE(TRIM(bo.iccid), '') <> '' AND TRIM(bo.iccid) <> '-'
         ORDER BY bo.versao DESC LIMIT 1),
        (SELECT l2.iccid FROM logistica l2
         WHERE COALESCE(TRIM(p2.pedido_bluechip), '') <> ''
           AND l2.nu_pedido = TRIM(p2.pedido_bluechip)
           AND COALESCE(TRIM(l2.iccid), '') <> '' AND TRIM(l2.iccid) <> '-'
         ORDER BY l2.id DESC LIMIT 1),
        (SELECT ro.iccid FROM legado.relatorio_objetos ro
         WHERE TRIM(CAST(ro.codigo_externo AS TEXT)) = p.proposta_isize
           AND COALESCE(TRIM(CAST(ro.iccid AS TEXT)), '') <> '' AND TRIM(CAST(ro.iccid AS TEXT)) <> '-'
         ORDER BY ro.id DESC LIMIT 1),
        (SELECT ro.iccid FROM legado.relatorio_objetos ro
         WHERE COALESCE(TRIM(p2.pedido_bluechip), '') <> ''
           AND TRIM(CAST(ro.nu_pedido AS TEXT)) = TRIM(p2.pedido_bluechip)
           AND COALESCE(TRIM(CAST(ro.iccid AS TEXT)), '') <> '' AND TRIM(CAST(ro.iccid AS TEXT)) <> '-'
         ORDER BY ro.id DESC LIMIT 1),
        (SELECT ro.iccid FROM legado.relatorio_objetos ro
         WHERE COALESCE(TRIM(p.cpf), '') <> ''
           AND REPLACE(REPLACE(REPLACE(TRIM(CAST(ro.documento AS TEXT)), '.', ''), '-', ''), '/', '')
               = REPLACE(REPLACE(REPLACE(TRIM(p.cpf), '.', ''), '-', ''), '/', '')
           AND COALESCE(TRIM(CAST(ro.iccid AS TEXT)), '') <> '' AND TRIM(CAST(ro.iccid AS TEXT)) <> '-'
         ORDER BY ro.id DESC LIMIT 1),
        (SELECT ro.iccid FROM legado.relatorio_objetos ro
         WHERE COALESCE(TRIM(port.telefone_portabilidade), '') <> ''
           AND TRIM(port.telefone_portabilidade) <> '-'
           AND REPLACE(REPLACE(REPLACE(REPLACE(TRIM(CAST(ro.telefone AS TEXT)), '-', ''), ' ', ''), '(', ''), ')', '')
               = REPLACE(REPLACE(REPLACE(REPLACE(TRIM(port.telefone_portabilidade), '-', ''), ' ', ''), '(', ''), ')', '')
           AND COALESCE(TRIM(CAST(ro.iccid AS TEXT)), '') <> '' AND TRIM(CAST(ro.iccid AS TEXT)) <> '-'
         ORDER BY ro.id DESC LIMIT 1),
        ''
    ) AS "ICCID"

FROM propostas p
INNER JOIN (
    SELECT proposta_isize, MAX(versao) AS mv
    FROM propostas GROUP BY proposta_isize
) pm ON p.proposta_isize = pm.proposta_isize AND p.versao = pm.mv
LEFT JOIN clientes c ON c.cpf = p.cpf
    AND c.versao = (SELECT MAX(versao) FROM clientes WHERE cpf = p.cpf)
LEFT JOIN portabilidade port ON port.proposta_isize = p.proposta_isize
    AND port.versao = (SELECT MAX(versao) FROM portabilidade WHERE proposta_isize = p.proposta_isize)
LEFT JOIN bluechip p2 ON p2.proposta_isize = p.proposta_isize
    AND p2.versao = (SELECT MAX(versao) FROM bluechip WHERE proposta_isize = p.proposta_isize)
WHERE p.proposta_isize IN (260009783, 260016445, 260018515, 260018870, 260016902, 260016731, 260018662, 260019747, 260019373, 260009263, 260019486, 260019545, 260019318, 260019283, 260019544, 260019857, 260017880, 260012815, 260007180, 260018977, 260008501, 260016434, 260020038, 260020040, 260018135, 260018408, 260019159, 260019509, 260020591, 260020674, 260019390, 260019971, 260008390, 260007136, 260017674, 260020088, 260020251, 260020444, 260017472, 260019836, 260010146, 260020399, 260017531, 260019620, 260013847, 260019377, 260016745, 260016309, 260017383, 260018482, 260019584, 260019471, 260019850, 260020213, 260017121, 260016732, 260020306, 260017533, 260019986, 260013788, 260018995, 260017538, 260017535, 260017603, 260017512, 260018381, 260018382, 260019276, 260020021, 260013744, 260013132, 260019305, 260005593, 260018754, 260018429, 260006577, 260017946, 260020655, 260017536, 260018580, 260017567, 260019436, 260005317, 260020740, 260005508, 260017735, 260020738, 260017137, 260019483, 260017224, 260019047, 260020396, 260019277, 260020201, 260017305, 260017593, 260019098, 260019312, 260019619, 260018407, 260017678, 260019972, 260017030, 260017532, 260017721, 260020198, 260017888, 260012888, 260017467, 260016313, 260016353, 260012823, 260018261, 260018150, 260012798, 260006852, 260016818, 260020661, 260007494, 260017995, 260017132, 260020228, 260005775, 260018556, 260019592, 260018435, 260017752, 260019980, 260012326, 260019353, 260012107, 260015748)
ORDER BY p.data_venda DESC;
