INSTALL excel;
LOAD excel;

INSTALL sqlite;
LOAD sqlite;

INSTALL odbc_scanner;
LOAD odbc_scanner;

SET VARIABLE conn = odbc_connect(
'Driver={ODBC Driver 18 for SQL Server};
Server=3fdb.vexten.com.br,1433;
Database=eva_activities;
Uid=mis;
Pwd=zFuQg%n52@;
Encrypt=yes;
TrustServerCertificate=yes;'
);

-- Attach banco V2 SQLite para consultar auditoria_vendas_tim
ATTACH IF NOT EXISTS '/Applications/Documentos/Projetos_python/3F_Qigger_DBGerenciador/data/portabilidade_v2.db' AS v2 (TYPE sqlite, READ_ONLY);

CREATE OR REPLACE TABLE raw_vendas_excel AS
SELECT *
FROM read_xlsx(
    '/Users/mac/Library/CloudStorage/OneDrive-Pessoal/Documents/VENDAS PRE_CTRL_MAR_26.xlsx',
    header = true,
    all_varchar = true,
    range = 'A1:AK10002'
);

CREATE TABLE IF NOT EXISTS raw_status_responsavel_duck (
    telefone_raw VARCHAR,
    cpf_raw VARCHAR,
    status_raw VARCHAR,
    responsavel_raw VARCHAR,
    dt_carga TIMESTAMP DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS stg_status_responsavel_duck (
    telefone VARCHAR,
    cpf VARCHAR,
    status_duck VARCHAR,
    responsavel_duck VARCHAR,
    dt_carga TIMESTAMP
);

DELETE FROM raw_status_responsavel_duck;

INSERT INTO raw_status_responsavel_duck (
    telefone_raw,
    cpf_raw,
    status_raw,
    responsavel_raw
)
SELECT
    CASE
        WHEN CAST("[Telefone Discado]" AS VARCHAR) LIKE '%E%'
            THEN CAST(CAST(CAST("[Telefone Discado]" AS DOUBLE) AS BIGINT) AS VARCHAR)
        ELSE regexp_replace(trim(CAST("[Telefone Discado]" AS VARCHAR)), '[^0-9]', '', 'g')
    END AS telefone_raw,
    CASE
        WHEN CAST(CPF AS VARCHAR) LIKE '%E%'
            THEN CAST(CAST(CAST(CPF AS DOUBLE) AS BIGINT) AS VARCHAR)
        ELSE regexp_replace(trim(CAST(CPF AS VARCHAR)), '[^0-9]', '', 'g')
    END AS cpf_raw,
    trim(CAST(STATUS AS VARCHAR)) AS status_raw,
    trim(CAST("BACKOFFICE RESPONSAVEL" AS VARCHAR)) AS responsavel_raw
FROM raw_vendas_excel;

CREATE OR REPLACE TABLE stg_status_responsavel_duck AS
WITH norm AS (
    SELECT
        NULLIF(regexp_replace(trim(coalesce(telefone_raw, '')), '[^0-9]', '', 'g'), '') AS telefone,
        NULLIF(regexp_replace(trim(coalesce(cpf_raw, '')), '[^0-9]', '', 'g'), '') AS cpf,
        CASE
            WHEN coalesce(trim(status_raw), '') = '' THEN NULL
            WHEN upper(trim(status_raw)) = 'DADOS CADASTRIS INVALIDOS' THEN 'DADOS CADASTRAIS INVALIDOS'
            ELSE upper(trim(status_raw))
        END AS status_duck,
        CASE
            WHEN coalesce(trim(responsavel_raw), '') = '' THEN NULL
            WHEN upper(trim(responsavel_raw)) IN ('KAUÃ', 'KAUA') THEN 'KAUA'
            WHEN upper(trim(responsavel_raw)) = upper(trim(coalesce(status_raw, ''))) THEN NULL
            WHEN upper(trim(responsavel_raw)) IN (
                'APROVADA','PENDENTE','DDD DIVERGENTE','CLIENTE JA MIGRADO',
                'LIMITE DE CREDITO','RESTRIÇÃO INTERNA','RESTRICAO INTERNA',
                'RESTRIÇÃO DE MERCADO','RESTRICAO DE MERCADO',
                'DADOS CADASTRIS INVALIDOS','DADOS CADASTRAIS INVALIDOS',
                'SCORE','ERRO SISTÊMICO','ERRO SISTEMICO'
            ) THEN NULL
            ELSE upper(trim(responsavel_raw))
        END AS responsavel_duck,
        dt_carga
    FROM raw_status_responsavel_duck
),
filtrado AS (
    SELECT * FROM norm
    WHERE telefone IS NOT NULL OR cpf IS NOT NULL
),
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY coalesce(telefone, '__SEM_TELEFONE__')
            ORDER BY dt_carga DESC, status_duck DESC, responsavel_duck DESC
        ) AS rn_tel,
        ROW_NUMBER() OVER (
            PARTITION BY coalesce(cpf, '__SEM_CPF__')
            ORDER BY dt_carga DESC, status_duck DESC, responsavel_duck DESC
        ) AS rn_cpf
    FROM filtrado
),
priorizado AS (
    SELECT telefone, cpf, status_duck, responsavel_duck, dt_carga
    FROM ranked
    WHERE (telefone IS NOT NULL AND rn_tel = 1)
       OR (telefone IS NULL AND cpf IS NOT NULL AND rn_cpf = 1)
)
SELECT
    telefone,
    cpf,
    coalesce(status_duck, 'PENDENTE')         AS status_duck,
    coalesce(responsavel_duck, 'SEM_RESPONSAVEL') AS responsavel_duck,
    dt_carga
FROM priorizado;

-- =====================================================================
-- VIEW PRINCIPAL
-- =====================================================================
DROP VIEW IF EXISTS vw_vendas_layout_final;
CREATE OR REPLACE VIEW vw_vendas_layout_final AS
WITH

-- -------------------------------------------------------------------
-- PARÂMETROS DE PLANO (tabela atualizada)
-- -------------------------------------------------------------------
plano_parametros AS (
    SELECT * FROM (
        VALUES
            ('TIM CONTROLE SMART',               98.99, 49.00, 49.99, 10, 20, 30, 'TIM CONTROLE SMART - DE  98,99 COM DESC 49'),
            ('TIM CONTROLE SMART',               95.99, 30.00, 65.99, 10, 20, 30, 'TIM CONTROLE SMART - DE  95,99 COM DESC 30'),
            ('TIM CONTROLE SMART',               98.99, 33.00, 65.99, 10, 20, 30, 'TIM CONTROLE SMART - DE  98,99 COM DESC 33'),
            ('TIM CONTROLE LIGACOES ILIMITADAS', 52.99, 22.00, 30.99, 20,  0, 20, 'TIM CONTROLE LIGACOES ILIMITADAS - DE  52,99 COM DESC 22'),
            ('TIM CONTROLE LIGACOES ILIMITADAS', 50.99, 20.00, 30.99, 20,  0, 20, 'TIM CONTROLE LIGACOES ILIMITADAS - DE  50,99 COM DESC 20'),
            ('TIM CONTROLE LIGACOES ILIMITADAS', 49.99, 19.00, 30.99, 20,  0, 20, 'TIM CONTROLE LIGACOES ILIMITADAS - DE  49,99 COM DESC 19'),
            ('TIM CONTROLE SMART',               96.99, 43.00, 53.99, 10, 20, 30, 'TIM CONTROLE SMART - DE  96,99 COM DESC 43'),
            ('TIM CONTROLE SMART',               98.99, 45.00, 53.99, 10, 20, 30, 'TIM CONTROLE SMART - DE  98,99 COM DESC 45'),
            ('TIM CONTROLE SMART',               95.99, 46.00, 49.99, 10, 20, 30, 'TIM CONTROLE SMART - DE  95,99 COM DESC 46'),
            ('TIM CONTROLE SMART',               96.99, 47.00, 49.99, 10, 20, 30, 'TIM CONTROLE SMART - DE  96,99 COM DESC 47'),
            ('TIM CONTROLE LIGACOES ILIMITADAS', 52.99, 22.00, 30.99,  3, 20, 23, 'TIM CONTROLE LIGACOES ILIMITADAS - DE  52,99 COM DESC 22'),
            ('TIM CONTROLE LIGACOES ILIMITADAS', 50.99, 20.00, 30.99,  3, 20, 23, 'TIM CONTROLE LIGACOES ILIMITADAS - DE  50,99 COM DESC 20'),
            ('TIM CONTROLE LIGACOES ILIMITADAS', 49.99, 19.00, 30.99,  3, 20, 23, 'TIM CONTROLE LIGACOES ILIMITADAS - DE  49,99 COM DESC 19'),
            ('TIM CONTROLE SMART',               98.99, 49.00, 49.99,  5, 30, 35, 'TIM CONTROLE SMART - DE  98,99 COM DESC 49'),
            ('TIM CONTROLE SMART',               96.99, 47.00, 49.99,  5, 30, 35, 'TIM CONTROLE SMART - DE  96,99 COM DESC 47'),
            ('TIM CONTROLE SMART',               95.99, 42.00, 53.99,  5, 30, 35, 'TIM CONTROLE SMART - DE  95,99 COM DESC 42'),
            ('TIM CONTROLE SMART',               95.99, 46.00, 49.99,  5, 30, 35, 'TIM CONTROLE SMART - DE  95,99 COM DESC 46'),
            ('TIM CONTROLE SMART',               98.99, 45.00, 53.99,  5, 30, 35, 'TIM CONTROLE SMART - DE  98,99 COM DESC 45'),
            ('TIM CONTROLE SMART',               98.99, 33.00, 65.99,  5, 30, 35, 'TIM CONTROLE SMART - DE  98,99 COM DESC 33'),
            ('TIM CONTROLE SMART',               96.99, 31.00, 65.99,  5, 30, 35, 'TIM CONTROLE SMART - DE  96,99 COM DESC 31'),
            ('TIM CONTROLE SMART',               96.99, 43.00, 53.99,  5, 30, 35, 'TIM CONTROLE SMART - DE  96,99 COM DESC 43'),
            ('TIM CONTROLE SMART',               95.99, 30.00, 65.99,  5, 30, 35, 'TIM CONTROLE SMART - DE  95,99 COM DESC 30'),
            ('TIM CONTROLE SMART',               95.99, 35.00, 60.99,  5, 30, 35, 'TIM CONTROLE SMART - DE  95,99 COM DESC 35'),
            ('TIM CONTROLE SMART',               98.99, 38.00, 60.99,  5, 30, 35, 'TIM CONTROLE SMART - DE  98,99 COM DESC 38'),
            ('TIM CONTROLE SMART',               96.99, 36.00, 60.99,  5, 30, 35, 'TIM CONTROLE SMART - DE  96,99 COM DESC 36'),
            ('TIM CONTROLE LIGACOES ILIMITADAS', 50.99, 20.00, 30.99,  3, 30, 33, 'TIM CONTROLE LIGACOES ILIMITADAS - DE  50,99 COM DESC 20'),
            ('TIM CONTROLE LIGACOES ILIMITADAS', 52.99, 22.00, 30.99,  3, 30, 33, 'TIM CONTROLE LIGACOES ILIMITADAS - DE  52,99 COM DESC 22'),
            ('TIM CONTROLE SMART',               96.99, 31.00, 65.99,  5, 40, 45, 'TIM CONTROLE SMART - DE  96,99 COM DESC 31'),
            ('TIM CONTROLE SMART',               96.99, 47.00, 49.99,  5, 40, 45, 'TIM CONTROLE SMART - DE  96,99 COM DESC 47'),
            ('TIM CONTROLE SMART',               96.99, 43.00, 53.99,  5, 40, 45, 'TIM CONTROLE SMART - DE  96,99 COM DESC 43'),
            ('TIM CONTROLE SMART',               95.99, 46.00, 49.99,  5, 40, 45, 'TIM CONTROLE SMART - DE  95,99 COM DESC 46'),
            -- novos parâmetros
            ('TIM CONTROLE LIGACOES ILIMITADAS', 50.99, 25.00, 25.99,  3, 20, 23, 'TIM CONTROLE LIGACOES ILIMITADAS - DE 50,99 COM DESC 25'),
            ('TIM CONTROLE LIGACOES ILIMITADAS', 52.99, 30.00, 22.99,  3, 20, 23, 'TIM CONTROLE LIGACOES ILIMITADAS - DE 52,99 COM DESC 30'),
            ('TIM CONTROLE LIGACOES ILIMITADAS', 52.99, 27.00, 25.99,  3, 20, 23, 'TIM CONTROLE LIGACOES ILIMITADAS - DE 52,99 COM DESC 27'),
            ('TIM CONTROLE LIGACOES ILIMITADAS', 49.99, 24.00, 25.99,  3, 20, 23, 'TIM CONTROLE LIGACOES ILIMITADAS - DE 49,99 COM DESC 24'),
            ('TIM CONTROLE LIGACOES ILIMITADAS', 49.99, 13.00, 36.99,  3, 30, 33, 'TIM CONTROLE LIGACOES ILIMITADAS - DE 49,99 COM DESC 13'),
            ('TIM CONTROLE SMART',               98.99, 68.00, 30.99,  3, 30, 33, 'TIM CONTROLE SMART - DE 98,99 COM DESC 68'),
            ('TIM CONTROLE SMART',               95.99, 65.00, 30.99,  3, 30, 33, 'TIM CONTROLE SMART - DE 95,99 COM DESC 65'),
            ('TIM CONTROLE SMART',               96.99, 66.00, 30.99,  3, 30, 33, 'TIM CONTROLE SMART - DE 96,99 COM DESC 66'),
            ('TIM CONTROLE SMART',               96.99, 66.00, 30.99,  5, 40, 45, 'TIM CONTROLE SMART - DE 96,99 COM DESC 66'),
            ('TIM CONTROLE SMART',               98.99, 68.00, 30.99,  5, 30, 35, 'TIM CONTROLE SMART - DE 98,99 COM DESC 68'),
            ('TIM CONTROLE SMART',               96.99, 66.00, 30.99,  3, 40, 43, 'TIM CONTROLE SMART - DE 96,99 COM DESC 66'),
            ('TIM CONTROLE SMART',               95.99, 65.00, 30.99,  5, 30, 35, 'TIM CONTROLE SMART - DE 95,99 COM DESC 65'),
            ('TIM CONTROLE SMART',               95.99, 65.00, 30.99,  3, 40, 43, 'TIM CONTROLE SMART - DE 95,99 COM DESC 65'),
            ('TIM CONTROLE SMART',               96.99, 66.00, 30.99,  5, 30, 35, 'TIM CONTROLE SMART - DE 96,99 COM DESC 66')
    ) AS t (
        nome_eva, valor_sem_fidel, desc_fidel, valor_fidel,
        franquia, bonus_12m, gb_12_meses, plano_parametrizado
    )
),

-- -------------------------------------------------------------------
-- BASE SQL SERVER
-- -------------------------------------------------------------------
base_sqlserver AS (
    SELECT
        regexp_replace(trim(coalesce(CAST(src."Numero de acesso" AS VARCHAR), '')), '[^0-9]', '', 'g') AS "Número de acesso",
        regexp_replace(trim(coalesce(CAST(src."CPF Limpo" AS VARCHAR), '')), '[^0-9]', '', 'g') AS "CPF Limpo",
        upper(trim(translate(coalesce(CAST(src."Nome do cliente" AS VARCHAR), ''),
            'ÁÀÂÃÄáàâãäÉÈÊËéèêëÍÌÎÏíìîïÓÒÔÕÖóòôõöÚÙÛÜúùûüÇçÑñ',
            'AAAAAaaaaaEEEEeeeeIIIIiiiiOOOOOoooooUUUUuuuuCcNn'))) AS "Nome do cliente",
        trim(coalesce(CAST(src."Data de nascimento" AS VARCHAR), '')) AS "Data de nascimento",
        upper(trim(translate(coalesce(CAST(src."Nome da mae" AS VARCHAR), ''),
            'ÁÀÂÃÄáàâãäÉÈÊËéèêëÍÌÎÏíìîïÓÒÔÕÖóòôõöÚÙÛÜúùûüÇçÑñ',
            'AAAAAaaaaaEEEEeeeeIIIIiiiiOOOOOoooooUUUUuuuuCcNn'))) AS "Nome da mãe",
        regexp_replace(trim(coalesce(CAST(src."Cep" AS VARCHAR), '')), '[^0-9]', '', 'g') AS "Cep",
        upper(trim(translate(coalesce(CAST(src."Estado" AS VARCHAR), ''),
            'ÁÀÂÃÄáàâãäÉÈÊËéèêëÍÌÎÏíìîïÓÒÔÕÖóòôõöÚÙÛÜúùûüÇçÑñ',
            'AAAAAaaaaaEEEEeeeeIIIIiiiiOOOOOoooooUUUUuuuuCcNn'))) AS "Estado",
        upper(trim(translate(coalesce(CAST(src."Logradouro" AS VARCHAR), ''),
            'ÁÀÂÃÄáàâãäÉÈÊËéèêëÍÌÎÏíìîïÓÒÔÕÖóòôõöÚÙÛÜúùûüÇçÑñ',
            'AAAAAaaaaaEEEEeeeeIIIIiiiiOOOOOoooooUUUUuuuuCcNn'))) AS "Logradouro",
        trim(coalesce(CAST(src."Numero" AS VARCHAR), '')) AS "Número",
        upper(trim(translate(coalesce(CAST(src."Complemento" AS VARCHAR), ''),
            'ÁÀÂÃÄáàâãäÉÈÊËéèêëÍÌÎÏíìîïÓÒÔÕÖóòôõöÚÙÛÜúùûüÇçÑñ',
            'AAAAAaaaaaEEEEeeeeIIIIiiiiOOOOOoooooUUUUuuuuCcNn'))) AS "Complemento",
        upper(trim(translate(coalesce(CAST(src."Referencia" AS VARCHAR), ''),
            'ÁÀÂÃÄáàâãäÉÈÊËéèêëÍÌÎÏíìîïÓÒÔÕÖóòôõöÚÙÛÜúùûüÇçÑñ',
            'AAAAAaaaaaEEEEeeeeIIIIiiiiOOOOOoooooUUUUuuuuCcNn'))) AS "Referência",
        upper(trim(translate(coalesce(CAST(src."Bairro" AS VARCHAR), ''),
            'ÁÀÂÃÄáàâãäÉÈÊËéèêëÍÌÎÏíìîïÓÒÔÕÖóòôõöÚÙÛÜúùûüÇçÑñ',
            'AAAAAaaaaaEEEEeeeeIIIIiiiiOOOOOoooooUUUUuuuuCcNn'))) AS "Bairro",
        upper(trim(translate(coalesce(CAST(src."Cidade" AS VARCHAR), ''),
            'ÁÀÂÃÄáàâãäÉÈÊËéèêëÍÌÎÏíìîïÓÒÔÕÖóòôõöÚÙÛÜúùûüÇçÑñ',
            'AAAAAaaaaaEEEEeeeeIIIIiiiiOOOOOoooooUUUUuuuuCcNn'))) AS "Cidade",
        lower(trim(coalesce(CAST(src."Email" AS VARCHAR), ''))) AS "Email",
        upper(trim(translate(coalesce(CAST(src."Plano" AS VARCHAR), ''),
            'ÁÀÂÃÄáàâãäÉÈÊËéèêëÍÌÎÏíìîïÓÒÔÕÖóòôõöÚÙÛÜúùûüÇçÑñ',
            'AAAAAaaaaaEEEEeeeeIIIIiiiiOOOOOoooooUUUUuuuuCcNn'))) AS "Plano",
        CAST(src."Plano Base"        AS VARCHAR) AS "__Plano Base",
        CAST(src."Desc Fidel Raw"    AS VARCHAR) AS "__Desc Fidel Raw",
        CAST(src."Franquia Raw"      AS VARCHAR) AS "__Franquia Raw",
        CAST(src."GB Bonus Raw"      AS VARCHAR) AS "__GB Bonus Raw",
        CAST(src."GB Total Raw"      AS VARCHAR) AS "__GB Total Raw",
        CAST(src."Preco"             AS VARCHAR) AS "Preço",
        CAST(src."Tipo do pagamento" AS VARCHAR) AS "Tipo do pagamento",
        CAST(src."Tipo da fatura"    AS VARCHAR) AS "Tipo da fatura",
        CAST(src."Banco"             AS VARCHAR) AS "Banco",
        CAST(src."Agencia"           AS VARCHAR) AS "Agência",
        CAST(src."Conta"             AS VARCHAR) AS "Conta",
        CAST(src."Data de vencimento da fatura" AS VARCHAR) AS "Data de vencimento da fatura",
        CAST(src."Data da venda"     AS VARCHAR) AS "Data da venda",
        CAST(src."Tem alteracao cadastral" AS VARCHAR) AS "Tem alteração cadastral?",
        CAST(src."Codigo externo"    AS VARCHAR) AS "Código externo",
        CAST(src."BackofficeOriginal" AS VARCHAR) AS "__Backoffice Original",
        CAST(src."StatusOriginal" AS VARCHAR) AS "__Status Original",
        CAST(src."ProtocoloQigger" AS VARCHAR) AS "__Protocolo Qigger",
        CAST(src."DataEnvioQigger" AS VARCHAR) AS "__Data Envio Qigger",
        CAST(src."NomeVendedor"    AS VARCHAR) AS "__Nome Vendedor",
        CAST(src."Supervisor"      AS VARCHAR) AS "__Supervisor",
        'SQLSERVER' AS "Origem Registro"
    FROM odbc_query(
        getvariable('conn'),
        '
        WITH base AS (
            SELECT
                v.*,
                CAST(v.[DATA_HORA_GRAVACAO] AS datetime) AS DataAtendimentoDate,
                CAST(v.[DATA NASCIMENTO] AS date) AS DataNascimentoDate,
                TRY_CONVERT(int, v.[DIA_VENCIMENTO]) AS DiaVencimentoInt,
                LTRIM(RTRIM(ISNULL(CONVERT(varchar(255), v.[EMAIL]), ''''))) AS EmailOriginal,
                UPPER(LTRIM(RTRIM(ISNULL(CONVERT(varchar(255), v.[EMAIL]), '''')))) AS EmailUpper,
                REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                    LTRIM(RTRIM(ISNULL(CONVERT(varchar(50), v.[TELEFONE_DISCADOR]), ''''))),
                    ''.'', ''''), ''-'', ''''), ''/'', ''''), ''('', ''''), '')'', ''''), '' '', ''''), ''+'', ''''), CHAR(9), ''''), CHAR(10), ''''), CHAR(13), '''') AS TelefoneDigits,
                REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                    LTRIM(RTRIM(ISNULL(CONVERT(varchar(50), v.[CPF]), ''''))),
                    ''.'', ''''), ''-'', ''''), ''/'', ''''), ''('', ''''), '')'', ''''), '' '', ''''), ''+'', ''''), CHAR(9), ''''), CHAR(10), ''''), CHAR(13), '''') AS CPFDigits,
                LTRIM(RTRIM(ISNULL(CONVERT(varchar(255), v.[NUMERO]), ''''))) AS NumeroBruto,
                LTRIM(RTRIM(ISNULL(CONVERT(varchar(255), v.[COMPLEMENTO]), ''''))) AS ComplementoBruto,
                LTRIM(RTRIM(ISNULL(CONVERT(varchar(255), v.[ENDEREÇO]), ''''))) AS EnderecoBruto,
                LTRIM(RTRIM(ISNULL(CONVERT(varchar(255), v.[TIPO LOGRADOURO]), ''''))) AS TipoLogradouroBruto,
                TRY_CONVERT(decimal(10,2), REPLACE(CONVERT(varchar(50), v.[VALOR]), '','', ''.'')) AS ValorNormalizado,
                TRY_CONVERT(int, REPLACE(CONVERT(varchar(50), v.[GBCORE]), '','', ''.'')) AS FranquiaNormalizada,
                TRY_CONVERT(int, REPLACE(CONVERT(varchar(50), v.[GBBONUS]), '','', ''.'')) AS Bonus12MNormalizado,
                TRY_CONVERT(int, REPLACE(CONVERT(varchar(50), v.[GBTOTAL]), '','', ''.'')) AS GB12MesesNormalizado,
                UPPER(LTRIM(RTRIM(ISNULL(CONVERT(varchar(255), v.[PRODUTO]), '''')))) AS ProdutoOriginal,
                CASE
                    WHEN UPPER(LTRIM(RTRIM(ISNULL(CONVERT(varchar(255), v.[PRODUTO]), '''')))) LIKE ''%LIGACOES ILIMITAD%''
                        THEN ''TIM CONTROLE LIGACOES ILIMITADAS''
                    WHEN UPPER(LTRIM(RTRIM(ISNULL(CONVERT(varchar(255), v.[PRODUTO]), '''')))) LIKE ''%SMART%''
                        THEN ''TIM CONTROLE SMART''
                    ELSE UPPER(LTRIM(RTRIM(ISNULL(CONVERT(varchar(255), v.[PRODUTO]), ''''))))
                END AS ProdutoPadronizado
            FROM [eva_activities].[dbo].[vwSales] v
            WHERE UPPER(LTRIM(RTRIM(ISNULL(CONVERT(varchar(100), v.[OPERACAO]), '''')))) = ''TIMPRE''
        ),
        norm_num AS (
            SELECT
                b.*,
                CASE
                    WHEN b.NumeroBruto = '''' THEN ''0''
                    WHEN UPPER(b.NumeroBruto) IN (''S/N'',''SN'',''SEM NUMERO'',''SEM NÚMERO'',''NAO TEM'',''NÃO TEM'',''NAO POSSUI'',''NÃO POSSUI'',''SEM NUM'',''N/A'',''NA'') THEN ''0''
                    WHEN PATINDEX(''%[0-9]%'', b.NumeroBruto) = 0 THEN ''0''
                    WHEN b.NumeroBruto NOT LIKE ''%[^0-9]%'' THEN b.NumeroBruto
                    ELSE SUBSTRING(
                        b.NumeroBruto,
                        PATINDEX(''%[0-9]%'', b.NumeroBruto),
                        CASE
                            WHEN PATINDEX(''%[^0-9]%'', SUBSTRING(b.NumeroBruto, PATINDEX(''%[0-9]%'', b.NumeroBruto), LEN(b.NumeroBruto))) = 0
                            THEN LEN(b.NumeroBruto)
                            ELSE PATINDEX(''%[^0-9]%'', SUBSTRING(b.NumeroBruto, PATINDEX(''%[0-9]%'', b.NumeroBruto), LEN(b.NumeroBruto))) - 1
                        END
                    )
                END AS NumeroNormalizado,
                CASE
                    WHEN b.NumeroBruto = '''' THEN NULLIF(b.ComplementoBruto, '''')
                    WHEN UPPER(b.NumeroBruto) IN (''S/N'',''SN'',''SEM NUMERO'',''SEM NÚMERO'',''NAO TEM'',''NÃO TEM'',''NAO POSSUI'',''NÃO POSSUI'',''SEM NUM'',''N/A'',''NA'') THEN NULLIF(b.ComplementoBruto, '''')
                    WHEN b.NumeroBruto LIKE ''%[^0-9]%'' AND PATINDEX(''%[0-9]%'', b.NumeroBruto) > 0
                    THEN LTRIM(RTRIM(CASE WHEN b.ComplementoBruto <> '''' THEN b.ComplementoBruto + '' '' + b.NumeroBruto ELSE b.NumeroBruto END))
                    ELSE NULLIF(b.ComplementoBruto, '''')
                END AS ComplementoNormalizado
            FROM base b
        ),
        norm_log AS (
            SELECT
                n.*,
                CASE
                    WHEN UPPER(LTRIM(RTRIM(ISNULL(n.TipoLogradouroBruto, '''')))) IN (''R'', ''RUA'') THEN ''RUA''
                    WHEN UPPER(LTRIM(RTRIM(ISNULL(n.TipoLogradouroBruto, '''')))) IN (''AV'', ''AVENIDA'') THEN ''AVENIDA''
                    WHEN UPPER(LTRIM(RTRIM(ISNULL(n.TipoLogradouroBruto, '''')))) IN (''AL'', ''ALAMEDA'') THEN ''ALAMEDA''
                    WHEN UPPER(LTRIM(RTRIM(ISNULL(n.TipoLogradouroBruto, '''')))) IN (''TRAV'', ''TRAVESSA'', ''TV'') THEN ''TRAVESSA''
                    WHEN UPPER(LTRIM(RTRIM(ISNULL(n.TipoLogradouroBruto, '''')))) IN (''EST'', ''ESTRADA'') THEN ''ESTRADA''
                    WHEN UPPER(LTRIM(RTRIM(ISNULL(n.TipoLogradouroBruto, '''')))) IN (''ROD'', ''RODOVIA'') THEN ''RODOVIA''
                    WHEN UPPER(LTRIM(RTRIM(ISNULL(n.TipoLogradouroBruto, '''')))) IN (''PRACA'', ''PRAÇA'') THEN ''PRACA''
                    WHEN UPPER(LTRIM(RTRIM(ISNULL(n.TipoLogradouroBruto, '''')))) = ''LARGO'' THEN ''LARGO''
                    WHEN UPPER(LTRIM(RTRIM(ISNULL(n.TipoLogradouroBruto, '''')))) = ''VIELA'' THEN ''VIELA''
                    WHEN UPPER(LTRIM(RTRIM(ISNULL(n.TipoLogradouroBruto, '''')))) = ''FAZENDA'' THEN ''FAZENDA''
                    ELSE UPPER(LTRIM(RTRIM(ISNULL(n.TipoLogradouroBruto, ''''))))
                END AS TipoLogradouroNormalizado,
                CASE
                    WHEN UPPER(LTRIM(RTRIM(ISNULL(n.EnderecoBruto, '''')))) LIKE ''RUA %''
                      OR UPPER(LTRIM(RTRIM(ISNULL(n.EnderecoBruto, '''')))) LIKE ''AVENIDA %''
                      OR UPPER(LTRIM(RTRIM(ISNULL(n.EnderecoBruto, '''')))) LIKE ''AV %''
                      OR UPPER(LTRIM(RTRIM(ISNULL(n.EnderecoBruto, '''')))) LIKE ''ALAMEDA %''
                      OR UPPER(LTRIM(RTRIM(ISNULL(n.EnderecoBruto, '''')))) LIKE ''TRAVESSA %''
                      OR UPPER(LTRIM(RTRIM(ISNULL(n.EnderecoBruto, '''')))) LIKE ''TV %''
                      OR UPPER(LTRIM(RTRIM(ISNULL(n.EnderecoBruto, '''')))) LIKE ''RODOVIA %''
                      OR UPPER(LTRIM(RTRIM(ISNULL(n.EnderecoBruto, '''')))) LIKE ''ESTRADA %''
                      OR UPPER(LTRIM(RTRIM(ISNULL(n.EnderecoBruto, '''')))) LIKE ''PRACA %''
                      OR UPPER(LTRIM(RTRIM(ISNULL(n.EnderecoBruto, '''')))) LIKE ''PRAÇA %''
                      OR UPPER(LTRIM(RTRIM(ISNULL(n.EnderecoBruto, '''')))) LIKE ''LARGO %''
                      OR UPPER(LTRIM(RTRIM(ISNULL(n.EnderecoBruto, '''')))) LIKE ''VIELA %''
                      OR UPPER(LTRIM(RTRIM(ISNULL(n.EnderecoBruto, '''')))) LIKE ''FAZENDA %''
                    THEN LTRIM(RTRIM(ISNULL(n.EnderecoBruto, '''')))
                    WHEN NULLIF(LTRIM(RTRIM(ISNULL(n.TipoLogradouroBruto, ''''))), '''') IS NOT NULL
                     AND NULLIF(LTRIM(RTRIM(ISNULL(n.EnderecoBruto, ''''))), '''') IS NOT NULL
                    THEN LTRIM(RTRIM(
                        CASE
                            WHEN UPPER(LTRIM(RTRIM(ISNULL(n.TipoLogradouroBruto, '''')))) IN (''R'', ''RUA'') THEN ''RUA''
                            WHEN UPPER(LTRIM(RTRIM(ISNULL(n.TipoLogradouroBruto, '''')))) IN (''AV'', ''AVENIDA'') THEN ''AVENIDA''
                            WHEN UPPER(LTRIM(RTRIM(ISNULL(n.TipoLogradouroBruto, '''')))) IN (''AL'', ''ALAMEDA'') THEN ''ALAMEDA''
                            WHEN UPPER(LTRIM(RTRIM(ISNULL(n.TipoLogradouroBruto, '''')))) IN (''TRAV'', ''TRAVESSA'', ''TV'') THEN ''TRAVESSA''
                            WHEN UPPER(LTRIM(RTRIM(ISNULL(n.TipoLogradouroBruto, '''')))) IN (''EST'', ''ESTRADA'') THEN ''ESTRADA''
                            WHEN UPPER(LTRIM(RTRIM(ISNULL(n.TipoLogradouroBruto, '''')))) IN (''ROD'', ''RODOVIA'') THEN ''RODOVIA''
                            WHEN UPPER(LTRIM(RTRIM(ISNULL(n.TipoLogradouroBruto, '''')))) IN (''PRACA'', ''PRAÇA'') THEN ''PRACA''
                            WHEN UPPER(LTRIM(RTRIM(ISNULL(n.TipoLogradouroBruto, '''')))) = ''LARGO'' THEN ''LARGO''
                            WHEN UPPER(LTRIM(RTRIM(ISNULL(n.TipoLogradouroBruto, '''')))) = ''VIELA'' THEN ''VIELA''
                            WHEN UPPER(LTRIM(RTRIM(ISNULL(n.TipoLogradouroBruto, '''')))) = ''FAZENDA'' THEN ''FAZENDA''
                            ELSE UPPER(LTRIM(RTRIM(ISNULL(n.TipoLogradouroBruto, ''''))))
                        END + '' '' + LTRIM(RTRIM(ISNULL(n.EnderecoBruto, '''')))
                    ))
                    ELSE LTRIM(RTRIM(ISNULL(n.EnderecoBruto, '''')))
                END AS LogradouroComposto
            FROM norm_num n
        ),
        final_prep AS (
            SELECT
                l.*,
                RIGHT(REPLICATE(''0'', 11) + ISNULL(l.TelefoneDigits, ''''), 11) AS TelefoneLimpo,
                RIGHT(REPLICATE(''0'', 11) + ISNULL(l.CPFDigits, ''''), 11) AS CPFLimpo,
                CASE
                    WHEN NULLIF(LTRIM(RTRIM(l.EmailUpper)), '''') IS NULL THEN 0
                    WHEN l.EmailUpper LIKE ''% %'' THEN 0
                    WHEN (LEN(l.EmailUpper) - LEN(REPLACE(l.EmailUpper, ''@'', ''''))) <> 1 THEN 0
                    WHEN CHARINDEX(''@'', l.EmailUpper) <= 1 THEN 0
                    WHEN CHARINDEX(''.'', l.EmailUpper, CHARINDEX(''@'', l.EmailUpper) + 2) = 0 THEN 0
                    WHEN LEFT(l.EmailUpper, 1) IN (''@'', ''.'') THEN 0
                    WHEN RIGHT(l.EmailUpper, 1) IN (''@'', ''.'') THEN 0
                    WHEN l.EmailUpper LIKE ''%@.%'' THEN 0
                    WHEN l.EmailUpper LIKE ''%.@%'' THEN 0
                    WHEN l.EmailUpper LIKE ''%..%'' THEN 0
                    ELSE 1
                END AS EmailValido
            FROM norm_log l
        )
        SELECT
            f.TelefoneLimpo AS [Numero de acesso],
            f.CPFLimpo AS [CPF Limpo],
            UPPER(LTRIM(RTRIM(ISNULL(CONVERT(varchar(255), f.[CLIENTE]), '''')))) AS [Nome do cliente],
            CONVERT(VARCHAR(10), f.DataNascimentoDate, 103) AS [Data de nascimento],
            UPPER(LTRIM(RTRIM(ISNULL(CONVERT(varchar(255), f.[NOME_MAE]), '''')))) AS [Nome da mae],
            LTRIM(RTRIM(ISNULL(CONVERT(varchar(20), f.[CEP]), ''''))) AS [Cep],
            UPPER(LTRIM(RTRIM(ISNULL(CONVERT(varchar(10), f.[UF]), '''')))) AS [Estado],
            UPPER(LTRIM(RTRIM(ISNULL(CONVERT(varchar(255), f.LogradouroComposto), '''')))) AS [Logradouro],
            f.NumeroNormalizado AS [Numero],
            UPPER(LTRIM(RTRIM(ISNULL(CONVERT(varchar(255), f.ComplementoNormalizado), '''')))) AS [Complemento],
            UPPER(LTRIM(RTRIM(ISNULL(CONVERT(varchar(255), f.[REFERENCIA]), '''')))) AS [Referencia],
            UPPER(LTRIM(RTRIM(ISNULL(CONVERT(varchar(255), f.[BAIRRO]), '''')))) AS [Bairro],
            UPPER(LTRIM(RTRIM(ISNULL(CONVERT(varchar(255), f.[CIDADE]), '''')))) AS [Cidade],
            LOWER(LTRIM(RTRIM(ISNULL(CONVERT(varchar(255), f.[EMAIL]), '''')))) AS [Email],
            UPPER(LTRIM(RTRIM(ISNULL(CONVERT(varchar(255), f.ProdutoPadronizado), '''')))) AS [Plano],
            ISNULL(CONVERT(varchar(255), f.ProdutoPadronizado), '''') AS [Plano Base],
            CAST('''' AS varchar(50)) AS [Desc Fidel Raw],
            ISNULL(CONVERT(varchar(50), f.[GBCORE]), '''') AS [Franquia Raw],
            ISNULL(CONVERT(varchar(50), f.[GBBONUS]), '''') AS [GB Bonus Raw],
            ISNULL(CONVERT(varchar(50), f.[GBTOTAL]), '''') AS [GB Total Raw],
            ISNULL(CONVERT(varchar(50), f.[VALOR]), '''') AS [Preco],
            ISNULL(CONVERT(varchar(30), f.[FORMA_PAGAMENTO]), '''') AS [Tipo do pagamento],
            CAST(CASE WHEN f.EmailValido = 1 THEN ''ONLINE'' ELSE ''DETALHADA'' END AS nvarchar(30)) AS [Tipo da fatura],
            ISNULL(CONVERT(varchar(100), f.[BANCO]), '''') AS [Banco],
            ISNULL(CONVERT(varchar(50), f.[AGENCIA]), '''') AS [Agencia],
            ISNULL(CONVERT(varchar(50), f.[CONTA]), '''') AS [Conta],
            CASE
                WHEN f.DiaVencimentoInt BETWEEN 1 AND 31
                THEN RIGHT(''0'' + CONVERT(varchar(2), f.DiaVencimentoInt), 2)
                ELSE NULL
            END AS [Data de vencimento da fatura],
            CONVERT(VARCHAR(10), CAST(f.DataAtendimentoDate AS date), 103) AS [Data da venda],
            CAST(''SIM'' AS varchar(3)) AS [Tem alteracao cadastral],
            ISNULL(CONVERT(varchar(100), f.[PEDIDO]), '''') AS [Codigo externo],
			ISNULL(CONVERT(varchar(100), f.[NOME_BACKOFFICE]), '''')       AS [BackofficeOriginal],
			ISNULL(CONVERT(varchar(100), f.[GRUPO_DE_STATUS_VENDA]), '''') AS [StatusOriginal],
			ISNULL(CONVERT(varchar(100), f.[PROTOCOLO_QIGGER]), '''')     AS [ProtocoloQigger],
			CASE WHEN f.[DATA_ENVIO_QIGGER] IS NOT NULL THEN CONVERT(varchar(10), CAST(f.[DATA_ENVIO_QIGGER] AS date), 103) ELSE '''' END AS [DataEnvioQigger],
			ISNULL(CONVERT(varchar(255), f.[RESPONSAVEL_VENDEDOR]), '''') AS [NomeVendedor],
			ISNULL(CONVERT(varchar(255), f.[SUPERVISOR]), '''')            AS [Supervisor]
        FROM final_prep f
        '
    ) AS src
),

-- -------------------------------------------------------------------
-- STATUS/RESPONSÁVEL DO DUCK (normalizado)
-- -------------------------------------------------------------------
duck_status AS (
    SELECT
        regexp_replace(trim(CAST(telefone AS VARCHAR)), '[^0-9]', '', 'g') AS telefone_norm,
        regexp_replace(trim(CAST(cpf AS VARCHAR)), '[^0-9]', '', 'g') AS cpf_norm,
        upper(trim(translate(coalesce(CAST(status_duck AS VARCHAR), ''),
            'ÁÀÂÃÄáàâãäÉÈÊËéèêëÍÌÎÏíìîïÓÒÔÕÖóòôõöÚÙÛÜúùûüÇçÑñ',
            'AAAAAaaaaaEEEEeeeeIIIIiiiiOOOOOoooooUUUUuuuuCcNn'))) AS status_bruto,
        upper(trim(translate(coalesce(CAST(responsavel_duck AS VARCHAR), ''),
            'ÁÀÂÃÄáàâãäÉÈÊËéèêëÍÌÎÏíìîïÓÒÔÕÖóòôõöÚÙÛÜúùûüÇçÑñ',
            'AAAAAaaaaaEEEEeeeeIIIIiiiiOOOOOoooooUUUUuuuuCcNn'))) AS responsavel_bruto
    FROM stg_status_responsavel_duck
),

-- -------------------------------------------------------------------
-- MATCH STATUS SQL SERVER: telefone tem prioridade, CPF só se telefone nulo
-- -------------------------------------------------------------------
match_sql AS (
    SELECT
        b."Código externo",
        coalesce(dt.status_bruto,      dc.status_bruto)      AS status_bruto,
        coalesce(dt.responsavel_bruto, dc.responsavel_bruto) AS responsavel_bruto
    FROM base_sqlserver b
    LEFT JOIN duck_status dt
        ON b."Número de acesso" <> ''
       AND coalesce(dt.telefone_norm, '') <> ''
       AND b."Número de acesso" = dt.telefone_norm
    LEFT JOIN duck_status dc
        ON coalesce(dt.telefone_norm, '') = ''  -- só entra se telefone NÃO bateu
       AND b."CPF Limpo" <> ''
       AND coalesce(dc.cpf_norm, '') <> ''
       AND b."CPF Limpo" = dc.cpf_norm
),

-- -------------------------------------------------------------------
-- PARAMETRIZAÇÃO DE PLANO - SQL SERVER
-- -------------------------------------------------------------------
base_parametrizada AS (
    SELECT
        b."Código externo",
        p.plano_parametrizado
    FROM (
        SELECT
            *,
            CASE
                WHEN upper(trim(coalesce("__Plano Base", ''))) LIKE 'TIM CONTROLE LIGACOES ILIMITAD%'
                THEN 'TIM CONTROLE LIGACOES ILIMITADAS'
                ELSE upper(trim(coalesce("__Plano Base", '')))
            END AS plano_base_norm,
            try_cast(replace(regexp_replace(trim(coalesce(CAST("Preço" AS VARCHAR), '')), '[^0-9,.-]', '', 'g'), ',', '.') AS DECIMAL(10,2)) AS preco_num,
            try_cast(replace(regexp_replace(trim(coalesce(CAST("__Desc Fidel Raw" AS VARCHAR), '')), '[^0-9,.-]', '', 'g'), ',', '.') AS DECIMAL(10,2)) AS desc_fidel_num,
            try_cast(replace(regexp_replace(trim(coalesce(CAST("__Franquia Raw" AS VARCHAR), '')), '[^0-9,.-]', '', 'g'), ',', '.') AS DECIMAL(10,2)) AS franquia_num,
            try_cast(replace(regexp_replace(trim(coalesce(CAST("__GB Bonus Raw" AS VARCHAR), '')), '[^0-9,.-]', '', 'g'), ',', '.') AS DECIMAL(10,2)) AS bonus_num,
            try_cast(replace(regexp_replace(trim(coalesce(CAST("__GB Total Raw" AS VARCHAR), '')), '[^0-9,.-]', '', 'g'), ',', '.') AS DECIMAL(10,2)) AS gb_total_num
        FROM base_sqlserver
    ) b
    LEFT JOIN plano_parametros p
        ON b.plano_base_norm  = p.nome_eva
       AND b.preco_num        = p.valor_fidel
       AND b.franquia_num     = p.franquia
       AND b.bonus_num        = p.bonus_12m
       AND b.gb_total_num     = p.gb_12_meses
),

-- -------------------------------------------------------------------
-- RESULTADO FINAL SQL SERVER
-- -------------------------------------------------------------------
base_sql_final AS (
    SELECT
        b."Número de acesso",
        b."CPF Limpo",
        b."Nome do cliente",
        b."Data de nascimento",
        b."Nome da mãe",
        b."Cep",
        b."Estado",
        b."Logradouro",
        b."Número",
        b."Complemento",
        b."Referência",
        b."Bairro",
        b."Cidade",
        b."Email",
        coalesce(bp.plano_parametrizado, b."Plano") AS "Plano",
        b."Preço",
        b."Tipo do pagamento",
        b."Tipo da fatura",
        b."Banco",
        b."Agência",
        b."Conta",
        b."Data de vencimento da fatura",
        b."Data da venda",
        b."Tem alteração cadastral?",
        b."Código externo",
CASE
    -- Se PROTOCOLO_QIGGER preenchido → forçar APROVADA
    WHEN coalesce(trim(b."__Protocolo Qigger"), '') <> ''
    THEN 'APROVADA'
    -- Se status original já é APROVADA → manter (imutável)
    WHEN upper(trim(coalesce(b."__Status Original", ''))) IN ('APROVADA', 'APROVADA MANUAL', 'APROVADA ROBO', 'APROVADA TL')
    THEN 'APROVADA'
    -- Duck tem prioridade para os demais
    WHEN coalesce(trim(ms.status_bruto), '') <> ''
    THEN CASE WHEN upper(trim(ms.status_bruto)) IN ('APROVADA', 'APROVADA MANUAL', 'APROVADA ROBO', 'APROVADA TL')
              THEN 'APROVADA'
              WHEN upper(trim(ms.status_bruto)) = 'DADOS CADASTRIS INVALIDOS'
              THEN 'DADOS CADASTRAIS INVALIDOS'
              ELSE upper(trim(ms.status_bruto)) END
    WHEN coalesce(trim(b."__Status Original"), '') <> ''
    THEN CASE WHEN upper(trim(b."__Status Original")) = 'DADOS CADASTRIS INVALIDOS'
              THEN 'DADOS CADASTRAIS INVALIDOS'
              ELSE upper(trim(b."__Status Original")) END
    ELSE 'PENDENTE'
END AS "Status",

CASE
    WHEN coalesce(trim(ms.responsavel_bruto), '') <> ''
    THEN upper(trim(ms.responsavel_bruto))
    WHEN coalesce(trim(b."__Backoffice Original"), '') <> ''
     AND upper(trim(b."__Backoffice Original")) NOT IN (
         'APROVADA','PENDENTE','DDD DIVERGENTE','CLIENTE JA MIGRADO',
         'LIMITE DE CREDITO','RESTRICAO INTERNA','RESTRICAO DE MERCADO',
         'DADOS CADASTRAIS INVALIDOS','SCORE','ERRO SISTEMICO'
     )
    THEN CASE 
              WHEN upper(trim(b."__Backoffice Original")) IN ('KAUÃ','KAUA') THEN 'KAUA'
              ELSE upper(trim(b."__Backoffice Original")) 
         END
    ELSE 'SEM_RESPONSAVEL'
END AS "Responsável",
        '' AS "__Data Emissao Excel",
        b."__Protocolo Qigger",
        b."__Data Envio Qigger",
        coalesce(b."__Nome Vendedor", '') AS "__Nome Vendedor",
        coalesce(b."__Supervisor", '')    AS "__Supervisor",
        'SQLSERVER' AS "Origem Registro"
    FROM base_sqlserver b
    LEFT JOIN match_sql ms ON b."Código externo" = ms."Código externo"
    LEFT JOIN base_parametrizada bp ON b."Código externo" = bp."Código externo"
),

-- -------------------------------------------------------------------
-- PLANILHA EXCEL - NORMALIZAÇÃO
-- -------------------------------------------------------------------
excel_norm AS (
    SELECT
        regexp_replace(trim(coalesce(CAST("[Telefone Discado]" AS VARCHAR), '')), '[^0-9]', '', 'g') AS telefone_norm,
        regexp_replace(trim(coalesce(CAST(CPF AS VARCHAR), '')), '[^0-9]', '', 'g') AS cpf_norm,
        upper(trim(translate(coalesce(CAST("[Nome Cliente]" AS VARCHAR), ''),
            'ÁÀÂÃÄáàâãäÉÈÊËéèêëÍÌÎÏíìîïÓÒÔÕÖóòôõöÚÙÛÜúùûüÇçÑñ',
            'AAAAAaaaaaEEEEeeeeIIIIiiiiOOOOOoooooUUUUuuuuCcNn'))) AS nome_cliente,
        trim(coalesce(CAST(Aniversario AS VARCHAR), '')) AS data_nascimento,
        upper(trim(translate(coalesce(CAST("[Nome Mae]" AS VARCHAR), ''),
            'ÁÀÂÃÄáàâãäÉÈÊËéèêëÍÌÎÏíìîïÓÒÔÕÖóòôõöÚÙÛÜúùûüÇçÑñ',
            'AAAAAaaaaaEEEEeeeeIIIIiiiiOOOOOoooooUUUUuuuuCcNn'))) AS nome_mae,
        regexp_replace(trim(coalesce(CAST(CEP AS VARCHAR), '')), '[^0-9]', '', 'g') AS cep_limpo,
        upper(trim(translate(coalesce(CAST(UF AS VARCHAR), ''),
            'ÁÀÂÃÄáàâãäÉÈÊËéèêëÍÌÎÏíìîïÓÒÔÕÖóòôõöÚÙÛÜúùûüÇçÑñ',
            'AAAAAaaaaaEEEEeeeeIIIIiiiiOOOOOoooooUUUUuuuuCcNn'))) AS estado,
        upper(trim(translate(coalesce(CAST("[Tipo Logrdouro]" AS VARCHAR), ''),
            'ÁÀÂÃÄáàâãäÉÈÊËéèêëÍÌÎÏíìîïÓÒÔÕÖóòôõöÚÙÛÜúùûüÇçÑñ',
            'AAAAAaaaaaEEEEeeeeIIIIiiiiOOOOOoooooUUUUuuuuCcNn'))) AS tipo_logradouro,
        upper(trim(translate(coalesce(CAST(Logradouro AS VARCHAR), ''),
            'ÁÀÂÃÄáàâãäÉÈÊËéèêëÍÌÎÏíìîïÓÒÔÕÖóòôõöÚÙÛÜúùûüÇçÑñ',
            'AAAAAaaaaaEEEEeeeeIIIIiiiiOOOOOoooooUUUUuuuuCcNn'))) AS logradouro_base,
        trim(coalesce(CAST(Numero AS VARCHAR), '')) AS numero,
        '' AS complemento,
        upper(trim(translate(coalesce(CAST("[Endereco Principal]" AS VARCHAR), ''),
            'ÁÀÂÃÄáàâãäÉÈÊËéèêëÍÌÎÏíìîïÓÒÔÕÖóòôõöÚÙÛÜúùûüÇçÑñ',
            'AAAAAaaaaaEEEEeeeeIIIIiiiiOOOOOoooooUUUUuuuuCcNn'))) AS referencia,
        upper(trim(translate(coalesce(CAST(Bairro AS VARCHAR), ''),
            'ÁÀÂÃÄáàâãäÉÈÊËéèêëÍÌÎÏíìîïÓÒÔÕÖóòôõöÚÙÛÜúùûüÇçÑñ',
            'AAAAAaaaaaEEEEeeeeIIIIiiiiOOOOOoooooUUUUuuuuCcNn'))) AS bairro,
        upper(trim(translate(coalesce(CAST(Cidade AS VARCHAR), ''),
            'ÁÀÂÃÄáàâãäÉÈÊËéèêëÍÌÎÏíìîïÓÒÔÕÖóòôõöÚÙÛÜúùûüÇçÑñ',
            'AAAAAaaaaaEEEEeeeeIIIIiiiiOOOOOoooooUUUUuuuuCcNn'))) AS cidade,
        lower(trim(coalesce(CAST(EMail AS VARCHAR), ''))) AS email,
        upper(trim(translate(coalesce(CAST("[Plano Indicado]" AS VARCHAR), ''),
            'ÁÀÂÃÄáàâãäÉÈÊËéèêëÍÌÎÏíìîïÓÒÔÕÖóòôõöÚÙÛÜúùûüÇçÑñ',
            'AAAAAaaaaaEEEEeeeeIIIIiiiiOOOOOoooooUUUUuuuuCcNn'))) AS plano,
        trim(coalesce(CAST("VALOR FINAL" AS VARCHAR), '')) AS preco,
        upper(trim(translate(coalesce(CAST("[Forma Pagamento]" AS VARCHAR), 'FATURA'),
            'ÁÀÂÃÄáàâãäÉÈÊËéèêëÍÌÎÏíìîïÓÒÔÕÖóòôõöÚÙÛÜúùûüÇçÑñ',
            'AAAAAaaaaaEEEEeeeeIIIIiiiiOOOOOoooooUUUUuuuuCcNn'))) AS tipo_pagamento,
        CASE
            WHEN trim(coalesce(CAST(EMail AS VARCHAR), '')) <> '' THEN 'ONLINE'
            ELSE 'DETALHADA'
        END AS tipo_fatura,
        trim(coalesce(CAST("[Data Vencimento]" AS VARCHAR), '')) AS data_vencimento_fatura,
        strftime(CAST("[Data Atendimento]" AS INTEGER) + DATE '1899-12-30', '%d/%m/%Y') AS data_venda,
        concat(
            'PLAN_',
            lpad(
                cast(row_number() over (
                    order by
                        coalesce(CAST("[Data Atendimento]" AS VARCHAR), ''),
                        coalesce(CAST("[Telefone Discado]" AS VARCHAR), ''),
                        coalesce(CAST(CPF AS VARCHAR), ''),
                        coalesce(CAST("[Nome Cliente]" AS VARCHAR), '')
                ) as varchar),
                10, '0'
            )
        ) AS codigo_externo_excel,
        upper(trim(translate(coalesce(CAST(STATUS AS VARCHAR), 'PENDENTE'),
            'ÁÀÂÃÄáàâãäÉÈÊËéèêëÍÌÎÏíìîïÓÒÔÕÖóòôõöÚÙÛÜúùûüÇçÑñ',
            'AAAAAaaaaaEEEEeeeeIIIIiiiiOOOOOoooooUUUUuuuuCcNn'))) AS status_excel,
        upper(trim(translate(coalesce(CAST("BACKOFFICE RESPONSAVEL" AS VARCHAR), 'SEM_RESPONSAVEL'),
            'ÁÀÂÃÄáàâãäÉÈÊËéèêëÍÌÎÏíìîïÓÒÔÕÖóòôõöÚÙÛÜúùûüÇçÑñ',
            'AAAAAaaaaaEEEEeeeeIIIIiiiiOOOOOoooooUUUUuuuuCcNn'))) AS responsavel_excel,
        -- Data emissão da planilha (serial Excel: dias desde 1899-12-30)
        CASE
            WHEN coalesce(trim(CAST("DATA EMISSÃO" AS VARCHAR)), '') = '' THEN ''
            WHEN regexp_matches(trim(CAST("DATA EMISSÃO" AS VARCHAR)), '^\d+$')
            THEN strftime(CAST(CAST("DATA EMISSÃO" AS INTEGER) + DATE '1899-12-30' AS DATE), '%d/%m/%Y')
            ELSE trim(CAST("DATA EMISSÃO" AS VARCHAR))
        END AS data_emissao_excel
    FROM raw_vendas_excel
    WHERE (CAST("[Data Atendimento]" AS INTEGER) + DATE '1899-12-30') >= DATE '2026-03-01'
),

-- -------------------------------------------------------------------
-- PLANILHA EXCEL - MATCH STATUS DO DUCK
-- CORREÇÃO: CPF só entra se telefone NÃO bateu
-- -------------------------------------------------------------------
excel_duck_match AS (
    SELECT
        e.codigo_externo_excel,
        coalesce(dt.status_duck,      dc.status_duck)      AS status_duck,
        coalesce(dt.responsavel_duck, dc.responsavel_duck) AS responsavel_duck
    FROM excel_norm e
    LEFT JOIN stg_status_responsavel_duck dt
        ON e.telefone_norm <> ''
       AND coalesce(dt.telefone, '') <> ''
       AND e.telefone_norm = dt.telefone
    LEFT JOIN stg_status_responsavel_duck dc
        ON coalesce(dt.telefone, '') = ''  -- só entra se telefone NÃO bateu
       AND e.cpf_norm <> ''
       AND coalesce(dc.cpf, '') <> ''
       AND e.cpf_norm = dc.cpf
),

-- -------------------------------------------------------------------
-- PLANILHA EXCEL - PARAMETRIZAÇÃO DE PLANO
-- -------------------------------------------------------------------
excel_parametrizada AS (
    SELECT
        e.codigo_externo_excel,
        (
            SELECT p.plano_parametrizado
            FROM plano_parametros p
            WHERE CASE
                    WHEN e.plano LIKE 'TIM CONTROLE LIGACOES ILIMITAD%'
                    THEN 'TIM CONTROLE LIGACOES ILIMITADAS'
                    ELSE e.plano
                  END = p.nome_eva
              AND try_cast(replace(e.preco, ',', '.') AS DECIMAL(10,2)) = p.valor_fidel
            LIMIT 1
        ) AS plano_parametrizado
    FROM excel_norm e
),

-- -------------------------------------------------------------------
-- PLANILHA EXCEL - RESULTADO FINAL
-- -------------------------------------------------------------------
excel_final AS (
    SELECT
        e.telefone_norm AS "Número de acesso",
        e.cpf_norm      AS "CPF Limpo",
        e.nome_cliente  AS "Nome do cliente",
        e.data_nascimento AS "Data de nascimento",
        e.nome_mae      AS "Nome da mãe",
        e.cep_limpo     AS "Cep",
        e.estado        AS "Estado",
        CASE
            WHEN e.logradouro_base LIKE 'RUA %' OR e.logradouro_base LIKE 'AVENIDA %'
              OR e.logradouro_base LIKE 'AV %'  OR e.logradouro_base LIKE 'ALAMEDA %'
              OR e.logradouro_base LIKE 'TRAVESSA %' OR e.logradouro_base LIKE 'TV %'
              OR e.logradouro_base LIKE 'RODOVIA %'  OR e.logradouro_base LIKE 'ESTRADA %'
              OR e.logradouro_base LIKE 'PRACA %'    OR e.logradouro_base LIKE 'LARGO %'
              OR e.logradouro_base LIKE 'VIELA %'    OR e.logradouro_base LIKE 'FAZENDA %'
            THEN e.logradouro_base
            WHEN coalesce(trim(e.tipo_logradouro), '') <> ''
             AND coalesce(trim(e.logradouro_base), '') <> ''
            THEN concat(
                CASE
                    WHEN e.tipo_logradouro IN ('R','RUA')          THEN 'RUA'
                    WHEN e.tipo_logradouro IN ('AV','AVENIDA')     THEN 'AVENIDA'
                    WHEN e.tipo_logradouro IN ('AL','ALAMEDA')     THEN 'ALAMEDA'
                    WHEN e.tipo_logradouro IN ('TRAV','TRAVESSA','TV') THEN 'TRAVESSA'
                    WHEN e.tipo_logradouro IN ('EST','ESTRADA')    THEN 'ESTRADA'
                    WHEN e.tipo_logradouro IN ('ROD','RODOVIA')    THEN 'RODOVIA'
                    WHEN e.tipo_logradouro IN ('PRACA')            THEN 'PRACA'
                    ELSE e.tipo_logradouro
                END, ' ', e.logradouro_base)
            ELSE e.logradouro_base
        END AS "Logradouro",
        e.numero        AS "Número",
        e.complemento   AS "Complemento",
        e.referencia    AS "Referência",
        e.bairro        AS "Bairro",
        e.cidade        AS "Cidade",
        e.email         AS "Email",
        coalesce(ep.plano_parametrizado, e.plano) AS "Plano",
        e.preco         AS "Preço",
        e.tipo_pagamento AS "Tipo do pagamento",
        e.tipo_fatura   AS "Tipo da fatura",
        ''              AS "Banco",
        ''              AS "Agência",
        ''              AS "Conta",
        e.data_vencimento_fatura AS "Data de vencimento da fatura",
        e.data_venda    AS "Data da venda",
        'SIM'           AS "Tem alteração cadastral?",
        e.codigo_externo_excel AS "Código externo",
        -- STATUS: duck tem prioridade, depois planilha, depois PENDENTE
        CASE
            WHEN coalesce(trim(edm.status_duck), '') <> ''
            THEN CASE WHEN upper(trim(edm.status_duck)) = 'DADOS CADASTRIS INVALIDOS'
                      THEN 'DADOS CADASTRAIS INVALIDOS'
                      ELSE upper(trim(edm.status_duck)) END
            WHEN coalesce(trim(e.status_excel), '') IN ('', 'PENDENTE') THEN 'PENDENTE'
            WHEN e.status_excel = 'DADOS CADASTRIS INVALIDOS' THEN 'DADOS CADASTRAIS INVALIDOS'
            ELSE e.status_excel
        END AS "Status",
        -- RESPONSÁVEL: duck tem prioridade, depois planilha, depois SEM_RESPONSAVEL
        CASE
            WHEN coalesce(trim(edm.responsavel_duck), '') <> ''
            THEN CASE WHEN upper(trim(edm.responsavel_duck)) IN ('KAUÃ','KAUA') THEN 'KAUA'
                      ELSE upper(trim(edm.responsavel_duck)) END
            WHEN coalesce(trim(e.responsavel_excel), '') IN ('', 'NULL', 'SEM_RESPONSAVEL') THEN 'SEM_RESPONSAVEL'
            WHEN upper(trim(e.responsavel_excel)) IN ('KAUÃ','KAUA') THEN 'KAUA'
            ELSE e.responsavel_excel
        END AS "Responsável",
        e.data_emissao_excel AS "__Data Emissao Excel",
        '' AS "__Protocolo Qigger",
        '' AS "__Data Envio Qigger",
        -- Vendedor/Supervisor: vazio para PLANILHA (não disponível na planilha Excel)
        '' AS "__Nome Vendedor",
        '' AS "__Supervisor",
        'PLANILHA' AS "Origem Registro"
    FROM excel_norm e
    LEFT JOIN excel_duck_match edm ON e.codigo_externo_excel = edm.codigo_externo_excel
    LEFT JOIN excel_parametrizada ep ON e.codigo_externo_excel = ep.codigo_externo_excel
    WHERE NOT EXISTS (
        SELECT 1 FROM base_sql_final s
        WHERE (
            (e.telefone_norm <> '' AND s."Número de acesso" <> ''
               AND e.telefone_norm = s."Número de acesso")
           OR (e.cpf_norm <> '' AND s."CPF Limpo" <> ''
               AND e.cpf_norm = s."CPF Limpo")
        )
        -- Só deduplica se mesma data de venda (evita remover vendas diferentes da mesma pessoa)
        AND coalesce(trim(e.data_venda), '') = coalesce(trim(s."Data da venda"), '')
    )
),

-- -------------------------------------------------------------------
-- REGISTROS DUCK SEM CORRESPONDÊNCIA EM NENHUMA FONTE
-- -------------------------------------------------------------------
duckdb_extra_final AS (
    SELECT
        coalesce(d.telefone, '') AS "Número de acesso",
        coalesce(d.cpf, '')      AS "CPF Limpo",
        '' AS "Nome do cliente",      '' AS "Data de nascimento",
        '' AS "Nome da mãe",          '' AS "Cep",
        '' AS "Estado",               '' AS "Logradouro",
        '' AS "Número",               '' AS "Complemento",
        '' AS "Referência",           '' AS "Bairro",
        '' AS "Cidade",               '' AS "Email",
        '' AS "Plano",                '' AS "Preço",
        '' AS "Tipo do pagamento",    '' AS "Tipo da fatura",
        '' AS "Banco",                '' AS "Agência",
        '' AS "Conta",
        '' AS "Data de vencimento da fatura",
        '' AS "Data da venda",        '' AS "Tem alteração cadastral?",
        '' AS "Código externo",
        CASE
            WHEN coalesce(trim(d.status_duck), '') = '' THEN 'PENDENTE'
            WHEN upper(trim(d.status_duck)) = 'DADOS CADASTRIS INVALIDOS' THEN 'DADOS CADASTRAIS INVALIDOS'
            ELSE upper(trim(d.status_duck))
        END AS "Status",
        CASE
            WHEN coalesce(trim(d.responsavel_duck), '') = '' THEN 'SEM_RESPONSAVEL'
            WHEN upper(trim(d.responsavel_duck)) IN ('KAUÃ','KAUA') THEN 'KAUA'
            ELSE upper(trim(d.responsavel_duck))
        END AS "Responsável",
        '' AS "__Data Emissao Excel",
        '' AS "__Protocolo Qigger",
        '' AS "__Data Envio Qigger",
        '' AS "__Nome Vendedor",
        '' AS "__Supervisor",
        'DUCKDB' AS "Origem Registro"
    FROM stg_status_responsavel_duck d
    WHERE NOT EXISTS (
        SELECT 1 FROM base_sql_final s
        WHERE (coalesce(d.telefone,'') <> '' AND s."Número de acesso" <> ''
               AND d.telefone = s."Número de acesso")
           OR (coalesce(d.cpf,'') <> '' AND s."CPF Limpo" <> ''
               AND d.cpf = s."CPF Limpo")
    )
    AND NOT EXISTS (
        SELECT 1 FROM excel_final e
        WHERE (coalesce(d.telefone,'') <> '' AND e."Número de acesso" <> ''
               AND d.telefone = e."Número de acesso")
           OR (coalesce(d.cpf,'') <> '' AND e."CPF Limpo" <> ''
               AND d.cpf = e."CPF Limpo")
    )
),

-- -------------------------------------------------------------------
-- UNIFICAÇÃO
-- -------------------------------------------------------------------
base_unificada AS (
    SELECT
        "Número de acesso",
        "CPF Limpo",
        "Nome do cliente",
        "Data de nascimento",
        "Nome da mãe",
        "Cep",
        "Estado",
        "Logradouro",
        "Número",
        "Complemento",
        "Referência",
        "Bairro",
        "Cidade",
        "Email",
        "Plano",
        "Preço",
        "Tipo do pagamento",
        "Tipo da fatura",
        "Banco",
        "Agência",
        "Conta",
        "Data de vencimento da fatura",
        "Data da venda",
        "Tem alteração cadastral?",
        "Código externo",
        "Status",
        "Responsável",
        "__Data Emissao Excel",
        "__Protocolo Qigger",
        "__Data Envio Qigger",
        "__Nome Vendedor",
        "__Supervisor",
        "Origem Registro"
    FROM base_sql_final
    UNION ALL
    SELECT
        "Número de acesso",
        "CPF Limpo",
        "Nome do cliente",
        "Data de nascimento",
        "Nome da mãe",
        "Cep",
        "Estado",
        "Logradouro",
        "Número",
        "Complemento",
        "Referência",
        "Bairro",
        "Cidade",
        "Email",
        "Plano",
        "Preço",
        "Tipo do pagamento",
        "Tipo da fatura",
        "Banco",
        "Agência",
        "Conta",
        "Data de vencimento da fatura",
        "Data da venda",
        "Tem alteração cadastral?",
        "Código externo",
        "Status",
        "Responsável",
        "__Data Emissao Excel",
        "__Protocolo Qigger",
        "__Data Envio Qigger",
        "__Nome Vendedor",
        "__Supervisor",
        "Origem Registro"
    FROM excel_final
    UNION ALL
    SELECT
        "Número de acesso",
        "CPF Limpo",
        "Nome do cliente",
        "Data de nascimento",
        "Nome da mãe",
        "Cep",
        "Estado",
        "Logradouro",
        "Número",
        "Complemento",
        "Referência",
        "Bairro",
        "Cidade",
        "Email",
        "Plano",
        "Preço",
        "Tipo do pagamento",
        "Tipo da fatura",
        "Banco",
        "Agência",
        "Conta",
        "Data de vencimento da fatura",
        "Data da venda",
        "Tem alteração cadastral?",
        "Código externo",
        "Status",
        "Responsável",
        "__Data Emissao Excel",
        "__Protocolo Qigger",
        "__Data Envio Qigger",
        "__Nome Vendedor",
        "__Supervisor",
        "Origem Registro"
    FROM duckdb_extra_final
)

-- -------------------------------------------------------------------
-- SELECT FINAL NO CABEÇALHO SOLICITADO
-- -------------------------------------------------------------------
SELECT
    u."Data da venda"                    AS "[Data Atendimento]",
    u."Número de acesso"                 AS "[Telefone Discado]",
    u."Nome do cliente"                  AS "[Nome Cliente]",
    u."CPF Limpo"                        AS "CPF",
    CASE
        WHEN coalesce(trim(u."Data de nascimento"), '') = '' THEN ''
        WHEN try_strptime(u."Data de nascimento", '%d/%m/%Y') IS NOT NULL
        THEN strftime(try_strptime(u."Data de nascimento", '%d/%m/%Y'), '%d/%m/%Y')
        WHEN try_strptime(u."Data de nascimento", '%Y-%m-%d') IS NOT NULL
        THEN strftime(try_strptime(u."Data de nascimento", '%Y-%m-%d'), '%d/%m/%Y')
        WHEN try_strptime(u."Data de nascimento", '%d/%m/%Y %H:%M:%S') IS NOT NULL
        THEN strftime(try_strptime(u."Data de nascimento", '%d/%m/%Y %H:%M:%S'), '%d/%m/%Y')
        WHEN try_strptime(u."Data de nascimento", '%Y-%m-%d %H:%M:%S') IS NOT NULL
        THEN strftime(try_strptime(u."Data de nascimento", '%Y-%m-%d %H:%M:%S'), '%d/%m/%Y')
        ELSE u."Data de nascimento"
    END                                  AS "Aniversario",
    u."Nome da mãe"                      AS "[Nome Mae]",
    u."Email"                            AS "EMail",
    u."Tem alteração cadastral?"         AS "[Troca Titularidade]",
    u."Plano"                            AS "[Plano Indicado]",
    NULL                                 AS "VALOR SIEBEL",
    NULL                                 AS "[Desconto Fidelizacao]",
    u."Preço"                            AS "VALOR FINAL",
    NULL                                 AS "[GB Score]",
    NULL                                 AS "[GB Bonus]",
    NULL                                 AS "[GB Total]",
    -- Data Vencimento: apenas o dia (DD) — ex: "15"
    CASE
        WHEN coalesce(trim(u."Data de vencimento da fatura"), '') = '' THEN ''
        -- Já é só o dia (1 ou 2 dígitos)
        WHEN regexp_matches(trim(u."Data de vencimento da fatura"), '^\d{1,2}$')
        THEN lpad(trim(u."Data de vencimento da fatura"), 2, '0')
        -- Formato dd/mm/aaaa → extrair dia
        WHEN try_strptime(u."Data de vencimento da fatura", '%d/%m/%Y') IS NOT NULL
        THEN strftime(try_strptime(u."Data de vencimento da fatura", '%d/%m/%Y'), '%d')
        -- Formato aaaa-mm-dd → extrair dia
        WHEN try_strptime(u."Data de vencimento da fatura", '%Y-%m-%d') IS NOT NULL
        THEN strftime(try_strptime(u."Data de vencimento da fatura", '%Y-%m-%d'), '%d')
        ELSE trim(u."Data de vencimento da fatura")
    END                                  AS "[Data Vencimento]",
    u."Tipo do pagamento"                AS "[Forma Pagamento]",
    u."Referência"                       AS "[Endereco Principal]",
    'BRASIL'                             AS "Pais",
    u."Cep"                              AS "CEP",
    u."Estado"                           AS "UF",
    u."Cidade"                           AS "Cidade",
    NULL                                 AS "[Tipo Logrdouro]",
    u."Logradouro"                       AS "Logradouro",
    u."Bairro"                           AS "Bairro",
    u."Número"                           AS "Numero",
    u."Complemento"                      AS "Complemento",
    NULL                                 AS "[Tipo Complemento]",
    u."Responsável"                      AS "BACKOFFICE RESPONSAVEL",
    CASE
        WHEN upper(trim(coalesce(u."Status", ''))) IN ('APROVADA', 'APROVADA MANUAL', 'APROVADA ROBO', 'APROVADA TL')
        THEN 'APROVADA'
        ELSE u."Status"
    END                                  AS "STATUS",
    CASE
        WHEN coalesce(trim(u."__Data Envio Qigger"), '') <> ''
        THEN u."__Data Envio Qigger"
        WHEN coalesce(trim(aud.data_final_processamento), '') <> ''
        THEN CASE
            WHEN try_strptime(aud.data_final_processamento, '%d/%m/%Y %H:%M:%S') IS NOT NULL
            THEN strftime(try_strptime(aud.data_final_processamento, '%d/%m/%Y %H:%M:%S'), '%d/%m/%Y')
            WHEN try_strptime(aud.data_final_processamento, '%d/%m/%Y') IS NOT NULL
            THEN aud.data_final_processamento
            ELSE aud.data_final_processamento
        END
        WHEN coalesce(trim(u."__Data Emissao Excel"), '') <> ''
        THEN u."__Data Emissao Excel"
        ELSE u."Data da venda"
    END                                  AS "DATA EMISSÃO",
    -- Vendedor e Supervisor: do EVA (SQLSERVER) ou vazio para outras origens
    coalesce(nullif(trim(u."__Nome Vendedor"), ''), '')  AS "RESPONSAVEL_VENDEDOR",
    coalesce(nullif(trim(u."__Supervisor"), ''), '')     AS "SUPERVISOR",
    CASE
        WHEN u."Origem Registro" = 'SQLSERVER' THEN 'OPERACAO'
        WHEN u."Origem Registro" = 'PLANILHA' THEN 'ENRIQUECIMENTO'
        WHEN u."Origem Registro" = 'DUCKDB' THEN 'ENRIQUECIMENTO'
        ELSE u."Origem Registro"
    END                                  AS "ORIGEM"
FROM base_unificada u
LEFT JOIN (
    SELECT numero_acesso, data_final_processamento, data_inicial_processamento,
           status_classificado, protocolo
    FROM v2.auditoria_vendas_tim
    WHERE (numero_acesso, versao) IN (
        SELECT numero_acesso, MAX(versao)
        FROM v2.auditoria_vendas_tim
        GROUP BY numero_acesso
    )
) aud ON regexp_replace(trim(coalesce(u."Número de acesso", '')), '[^0-9]', '', 'g') = aud.numero_acesso
WHERE try_strptime(NULLIF(u."Data da venda", ''), '%d/%m/%Y') >= DATE '2026-04-01'
  AND try_strptime(NULLIF(u."Data da venda", ''), '%d/%m/%Y') <=  DATE '2026-04-30'
ORDER BY try_strptime(NULLIF(u."Data da venda", ''), '%d/%m/%Y') ASC;

-- =====================================================================
-- EXIBIR RESULTADO NA TELA
-- =====================================================================
SELECT * FROM vw_vendas_layout_final;
