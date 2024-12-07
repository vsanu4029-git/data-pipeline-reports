-- FUNCTION: deviceSchema.func_dim_accounts_refresh()

-- DROP FUNCTION IF EXISTS "deviceSchema".func_dim_accounts_refresh();

CREATE OR REPLACE FUNCTION "deviceSchema".func_dim_accounts_refresh(
	)
    RETURNS void
    LANGUAGE 'sql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
INSERT INTO "deviceSchema".dim_accounts
SELECT		"accountAnonExternalIdNew",
			"partnerCustomerId",
			"tcid",
			"partnerId",
			"mak",
			"optStatus",
			"updateDate"   
FROM
(
SELECT 		*,
			row_number() OVER (partition by "accountAnonExternalIdNew" order by "updateDate" desc) as rnk
FROM
(
SELECT      	"accountAnonExternalIdNew",
                "partnerCustomerId",
                "tcid",
                "partnerId",
                "mak",
                "optStatus",
                "updateDate"      
FROM
(
    SELECT 		CONCAT("accountAnonExternalId",'_000000',SUBSTRING("partnerId",LENGTH("partnerId")-3,4)) as "accountAnonExternalIdNew",
                "partnerCustomerId",
                COALESCE("tiVoCustomerId","partnerCustomerId") as "tcid",
               	CONCAT('000000',SUBSTRING("partnerId",LENGTH("partnerId")-3,4)) "partnerId",
                "mak",
                "optStatus",
                "updateDate",
                row_number() OVER (PARTITION by COALESCE("tiVoCustomerId","partnerCustomerId") order by "updateDate" desc) as rn
    FROM 		"deviceSchema".stg_transientfeaccounthistory
)
where rn = 1

UNION

SELECT      	"accountAnonExternalIdNew",
                "partnerCustomerId",
                "tcid",
                "partnerId",
                "mak",
                "optStatus",
                "updateDate"        
FROM
(
    SELECT 		CONCAT("accountAnonExternalId",'_000000',SUBSTRING("partnerId",LENGTH("partnerId")-3,4)) as "accountAnonExternalIdNew",
                "partnerCustomerId",
                COALESCE("tiVoCustomerId","partnerCustomerId") as "tcid",
               	CONCAT('000000',SUBSTRING("partnerId",LENGTH("partnerId")-3,4)) "partnerId",
                "mak",
                "optStatus",
                "updateDate",
                row_number() OVER (PARTITION by COALESCE("tiVoCustomerId","partnerCustomerId") order by "updateDate" desc) as rn
    FROM 		"deviceSchema".stg_transientfeaccounthistorylatam
)
where rn = 1
) as t1
	) where rnk = 1

ON CONFLICT ("accountAnonExternalId")
DO UPDATE 
SET
"accountAnonExternalId" = EXCLUDED."accountAnonExternalId",
"partnerCustomerId" = EXCLUDED."partnerCustomerId",
"tiVoCustomerId" = EXCLUDED."tiVoCustomerId",
"partnerId" = EXCLUDED."partnerId",
mak = EXCLUDED.mak,
"optStatus" = EXCLUDED."optStatus",
"updateDate" = EXCLUDED."updateDate"
$BODY$;

ALTER FUNCTION "deviceSchema".func_dim_accounts_refresh()
    OWNER TO postgres;
