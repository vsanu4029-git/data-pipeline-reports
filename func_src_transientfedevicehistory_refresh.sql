-- FUNCTION: deviceSchema.func_src_transientfedevicehistory_refresh()

-- DROP FUNCTION IF EXISTS "deviceSchema".func_src_transientfedevicehistory_refresh();

CREATE OR REPLACE FUNCTION "deviceSchema".func_src_transientfedevicehistory_refresh(
	)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
BEGIN
	TRUNCATE TABLE "deviceSchema".src_transientfedevicehistory;

	INSERT into "deviceSchema".src_transientfedevicehistory
		SELECT 		"bodyId", 
					"hardwareSerialNumber", 
					"caDeviceId", 
					"deviceType", 
					"manufacturedDate", 
					"serviceState" ,
					"updateDate",
					"activationDate",
					CONCAT('000000',SUBSTRING("partnerId",LENGTH("partnerId")-3,4)),
					CONCAT("accountAnonExternalId",'_000000',SUBSTRING("partnerId",LENGTH("partnerId")-3,4)) as "accountAnonExternalId"
		FROM 
			(SELECT 	*, 
						row_number() over (partition by "bodyId" order by "updateDate" desc) as row 
			FROM "deviceSchema".stg_transientfedevicehistory) 
			where row=1
			UNION
		SELECT 		"bodyId", 
					"hardwareSerialNumber", 
					"caDeviceId", 
					"deviceType", 
					"manufacturedDate", 
					"serviceState" ,
					"updateDate",
					"activationDate",
					CONCAT('000000',SUBSTRING("partnerId",LENGTH("partnerId")-3,4)),
					CONCAT("accountAnonExternalId",'_000000',SUBSTRING("partnerId",LENGTH("partnerId")-3,4)) as "accountAnonExternalId"
		FROM 
			(SELECT *, 
			 row_number() over (partition by "bodyId" order by "updateDate" desc) as row 
			 FROM "deviceSchema".stg_transientfedevicehistorylatam) 
		where row=1;
END;
$BODY$;

ALTER FUNCTION "deviceSchema".func_src_transientfedevicehistory_refresh()
    OWNER TO postgres;
