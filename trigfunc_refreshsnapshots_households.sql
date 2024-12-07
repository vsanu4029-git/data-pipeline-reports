-- FUNCTION: deviceSchema.trigfunc_refreshsnapshots_households()

-- DROP FUNCTION IF EXISTS "deviceSchema".trigfunc_refreshsnapshots_households();

CREATE OR REPLACE FUNCTION "deviceSchema".trigfunc_refreshsnapshots_households()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF
AS $BODY$
DECLARE
	"originalCount" BIGINT;
	start_time TIMESTAMP;
	end_time TIMESTAMP;
BEGIN
	BEGIN
		start_time := clock_timestamp();
		"originalCount":=(select count(*) from "deviceSchema".dim_snapshot_daily_active_households);
		
		PERFORM "deviceSchema".func_dim_snapshots_daily_active_households_refresh();
		
		end_time := clock_timestamp();
		INSERT INTO "deviceSchema".log_transactions
		("transactionId", "transactionType", "transactionRunTime", "transactionEndTime", "tableName", status, "rowsCount", "originalRowCount")
		VALUES
		(NEXTVAL('seq_transactionid'),'Upsert',start_time, end_time,'dim_snapshot_daily_active_households','success',(select count(*) from "deviceSchema".dim_snapshot_daily_active_households), "originalCount");
	EXCEPTION
		WHEN unique_violation THEN
			RAISE NOTICE 'Unique violation occurred during UPSERT, skipping operation';
			INSERT INTO "deviceSchema".log_transactions
			("transactionId", "transactionType", "transactionRunTime", "transactionEndTime", "tableName", status, "message", "rowsCount", "originalRowCount")
			VALUES
			(NEXTVAL('seq_transactionid'),'Upsert',start_time, end_time,'dim_snapshot_daily_active_households','Warning', 'Unique violation occurred during UPSERT, skipping operation',(select count(*) from "deviceSchema".dim_snapshot_daily_active_households),"originalCount");
	
		WHEN others THEN
			INSERT INTO "deviceSchema".log_transactions
			("transactionId", "transactionType", "transactionRunTime", "transactionEndTime", "tableName", status, "message", "rowsCount", "originalRowCount")
			VALUES
			(NEXTVAL('seq_transactionid'),'Upsert',start_time, end_time,'dim_snapshot_daily_active_households','Errror', SQLERRM,(select count(*) from "deviceSchema".dim_snapshot_daily_active_households),"originalCount");
	END;

RETURN NEW;

END;
$BODY$;

ALTER FUNCTION "deviceSchema".trigfunc_refreshsnapshots_households()
    OWNER TO postgres;
