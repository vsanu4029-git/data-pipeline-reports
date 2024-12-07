-- FUNCTION: deviceSchema.func_src_calldate_refresh()

-- DROP FUNCTION IF EXISTS "deviceSchema".func_src_calldate_refresh();

CREATE OR REPLACE FUNCTION "deviceSchema".func_src_calldate_refresh(
	)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
BEGIN
TRUNCATE "deviceSchema".src_calldate;

INSERT INTO "deviceSchema".src_calldate (
"insertTimeStamp", "callId", "tcdId", "callTime", disconnect, "ipAdd", "tcpPort", 
"sessionTime", "iocTects", "oocTects", "camId", "swVer", "callType", "serverName", 
"tollFreeAuth", "areaCode", "curPhNum", "callerId", "mfsTotMedia", "mfsTotApp", 
"irDb", "dialCode", hda, hdb, "postalCode", "headendId", "pgdLeft", "sourcesSt", 
"sourcesCon", "sourcesDrm", "sourcesEkr", "sourcesCcn", "sourcesBrn", "sourcesLin", "sourcesIrs", 
"configsZip", "configsDar", "configsRcq", "configsTz", "configsAsVal", "configsTun", "configsSuc", "configsSta", 
"configsExp", "phoneTollFreeAuth", "phoneAreaCode", "phoneDialPrefix", "phoneCallWaitPrefix", "phoneDialToneCheck", 
"phoneOffHookCheck", "phoneTonePulseDial", "phoneCurPhNum", "doneTimeStamp", broadband, "netInfoState", "netInfoExtstate", 
"netInfoRoute", "netInfoMedium", "netInfoName", "netIpInfoAuto", "netWirelessInfoAdhoc", "netWirelessInfoSignal", 
"netInfoMac", "configsDst", "configsInitState", nid, nrp, "controllerId", "plantId", skip
)
SELECT * FROM "deviceSchema".stg_bsmcalldate
UNION
SELECT * FROM "deviceSchema".stg_ibsmcalldate
;
END;
$BODY$;

ALTER FUNCTION "deviceSchema".func_src_calldate_refresh()
    OWNER TO postgres;
