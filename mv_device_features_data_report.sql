-- View: deviceSchema.mv_device_features_data_report

-- DROP MATERIALIZED VIEW IF EXISTS "deviceSchema".mv_device_features_data_report;

CREATE MATERIALIZED VIEW IF NOT EXISTS "deviceSchema".mv_device_features_data_report
TABLESPACE pg_default
AS
 SELECT d."tiVoSerialNumber" AS tsn,
    i."codeName" AS "Code Name",
    i."externalProductName" AS "External Product Name",
    p."partnerParentName" AS "Partner Parent Name",
    d."serviceStateId" AS "Service State",
    d."softwareVersion" AS "Software Version",
    d."softwareVersionGroup-value" AS "softwareVersionGroup-code",
    i.series AS "Series",
        CASE
            WHEN d."kvGroup-groups" ~~ '%DG_tivomax_promo%'::text THEN 'Y'::text
            ELSE 'N'::text
        END AS dg_tivomax_promo,
        CASE
            WHEN d."kvGroup-groups" ~~ '%DG_tivomax_adroll%'::text THEN 'Y'::text
            ELSE 'N'::text
        END AS dg_tivomax_adroll,
        CASE
            WHEN d."kvGroup-groups" ~~ '%DG_tivomax_noadroll%'::text THEN 'Y'::text
            ELSE 'N'::text
        END AS dg_tivomax_noadroll,
        CASE
            WHEN d."kvGroup-groups" ~~ '%DG_retail_tivo_s5_US%'::text THEN 'Y'::text
            ELSE 'N'::text
        END AS dg_retail_tivo_s5_us,
        CASE
            WHEN d."kvGroup-groups" ~~ '%DG_retail_tivo_s6_US%'::text THEN 'Y'::text
            ELSE 'N'::text
        END AS dg_retail_tivo_s6_us,
        CASE
            WHEN d."kvGroup-groups" ~~ '%DG_retail_tivo_s7_US%'::text THEN 'Y'::text
            ELSE 'N'::text
        END AS dg_retail_tivo_s7_us,
        CASE
            WHEN d."kvGroup-groups" ~~ '%DG_retail_tivo_notuner_s6_US%'::text THEN 'Y'::text
            ELSE 'N'::text
        END AS dg_retail_tivo_notuner_s6_us,
        CASE
            WHEN d."kvGroup-groups" ~~ '%DG_webkit_tivoplus%'::text THEN 'Y'::text
            ELSE 'N'::text
        END AS dg_webkit_tivoplus,
        CASE
            WHEN d."kvGroup-groups" ~~ '%DG_webkit_tubitv%'::text THEN 'Y'::text
            ELSE 'N'::text
        END AS dg_webkit_tubitv,
        CASE
            WHEN d."kvGroup-groups" ~~ '%DG_webkit_plutotv%'::text THEN 'Y'::text
            ELSE 'N'::text
        END AS dg_webkit_plutotv
   FROM "deviceSchema".dim_devices d
     JOIN "deviceSchema".dim_items i ON d."skuNumber" = i."skuNumber"
     JOIN "deviceSchema".dim_partners p ON d."partnerId" = p."partnerId"
WITH DATA;

ALTER TABLE IF EXISTS "deviceSchema".mv_device_features_data_report
    OWNER TO postgres;