-- IPS Cash failed-records SQL. Use file_id parameter.
INSERT INTO app.fact_transaction_reject (
    staging_table,
    staging_record_id,
    source_file_id,
    reject_reason_code,
    rejected_at,
    source_device_terminal_id,
    transaction_datetime,
    transaction_amount,
    payment_method_id,
    device_id,
    settlement_system_id,
    location_id,
    charge_code_id
)
SELECT
    'ips_cash_staging',
    s.id,
    s.source_file_id,
    CASE
        WHEN da.device_id IS NULL THEN 'NO_ACTIVE_DEVICE_ASSIGNMENT'
        WHEN d.device_id IS NULL THEN 'DEVICE_NOT_FOUND'
        WHEN da.location_id IS NULL THEN 'LOCATION_NOT_FOUND'
        WHEN cc.charge_code_id IS NULL THEN 'CHARGE_CODE_NOT_FOUND'
        WHEN pm.payment_method_id IS NULL THEN 'PAYMENT_METHOD_NOT_FOUND'
        WHEN ss.settlement_system_id IS NULL THEN 'SETTLEMENT_SYSTEM_NOT_FOUND'
        ELSE 'UNKNOWN_ERROR'
    END AS reject_reason_code,
    GETDATE() rejected_at,
    s.pole_ser_no,
    CONVERT(DATETIME, CONVERT(VARCHAR, CAST(s.collection_date AS DATE), 120) + ' ' + s.collection_time) transaction_datetime,
    s.coin_revenue,
    COALESCE(CAST(pm.payment_method_id As VARCHAR(50)), 'NO_PAYMENT_METHOD') payment_method,
    COALESCE(CAST(d.device_id As VARCHAR(50)), 'DEVICE_NOT_FOUND') device_id,
    COALESCE(CAST(ss.settlement_system_id As VARCHAR(50)), 'SETTLEMENT_SYSTEM_NOT_FOUND') settlement_system_id,
    COALESCE(CAST(da.location_id As VARCHAR(50)), 'LOCATION_NOT_FOUND') location_id,
    COALESCE(CAST(cc.charge_code_id As VARCHAR(50)), 'CHARGE_CODE_NOT_FOUND') charge_code_id
FROM app.ips_cash_staging s
LEFT JOIN app.dim_device d ON (d.device_terminal_id = s.pole_ser_no)
LEFT JOIN app.fact_device_assignment da ON (da.device_id = d.device_id 
                                            AND CONVERT(DATETIME, CONVERT(VARCHAR, CAST(s.collection_date AS DATE), 120) + ' ' + s.collection_time) >= da.assign_date 
                                            AND CONVERT(DATETIME, CONVERT(VARCHAR, CAST(s.collection_date AS DATE), 120) + ' ' + s.collection_time) < COALESCE(da.end_date, '9999-12-31'))
LEFT JOIN app.dim_payment_method pm On ('Cash'=pm.payment_method_brand)
LEFT JOIN app.dim_charge_code cc On (da.location_id=cc.location_id AND 1=cc.program_type_id)
LEFT JOIN app.dim_settlement_system ss On (ss.system_name='IPS')
WHERE 
    s.source_file_id = :file_id
    AND (
        d.device_id IS NULL
        OR da.device_id IS NULL
        OR da.location_id IS NULL
        OR cc.charge_code_id IS NULL
        OR pm.payment_method_id IS NULL
        OR ss.settlement_system_id IS NULL
    )
    AND s.processed_to_final = 0