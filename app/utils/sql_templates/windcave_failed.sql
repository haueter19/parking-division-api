-- Paste the INSERT ... SELECT SQL that inserts rejected windcave rows into app.fact_transaction_reject
-- Use :file_id as the parameter placeholder.
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
    'windcave_staging',
    s.id,
    s.source_file_id,
    CASE
        WHEN d.device_id IS NULL THEN 'DEVICE_NOT_FOUND'
        WHEN da.device_id IS NULL THEN 'NO_ACTIVE_DEVICE_ASSIGNMENT'
        WHEN da.location_id IS NULL THEN 'LOCATION_NOT_FOUND'
        WHEN cc.charge_code_id IS NULL THEN 'CHARGE_CODE_NOT_FOUND'
        WHEN pm.payment_method_id IS NULL THEN 'PAYMENT_METHOD_NOT_FOUND'
        WHEN ss.settlement_system_id IS NULL THEN 'SETTLEMENT_SYSTEM_NOT_FOUND'
        ELSE 'UNKNOWN_ERROR'
    END AS reject_reason_code,
    GETDATE(),
    s.device_id,
    s.time,
    s.amount,
    COALESCE(CAST(pm.payment_method_id As VARCHAR(10)), 'NO_PAYMENT_METHOD') payment_method,
    COALESCE(CAST(d.device_id As VARCHAR(10)), 'DEVICE_NOT_FOUND') device_id,
    COALESCE(CAST(ss.settlement_system_id As VARCHAR(10)), 'SETTLEMENT_SYSTEM_NOT_FOUND') settlement_system_id,
    COALESCE(CAST(da.location_id As VARCHAR(10)), 'LOCATION_NOT_FOUND') location_id,
    COALESCE(CAST(cc.charge_code_id As VARCHAR(10)), 'CHARGE_CODE_NOT_FOUND') charge_code_id
FROM app.windcave_staging s
LEFT JOIN app.dim_device d ON (d.device_terminal_id = CASE WHEN s.device_id LIKE '[A-Z]%' THEN s.device_id ELSE LEFT(s.txnref,3) END)
LEFT JOIN app.fact_device_assignment da ON (da.device_id = d.device_id AND s.time >= da.assign_date AND s.time <COALESCE(da.end_date, '9999-12-31'))
LEFT JOIN app.dim_payment_method pm On (s.card_type=pm.payment_method_brand)
LEFT JOIN app.dim_charge_code cc On (da.location_id=cc.location_id AND 1=cc.program_type_id)
LEFT JOIN app.dim_settlement_system ss On (ss.system_name='Windcave')
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
    AND s.voided = 0