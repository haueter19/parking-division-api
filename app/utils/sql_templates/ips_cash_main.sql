-- IPS Cash main SQL. Use file_id parameter.
SELECT
    CONVERT(DATETIME, CONVERT(VARCHAR, CAST(s.collection_date AS DATE), 120) + ' ' + s.collection_time) transaction_date,
    s.coin_revenue transaction_amount,
    CONVERT(DATETIME, CONVERT(VARCHAR, CAST(s.collection_date AS DATE), 120) + ' ' + s.collection_time) settle_date,
    s.coin_revenue settle_amount,
    'ips_cash_staging' staging_table,
    s.source_file_id,
    s.id staging_record_id, 
    1 As payment_method_id,
    d.device_id, 
    ss.settlement_system_id, 
    da.location_id, 
    1 as program_id,
    cc.charge_code_id,
    CAST(s.id As VARCHAR) reference_number
FROM app.ips_cash_staging s
INNER JOIN app.dim_device d ON (d.device_terminal_id = s.pole_ser_no)
INNER JOIN app.fact_device_assignment da ON (da.device_id = d.device_id 
                                            AND CONVERT(DATETIME, CONVERT(VARCHAR, CAST(s.collection_date AS DATE), 120) + ' ' + s.collection_time) >= da.assign_date 
                                            AND CONVERT(DATETIME, CONVERT(VARCHAR, CAST(s.collection_date AS DATE), 120) + ' ' + s.collection_time) < COALESCE(da.end_date, '9999-12-31'))
--INNER JOIN app.dim_payment_method pm On (s.partner_name=pm.payment_method_brand)
INNER JOIN app.dim_location l On (l.location_id=da.location_id)
INNER JOIN app.dim_charge_code cc On (da.location_id=cc.location_id AND 1=cc.program_type_id)
INNER JOIN app.dim_settlement_system ss On (ss.system_name='IPS')
WHERE
    s.source_file_id = :file_id
    AND s.processed_to_final = 0