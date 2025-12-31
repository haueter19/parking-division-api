-- IPS CC main SQL. Use :file_id parameter.
-- Example: INSERT INTO app.fact_transaction (...) SELECT ... FROM app.ips_cc_staging s WHERE s.source_file_id = :file_id
SELECT
    s.transaction_date_time transaction_date, 
    s.amount transaction_amount, 
    s.transaction_date_time settle_date, 
    s.amount settle_amount, 
    'ips_cc_staging' staging_table, 
    s.source_file_id, 
    s.id As staging_record_id,
    pm.payment_method_id, 
    d.device_id, 
    4 As settlement_system_id, 
    da.location_id, 
    1 as program_id, 
    ss.settlement_system_id, 
    cc.charge_code_id, 
    s.transaction_reference reference_number
FROM app.ips_cc_staging s
INNER JOIN app.dim_device d ON (d.device_terminal_id = s.pole)
INNER JOIN app.fact_device_assignment da ON (da.device_id = d.device_id AND s.transaction_date_time >= da.assign_date AND s.transaction_date_time < COALESCE(da.end_date, '9999-12-31'))
INNER JOIN app.dim_payment_method pm On (s.card_type=pm.payment_method_brand)
INNER JOIN app.dim_location l On (l.location_id=da.location_id)
INNER JOIN app.dim_charge_code cc On (da.location_id=cc.location_id AND 1=cc.program_type_id)
INNER JOIN app.dim_settlement_system ss On (ss.system_name='IPS')
WHERE
    s.source_file_id = :file_id
    AND s.processed_to_final = 0