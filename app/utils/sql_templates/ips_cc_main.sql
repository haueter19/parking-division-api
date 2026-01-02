-- IPS CC main SQL. Use file_id parameter.
INSERT INTO app.fact_transaction (
    transaction_date,
    transaction_amount,
    settle_date,
    settle_amount,
    staging_table,
    source_file_id,
    staging_record_id,
    payment_method_id,
    device_id,
    settlement_system_id,
    location_id,
    program_id,
    charge_code_id,
    reference_number
)
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
    ss.settlement_system_id, 
    da.location_id, 
    1 as program_id,
    cc.charge_code_id, 
    s.transaction_reference reference_number
FROM app.ips_cc_staging s
INNER JOIN app.dim_device d ON (d.device_terminal_id = s.pole)
INNER JOIN app.fact_device_assignment da ON (da.device_id = d.device_id AND s.transaction_date_time >= da.assign_date AND s.transaction_date_time < COALESCE(da.end_date, '9999-12-31'))
INNER JOIN app.dim_payment_method pm On (CASE WHEN s.card_type = 'VISA' THEN 'Visa' WHEN s.card_type = 'MC' THEN 'Mastercard' WHEN s.card_type = 'DISC' THEN 'Discover' ELSE s.card_type END=pm.payment_method_brand)
--INNER JOIN app.dim_location l On (l.location_id=da.location_id)
INNER JOIN app.dim_charge_code cc On (da.location_id=cc.location_id AND 1=cc.program_type_id)
INNER JOIN app.dim_settlement_system ss On (ss.system_name='IPS')
WHERE
    s.source_file_id = :file_id
    AND s.processed_to_final = 0