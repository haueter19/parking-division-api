-- IPS Mobile main SQL. Use :file_id parameter.
-- Example: INSERT INTO app.fact_transaction (...) SELECT ... FROM app.ips_mobile_staging s WHERE s.source_file_id = :file_id
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
    s.received_date_time transaction_date,
    s.paid transaction_amount,
    s.received_date_time settle_date,
    s.paid + s.convenience_fee settle_amount,
    'ips_mobile_staging' staging_table,
    s.source_file_id,
    s.id staging_record_id, 
    pm.payment_method_id,
    d.device_id, 
    ss.settlement_system_id, 
    da.location_id, 
    1 as program_id,
    cc.charge_code_id,
    s.prid reference_number
FROM app.ips_mobile_staging s
INNER JOIN app.dim_device d ON (d.device_terminal_id = s.space_name)
INNER JOIN app.fact_device_assignment da ON (da.device_id = d.device_id AND s.received_date_time >= da.assign_date AND s.received_date_time < COALESCE(da.end_date, '9999-12-31'))
INNER JOIN app.dim_payment_method pm On (s.partner_name=pm.payment_method_brand)
INNER JOIN app.dim_location l On (l.location_id=da.location_id)
INNER JOIN app.dim_charge_code cc On (da.location_id=cc.location_id AND 1=cc.program_type_id)
INNER JOIN app.dim_settlement_system ss On (ss.system_name='IPS')
WHERE
    s.source_file_id = :file_id
    AND s.processed_to_final = 0