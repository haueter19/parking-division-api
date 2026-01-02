-- Payments Insider main SQL (Sales->fact_transaction). Use file_id parameter.
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
    CONVERT(DATETIME, CONVERT(VARCHAR, CAST(s.transaction_date AS DATE), 120) + ' ' + s.transaction_time) transaction_date, 
    s.transaction_amount, 
    p.payment_date settle_date, 
    p.transaction_amount settle_amount, 
    'payments_insider_sales_staging' staging_table, 
    s.source_file_id, 
    s.id, 
    pm.payment_method_id, 
    d.device_id, 
    settlement_system_id, 
    da.location_id, 
    CASE WHEN d.device_type = 'Portable CC Reader' THEN 2 ELSE 1 END program_id,
    cc.charge_code_id,
    CONCAT(REPLACE(s.card_number, '*', ''),s.authorization_code) reference_number
FROM app.payments_insider_sales_staging s 
LEFT JOIN app.payments_insider_payments_staging p On (s.card_number=p.card_number and s.authorization_code=p.authorization_code)
INNER JOIN app.dim_payment_method pm On (s.card_brand=pm.payment_method_brand)
INNER JOIN app.dim_device d ON (d.terminal_id = s.terminal_id)
INNER JOIN app.fact_device_assignment da ON (da.device_id = d.device_id AND CONVERT(DATETIME, CONVERT(VARCHAR, CAST(s.transaction_date AS DATE), 120) + ' ' + s.transaction_time) >= da.assign_date AND CONVERT(DATETIME, CONVERT(VARCHAR, CAST(s.transaction_date AS DATE), 120) + ' ' + s.transaction_time) < COALESCE(da.end_date, '9999-12-31'))
INNER JOIN app.dim_charge_code cc On (da.location_id=cc.location_id AND cc.program_type_id=CASE WHEN d.device_type = 'Portable CC Reader' THEN 2 ELSE 1 END)
INNER JOIN app.dim_settlement_system ss On (ss.system_name='PI')
WHERE 
    s.source_file_id = :file_id
    AND s.processed_to_final = 0