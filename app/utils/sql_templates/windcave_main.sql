-- Paste the main INSERT SELECT SQL used to move windcave staging into fact_transaction here.
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
    s.time,
    s.amount,
    s.settlement_date,
    s.amount,
    'windcave_staging',
    s.source_file_id,
    s.id,
    pm.payment_method_id,
    d.device_id,
    2, -- settlement_system_id hardcoded to 2 for Windcave transactions
    da.location_id,
    1, -- program_id hardcoded to 1 for Windcave transactions
    cc.charge_code_id,
    s.dpstxnref
FROM app.windcave_staging s
INNER JOIN app.dim_device d ON (d.device_terminal_id = CASE WHEN s.device_id LIKE '[A-Z]%' THEN s.device_id ELSE LEFT(s.txnref,3) END)
INNER JOIN app.fact_device_assignment da ON (da.device_id = d.device_id AND s.time >= da.assign_date AND s.time < COALESCE(da.end_date, '9999-12-31'))
INNER JOIN app.dim_payment_method pm ON (pm.payment_method_brand = s.card_type)
INNER JOIN app.dim_charge_code cc ON (cc.location_id = da.location_id AND cc.program_type_id = 1)
WHERE 
    s.source_file_id = :file_id
    AND s.processed_to_final = 0
    AND s.voided = 0   