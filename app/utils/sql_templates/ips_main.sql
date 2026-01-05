-- SQL query to process records from IPS_staging to final transaction table
-- First part handles transaction types that are not 'Coin & Card'
DECLARE @file_id INT = :file_id;

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
    CONVERT(DATETIME, CONVERT(VARCHAR, CAST(s.date AS DATE), 120) + ' ' + s.time) transaction_date,
    s.total transaction_amount, 
    s.date settle_date, 
    s.total settle_amount, 
    'ips_staging', 
    s.source_file_id, 
    s.id,
    pm.payment_method_id,
    d.device_id,
    ss.settlement_system_id,
    da.location_id,
    1,
    cc.charge_code_id,
    CASE
        WHEN s.transaction_id IS NULL THEN CONCAT(s.pole,'_',CAST(s.id As VARCHAR(12)),'_',CAST(s.source_file_id As VARCHAR(12)))
        ELSE s.transaction_id
    END As reference_number
FROM app.ips_staging s
INNER JOIN app.dim_device d ON (d.device_terminal_id = s.space_name)
INNER JOIN app.fact_device_assignment da ON (da.device_id = d.device_id AND s.date >= da.assign_date AND s.date < COALESCE(da.end_date, '9999-12-31'))
INNER JOIN app.dim_charge_code cc On (da.location_id=cc.location_id AND 1=cc.program_type_id)
INNER JOIN app.dim_payment_method pm On (
        CASE 
            WHEN s.card_type = 'VISA' THEN 'Visa' 
            WHEN s.card_type = 'MC' THEN 'Mastercard' 
            WHEN s.card_type = 'DISC' THEN 'Discover' 
            WHEN s.card_type IS NULL AND s.transaction_type = 'Coins' THEN 'Cash'
            WHEN s.card_type LIKE '%Remote%' THEN 'Remote/PBC'
            WHEN s.card_type LIKE '%PBC%' THEN 'Remote/PBC'
            ELSE s.card_type 
        END=pm.payment_method_brand
)
INNER JOIN app.dim_settlement_system ss On (ss.system_name='IPS')
WHERE
    s.source_file_id = @file_id
    AND s.processed_to_final = 0
    AND s.transaction_type != 'Coin & Card'

UNION ALL
-- 2nd section handles the coin portion of the 'Coin & Card' transactions
SELECT
    CONVERT(DATETIME, CONVERT(VARCHAR, CAST(s.date AS DATE), 120) + ' ' + s.time),
    s.coin, 
    s.date, 
    s.coin, 
    'ips_staging', 
    s.source_file_id, 
    s.id,
    pm.payment_method_id,
    d.device_id,
    ss.settlement_system_id,
    da.location_id,
    1,
    cc.charge_code_id,
    CASE
        WHEN s.transaction_id IS NULL THEN CONCAT(s.pole,'_',CAST(s.id As VARCHAR(12)),'_',CAST(s.source_file_id As VARCHAR(12)),'_COIN')
        ELSE CONCAT(s.transaction_id, '_COIN')
    END As reference_number
FROM app.ips_staging s
INNER JOIN app.dim_device d ON (d.device_terminal_id = s.space_name)
INNER JOIN app.fact_device_assignment da ON (da.device_id = d.device_id AND s.date >= da.assign_date AND s.date < COALESCE(da.end_date, '9999-12-31'))
INNER JOIN app.dim_charge_code cc On (da.location_id=cc.location_id AND 1=cc.program_type_id)
INNER JOIN app.dim_payment_method pm On (pm.payment_method_brand='Cash')
INNER JOIN app.dim_settlement_system ss On (ss.system_name='IPS')
WHERE
    s.source_file_id = @file_id
    AND s.processed_to_final = 0
    AND s.transaction_type = 'Coin & Card'
    AND s.coin > 0
    
UNION ALL
-- 3rd section handles the card portion of the 'Coin & Card' transactions
SELECT
    CONVERT(DATETIME, CONVERT(VARCHAR, CAST(s.date AS DATE), 120) + ' ' + s.time),
    s.credit_card,
    s.date,
    s.credit_card, 
    'ips_staging',
    s.source_file_id, 
    s.id,
    pm.payment_method_id,
    d.device_id,
    ss.settlement_system_id,
    da.location_id,
    1,
    cc.charge_code_id,
    CASE
        WHEN s.transaction_id IS NULL THEN CONCAT(s.pole,'_',CAST(s.id As VARCHAR(12)),'_',CAST(s.source_file_id As VARCHAR(12)),'_CARD')
        ELSE CONCAT(s.transaction_id, '_CARD')
    END As reference_number
FROM app.ips_staging s
INNER JOIN app.dim_device d ON (d.device_terminal_id = s.space_name)
INNER JOIN app.fact_device_assignment da ON (da.device_id = d.device_id AND s.date >= da.assign_date AND s.date < COALESCE(da.end_date, '9999-12-31'))
INNER JOIN app.dim_charge_code cc On (da.location_id=cc.location_id AND 1=cc.program_type_id)
INNER JOIN app.dim_payment_method pm On (
    CASE 
        WHEN s.card_type = 'VISA' THEN 'Visa' 
        WHEN s.card_type = 'MC' THEN 'Mastercard' 
        WHEN s.card_type = 'DISC' THEN 'Discover' 
        ELSE s.card_type 
    END=pm.payment_method_brand
)
INNER JOIN app.dim_settlement_system ss On (ss.system_name='IPS')
WHERE
    s.source_file_id = @file_id
    AND s.processed_to_final = 0
    AND s.transaction_type = 'Coin & Card'
    AND s.credit_card > 0