-- SQL to update fact_transaction with settle_date and settle_amount from payments_insider_payments_staging
-- Assumes that payments_insider_sales_staging has already been processed into fact_transaction
UPDATE t 
SET 
    t.settle_date = p.payment_date,
    t.settle_amount = p.transaction_amount,
    t.updated_at = GETDATE()
FROM
    app.fact_transaction t
INNER JOIN app.payments_insider_sales_staging s On (s.id = t.staging_record_id)
INNER JOIN app.payments_insider_payments_staging p On (s.card_number=p.card_number and s.authorization_code=p.authorization_code)
WHERE 
    p.source_file_id = :file_id -- payments file id
    AND t.staging_table = 'payments_insider_sales_staging'
    AND p.processed_to_final = 0
    AND s.processed_to_final = 1