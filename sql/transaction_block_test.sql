BEGIN TRAN;
UPDATE fact_employee_activity
SET status = 'BlockedTest'
WHERE activity_id = 1;
-- COMMIT
-- ROLLBACK
