-- ============================================
-- Script: 03_insert_data.sql
-- Purpose: Insert test data to fact table
-- ============================================

-- Temporary variables
DECLARE @i INT = 0;

WHILE @i < 100000  -- max number records for generate
BEGIN
    INSERT INTO fact_employee_activity (
        employee_id,
        date_id,
        project_id,
        job_id,
        department_id,
        location_id,
        time_type_id,
        hours_worked,
        activity_type,
        status,
        overtime,
        start_time,
        end_time,
        comment
    )
    SELECT
        ABS(CHECKSUM(NEWID())) % 10 + 1, -- employee_id 1–10
        (SELECT TOP 1 date_id FROM dim_date ORDER BY NEWID()), -- losowy date_id
        ABS(CHECKSUM(NEWID())) % 3 + 1,  -- project_id 1–3
        ABS(CHECKSUM(NEWID())) % 3 + 1,  -- job_id 1–3
        ABS(CHECKSUM(NEWID())) % 3 + 1,  -- department_id 1–3
        ABS(CHECKSUM(NEWID())) % 3 + 1,  -- location_id 1–3
        ABS(CHECKSUM(NEWID())) % 4 + 1,  -- time_type_id 1–4
        ROUND(RAND() * 12, 2),           -- hours_worked 0–12
        CASE ABS(CHECKSUM(NEWID())) % 4
            WHEN 0 THEN 'Work'
            WHEN 1 THEN 'Training'
            WHEN 2 THEN 'Meeting'
            ELSE 'Leave'
        END,
        CASE ABS(CHECKSUM(NEWID())) % 3
            WHEN 0 THEN 'Completed'
            WHEN 1 THEN 'In Progress'
            ELSE 'Cancelled'
        END,
        ABS(CHECKSUM(NEWID())) % 2,     -- overtime: 0 lub 1
        DATEADD(HOUR, ABS(CHECKSUM(NEWID())) % 8 + 8, GETDATE()), -- start_time
        DATEADD(HOUR, ABS(CHECKSUM(NEWID())) % 8 + 17, GETDATE()),-- end_time
        'Test data row #' + CAST(@i AS NVARCHAR)
    
    SET @i += 1;
END;
