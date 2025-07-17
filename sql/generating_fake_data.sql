-- ============================================
-- Script: 03_insert_data.sql
-- Purpose: Fill dim tables fake data
-- ============================================

-- dim_date (last 3 years)
DECLARE @i INT = 0;
WHILE @i < 1095
BEGIN
    INSERT INTO dim_date (date_id, full_date, year, month, day)
    SELECT 
        CONVERT(INT, FORMAT(DATEADD(DAY, -@i, GETDATE()), 'yyyyMMdd')),
        CAST(DATEADD(DAY, -@i, GETDATE()) AS DATE),
        YEAR(DATEADD(DAY, -@i, GETDATE())),
        MONTH(DATEADD(DAY, -@i, GETDATE())),
        DAY(DATEADD(DAY, -@i, GETDATE()));
    SET @i += 1;
END;

-- dim_department
INSERT INTO dim_department (department_id, department_name, description)
VALUES 
(1, 'Marketing', 'Digital campaigns'),
(2, 'IT', 'Infrastructure and development'),
(3, 'HR', 'Recruitment and internal training');

-- dim_location
INSERT INTO dim_location (location_id, city, country)
VALUES 
(1, 'Berlin', 'Germany'),
(2, 'Warsaw', 'Poland'),
(3, 'Amsterdam', 'Netherlands');

-- dim_job
INSERT INTO dim_job (job_id, job_title, job_description)
VALUES 
(1, 'Data Engineer', 'Builds pipelines and models'),
(2, 'SQL Developer', 'Writes stored procedures'),
(3, 'Business Analyst', 'Creates reports and insights');

-- dim_project
INSERT INTO dim_project (project_id, project_code, project_name)
VALUES 
(1, 'PRJ-1001', 'ERP Migration'),
(2, 'PRJ-1002', 'Data Warehouse Build'),
(3, 'PRJ-1003', 'Customer 360 Analysis');

-- dim_time_type
INSERT INTO dim_time_type (time_type_id, time_type)
VALUES 
(1, 'Full-Time'),
(2, 'Part-Time'),
(3, 'Internship'),
(4, 'Contract');

-- dim_employee (10 example employees)
INSERT INTO dim_employee (employee_id, first_name, last_name, job_title, department_id)
VALUES 
(1, 'Anna', 'Kowalska', 'Data Engineer', 2),
(2, 'John', 'Smith', 'Business Analyst', 1),
(3, 'Maria', 'Nowak', 'SQL Developer', 2),
(4, 'Peter', 'Müller', 'Data Engineer', 2),
(5, 'Olga', 'Ivanova', 'Business Analyst', 1),
(6, 'Liam', 'Johnson', 'SQL Developer', 2),
(7, 'Eva', 'Novak', 'Data Engineer', 2),
(8, 'Max', 'Klein', 'Data Engineer', 2),
(9, 'Sara', 'Lind', 'Business Analyst', 1),
(10, 'Tom', 'Lewandowski', 'SQL Developer', 2);
