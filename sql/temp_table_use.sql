-- Buffer in tmp table
SELECT *
INTO #tmp_fact
FROM fact_employee_activity
WHERE hours_worked > 10 AND status = 'Completed';

-- Use tmp table
SELECT 
    f.department_id,
    d.department_name,
    COUNT(*) AS total_activities,
    AVG(f.hours_worked) AS avg_hours
FROM #tmp_fact f
JOIN dim_department d ON f.department_id = d.department_id
GROUP BY f.department_id, d.department_name;

-- Optional drop tmp table
DROP TABLE #tmp_fact;
