-- Temp table
SELECT *
INTO #tmp_activity1
FROM fact_employee_activity
WHERE hours_worked > 10
  AND start_time >= '2024-01-01';

-- Join & Aggregate on temp
SELECT 
    d.department_name,
    p.project_name,
    COUNT(*) AS activity_count,
    AVG(t.hours_worked) AS avg_hours
FROM #tmp_activity t
JOIN dim_department d ON t.department_id = d.department_id
JOIN dim_project p ON t.project_id = p.project_id
GROUP BY d.department_name, p.project_name;
