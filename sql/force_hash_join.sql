SELECT f.employee_id, p.project_name
FROM fact_employee_activity f
JOIN dim_project p ON f.project_id = p.project_id
OPTION (HASH JOIN);
