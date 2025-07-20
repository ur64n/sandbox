SELECT f.activity_id, e.first_name, d.department_name
FROM fact_employee_activity f
JOIN dim_employee e ON f.employee_id = e.employee_id
JOIN dim_department d ON f.department_id = d.department_id
WHERE f.status = 'Completed'
OPTION (FORCE ORDER);
