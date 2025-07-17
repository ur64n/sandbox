-- JOIN / WHERE na employee + date
CREATE NONCLUSTERED INDEX idx_fact_employee_date
ON fact_employee_activity(employee_id, date_id);

-- JOIN na project
CREATE NONCLUSTERED INDEX idx_fact_project
ON fact_employee_activity(project_id);

-- Agregacje po status
CREATE NONCLUSTERED INDEX idx_fact_status
ON fact_employee_activity(status);

-- Filtry po datach (start_time)
CREATE NONCLUSTERED INDEX idx_fact_start_time
ON fact_employee_activity(start_time);

-- Przykład: SELECT z WHERE + JOIN po department
CREATE NONCLUSTERED INDEX idx_fact_department
ON fact_employee_activity(department_id);
