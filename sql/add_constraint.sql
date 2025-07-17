-- ============================================
-- Script: 02_add_constraints.sql
-- Purpose: Forgeing keys for fact tables
-- ============================================

-- FK: fact_employee_activity.employee_id -> dim_employee.employee_id
ALTER TABLE fact_employee_activity
ADD CONSTRAINT fk_fact_employee
    FOREIGN KEY (employee_id) REFERENCES dim_employee(employee_id);

-- FK: fact_employee_activity.date_id -> dim_date.date_id
ALTER TABLE fact_employee_activity
ADD CONSTRAINT fk_fact_date
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id);

-- FK: fact_employee_activity.project_id -> dim_project.project_id
ALTER TABLE fact_employee_activity
ADD CONSTRAINT fk_fact_project
    FOREIGN KEY (project_id) REFERENCES dim_project(project_id);

-- FK: fact_employee_activity.job_id -> dim_job.job_id
ALTER TABLE fact_employee_activity
ADD CONSTRAINT fk_fact_job
    FOREIGN KEY (job_id) REFERENCES dim_job(job_id);

-- FK: fact_employee_activity.department_id -> dim_department.department_id
ALTER TABLE fact_employee_activity
ADD CONSTRAINT fk_fact_department
    FOREIGN KEY (department_id) REFERENCES dim_department(department_id);

-- FK: fact_employee_activity.location_id -> dim_location.location_id
ALTER TABLE fact_employee_activity
ADD CONSTRAINT fk_fact_location
    FOREIGN KEY (location_id) REFERENCES dim_location(location_id);

-- FK: fact_employee_activity.time_type_id -> dim_time_type.time_type_id
ALTER TABLE fact_employee_activity
ADD CONSTRAINT fk_fact_time_type
    FOREIGN KEY (time_type_id) REFERENCES dim_time_type(time_type_id);
