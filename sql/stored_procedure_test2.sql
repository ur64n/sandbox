CREATE OR ALTER PROCEDURE usp_GetAverageHours
    @project_id INT,
    @only_overtime BIT
AS
BEGIN
    SELECT 
        project_id,
        AVG(hours_worked) AS avg_hours
    FROM fact_employee_activity
    WHERE project_id = @project_id
      AND overtime = @only_overtime
    GROUP BY project_id;
END;
