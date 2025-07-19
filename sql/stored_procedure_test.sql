CREATE OR ALTER PROCEDURE usp_GetActivityByStatus
	@status NVARCHAR (50)
AS
BEGIN
	SELECT
		activity_id,
		employee_id,
		project_id,
		status,
		hours_worked
	FROM fact_employee_activity
	WHERE status = @status
END;
