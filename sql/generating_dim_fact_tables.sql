-- ============================================
-- Script: 01_create_tables.sql
-- Purpose: Creating tables structure (without FK)
-- ============================================

-- Dimensions

CREATE TABLE dim_date (
    date_id INT PRIMARY KEY,              -- Format YYYYMMDD
    full_date DATE,
    year INT,
    month INT,
    day INT
);

CREATE TABLE dim_employee (
    employee_id INT PRIMARY KEY,
    first_name NVARCHAR(50),
    last_name NVARCHAR(50),
    job_title NVARCHAR(100),
    department_id INT                      -- FK dodamy osobno
);

CREATE TABLE dim_department (
    department_id INT PRIMARY KEY,
    department_name NVARCHAR(100),
    description NVARCHAR(150)
);

CREATE TABLE dim_location (
    location_id INT PRIMARY KEY,
    city NVARCHAR(100),
    country NVARCHAR(100)
);

CREATE TABLE dim_job (
    job_id INT PRIMARY KEY,
    job_title NVARCHAR(100),
    job_description NVARCHAR(150)
);

CREATE TABLE dim_project (
    project_id INT PRIMARY KEY,
    project_code NVARCHAR(50),
    project_name NVARCHAR(100)
);

CREATE TABLE dim_time_type (
    time_type_id INT PRIMARY KEY,
    time_type NVARCHAR(50)
);

-- Fact table (without FK)

CREATE TABLE fact_employee_activity (
    activity_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    employee_id INT,
    date_id INT,
    project_id INT,
    job_id INT,
    department_id INT,
    location_id INT,
    time_type_id INT,
    hours_worked FLOAT,
    activity_type NVARCHAR(50),
    status NVARCHAR(50),
    overtime BIT,
    start_time DATETIME2,
    end_time DATETIME2,
    comment NVARCHAR(200)
);
