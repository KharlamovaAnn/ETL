-- Script for Target MySQL Database
-- Host: 95.131.149.21
-- Database: mgpu_ico_etl_XX

CREATE TABLE IF NOT EXISTS call_center_analytics (
    call_id INT PRIMARY KEY,
    operator_name VARCHAR(255),
    kpi_score DECIMAL(3, 2),
    topic_name VARCHAR(100),
    call_date TIMESTAMP,
    duration_sec INT
);

-- Analytical View
CREATE OR REPLACE VIEW view_call_center_report AS
SELECT 
    topic_name,
    CASE 
        WHEN kpi_score >= 4.5 THEN 'High Performing'
        WHEN kpi_score >= 3.0 THEN 'Standard'
        ELSE 'Needs Improvement'
    END AS operator_performance_category,
    AVG(duration_sec) as avg_duration,
    COUNT(*) as total_calls
FROM call_center_analytics
GROUP BY topic_name, operator_performance_category;
