/*
 airflow_db creation upon postgres container initialization script, this should run 
 v1.0
 */

-- this checks pg_database first and only executes CRATE DATABASE if airflow_db does not exist, yea sadly postgres don't support CREATE DATABASE IF NOT EXISTS
SELECT 'CREATE DATABASE airflow_db'
WHERE NOT EXISTS (SELECT FROM pg_database
                    WHERE datname = 'airflow_db')\gexec -- \gexec executes the text in the query bugger, then treats each column of each row in the resulting output as a new SQL query to be executed. in this case CREATE DATABASE airflow_db will be executed if the condition is fulfilled

-- proceed to greant privileges to user
GRANT ALL PRIVILEGES
ON DATABASE airflow_db
TO apollo_user;