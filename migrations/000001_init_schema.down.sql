-- Drop in reverse FK-dependency order.
-- (Dropping a table that another table references would fail without CASCADE.)
DROP TABLE IF EXISTS workflow_events;
DROP TABLE IF EXISTS task_executions;
DROP TABLE IF EXISTS workflow_executions;
DROP TABLE IF EXISTS workflow_steps;
DROP TABLE IF EXISTS workflow_definitions;
DROP EXTENSION IF EXISTS "uuid-ossp";


