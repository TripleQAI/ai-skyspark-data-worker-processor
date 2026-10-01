-- Create the SkySpark ingestion control database and its application role.
--
-- RUN THIS CONNECTED TO THE `postgres` (or `tsdb`) MAINTENANCE DATABASE, as a
-- role that can CREATE DATABASE and CREATE ROLE. It is the only step that
-- cannot live in migrations/control/: CREATE DATABASE cannot run inside a
-- transaction block, and the migration runner wraps every file in one.
--
-- After this, connect to skyspark_control and run:
--
--     skyspark-control migrate --migrations migrations/control
--
-- which applies the 10 files in migrations/control/ and records each in
-- public.control_schema_migrations.
--
-- WHY A SEPARATE DATABASE, not a schema in an existing one. The control store
-- holds run/job state, fenced leases, both outboxes, and the source permit
-- pool -- the coordination substrate the dispatcher and every worker contend
-- on with SELECT ... FOR UPDATE SKIP LOCKED. Sharing a database with an
-- unrelated system (the Authorization Server's user_schema, say) would share
-- one connection limit, one restore boundary, and one blast radius between two
-- systems that fail for entirely different reasons.
--
-- NOTHING HERE CREATES A SCHEMA OR TABLE. migrations/control/0001_initial.sql
-- owns `CREATE SCHEMA ingestion` and all 22 tables. This file creates only the
-- database and the role, so the migration runner stays the single source of
-- truth for schema shape.

-- ---------------------------------------------------------------------------
-- 1. The application role.
-- ---------------------------------------------------------------------------
--
-- NOT a superuser and NOT the database owner. The workers, control services
-- and Lambdas all connect as this role; it needs DML and the ability to create
-- the schema's objects during migration, nothing more.
--
-- Replace the password before running. Do not leave it in a file you commit --
-- it goes straight into the Secrets Manager `dsn`, and nowhere else.

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'skyspark_control_app') THEN
        CREATE ROLE skyspark_control_app
            LOGIN
            PASSWORD 'REPLACE_WITH_A_GENERATED_PASSWORD'
            -- Bounded so a connection leak in one service cannot exhaust the
            -- instance for the others. Raise it from a measured task count:
            -- 4 worker services x up to 9 tasks x 4 job slots, plus 4
            -- dispatchers, 2 publishers, 1 recovery, plus 3 Lambdas, is well
            -- past this -- size it against the Tiger Cloud tier's own limit
            -- before the full-scale run.
            CONNECTION LIMIT 60;
    END IF;
END
$$;

-- ---------------------------------------------------------------------------
-- 2. The database.
-- ---------------------------------------------------------------------------
--
-- Owned by the application role so `skyspark-control migrate` can create the
-- ingestion schema without a second, more privileged connection.
--
-- CREATE DATABASE cannot run in a transaction block or inside the DO above, so
-- this is a bare statement. If it fails with "already exists", the database is
-- already there and the migration step is the next thing to run.
--
-- Tiger Cloud note: some managed tiers refuse CREATE DATABASE from SQL. If
-- this statement is rejected, create the database from the Tiger Cloud console
-- instead, set its owner to skyspark_control_app, and skip to section 3.

CREATE DATABASE skyspark_control
    OWNER skyspark_control_app
    ENCODING 'UTF8'
    TEMPLATE template0;

COMMENT ON DATABASE skyspark_control IS
    'SkySpark ingestion control store: runs, jobs, leases, dispatch/publication outboxes, source permits, site checkpoints, inventory registry. Certified data lands in S3, not here.';

-- ---------------------------------------------------------------------------
-- 3. Lock down the public schema. RUN THESE CONNECTED TO skyspark_control.
-- ---------------------------------------------------------------------------
--
-- Everything above ran against the maintenance database. Reconnect to
-- skyspark_control before running the rest:
--
--     \connect skyspark_control
--
-- PostgreSQL 15 and later already revoke public CREATE on the public schema;
-- these statements are explicit so the result does not depend on the server
-- version. public is not empty here -- the migration runner puts its
-- control_schema_migrations registry there -- so the role keeps USAGE and
-- CREATE on it.

REVOKE ALL ON SCHEMA public FROM PUBLIC;
GRANT USAGE, CREATE ON SCHEMA public TO skyspark_control_app;

-- No GRANT is needed on the ingestion schema: skyspark_control_app owns the
-- database and creates that schema itself during migration, so it owns every
-- object in it. A grant here would be a no-op that reads as a requirement.
