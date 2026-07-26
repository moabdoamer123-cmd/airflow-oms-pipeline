-- ============================================================
-- Analytics Star Schema DDL — CORRECTED VERSION
-- Run this once on Neon Postgres BEFORE running the transformation DAG.
--
-- Fixes applied vs previous draft:
--   1. customerid / storeid are VARCHAR(10) to match oms_core source
--      types exactly (was INT — would break FK constraints on join).
--   2. phone widened to VARCHAR(50) to match source (was VARCHAR(20),
--      would truncate values like '+1-406-250-9199x4380').
--   3. fact_sales uses orderitemid as PK (already unique in source),
--      avoiding false conflicts when an order has multiple lines for
--      the same product.
-- ============================================================

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;

-- 1. Dimension: Customer
CREATE TABLE IF NOT EXISTS analytics.dim_customer (
    customerid VARCHAR(10) PRIMARY KEY,
    firstname  VARCHAR(50),
    lastname   VARCHAR(50),
    email      VARCHAR(100),
    phone      VARCHAR(50)
);

-- 2. Dimension: Product
CREATE TABLE IF NOT EXISTS analytics.dim_product (
    productid     INT PRIMARY KEY,
    name          VARCHAR(100),
    category      VARCHAR(50),
    retailprice   NUMERIC(10, 2)
);

-- 3. Dimension: Store
CREATE TABLE IF NOT EXISTS analytics.dim_store (
    storeid    VARCHAR(10) PRIMARY KEY,
    storename  VARCHAR(50),
    city       VARCHAR(50),
    state      VARCHAR(10)
);

-- 4. Dimension: Employee (was missing entirely before)
CREATE TABLE IF NOT EXISTS analytics.dim_employee (
    employeeid INT PRIMARY KEY,
    firstname  VARCHAR(50),
    lastname   VARCHAR(50),
    jobtitle   VARCHAR(50),
    managerid  INT
);

-- 5. Dimension: Date
CREATE TABLE IF NOT EXISTS analytics.dim_date (
    datekey     DATE PRIMARY KEY,
    year        INT,
    quarter     INT,
    month       INT,
    day         INT,
    dayofweek   VARCHAR(20),
    is_weekend  BOOLEAN
);

-- 6. Fact: Sales
-- NOTE: orderitemid is the PK (unique per line item in source),
-- NOT (orderid, productid), which would falsely conflict if an
-- order has two separate lines for the same product.
CREATE TABLE IF NOT EXISTS analytics.fact_sales (
    orderitemid  INT PRIMARY KEY,
    orderid      INT NOT NULL,
    customerid   VARCHAR(10) REFERENCES analytics.dim_customer(customerid),
    productid    INT REFERENCES analytics.dim_product(productid),
    storeid      VARCHAR(10) REFERENCES analytics.dim_store(storeid),
    employeeid   INT REFERENCES analytics.dim_employee(employeeid),
    orderdate    DATE REFERENCES analytics.dim_date(datekey),
    status_code  VARCHAR(2),
    status_desc  VARCHAR(20),
    quantity     INT,
    unitprice    NUMERIC(10, 2),
    total_price  NUMERIC(10, 2)
);