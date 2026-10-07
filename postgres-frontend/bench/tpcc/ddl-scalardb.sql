-- TPC-C schema for ScalarDB through the PostgreSQL frontend (BenchBase's TPC-C).
-- Column order mirrors BenchBase's ddl-postgres.sql because its loader inserts positionally, and
-- ScalarDB lists a table's key columns first, so every table's keys are its leading columns.
-- Keys: the warehouse (and district, where the workload reads within one) is the partition key,
-- the row's id the clustering key, so every TPC-C read is a Get or a scan of one partition.
-- DECIMAL becomes DOUBLE PRECISION, CHAR/VARCHAR become TEXT; foreign keys and the composite
-- secondary indexes are dropped (the frontend scans the partition with the condition pushed).
CREATE COORDINATOR TABLES IF NOT EXISTS;
CREATE SCHEMA IF NOT EXISTS tpcc;
DROP TABLE IF EXISTS history;
DROP TABLE IF EXISTS new_order;
DROP TABLE IF EXISTS order_line;
DROP TABLE IF EXISTS oorder;
DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS district;
DROP TABLE IF EXISTS stock;
DROP TABLE IF EXISTS item;
DROP TABLE IF EXISTS warehouse;
CREATE TABLE warehouse (w_id INT, w_ytd DOUBLE PRECISION, w_tax DOUBLE PRECISION, w_name TEXT, w_street_1 TEXT, w_street_2 TEXT, w_city TEXT, w_state TEXT, w_zip TEXT, PRIMARY KEY (w_id));
CREATE TABLE item (i_id INT, i_name TEXT, i_price DOUBLE PRECISION, i_data TEXT, i_im_id INT, PRIMARY KEY (i_id));
CREATE TABLE stock (s_w_id INT, s_i_id INT, s_quantity INT, s_ytd DOUBLE PRECISION, s_order_cnt INT, s_remote_cnt INT, s_data TEXT, s_dist_01 TEXT, s_dist_02 TEXT, s_dist_03 TEXT, s_dist_04 TEXT, s_dist_05 TEXT, s_dist_06 TEXT, s_dist_07 TEXT, s_dist_08 TEXT, s_dist_09 TEXT, s_dist_10 TEXT, PRIMARY KEY (s_w_id, s_i_id));
CREATE TABLE district (d_w_id INT, d_id INT, d_ytd DOUBLE PRECISION, d_tax DOUBLE PRECISION, d_next_o_id INT, d_name TEXT, d_street_1 TEXT, d_street_2 TEXT, d_city TEXT, d_state TEXT, d_zip TEXT, PRIMARY KEY (d_w_id, d_id));
CREATE TABLE customer (c_w_id INT, c_d_id INT, c_id INT, c_discount DOUBLE PRECISION, c_credit TEXT, c_last TEXT, c_first TEXT, c_credit_lim DOUBLE PRECISION, c_balance DOUBLE PRECISION, c_ytd_payment DOUBLE PRECISION, c_payment_cnt INT, c_delivery_cnt INT, c_street_1 TEXT, c_street_2 TEXT, c_city TEXT, c_state TEXT, c_zip TEXT, c_phone TEXT, c_since TIMESTAMP, c_middle TEXT, c_data TEXT, PRIMARY KEY (c_w_id, c_d_id, c_id)) WITH (partition_key = 'c_w_id, c_d_id', clustering_key = 'c_id');
-- history is insert-only in TPC-C; its keys are the leading columns of BenchBase's column order
-- because ScalarDB lists key columns first and the loader inserts positionally
CREATE TABLE history (h_c_id INT, h_c_d_id INT, h_c_w_id INT, h_d_id INT, h_w_id INT, h_date TIMESTAMP, h_amount DOUBLE PRECISION, h_data TEXT, PRIMARY KEY (h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date)) WITH (partition_key = 'h_c_id, h_c_d_id, h_c_w_id', clustering_key = 'h_d_id, h_w_id, h_date');
CREATE TABLE oorder (o_w_id INT, o_d_id INT, o_id INT, o_c_id INT, o_carrier_id INT, o_ol_cnt INT, o_all_local INT, o_entry_d TIMESTAMP, PRIMARY KEY (o_w_id, o_d_id, o_id)) WITH (partition_key = 'o_w_id, o_d_id', clustering_key = 'o_id');
CREATE TABLE new_order (no_w_id INT, no_d_id INT, no_o_id INT, PRIMARY KEY (no_w_id, no_d_id, no_o_id)) WITH (partition_key = 'no_w_id, no_d_id', clustering_key = 'no_o_id');
CREATE TABLE order_line (ol_w_id INT, ol_d_id INT, ol_o_id INT, ol_number INT, ol_i_id INT, ol_delivery_d TIMESTAMP, ol_amount DOUBLE PRECISION, ol_supply_w_id INT, ol_quantity DOUBLE PRECISION, ol_dist_info TEXT, PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number)) WITH (partition_key = 'ol_w_id, ol_d_id', clustering_key = 'ol_o_id, ol_number');

-- BenchBase's ddl-postgres.sql has idx_customer_name ON customer (c_w_id, c_d_id, c_last, c_first) for
-- the by-last-name lookups in Payment and OrderStatus. ScalarDB indexes cover one column; with this
-- one PostgreSQL intersects it with the primary key instead of filtering the district's 3000 customers.
CREATE INDEX idx_customer_name ON tpcc.customer (c_last);
