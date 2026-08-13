-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- ANSI mode round tests for integer types at a negative scale.
--
-- Spark's RoundBase rounds through BigDecimal and converts back with
-- toByteExact / toShortExact / toIntExact / toLongExact. MathUtils.withOverflow
-- catches the JDK ArithmeticException and passes its message through verbatim as
-- the `message` parameter of ARITHMETIC_OVERFLOW, so the rendered message is the
-- bare word "Overflow" for every width, with no type name and no try_ hint. That
-- shape is the same on 3.4, 3.5 and 4.x: all four versions wrap the same
-- toXExact calls in withOverflow, and withOverflow passes e.getMessage unchanged.

-- Config: spark.sql.ansi.enabled=true

-- ============================================================================
-- Test data setup
-- ============================================================================

statement
CREATE TABLE ansi_test_round_byte(v tinyint) USING parquet

statement
INSERT INTO ansi_test_round_byte VALUES (125), (124)

statement
CREATE TABLE ansi_test_round_short(v short) USING parquet

statement
INSERT INTO ansi_test_round_short VALUES (32765), (32764)

statement
CREATE TABLE ansi_test_round_int(v int) USING parquet

statement
INSERT INTO ansi_test_round_int VALUES (2147483645), (2147483644)

statement
CREATE TABLE ansi_test_round_long(v long) USING parquet

statement
INSERT INTO ansi_test_round_long VALUES (9223372036854775805), (9223372036854775804)

-- ============================================================================
-- Rounding up out of range throws
-- ============================================================================

-- 125 rounds to 130, which does not fit a tinyint
query expect_error(ARITHMETIC_OVERFLOW] Overflow.)
SELECT round(v, -1) FROM ansi_test_round_byte WHERE v = 125

-- 32765 rounds to 32770, which does not fit a smallint
query expect_error(ARITHMETIC_OVERFLOW] Overflow.)
SELECT round(v, -1) FROM ansi_test_round_short WHERE v = 32765

-- 2147483645 rounds to 2147483650, which does not fit an int
query expect_error(ARITHMETIC_OVERFLOW] Overflow.)
SELECT round(v, -1) FROM ansi_test_round_int WHERE v = 2147483645

-- 9223372036854775805 rounds to 9223372036854775810, which does not fit a long
query expect_error(ARITHMETIC_OVERFLOW] Overflow.)
SELECT round(v, -1) FROM ansi_test_round_long WHERE v = 9223372036854775805

-- ============================================================================
-- One below each of those rounds down and succeeds
-- ============================================================================

query
SELECT round(v, -1) FROM ansi_test_round_byte WHERE v = 124

query
SELECT round(v, -1) FROM ansi_test_round_short WHERE v = 32764

query
SELECT round(v, -1) FROM ansi_test_round_int WHERE v = 2147483644

query
SELECT round(v, -1) FROM ansi_test_round_long WHERE v = 9223372036854775804
