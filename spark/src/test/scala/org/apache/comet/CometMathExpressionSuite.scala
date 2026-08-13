/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.comet

import scala.util.Random

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

import org.apache.comet.CometSparkSessionExtensions.isSpark35Plus
import org.apache.comet.testing.{DataGenOptions, FuzzDataGenerator}

class CometMathExpressionSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  test("abs") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      val df = createTestData(generateNegativeZero = false)
      df.createOrReplaceTempView("tbl")
      for (field <- df.schema.fields) {
        val col = field.name
        checkSparkAnswerAndOperator(s"SELECT $col, abs($col) FROM tbl ORDER BY $col")
      }
    }
  }

  test("abs - negative zero") {
    val df = createTestData(generateNegativeZero = true)
    df.createOrReplaceTempView("tbl")
    for (field <- df.schema.fields.filter(f =>
        f.dataType == DataTypes.FloatType || f.dataType == DataTypes.DoubleType)) {
      val col = field.name
      checkSparkAnswerAndOperator(
        s"SELECT $col, abs($col) FROM tbl WHERE CAST($col as string) = '-0.0' ORDER BY $col")
    }
  }

  test("abs (ANSI mode)") {
    val df = createTestData(generateNegativeZero = false)
    df.createOrReplaceTempView("tbl")
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      for (field <- df.schema.fields) {
        val col = field.name
        checkSparkAnswerMaybeThrows(sql(s"SELECT $col, abs($col) FROM tbl ORDER BY $col")) match {
          case (Some(sparkExc), Some(cometExc)) =>
            val cometErrorPattern =
              """.+[ARITHMETIC_OVERFLOW].+overflow. If necessary set "spark.sql.ansi.enabled" to "false" to bypass this error.""".r
            assert(cometErrorPattern.findFirstIn(cometExc.getMessage).isDefined)
            assert(sparkExc.getMessage.contains("overflow"))
          case (Some(_), None) =>
            fail("Exception should be thrown")
          case (None, Some(cometExc)) =>
            throw cometExc
          case _ =>
        }
      }
    }
  }

  private def createTestData(generateNegativeZero: Boolean) = {
    val r = new Random(42)
    val schema = StructType(
      Seq(
        StructField("c0", DataTypes.ByteType, nullable = true),
        StructField("c1", DataTypes.ShortType, nullable = true),
        StructField("c2", DataTypes.IntegerType, nullable = true),
        StructField("c3", DataTypes.LongType, nullable = true),
        StructField("c4", DataTypes.FloatType, nullable = true),
        StructField("c5", DataTypes.DoubleType, nullable = true),
        StructField("c6", DataTypes.createDecimalType(10, 2), nullable = true)))
    FuzzDataGenerator.generateDataFrame(
      r,
      spark,
      schema,
      1000,
      DataGenOptions(generateNegativeZero = generateNegativeZero))
  }

  test("width_bucket") {
    assume(isSpark35Plus, "width_bucket was added in Spark 3.5")
    withSQLConf("spark.comet.exec.localTableScan.enabled" -> "true") {
      spark
        .createDataFrame(
          Seq((5.3, 0.2, 10.6, 5), (8.1, 0.0, 5.7, 4), (-0.9, 5.2, 0.5, 2), (-2.1, 1.3, 3.4, 3)))
        .toDF("c1", "c2", "c3", "c4")
        .createOrReplaceTempView("width_bucket_test")
      checkSparkAnswerAndOperator(
        "SELECT c1, width_bucket(c1, c2, c3, c4) FROM width_bucket_test")
    }
  }

  test("width_bucket - edge cases") {
    assume(isSpark35Plus, "width_bucket was added in Spark 3.5")
    withSQLConf("spark.comet.exec.localTableScan.enabled" -> "true") {
      spark
        .createDataFrame(Seq(
          (0.0, 10.0, 0.0, 5), // Value equals max (reversed bounds)
          (10.0, 0.0, 10.0, 5), // Value equals max (normal bounds)
          (10.0, 0.0, 0.0, 5), // Min equals max - returns NULL
          (5.0, 0.0, 10.0, 0) // Zero buckets - returns NULL
        ))
        .toDF("c1", "c2", "c3", "c4")
        .createOrReplaceTempView("width_bucket_edge")
      checkSparkAnswerAndOperator(
        "SELECT c1, width_bucket(c1, c2, c3, c4) FROM width_bucket_edge")
    }
  }

  test("width_bucket - NaN values") {
    assume(isSpark35Plus, "width_bucket was added in Spark 3.5")
    withSQLConf("spark.comet.exec.localTableScan.enabled" -> "true") {
      spark
        .createDataFrame(
          Seq((Double.NaN, 5.0, 0.0), (5.0, Double.NaN, 0.0), (5.0, 0.0, Double.NaN)))
        .toDF("c1", "c2", "c3")
        .createOrReplaceTempView("width_bucket_nan")
      checkSparkAnswerAndOperator("SELECT c1, width_bucket(c1, c2, c3, 5) FROM width_bucket_nan")
    }
  }

  test("width_bucket - with range data") {
    assume(isSpark35Plus, "width_bucket was added in Spark 3.5")
    withSQLConf("spark.comet.exec.localTableScan.enabled" -> "true") {
      spark
        .range(10)
        .selectExpr("id", "CAST(id AS DOUBLE) as value")
        .createOrReplaceTempView("width_bucket_range")
      checkSparkAnswerAndOperator(
        "SELECT id, width_bucket(value, 0.0, 10.0, 5) FROM width_bucket_range ORDER BY id")
    }
  }

  private def causeChain(error: Throwable): Seq[Throwable] =
    Iterator.iterate(error)(_.getCause).takeWhile(_ != null).toSeq

  private def deepestSparkThrowable(error: Throwable): SparkThrowable with Throwable =
    causeChain(error)
      .collect { case e: SparkThrowable with Throwable => e }
      .lastOption
      .getOrElse(
        fail(s"No SparkThrowable in cause chain: ${causeChain(error).map(_.getClass.getName)}"))

  test("round ANSI overflow raises Spark's exception, not a native one") {
    withSQLConf(
      SQLConf.ANSI_ENABLED.key -> "true",
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
        "org.apache.spark.sql.catalyst.optimizer.ConstantFolding") {
      Seq(
        "SELECT round(CAST(2147483645 AS INT), -1)",
        "SELECT round(CAST(9223372036854775805 AS BIGINT), -1)").foreach { query =>
        val df = sql(query)
        checkCometOperators(stripAQEPlan(df.queryExecution.executedPlan))

        val (sparkError, cometError) = checkSparkAnswerMaybeThrows(df)
        val sparkFailure = sparkError.getOrElse(fail(s"Spark did not fail for: $query"))
        val cometFailure = cometError.getOrElse(fail(s"Comet did not fail for: $query"))
        val expected = deepestSparkThrowable(sparkFailure)
        val actual = deepestSparkThrowable(cometFailure)

        assert(actual.getClass == expected.getClass)
        assert(actual.getErrorClass == expected.getErrorClass)
        assert(actual.getSqlState == expected.getSqlState)
        assert(actual.getMessageParameters == expected.getMessageParameters)
        // Spark appends a SQL query context that Comet does not carry for this expression, so the
        // messages agree up to that suffix rather than exactly. Only `Cast`, `CheckOverflow`,
        // `ListExtract` and `SumDecimal` propagate a context today.
        assert(expected.getMessage.startsWith(actual.getMessage))
        assert(!causeChain(cometFailure).exists(_.isInstanceOf[CometNativeException]))
      }
    }
  }
}
