package day18

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Day 18 — Joins
 *
 * Covers:
 *   1. inner, left, right and full joins
 *   2. Handling ambiguous column names with aliases
 *   3. Null handling after left joins
 *   4. Shuffle sort merge join, explained with real .explain() plans
 *   5. Scenario: joining orders, customers and payments
 *
 * Run with:
 *   sbt "run local[2]"
 *   sbt "run local[4]"
 */
object JoinsApp {

  def main(args: Array[String]): Unit = {
    val masterUrl = if (args.nonEmpty) args(0) else "local[*]"

    val spark = SparkSession.builder()
      .appName("Day18-JoinsApp")
      .master(masterUrl)
      .getOrCreate()

    val customersDf = csv(spark, "customers.csv")
    val ordersDf = csv(spark, "orders.csv")
    val paymentsDf = csv(spark, "payments.csv")

    println(s"customers: ${customersDf.count()} | orders: ${ordersDf.count()} | payments: ${paymentsDf.count()}")

    section("1. Inner, left, right and full joins")
    joinTypesDemo(customersDf, ordersDf)

    section("2. Ambiguous column names, handled with aliases")
    ambiguousColumnsDemo(customersDf, ordersDf)

    section("3. Null handling after left joins")
    nullHandlingDemo(customersDf, ordersDf)

    section("4. Shuffle sort merge join, explained")
    sortMergeJoinDemo(spark, ordersDf, paymentsDf)

    section("5. Scenario: joining orders, customers and payments")
    threeWayScenario(customersDf, ordersDf, paymentsDf)

    spark.stop()
  }

  def csv(spark: SparkSession, name: String): DataFrame =
    spark.read.option("header", "true").option("inferSchema", "true")
      .csv(s"src/main/resources/$name")

  // ---------------------------------------------------------------------
  // 1. inner, left, right, full
  // ---------------------------------------------------------------------
  def joinTypesDemo(customersDf: DataFrame, ordersDf: DataFrame): Unit = {
    // Both sides have a column named "status" - rename up front here so
    // the four join demos below aren't also tangled up with section 2's
    // ambiguity problem.
    val customers = customersDf.withColumnRenamed("status", "customerStatus")
    val orders = ordersDf.withColumnRenamed("status", "orderStatus")
    val joinCond = customers("customerId") === orders("customerId")

    val inner = customers.join(orders, joinCond, "inner")
    val left = customers.join(orders, joinCond, "left")
    val right = customers.join(orders, joinCond, "right")
    val full = customers.join(orders, joinCond, "full")

    println(f"${"join type"}%-8s ${"row count"}%10s   meaning")
    println(f"${"inner"}%-8s ${inner.count()}%10d   only customers that have at least one order")
    println(f"${"left"}%-8s ${left.count()}%10d   every customer, orders attached where they exist (nulls otherwise)")
    println(f"${"right"}%-8s ${right.count()}%10d   every order, customer attached where it exists (nulls for orphan orders)")
    println(f"${"full"}%-8s ${full.count()}%10d   every customer AND every order, matched where possible")

    println("\nCustomers with ZERO orders (only visible via LEFT, missing from INNER):")
    left.filter(orders("orderId").isNull)
      .select(customers("customerId"), customers("name"))
      .orderBy(customers("customerId"))
      .show(truncate = false)

    println("Orders with NO matching customer - 'orphan' rows (only visible via RIGHT or FULL):")
    right.filter(customers("customerId").isNull)
      .select(orders("orderId"), orders("customerId").as("missingCustomerId"), orders("amount"))
      .show(truncate = false)
  }

  // ---------------------------------------------------------------------
  // 2. Ambiguous column names
  // ---------------------------------------------------------------------
  def ambiguousColumnsDemo(customersDf: DataFrame, ordersDf: DataFrame): Unit = {
    val joined = customersDf.join(ordersDf, customersDf("customerId") === ordersDf("customerId"), "inner")

    println("Both customers.csv and orders.csv have a 'status' column - selecting it bare fails:")
    try {
      joined.select("status") // ambiguous: which "status"?
    } catch {
      case e: org.apache.spark.sql.AnalysisException =>
        println(s"  Caught: ${e.getMessage.linesIterator.next()}")
    }

    println("\nFix 1 - qualify with the DataFrame reference used in the join condition:")
    joined.select(
      customersDf("customerId"),
      customersDf("status").as("customerStatus"),
      ordersDf("status").as("orderStatus")
    ).show(5, truncate = false)

    println("Fix 2 - alias each DataFrame BEFORE joining, then qualify with the alias in SQL-style dot notation:")
    val c = customersDf.as("c")
    val o = ordersDf.as("o")
    val joinedAliased = c.join(o, col("c.customerId") === col("o.customerId"), "inner")
    joinedAliased.select(
      col("c.customerId"),
      col("c.status").as("customerStatus"),
      col("o.status").as("orderStatus")
    ).show(5, truncate = false)

    println("Fix 3 - simplest when the JOIN KEY itself has the same name on both sides: use the")
    println("string-sequence join form, which keeps only ONE copy of the shared key column:")
    customersDf.join(ordersDf, Seq("customerId"), "inner")
      .select("customerId", "name", "orderId", "amount")
      .show(5, truncate = false)
  }

  // ---------------------------------------------------------------------
  // 3. Null handling after left joins
  // ---------------------------------------------------------------------
  def nullHandlingDemo(customersDf: DataFrame, ordersDf: DataFrame): Unit = {
    // Seq("customerId") only dedupes the JOIN KEY itself - both sides still
// have their own "status" column after the join, so rename up front
// (same fix as section 2) to keep every column in `summary` unambiguous.
val customers = customersDf.withColumnRenamed("status", "customerStatus")
val summary = customers.join(ordersDf, Seq("customerId"), "left")

    println("A LEFT join fills every unmatched right-side column with NULL. Raw result for a")
    println("customer with no orders (orderId, orderDate, amount, status all NULL):")
    summary.filter(col("name") === summary.filter(col("orderId").isNull).select("name").first().getString(0))
      .select("customerId", "name", "orderId", "amount")
      .show(3, truncate = false)

    println("isNull / isNotNull to split the two groups:")
    val withOrders = summary.filter(col("orderId").isNotNull)
    val withoutOrders = summary.filter(col("orderId").isNull)
    println(s"  rows with an order   : ${withOrders.count()}")
    println(s"  rows with NO order   : ${withoutOrders.select("customerId").distinct().count()} distinct customers")

    println("\ncoalesce(...) / na.fill(...) to turn NULL into a safe default for reporting:")
    val cleaned = summary
      .withColumn("amount", coalesce(col("amount"), lit(0.0)))
      .na.fill("NO_ORDERS", Seq("status"))
    cleaned.filter(col("orderId").isNull)
      .select("customerId", "name", "amount", "status")
      .show(5, truncate = false)

    println("Per-customer order count and total spend - customers with no orders correctly show 0,")
    println("NOT null, because count(orderId) skips nulls and sum(amount) is wrapped in coalesce:")
    val perCustomer = summary.groupBy("customerId", "name").agg(
      count("orderId").as("orderCount"),               // count() ignores nulls automatically
      coalesce(sum("amount"), lit(0.0)).as("totalSpend") // sum() of an all-null group is null -> coalesce it
    )
    perCustomer.filter(col("orderCount") === 0).show(5, truncate = false)
  }

  // ---------------------------------------------------------------------
  // 4. Shuffle sort merge join
  // ---------------------------------------------------------------------
  def sortMergeJoinDemo(spark: SparkSession, ordersDf: DataFrame, paymentsDf: DataFrame): Unit = {
    println(
      """How Spark actually executes a join depends on the data size, not the
        |join TYPE (inner/left/...) you wrote - "sort merge join" and "broadcast
        |hash join" are both valid PHYSICAL strategies for an inner join.
        |
        |BROADCAST HASH JOIN: if one side is small enough (under
        |spark.sql.autoBroadcastJoinThreshold, 10MB by default), Spark sends a
        |full copy of that small side to EVERY executor and builds an in-memory
        |hash table there. No shuffle at all for the large side. Fast, but only
        |works when one side truly fits in memory.
        |
        |SHUFFLE SORT MERGE JOIN: when BOTH sides are too large to broadcast,
        |Spark instead:
        | 1. SHUFFLES both sides by the join key, so all rows with the same key
        |    land on the same partition (this is the expensive network step).
        | 2. SORTS each partition by the join key.
        | 3. MERGES the two sorted streams in one pass, like the merge step of
        |    merge-sort - matching keys line up without needing a hash table,
        |    which also makes it memory-efficient for very large partitions.
        |It's the default, general-purpose strategy whenever neither side is
        |small enough to broadcast.
        |""".stripMargin)

    println("Plan A - our orders/payments data is small, so Spark picks BroadcastHashJoin by default:")
    ordersDf.join(paymentsDf, Seq("orderId"), "inner").explain()

    println("\nPlan B - force a sort-merge join by disabling auto-broadcast, to see its shape:")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    ordersDf.join(paymentsDf, Seq("orderId"), "inner").explain()
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760") // restore the default (10MB)

    println("Look for \"SortMergeJoin\" plus an \"Exchange\" (the shuffle) and a \"Sort\" feeding each")
    println("side in Plan B, versus \"BroadcastHashJoin\" plus a \"BroadcastExchange\" in Plan A.")
  }

  // ---------------------------------------------------------------------
  // 5. Scenario: orders + customers + payments
  // ---------------------------------------------------------------------
  def threeWayScenario(customersDf: DataFrame, ordersDf: DataFrame, paymentsDf: DataFrame): Unit = {
    val c = customersDf.as("c")
    val o = ordersDf.as("o")
    val p = paymentsDf.as("p")

    // customers INNER orders (only customers who've ordered something) LEFT
    // payments (keep every such order, whether or not it's been paid yet).
    val report = c.join(o, col("c.customerId") === col("o.customerId"), "inner")
      .join(p, col("o.orderId") === col("p.orderId"), "left")
      .select(
        col("o.orderId"),
        col("c.customerId"),
        col("c.name").as("customerName"),
        col("c.city"),
        col("o.amount").as("orderAmount"),
        col("o.status").as("orderStatus"),
        col("p.paymentMethod"),
        col("p.paidAmount")
      )
      .withColumn("paymentStatus", when(col("paidAmount").isNull, "UNPAID").otherwise("PAID"))
      .withColumn("amountOutstanding", when(col("paidAmount").isNull, col("orderAmount")).otherwise(lit(0.0)))

    println(s"Combined order report: ${report.count()} rows (one per order, customer always present, payment optional)")
    report.orderBy(col("paymentStatus"), desc("orderAmount")).show(10, truncate = false)

    println("Unpaid orders by city - where is outstanding revenue concentrated?")
    report.filter(col("paymentStatus") === "UNPAID")
      .groupBy("city")
      .agg(count("*").as("unpaidOrders"), round(sum("amountOutstanding"), 2).as("outstandingAmount"))
      .orderBy(desc("outstandingAmount"))
      .show(truncate = false)

    println("Payment method breakdown (paid orders only):")
    report.filter(col("paymentStatus") === "PAID")
      .groupBy("paymentMethod")
      .agg(count("*").as("orders"), round(sum("paidAmount"), 2).as("totalPaid"))
      .orderBy(desc("totalPaid"))
      .show(truncate = false)

    val totalOutstanding = report.filter(col("paymentStatus") === "UNPAID")
      .agg(round(sum("amountOutstanding"), 2).as("t")).first().getAs[Double]("t")
    println(f"\nTotal outstanding across all customers' orders: Rs. $totalOutstanding%,.2f")
  }

  def section(title: String): Unit = {
    println("\n" + "=" * 70)
    println(title)
    println("=" * 70)
  }
}
