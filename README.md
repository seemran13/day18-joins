# Day 18 — Joins

Inner/left/right/full joins, resolving ambiguous column names, null
handling after a left join, shuffle sort merge join explained with real
`.explain()` plans, and a three-way orders/customers/payments scenario.

## Run

```bash
sbt "run local[4]"
```

## Data

- `customers.csv` — 40 customers; customers C35–C40 deliberately have no orders
- `orders.csv` — 150 orders; 8 of them reference customer ids that don't
  exist (orphan orders, for right/full joins)
- `payments.csv` — payments for 112 of the 150 orders (38 orders unpaid,
  for left-join null handling)

Both `customers.csv` and `orders.csv` have a column named `status`,
deliberately, to force the ambiguous-column problem in section 2.

## What it covers

1. **inner/left/right/full**: all four run over the same customers/orders
   pair, with row counts explained and the two "only visible with a
   non-inner join" cases pulled out explicitly — customers with zero
   orders, and orphan orders with no matching customer.
2. **Ambiguous columns**: `joined.select("status")` is shown actually
   throwing `AnalysisException`, then three fixes: qualifying with the
   original DataFrame reference (`customersDf("status")`), aliasing each
   DataFrame before the join and using `col("c.status")`, and using the
   `Seq("customerId")` join form, which keeps only one copy of a shared
   join key.
3. **Null handling after left joins**: `isNull`/`isNotNull` to split
   matched vs unmatched rows, `coalesce`/`na.fill` to turn nulls into safe
   defaults, and why `count(orderId)` and `coalesce(sum(amount), 0)`
   correctly show `0` rather than `null` for a customer with no orders.
4. **Shuffle sort merge join**: explained conceptually (shuffle by key,
   sort each side, merge in one pass), then shown for real — the same
   join `.explain()`'d once as Spark's default `BroadcastHashJoin` (data
   is small) and once with `spark.sql.autoBroadcastJoinThreshold` forced
   to `-1` to see the `SortMergeJoin` + `Exchange` + `Sort` shape instead.
5. **Scenario**: customers inner-joined to their orders, left-joined to
   payments (so unpaid orders still appear, with nulls), aliases used
   throughout to keep every column unambiguous, then reports on
   outstanding revenue by city and paid revenue by payment method.

## Push to GitHub

```bash
git init && git add . && git commit -m "Day 18: Joins"
git branch -M main
git remote add origin https://github.com/seemran13/day18-joins.git
git push -u origin main
```
