---
title: The Hidden Cost of Running Iceberg: Maintenance, Delete Files, and Orphan Files
published: false
tags: dataengineering, apacheiceberg, datalake, bigdata
canonical_url:
cover_image:
---

# The Hidden Cost of Running Iceberg: Maintenance, Delete Files, and Orphan Files

> Why MOR helped ingestion, why queries still slowed down, and what delete files, snapshots, and orphan files taught me about operating Iceberg.

## Intro

When we made the move to Iceberg, after the initial setup, it seemed fine. Data would flow, `MERGE INTO` would run, and every 10 minutes new data was present in Iceberg. After running in production for a while, we saw that small, dimensional tables were doing perfectly fine.

But for high transactional tables like orders and accounts, the ingestion time was increasing significantly. With copy‑on‑write (COW), every small change was rewriting entire data files. Ingestion that needed to finish in a 10‑minute window was taking way too long. So we moved those tables to merge‑on‑read (MOR).

MOR fixed the write side. Ingestion fit inside the window again. But over the following weeks, a different kind of pressure started showing up on the read side. Queries got slower. Storage costs drifted. Maintenance signals we were not tracking started to matter. This post is about that second phase: not adopting Iceberg, but operating it.

## 1. Why Iceberg Looks Simple During Migration

Most teams come to Iceberg with a specific checklist: Can we read parquet? Can we run MERGE INTO? Can we register tables in our catalog? Can our BI tools query it? Once those boxes are checked, migration feels done. And honestly, it kind of is - from a format‑compatibility standpoint.

What does not make the checklist is everything that happens after the first few weeks. How fast do snapshots accumulate? How many delete files does a MERGE INTO produce on a table with 200 million rows? What happens to query latency when the partition strategy made sense at migration time but does not hold under real CDC volume? These are not migration questions. They are operational questions, and most teams do not ask them until something starts degrading.

The tricky part is that Iceberg gives you very little reason to worry early on. Small tables work beautifully. Moderate write volumes are fine. The format is elegant, the tooling connects, and dashboards light up. It is only when write frequency, table size, and time all compound together that the gaps become visible.

Migration proves compatibility. It does not prove operational health.

## 2. Why COW Broke and MOR Introduced New Pain

The ingestion pipeline was a CDC flow from Aurora via AWS DMS, emitting a `MERGE INTO` every ten minutes. For dimensional tables (a few thousands rows), copy-on-write (COW) behaved nicely. However, for our heavy transactional tables like orders, operations and accounts, the data volume sat in the 50-100 million row range with each ingestion.

With these larger tables, COW timing became highly inconsistent. Some runs finished within the ten-minute window, but others blew past it, sometimes taking 30 minutes or more to complete. Because COW rewrites entire data files for every small change, the write-latency variance made our pipeline unreliable.

To fix this, we switched those heavy tables to merge-on-read (MOR) while keeping the small tables on COW. MOR immediately solved the write-side problem: the ten-minute window was met again and the pipeline became predictable.

> **The Tradeoff**
> - **COW** shifts cost into writes: every merge rewrites entire data files.
> - **MOR** shifts cost into reads: writes are cheap, but readers must reconcile delete files.
> - Neither mode is "free"—the right choice depends on the shape of your workload.

## 3. What Starts Breaking After a Few Weeks

The first sign something was wrong was not a monitoring alert. Someone on the data team noticed that a daily pipeline computing joins on sales data was getting slower day by day. During migration it was taking around an hour. A few weeks later, the same pipeline was taking eight hours and eventually timing out completely.

The frustrating part was that nothing had changed on our end. Same query, same data volume. But under the hood, a lot had changed. Each `MERGE INTO` run was producing delete files, and those were accumulating silently. The query engine had to scan all of them on every read, and the cost of that grew with every ingestion cycle.

### Small Files Compound the Problem

We partitioned by day using the `updated_on` column from source and queried incrementally. Simple enough strategy. But CDC doesn't write neatly one file per partition. Each 10-minute `MERGE INTO` was creating new small files inside the same day partitions. Over a few weeks, what should have been a handful of files per partition became hundreds of tiny ones. Scan plans got longer, planning overhead crept up, and the query engine had more work to do before it even started returning rows.

### The Observability Gap

We were watching query latency, but that was the output, not the cause. We had no view into snapshot counts, delete-file growth, or how fragmented individual tables were getting. By the time the 8-hour timeout showed up, the tables had already been quietly degrading for weeks.



## 4. Delete Files Are a Bigger Problem Than They First Seem

After a few weeks of MOR, positional delete files begin to stack up. Each `MERGE INTO` now writes a tiny delete file marking rows to be ignored at read time. Because these delete files must be consulted on every scan, they increase both read latency and S3 request volume.

### What You Notice First

The drift is gradual. You don't usually get a single "it broke" alert. Instead, certain tables just start to age badly, and reads become less predictable. For us, the specific moment was noticing our AWS billing: our baseline storage costs were around $30-$40 per day, but our S3 API costs spiked to around $150 per day.

### What Is Happening Under the Hood

To understand why API costs spike, consider what the query engine has to do. A read query must scan data files and check every delete file to filter out stale rows.

We pulled stats on one of our high-CDC datasets (`iceberg.gold.dm_hourly_user_performance_kpi`) that hadn't run maintenance. The numbers were staggering:
- **Total Data Files:** 105
- **Total Delete Files:** 1,867
- **Data Files Scanned:** 4,378
- **Delete Files Scanned:** 6,252,192
- **Total S3 GET Calls:** 6,313,230

For just 2,902 query executions, the engine had to scan over 6.2 million delete files, resulting in an incredible 99.9% delete-file scan ratio. This is the exact mechanism that drives up S3 API costs.

![Manifest tree showing delete files](./img/Screenshot%202026-06-13%20at%2012.33.53%E2%80%AFPM.png)
*Manifest tree showing how delete files accumulate alongside data files.*

## 5. Storage Cost Is Not Just About Data Size

Beyond the read-time penalty, delete files and accumulated snapshots quietly inflate your storage usage. Storage cost drifts away from the "actual data" mental model because live data is only part of the bill. 

For one of our tables, we saw the total storage footprint increase by 1.5x in just one month, even though the raw data size hadn't grown by nearly that amount.

This bloat comes from three main sources:
- **Snapshot accumulation:** Every snapshot creates a new metadata file and keeps history alive.
- **Orphan files:** Files left behind after failed jobs or interrupted compactions quietly add waste.
- **Metadata growth:** Manifest lists, delete-file logs, and checkpoint files scale continuously.

![Storage analytics view](./img/Screenshot%202026-06-13%20at%2012.34.35%E2%80%AFPM.png)
*Storage analytics showing file sizes, counts, and partition distribution.*

## 6. Building Fern to Actually Understand What Was Happening

Honestly, Fern started because I was confused. We had run maintenance on some of the tables that were degrading, and nothing seemed to obviously change. I wasn't sure what I was supposed to be looking at. When compaction runs, does a new data file get joined to a new snapshot? Do old snapshots merge into one? Where do the delete files actually go?

The Iceberg docs explain the concepts clearly enough, but I couldn't see it happening. So I built Fern mostly to visualize the relationship between snapshots, manifests, and the actual data files underneath. I wanted to watch the state change after a maintenance run and understand what compaction actually did.

What I found once I had that view was more alarming than I expected. Some tables had over 3,500 snapshots. Others had exponentially more delete files than data files, because compaction rewrites data files but it does not automatically clean up the delete files from previous cycles. Those keep accumulating unless you explicitly expire them. We hadn't been doing that consistently, and it showed.

The most useful thing wasn't a single feature—it was just being able to look at the whole catalog and compare tables side by side. There's no native Iceberg view that gives you a ranked list of tables by delete-file count or snapshot age. You have to query each table individually. Fern is that view.

What I want to build next is a cron that runs the maintenance checks automatically, generates alerts when a table crosses a threshold, and then triggers the cleanup through the Iceberg APIs without manual intervention. That's where this is headed.

![Catalog-level table health overview](./img/Screenshot%202026-06-13%20at%2012.44.37%E2%80%AFPM.png)
*All tables ranked by health signals. This is the view I wish I had before the 8-hour timeout.*

![Optimization suggestions](./img/Screenshot%202026-06-13%20at%2012.34.18%E2%80%AFPM.png)
*Suggested maintenance operations based on the table's current state.*

## Conclusion

Iceberg adoption is generally much easier than Iceberg operations. Write-side configurations like MOR solve the immediate pain of the ingestion window, but they introduce new pain in the form of delete files and query debt.

The hardest part of Iceberg was not learning the format. It was learning how quickly operational debt can build when write patterns, delete files, and maintenance needs stop being visible. 

Storage costs aren't just your data—they are your waste, your history, and your metadata. Maintenance needs visibility, not just scheduled cron jobs.

## P.S.

If you are wrestling with Iceberg's hidden costs or want a better way to inspect these operational signals, you might find Fern useful. It started as a learning project, but it's completely open source. 

Check out the repo at [https://github.com/dikshantks/fern](https://github.com/dikshantks/fern). Feedback and contributions are always welcome!
