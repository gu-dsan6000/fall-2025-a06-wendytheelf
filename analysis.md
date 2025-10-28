# Analysis Report

All tasks were executed on an AWS Spark cluster using PySpark, and outputs were saved to the `data/output/` directory.  
Screenshots of the Spark Web UI and visualization results are referenced where appropriate.

---

## Problem 1:

### **Goal**
Analyze the distribution of log levels (INFO, WARN, ERROR, DEBUG) across all system log files to understand message frequency and system reliability.

### **Approach**
- Loaded all log files into a Spark DataFrame.
- Extracted log levels using a regular expression pattern.
- Aggregated counts by log level using `groupBy` and `count`.
- Collected a 10-row random sample of log entries.
- Computed total log lines and summary statistics.

### **Key Findings**

| Metric | Value |
|:-------|:------|
| **Total log lines processed** | 33,236,605 |
| **Lines with identifiable log levels** | 27,410,336 |
| **Unique log levels found** | 3 (INFO, ERROR, WARN) |

**Log Level Distribution:**
INFO : 27,389,482 (99.92%)
ERROR : 11,259 (0.04%)
WARN : 9,595 (0.04%)


### **Interpretation**
- The majority of logs were **INFO**, suggesting normal system operation.
- Very few **ERROR** and **WARN** messages indicate high system stability.
- Absence of **DEBUG** implies production-level logging (minimal verbosity).

### **Performance Observation**
- **Processing time:** 238.83 seconds on the cluster.  
- The job scaled linearly with dataset size; minimal shuffling occurred since aggregation was on a single column.

### **Output Files**
- `data/output/problem1_counts.csv` — Counts of each log level  
- `data/output/problem1_sample.csv` — 10 random log samples  
- `data/output/problem1_summary.txt` — Summary statistics  

---

## Problem 2: Cluster Usage Analysis

### **Goal**
Investigate cluster utilization patterns to identify which clusters were most heavily used and how application workloads were distributed over time.

### **Approach**
- Parsed Spark event logs and extracted cluster IDs, application IDs, start, and end times.
- Aggregated the number of applications per cluster.
- Computed total clusters, total applications, and the average per cluster.
- Generated Seaborn-based time-series visualizations showing application execution timelines.

### **Key Findings**

| Metric | Value |
|:-------|:------|
| **Total unique clusters** | 6 |
| **Total applications** | 193 |
| **Average applications per cluster** | 32.17 |

**Most Active Clusters:**
Cluster 1485248649253 — 180 applications
Cluster 1472621869829 — 8 applications
Cluster 1448006111297 — 2 applications
Clusters 1440487435730, 1460011102909, 1474351042505 — 1 each


### **Interpretation**
- One main cluster (`1485248649253`) dominated workloads, handling 180 applications.
- Remaining clusters showed minimal or test-level activity.
- The timeline visualization displayed clear bursts of job execution, reflecting batch-scheduled workloads.

### **Performance Observation**
- **Execution time:** ~12 minutes on Spark cluster.  
- Heavy timestamp parsing and group-by operations introduced shuffle overhead, mitigated using caching and selective column projection.  
- Executor parallelism improved throughput and reduced stage delays.

### **Output Files**
- `data/output/problem2_timeline.csv` — Time-series dataset of application runs  
- `data/output/problem2_summary.txt` — Summary statistics  
- `data/output/problem2_clusters.csv` — Cluster-level usage counts  

---

## 🖥️ Spark Web UI Screenshots
![Cluster Timeline Plot](/home/ubuntu/dsan6000/fall-2025-a06-wendytheelf/Screenshot 2025-10-28 013304.png)

---

## Summary

| Aspect | Observation |
|:-------|:-------------|
| **Cluster Health** | Stable — negligible WARN/ERROR rates |
| **Computation Scaling** | Linear with dataset size |
| **Cluster Utilization** | Highly skewed toward one production cluster |
| **Optimizations Used** | Caching, column projection, selective filtering |
| **Result Validation** | Verified via Spark Web UI and output summaries |

