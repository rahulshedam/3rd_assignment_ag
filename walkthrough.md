# Delivery Analytics System - Walkthrough

## Overview
This system aggregates multi-domain logistics data to identify the root causes of delivery failures and delays. It provides an interactive command-line interface (CLI) to answer specific operational questions and generates comprehensive reports.

## Prerequisites
- Python 3.x
- Dependencies installed:
  ```bash
  pip install -r requirements.txt
  ```

## command Cheat Sheet (Sample Use Cases)

Here are the exact commands to run for the use cases defined in the assignment:

### 1. General Analysis
**"Ask questions in plain English (Natural Language)"**
Use the `--ask` flag to query the system without remembering specific filters.
```bash
python delivery_analytics.py --ask "Why was order 123 late?"
python delivery_analytics.py --ask "Compare Mumbai and New Delhi"
python delivery_analytics.py --ask "What is wrong with Client Saini?"
```

**"Generate a full report and see top failure reasons."**
```bash
python delivery_analytics.py --report
```
*Output: `analysis_report.md`*

**"Show me aggregate insights on the console."**
```bash
python delivery_analytics.py --show_insights
```

### 2. Specific Questions

**Q1: "Why were deliveries delayed in city X yesterday?"**
*Example: New Delhi*
```bash
python delivery_analytics.py --filter_city "New Delhi"
```

**Q2: "Why did Client X’s orders fail in the past week?"**
*Example: Client Saini*
```bash
python delivery_analytics.py --filter_client "Saini"
```

**Q3: "Explain the top reasons for delivery failures linked to Warehouse B?"**
*Example: Warehouse 1*
```bash
python delivery_analytics.py --filter_warehouse "Warehouse 1"
```

**Q4: "Compare delivery failure causes between City A and City B?"**
*Example: Mumbai vs New Delhi*
```bash
python delivery_analytics.py --compare_cities "Mumbai" "New Delhi"
```

**Q5/Q6: "What are likely causes of failure during festivals / If we onboard new clients?"**
Use the global insights to find systemic patterns (e.g., Weather, Traffic, Specific Event Types).
```bash
python delivery_analytics.py --show_insights
```
*Look for "Event: Festival" or high failure rates under specific conditions.*

### 3. Deep Dive into Specific Orders
**"Why did this specific order fail?"**
*Example: Order 123*
```bash
python delivery_analytics.py --query_order 123
```

## Interpreting the Output
The system generates a **Consolidated Reason** by checking multiple data sources:
- **Fleet**: GPS Delay Notes (e.g., "Heavy congestion", "Address not found").
- **Warehouse**: Internal Notes (e.g., "Stock delay").
- **Weather**: Conditions (e.g., "Rain", "Fog").
- **Traffic**: Conditions (e.g., "Heavy", "Jam").
- **Customer**: Feedback text and sentiment.

If an order is **Late** or **Failed**, the system lists all identified negative factors.
