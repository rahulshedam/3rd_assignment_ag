# Delivery Analytics System – Walkthrough (AI-Powered Edition)

## Overview

This system aggregates multi-domain logistics data and uses **Google Gemini** to:
- Understand *any* natural language question (not just pre-defined patterns)
- Generate **human-readable, narrative explanations** instead of raw tables
- Provide root-cause analysis and actionable recommendations

---

## Prerequisites

**1. Python 3.x**

**2. Install dependencies:**
```bash
pip install -r requirements.txt
```

**3. Set your Gemini API key (free tier available):**

Get a free key at: https://aistudio.google.com/app/apikey

PowerShell:
```powershell
$env:GEMINI_API_KEY = "AIza..."
```

Bash / zsh:
```bash
export GEMINI_API_KEY="AIza..."
```

> The system uses `gemini-2.5-flash` by default (available on Google AI Studio free tier).
> Override the model with `$env:GEMINI_MODEL="gemini-2.0-flash"` or `gemini-2.5-flash-lite` if needed.

---

## Sample Use Cases (from the Assignment)

### Use Case 1 – Deliveries delayed in a city
```bash
python delivery_analytics.py --ask "Why were deliveries delayed in New Delhi yesterday?"
```

### Use Case 2 – Client-specific failures
```bash
python delivery_analytics.py --ask "Why did Client Saini's orders fail in the past week?"
```

### Use Case 3 – Warehouse root-cause analysis
```bash
python delivery_analytics.py --ask "Explain the top reasons for delivery failures linked to Warehouse 1 in August"
```

### Use Case 4 – City comparison
```bash
python delivery_analytics.py --ask "Compare delivery failure causes between Mumbai and New Delhi last month"
```

### Use Case 5 – Festival period analysis
```bash
python delivery_analytics.py --ask "What are the likely causes of delivery failures during festival periods and how should we prepare?"
```

### Use Case 6 – New client onboarding risk
```bash
python delivery_analytics.py --ask "If we onboard a new client with 20000 extra monthly orders, what failure risks should we expect and how do we mitigate them?"
```

---

## All Available Commands

### Ask anything in plain English (AI-powered)
```bash
python delivery_analytics.py --ask "<your question>"
```
The AI will detect the intent, apply the right filters, and return a narrative answer.

### Filter by city
```bash
python delivery_analytics.py --filter_city "New Delhi" --show_insights
```

### Filter by client
```bash
python delivery_analytics.py --filter_client "Saini" --show_insights
```

### Filter by warehouse
```bash
python delivery_analytics.py --filter_warehouse "Warehouse 1" --show_insights
```

### Compare two cities
```bash
python delivery_analytics.py --compare_cities "Mumbai" "New Delhi"
```

### Analyse a specific order
```bash
python delivery_analytics.py --query_order 123
```

### Overall aggregate insights
```bash
python delivery_analytics.py --show_insights
```

### Generate a full narrative report (saved to analysis_report.md)
```bash
python delivery_analytics.py --report
```

---

## How the AI Works

1. **Intent Detection** – Your question is sent to Gemini along with the list of all known
   cities, clients, and warehouses in the dataset. The model returns a structured JSON object
   identifying what you're asking about (city, client, warehouse, specific order, or general).

2. **Data Filtering** – The system applies the detected filters to the unified DataFrame.

3. **Narrative Generation** – A concise data summary (failure rates, top reasons, weather/traffic
   breakdown) is sent back to Gemini. The model responds as a logistics analyst with:
   - A direct answer in plain English
   - Root causes ranked by severity with specific numbers
   - 2–3 actionable recommendations

---

## Sample AI Output

**Query:** `"Why were deliveries delayed in Mumbai last week?"`

**Response (example):**
> Mumbai experienced a notably high delivery failure and delay rate last week, with approximately
> 38% of orders either failing or arriving late. The analysis points to three dominant root causes:
>
> **1. Fleet and Transit Issues (52% of failures)** – Heavy congestion on key routes (R2, R5) and
> address-not-found incidents together account for the majority of delays. Route R2 alone shows a
> 41% late-delivery rate.
>
> **2. Weather Conditions (31% of failures)** – Rain and fog conditions during the delivery window
> significantly impacted last-mile performance. Orders during rainy days were 2× more likely to
> be late.
>
> **3. Warehouse Dispatch Delays (17% of failures)** – Stock delays at Warehouse 3 caused late
> dispatch for a cluster of orders, compounding transit delays.
>
> **Recommendations:**
> - Deploy dynamic re-routing for drivers on R2 and R5 during peak hours.
> - Implement weather-triggered SLA extensions and proactive customer notifications.
> - Audit Warehouse 3 picking workflows to reduce pre-dispatch delays.
