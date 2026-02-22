# Delivery Failure Analysis System – Solution Design (AI-Powered)

## 1. Problem Overview

Logistics operations suffer from data silos. Order data, fleet logs, warehouse records, and external
factors (weather/traffic) exist in separate systems, making it difficult to understand *why* a
delivery failed or was delayed. This solution aggregates these disparate data sources and uses a
Large Language Model (LLM) to provide automated, human-readable, actionable insights.

---

## 2. Solution Approach

The solution is a Python-based analytics engine with three layers:

1. **Data Aggregation** – Ingests structured CSVs (Orders, Fleet, Warehouse, Weather, Feedback) and
   joins them into a unified "Order Journey" dataset.
2. **Event Correlation** – Time-aligns and links events (e.g., matching a "Late" delivery with a
   "Heavy Rain" weather record for the same `order_id`).
3. **AI-Powered Insight Generation** – Uses Google Gemini for two distinct tasks:
   - **Intent Parsing**: Interprets *any* free-form natural language question into structured
     query parameters (city, client, warehouse, order ID, time range).
   - **Narrative Generation**: Converts filtered data summaries into clear, human-readable
     explanations with root-cause analysis and actionable recommendations.
   - **SDK**: `google-genai`. **Default model**: `gemini-2.5-flash`. Configurable via
     `GEMINI_MODEL` environment variable (e.g. `gemini-2.0-flash`, `gemini-2.5-flash-lite`).

---

## 3. System Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                    Data Sources (CSVs)                        │
│  orders · clients · fleet_logs · warehouse_logs              │
│  weather · drivers · warehouses · feedback                   │
└────────────────────────┬─────────────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────────────────┐
│              Analytics Engine (Python / pandas)               │
│                                                               │
│   1. Data Ingestion   →   2. Aggregation   →   3. Enrichment │
│      (load_data)           (combine_data)      (enrich_data)  │
│                                                               │
│   Outputs: Unified DataFrame with consolidated_reason column  │
└───────────────────────┬──────────────────────────────────────┘
                        │
          ┌─────────────┴──────────────┐
          │                            │
          ▼                            ▼
┌─────────────────────┐    ┌───────────────────────────────────┐
│  User Query (CLI)   │    │     AI Layer (Google Gemini)       │
│                     │    │                                    │
│  --ask "Why did     │───▶│  Step 1: llm_parse_intent()        │
│   deliveries fail   │    │    → Returns structured JSON:      │
│   in Mumbai?"       │    │      action, filters, cities,      │
│                     │    │      order_id, time_range          │
│  --filter_city      │    │                                    │
│  --compare_cities   │    │  Step 2: Filter DataFrame          │
│  --query_order      │    │    → Apply extracted parameters    │
│  --show_insights    │    │                                    │
│  --report           │    │  Step 3: llm_generate_narrative()  │
└─────────────────────┘    │    → Converts data summary to      │
                           │      human-readable analysis with  │
                           │      root causes + recommendations  │
                           └───────────────────────────────────┘
```

---

## 4. Key Components

### A. Data Integration

`orders.csv` is the central fact table. All other datasets join via `order_id` (or `client_id`,
`driver_id`, `warehouse_id`). Duplicate logs per order (multiple GPS pings, warehouse scans) are
resolved by selecting the **latest** record via `groupby().last()`.

### B. Root-Cause Heuristics (generate_reason)

Each order gets a `consolidated_reason` string built from a priority chain:

1. `failure_reason` column (order-level failure label)
2. `gps_delay_notes` (fleet/transit issues, e.g. "Address not found")
3. `warehouse_notes` (stock delay, mis-pick, etc.)
4. `weather_condition` (Rain, Fog, Storm)
5. `traffic_condition` (Heavy, Jam)
6. `event_type` (Strike, Festival, etc.)

### C. LLM Intent Parser (`llm_parse_intent`)

Instead of fragile regex/keyword matching, the user's question is sent to **Gemini** (default: gemini-2.5-flash)
with a prompt that includes the actual list of cities, client names, and warehouse names from the
dataset. The model returns structured JSON (via `response_mime_type="application/json"`), enabling
it to handle **any phrasing** – not just pre-defined patterns.

Example: *"What's going wrong with Saini's shipments this month?"* correctly maps to
`{"action": "filter_analysis", "filters": {"client": "Saini"}, "time_range": "this month"}`.

### D. LLM Narrative Generator (`llm_generate_narrative`)

A concise data summary (totals, failure rates, top reasons, weather/traffic breakdowns) is sent to
**Gemini** instructing it to act as a logistics analyst. The model returns:

- A direct answer to the user's question
- Key root causes with specific numbers
- Severity-ranked issues
- 2–3 actionable operational recommendations

Output is **plain English prose** – no raw tables, no SQL, no markdown code blocks.

---

## 5. Why LLM over Rule-Based NLP?

| Capability | Previous (Rule-Based) | New (LLM-Based) |
|---|---|---|
| Handles only predefined query patterns | ✅ Only known queries | ✅ Any natural language |
| Entity extraction | Regex + exact string match | Semantic matching via LLM |
| Output format | Raw pandas tables | Human-readable narrative |
| Root cause explanation | Static template strings | Contextual LLM analysis |
| Handles typos / paraphrasing | ❌ Breaks | ✅ Robust |
| Actionable recommendations | ❌ None | ✅ Auto-generated |

---

## 6. Potential Extensions

- **Real-time Ingestion**: Replace CSV reads with Kafka/database streams.
- **Predictive Model**: Train a classifier on the enriched DataFrame to predict future delays.
- **Web Dashboard**: Expose the engine via a FastAPI backend + React/Streamlit frontend.
- **Multi-turn Chat**: Maintain conversation history for follow-up questions.
