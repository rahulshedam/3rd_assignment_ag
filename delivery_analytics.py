import pandas as pd
import argparse
import os
import json
from google import genai
from google.genai import types

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DATA_DIR = r"c:/Talentica/AI Assignments/3rd_assignment_ag/third-assignment-sample-data-set"
OUTPUT_REPORT = "analysis_report.md"

# Default model; override with GEMINI_MODEL env var if needed
# Options: gemini-2.5-flash, gemini-2.0-flash, gemini-2.5-flash-lite
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")

_llm_client = None

def get_llm_client():
    """Return a configured Gemini client, reading GEMINI_API_KEY from the environment."""
    global _llm_client
    if _llm_client is None:
        api_key = os.environ.get("GEMINI_API_KEY")
        if not api_key:
            raise EnvironmentError(
                "GEMINI_API_KEY environment variable is not set.\n"
                "Get a free key at: https://aistudio.google.com/app/apikey\n"
                "Then run (PowerShell):  $env:GEMINI_API_KEY='AIza...'\n"
                "        or (bash/zsh):  export GEMINI_API_KEY='AIza...'"
            )
        _llm_client = genai.Client(api_key=api_key)
    return _llm_client


# ---------------------------------------------------------------------------
# Data Loading & Merging
# ---------------------------------------------------------------------------

def load_data():
    """Load all CSV datasets from the data directory."""
    print("Loading datasets...")
    try:
        orders        = pd.read_csv(os.path.join(DATA_DIR, "orders.csv"))
        clients       = pd.read_csv(os.path.join(DATA_DIR, "clients.csv"))
        drivers       = pd.read_csv(os.path.join(DATA_DIR, "drivers.csv"))
        warehouses    = pd.read_csv(os.path.join(DATA_DIR, "warehouses.csv"))
        fleet_logs    = pd.read_csv(os.path.join(DATA_DIR, "fleet_logs.csv"))
        warehouse_logs = pd.read_csv(os.path.join(DATA_DIR, "warehouse_logs.csv"))
        weather       = pd.read_csv(os.path.join(DATA_DIR, "weather.csv"))
        feedback      = pd.read_csv(os.path.join(DATA_DIR, "feedback.csv"))

        for col in ['order_date', 'promised_delivery_date', 'actual_delivery_date']:
            if col in orders.columns:
                orders[col] = pd.to_datetime(orders[col], errors='coerce')

        return {
            "orders": orders, "clients": clients, "drivers": drivers,
            "warehouses": warehouses, "fleet_logs": fleet_logs,
            "warehouse_logs": warehouse_logs, "weather": weather,
            "feedback": feedback,
        }
    except Exception as e:
        print(f"Error loading data: {e}")
        return None


def combine_data(data):
    """Merge all datasets into one unified order-centric DataFrame."""
    print("Combining data...")
    df = data["orders"].copy()

    # Clients
    clients = data["clients"][['client_id', 'client_name', 'city', 'state']].rename(
        columns={'city': 'client_city', 'state': 'client_state'})
    df = df.merge(clients, on='client_id', how='left')

    # Fleet logs – keep latest log per order
    fleet_agg = (data["fleet_logs"]
                 .sort_values('created_at')
                 .groupby('order_id').last()
                 .reset_index())
    df = df.merge(
        fleet_agg[['order_id', 'driver_id', 'gps_delay_notes', 'route_code']],
        on='order_id', how='left')

    # Drivers
    drivers = data["drivers"][['driver_id', 'driver_name', 'partner_company', 'city']].rename(
        columns={'city': 'driver_city'})
    df = df.merge(drivers, on='driver_id', how='left')

    # Warehouse logs – keep latest log per order
    wh_agg = (data["warehouse_logs"]
              .sort_values('picking_end')
              .groupby('order_id').last()
              .reset_index())
    df = df.merge(
        wh_agg[['order_id', 'warehouse_id', 'notes']].rename(columns={'notes': 'warehouse_notes'}),
        on='order_id', how='left')

    # Warehouses
    warehouses = data["warehouses"][['warehouse_id', 'warehouse_name', 'city']].rename(
        columns={'city': 'warehouse_city'})
    df = df.merge(warehouses, on='warehouse_id', how='left')

    # Weather / external factors – keep latest per order
    weather_agg = data["weather"].groupby('order_id').last().reset_index()
    df = df.merge(
        weather_agg[['order_id', 'weather_condition', 'traffic_condition', 'event_type']],
        on='order_id', how='left')

    # Customer feedback – keep latest per order
    fb_agg = data["feedback"].groupby('order_id').last().reset_index()
    df = df.merge(
        fb_agg[['order_id', 'feedback_text', 'rating', 'sentiment']],
        on='order_id', how='left')

    return df


def assess_performance(df):
    """Classify each order as late or failed."""
    df['is_late'] = (
        (df['actual_delivery_date'] > df['promised_delivery_date']) &
        (df['status'] == 'Delivered'))
    df['is_failed'] = df['status'].isin(['Failed', 'Returned', 'Cancelled'])
    return df


def generate_reason(row):
    """Build a consolidated reason string from all available signals."""
    reasons = []

    if row['is_failed'] and pd.notna(row.get('failure_reason')):
        reasons.append(f"Status: {row['failure_reason']}")

    if pd.notna(row.get('gps_delay_notes')):
        reasons.append(f"Fleet: {row['gps_delay_notes']}")

    if pd.notna(row.get('warehouse_notes')):
        reasons.append(f"Warehouse: {row['warehouse_notes']}")

    if pd.notna(row.get('weather_condition')) and row['weather_condition'] not in ['Clear', 'Sunny']:
        reasons.append(f"Weather: {row['weather_condition']}")

    if pd.notna(row.get('traffic_condition')) and row['traffic_condition'] in ['Heavy', 'Jam']:
        reasons.append(f"Traffic: {row['traffic_condition']}")

    if pd.notna(row.get('event_type')):
        reasons.append(f"Event: {row['event_type']}")

    if not reasons and (row['is_late'] or row['is_failed']):
        return "Unknown Operational Delay"

    return "; ".join(reasons)


def enrich_data(df):
    df = assess_performance(df)
    df['consolidated_reason'] = df.apply(generate_reason, axis=1)
    return df


# ---------------------------------------------------------------------------
# LLM-Powered Intent Parsing
# ---------------------------------------------------------------------------

def llm_parse_intent(question: str, data: dict) -> dict:
    """
    Ask Gemini to extract structured intent from a free-form user question.
    Returns a dict with keys: action, order_id, cities, filters, time_range.
    """
    cities     = sorted(data['orders']['city'].dropna().unique().tolist())
    clients    = sorted(data['clients']['client_name'].dropna().unique().tolist())
    warehouses = sorted(data['warehouses']['warehouse_name'].dropna().unique().tolist())

    prompt = f"""You are an intent-extraction assistant for a logistics delivery analytics system.

Available entities in the dataset:
- Cities (delivery): {', '.join(cities[:30])}
- Client names: {', '.join(clients[:30])}
- Warehouse names: {', '.join(warehouses)}

Parse the user's question and return a JSON object with EXACTLY these fields:
{{
  "action": "<one of: query_order | compare_cities | filter_analysis | show_insights>",
  "order_id": <integer order ID if a specific order is mentioned, else null>,
  "cities": [<city1>, <city2>] if comparing two cities, else null,
  "filters": {{
    "city": "<city name if a single city is mentioned, else null>",
    "client": "<client name if a client is mentioned, else null>",
    "warehouse": "<warehouse name if a warehouse is mentioned, else null>"
  }},
  "time_range": "<description of any time period mentioned, e.g. 'last week', 'August', else null>"
}}

Rules:
- Use action=query_order when a specific order number is asked about.
- Use action=compare_cities when the question asks to compare exactly two cities.
- Use action=filter_analysis when the question is about a specific city, client, or warehouse.
- Use action=show_insights for general/aggregate questions.
- Match city/client/warehouse names case-insensitively to the lists above; use the canonical name.
- Return ONLY valid JSON, no markdown fences or extra text.

User question: {question}"""

    client = get_llm_client()
    response = client.models.generate_content(
        model=GEMINI_MODEL,
        contents=prompt,
        config=types.GenerateContentConfig(
            response_mime_type="application/json",
            temperature=0,
        ),
    )
    return json.loads(response.text)


# ---------------------------------------------------------------------------
# LLM-Powered Narrative Generation
# ---------------------------------------------------------------------------

def llm_generate_narrative(question: str, data_summary: str, context: str) -> str:
    """
    Ask Gemini to turn a data summary into a clear, human-readable analysis.
    """
    prompt = f"""You are an expert logistics analyst providing human-readable insights to operations managers.

Your response must:
1. Directly answer the question in plain English.
2. Highlight the key root causes and patterns with specific numbers.
3. Call out the most critical issues in order of severity.
4. End with 2-3 concise, actionable recommendations.
5. Use short paragraphs and bullet points - NO raw tables, NO SQL, NO markdown code blocks.
6. Keep the tone professional but conversational.

Question: {question}

Context: {context}

Data Summary:
{data_summary}

Please provide a thorough, human-readable analysis."""

    client = get_llm_client()
    response = client.models.generate_content(
        model=GEMINI_MODEL,
        contents=prompt,
        config=types.GenerateContentConfig(temperature=0.3),
    )
    return response.text


# ---------------------------------------------------------------------------
# Data Summary Helpers (feed to LLM)
# ---------------------------------------------------------------------------

def prepare_data_summary(df: pd.DataFrame) -> str:
    """Produce a compact textual summary of an (optionally filtered) DataFrame."""
    if df.empty:
        return "No orders found matching the given criteria."

    total    = len(df)
    failures = df[df['is_failed'] | df['is_late']]
    rate     = len(failures) / total * 100 if total > 0 else 0.0

    lines = [
        f"Total orders: {total}",
        f"Failed or late: {len(failures)} ({rate:.1f}%)",
        f"Successfully on-time: {total - len(failures)} ({100 - rate:.1f}%)",
    ]

    if not failures.empty:
        lines.append("\nTop 5 consolidated failure reasons:")
        for reason, cnt in failures['consolidated_reason'].value_counts().head(5).items():
            lines.append(f"  • {reason}: {cnt} orders ({cnt / len(failures) * 100:.1f}%)")

        if 'weather_condition' in df.columns:
            wc = failures['weather_condition'].value_counts().head(4)
            lines.append("\nWeather conditions during failures:")
            for cond, cnt in wc.items():
                if pd.notna(cond):
                    lines.append(f"  • {cond}: {cnt}")

        if 'traffic_condition' in df.columns:
            tc = failures['traffic_condition'].value_counts().head(3)
            lines.append("\nTraffic conditions during failures:")
            for cond, cnt in tc.items():
                if pd.notna(cond):
                    lines.append(f"  • {cond}: {cnt}")

        if 'city' in df.columns:
            lines.append("\nTop 5 cities by failure count:")
            for city, cnt in failures['city'].value_counts().head(5).items():
                if pd.notna(city):
                    city_total = len(df[df['city'] == city])
                    lines.append(f"  • {city}: {cnt} failures out of {city_total} orders")

        if 'client_name' in df.columns:
            lines.append("\nTop 5 clients by failure count:")
            for client, cnt in failures['client_name'].value_counts().head(5).items():
                if pd.notna(client):
                    lines.append(f"  • {client}: {cnt}")

        if 'warehouse_name' in df.columns:
            lines.append("\nTop 5 warehouses by failure count:")
            for wh, cnt in failures['warehouse_name'].value_counts().head(5).items():
                if pd.notna(wh):
                    lines.append(f"  • {wh}: {cnt}")

        if 'event_type' in df.columns:
            events = failures['event_type'].dropna().value_counts().head(3)
            if not events.empty:
                lines.append("\nExternal events linked to failures:")
                for ev, cnt in events.items():
                    lines.append(f"  • {ev}: {cnt}")

        if 'feedback_text' in df.columns:
            samples = failures['feedback_text'].dropna().head(5).tolist()
            if samples:
                lines.append("\nSample customer feedback from failed/late orders:")
                for fb in samples:
                    lines.append(f"  - \"{fb}\"")

    return "\n".join(lines)


def prepare_single_order_summary(row) -> str:
    """Build a detailed text block for a single order row."""
    fields = {
        "Order ID":          row.get('order_id'),
        "Customer":          row.get('customer_name'),
        "Status":            row.get('status'),
        "Performance":       ("LATE" if row.get('is_late') else
                              "FAILED" if row.get('is_failed') else "ON TIME"),
        "Promised delivery": row.get('promised_delivery_date'),
        "Actual delivery":   row.get('actual_delivery_date'),
        "City":              row.get('city'),
        "Client":            row.get('client_name'),
        "Warehouse":         row.get('warehouse_name'),
        "Fleet note":        row.get('gps_delay_notes'),
        "Warehouse note":    row.get('warehouse_notes'),
        "Weather":           row.get('weather_condition'),
        "Traffic":           row.get('traffic_condition'),
        "External event":    row.get('event_type'),
        "Customer feedback": row.get('feedback_text'),
        "Feedback rating":   row.get('rating'),
        "Failure reason":    row.get('failure_reason'),
    }
    lines = []
    for k, v in fields.items():
        if v is not None and pd.notna(v):
            lines.append(f"  {k}: {v}")
    return "\n".join(lines)


def prepare_city_comparison_summary(df: pd.DataFrame, city1: str, city2: str) -> str:
    """Build a side-by-side comparison summary for two cities."""
    lines = []
    for city in [city1, city2]:
        city_df = df[df['city'].str.lower() == city.lower()]
        if city_df.empty:
            lines.append(f"\n{city}: No data available.")
            continue
        failures = city_df[city_df['is_failed'] | city_df['is_late']]
        rate = len(failures) / len(city_df) * 100
        lines.append(f"\n{city}:")
        lines.append(f"  Total orders: {len(city_df)}, Failed/Late: {len(failures)} ({rate:.1f}%)")
        if not failures.empty:
            lines.append("  Top reasons:")
            for reason, cnt in failures['consolidated_reason'].value_counts().head(4).items():
                lines.append(f"    • {reason}: {cnt}")
            wc = failures['weather_condition'].value_counts().head(2)
            if not wc.empty:
                lines.append(f"  Dominant weather: {', '.join(f'{c} ({n})' for c, n in wc.items() if pd.notna(c))}")
            tc = failures['traffic_condition'].value_counts().head(2)
            if not tc.empty:
                lines.append(f"  Dominant traffic: {', '.join(f'{c} ({n})' for c, n in tc.items() if pd.notna(c))}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Analysis Entry Points
# ---------------------------------------------------------------------------

def analyze_order(rich_df: pd.DataFrame, order_id: int, question: str):
    order = rich_df[rich_df['order_id'] == order_id]
    if order.empty:
        print(f"Order {order_id} not found in the dataset.")
        return
    summary  = prepare_single_order_summary(order.iloc[0])
    narrative = llm_generate_narrative(
        question, summary,
        f"Detailed analysis for Order {order_id}"
    )
    print(f"\n{narrative}")


def analyze_filtered(rich_df: pd.DataFrame, question: str, context: str):
    summary  = prepare_data_summary(rich_df)
    narrative = llm_generate_narrative(question, summary, context)
    print(f"\n{narrative}")


def analyze_comparison(rich_df: pd.DataFrame, city1: str, city2: str, question: str):
    summary  = prepare_city_comparison_summary(rich_df, city1, city2)
    narrative = llm_generate_narrative(
        question, summary,
        f"Side-by-side comparison: {city1} vs {city2}"
    )
    print(f"\n{narrative}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="AI-Powered Delivery Failure Analysis System",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Examples:
  python delivery_analytics.py --ask "Why were deliveries delayed in Mumbai last week?"
  python delivery_analytics.py --ask "Why did Client Saini's orders fail?"
  python delivery_analytics.py --ask "Compare delivery failures between Mumbai and Delhi"
  python delivery_analytics.py --ask "What are the top failure reasons for Warehouse 1?"
  python delivery_analytics.py --ask "What happens during festival periods?"
  python delivery_analytics.py --report
  python delivery_analytics.py --filter_city "New Delhi" --show_insights"""
    )
    parser.add_argument("--ask",             type=str,  help="Ask any question in plain English (AI-powered)")
    parser.add_argument("--query_order",     type=int,  help="Deep-dive into a specific order ID")
    parser.add_argument("--filter_city",     type=str,  help="Filter analysis by city name")
    parser.add_argument("--filter_client",   type=str,  help="Filter analysis by client name")
    parser.add_argument("--filter_warehouse",type=str,  help="Filter analysis by warehouse name or city")
    parser.add_argument("--compare_cities",  nargs=2,   help="Compare two cities: --compare_cities CityA CityB")
    parser.add_argument("--show_insights",   action="store_true", help="Show aggregate insights")
    parser.add_argument("--report",          action="store_true", help="Generate a full narrative report file")
    args = parser.parse_args()

    data = load_data()
    if not data:
        return

    full_df = combine_data(data)
    rich_df = enrich_data(full_df)

    # ------------------------------------------------------------------
    # AI Natural Language path  (--ask)
    # ------------------------------------------------------------------
    if args.ask:
        print(f"\nQuestion: \"{args.ask}\"")
        print("Thinking...\n" + "-" * 60)
        try:
            intent = llm_parse_intent(args.ask, data)
            action = intent.get('action', 'show_insights')

            if action == 'query_order' and intent.get('order_id'):
                analyze_order(rich_df, int(intent['order_id']), args.ask)

            elif action == 'compare_cities' and intent.get('cities') and len(intent['cities']) >= 2:
                analyze_comparison(rich_df, intent['cities'][0], intent['cities'][1], args.ask)

            elif action == 'filter_analysis':
                filters   = intent.get('filters', {}) or {}
                subset    = rich_df.copy()
                desc_parts = []

                city = filters.get('city')
                if city:
                    subset = subset[subset['city'].str.lower() == city.lower()]
                    desc_parts.append(f"City={city}")

                client = filters.get('client')
                if client:
                    subset = subset[subset['client_name'].str.contains(client, case=False, na=False)]
                    desc_parts.append(f"Client={client}")

                warehouse = filters.get('warehouse')
                if warehouse:
                    mask = (subset['warehouse_name'].str.lower() == warehouse.lower()) | \
                           (subset['warehouse_city'].str.lower() == warehouse.lower())
                    subset = subset[mask]
                    desc_parts.append(f"Warehouse={warehouse}")

                time_range = intent.get('time_range')
                if time_range:
                    desc_parts.append(f"Period={time_range}")

                context = f"Filtered view ({', '.join(desc_parts)})" if desc_parts else "Aggregate view"
                analyze_filtered(subset, args.ask, context)

            else:
                analyze_filtered(rich_df, args.ask, "Full dataset – aggregate view")

        except EnvironmentError as e:
            print(f"\nConfiguration error:\n{e}")
        except Exception as e:
            print(f"\nAI analysis failed: {e}")
            print("Tip: Check that GEMINI_API_KEY is set correctly.")
        return

    # ------------------------------------------------------------------
    # Direct CLI flag paths
    # ------------------------------------------------------------------
    if args.query_order:
        analyze_order(rich_df, args.query_order,
                      f"Give a detailed explanation for why order {args.query_order} failed or was delayed.")
        return

    if args.compare_cities:
        analyze_comparison(rich_df, args.compare_cities[0], args.compare_cities[1],
                           f"Compare delivery failure causes between {args.compare_cities[0]} and {args.compare_cities[1]}.")
        return

    # Build filtered subset for insight commands
    subset     = rich_df.copy()
    desc_parts = []

    if args.filter_city:
        subset = subset[subset['city'] == args.filter_city]
        desc_parts.append(f"City={args.filter_city}")

    if args.filter_client:
        subset = subset[subset['client_name'].str.contains(args.filter_client, case=False, na=False)]
        desc_parts.append(f"Client={args.filter_client}")

    if args.filter_warehouse:
        mask = ((subset['warehouse_city'] == args.filter_warehouse) |
                (subset['warehouse_name'] == args.filter_warehouse))
        subset = subset[mask]
        desc_parts.append(f"Warehouse={args.filter_warehouse}")

    if args.show_insights or desc_parts:
        context = f"Filtered: {', '.join(desc_parts)}" if desc_parts else "Full dataset"
        question = f"What are the main delivery failure patterns and root causes? ({context})"
        analyze_filtered(subset, question, context)

    if args.report:
        print(f"\nGenerating full report → {OUTPUT_REPORT} ...")
        question = (
            "Generate a comprehensive delivery failure analysis report. "
            "Cover: overall stats, top root causes, city/warehouse hotspots, "
            "weather/traffic impact, and actionable recommendations."
        )
        summary   = prepare_data_summary(rich_df)
        narrative = llm_generate_narrative(question, summary, "Full dataset – executive report")
        with open(OUTPUT_REPORT, "w", encoding="utf-8") as f:
            f.write("# Delivery Failure Analysis Report\n\n")
            f.write(narrative)
        print(f"Report saved to {OUTPUT_REPORT}")

    if not any([args.ask, args.query_order, args.compare_cities,
                args.show_insights, args.filter_city, args.filter_client,
                args.filter_warehouse, args.report]):
        parser.print_help()


if __name__ == "__main__":
    main()
