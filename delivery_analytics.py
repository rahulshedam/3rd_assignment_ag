import pandas as pd
import argparse
import os
from tabulate import tabulate

# Configuration
DATA_DIR = r"c:/Talentica/AI Assignments/3rd_assignment_ag/third-assignment-sample-data-set"
OUTPUT_REPORT = "analysis_report.md"

def load_data():
    """Load all CSV datasets."""
    print("Loading datasets...")
    try:
        orders = pd.read_csv(os.path.join(DATA_DIR, "orders.csv"))
        clients = pd.read_csv(os.path.join(DATA_DIR, "clients.csv"))
        drivers = pd.read_csv(os.path.join(DATA_DIR, "drivers.csv"))
        warehouses = pd.read_csv(os.path.join(DATA_DIR, "warehouses.csv"))
        fleet_logs = pd.read_csv(os.path.join(DATA_DIR, "fleet_logs.csv"))
        warehouse_logs = pd.read_csv(os.path.join(DATA_DIR, "warehouse_logs.csv"))
        weather = pd.read_csv(os.path.join(DATA_DIR, "weather.csv"))
        feedback = pd.read_csv(os.path.join(DATA_DIR, "feedback.csv"))
        
        # Convert dates
        date_cols = ['order_date', 'promised_delivery_date', 'actual_delivery_date']
        for col in date_cols:
            if col in orders.columns:
                orders[col] = pd.to_datetime(orders[col], errors='coerce')
                
        return {
            "orders": orders, "clients": clients, "drivers": drivers,
            "warehouses": warehouses, "fleet_logs": fleet_logs,
            "warehouse_logs": warehouse_logs, "weather": weather,
            "feedback": feedback
        }
    except Exception as e:
        print(f"Error loading data: {e}")
        return None

def combine_data(data):
    """Merge datasets into a single rich dataframe centered on orders."""
    print("Combining data...")
    df = data["orders"].copy()
    
    # 1. Clients
    clients = data["clients"][['client_id', 'client_name', 'city', 'state']].rename(
        columns={'city': 'client_city', 'state': 'client_state'}
    )
    df = df.merge(clients, on='client_id', how='left')
    
    # 2. Fleet Logs (get driver_id and notes)
    # Group by order to resolve duplicates (take latest)
    fleet_agg = data["fleet_logs"].sort_values('created_at').groupby('order_id').last().reset_index()
    fleet_subset = fleet_agg[['order_id', 'driver_id', 'gps_delay_notes', 'route_code']]
    df = df.merge(fleet_subset, on='order_id', how='left')
    
    # 3. Drivers (now that we have driver_id)
    drivers = data["drivers"][['driver_id', 'driver_name', 'partner_company', 'city']].rename(
        columns={'city': 'driver_city'}
    )
    df = df.merge(drivers, on='driver_id', how='left')

    # 4. Warehouse Logs
    wh_agg = data["warehouse_logs"].sort_values('picking_end').groupby('order_id').last().reset_index()
    wh_subset = wh_agg[['order_id', 'warehouse_id', 'notes']].rename(columns={'notes': 'warehouse_notes'})
    df = df.merge(wh_subset, on='order_id', how='left')
    
    # 5. Warehouses
    warehouses = data["warehouses"][['warehouse_id', 'warehouse_name', 'city']].rename(
        columns={'city': 'warehouse_city'}
    )
    df = df.merge(warehouses, on='warehouse_id', how='left')

    # 6. Weather
    weather_agg = data["weather"].groupby('order_id').last().reset_index()
    weather_subset = weather_agg[['order_id', 'weather_condition', 'traffic_condition', 'event_type']]
    df = df.merge(weather_subset, on='order_id', how='left')

    # 7. Feedback
    feedback_agg = data["feedback"].groupby('order_id').last().reset_index()
    feedback_subset = feedback_agg[['order_id', 'feedback_text', 'rating', 'sentiment']]
    df = df.merge(feedback_subset, on='order_id', how='left')

    return df

def assess_performance(df):
    """Determine Late/Failed status."""
    df['is_late'] = (df['actual_delivery_date'] > df['promised_delivery_date']) & (df['status'] == 'Delivered')
    df['is_failed'] = df['status'].isin(['Failed', 'Returned', 'Cancelled'])
    return df

def generate_reason(row):
    """Heuristic to generate a primary reason string."""
    reasons = []
    
    # 1. Delivery Failure/Return
    if row['is_failed']:
        if pd.notna(row.get('failure_reason')):
            reasons.append(f"Status: {row['failure_reason']}")
    
    # 2. Fleet/Transit Issues
    if pd.notna(row.get('gps_delay_notes')):
        reasons.append(f"Fleet: {row['gps_delay_notes']}")
        
    # 3. Warehouse Issues
    if pd.notna(row.get('notes')): # warehouse notes
        reasons.append(f"Warehouse: {row['notes']}")
        
    # 4. External Factors
    if pd.notna(row.get('weather_condition')) and row['weather_condition'] not in ['Clear', 'Sunny']:
         reasons.append(f"Weather: {row['weather_condition']}")
    if pd.notna(row.get('traffic_condition')) and row['traffic_condition'] in ['Heavy', 'Jam']:
         reasons.append(f"Traffic: {row['traffic_condition']}")
    if pd.notna(row.get('event_type')): # Strikes, etc
         reasons.append(f"Event: {row['event_type']}")
         
    if not reasons and (row['is_late'] or row['is_failed']):
        return "Unknown Operational Delay"
    
    return "; ".join(reasons)

def enrich_data(df):
    df = assess_performance(df)
    df['consolidated_reason'] = df.apply(generate_reason, axis=1)
    return df

def query_order(df, order_id):
    order = df[df['order_id'] == order_id]
    if order.empty:
        print(f"Order {order_id} not found.")
        return

    row = order.iloc[0]
    print(f"\n--- Order {order_id} Analysis ---")
    print(f"Customer: {row['customer_name']}")
    print(f"Status: {row['status']}")
    print(f"Promised: {row['promised_delivery_date']}")
    print(f"Actual:   {row['actual_delivery_date']}")
    print(f"Performance: {'LATE' if row.get('is_late') else 'ON TIME' if row['status']=='Delivered' else row['status'].upper()}")
    
    print("\n[Identified Factors]")
    if pd.notna(row.get('gps_delay_notes')): print(f"- Fleet Log: {row['gps_delay_notes']}")
    if pd.notna(row.get('notes')): print(f"- Warehouse Log: {row['notes']}")
    if pd.notna(row.get('weather_condition')): print(f"- Weather: {row['weather_condition']}")
    if pd.notna(row.get('traffic_condition')): print(f"- Traffic: {row['traffic_condition']}")
    if pd.notna(row.get('feedback_text')): 
        print(f"\n[Customer Feedback]\n\"{row['feedback_text']}\" (Rating: {row.get('rating')})")

def generate_insights(df, title="Aggregate Insights"):
    print(f"\n--- {title} ---")
    
    failures = df[df['is_failed'] | df['is_late']]
    if failures.empty:
        print("No failures or late deliveries found in this slice.")
        return

    print("\n[Top Failure Reasons]")
    print(failures['consolidated_reason'].value_counts().head(5).to_markdown())

    print("\n[Impact of Weather (Late Rate %)]")
    weather_impact = df.groupby('weather_condition')['is_late'].mean().mul(100).sort_values(ascending=False)
    print(weather_impact.head(5).to_markdown())
    
    print("\n[Warehouse Performance (Top 5 Highest Late Rates)]")
    wh_perf = df.groupby('warehouse_city')['is_late'].mean().mul(100).sort_values(ascending=False)
    print(wh_perf.head(5).to_markdown())

def compare_cities(df, city1, city2):
    print(f"\n--- Comparing {city1} vs {city2} ---")
    c1_df = df[df['city'] == city1]
    c2_df = df[df['city'] == city2]
    
    if c1_df.empty: print(f"No data for {city1}"); return
    if c2_df.empty: print(f"No data for {city2}"); return

    c1_fail_rate = (c1_df['is_failed'] | c1_df['is_late']).mean() * 100
    c2_fail_rate = (c2_df['is_failed'] | c2_df['is_late']).mean() * 100
    
    print(f"Failure/Late Rate: {city1}={c1_fail_rate:.1f}%, {city2}={c2_fail_rate:.1f}%")
    
    print(f"\n[Top Reasons in {city1}]")
    print(c1_df[c1_df['is_failed'] | c1_df['is_late']]['consolidated_reason'].value_counts().head(3).to_markdown())
    
    print(f"\n[Top Reasons in {city2}]")
    print(c2_df[c2_df['is_failed'] | c2_df['is_late']]['consolidated_reason'].value_counts().head(3).to_markdown())

def main():
    parser = argparse.ArgumentParser(description="Delivery Failure Analysis System")
import re

def parse_natural_language(question, data):
    """Parse natural language question to identify intents and entities."""
    question = question.lower()
    
    # 1. Identify Order ID
    order_match = re.search(r'order\s*#?\s*(\d+)', question)
    if order_match:
        return {'action': 'query_order', 'id': int(order_match.group(1))}
    
    # 2. Identify Entities from Data
    # specific entities first (longest match preference)
    found_filters = {}
    
    # Clients
    clients = data['clients']['client_name'].dropna().unique()
    for client in clients:
        if client.lower() in question:
            found_filters['client'] = client
            break # assume one client context for simplicity
            
    # Cities (normalize to title case for matching logic if needed, but simple contains is enough)
    # Get all unique cities from orders and warehouses
    all_cities = set(data['orders']['city'].dropna().unique())
    all_cities.update(data['warehouses']['city'].dropna().unique())
    
    found_cities = []
    for city in all_cities:
        # strict word boundary check might be better, but simple substring for now
        # avoiding partial matches like "New" in "New Delhi" if input is just "Delhi" is tricky
        # simpler approach: exact lower case match in input string
        if city.lower() in question:
            found_cities.append(city)
            
    # Warehouses
    warehouses = data['warehouses']['warehouse_name'].dropna().unique()
    for wh in warehouses:
        if wh.lower() in question:
            found_filters['warehouse'] = wh
            break

    # 3. Determine Intent
    if len(found_cities) >= 2:
        return {'action': 'compare_cities', 'cities': found_cities[:2]}
    elif len(found_cities) == 1:
        found_filters['city'] = found_cities[0]
        
    if found_filters:
        return {'action': 'filter_analysis', 'filters': found_filters}
        
    # Default fallback
    return {'action': 'show_insights'}

def main():
    parser = argparse.ArgumentParser(description="Delivery Failure Analysis System")
    parser.add_argument("--query_order", type=int, help="Analyze a specific order ID")
    parser.add_argument("--show_insights", action="store_true", help="Show aggregate insights")
    parser.add_argument("--filter_city", type=str, help="Filter analysis by City")
    parser.add_argument("--filter_client", type=str, help="Filter analysis by Client Name")
    parser.add_argument("--filter_warehouse", type=str, help="Filter analysis by Warehouse City or Name")
    parser.add_argument("--compare_cities", nargs=2, help="Compare two cities. Usage: --compare_cities \"City A\" \"City B\"")
    parser.add_argument("--ask", type=str, help="Ask a question in natural language. E.g., 'Why did order 123 fail?' or 'Compare Mumbai and Delhi'")
    parser.add_argument("--report", action="store_true", help="Generate full markdown report")
    args = parser.parse_args()

    data = load_data()
    if not data: return
    
    # Handle NLP Input
    if args.ask:
        print(f"Interpreting: '{args.ask}'")
        intent = parse_natural_language(args.ask, data)
        
        if intent['action'] == 'query_order':
            args.query_order = intent['id']
            print(f"-> Detected Intent: Analyze Order {args.query_order}")
        
        elif intent['action'] == 'compare_cities':
            args.compare_cities = intent['cities']
            print(f"-> Detected Intent: Compare {args.compare_cities[0]} and {args.compare_cities[1]}")
            
        elif intent['action'] == 'filter_analysis':
            filters = intent['filters']
            print(f"-> Detected Intent: Analysis Filters {filters}")
            if 'city' in filters: args.filter_city = filters['city']
            if 'client' in filters: args.filter_client = filters['client']
            if 'warehouse' in filters: args.filter_warehouse = filters['warehouse']
        else:
            print("-> Detected Intent: General Insights (No specific entities found)")
            args.show_insights = True

    full_df = combine_data(data)
    rich_df = enrich_data(full_df)

    # Handle Queries
    if args.query_order:
        query_order(rich_df, args.query_order)
        return

    # Filter Data for Insights if requested
    analysis_df = rich_df
    context_title = "Aggregate Insights"
    
    if args.filter_city:
        analysis_df = analysis_df[analysis_df['city'] == args.filter_city]
        context_title = f"Insights for City: {args.filter_city}"
        
    if args.filter_client:
        analysis_df = analysis_df[analysis_df['client_name'].str.contains(args.filter_client, case=False, na=False)]
        context_title = f"Insights for Client: {args.filter_client}"

    if args.filter_warehouse:
        # Match against warehouse_city or warehouse_name
        # Note: warehouse_name might be "Warehouse 1"
        mask = (analysis_df['warehouse_city'] == args.filter_warehouse) | (analysis_df['warehouse_name'] == args.filter_warehouse)
        analysis_df = analysis_df[mask]
        context_title = f"Insights for Warehouse: {args.filter_warehouse}"

    if args.compare_cities:
        compare_cities(full_df, args.compare_cities[0], args.compare_cities[1])
        return

    if args.show_insights or args.filter_city or args.filter_client or args.filter_warehouse:
        generate_insights(analysis_df, title=context_title)
        
    if args.report:
        # Placeholder for full report generation
        print(f"Generating full report to {OUTPUT_REPORT}...")
        with open(OUTPUT_REPORT, "w") as f:
            f.write("# Delivery Analysis Report\n\n")
            f.write("## Overview\n")
            f.write(f"Total Orders Analyzed: {len(rich_df)}\n")
            f.write(f"Total Failed/Late: {len(rich_df[rich_df['is_failed'] | rich_df['is_late']])}\n\n")
            f.write("## Top Failure Reasons\n")
            failures = rich_df[rich_df['is_failed'] | rich_df['is_late']]
            f.write(failures['consolidated_reason'].value_counts().head(20).to_markdown())
        print("Report generated.")

    if not (args.query_order or args.show_insights or args.report or args.filter_city or args.filter_client or args.filter_warehouse or args.compare_cities):
        print("No action specified.")
        print("Examples:")
        print("  python delivery_analytics.py --ask \"Why was order 101 late?\"")
        print("  python delivery_analytics.py --ask \"Compare Mumbai and Delhi\"")

if __name__ == "__main__":
    main()
