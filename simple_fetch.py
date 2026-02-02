import yaml
import requests
import csv
import sys

def load_config():
    """Load configuration from YAML file"""
    with open('workflow.yml', 'r') as file:
        return yaml.safe_load(file)

def fetch_data(api_url):
    """Fetch data from API"""
    try:
        response = requests.get(api_url, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching data: {e}")
        sys.exit(1)

def save_to_csv(data, filename, fields):
    """Save ID and SKU to CSV file"""
    id_value = data.get('optionId', '')
    sku_value = data.get('modelNumber', '')
    
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(fields)  # Header
        writer.writerow([id_value, sku_value])  # Data
    
    print(f"✅ Data saved to: {filename}")
    print(f"📊 ID: {id_value}")
    print(f"📊 SKU: {sku_value}")

def main():
    """Main function"""
    print("🚀 Starting BedBathBeyond Product Fetcher")
    
    # Load configuration
    config = load_config()
    
    # Get workflow info
    workflow = config.get('workflow', {})
    print(f"📋 Workflow: {workflow.get('name', 'Unknown')}")
    
    # Get API URL
    api_url = config.get('api_url', '')
    if not api_url:
        print("❌ No API URL found in configuration")
        sys.exit(1)
    
    # Get output settings
    output = config.get('output', {})
    csv_file = output.get('csv_file', 'output.csv')
    fields = output.get('fields', ['ID', 'SKU'])
    
    # Fetch data
    print(f"🌐 Fetching data from: {api_url}")
    data = fetch_data(api_url)
    
    # Save to CSV
    save_to_csv(data, csv_file, fields)
    
    print("✅ Process completed successfully!")

if __name__ == "__main__":
    main()