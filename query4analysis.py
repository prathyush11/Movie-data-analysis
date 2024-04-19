import json
import matplotlib.pyplot as plt

def generate_pie_chart(company_name, data):
    company_name_lower = company_name.lower()

    company_data = next((entry for entry in data if entry['production_company']['name'].lower() == company_name_lower), None)

    if company_data:
        seasons = [season['season'] for season in company_data['seasons']]
        success_rates = [season['likelihood_of_success'] for season in company_data['seasons']]

        # Create a pie chart
        plt.figure(figsize=(8, 8))
        plt.pie(success_rates, labels=seasons, autopct='%1.1f%%', startangle=140)
        plt.title(f"Success Rate by Season for {company_data['production_company']['name']}")
        plt.axis('equal')
        plt.show()
    else:
        print(f"Production company '{company_name}' not found in the data.")

# Load JSON data from file
file_path = 'output4.json' 
with open(file_path, 'r') as file:
    json_data = json.load(file)

company_name_input = input("Enter a production company name: ")
generate_pie_chart(company_name_input, json_data)
