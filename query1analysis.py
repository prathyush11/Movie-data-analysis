import json
import matplotlib.pyplot as plt

def load_data(file_path):
    """Load JSON data from file."""
    with open(file_path, 'r') as file:
        data = json.load(file)
    return data

def generate_bar_chart():
    file_path = 'output1.json'
    data = load_data(file_path)

    user_country_input = input("Enter the production country: ").lower()

    country_data = next((item for item in data if (
        item['production_country'].lower() == user_country_input or
        item['iso_3166_1'].lower() == user_country_input
    )), None)

    if country_data is None:
        print(f"No data found for the production country: {user_country_input.capitalize()}")
        return

    genres = country_data['genres']

    genre_names = [genre['genre'] for genre in genres]
    avg_roi_values = [genre['avg_roi'] for genre in genres]
    is_profitable = [genre['is_profitable'] for genre in genres]

    bar_colors = ['green' if profitable else 'red' for profitable in is_profitable]


    fig, ax = plt.subplots(figsize=(12, 8))
    x_indices = range(len(genre_names))
    bars = ax.bar(x_indices, avg_roi_values, color=bar_colors)
    ax.set_xticks(x_indices)
    ax.set_xticklabels(genre_names, rotation=45, ha='right')

    ax.set_xlabel('Genre')
    ax.set_ylabel('Average ROI')
    ax.set_title(f'Average ROI by Genre for {country_data["production_country"].capitalize()}')

    profitable_patch = plt.Line2D([0], [0], color='green', linewidth=10)
    non_profitable_patch = plt.Line2D([0], [0], color='red', linewidth=10)
    ax.legend([profitable_patch, non_profitable_patch], ['Profitable', 'Non-profitable'])

    # Show plot
    plt.tight_layout()
    plt.show()

generate_bar_chart()
