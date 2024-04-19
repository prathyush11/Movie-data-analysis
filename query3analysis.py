import json
import matplotlib.pyplot as plt


json_file_path = 'output3.json'


with open(json_file_path, 'r') as file:
    data = json.load(file)


decades = sorted({entry['_id']['decade'] for entry in data})
genres = sorted({entry['_id']['genre_name'] for entry in data})


num_decades = len(decades)
num_genres = len(genres)
genre_scores = {genre: {decade: 0 for decade in decades} for genre in genres}


for entry in data:
    genre = entry['_id']['genre_name']
    decade = entry['_id']['decade']
    genre_scores[genre][decade] = entry['popularity_score']

# Initialize plot
fig, ax = plt.subplots(figsize=(12, 8))


bar_width = 0.2
index = list(range(num_decades))


for i, genre in enumerate(genres):
    genre_scores_list = [genre_scores[genre][decade] for decade in decades]
    ax.bar([pos + i * bar_width for pos in index], genre_scores_list, width=bar_width, label=genre)


ax.set_xlabel('Decade')
ax.set_ylabel('Popularity Score')
ax.set_title('Popularity Score by Genre and Decade')
ax.set_xticks([pos + (num_genres - 1) * bar_width / 2 for pos in index])
ax.set_xticklabels(decades)
ax.legend()


plt.xticks(rotation=45)

# Show plot
plt.tight_layout()
plt.show()
