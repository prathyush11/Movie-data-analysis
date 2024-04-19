import json
import matplotlib.pyplot as plt


with open('output2.json', 'r') as file:
    data = json.load(file)


keywords = [item["_id"] for item in data]
scores = [item["score"] for item in data]
counts = [item["count"] for item in data]


colors = plt.cm.viridis(range(len(keywords)))

# Plotting
plt.figure(figsize=(12, 10))
bars = plt.barh(keywords, scores, color=colors)
plt.xlabel('Score')
plt.ylabel('Keyword')
plt.title('Scores for Keywords')
plt.gca().invert_yaxis()  


for bar, count in zip(bars, counts):
    plt.text(bar.get_width() - 0.5, bar.get_y() + bar.get_height() / 2, f'{count} ({bar.get_width():.2f})',
             va='center', ha='right', color='white')


plt.xticks(range(0, int(max(scores)) + 1, 2))

plt.margins(y=0.01)
plt.tight_layout()

plt.show()
