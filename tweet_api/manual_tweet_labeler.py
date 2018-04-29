import csv

manual_classifier = {
 '#AvengersInfinityWar': 'Entertainment',
 '#BCBF_18': 'Technology',
 '#Blindspot': 'Entertainment',
 '#BrightonSEO': 'Technology',
 '#FlashbackFriday': 'Mood',
 '#FootballShirtFriday': 'Sports',
 '#FridayMotivation': 'Mood',
 '#HIP613': 'Technology',
 '#MakeFilmsIrish': 'Entertainment',
 '#OnceUponATime': 'Mood',
 '#trumpvisit': 'Politics',
 'Amber Rudd': 'Politics',
 'Back to the Future': 'Entertainment',
 'Go Jets': 'Sports',
 'Korea': 'Politics',
 'Marcus Stroman': 'Sports',
 'Steven Gerrard': 'Sports'}

class_map = {'Entertainment': 1, 'Mood': 2, 'Politics': 3,
             'Sports': 4, 'Technology': 5}


def annotate_tweets(manual_classifier, class_map):
    with open('../trend_classifier/labeled_markers.csv', 'w', encoding='utf-8') as output_file:
        output_writer = csv.writer(output_file)
        output_writer.writerow(['body', 'topic', 'trend'])
        with open('../trend_classifier/markers.csv', encoding='utf-8') as input_file:
            reader = csv.reader(input_file)
            for row in reader:
                if len(row) < 3:
                    continue
                if row[2] in manual_classifier:
                    new_row = [row[0],
                               class_map[manual_classifier[row[2]]], row[2]]
                    output_writer.writerow(new_row.copy())


if __name__ == "__main__":
    annotate_tweets(manual_classifier, class_map)