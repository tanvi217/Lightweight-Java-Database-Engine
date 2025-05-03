import csv

with open('C:/Users/Tanvi/Desktop/title.basics.tsv', 'r', encoding='utf-8', errors='replace', newline='') as infile, \
     open('C:/Users/Tanvi/Desktop/movies_pre.tsv', 'w', encoding='utf-8', newline='') as outfile:
    reader = csv.DictReader(infile, delimiter='\t')
    writer = csv.writer(outfile, delimiter='\t')
    writer.writerow(['movieid','title'])
    for row in reader:
        tconst = row['tconst']
        if len(tconst) != 9:
            continue
        title = row['primaryTitle'][:30]
        writer.writerow([tconst, title])

with open('C:/Users/Tanvi/Desktop/title.principals.tsv', 'r', encoding='utf-8', errors='replace', newline='') as infile, \
     open('C:/Users/Tanvi/Desktop/workedon_pre.tsv', 'w', encoding='utf-8', newline='') as outfile:
    reader = csv.DictReader(infile, delimiter='\t')
    writer = csv.writer(outfile, delimiter='\t')
    writer.writerow(['movieid','personid','category'])
    for row in reader:
        tconst = row['tconst']
        if len(tconst) != 9:
            continue
        nconst = row['nconst']
        writer.writerow([tconst, nconst, row['category']])

with open('C:/Users/Tanvi/Desktop/name.basics.tsv', 'r', encoding='utf-8', errors='replace', newline='') as infile, \
     open('C:/Users/Tanvi/Desktop/people_pre.tsv', 'w', encoding='utf-8', newline='') as outfile:
    reader = csv.DictReader(infile, delimiter='\t')
    writer = csv.writer(outfile, delimiter='\t')
    writer.writerow(['personid','name'])
    for row in reader:
        nconst = row['nconst']
        name = row['primaryName'][:105]
        writer.writerow([nconst, name])
