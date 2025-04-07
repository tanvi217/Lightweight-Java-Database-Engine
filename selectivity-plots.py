import matplotlib.pyplot as plt
import csv

selectivity, scan_time, index_time = [], [], []
with open('test_results.csv', 'r') as f:
    reader = csv.reader(f)
    for row in reader:
        selectivity.append(float(row[0]))
        scan_time.append(float(row[1]))
        index_time.append(float(row[2]))
        print(f"Selectivity: {row[0]}, Scan Time: {row[1]}, Index Time: {row[2]}")

ratio = [s / i for s, i in zip(scan_time, index_time)]

plt.figure(figsize=(10, 6))
plt.plot(selectivity, scan_time, label='Sequential Scan Time (ms)', marker='o', color='blue')
plt.plot(selectivity, index_time, label='Index Range Query Time (ms)', marker='o', color='green')
plt.xlabel('Selectivity')
plt.ylabel('Execution Time (ms)')
plt.title('Movie title Search Performance Comparison: Scan vs. Index Range Query')
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.savefig('Movie title Search Performance Comparison.png')
plt.show()

plt.figure(figsize=(10, 6))
plt.plot(selectivity, ratio, label='Ratio (Scan/Index)', marker='o', color='red')
plt.xlabel('Selectivity')
plt.ylabel('Ratio (Scan Time / Index Time)')
plt.title('Movie title Performance Ratio: Scan vs. Index Range Query')
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.savefig('Movie title Search Scan vs Index Ratio.png')
plt.show()