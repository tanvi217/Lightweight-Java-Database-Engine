import matplotlib.pyplot as plt

# sample data for the plot - replace with actual data
selectivities = [5, 10, 15]
measuredIOs = [100, 105, 110]
estimatedIOs = [100, 105, 110]

plt.plot(selectivities, measuredIOs, marker='o', label='Measured I/O')
plt.plot(selectivities, estimatedIOs, marker='s', label='Estimated I/O')
plt.xlabel('Selectivity of Movies range predicate (p)')
plt.ylabel('Total I/O operations')
plt.title('Measured vs. Estimated I/O for BNL Plan')
plt.legend()
plt.tight_layout()
plt.show()
