import matplotlib.pyplot as plt

# sample data for the plot - replace with actual data for measured IO
selectivities = [2.6972e-5, 2.0435e-5, 2.4893e-5]
measuredIOs = [1676990, 1676991, 1676992]
estimatedIOs = [1676995, 1676987, 1676994]

plt.plot(selectivities, measuredIOs, marker='o', label='Measured I/O')
plt.plot(selectivities, estimatedIOs, marker='s', label='Estimated I/O')
plt.xlabel('Selectivity of Movies range predicate (p)')
plt.ylabel('Total I/O operations')
plt.title('Measured vs. Estimated I/O for BNL Plan')
plt.legend()
plt.tight_layout()
plt.show()
