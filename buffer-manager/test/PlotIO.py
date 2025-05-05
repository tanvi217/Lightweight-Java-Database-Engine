import matplotlib.pyplot as plt

# sample data for the plot - replace with actual data for measured IO
selectivities = [2.6972e-5, 2.0435e-5, 2.4893e-5]
# first entry acheived from running ant query-imdb -Dargs="'Wisd' 'Wissenschaft' '200'"
#second entry acheived from running ant query-imdb -Dargs="'Billy Bubbles' 'Billys Nightmare' '200'"
# third entry acheived from running ant query-imdb -Dargs="'Close Encounters' 'Closer' '200'"
# all had line System.out.println("Number of IOs "+ (movies.numIOs+people.numIOs+workedOn.numIOs)); at the bottom of RunIMDbQuery.java UNCOMMENTED
measuredIOs = [11042581, 11042581, 11042581]
estimatedIOs = [1676995, 1676987, 1676994]

plt.plot(selectivities, measuredIOs, marker='o', label='Measured I/O')
plt.plot(selectivities, estimatedIOs, marker='s', label='Estimated I/O')
plt.xlabel('Selectivity of Movies range predicate (p)')
plt.ylabel('Total I/O operations')
plt.title('Measured vs. Estimated I/O for BNL Plan')
plt.legend()
plt.tight_layout()
plt.show()
