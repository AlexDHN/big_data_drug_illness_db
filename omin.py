from csv import reader
import re


def au(disease_label):  # #614920 PEROXISOME BIOGENESIS DISORDER 14B; PEX14B into Peroxisome biogenesis disorder 14b; pex14b
    acc = re.search(r"(#|%)*[0-9]{3,6} ", disease_label)
    if acc:
        disease_label = disease_label[acc.regs[0][1]:]
    disease_label = disease_label[0].upper() + disease_label[1:].lower()
    return str(disease_label)


def loading_omim(path):
    print("Start loading Omim\n")
    tuple_omim = []  # Dictionary in which there will be illness with signs and symptoms
    aux = 0
    acc = ""
    with open(path + '/OMIM/omim.txt') as f:
        for line in f:
            if line == "*FIELD* TI\n":
                aux = 1  # state in which we know that we will read the name of the illness
            elif line == "*FIELD* CS\n":
                aux = 2  # state in which we know that we will read signs and symptoms of the illness
            elif aux == 1:
                illness = line
                illness = au(re.sub(r"\n", "", illness))
                aux = 0
            elif aux == 2:
                if line[:7] == "*FIELD*":  # We have seen all the signs and symptoms
                    aux = 0
                    acc = re.sub(r"\n", "", acc)
                    tuple_omim.append((illness, acc.lower()))
                    acc = ""
                else:
                    acc += line  # We store all the signs and symptoms
    print("End loading Omim\n")
    return tuple_omim


"""
with open('data/OMIM/omim_onto.csv') as csvDataFile:

    # read file as csv file
    csvReader = reader(csvDataFile)

    # for every row, print the row
    for row in csvReader:
        print(row)
"""

"""
def loading_omim():
    print("Start loading Omim\n")
    dico_omim = {}  # Dictionary in which there will be illness with signs and symptoms
    aux = 0
    acc = ""
    with open('data/OMIM/omim.txt') as f:
        for line in f:
            if line == "*FIELD* TI\n":
                aux = 1  # state in which we know that we will read the name of the illness
            elif line == "*FIELD* CS\n":
                aux = 2  # state in which we know that we will read signs and symptoms of the illness
            elif aux == 1:
                illness = line[8:]
                illness = re.sub(r"\n", "", illness)
                aux = 0
            elif aux == 2:
                if line[:7] == "*FIELD*":  # We have seen all the signs and symptoms
                    aux = 0
                    acc = re.sub(r"\n", "", acc)
                    dico_omim[illness] = acc.lower()
                    acc = ""
                else:
                    acc += line  # We store all the signs and symptoms
    print("End loading Omim\n")
    return dico_omim

"""
"""
from pyspark import SparkContext

sc = SparkContext("local", "First App")

# Print the version of SparkContext
print("The version of Spark Context in the PySpark shell is", sc.version)

# Print the Python version of SparkContext
print("The Python version of Spark Context in the PySpark shell is", sc.pythonVer)

# Print the master of SparkContext
print("The master of Spark Context in the PySpark shell is", sc.master)

# Create a fileRDD from file_path
fileRDD = sc.textFile('data/OMIM/omim.txt')
# Filter the fileRDD to select lines with Spark keyword

RDD_flapmap = fileRDD.flatMap(lambda x: x.split("*RECORD*"))

fileRDD_filter = fileRDD.filter(lambda line: '#' in line)

# How many lines are there in fileRDD?
print("The total number of lines with the keyword Spark is", fileRDD_filter.count())

rdd = fileRDD.zipWithIndex()
aux = rdd.filter(lambda phrase, num_ligne: phrase == '*FIELD* TI')

for line in rdd.take(10):
    print(line)
# Print the first four lines of fileRDD

# Create PairRDD Rdd with key value pairs
x = sc.parallelize([("a", 1), ("b", 4), ("b", 5), ("a", 2)])
y = sc.parallelize([("a", 3), ("c", None)])
print(x.subtractByKey(y).collect())  # " [('b', 4), ('b', 5)]"

for line in Rdd_Reduced.take(3):
    print(line)

"""
