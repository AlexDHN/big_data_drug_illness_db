# coding: utf-8

from pyspark import SparkContext
from drugbank import loading_drugbank
from omin import loading_omim
from hpo import loading_hpo

sc = SparkContext("local", "Project_DB")
tuple_drugbank_indication, tuple_drug_bank_toxicity = loading_drugbank()  # tuples in which there will be drug which indication and # toxicity
tuple_omim = loading_omim()  # tuple in which there will be illness with signs and symptoms
tuple_hpo = loading_hpo()    # tuple contenting id with symptoms and synonyms (not grouping by yet)

rdd_drug_indication = sc.parallelize(tuple_drugbank_indication, 2)
rdd_drug_toxicity = sc.parallelize(tuple_drug_bank_toxicity, 2)
rdd_illness_symptoms = sc.parallelize(tuple_omim, 2)
rdd_hpo_id_symptoms = sc.parallelize(tuple_hpo, 2).groupByKey().mapValues(list)  # Here we group all the symptoms with the same id


def get_by_symptoms(rdd, symptoms):
    return rdd.filter(lambda tup: symptoms in tup[1]).collect()


aux = get_by_symptoms(rdd_drug_toxicity, "psychomotor")
for i in aux:
    print(i[0])

aux = get_by_symptoms(rdd_illness_symptoms, "psychomotor")
for i in aux:
    print(i[0])

"""
fileRDD_filter = rdd_drug_toxicity.filter(lambda tup: 'anaphylactic reactions' in tup[1])
print(fileRDD_filter.collect())

for line in fileRDD_filter.take(1):
    print(line)
"""
