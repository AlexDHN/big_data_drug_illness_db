# coding: utf-8
import re

from pyspark.sql import SparkSession
from drugbank import loading_drugbank
from omin import loading_omim
from hpo import loading_hpo_obo, loading_hpo_annotations
from sider import loading_sider_toxicity_cid, loading_sider_indications_cid
from stitch import loading_stitch_chemical_sources, loading_stitch_br

spark = SparkSession.builder.getOrCreate()


def load_data(path):
    sc = spark.sparkContext
    # sc = SparkContext("local", "Project_DB")
    tuple_drugbank_indication, tuple_drug_bank_toxicity = loading_drugbank(
        path)  # tuples in which there will be drug which indication and # toxicity
    rdd_drug_indication = sc.parallelize(tuple_drugbank_indication, 3)
    rdd_drug_toxicity = sc.parallelize(tuple_drug_bank_toxicity, 3)
    rdd_illness_symptoms = sc.parallelize(loading_omim(path), 3)  # Pair RDD of illness with their symptoms
    rdd_hpo_id_symptoms = sc.parallelize(loading_hpo_obo(path), 3).groupByKey().mapValues(
        list)  # Pair RDD id with associate symptoms
    rdd_hpo_annotations_id_illness = sc.parallelize(loading_hpo_annotations(path), 3).groupByKey().mapValues(
        list)  # Pair RDD id with associate illness
    rdd_indication_sider = sc.parallelize(loading_sider_indications_cid(), 3).distinct().mapValues(lambda
                                                                                                       x: x.lower())  # .groupByKey().mapValues(list)  # Rdd of sider database with sidecompoun_id , list of indication
    rdd_toxicity_sider = sc.parallelize(loading_sider_toxicity_cid(), 3).distinct().mapValues(
        lambda x: x.lower())  # Rdd of sider database with sidecompoun_id, list of toxicity
    rdd_stitch_CID_to_ATC = sc.parallelize(loading_stitch_chemical_sources(path), 3)
    rdd_stitch_ATC_to_drug = sc.parallelize(loading_stitch_br(path), 3)
    df_stitch_CID_to_ATC = spark.createDataFrame(rdd_stitch_CID_to_ATC, schema='cid_1 string, atc string')
    df_stitch_CID_to_ATC_aux = spark.createDataFrame(rdd_stitch_CID_to_ATC.map(lambda tup: (tup[0][:12], tup[1])),
                                                     schema='cid_1 string, atc string')
    df_stitch_ATC_to_drug = spark.createDataFrame(rdd_stitch_ATC_to_drug, schema='atc string, drug string')

    return rdd_drug_indication, rdd_drug_toxicity, rdd_illness_symptoms, rdd_hpo_id_symptoms, rdd_hpo_annotations_id_illness, rdd_indication_sider, rdd_toxicity_sider, df_stitch_CID_to_ATC, df_stitch_CID_to_ATC_aux, df_stitch_ATC_to_drug


# rdd_drug_indication, rdd_drug_toxicity, rdd_illness_symptoms, rdd_hpo_id_symptoms, rdd_hpo_annotations_id_illness, rdd_indication_sider, rdd_toxicity_sider, df_stitch_CID_to_ATC, df_stitch_CID_to_ATC_aux, df_stitch_ATC_to_drug = load_data("C:/Users/Alexandre/Documents/Cour_Telecom/GMD/gmd_dhenin_jurczack_leana/data")


def show_rdd(rdd):  # Just to see what there is in rdd
    a = rdd.take(20)
    for i in a:
        print(i)


def find_etoile(word):  # In a symptom like like head* return position of * ie 4
    n = len(word)
    for i in range(n):
        if word[i] == '*':
            return i
    return -1


def adapt_regex(symptom):  # return the regex corresponding to head* * for instance
    regex_word = r'\b[^\s]*\b'
    words = re.split(r"\s", symptom)
    regex = ""
    l = len(words)
    for i in range(l):
        aux = find_etoile(words[i])
        if words[i] == '*':
            regex += regex_word
            if i != l - 1:
                regex += " "
        elif aux == -1:
            regex += words[i]
            if i != l - 1:
                regex += " "
        else:
            regex += rf"\b{words[i][0:aux]}[^\s]*{words[i][aux + 1:]}\b"
            if i != l - 1:
                regex += " "
    return rf"{regex}"


def get_illness_and_synonyms_from_hpo(symptom, rdd_hpo_id_symptoms,
                                      rdd_hpo_annotations_id_illness):  # for a symptoms we give corresponding illness from hpo and all synonyms of this symptom
    #  aux = rdd_hpo_id_symptoms.filter(lambda tup: [match for match in tup[1] if re.search(adapt_regex(symptom), match)] != [])  # Mean that we take the id and synonyms if we find one corresponding in the list of synonyms
    aux = rdd_hpo_id_symptoms.filter(lambda tup: [match for match in tup[1] if re.search(rf'\b{symptom}\b', match)] != [])  # Mean that we take the id and synonyms if we find one corresponding in the list of synonyms
    all_id = aux.keys().collect()  # We get all the id of illness with symptoms associate
    symptoms = aux.values().collect()  # list of list of all synonyms of the symptoms
    #  symptoms = [x for lst in symptoms for x in lst]
    all_symptoms = [symptom]  # just reformat data
    for lst in symptoms:
        for element in lst:
            if element not in all_symptoms:
                all_symptoms.append(element)
    acc = rdd_hpo_annotations_id_illness.filter(lambda tup: tup[0] in all_id).values().collect()
    illness = []
    for lst in acc:
        for element in lst:
            if element not in illness:
                illness.append(element)
    return illness, all_symptoms


def get_drug_from_drugbank(symptom, rdd_drug_toxicity):  # for a given symptoms we give drug with this toxicity
    return rdd_drug_toxicity.filter(lambda tup: re.search(rf'\b{symptom}\b', tup[1])).keys().collect()


def get_indication_from_drugbank(symptom,
                                 rdd_drug_indication):  # # for a given symptom we give drug indicate to this symptom
    return rdd_drug_indication.filter(lambda tup: re.search(rf'\b{symptom}\b', tup[1])).keys().collect()


def get_illness_from_omim(symptom, rdd_illness_symptoms):  # for a symptoms we give corresponding illness from omim
    return rdd_illness_symptoms.filter(lambda tup: re.search(rf'\b{symptom}\b', tup[1])).keys().collect()


def get_drug_from_sider(symptom, rdd_toxicity_sider, df_stitch_CID_to_ATC,
                        df_stitch_ATC_to_drug):  # for a given symptoms we give drug with this toxicity
    #   df_sider = spark.createDataFrame(rdd_toxicity_sider.filter(lambda tup: re.search(rf'\b{symptom}\b', tup[1])), schema='cid_0 string, sy string').distinct()
    df_sider = spark.createDataFrame(rdd_toxicity_sider.filter(lambda tup: symptom == tup[1]),
                                     schema='cid_0 string, sy string').distinct()
    #   df_sider.show()
    aux = df_sider.join(df_stitch_CID_to_ATC, df_sider.cid_0 == df_stitch_CID_to_ATC.cid_1, 'inner')
    #   aux.show()
    return df_stitch_ATC_to_drug.join(aux, aux.atc == df_stitch_ATC_to_drug.atc, "inner").select(
        "drug").distinct().select('drug').rdd.flatMap(lambda x: x).collect()


def get_indication_from_sider(symptom, rdd_indication_sider, df_stitch_CID_to_ATC_aux,
                              df_stitch_ATC_to_drug):  # for a given symptom we give drug indicate to this symptom
    #  df_sider = spark.createDataFrame(rdd_indication_sider.filter(lambda tup: re.search(rf'\b{symptom}\b', tup[1])), schema='cid_0 string, sy string').distinct()
    df_sider = spark.createDataFrame(rdd_indication_sider.filter(lambda tup: symptom == tup[1]),
                                     schema='cid_0 string, sy string').distinct()
    #  df_sider.show()
    aux = df_sider.join(df_stitch_CID_to_ATC_aux, df_sider.cid_0 == df_stitch_CID_to_ATC_aux.cid_1, 'inner')
    #  aux.show()
    return df_stitch_ATC_to_drug.join(aux, aux.atc == df_stitch_ATC_to_drug.atc, "inner").select(
        "drug").distinct().select('drug').rdd.flatMap(lambda x: x).collect()


def get_result_by_rdd(sympt):
    hpo_illness, all_symptoms = get_illness_and_synonyms_from_hpo(sympt.lower())
    # hpo_illness = [x for lst in b for x in lst]  # just reformat data
    aux_omim_illness = []
    aux_drugbank_drug = []
    aux_sider_drug = []
    for symptom in all_symptoms:  # For all symptoms we are looking for correspondence
        aux_omim_illness.append(get_illness_from_omim(symptom))
        aux_drugbank_drug.append(get_drug_from_drugbank(symptom))
        aux_sider_drug.append(get_drug_from_sider(symptom))
    # Here we just reformat data
    omim_illness = []
    drugbank_drug = []
    sider_drug = []
    for element in aux_omim_illness:
        if element not in omim_illness:
            omim_illness.append(element)
    omim_illness = [x for lst in omim_illness for x in lst]
    for element in aux_drugbank_drug:
        if element not in drugbank_drug:
            drugbank_drug.append(element)
    drugbank_drug = [x for lst in drugbank_drug for x in lst]
    for element in aux_sider_drug:
        if element not in sider_drug:
            sider_drug.append(element)
    sider_drug = [x for lst in sider_drug for x in lst]
    return all_symptoms, hpo_illness, omim_illness, drugbank_drug, sider_drug


def get_res(sympt, rdd_drug_indication, rdd_drug_toxicity, rdd_illness_symptoms, rdd_hpo_id_symptoms,
            rdd_hpo_annotations_id_illness, rdd_indication_sider, rdd_toxicity_sider, df_stitch_CID_to_ATC,
            df_stitch_CID_to_ATC_aux, df_stitch_ATC_to_drug):
    hpo_illness, all_symptoms = get_illness_and_synonyms_from_hpo(sympt.lower(), rdd_hpo_id_symptoms,
                                                                  rdd_hpo_annotations_id_illness)
    # hpo_illness = [x for lst in b for x in lst]  # just reformat data
    omim_illness = []
    drugbank_drug = []
    sider_drug = []
    drugbank_indication = []
    sider_indication = []
    for symptom in all_symptoms:  # For all symptoms we are looking for correspondence
        omim_illness.extend(get_illness_from_omim(symptom, rdd_illness_symptoms))
        drugbank_drug.extend(get_drug_from_drugbank(symptom, rdd_drug_toxicity))
        sider_drug.extend(get_drug_from_sider(symptom, rdd_toxicity_sider, df_stitch_CID_to_ATC, df_stitch_ATC_to_drug))
        drugbank_indication.extend(get_indication_from_drugbank(symptom, rdd_drug_indication))
        sider_indication.extend(
            get_indication_from_sider(symptom, rdd_indication_sider, df_stitch_CID_to_ATC_aux, df_stitch_ATC_to_drug))
    omim_illness = list(set(omim_illness))
    drugbank_drug = list(set(drugbank_drug))
    sider_drug = list(set(sider_drug))
    drugbank_indication = list(set(drugbank_indication))
    sider_indication = list(set(sider_indication))
    return all_symptoms, hpo_illness, omim_illness, drugbank_drug, sider_drug, drugbank_indication, sider_indication


def intersection(lst1, lst2):  # Intersection of two list
    return list(set(lst1) & set(lst2))


def union(lst1, lst2):  # Union of two list
    print(list(set(lst1) | set(lst2)))
    return list(set(lst1) | set(lst2))


def fusion(res1, condi, res2):
    print("FUSIONNNNNNN")
    print(condi)
    print(res1)
    print(res2)
    if condi == "AND":
        return intersection(res1[1] + res1[2], res2[1] + res2[2]), intersection(res1[3] + res1[4],
                                                                                res2[3] + res2[4]), intersection(
            res1[5] + res1[6], res2[5] + res2[6])
    else:
        return union(res1[1] + res1[2], res2[1] + res2[2]), union(res1[3] + res1[4], res2[3] + res2[4]), union(
            res1[5] + res1[6], res2[5] + res2[6])


def get_and_or(symptom, rdd_drug_indication, rdd_drug_toxicity, rdd_illness_symptoms, rdd_hpo_id_symptoms,
               rdd_hpo_annotations_id_illness, rdd_indication_sider, rdd_toxicity_sider, df_stitch_CID_to_ATC,
               df_stitch_CID_to_ATC_aux, df_stitch_ATC_to_drug):
    r0 = r"(AND|OR)"  # Regex that match AND in a AND b
    r1 = r"^[^(AND|OR)]*"  # Regex that match a in a AND b
    r2 = r"(?<= AND ).*"  # Regex that match b in a AND b
    r3 = r"(?<= OR ).*"  # Regex that match b in a OR b
    condi = re.search(r0, symptom)
    if condi:
        if condi.group() == "AND":
            return fusion(get_res(re.search(r1, symptom).group()[:-1], rdd_drug_indication, rdd_drug_toxicity,
                                  rdd_illness_symptoms, rdd_hpo_id_symptoms, rdd_hpo_annotations_id_illness,
                                  rdd_indication_sider, rdd_toxicity_sider, df_stitch_CID_to_ATC,
                                  df_stitch_CID_to_ATC_aux, df_stitch_ATC_to_drug), condi.group(),
                          get_and_or(re.search(r2, symptom).group(), rdd_drug_indication, rdd_drug_toxicity,
                                     rdd_illness_symptoms, rdd_hpo_id_symptoms, rdd_hpo_annotations_id_illness,
                                     rdd_indication_sider, rdd_toxicity_sider, df_stitch_CID_to_ATC,
                                     df_stitch_CID_to_ATC_aux, df_stitch_ATC_to_drug))
        else:
            return fusion(get_res(re.search(r1, symptom).group()[:-1], rdd_drug_indication, rdd_drug_toxicity,
                                  rdd_illness_symptoms, rdd_hpo_id_symptoms, rdd_hpo_annotations_id_illness,
                                  rdd_indication_sider, rdd_toxicity_sider, df_stitch_CID_to_ATC,
                                  df_stitch_CID_to_ATC_aux,
                                  df_stitch_ATC_to_drug), condi.group(),
                          get_and_or(re.search(r3, symptom).group(), rdd_drug_indication, rdd_drug_toxicity,
                                     rdd_illness_symptoms, rdd_hpo_id_symptoms, rdd_hpo_annotations_id_illness,
                                     rdd_indication_sider, rdd_toxicity_sider, df_stitch_CID_to_ATC,
                                     df_stitch_CID_to_ATC_aux,
                                     df_stitch_ATC_to_drug))
    else:
        return get_res(symptom, rdd_drug_indication, rdd_drug_toxicity, rdd_illness_symptoms, rdd_hpo_id_symptoms,
                       rdd_hpo_annotations_id_illness, rdd_indication_sider, rdd_toxicity_sider, df_stitch_CID_to_ATC,
                       df_stitch_CID_to_ATC_aux, df_stitch_ATC_to_drug)


#rdd_drug_indication, rdd_drug_toxicity, rdd_illness_symptoms, rdd_hpo_id_symptoms, rdd_hpo_annotations_id_illness, rdd_indication_sider, rdd_toxicity_sider, df_stitch_CID_to_ATC, df_stitch_CID_to_ATC_aux, df_stitch_ATC_to_drug = load_data("C:/Users/Alexandre/Documents/Cour_Telecom/GMD/gmd_dhenin_jurczack_leana/data")

#a, b = get_illness_and_synonyms_from_hpo("hypo*",rdd_hpo_id_symptoms, rdd_hpo_annotations_id_illness)

# all_symptoms, a_hpo_illness, a_omim_illness, a_drugbank_toxicity, a_sider_toxicity, a_drugbank_indication, a_sider_indication = get_res(("Headache")
# rep = all_symptoms, a_hpo_illness, a_omim_illness, a_drugbank_toxicity, a_sider_toxicity, a_drugbank_indication, a_sider_indication
# a, all = get_illness_and_synonyms_from_hpo("Headache")

"""
re.search(rf'\b{symptom}\b', p)

avant = ""
apres = "lat"
regex = rf"\b{avant}[^\s]*{apres}\b"
regex_word = r'\b[^\s]*\b'
aux = regex + " " + regex_word
print(aux)
test = "bolat chocolat au carlat"
aa = rf'{aux}'
condi = re.search(rf'{aux}', test)
if condi:
    print(condi.group())

w = 'et*lig'

test = "headache"
test_oui = "tout ce headache je pense c'est que bang et ouai"
condi = re.search(adapt_regex(test), test_oui)
if condi:
    print(condi.group())

def aux(symptom):
    r0 = r"(AND|OR)"  # Regex that match AND in a AND b
    r1 = r"^[^(AND|OR)]*"  # Regex that match a in a AND b
    r2 = r"(?<= AND ).*"  # Regex that match b in a AND b
    r3 = r"(?<= OR ).*"  # Regex that match b in a OR b
    condi = re.search(r0, symptom)
    if condi:
        if condi.group() == "AND":
            return fusion(([], ["1","2"], [], [], [], ["5","8"], []), condi.group(), aux(re.search(r2, symptom).group()))
        else:
            return fusion(([], ["1","2"], [], [], ["9"], ["5","8"], []), condi.group(), aux(re.search(r3, symptom).group()))
        # return fusion(get_res(re.search(r1, symptom).group()[:-1], rdd_drug_indication, rdd_drug_toxicity, rdd_illness_symptoms, rdd_hpo_id_symptoms, rdd_hpo_annotations_id_illness, rdd_indication_sider, rdd_toxicity_sider, df_stitch_CID_to_ATC, df_stitch_CID_to_ATC_aux, df_stitch_ATC_to_drug), condi.group(), aux(re.search(r2, symptom).group(), rdd_drug_indication, rdd_drug_toxicity, rdd_illness_symptoms, rdd_hpo_id_symptoms, rdd_hpo_annotations_id_illness, rdd_indication_sider, rdd_toxicity_sider, df_stitch_CID_to_ATC, df_stitch_CID_to_ATC_aux, df_stitch_ATC_to_drug))
    else:
        print(symptom)
        return [], [], [], [], [], [], []
        # return get_res(symptom, rdd_drug_indication, rdd_drug_toxicity, rdd_illness_symptoms, rdd_hpo_id_symptoms, rdd_hpo_annotations_id_illness, rdd_indication_sider, rdd_toxicity_sider, df_stitch_CID_to_ATC, df_stitch_CID_to_ATC_aux, df_stitch_ATC_to_drug)


a = "small penis OR long penis"
a = aux(a)

aaa = aux("small penis OR long penis", rdd_drug_indication, rdd_drug_toxicity, rdd_illness_symptoms,
          rdd_hpo_id_symptoms, rdd_hpo_annotations_id_illness, rdd_indication_sider, rdd_toxicity_sider,
          df_stitch_CID_to_ATC, df_stitch_CID_to_ATC_aux, df_stitch_ATC_to_drug)


def aux(symptom):
    r0 = r"(AND|OR)"  # Regex that match AND in a AND b
    r1 = r"^[^(AND|OR)]*"  # Regex that match a in a AND b
    r2 = r"(?<= AND ).*"  # Regex that match b in a AND b
    r3 = r"(?<= OR ).*"  # Regex that match b in a OR b
    condi = re.search(r0, symptom)
    if condi:
        if condi.group() == "AND":
            return fusion(([], [], [], [], [], [], []), condi.group(), aux(re.search(r2, symptom).group()))
        else:
            return fusion(([], [], [], [], [], [], []), condi.group(), aux(re.search(r3, symptom).group()))
        # return fusion(get_res(re.search(r1, symptom).group()[:-1], rdd_drug_indication, rdd_drug_toxicity, rdd_illness_symptoms, rdd_hpo_id_symptoms, rdd_hpo_annotations_id_illness, rdd_indication_sider, rdd_toxicity_sider, df_stitch_CID_to_ATC, df_stitch_CID_to_ATC_aux, df_stitch_ATC_to_drug), condi.group(), aux(re.search(r2, symptom).group(), rdd_drug_indication, rdd_drug_toxicity, rdd_illness_symptoms, rdd_hpo_id_symptoms, rdd_hpo_annotations_id_illness, rdd_indication_sider, rdd_toxicity_sider, df_stitch_CID_to_ATC, df_stitch_CID_to_ATC_aux, df_stitch_ATC_to_drug))
    else:
        print(symptom)
        return [], [], [], [], [], [], []
        # return get_res(symptom, rdd_drug_indication, rdd_drug_toxicity, rdd_illness_symptoms, rdd_hpo_id_symptoms, rdd_hpo_annotations_id_illness, rdd_indication_sider, rdd_toxicity_sider, df_stitch_CID_to_ATC, df_stitch_CID_to_ATC_aux, df_stitch_ATC_to_drug)

a = aux("small penis OR long penis")
a = "small penis OR long penis"
r0 = r"(AND|OR)"  # Regex that match AND in a AND b
r1 = r"^[^(AND|OR)]*"  # Regex that match a in a AND b
r2 = r"(?<= AND ).*"  # Regex that match b in a AND b
r3 = r"(?<= OR ).*"  # Regex that match b in a OR b


sympto = "Brain tumour"
sympto = 'gastrointestinal pain'
sympto = "scrotum"

sympto1 = "myocardial infarction"
sympto1 = 'shawl scrotum'
sympto2 = 'heparin-induced thrombocytopenia'
symp_et = "shawl scrotum OU heparin-induced thrombocytopenia"

aa = aux(symp_et)
a = get_res(sympto1)
b = get_res(sympto2)


df_sider = spark.createDataFrame(rdd_toxicity_sider.filter(lambda tup: sympto == tup[1]), schema='cid_0 string, sy string').distinct()
aux = df_sider.join(df_stitch_CID_to_ATC, df_sider.cid_0 == df_stitch_CID_to_ATC.cid_1, 'inner')
aa = df_stitch_ATC_to_drug.join(aux, aux.atc == df_stitch_ATC_to_drug.atc, "inner").select(
    "drug").distinct().select('drug').rdd.flatMap(lambda x: x).collect()

def rdd_symptoms_rec(acc1, acc2, acc3, f1, f2, f3, symptoms):
    print(symptoms)
    if len(symptoms) == 0:
        return acc1.distinct().collect(), acc2.distinct().collect(), acc3.distinct().collect(),
    else:
        print(symptoms[0])
        return rdd_symptoms_rec(acc1.union(f1(symptoms[0])), acc2.union(f2(symptoms[0])), acc3.union(f3(symptoms[0])), f1, f2, f3, symptoms[1:])

a1 = rdd_symptoms_rec(sc.parallelize(""), sc.parallelize(""), sc.parallelize(""), get_drug_from_drugbank, get_drug_from_sider, get_illness_from_omim, all_symptoms)
"""

# ans = get_drug_from_sider("arm")
"""
symptoms = ["palpitations", "sinusitis"]

a = get_drug_from_sider(symptom)
b = get_drug_from_drugbank(symptom)
d = get_illness_from_omim(symptom)

aa = sc.parallelize(a).distinct().collect()
bb = sc.parallelize(b).distinct().collect()
cc = sc.parallelize(c).distinct().collect()
dd = sc.parallelize(d).distinct().collect()

aaa = sc.parallelize(a).distinct()
bbb = sc.parallelize(b).distinct()
ccc = sc.parallelize(c).distinct()
ddd = sc.parallelize(d).distinct()

aaaa = aaa.union(bbb).distinct().collect()
cccc = ccc.union(ddd).distinct().collect()

for symptom in symptoms:
    df_sider = spark.createDataFrame(rdd_toxicity_sider.filter(lambda tup: re.search(rf'\b{symptom}\b', tup[1])),
                                     schema='cid_0 string, sy string').distinct()
    aux = df_sider.join(df_stitch_CID_to_ATC, df_sider.cid_0 == df_stitch_CID_to_ATC.cid_1, 'inner')
    rep = df_stitch_ATC_to_drug.join(aux, aux.atc == df_stitch_ATC_to_drug.atc, "inner").select(
        "drug").distinct().select('drug')  # .rdd.flatMap(lambda x: x).collect()

    att = att.union(rep)

ans = att.distinct().rdd.flatMap(lambda x: x).collect()

att = 0
att = spark.createDataFrame([], schema="drug string")
symptom = "sinusitis"
df_sider = spark.createDataFrame(rdd_toxicity_sider.filter(lambda tup: re.search(rf'\b{symptom}\b', tup[1])),
                                     schema='cid_0 string, sy string').distinct()
aux = df_sider.join(df_stitch_CID_to_ATC, df_sider.cid_0 == df_stitch_CID_to_ATC.cid_1, 'inner')
rep = df_stitch_ATC_to_drug.join(aux, aux.atc == df_stitch_ATC_to_drug.atc, "inner").select(
        "drug").distinct().select('drug')  # .rdd.flatMap(lambda x: x).collect()

att = att.union(rep).rdd.flatMap(lambda x: x).collect()


# 459

show_rdd(rdd_illness_symptoms)

show_rdd(rdd_toxicity_sider)

for i in answer[2]:
    print(i in rep)

acc = rdd_hpo_id_symptoms.values().collect()
for lst in acc:
    for line in lst:
        if "bleeding" in line:
            print(line)


def aux(lst, word):
    for i in lst:
        if word in i:
            return True
    return False


aux = rdd_hpo_id_symptoms.filter(lambda tup: [match for match in tup[1] if "bleeding" in match] != [])
all_id = aux.keys().collect()  # We get all the id of illness with symptoms associate
all_symptoms = aux.values().collect()  # list of list of all synonyms of the symptoms

test = ["Je pense donc je suis", "Ceci est un essaie"," POurquoi pas"]
a = lambda lst: x in lambda line:

matches = [match for match in test if "Jehb" in match] != []

sympt = "coma"
aux = loading_sider_toxicity_cid()
rdd_toxicity_sider = sc.parallelize(aux, 3)
acc = rdd_toxicity_sider.distinct().mapValues(lambda x: x.lower()) # Rdd of sider database with sidecompoun_id, list of toxicity
acc = acc.filter(lambda tup: re.search(rf'\b{sympt}\b', tup[1]))
syno_sider = acc.values().distinct().collect()
aux = acc.collect()
for i in aux:
    print(i)
    
    
sympt = "coma"
aux = loading_sider_toxicity_cid()
rdd_toxicity_sider = sc.parallelize(aux, 3)
acc = rdd_toxicity_sider.distinct().mapValues(
    lambda x: x.lower())  # Rdd of sider database with sidecompoun_id, list of toxicity
acc = acc.filter(lambda tup: re.search(rf'\b{sympt}\b', tup[1]))
syno_sider = acc.values().distinct().collect()
aux = acc.collect()
for i in aux:
    print(i)


"""
