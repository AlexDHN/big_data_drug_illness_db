import re
import sqlite3 as sql


def aux(disease_label):  # #614920 PEROXISOME BIOGENESIS DISORDER 14B; PEX14B into Peroxisome biogenesis disorder 14b; pex14b
    acc = re.search(r"(#|%)*[0-9]{3,6} ", disease_label)
    if acc:
        disease_label = disease_label[acc.regs[0][1]:]
    disease_label = disease_label[0].upper() + disease_label[1:].lower()
    return disease_label


def loading_hpo_annotations(path):
    con = sql.connect(path + '/HPO/hpo_annotations.sqlite')  # we are now connected to the database
    cur = con.cursor()
    cur.execute('SELECT sign_id,disease_label FROM phenotype_annotation')
    tuple_hpo_annotations = cur.fetchall()
    n = len(tuple_hpo_annotations)
    for i in range(n):
        tuple_hpo_annotations[i] = tuple_hpo_annotations[i][0], aux(tuple_hpo_annotations[i][1])
    cur.close()
    con.close()
    return tuple_hpo_annotations


def loading_hpo_obo(path):
    print("Start loading HPO\n")
    tuple_hpo = []  # Tuple in which there will be id with symptoms and synonyms
    id_syno = ""
    aux = []
    with open(path + '/HPO/hpo.obo') as f:
        for line in f:  # When there is [:-1] it's for withdrawing the \n
            if line[:8] == "synonym:":
                tuple_hpo.append((id_syno, re.search(r"\".*\"", line).group()[1:-1].lower()))
            elif line == "[Term]":
                synonyms_list = []
            elif line[:3] == "id:":
                id_syno = line[4:][:-1]
            elif line[:4] == "name":
                tuple_hpo.append((id_syno, line[6:][:-1].lower()))
    print("End loading HPO\n")
    return tuple_hpo
