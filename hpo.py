import re
import sqlite3 as sql


def loading_hpo_annotations():
    con = sql.connect('data/HPO/hpo_annotations.sqlite')  # we are now connected to the database
    cur = con.cursor()
    cur.execute('SELECT sign_id,disease_label FROM phenotype_annotation')
    tuple_hpo_annotations = cur.fetchall()
    cur.close()
    con.close()
    return tuple_hpo_annotations


def loading_hpo_obo():
    print("Start loading HPO\n")
    tuple_hpo = []  # Tuple in which there will be id with symptoms and synonyms
    id_syno = ""
    aux = []
    with open('data/HPO/hpo.obo') as f:
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
