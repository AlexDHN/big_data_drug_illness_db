import re


def loading_hpo():
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
