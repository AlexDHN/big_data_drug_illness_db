# coding: utf-8
from lxml import etree


def loading_drugbank():  # We are using tuples to use pair RDD in PySpark
    print("Start loading DrugBank\n")
    tree = etree.parse("data/DRUGBANK/drugbank.xml")  # load xml file as ElementTree
    root = tree.getroot()
    tuple_drugbank_indication = []  # List in which there will be tuple drug with indication
    tuple_drug_bank_toxicity = []  # List in which there will be tuple drug with toxicity
    for drug in root:  # we only look at indication and toxicity for a specific drug
        toxicity = drug.find("{http://www.drugbank.ca}toxicity").text
        indication = drug.find("{http://www.drugbank.ca}indication").text
        name = drug.find("{http://www.drugbank.ca}name").text
        tuple_drugbank_indication.append((str(name), str(indication).lower()))
        tuple_drug_bank_toxicity.append((name, str(toxicity).lower()))
    print("End loading DrugBank\n")
    return tuple_drugbank_indication, tuple_drug_bank_toxicity


'''
def loading_drugbank():
    print("Start loading DrugBank\n")
    tree = etree.parse("data/DRUGBANK/drugbank.xml")  # load xml file as ElementTree
    root = tree.getroot()
    dico_drugbank = {}  # Dictionary in which there will be drug which indication and toxicity

    for drug in root:
        for element in drug:  # we only look at indication and toxicity for a specific drug
            toxicity = drug.find("{http://www.drugbank.ca}toxicity").text
            indication = drug.find("{http://www.drugbank.ca}indication").text
            name = drug.find("{http://www.drugbank.ca}name").text
            dico_drugbank[name] = [indication, toxicity]
    print("End loading DrugBank\n")
    return dico_drugbank
'''
'''
i = 0
for drug in root:
    for element in drug:
        ele_name = element.tag
        ele_value = drug.find(element.tag).text
        print(ele_name, ' : ', ele_value)
    end = drug


for element in root[1]:
    ele_name = element.tag
    ele_value = root[1].find(element.tag).text
    print(ele_name, ' : ', ele_value)

'''
