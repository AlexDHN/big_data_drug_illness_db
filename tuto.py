test = etree.parse("data/DRUGBANK/test.xml")
root = test.getroot()

tag = root.tag
print(tag)

# get all attributes
attributes = root.attrib
print(attributes)

# iterate over all the nodes with tag name - holiday
for user in root.findall('user'):
    print(user)

for user in root.findall('user'):
    # get all attributes of a node
    attributes = user.attrib
    print(attributes)

    # get a particular attribute
    data_id = attributes.get('data-id')
    print(data_id)

for user in root.findall('user'):
    # access all elements in node
    for element in user:
        ele_name = element.tag
        ele_value = user.find(element.tag).text
        print(ele_name, ' : ', ele_value)


tree = etree.parse("data/DRUGBANK/drugbank.xml")
root = tree.getroot()

tag = root.tag
print(tag)

# get all attributes
attributes = root.attrib
print(attributes)

for drug in root.findall('./drug'):
    print("1")
    print(drug)

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