import mysql.connector


def loading_sider_toxicity_cid():  # Select stitch-compound with side effect in meddra_all_se
    print("Start loading SIDER_toxicity\n")
    con = mysql.connector.connect(user='gmd-read',
                                  password='esial',
                                  host='neptune.telecomnancy.univ-lorraine.fr',
                                  database='gmd')
    cur = con.cursor()
    cur.execute('SELECT stitch_compound_id1, stitch_compound_id2, side_effect_name from meddra_all_se')
    tuple_sider_toxicity = cur.fetchall()
    cur.close()
    print("End loading SIDER_toxicity\n")
    a = lambda x: (x[0] + " " + x[1], x[2])
    return [a(i) for i in tuple_sider_toxicity]


def loading_sider_indications_cid():  # Select stitch-compound with side effect in meddra_all_se
    print("Start loading SIDER_indications\n")
    con = mysql.connector.connect(user='gmd-read',
                                  password='esial',
                                  host='neptune.telecomnancy.univ-lorraine.fr',
                                  database='gmd')
    cur = con.cursor()
    cur.execute('SELECT stitch_compound_id, concept_name from meddra_all_indications')
    tuple_sider_indication = cur.fetchall()
    cur.close()
    print("End loading SIDER_indications\n")
    return tuple_sider_indication


"""
field_names = [i[0] for i in cur.description]  # to getname of the columns


a = loading_sider_toxicity_cid()

con = mysql.connector.connect(user='gmd-read',
                              password='esial',
                              host='neptune.telecomnancy.univ-lorraine.fr',
                              database='gmd')

cur = con.cursor()
cur.execute('SELECT * from meddra_all_indications WHERE cui = "C1824797"')
# cur.execute('SELECT * from meddra_all_indications')
exemple = cur.fetchall()
cur.close()

"""
con = mysql.connector.connect(user='gmd-read',
                              password='esial',
                              host='neptune.telecomnancy.univ-lorraine.fr',
                              database='gmd')

cur = con.cursor()
cur.execute('SELECT * from meddra_all_se')
# cur.execute('SELECT * from meddra_all_indications')
exemple = cur.fetchall()
cur.close()