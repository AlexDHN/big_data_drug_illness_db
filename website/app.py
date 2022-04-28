from flask import render_template, request, flash, request, jsonify, Flask
from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField
from wtforms.validators import DataRequired
from main import load_data

global rdd_drug_indication, rdd_drug_toxicity, rdd_illness_symptoms, rdd_hpo_id_symptoms, rdd_hpo_annotations_id_illness, rdd_indication_sider, rdd_toxicity_sider, df_stitch_CID_to_ATC, df_stitch_CID_to_ATC_aux, df_stitch_ATC_to_drug
from main import *

rdd_drug_indication, rdd_drug_toxicity, rdd_illness_symptoms, rdd_hpo_id_symptoms, rdd_hpo_annotations_id_illness, rdd_indication_sider, rdd_toxicity_sider, df_stitch_CID_to_ATC, df_stitch_CID_to_ATC_aux, df_stitch_ATC_to_drug = load_data("C:/Users/Alexandre/Documents/Cour_Telecom/GMD/gmd_dhenin_jurczack_leana/data")

app = Flask(__name__)
app.config['SECRET_KEY'] = 'my super secret key'

research = ""

# Create Form Class
class SearchForm(FlaskForm):
    search = StringField("search", validators=[DataRequired()])
    submit = SubmitField("Search")


"""
@app.route('/', methods=['GET', 'POST'])
def home():
    bool = 0;
    return render_template("home.html", research=research, bool=bool)
"""


# @app.route('/results/?<research>', methods=['GET', 'POST'])
# def results(research):
#     return render_template("results.html", research=research)

@app.route('/results', methods=['GET', 'POST'])
def results():
    return render_template("results.html", research=research)


# f = open('../variable.txt', 'r')
# variable = eval(f.read())


@app.route('/', methods=['GET', 'POST'])
def homebis():
    thing = None
    andor = None
    form = SearchForm()
    if form.validate_on_submit():
        thing = form.search.data
        form.search.data = ''
        if ' AND ' in thing or ' OR ' in thing:
            print("ici")
            andor = thing
            print(andor)
            variable = get_and_or(andor, rdd_drug_indication, rdd_drug_toxicity, rdd_illness_symptoms,rdd_hpo_id_symptoms,rdd_hpo_annotations_id_illness, rdd_indication_sider, rdd_toxicity_sider,df_stitch_CID_to_ATC, df_stitch_CID_to_ATC_aux, df_stitch_ATC_to_drug)
            print(variable)
            print(andor)
            print(thing)
    synonyms = []
    dis_cause_both = []
    hpo = []
    omim = []
    drug_cause_both = []
    drug_cause_drugbank = []
    drug_cause_sider = []
    drug_cure_both = []
    drug_cure_drugbank = []
    drug_cure_sider = []
    dis_cause = []
    drug_cause = []
    drug_cure = []

    if (thing and not andor):
        variable = get_res(thing, rdd_drug_indication, rdd_drug_toxicity, rdd_illness_symptoms, rdd_hpo_id_symptoms,
                           rdd_hpo_annotations_id_illness, rdd_indication_sider, rdd_toxicity_sider,
                           df_stitch_CID_to_ATC, df_stitch_CID_to_ATC_aux, df_stitch_ATC_to_drug)
        dis_cause_both = []
        hpo = variable[1]
        omim = variable[2]
        synonyms = variable[0]

        hpo_temp = []
        for i in range(len(hpo)):
            if hpo[i] in omim:
                dis_cause_both.append(hpo[i])
            else:
                hpo_temp.append(hpo[i])
        hpo = hpo_temp
        omim_temp = []
        for i in range(len(omim)):
            if omim[i] not in dis_cause_both:
                omim_temp.append(omim[i])
        omim = omim_temp

        drug_cause_both = []
        drug_cause_drugbank = variable[3]
        drug_cause_sider = variable[4]

        drug_cause_drugbank_temp = []
        for i in range(len(drug_cause_drugbank)):
            if drug_cause_drugbank[i] in drug_cause_sider:
                drug_cause_both.append(drug_cause_drugbank[i])
            else:
                drug_cause_drugbank_temp.append(drug_cause_drugbank[i])
        drug_cause_drugbank = drug_cause_drugbank_temp
        drug_cause_sider_temp = []
        for i in range(len(drug_cause_sider)):
            if drug_cause_sider[i] not in drug_cause_both:
                drug_cause_drugbank_temp.append(drug_cause_sider[i])
        drug_cause_sider = drug_cause_sider_temp

        drug_cure_both = []
        drug_cure_drugbank = variable[5]
        drug_cure_sider = variable[6]

        drug_cure_drugbank_temp = []
        for i in range(len(drug_cure_drugbank)):
            if drug_cure_drugbank[i] in drug_cure_sider:
                drug_cure_both.append(drug_cure_drugbank[i])
            else:
                drug_cure_drugbank_temp.append(drug_cure_drugbank[i])
        drug_cure_drugbank = drug_cure_drugbank_temp
        drug_cure_sider_temp = []
        for i in range(len(drug_cure_sider)):
            if drug_cure_sider[i] not in drug_cure_both:
                drug_cure_sider_temp.append(drug_cure_sider[i])
        drug_cure_sider = drug_cure_sider_temp

    elif andor != None:
        print(andor)
        dis_cause = variable[0]
        drug_cause = variable[1]
        drug_cure = variable[2]

    return render_template("homebis.html",
                           form=form,
                           thing=thing,
                           andor=andor,
                           synonyms=synonyms,
                           dis_cause_both=dis_cause_both,
                           hpo=hpo,
                           omim=omim,
                           drug_cause_both=drug_cause_both,
                           drug_cause_drugbank=drug_cause_drugbank,
                           drug_cause_sider=drug_cause_sider,
                           drug_cure_both=drug_cure_both,
                           drug_cure_drugbank=drug_cure_drugbank,
                           drug_cure_sider=drug_cure_sider,
                           dis_cause=dis_cause,
                           drug_cause=drug_cause,
                           drug_cure=drug_cure
                           )


app.run(debug=False, host='0.0.0.0')  # Port 5000 par défaut

"""
print(__name__)
app = Flask(__name__)
app.config['SECRET_KEY'] = 'hjshjhdjah kjshkjdhjs'
app.run(debug=True)
app.run(host='0.0.0.0')  # Port 5000 par défaut
"""
