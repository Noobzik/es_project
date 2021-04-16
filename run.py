from flask import (Flask, request, make_response, render_template)
from elasticsearch import Elasticsearch, helpers
import requests
import json

app = Flask(__name__, static_url_path='/static')


@app.route("/")
def hello():
    return render_template("content.html")

@app.route("/home")
def home():
    return render_template("content.html")

@app.route("/load")
def load():
    number=request.args.get('number')
    es = ES_Data()
    result_data = es.search(number)
    response = make_response(render_template("table.html", data=result_data))
    return response

@app.route("/goToForm")
def goToForm():
    response = make_response(render_template("create.html"))
    return response

@app.route("/load_categorie")
def load_categorie():
    value = request.args.get('keyword')
    es = ES_Data()
    result_data = es.search_categorie(value)
    response = make_response(render_template("table.html", data=result_data))
    return response

@app.route("/postForm")
def postForm():
    matiere     = request.args.get('matiere')
    coef        = request.args.get('coef')
    intervenant = request.args.get('intervenant')
    sexe        = request.args.get('sexe')
    categorie   = request.args.get('categorie')
    nbheure     = request.args.get('nbheure')
    dateDebut   = request.args.get('dateDebut')
    dateFin     = request.args.get('dateFin')
    description = request.args.get('description')

    es = ES_Data()
    result_data = es.postForm(matiere, coef, intervenant, sexe, categorie, nbheure, dateDebut, dateFin, description)
    if (result_data['_shards']['failed'] == 0):
        res = "Document added successfully"
    else :
        res = "please try again"
    response = make_response(render_template("content.html", formReponse = res))
    return response


@app.route("/recherche")
def recherche():
    mot = request.args.get('mot')
    es = ES_Data()
    result_data = es.recherche(mot)
    response = make_response(render_template("table.html", data=result_data))
    return response

@app.route("/load_sexe")
def load_sexe():
    value = request.args.get('keyword')
    es = ES_Data()
    result_data = es.search_sexe(value)
    response = make_response(render_template("table.html", data=result_data))
    return response

@app.route("/load_duree_cours")
def load_duree_cours():
    es = ES_Data()
    result_data = es.duree_cours()
    response = make_response(render_template("table_res.html", data=result_data))
    return response

@app.route("/load_sort_heure")
def load_sort_heure():
    es = ES_Data()
    result_data = es.sort_heure()
    response = make_response(render_template("table.html", data=result_data))
    return response

@app.route("/load_sort_coeff")
def load_sort_coeff():
    es = ES_Data()
    result_data = es.sort_coeff()
    response = make_response(render_template("table.html", data=result_data))
    return response

@app.route("/load_sort_matiere")
def load_sort_matiere():
    es = ES_Data()
    result_data = es.sort_matiere()
    response = make_response(render_template("table.html", data=result_data))
    return response

@app.route("/load_aggs_nb_heure")
def load_aggs_nb_heure():
    es = ES_Data()
    result_data = es.aggs_nb_heure()
    response = make_response(render_template("table_aggs.html", data=result_data))
    return response


class ES_Data:

    def __init__(self):
        self.es = Elasticsearch(
            [
                'http://localhost:9200/'
            ],
            timeout=100
        )
    
    # Params : self -> self, number -> nombre elem à renvoyer
    # Returns: List[Tuple] resultat de la requete parsé
    # Le résultat de la requete sur l'index ESGI de taille number
    def search(self, number):
        es_query = {
            'query': {
                "match_all":{}
            },
            'size': number
        }
        es_result = self.es.search(index="esgi", body=es_query)['hits']['hits']
        result = []

        for c in es_result:
            result.append(tuple(c.items()))
        if (result):
            print("search : requete passé")
        return result


    # Recherche du terme value dans le champs catégorie
    def search_categorie(self, value):
        es_query = {
            "query": {
                "term": {
                    "categorie": value
                }
            }
        }
        es_result = self.es.search(index="esgi", body=es_query, size=1000)['hits']['hits']
        result = []

        for c in es_result:
            result.append(tuple(c.items()))
        if (result):
            print("search_categorie : requete passé")
        return result

    # Création d'un nouveau document dont les champs sont les paramètres
    def postForm(self, matiere, coef, intervenant, sexe, categorie, nbheure, dateDebut, dateFin, description) :
        es_query = {
            "matiere": matiere,
            "coefficient": coef,
            "intervenant": intervenant,
            "sexe": sexe,
            "categorie": categorie,
            "nbr_heures": nbheure,
            "date_debut": dateDebut,
            "date_fin": dateFin,
            "description": description
            }
        es_result = self.es.index(index="esgi", body = es_query)
        if (es_result):
            print("postForm : requete passé")
        return es_result

    # Renvoie la recherche de mot dans le champs description
    def recherche(self, mot):
        es_query = {
          "query": {
            "match": {
                "description": mot
            }
          }
        }
        es_result = self.es.search(index="esgi", body=es_query)['hits']['hits']
        result = []

        for c in es_result:
            result.append(tuple(c.items()))
        if (result):
            print("recherche : requete passé")
        return result

    # recherche à score constant sur le champs sexe
    def search_sexe(self, value):
        es_query = {
          "query": {
            "constant_score": {
              "filter": {
                "term": {
                  "sexe": value
                }
              }
            }
          }
        }
        es_result = self.es.search(index="esgi", body=es_query, size=1000)['hits']['hits']
        result = []
        for c in es_result:
            result.append(tuple(c.items()))
        if (result):
            print("search_sexe : requete passé")
        return result

    # Trie en fonction de l'heure par ordre décroissant
    def sort_heure(self):
        es_query ={
            "sort": [{"nbr_heures": {"order": "desc"}}]
        }
        es_result = self.es.search(index="esgi", body=es_query, size=1000)['hits']['hits']
        result = []

        for c in es_result:
            result.append(tuple(c.items()))
        if (result):
            print("sort_heure : requete passé")
        return result

    # Trie en fonction du coefficient par ordre décroissant
    def sort_coeff(self):
        es_query ={
          "sort":[{"coefficient": {"order": "desc"}}]
        }
        es_result = self.es.search(index="esgi", body=es_query, size=1000)['hits']['hits']
        result = []

        for c in es_result:
            result.append(tuple(c.items()))
        if (result):
            print("sort_coeff : requete passé")
        return result

    # Trie en fonction de la matière par ordre croissant
    def sort_matiere(self):
        es_query = {
          "sort":[{"matiere.raw": {"order": "asc"}}]
        }
        es_result = self.es.search(index="esgi", body=es_query, size=1000)['hits']['hits']
        result = []

        for c in es_result:
            result.append(tuple(c.items()))
        if (result):
            print("sort_matiere : requete passé")
        return result

    # Aggrège les documents par catégorie, puis calcule la somme des durées des cours
    def aggs_nb_heure(self):
        es_query = {
          "aggs": {
            "avg_per_nb_heure": {
              "terms": {
                "field": "categorie"
              },
              "aggs": {
                "sum_per_categorie": {
                  "sum": {
                    "field": "nbr_heures"
                  }
                }
              }
            }
          }
        }
        es_result = self.es.search(index="esgi", body=es_query, size=1000)['aggregations']['avg_per_nb_heure']['buckets']
        result = []
        for c in es_result:
            result.append(tuple(c.items()))
        if (result):
            print("aggs_nb_heure : requete passé")
        return result

    # Calcule la durée de chaque cours en fonction de ses dates de début et de fin
    def duree_cours(self):
        es_query = {
            "script_fields": {
              "duree_en_jours": {
                "script": "(doc.date_fin.value.millis - doc.date_debut.value.millis)/ 1000 /3600/24"
              }
            },
            "_source": True
        }
        es_result = self.es.search(index="esgi", body=es_query, size=1000)['hits']['hits']
        result = []

        for c in es_result:
            result.append(tuple(c.items()))
        if (result):
            print("duree cours : requete passé")
        return result

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=3000)
