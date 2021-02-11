# -*- coding: utf-8 -*-
"""
Created on Wed Nov  4 15:25:30 2020

@author: SOW - FISCHER - CHASSAGNON
"""

# EXAMEN COMPUTER SCIENCE FOR BIG DATA
# AUTEURS : SOW - FISCHER - CHASSAGNON

############################ IMPORT PACKAGES  ###########################
from py2neo import Graph, Node, Relationship
from  py2neo.ogm import *
import pandas as pd
import time
import numpy as np

#############################################################################
################# Importation de la base de donnée dans Neo4j ###############

# En amont :
    # 1) Ouvrir Neo4j et créer une nouvel base locale intitulée "Formule1"
    # 2) avec mot de passe : Neo4j
    # 3) La demarrer 
    # 4) et l'ouvrir 

############################ CREATION DES TABLES ############################

path_data = "C:/data/Formule1/"

graph_db = Graph("bolt://127.0.0.1:7687", auth=("neo4j", "Neo4j"))

graph_db.run("MATCH (n) DETACH DELETE n")

# Dans cette partie, nous créeons les tables de données dans Neo4j via Python 
# à partir de nos documents excels

# Lors de la création des tables, nous pouvons modifier leur contenu en séléctionnant 
# dans le "Node" que les informations qui nous interessent. Pour cela, il est important 
# de diviser son document excel de base (col=l.split(",")) et indiquer l'indice des colonnes
# que l'on souhaite garder. 

# Les différentes infos gardées dans chaque table sont indiquées dans le schémas de la base. 
# Celles en italiques corresspondent à celles que nosu avons enlevées. 

# ____# Table circuits #_______
circuits = {}

with open(path_data+"circuits.csv", "r", errors='ignore', encoding='utf-8') as f:
    first = True
    for l in f :
        if not first:
            col = l.split(",")
            print(col)
            circuits[col[0]] =  Node("Circuit",
                                  circuitId = int(col[0]),
                                  name = str(col[2]),
                                  location=str(col[3]),
                                  country=str(col[4]))
        else :
            first = False
            
print(circuits)
    
    
# ____# Table pilote #_______
drivers = {}

with open(path_data+"drivers.csv", "r", errors='ignore', encoding='utf-8') as f:
    first = True
    for l in f :
        if not first:
            col = l.split(",")
            print(col)
            drivers[col[0]] =  Node("drivers",
                                  id = int(col[0]),
                                  code= str(col[3]),
                                  name=str(col[4]),
                                  surname=str(col[5]),
                                  birth=str(col[6]),
                                  nationality=str(col[7]))
        else :
            first = False

print(drivers)

#____# Table courses #_______
races = {}

with open(path_data+"races.csv", "r") as f:
    first = True
    for l in f :
        if not first:
            col = l.split(",")
            print(col)
            races[col[0]] =  Node("Race",
                                  id = int(col[0]),
                                  year= int(col[1]),
                                  tour=str(col[2]),
                                  circuitId=int(col[3]),
                                  name=str(col[4]),
                                  date=str(col[5])) #rajouter.strip a la fin si marche pas ?
        else :
            first = False

print(races)

#____# Table Résultats #_______
results = {}
df = pd.read_csv(path_data+"results.csv",delimiter=",")

for index,row in df.iterrows():
    print(row)
    results[row['resultId']] = Node("Result",
                                id=int(row['resultId']),
                                race=int(row["raceId"]),
                                driver=int(row["driverId"]),
                                constructor=int(row["constructorId"]),
                                position=str(row["position"]),
                                points=int(row["points"]),
                                laps=int(row["laps"]),
                                time=str(row["time"]),
                                milliseconds=str(row["milliseconds"]),
                                fastestlap=str(row["fastestLap"]),
                                rank=str(row["rank"]),
                                fastestLapTime=str(row["fastestLapTime"]),
                                fastestLapspeed=(row["fastestLapSpeed"]))
print(results)

#____# Table Ecuries #_______
constructors = {}

with open(path_data+"constructors.csv", "r") as f:
    first = True
    for l in f :
        if not first:
            col = l.split(",")
            print(col)
            constructors[col[0]] =  Node("Constructor",
                                  id = int(col[0]),
                                  name = str(col[2]),
                                  nationalityC=str(col[3])) #rajouter.strip a la fin si marche pas ?
        else :
            first = False
            
print(constructors)

############################ RELATION ENTRE LES TABLES ############################

rel=[]

# Dans cette partie nous créeons les relations entre les tables et les noeuds. 
# La première relation serra plus detaillée dans sa construction afin de servir d'exemple pour la réalisation des suivantes. 

# Exemple : On va chercher a créer une relation entre la course et le circuit sur lequel elle a lieu

# Relation Race-Circuit : "SUR LE CIRCUIT" ###
# La première étape consiste à importer le document csv ou les deux informations sont reliées (présentes sur sur la même ligne).
df = pd.read_csv(path_data+"races.csv",delimiter=",")

# Puis le principe consiste à indiquer dans quelle colone (du document csv) on va retrouver l'information.
# (On peut le faire par le numéro de la colonne avec l.split ou avec le nom de la colonne)
for index,row in df.iterrows():
    course = races[str(row['raceId'])]
    # Pour l'information concernant la course c'est dans la colonne 'raceId' 
    # Pour la première ligne, raceId = 1 
    circuit = circuits[str(row['circuitId'])]
    # Pour l'information concernant le circuit c'est dans la colonne 'circuitId'
    # Pour la première ligne du doc csv, circuitId = 1 
    
    rel.append(Relationship(course,"SUR LE CIRCUIT ",circuit))
    # Ici on lie alors par la relation "SUR LE CIRCUIT" 
    # le noeud de la base circuit, dont le circuitId = 1,  
    # avec le noeud de la base races, dont le raceId = 1. 
    
# Les relations suivantes suivent la même méthode/structure
    
# relation Pilote - Course : "A PARTICIPE" ### ok 
df = pd.read_csv(path_data+"results.csv",delimiter=",")
for index,row in df.iterrows():
    course = races[str(row['raceId'])]
    pilote = drivers[str(row['driverId'])]
    rel.append(Relationship(pilote,"A PARTICIPE ",course))
    
# Relation Résulats - Pilote ###
df = pd.read_csv(path_data+"results.csv",delimiter=",")
for index,row in df.iterrows():
    resultat = results[row['resultId']]
    pilote = drivers[str(row['driverId'])]
    rel.append(Relationship(pilote,"RESULTATS PILOTE",resultat))

# Relation Résulats - Ecurie ###
df = pd.read_csv(path_data+"results.csv",delimiter=",")
for index,row in df.iterrows():
    resultat = results[row['resultId']]
    constructeur = constructors[str(row['constructorId'])]
    rel.append(Relationship(constructeur,"RESULTATS ECURIE",resultat))

# Relation Résulats - course ###
df = pd.read_csv(path_data+"results.csv",delimiter=",")
for index,row in df.iterrows():
    resultat = results[row['resultId']]
    course = races[str(row['raceId'])]
    rel.append(Relationship(resultat,"A LA COURSE ",course))
    
for r in rel :
    graph_db.create(r)
# Environ 10min donc soyez patients 
    
############# COMMANDE pour NEO4J ###################
    
# 1) Voir le réseau/graph dans Neo4j # ok
    graph_db.run("MATCH (n1)-[r]->(n2) RETURN r, n1, n2 LIMIT 25")  'Dans Neo4j'
    # limite est le nb de noeuds affichés (plus on en affiche plus c'est long)
    
# 2) Temps pour afficher tout le réseau depuis 1950 # ok
    debut = time.time()
    graph_db.run("MATCH (n1)-[r]->(n2) RETURN r, n1, n2")
    fin = time.time()
    print(round(fin - debut, 2), "s")

# 2) J'enleve toute les courses inférieure aux années 2000 # ok
    graph_db.run("MATCH (c:Race) WHERE c.year<2000 OPTIONAL MATCH (c)-[r]-() DELETE c,r")
    
# 3) Tous les pilotes allemands # ok
    list(graph_db.run("MATCH (d:drivers) WHERE d.nationality= 'German' RETURN d"))
    
# 4) Toutes les courses réalisé sur le circuit de Monaco # ok 
    list(graph_db.run("MATCH (n:Circuit)-[:`SUR LE CIRCUIT `]-(m) Where n.name='Circuit de Monaco' Return n,m"))

# 5) Toutes les courses réalisées après 2004 sur le circuit de Silverstone # ok mais pas toutes
    graph_db.run("MATCH (n:Circuit {name:'Silverstone Circuit'})-[:`SUR LE CIRCUIT `]-(course:Race) WHERE course.year>2004 Return course.name, course.year")
    # Dans neo4j : MATCH (n:Circuit {name:'Silverstone Circuit'})-[:`SUR LE CIRCUIT `]-(course:Race) WHERE course.year>2004 Return course,n
    
# 6) Qui a gagné le Grand Prix des USA en 2012 ? Et pour quelle écurie courait - il ? # ok 
    graph_db.run("Match (course:Race {name:'United States Grand Prix', year:2012})-[:`A LA COURSE `]-(res:Result {rank:'1.0'}), (pilote:drivers)-[:`RESULTATS PILOTE`]-(res),(ecurie:Constructor)-[:`RESULTATS ECURIE`]-(res) Return pilote.name, pilote.surname, ecurie.name, res.rank, course.name")
    # dans neo4j : Match (r:Race {name:'United States Grand Prix', year:2012})-[:`A LA COURSE `]-(res:Result {rank:'1.0'}), (pilote:drivers)-[:`RESULTATS PILOTE`]-(res),(ecurie:Constructor)-[:`RESULTATS ECURIE`]-(res) Return pilote,ecurie
    
# 7) Avec quelle écurie courait Fernando Alonso au grand prix de Malaisie en 2016 ? # ok 
    graph_db.run("Match (r:Race {name:'Malaysian Grand Prix', year:2016})-[:`A LA COURSE `]-(res:Result), (p:drivers {name:'Fernando'})-[:`RESULTATS PILOTE`]-(res),(ecurie:Constructor)-[:`RESULTATS ECURIE`]-(res) Return ecurie.name")
    # Dans neo4j : Match (r:Race {name:'Malaysian Grand Prix', year:2016})-[:`A LA COURSE `]-(res:Result), (p:drivers {name:'Fernando'})-[:`RESULTATS PILOTE`]-(res),(ecurie:Constructor)-[:`RESULTATS ECURIE`]-(res) Return ecurie
    
# 8) Nombre de course qu'a fait Micheal Schumacher ? # ok
    schumi_races = list(graph_db.run("Match (c:Race)-[:`A PARTICIPE `]-(p:drivers {surname:'Schumacher'}) Return c"))
    print("Schumi a eu le tps de faire ", int(len(schumi_races)), " avant de tomber sur une pierre")
    # Dans neo4j : Match (c:Race)-[:`A PARTICIPE `]-(p:drivers {surname:'Schumacher'}) Return c 
 
# 9) Quelle est la date de la première course de Jaguar ? Quelle était la course ? Sur quel circuit ? Quels étaient les deux pilotes qui les représentaient ? # ok 
    graph_db.run("MATCH (e:Constructor {name:'Jaguar'})-[:`RESULTATS ECURIE`]-(res:Result), (course:Race)-[:`A LA COURSE `]-(res) Return min(course.date)")
    graph_db.run("MATCH (course:Race {date:'2000-03-12'})-[:`SUR LE CIRCUIT `]-(circuit) Return course.name,circuit.name")
    graph_db.run("MATCH (r:Race {date:'2000-03-12'})-[:`A LA COURSE `]-(f:Result),(f:Result)-[:`RESULTATS ECURIE`]-(e:Constructor  {name:'Jaguar'}),(f:Result)-[:`RESULTATS PILOTE`]-(pilote:drivers), (c:Circuit)-[:`SUR LE CIRCUIT `]-(r:Race) Return pilote.name, pilote.surname")
    
# 10) Donner le schéma complet pour l'écurie Renault au Grand prix de Singapour, le 17 Septembre 2017 ? # ok 
#     C'est à dire le circuit de la course, les pilotes alignés par l'écurie, et leurs résultats respectifs. 
    graph_db.run("MATCH (course:Race {date:'2017-09-17', name:'Singapore Grand Prix'})-[:`A LA COURSE `]-(res:Result),(res:Result)-[:`RESULTATS ECURIE`]-(ecurie:Constructor {name:'Renault'}),(res:Result)-[:`RESULTATS PILOTE`]-(pilote:drivers), (circuit:Circuit)-[:`SUR LE CIRCUIT `]-(course:Race)Return course.name,res.rank,ecurie.name,pilote.name,pilote.surname,circuit.name")
    # Dans neo4j : MATCH (r:Race {date:'2017-09-17', name:'Singapore Grand Prix'})-[:`A LA COURSE `]-(f:Result),(f:Result)-[:`RESULTATS ECURIE`]-(e:Constructor {name:'Renault'}),(f:Result)-[:`RESULTATS PILOTE`]-(p:drivers), (c:Circuit)-[:`SUR LE CIRCUIT `]-(r:Race)Return r,f,e,p,c

# 11) Quels sont les pilotes dont le nom commence par "Ni" ? # ok
    graph_db.run("MATCH (pilote:drivers) WHERE pilote.name STARTS WITH 'Ni' RETURN pilote.name, pilote.surname")
    
# 12) Quel est le classement des pilotes les plsu titrés ? Top 3 pour l'instant # ok
    graph_db.run("Match (p:drivers)-[:`RESULTATS PILOTE`]-(res:Result {rank:'1.0'}) Return count(res) as nbtitre,p.name,p.surname,p.nationality ORDER BY nbtitre desc")
    
    
# 13) Chemin le plus court entre Mercedes et le circuit d'Istanbul ? Dans neo4j
    list(graph_db.run("MATCH (e:Constructor {name:'Mercedes'}), (c:Circuit {name:'Istanbul Park'}), p=shortestpath((e)-[*]-(c)) Return p"))
    # dans neo4j : MATCH (e:Constructor {name:'Mercedes'}), (c:Circuit {name:'Istanbul Park'}), p=shortestpath((e)-[*]-(c)) Return p
    
# 14) Afficher tout le graph concernant l'écurie McLaren et compter les courses auxquelles l'écurie a participé 
    list(graph_db.run("MATCH p =(t { name: 'McLaren' })-->() RETURN p"))
    # Dans neo4j : MATCH p =(t { name: 'McLaren' })-->() RETURN p
       
    
