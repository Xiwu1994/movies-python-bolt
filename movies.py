#!/usr/bin/env python
from json import dumps

from flask import Flask, g, Response, request

from neo4j.v1 import GraphDatabase, basic_auth

app = Flask(__name__, static_url_path='/static/')
driver = GraphDatabase.driver('bolt://localhost', auth=basic_auth("neo4j", "root"))
# basic auth with: driver = GraphDatabase.driver('bolt://localhost', auth=basic_auth("<user>", "<pwd>"))

def get_db():
    if not hasattr(g, 'lineage_analysis.db'):
        g.neo4j_db = driver.session()
    return g.neo4j_db

@app.teardown_appcontext
def close_db(error):
    if hasattr(g, 'lineage_analysis.db'):
        g.neo4j_db.close()

@app.route("/")
def get_index():
    return app.send_static_file('index.html')

def serialize_cast(cast):
    return {
        'name': cast[0],
        'job': cast[1],
        'role': cast[2]
    }

@app.route("/graph")
def get_graph():
    try:
        q = request.args["q"]
    except KeyError:
        return Response(build_graph(), mimetype="application/json")
    else:
        return Response(build_graph(q), mimetype="application/json")

def build_graph(table = ""):
    print "build_graph(table = '') table:", table
    db = get_db()
    if table == "":
        results = db.run(" MATCH (m:Table)<-[:depTable]-(a:Table) \
              RETURN a.name as table, collect(m.name) as cast \
              LIMIT 100 ")
    else:
        results = db.run("MATCH (m:Table)<-[:depTable]-(a:Table) WHERE a.name =~ {table} \
        RETURN a.name as table, collect(m.name) as cast LIMIT 100", 
        {"table": "(?i).*" + table + ".*"})
    
    nodes = []
    rels = []
    i = 0
    for record in results:
        print record['table']
        nodes.append({"name": record["table"], "label": "Table"})
        target = i
        i += 1
        for name in record['cast']:
            actor = {"name": name, "label": "Table"}
            try:
                source = nodes.index(actor)
            except ValueError:
                nodes.append(actor)
                source = i
                i += 1
            rels.append({"source": source, "target": target})
    return dumps({"nodes": nodes, "links": rels})

@app.route("/search")
def get_search():
    try:
        q = request.args["q"]
    except KeyError:
        return []
    else:
        db = get_db()
        results = db.run("MATCH (table:Table) "
                 "WHERE table.name =~ {table} "
                 "RETURN table", {"table": "(?i).*" + q + ".*"})
        res_list = [record['table']['name'] for record in results]
        return Response(dumps(res_list),
            mimetype="application/json")

if __name__ == '__main__':
    app.run(port=8080)
