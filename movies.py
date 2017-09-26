#!/usr/bin/env python
from json import dumps

from flask import Flask, g, Response, request

from neo4j.v1 import GraphDatabase, basic_auth

app = Flask(__name__, static_url_path='/static/')
driver = GraphDatabase.driver('bolt://localhost', auth=basic_auth("neo4j", "root"))
# basic auth with: driver = GraphDatabase.driver('bolt://localhost', auth=basic_auth("<user>", "<pwd>"))

def get_db():
    if not hasattr(g, 'neo4j_db'):
        g.neo4j_db = driver.session()
    return g.neo4j_db

@app.teardown_appcontext
def close_db(error):
    if hasattr(g, 'neo4j_db'):
        g.neo4j_db.close()

@app.route("/")
def get_index():
    return app.send_static_file('index.html')

def serialize_movie(movie):
    return {
        'id': movie['id'],
        'title': movie['title'],
        'summary': movie['summary'],
        'released': movie['released'],
        'duration': movie['duration'],
        'rated': movie['rated'],
        'tagline': movie['tagline']
    }

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

def build_graph(title = ""):
    print "title: ", title
    db = get_db()
    if title == "":
        results = db.run(" MATCH (m:Movie)<-[:ACTED_IN]-(a:Person) \
              RETURN m.title as movie, collect(a.name) as cast \
              LIMIT 100 ")
    else:
        results = db.run("MATCH (m:Movie)<-[:ACTED_IN]-(a:Person) WHERE m.title =~ {title} \
        RETURN m.title as movie, collect(a.name) as cast LIMIT 100", 
        {"title": "(?i).*" + title + ".*"})
    
    nodes = []
    rels = []
    i = 0
    for record in results:
        nodes.append({"title": record["movie"], "label": "movie"})
        target = i
        i += 1
        for name in record['cast']:
            actor = {"title": name, "label": "actor"}
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
        results = db.run("MATCH (movie:Movie) "
                 "WHERE movie.title =~ {title} "
                 "RETURN movie", {"title": "(?i).*" + q + ".*"})
        res_list = [serialize_movie(record['movie']) for record in results]
        return Response(dumps(res_list),
                        mimetype="application/json")

@app.route("/movie/<title>")
def get_movie(title):
    db = get_db()
    results = db.run("MATCH (movie:Movie {title:{title}}) "
             "OPTIONAL MATCH (movie)<-[r]-(person:Person) "
             "RETURN movie.title as title, "
             "collect([person.name, "
             "head(split(lower(type(r)), '_')), r.roles]) as cast "
             "LIMIT 1", {"title": title})

    result = results.single();
    return Response(dumps({"title": result['title'],
                           "cast": [serialize_cast(member)
                                    for member in result['cast']]}),
                    mimetype="application/json")

if __name__ == '__main__':
    app.run(port=8080)
