#!/usr/bin/env python
# coding:utf-8
from json import dumps
from flask import Flask, g, Response, request
from neo4j.v1 import GraphDatabase, basic_auth
from click.types import STRING

debugFlag = True


app = Flask(__name__, static_url_path='/static/')
driver = GraphDatabase.driver('bolt://localhost', auth=basic_auth("neo4j", "root"))

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

@app.route("/tree")
def get_graph_table():
    try:
        name = request.args["name"]
        flag = request.args["flag"]
    except KeyError:
        return Response(build_tree(), mimetype="application/json")
    else:
        return Response(build_tree(name, flag), mimetype="application/json")
    
def chang_name(table_name_and_id_dict, children_list):
    for elem in children_list:
        elem['name'] = table_name_and_id_dict[elem['name']]
        if 'children' in elem:
            elem['children'] = chang_name(table_name_and_id_dict, elem['children'])
    return children_list

def build_tree(name="", flag="0"):
    table_name_and_id_dict = {}
    db = get_db()
    if name=="":
        results = db.run("MATCH (n:Table)-[r:depTable*]->(m:Table) WHERE n.name = 'app.app_base_customer_info'\
            RETURN n as source_table,m as target_table,r as relation_list")
        table_name_and_id_dict['0'] = 'app.app_base_customer_info'
    elif flag == "0":
        results = db.run("MATCH (n:Table)-[r:depTable*]->(m:Table) WHERE n.name =~ {table}\
            RETURN n as source_table,m as target_table,r as relation_list", {"table": name})
        table_name_and_id_dict['0'] = name
    else:
        results = db.run("MATCH (n:Column)-[r:depColumn*]->(m:Column) WHERE n.name =~ {column}\
            RETURN n as source_table,m as target_table,r as relation_list", {"column": name})
        table_name_and_id_dict['0'] = name
    return_dict = {} #最后输出结果
    return_dict['name'] = '0'
    for recode in results:
        table_id = str(recode['target_table'].id)
        table_name = recode['target_table']['name']    
        table_name_and_id_dict.setdefault(table_id, table_name)
        if debugFlag:
            print 'target_table', table_name, 'relation_list', recode['relation_list']
        # 下面是主题
        return_dict.setdefault('children', [])
        
        tmp = return_dict['children'] # tmp是一个list
        for deep_relation in recode['relation_list']:
            if len(tmp)!=0 and str(deep_relation.end) == tmp[len(tmp)-1]["name"]:
                tmp[len(tmp)-1].setdefault('children', [])
                tmp = tmp[len(tmp)-1]['children']
            else:
                new_node = {}
                new_node['name'] = str(deep_relation.end)
                tmp.append(new_node)
                tmp[len(tmp)-1]["size"] = int(tmp[len(tmp)-1]["name"])
    return_dict['name'] = table_name_and_id_dict[return_dict['name']]
    if 'children' in return_dict:
        return_dict['children'] = chang_name(table_name_and_id_dict, return_dict['children'])
    return dumps(return_dict)

@app.route("/search_table")
def get_search_table():
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

@app.route("/search_column")
def get_search_column():
    try:
        q = request.args["q"]
    except KeyError:
        return []
    else:
        db = get_db()
        results = db.run("MATCH (column:Column) "
                 "WHERE column.name =~ {column} "
                 "RETURN column", {"column": "(?i).*" + q + ".*"})
        res_list = [record['column']['name'] for record in results]
        return Response(dumps(res_list),
            mimetype="application/json")

if __name__ == '__main__':
    app.run(port=8080)
