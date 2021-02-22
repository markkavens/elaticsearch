from flask import *
import requests
import json
import os ,re

app = Flask(__name__)
app.secret_key = os.urandom(24)

ESURL = 'http://localhost:9200/netflix/'

#####################

class Query:
    def __init__(self):
        self.QUERY = { 
            "from" : 0,
            "size" : 500,
            "query": {
                    "bool": { 
                        "must": [],
                        "must_not": [],
                        "filter": [],
                        "should": []
                    }
                },
            "sort": []
        }

#####################

def error(message):
    return { 'error': True , "message" : message }

@app.route('/')
def home():
    return '<h1>API HOME</h1>'

## format elasticsearch output for api output
def prepareOutput(x):
    results = {'hits':[] , 'error' : False}
    for i in x['hits']['hits'] : 
        results['hits'].append(i['_source'])
    return results


@app.route('/autocomplete/' ,methods=["POST"])
def autocomplete():
    inp = {}
    if request.method=='POST':
        print(request.get_json())
        inp = request.get_json()
    else :
        return error('only POST allowed')

    print(inp)

    ## query object
    q = Query()

    ## 5 results
    q.QUERY['size'] = 5

    ## build query according to inp
    if inp=={} or 'queryString' not in list(inp.keys()):
        return error('queryString is required')

    q.QUERY['query']['bool']['must'].append( { "match_phrase_prefix":{ "title":{ "query": inp['queryString'] } } } )

    ## child mode
    if 'childMode' in list(inp.keys()) and inp['childMode']==True:
        q.QUERY['query']['bool']['must_not'].append({ "match": { "rating": "r" } } )
        q.QUERY['query']['bool']['must_not'].append({ "match": { "rating": "nc" } } )
        q.QUERY['query']['bool']['must_not'].append({ "wildcard": { "rating": "pg" } } )

    ## query elastic search
    try:
        query = json.dumps(q.QUERY)
        response = requests.post(ESURL+'_search/', data=query, headers = {"Content-type": "application/json"})
        results = json.loads(response.text)
        return prepareOutput(results)
    except:
        return error("Could not connect to elasticsearch")


@app.route('/paginate/' ,methods=['POST','GET'])
def paginate():
    inp = {}
    if request.method=='POST':
        print(request.get_json())
        inp = request.get_json()
    else :
        inp = request.args.to_dict(flat=True)

    print(inp)

    ## query object
    q = Query()

    ## input empty error
    if inp=={}:
        return error('no input')

    ## if type set
    if 'type' in list(inp.keys()):
        q.QUERY['query']['bool']['must'].append({ "match":{ "type": inp['type'] } } )

    offset = q.QUERY['size']
    ## setting default pageSize
    if 'pageSize' in list(inp.keys()):
        offset = inp['pageSize']
        q.QUERY['size'] = inp['pageSize']

    ## setting default page number
    if 'pageNo' in list(inp.keys()):
        offset *= (inp['pageNo']-1)
        q.QUERY['from'] = offset
    
    ## sort by release year
    q.QUERY['sort'].append({ "release_year" : { "order": "desc" } })
    
    try:
        query = json.dumps(q.QUERY)
        response = requests.post(ESURL+'_search/', data=query, headers = {"Content-type": "application/json"})
        results = json.loads(response.text)
        return prepareOutput(results)
    except:
        return error("Could not connect to elasticsearch")

## parse queryString to be used for genre matching
def parseQuery(s):
    s = s.upper()
    s = re.sub("[A-Za-z]+", lambda ele: " " + ele[0] + " ", s) 
    s = s.strip()
    s = re.sub(' +',' ',s)
    print(str(s))
    l = s.split(" ") 
    res = []
    for i in l:
        if i not in ["AND","OR","NOT"] and re.match("[A-Za-z]+",i):
            i = "".join(list(i)[:-1])
            temp = "/.*"+i.lower()+".*/"
            res.append(temp)
        else :
            res.append(i)
    
    ans = " ".join(res)
    print(ans)
    return ans


@app.route('/custom/', methods=['POST'])
def custom():
    if request.method!='POST':
        return error('only post requests allowed')

    q = Query()
    inp = request.get_json()
    if inp=={}:
        return error('no input given')

    ## if type set
    if 'type' not in list(inp.keys()) or inp['type'] not in ['exact', 'prefix','genre'] :
        return error('type is required')
    
    ## query string 
    if 'queryString' not in list(inp.keys()):
        return error('queryString is required')
    
    ## exact match
    if inp['type']=='exact':
        ## if field not set
        if 'field' not in list(inp.keys()) :
            return error('field is required when type is exact') 
        else : 
            q.QUERY['query']['bool']['must'].append({ "match_phrase":{ inp['field']: inp['queryString'] } } )

    ## prefix endpoint
    elif inp['type'] == 'prefix' :
        inp['field'] = 'description'
        q.QUERY['query']['bool']['must'].append({ "prefix":{ "description": inp['queryString'].lower() } } )

    ## genre endpoint    
    elif inp['type'] == 'genre' :
        inp['field'] = 'listed_in'
        q.QUERY['query'].pop('bool')
        q.QUERY['query'] = {'query_string' : {'default_field':"" , 'query':"" } }
        q.QUERY['query']['query_string']['default_field'] = inp['field']
        q.QUERY['query']['query_string']['query'] = parseQuery(inp['queryString'])
    
    try:
        query = json.dumps(q.QUERY)
        response = requests.post(ESURL+'_search/', data=query, headers = {"Content-type": "application/json"})
        results = json.loads(response.text)
        return prepareOutput(results)
    except:
        return error("Could not connect to elasticsearch")



if __name__ == '__main__':
    app.run(debug=True, port = 5500)


