import sys
import untangle
import requests

RESULT_LIMIT = 100
SOLR_HOST = 'http://localhost:8983'


def get_query(topic, qopt):
    query = topic.title.cdata

    if("desc" in qopt):
        query = query + " " + topic.desc.cdata
    
    if("narr" in qopt):
        query = query + " " + topic.narr.cdata
    
    return query.replace(':', '')

def show_query_result(number, docs, student='if-solr'):
    index = 0
    for doc in docs:
        doc_id = doc['docid']
        score = doc['score']
        print('{0:<7} {1:5} {2:20} {3:<5} {4:<12} {5:18}'.format(
            number, "Q0", doc_id[0], index, score, student))
        index += 1


def execute_queries(collection, queries, qopt, lang):
    obj = untangle.parse(queries)

    for topic in obj.root.top:
        query = get_query(topic, qopt)

        r = requests.get('{}/solr/{}/select'.format(SOLR_HOST, collection),
                        params={
                            'q': query,
                            'fl': '*, score',
                            'rows': RESULT_LIMIT,
                            'df': lang # '_text_es_'
                        })
        if 'error' in r.json():
            print("\nERROR:",r.json()['error']['msg'])
            exit()

        result = r.json()
        docs = result['response']['docs']

        show_query_result(topic.num.cdata, docs)

# python3 execute_queries.py folhaSP pt queries.xml title > results.txt
if __name__ == "__main__":

    queries = 'files/queries.xml'
    collection = 'informationRetrieval'
    qopt = 'title'
    lang = '_text_es_'

    if len(sys.argv) >= 2:
        collection = sys.argv[1]

    if len(sys.argv) >= 3:
        lang = '_text_{}_'.format(sys.argv[2])

    if len(sys.argv) >= 4:
        queries = sys.argv[3]

    if len(sys.argv) >= 5:
        qopt = sys.argv[4]

    execute_queries(collection, queries, qopt, lang)
    
    exit()
