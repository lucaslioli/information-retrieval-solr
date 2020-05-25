import sys
import untangle
import requests

RESULT_LIMIT = 100
SOLR_HOST = 'http://localhost:8983'


def get_query(topic, qopt):
    query = topic.title.cdata

    if(qopt == "desc"):
        query = query + " " + topic.desc.cdata
    
    if(qopt == "narr"):
        query = query + " " + topic.desc.cdata  + " " + topic.narr.cdata
    
    return query.replace(':', '')

def show_query_result(number, docs, student='francisco_e_lucas'):
    index = 0
    for doc in docs:
        doc_id = doc['docid']
        score = doc['score']
        print('{0:<7} {1:5} {2:20} {3:<5} {4:<12} {5:18}'.format(
            number, "Q0", doc_id[0], index, score, student))
        index += 1


def execute_queries(collection, queries, qopt):
    obj = untangle.parse(queries)

    for topic in obj.root.top:
        query = get_query(topic, qopt)

        r = requests.get('{}/solr/{}/select'.format(SOLR_HOST, collection),
                        params={
                            'q': query,
                            'fl': '*, score',
                            'rows': RESULT_LIMIT,
                            'df': '_text_pt_'
                        })
        if 'error' in r.json():
            print("\nERROR:",r.json()['error']['msg'])
            exit()

        result = r.json()
        docs = result['response']['docs']

        show_query_result(topic.num.cdata, docs)

# python3 execute_queries.py folhaSP queries.xml > results.txt
if __name__ == "__main__":

    queries = 'files/queries.xml'
    collection = 'informationRetrieval'
    qopt = "title"

    if len(sys.argv) >= 2:
        collection = sys.argv[1]

    if len(sys.argv) >= 3:
        queries = sys.argv[2]

    if len(sys.argv) >= 4:
        qopt = sys.argv[3]

    execute_queries(collection, queries, qopt)