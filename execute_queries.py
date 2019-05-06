import untangle
import requests

RESULT_LIMIT = 100
SOLR_HOST = 'http://localhost:8983'
COLLECTION = 'informationRetrieval'


def get_query(topic):
    return topic.title.cdata.replace(':', '') + " " + topic.desc.cdata


def show_query_result(number, docs, student='francisco_e_lucas'):
    index = 0
    for doc in docs:
        doc_id = doc['docid']
        score = doc['score']
        print('{0:<7} {1:5} {2:20} {3:<5} {4:<12} {5:18}'.format(
            number, "Q0", doc_id[0], index, score, student))
        index += 1


def execute_queries(collection):
    obj = untangle.parse('files/queries.xml')

    for topic in obj.root.top:
        query = get_query(topic)

        r = requests.get('{}/solr/{}/select'.format(SOLR_HOST, collection),
                         params={'q': query, 'fl': '*, score', 'rows': RESULT_LIMIT})
        result = r.json()
        docs = result['response']['docs']

        show_query_result(topic.num.cdata, docs)


if __name__ == "__main__":
    execute_queries(COLLECTION)
