import sys
import glob
import xmltodict
import json
import requests
import time

SOLR_HOST = 'http://localhost:8983'


def delete_collection(collection):
    requests.get('{}/solr/admin/collections'.format(SOLR_HOST), params={
        'action': 'DELETE',
        'name': collection})


def delete_field(collection, name, field='field-type', has_copy=False):
    if has_copy == True:
        r = requests.post('{}/solr/{}/schema'.format(SOLR_HOST, collection), json={
            'delete-copy-field': { "dest": name, "source": "*" } })

    r = requests.post('{}/solr/{}/schema'.format(SOLR_HOST, collection), json={
        'delete-{}'.format(field): { "name": name } })


def field_existis(collection, name, field='fieldtypes'):
    get_field = requests.get('{}/solr/{}/schema/{}/{}'\
        .format(SOLR_HOST, collection, field, name)).json()

    if(get_field['responseHeader']['status'] == 0):
        return True

    return False


def create_collection(collection):
    print('Creating collection {} ... '.format(collection), end='')
    r = requests.get('{}/solr/admin/collections'.format(SOLR_HOST), params={
        'action': 'CREATE',
        'name': collection,
        'numShards': 1}).json()

    if 'error' in r:
        print("\nERROR:",r['error']['msg'])
        exit()
    
    print('collection created!')


def create_stem_field_type(collection, name='stem_es'):
    print('Creating stemmer field type {}  ... '.format(
        collection), end='')

    if(field_existis(collection, name, "fieldtypes")):
        delete_field(collection, name, "field-type")

    lang = 'Spanish' if name == 'stem_es' else 'Portuguese'

    r = requests.post('{}/solr/{}/schema'.format(SOLR_HOST, collection), json={
        "add-field-type": {
            "name": name,
            "class": "solr.TextField",
            "positionIncrementGap": "100",
            "analyzer": {
                "tokenizer": {"class": "solr.StandardTokenizerFactory"},
                "filters": [
                    {"class": "solr.LowerCaseFilterFactory"},
                    {"class": "solr.{}LightStemFilterFactory".format(lang)},
                    {"class": "solr.StopFilterFactory",
                     "words": "stopwords.txt",
                     "ignoreCase": "true"},
                ]}
        },
    }).json()

    if 'error' in r:
        print("\nERROR:",r['error']['msg'])
        exit()

    print('stemmer field created!')


def create_ngram_field_type(collection, name='text_ngram', minSize=1, maxSize=2):
    print('Creating ngram field type {}  ... '.format(
        name, collection), end='')

    if(field_existis(collection, "text", "fieldtypes")):
        delete_field(collection, "text", "field-type")

    r = requests.post('{}/solr/{}/schema'.format(SOLR_HOST, collection), json={
        "add-field-type": {
            "name": name,
            "class": "solr.TextField",
            "analyzer": {
                "tokenizer": {
                    "class": "solr.NGramTokenizerFactory",
                    "minGramSize": minSize,
                    "maxGramSize": maxSize
                }
            }
        }
    }).json()

    if 'error' in r:
        print("\nERROR:",r['error']['msg'])
        exit()

    print('ngram field created!')


def create_schema_field(collection, name, field_type, stored=True):
    print('Creating field ({}: {}) in {} ... '.format(
        name, field_type, collection), end='')

    if(field_existis(collection, name, "fields")):
        delete_field(collection, name, "field", True)

    r = requests.post('{}/solr/{}/schema'.format(SOLR_HOST, collection), json={
        "add-field": {
            "name": name,
            "type": field_type,
            "stored": stored,
            "indexed": True,
            "multiValued": True}
    }).json()

    if 'error' in r:
        print("\nERROR:",r['error']['msg'])
        exit()

    print('field created!')


def create_copy_field(collection, dest='_text_', source='*'):
    print('Create copy field in {} from {} to {}  ... '.format(
        collection, source, dest), end='')

    r = requests.post('{}/solr/{}/schema'.format(SOLR_HOST, collection), json={
        'add-copy-field': {
            'dest': dest,
            'source': source,
        },
    }).json()

    if 'error' in r:
        print("\nERROR:",r['error']['msg'])
        exit()

    print('copy field created!')


def post_documents_solr(collection, json_data):
    r = requests.post('{}/solr/{}/update/json/docs?commit=true'\
        .format(SOLR_HOST, collection), json=json_data).json()

    if 'error' in r:
        print("\nERROR:",r['error']['msg'])
        exit()

    elapsed_time = r['responseHeader']['QTime']
    return elapsed_time


def index_documents(documents_path, collection='informationRetrieval', lang='es'):
    files_path = '{}**/*.xml'.format(documents_path)
    files = glob.glob(files_path, recursive=True)

    if(len(files) == 0):
        print("No files in the path specified: {}".format(files_path))
        exit()

    delete_collection(collection)
    create_collection(collection)
    # create_ngram_field_type(collection, 'text_ngram', 3, 6)
    create_stem_field_type(collection, 'stem_{}'.format(lang))
    # create_schema_field(collection, 'text', 'text_{}'.format(lang))
    # create_schema_field(collection, 'title', 'text_{}'.format(lang))
    create_schema_field(collection, '_text_{}_'.format(lang), 'text_{}'.format(lang), stored=False)
    create_copy_field(collection, '_text_', '*')
    create_copy_field(collection, '_text_{}_'.format(lang), '*')

    time.sleep(1)

    print("Total of {} files found. Start indexing!".format(len(files)))

    for i, f in enumerate(files):
        print('Indexing {} ... {} '.format(i+1, f), end='')
        with open(f) as file:
            json_data = []
            xml_doc = xmltodict.parse(file.read())

            # Multiple docs by file
            if len(xml_doc['add']['doc']) > 1:
                for doc in xml_doc['add']['doc']:
                    fields = doc['field']
                    json_doc = {}
                    for field in fields:
                        json_doc[field['@name']
                                ] = field["#text"] if "#text" in field else ''

                    json_doc['id'] = json_doc['docid']
                    json_data.append(json_doc)

            # Unique doc in a file
            else:
                fields = xml_doc['add']['doc']['field']
                json_doc = {}
                for field in fields:
                    json_doc[field['@name']
                             ] = field["#text"] if "#text" in field else ''

                json_doc['id'] = json_doc['docid']
                json_data.append(json_doc)

            elapsed_time = post_documents_solr(collection, json_data)

            print('... {} doc(s) in {}ms'.format(
                len(xml_doc['add']['doc']), elapsed_time))


# python3 index_documents.py folhaSP pt /home/user/Datasets/
if __name__ == "__main__":

    collection = 'informationRetrieval'
    lang = 'es'
    documents_folder = 'files/documents/'

    if len(sys.argv) >= 2:
        collection = sys.argv[1]

    if len(sys.argv) >= 3:
        lang = sys.argv[2]

    if len(sys.argv) >= 4:
        documents_folder = sys.argv[3]

    index_documents(documents_folder, collection, lang)

    exit()
