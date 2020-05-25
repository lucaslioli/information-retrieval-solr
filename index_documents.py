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


def create_collection(collection):
    print('Creating collection {} ... '.format(collection), end='')
    r = requests.get('{}/solr/admin/collections'.format(SOLR_HOST), params={
        'action': 'CREATE',
        'name': collection,
        'numShards': 1})

    if 'error' in r.json():
        print("\nERROR:",r.json()['error']['msg'])
        exit()
    
    print('collection created!')


def create_stem_field_type(collection, name='stem_pt'):
    print('Creating stemmer field type {}  ... '.format(
        collection), end='')
    requests.post('{}/solr/{}/schema'.format(SOLR_HOST, collection), json={
        'add-field-type': {
            "name": name,
            "class": "solr.TextField",
            "positionIncrementGap": "100",
            "analyzer": {
                "tokenizer": {"class": "solr.StandardTokenizerFactory"},
                "filters": [
                    {"class": "solr.LowerCaseFilterFactory"},
                    {"class": "solr.SpanishLightStemFilterFactory"},
                    {"class": "solr.StopFilterFactory",
                     "words": "stopwords.txt",
                     "ignoreCase": "true"},
                ]}
        },
    })
    print('stemmer field created!')


def create_schema_field(collection, name, field_type, stored=True):
    print('Creating field ({}: {}) in {} ... '.format(
        name, field_type, collection), end='')
    requests.post('{}/solr/{}/schema'.format(SOLR_HOST, collection), json={
        "add-field": {
            "name": name,
            "type": field_type,
            "stored": stored,
            "indexed": True,
            "multiValued": True}
    })
    print('field created!')


def create_copy_field(collection, dest='_text_', source='*'):
    print('Create copy field in {} from {} to {}  ... '.format(
        collection, source, dest), end='')
    requests.post('{}/solr/{}/schema'.format(SOLR_HOST, collection), json={
        'add-copy-field': {
            'dest': dest,
            'source': source,
        },
    })
    print('copy field created!')


def post_documents_solr(collection, json_data):
    r = requests.post(
        '{}/solr/{}/update/json/docs?commit=true'.format(SOLR_HOST, collection), json=json_data)

    if 'error' in r.json():
        print("\nERROR:",r.json()['error']['msg'])
        exit()

    elapsed_time = r.json()['responseHeader']['QTime']
    return elapsed_time


def index_documents(documents_path, collection='informationRetrieval'):
    files = glob.glob('{}/**/*.xml'.format(documents_path), recursive=True)

    delete_collection(collection)
    create_collection(collection)
    create_schema_field(collection, '_text_pt_', 'text_pt', stored=False)
    create_copy_field(collection, '_text_', '*')
    create_copy_field(collection, '_text_pt_', '*')

    time.sleep(1)

    for f in files:
        print('Indexing ... {} '.format(f), end='')
        with open(f) as file:
            json_data = []
            xml_doc = xmltodict.parse(file.read())
            for doc in xml_doc['add']['doc']:
                fields = doc['field']
                json_doc = {}
                for field in fields:
                    json_doc[field['@name']
                             ] = field["#text"] if "#text" in field else ''

                json_doc['id'] = json_doc['docid']
                json_data.append(json_doc)

            elapsed_time = post_documents_solr(collection, json_data)

            print('... {} docs in {}ms'.format(
                len(xml_doc['add']['doc']), elapsed_time))


if __name__ == "__main__":

    documents_folder = 'files/documents'
    collection = 'informationRetrieval'

    if len(sys.argv) >= 2:
        collection = sys.argv[1]

    if len(sys.argv) >= 3:
        documents_folder = sys.argv[2]

    index_documents(documents_folder, collection)

