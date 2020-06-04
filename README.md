# Information Retrieval - Apache Solr

Work developed for Information Retrieval discipline of Postgraduate in Computer Science course, at UFRGS.
The objective is to practice the topics teached in class using a Information Retrieval software.
We have chosen [Apache Solr](https://lucene.apache.org/solr/).

## Requirements

* [Apache Solr installation](https://lucene.apache.org/solr/guide/7_7/installing-solr.html)
* Python 3.6+
* PIP Packages: untangle; xmltodict; requests.

## Steps to run
Considering that Solr have already been started.

### 1. Pre-processing documents

Convert all collection files from SGML to XML before indexing. All files should be located at `files/documents` folder.

```
$ python3 preprocess_documents.py files/documents
```

### 2. Indexing documents
**Choose one of the commands below**. It's optional to inform collection name, language (es, pt, ...) and documents folder, but you have to keep that same order. The default options are ```informationRetrieval```, ```es``` and ```files/documents/```. 
```
$ python3 index_documents.py
$ python3 index_documents.py collectionName language /home/user/dataset/
```

### 3. Executing queries
**Choose one of the commands below**. It's optional to inform collection name, language (es, pt, ...), queries file and option to create the querie, but you have to keep that same orders. The default options are ```informationRetrieval```, ```es```, ```files/queries.xml``` and ```title```. The possible options to create the queries are ```title```, ```desc```, ```narr```, or ```desc-narr```, but all of them will consider the title.
```
$ python3 execute_queries.py > results.txt
$ python3 execute_queries.py collectionName language queries.xml title > results.txt
```
