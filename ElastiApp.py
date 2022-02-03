from elasticsearch import Elasticsearch
import json
#import requests
import logging
#from pyspark.sql import SparkSession


def connect_to_elasticsearch():
	_es = None 
	_es = Elasticsearch(
	      [{'host':'kib.t1.soc.ecssec.ru',
 	        'port':9200,
                'http_auth':('', '')}])
	#print(_es.ping())
	if _es.ping():
		print("Connected")
	else:
		print("Unable to connect")
	return _es.ping()


if __name__ == "__main__":
	test_index = ''
	logs_index = "dbkasperskysc_index_2021.06.28"

	_es = Elasticsearch(
              [{'host':'kib.t1.soc.ecssec.ru',
                'port':9200,
                'http_auth':('', '')}])

	if(connect_to_elasticsearch()):
		result = _es.get(
			index=logs_index,
			id="DU8oUnoBJ-JE91v2wv_H")
		print(result)
	else:
		print("Unable to connect")

	#logging.basicConfig(level=logging.ERROR)
