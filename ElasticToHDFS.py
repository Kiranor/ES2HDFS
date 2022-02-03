import configparser
from io import BufferedReader
import subprocess
import sys
from configparser import ConfigParser
from elasticsearch import Elasticsearch
from elasticsearch.client import indices


def get_all_indices(elastic):
    """Returns all indexes from Elasticsearch

    Args:s
        elastic (elasticsearch.client.Elasticsearch): [Elasticsearch object]

    Returns:
        [list of strings]: [list of all indeсes]
    """
    return [n for n in sorted(list(elastic.indices.get_alias("*").keys())) if not n.startswith(".")]

def get_number_of_docs(elastic, logs_index):
    """Returns number of docs in passed index
    
    Args:
        elastic (elasticsearch.client.Elasticsearch): [Elasticsearch object]
        logs_index (string): [index name]

    Returns:s
        [int]: [number of docs]
    """
    return elastic.search(
        index=logs_index,
        body={
            "query":{
                "match":{
                    "_index":logs_index
                }
            }
        },
        size=0, track_total_hits=True)["hits"]["total"]["value"]
    
def create_configs():
    config = ConfigParser()
    
    config["Elasticsearch"] = {
        "ES_HOST" : "kib.t1.soc.ecssec.ru",
        "ES_PORT" : "9200",
        "ES_AUTH" : ":",
        "ES_SAVE_PATH" : "/tmp/dumps"
    }
    config["HDFS"] = {
        "HDFS_SAVE_PATH" : "/tmp/test/output",
        "HDFS_PATH" : "/bin/hdfs"
    }
    config["Elasticdump"] = {
        "ED_PATH" : "/bin/elasticdump"
    }
    config["gzip"] = {
        "GZ_PATH" : "/bin/gzip"
    }
    with open("config.ini", "w") as c:
        config.write(c)

def read_configs():
    
    conf = ConfigParser()
    conf.read("config.ini")
    
    es_conf = conf["Elasticsearch"]
    hdfs_conf = conf["HDFS"]
    ed_conf = conf["Elasticdump"]
    gz_conf = conf["gzip"]

    global es_host, es_port, es_auth, es_save_path
    es_host = es_conf["es_host"]
    es_port = es_conf["es_port"]
    es_auth = es_conf["es_auth"]
    es_save_path = es_conf["es_save_path"]
    
    global hdfs_save_path, hdfs_path
    hdfs_save_path = hdfs_conf["hdfs_save_path"]
    hdfs_path = hdfs_conf["hdfs_path"]
    
    global ed_path
    ed_path = ed_conf["ed_path"]
    
    global gz_path
    gz_path = gz_conf["gz_path"]

if __name__ == "__main__":
    
    if len(sys.argv) < 2:
        print("Start dumps? (y or n)")
        if input() == "n":
            print("Exiting")
            exit() 
    
    try:
        if sys.argv[1] == "--configs":
            create_configs()
            print("Created configs.ini")
            exit()
    except:
        pass
        
    read_configs()
    
    #"http://:@kib.t1.soc.ecssec.ru:9200/"
    es_path = f"http://{es_auth}@{es_host}:{es_port}/"
    print(es_path)
    _es = Elasticsearch(
        [{
            "host" : es_host,
            "port" : es_port, 
            "http_auth" : es_auth
        }])
    
    indeces = get_all_indices(_es)
    
    try: 
        if sys.argv[1] == "--indeces":
            with open("old_indeces.txt", "w") as old:
                for index in indeces:
                    old.write(index + "\n")
            print("Created indeces.txt")
            exit()
    except:
        pass

    if(_es.ping()):
             
        with open("old_indeces.txt", "r") as old:
            old_indeces = old.read().split()
                
        if old_indeces != indeces:
            indeces = list(filter(lambda x: x not in old_indeces, indeces))
        for index in indeces:
            if "winlogbeat" in index:
                continue
            try:
                #http://:#kib.t1.soc.ecssec.ru:9200/index
                es_input_path = f"http://{es_auth}@{es_host}:{es_port}/{index}"#es_path + index
                #/tmp/dumps/index.json
                es_save_path_json = f"{es_save_path}/{index}.json" 
                #"/tmp/test/output/index.json.gz"
                hdfs_save_path_gz = f"{hdfs_save_path}/{index}.json.gz"
                
                elastic_args = [
                    ("/bin/elasticdump"),
                    ("--type=data"),
                    ("--input=" + es_input_path),
                    ("--output=" + es_save_path_json)]
                    
                hdfs_args = [
                    ("/bin/hdfs"),
                    ("dfs"),
                    ("-put"),
                    #local path to file to copy from
                    (f"{es_save_path_json}.gz"),
                    #hdfs save path to file to copy at
                    (hdfs_save_path_gz)]

                #starting dump
                print(f"Starting dump of {index}")
                subprocess.call(elastic_args)
                print(f"Dump of index {index} downloaded successfully")
                
                #compressing dump to .gz                
                print(f"Starting compressing of {index}")

                subprocess.call(["/bin/gzip", f"{es_save_path_json}"])
                print(f"Dump of index {index} compressed successfully")
                
                #sending dump to hdfs
                print(f"Starting writing to hdfs of {index}")
                subprocess.call(hdfs_args)
                print(f"Dump of index {index} successfully written to hdfs")
                
            except:
                continue
    else:
        print("Unable to connect to Elasticsearch server")
        
    with open("old_indeces.txt", "w") as f:
        for i in sorted(list(set(old_indeces + indeces))):
            f.write(f"{i}\n")
                           
    print("All indeces was dumped successfully")    

