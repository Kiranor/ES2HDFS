def get_all_indices(elastic):
    return [n for n in sorted(list(elastic.indices.get_alias("*").keys())) if not n.startswith(".")]


def connect_to_elastic(elastic):
    if(elastic.ping()):
        print("Connected")
    else:
        print("Unable to connect")
    return elastic.ping()


def get_number_of_docs(elastic, logs_index):
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
