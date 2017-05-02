# encoding: utf-8
import csv
import numpy as np
import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch import helpers

_es = Elasticsearch([
    {"host": "192.168.1.10", "port": 9200}
])


def get_papers_id_of_field(field):
    papers_id_list = list()
    body = {
        "query": {
            "bool": {
                "must": [
                    {
                        "match_phrase": {
                            "study.study": field
                        }
                    }
                ]
            }
        }
    }
    res = helpers.scan(_es, query=body, index="paper_v2")
    c = 0
    for hit in res:
        papers_id_list += {"_id": hit["_id"], "id": hit["_source"]["id"]}
        if c % 10000 == 0:
            print c / 10000
        c += 1
    return pd.DataFrame(papers_id_list)

print get_papers_id_of_field("machine learning")



