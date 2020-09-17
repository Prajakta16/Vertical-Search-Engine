import pickle

from elasticsearch import Elasticsearch

MARITIME_INLINKS = "./input/all_maritime_inlinks"
MARITIME_OUTLINKS = "./input/all_maritime_outlinks"
MARITIME_CORPUS = "./input/all_maritime_courpus"
GRADE_RESULT_FILE = "results/gradeResultPD3"


def write_data(inlinkfile, outlinkfile, corpusfile, result):
    no_hits = len(result["hits"]["hits"])
    for j in range(0, no_hits):
        doc_id = result["hits"]["hits"][j]["_source"]["id"]
        corpusfile.write("\n" + doc_id)
        inlinkfile.write("\n" + doc_id)
        no_inlinks = len(result["hits"]["hits"][j]["_source"]["inlinks"])
        for k in range(0, no_inlinks):
            inlink = result["hits"]["hits"][j]["_source"]["inlinks"][k]
            inlinkfile.write(" " + inlink)

        outlinkfile.write("\n" + doc_id)
        no_outlinks = len(result["hits"]["hits"][j]["_source"]["outlinks"])
        for l in range(0, no_outlinks):
            outlink = result["hits"]["hits"][j]["_source"]["outlinks"][l]
            outlinkfile.write(" " + outlink)


def write_score_data(file, result):
    topic_id = 3
    username = "prajakta"
    no_hits = len(result["hits"]["hits"])
    for j in range(0, no_hits):
        doc_id = result["hits"]["hits"][j]["_source"]["id"]
        id = result["hits"]["hits"][j]["_id"]
        print(doc_id)
        score = input("Enter score: ")
        file.write(""+str(topic_id)+" "+username+" "+id+" "+str(score)+"\n")


class Index:
    # cloud settings
    INDEX_NAME = 'maritime_accidents'
    TYPE_NAME = 'document'
    USERNAME = 'elastic'
    PASSWORD = 'dY5DwHK0PxQ7PgNC0Za8P0Ec'
    es = Elasticsearch('https://bf7d8087ca284033b930a990073d7b4e.us-central1.gcp.cloud.es.io',
                       http_auth=(USERNAME, PASSWORD), scheme='https', port=9243)

    # localhost settings
    # ES_HOST = {"host": "localhost", "port": 9200}
    # INDEX_NAME = 'maritime_accidents'
    # TYPE_NAME = 'document'
    # es = Elasticsearch(hosts=[ES_HOST], timeout=3600)

    def get_linkgraph(self):
        search_query_body = {
            "query": {
                "match_all": {}
            },
            "_source": ["inlinks", "outlinks", "id"],
            "size": 100
        }
        result = self.es.search(index=self.INDEX_NAME, body=search_query_body, scroll='10m')
        scroll_id = result["_scroll_id"]

        with open(MARITIME_CORPUS, mode='w') as corpus_file:
            with open(MARITIME_INLINKS, mode='w') as inlinks_file:
                with open(MARITIME_OUTLINKS, mode='w') as outlinks_file:
                    write_data(inlinks_file, outlinks_file, corpus_file, result)
                    for i in range(2, 1100):
                        print("Processing 100 docs upto "+str(i*100))
                        sc_result = self.es.scroll(scroll_id=scroll_id, scroll='10m')
                        write_data(inlinks_file, outlinks_file, corpus_file, sc_result)
                outlinks_file.close()
            inlinks_file.close()
        corpus_file.close()

        with open(MARITIME_INLINKS, mode='r') as inlinks_file:
            content = inlinks_file.read().split("\n")
            print(len(content))
        inlinks_file.close()

        with open(MARITIME_OUTLINKS, mode='r') as outlinks_file:
            content = outlinks_file.read().split("\n")
            print(len(content))
        outlinks_file.close()

    def getSearchResultsForTopic(self, topic):
        body = {
            "query": {
                "query_string": {
                    "default_field": "text",
                    "query": topic
                }
            },
            "size": 10
            , "_source": ["text", "id"]
        }
        result = self.es.search(index=self.INDEX_NAME, body=body, scroll='30s')
        scroll_id = result["_scroll_id"]

        # with open(GRADE_RESULT_FILE, mode='w') as file:
        #     write_score_data(file, result)
        # file.close()

        for i in range(1, 22):
            with open(GRADE_RESULT_FILE, mode='a') as file:
                print("Processing 10 docs upto " + str(i * 10))
                sc_result = self.es.scroll(scroll_id=scroll_id, scroll='30s')
                scroll_id = sc_result["_scroll_id"]
                if i > 19:
                    write_score_data(file, sc_result)
            file.close()
        pass


if __name__ == '__main__':
    maritime_index = Index()
    # maritime_index.get_linkgraph()
    topic = "costa concordia disaster and recovery"
    maritime_index.getSearchResultsForTopic(topic)
