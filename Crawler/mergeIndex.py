import pickle
import re
import os
import time

from elasticsearch import Elasticsearch
from elasticsearch import helpers

CRAWLED_DATA_FOLDER = "./CRAWLED_DATA_FOLDER"
PARTIAL_INDEXING_FOLDER = "./partial_indexing_data/"
SYNCED_INDEXING_FOLDER = "./synced_indexed_folder/"
FINAL_INDEX_FILE = "./final_indexed_file"
STOPWORDS_FILE = "topic_keywords/stoplist.txt"
TEMP_FOLDER = "./temp_data/"
new_count = 0
existing_count = 0
failed_bulk_data = []


def read_data(batch_no, batch_size, folder_name, extension):
    start = int(((batch_no - 1) * batch_size / 100) + 1)  # docs crawled in last batch + 1
    end = start + batch_size / 100
    data_to_be_indexed = {}
    while start < end:
        try:
            # print("Reading from file " + folder_name+str(start)+""+extension)
            filepath = open(os.path.join(folder_name, str(start) + "" + extension), 'rb')
            new_dict = pickle.load(filepath)
            filepath.close()
            data_to_be_indexed.update(new_dict)  # merging existing data with new data
            start += 1
        except Exception:
            print("Cannot read data for " + str(start) + "" + extension)
            start += 1
            pass
    return data_to_be_indexed


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

    def delete_and_create_new_index(self):
        # if self.es.indices.exists(self.INDEX_NAME):
        #     print("index already exists... deleting " + self.INDEX_NAME + " index...")
        #     res = self.es.indices.delete(index=self.INDEX_NAME, ignore=[400, 404])
        #     print(" response: '%s'" % res)

        request_body = {
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
                "max_result_window": 90000,
                "analysis": {
                    "filter": {
                        "english_stop": {
                            "type": "stop",
                            "stopwords": ["a", "about", "above", "according", "across", "after", "afterwards",
                                          "again", "against", "albeit", "all", "almost", "alone", "along",
                                          "already", "also", "although", "always", "am", "among", "amongst", "an",
                                          "and", "another", "any", "anybody", "anyhow", "anyone", "anything",
                                          "anyway", "anywhere", "apart", "are", "around", "as", "at", "av", "be",
                                          "became", "because", "become", "becomes", "becoming", "been", "before",
                                          "beforehand", "behind", "being", "below", "beside", "besides", "between",
                                          "beyond", "both", "but", "by", "can", "cannot", "canst", "certain", "cf",
                                          "choose", "contrariwise", "cos", "could", "cu", "day", "do", "does",
                                          "doing", "dost", "doth", "double", "down", "dual", "during", "each",
                                          "either", "else", "elsewhere", "enough", "et", "etc", "even", "ever",
                                          "every", "everybody", "everyone", "everything", "everywhere", "except",
                                          "excepted", "excepting", "exception", "exclude", "excluding", "exclusive",
                                          "far", "farther", "farthest", "few", "ff", "first", "for", "formerly",
                                          "forth", "forward", "from", "front", "further", "furthermore", "furthest",
                                          "get", "go", "had", "halves", "hardly", "has", "hast", "hath", "have",
                                          "he", "hence", "henceforth", "her", "here", "hereabouts", "hereafter",
                                          "hereby", "herein", "hereto", "hereupon", "hers", "herself", "him",
                                          "himself", "hindmost", "his", "hither", "hitherto", "how", "however",
                                          "howsoever", "i", "ie", "if", "in", "inasmuch", "inc", "include",
                                          "included", "including", "indeed", "indoors", "inside", "insomuch",
                                          "instead", "into", "inward", "inwards", "is", "it", "its", "itself",
                                          "just", "kind", "kg", "km", "last", "latter", "latterly", "less", "lest",
                                          "let", "like", "little", "ltd", "many", "may", "maybe", "me", "meantime",
                                          "meanwhile", "might", "moreover", "most", "mostly", "more", "mr", "mrs",
                                          "ms", "much", "must", "my", "myself", "namely", "need", "neither",
                                          "never", "nevertheless", "next", "no", "nobody", "none", "nonetheless",
                                          "noone", "nope", "nor", "not", "nothing", "notwithstanding", "now",
                                          "nowadays", "nowhere", "of", "off", "often", "ok", "on", "once", "one",
                                          "only", "onto", "or", "other", "others", "otherwise", "ought", "our",
                                          "ours", "ourselves", "out", "outside", "over", "own", "per", "perhaps",
                                          "plenty", "provide", "quite", "rather", "really", "round", "said", "sake",
                                          "same", "sang", "save", "saw", "see", "seeing", "seem", "seemed",
                                          "seeming", "seems", "seen", "seldom", "selves", "sent", "several",
                                          "shalt", "she", "should", "shown", "sideways", "since", "slept", "slew",
                                          "slung", "slunk", "smote", "so", "some", "somebody", "somehow", "someone",
                                          "something", "sometime", "sometimes", "somewhat", "somewhere", "spake",
                                          "spat", "spoke", "spoken", "sprang", "sprung", "stave", "staves", "still",
                                          "such", "supposing", "than", "that", "the", "thee", "their", "them",
                                          "themselves", "then", "thence", "thenceforth", "there", "thereabout",
                                          "thereabouts", "thereafter", "thereby", "therefore", "therein", "thereof",
                                          "thereon", "thereto", "thereupon", "these", "they", "this", "those",
                                          "thou", "though", "thrice", "through", "throughout", "thru", "thus",
                                          "thy", "thyself", "till", "to", "together", "too", "toward", "towards",
                                          "ugh", "unable", "under", "underneath", "unless", "unlike", "until", "up",
                                          "upon", "upward", "upwards", "us", "use", "used", "using", "very", "via",
                                          "vs", "want", "was", "we", "week", "well", "were", "what", "whatever",
                                          "whatsoever", "when", "whence", "whenever", "whensoever", "where",
                                          "whereabouts", "whereafter", "whereas", "whereat", "whereby", "wherefore",
                                          "wherefrom", "wherein", "whereinto", "whereof", "whereon", "wheresoever",
                                          "whereto", "whereunto", "whereupon", "wherever", "wherewith", "whether",
                                          "whew", "which", "whichever", "whichsoever", "while", "whilst", "whither",
                                          "who", "whoa", "whoever", "whole", "whom", "whomever", "whomsoever",
                                          "whose", "whosoever", "why", "will", "wilt", "with", "within", "without",
                                          "worse", "worst", "would", "wow", "ye", "yet", "year", "yippee", "you",
                                          "your", "yours", "yourself", "yourselves"]
                            # "stopwords_path": "./stoplist.txt"
                        }
                    },
                    "analyzer": {
                        "stopped": {
                            "type": "custom",
                            "tokenizer": "standard",
                            "filter": [
                                "lowercase",
                                "porter_stem",
                                "english_stop"
                            ]
                        }
                    }
                }
            },
            "mappings": {
                "properties": {
                    "_size": {
                        "enabled": True
                    },
                    "text": {
                        "type": "text",
                        "fielddata": True,
                        "analyzer": "stopped",
                        "index_options": "positions"
                    },
                    "id": {
                        "type": "keyword",
                        "index": True
                    },
                    "inlinks": {
                        "type": "keyword",
                        "index": True
                    },
                    "outlinks": {
                        "type": "keyword",
                        "index": True
                    }
                }
            }
        }

        print("creating " + self.INDEX_NAME + " index...")
        res = self.es.indices.create(index=self.INDEX_NAME, body=request_body)
        print(" response: '%s'" % res)

    def index_data(self, data, batch_no):
        global new_count, existing_count, failed_bulk_data
        bulk_data1 = []  # list of all crawled urls to be indexed
        bulk_data2 = []

        # data[url] = {"id": url, "text": text, # "inlinks": (set)inlinks[url], "outlinks": (set)outlinks}
        for key in data:
            exists, old_inlinks = self.check_doc_exists(key)

            if "inlinks" in data[key]:
                data[key]["inlinks"] = list(data[key]["inlinks"])
            if "outlinks" in data[key]:
                data[key]["outlinks"] = list(data[key]["outlinks"])

            if not exists:  # create a new entry
                # print("New " + key)
                new_count += 1
                data_refined = {
                    "_index": self.INDEX_NAME,
                    "_id": str(key).replace("://", "-").replace("/", "-"),
                    "_source": data[key]
                }
                if len(bulk_data1) <= 500:
                    bulk_data1.append(data_refined)
                else:
                    bulk_data2.append(data_refined)

            else:  # get existing inlinks and merge
                if len(old_inlinks) > 0:
                    new_inlinks = list(set(old_inlinks + data[key]["inlinks"]))
                    data[key]["inlinks"] = new_inlinks
                    change = len(old_inlinks) - len(new_inlinks)
                    # print("New inlinks added for " + key + " " + str(change))
                    if change != 0:
                        existing_count += 1
                        data_refined = {
                            "_index": self.INDEX_NAME,
                            "_id": str(key).replace("://", "-").replace("/", "-"),
                            "_source": data[key]
                        }
                        if len(bulk_data1) <= 500:
                            bulk_data1.append(data_refined)
                        else:
                            bulk_data2.append(data_refined)

        print(len(bulk_data1), len(bulk_data2))
        print("----------------Indexing bulk data--------------------")
        if len(bulk_data1) > 0:
            try:
                res = helpers.bulk(self.es, bulk_data1)
                print(res)
            except Exception:
                print("Indexing failed for chunk " + str(batch_no) + "-1, retrying")
                i = 0
                while i <= 500:
                    try:
                        res = helpers.bulk(self.es, bulk_data1[i:i + 10])
                        print(res)
                        i = i + 10
                    except Exception:
                        print("Indexing failed for chunk " + str(i) + "+10 docs")
                        pass
                pass
        if len(bulk_data2) > 0:
            try:
                res = helpers.bulk(self.es, bulk_data2)
                print(res)
            except Exception:
                print("Indexing failed for chunk " + str(batch_no) + "-1, retrying")
                i = 0
                while i <= 500:
                    try:
                        res = helpers.bulk(self.es, bulk_data2[i:i + 10])
                        print(res)
                        i = i + 10
                    except Exception:
                        print("Indexing failed for chunk " + str(i) + "+10 docs")
                        pass
                pass

    def check_doc_exists(self, doc_id):
        # print("Checking if doc exists on es")
        try:
            # No exception indicates record is found in the index
            result = self.es.get(index=self.INDEX_NAME, id=doc_id.replace("://", "-").replace("/", "-"))
            # print(doc_id + " " + str(result["found"]))
            return True, result["_source"]["inlinks"]
        except:
            # print(doc_id + " not found")
            return False, []


if __name__ == '__main__':
    start_time = time.time()
    batch_size = 1000
    no_batches = int(40000 / batch_size)
    maritime_index = Index()

    # Deletion should be only run by the first team member
    # maritime_index.delete_and_create_new_index()
    # exit()

    for i in range(1, no_batches + 2):
        print("Processing batch " + str(i))
        data_to_be_indexed_dict = read_data(i, batch_size, SYNCED_INDEXING_FOLDER, "")
        print("New docs to index " + str(len(data_to_be_indexed_dict)))

        maritime_index.index_data(data_to_be_indexed_dict, i)
        print(new_count)
        print(existing_count)

        print("Indexed total of" + str(i * batch_size) + "docs")
        print("--- %s seconds ---" % (time.time() - start_time))
