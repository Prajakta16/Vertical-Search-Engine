# 1. Read the query file
# 2. Access each query
# 3. Apply stopwards and stemwords to trim the query, optionally use pseudo relevance
# 4. Use elasticsearch API to get tf, df of each term
# 5. Use formula for vector space model and language model to calculate scores
# 6. Rank top 1000 docs and write into a file

import math
import os
import pickle

from elasticsearch import Elasticsearch

DOC_LENGTH_FILE = "./preprocessed_files/DocLength"
QUERY_FILE = "./query.txt"
STOPWORDS_FILE = "./stoplist.txt"
RESULT_ESBUILTIN_FILE = "./result/esbuiltin.txt"
RESULT_BM25_FILE = "./result/result_bm25.txt"
PICKLE_DOCLENGTH_DICT_FILE_LOCATION = "result/docLengthFinal.pickle"

INDEX_NAME = 'maritime_accidents'
TYPE_NAME = 'document'
USERNAME = 'elastic'
PASSWORD = 'dY5DwHK0PxQ7PgNC0Za8P0Ec'
es = Elasticsearch('https://bf7d8087ca284033b930a990073d7b4e.us-central1.gcp.cloud.es.io',
                   http_auth=(USERNAME, PASSWORD), scheme='https', port=9243, timeout=3600)

stopwords = []
doc_length_dict = {}
doc_dict = {}
corpus_details_dict = {}
term_details = {}

total_docs = 0
avg_length = 0
smooting_param = 0.21
V = 0
total_docs_length = 0
k1 = 1.2
k2 = 100
b = 0.75


def _read_queries():
    dictt = {}
    with open(QUERY_FILE, 'r') as query_file:
        line = query_file.readline()
        while line:
            query_split = line.split(".   ")
            query_no = query_split[0]
            if len(query_split) > 1:
                query = query_split[1].strip().replace("\n", "")
                query_terms = _refine_query(query)
                dictt[query_no] = query_terms  # form a dictionary for all terms in a query
            line = query_file.readline()
    query_file.close()
    return dictt


def analyze_query(query):
    analyze_query_body = {
        "analyzer": "stopped",
        "text": query
    }
    result = es.indices.analyze(index=INDEX_NAME, body=analyze_query_body)
    length = len(result["tokens"])
    analysed_query = []
    for i in range(0, length):  # appending the words returned from analyze API
        # if result["tokens"][i]["token"] not in ('thi', 'hi', 'ls'):
        analysed_query.append(result["tokens"][i]["token"])
    return analysed_query


def _refine_query(actual_query):
    print("Actual query: " + str(actual_query))
    shortedned_query = actual_query.replace(",", "").replace("\"", "").split(" ")

    words = [
        "any", "anticipate", "air", "actual", "area", "application",
        "basis",
        "cite", "current", "contrast", "controversy",
        "discuss", "document", "Document", "directly", "describe", "develop",
        "event", "exist",
        "group",
        "include", "identify", "instances",
        "location", "level", "least",
        "method", "make", "motivation",
        "one",
        "predict", "prediction", "product", "pending", "perpetrated", 'platform',
        "report", "role", "result",
        "solely", "second", "some", "system", "side", "specific", "success", "since", "something", "support", "study",
        "taken", "type",
        "water", "will", "worldwide"
    ]

    for i in words:  # removing all redundant words from query
        while i in shortedned_query:
            shortedned_query.remove(i)

    formatted_query = []
    text = ""
    for word in shortedned_query:
        text += " " + word
    formatted_query += analyze_query(text)

    red_words = ["side", "action", "actual", "ha", "studi", "system", "someth", 'motiv', "perpetr", "ongo", "measur",
                 "specifi", "unsubstanti", "product", "level", "contrast", 'standard',
                 "make", "preliminari", "exist", "take", ""]
    for j in red_words:  # removing all redundant words from query
        while j in formatted_query:
            formatted_query.remove(j)
    print("formatted query" + str(formatted_query))

    return formatted_query


def _get_term_details(term_to_search, query_docs):
    query_body = {
        "query": {"term": {"text": term_to_search}},
        "track_total_hits": True,
        "track_scores": True,
        "size": 40000,
        "_source": {
            "excludes": ["text", "inlinks", "outlinks"]},
        "explain": True
    }
    res = es.search(index=INDEX_NAME, body=query_body)
    df = res["hits"]["total"]["value"]  # value gives the total matching documents

    if df < 40000:
        size = df
    else:
        size = 40000

    # analysing each of the matching document
    matching_docs = {}
    ttf = 0
    for i in range(0, size):
        doc_id = res["hits"]["hits"][i]["_id"]
        doclength = doc_length_dict[doc_id]
        # value gives the freq, occurrences of term within document
        tf_value = int(res["hits"]["hits"][i]["_explanation"]["details"][0]["details"][2]["details"][0]["value"])
        ttf = ttf + tf_value
        matching_docs[doc_id] = {"tf": tf_value, "length": doclength}
        if doc_id not in query_docs:
            query_docs[doc_id] = doclength

    term_detail = {'df': df, 'ttf': ttf, 'matching_docs': matching_docs}
    if term_to_search in ['korea']:
        print("For term " + term_to_search + " df=" + str(df) + " ttf=" + str(ttf))
    return term_detail, query_docs


def _sort_doc_by_score(doc_score, reqd_docs):
    if len(doc_score) < reqd_docs:
        num_docs = len(doc_score)
    else:
        num_docs = reqd_docs

    # Ranking 1000 top documents
    ranked_docs = {k: v for k, v in sorted(doc_score.items(), reverse=True, key=lambda item: item[1])[:num_docs]}
    return ranked_docs


def _write_result(ranked_docs, file_location):
    with open(file_location, mode='w') as result_file:
        for query in ranked_docs:  # range(50, 101):  # accessing all queries in ranked docs
            ranked_doclist = ranked_docs[query]  # accessing top 1000 results for given query
            rank = 1
            for doc in ranked_doclist:
                score = str(ranked_docs[query][doc])
                result_file.write(
                    query + " " + doc + " " + str(rank) + " " + score + "\n")
                rank = rank + 1
    result_file.close()


def _calculate_bm25_score(query_num):
    bm25_doc_score = {}
    query_text = query_dict[query_num]
    for word in query_text:  # accessing all words in a query
        detail = term_details[word]
        df = detail["df"]
        for doc in detail["matching_docs"]:  # accessing all matching docs in term detail dict
            docid = doc
            tf = detail["matching_docs"][doc]["tf"]
            len_d = detail["matching_docs"][doc]["length"]
            t1 = math.log((total_docs + 0.5) / (df + 0.5))
            t2 = (tf + k1 * tf) / (tf + k1 * ((1 - b) + b * len_d / avg_length))
            tf_wq = query_dict[query_num].count(word)
            t3 = (tf_wq + k2 * tf_wq) / (tf_wq + k2)
            bm25_score = t1 * t2 * t3
            if docid in bm25_doc_score:
                bm25_doc_score[docid] = bm25_score + bm25_doc_score[docid]
            else:
                bm25_doc_score[docid] = bm25_score
    ranked_docs_bm25 = _sort_doc_by_score(bm25_doc_score, 200)
    return ranked_docs_bm25


def _analyse_esbuiltin_score(query_num):
    esbuiltin_doc_score = {}
    query = ""
    query_words = len(query_dict[query_num])
    for i in range(0, query_words):  # accessing all words in the query
        query = query + " " + str(query_dict[query_num][i])

    query_body = {
        "query": {
            "match": {
                "text": {
                    "query": query
                }
            }
        },
        "track_total_hits": True,
        "track_scores": True,
        "size": 2000,
        "_source": ["docno"]
    }
    res = es.search(index=INDEX_NAME, body=query_body)["hits"]["hits"]
    hits = len(res)
    for i in range(0, hits):
        score = res[i]["_score"]
        doc_no = res[i]["_id"]
        esbuiltin_doc_score[doc_no] = score
    ranked_docs_esbuiltin = _sort_doc_by_score(esbuiltin_doc_score, 200)
    return ranked_docs_esbuiltin


def execute_query(query_no):
    print("Processing query " + query_no + str(query_dict[query_no]))
    query_docs = {}  # contains all matching docs with length for the query

    for term in query_dict[query_no]:  # form each term in the query
        if term not in term_details:  # check if term is already processed
            term_details[term], query_docs = _get_term_details(term,
                                                               query_docs)  # get DF, TF, TTF and matching docs for a particular term

    print(len(term_details))
    print("rank all matching docs based on a scoring function")
    result_bm25[query_no] = _calculate_bm25_score(query_no)
    result_esbuiltin[query_no] = _analyse_esbuiltin_score(query_no)
    return result_bm25, result_esbuiltin


def get_doc_info(doc):
    query_body = {
        "fields": ["text"],
        "offsets": False,
        "payloads": False,
        "positions": False,
        "term_statistics": True,
        "field_statistics": True
    }
    length = 0
    query = es.termvectors(index=INDEX_NAME, id=doc, body=query_body)
    if "term_vectors" in query.keys():  # checking that doc exists in dump
        if bool(query["term_vectors"]):  # checking that term vector dictionary is not empty
            query = query["term_vectors"]
            if bool(query["text"]):  # checking that text dictionary is not empty
                query = query["text"]
                if len(query["terms"]) > 0:
                    for term in query["terms"]:
                        length = length + query["terms"][term]["term_freq"]
            else:
                print(doc + "text{} is empty... seems no text")
    return length


# def get_length_all_docs(final):
#     doc_length_dict = {}
#     total_length = 0
#
#     query_body = {
#         "query": {
#             "match_all": {}
#         },
#         "size": 100,
#         "_source": []
#     }
#     res = es.search(index=INDEX_NAME, body=query_body, scroll='10m')
#     scroll_id = res["_scroll_id"]
#
#     # analysing each  document
#     count = 0
#     for j in range(1, 1102):
#         print(j)
#         num_docs = len(res["hits"]["hits"])
#         for k in range(0, num_docs):
#             doc = res["hits"]["hits"][k]["_id"]
#             if doc not in final:
#                 doc_length_dict[doc] = get_doc_info(doc)
#
#         count += 100
#         if count % 10000 == 0 and count > 100000:
#             print(count)
#             print(len(doc_length_dict))
#             file = open(PICKLE_DOCLENGTH_DICT_FILE_LOCATION+"_"+str(count/10000), 'wb')
#             pickle.dump(doc_length_dict, file)
#             doc_length_dict = dict()
#             print("Saved doc length info on a file")
#             file.close()
#         res = es.scroll(scroll_id=scroll_id, scroll='10m')


if __name__ == '__main__':
    doc_length_file = open(PICKLE_DOCLENGTH_DICT_FILE_LOCATION, 'rb')
    doc_length_dict = pickle.load(doc_length_file)
    print(len(doc_length_dict))
    doc_length_file.close()

    total_docs = 109215
    print("Total docs " + str(total_docs))

    total_length = 0
    for doc in doc_length_dict:
        total_length += doc_length_dict[doc]

    print(total_length)
    avg_length = total_length / total_docs
    print("Average length " + str(avg_length))

    with open(STOPWORDS_FILE, mode='r') as stoplist:
        stopwords = stoplist.read().splitlines()

    print("Reading queries")
    query_dict = _read_queries()  # query_dict[query_no] has list of all refined query words

    result_bm25 = {}
    result_esbuiltin = {}

    for query_no in query_dict:  # execute each query abd get result for each model
        result_bm25, result_esbuiltin = execute_query(query_no)

    _write_result(result_bm25, RESULT_BM25_FILE)
    _write_result(result_esbuiltin, RESULT_ESBUILTIN_FILE)

    # doc_length_file = open("./docLengthFinal.pickle", 'rb')
    # d1 = pickle.load(doc_length_file)
    # print(len(d1))
    # doc_length_file.close()
    #
    # doc_length_file = open("./docLength5.pickle_11.0", 'rb')
    # d2 = pickle.load(doc_length_file)
    # print(len(d2))
    # doc_length_file.close()
    #
    # final = dict()
    # for url in d1:
    #     if url not in final:
    #         final[url] = d1[url]
    # for url in d2:
    #     if url not in final:
    #         final[url] = d2[url]
    #
    # print(len(final))
    # file = open("./docLengthFinal.pickle", 'wb')
    # pickle.dump(final, file)
    # exit()
    #
    # doc_length_file = open("./docLengthFinal.pickle", 'rb')
    # final = pickle.load(doc_length_file)
    # print(len(final))
    # doc_length_file.close()
    #
    # get_length_all_docs(final)
    # exit()
