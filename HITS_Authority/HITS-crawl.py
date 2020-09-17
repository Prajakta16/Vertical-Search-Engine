import math
import random

from EsIndex import Index, MARITIME_INLINKS, MARITIME_OUTLINKS
from multiprocessing.context import Process

TOP500_AUTHORITY_PAGES = "./results/hitstop500authorities"
TOP500_HUB_PAGES = "./results/hitstop500hubs"

TOP500_SALSA_HUB_PAGES = "./results/salsatop500hubs"
TOP500_SALSA_AUTH_PAGES = "./results/salsatop500authorities"
temp_salsa_hub_scores = dict()


def getPerplexity(score_dict):
    """
    entropy H(x) = summation_(i=0 to n-1) (p_i log(p_i))
    """
    entropy = 0
    try:
        for value in score_dict.values():  # Finding the Shannon entropy for each inlink
            try:
                entropy += - (value * math.log(value, 2))
            except Exception:
                pass
        perplexity = math.pow(2, entropy)  # Calulating the perplexity
        return perplexity
    except ValueError:
        print("Error while calculating perplexity")
        return 0


def read_file(file):
    links = dict()

    print("Reading file " + file)
    with open(file, mode='r') as filee:
        all_lines = filee.read().split("\n")
    filee.close()
    # print("Length of file is "+str(len(all_lines)))

    for line in all_lines:
        all_links_in_line = line.split(" ")
        count_other_links = len(all_links_in_line) - 1
        page_id = all_links_in_line[0].strip()

        if count_other_links == 0:  # no inlinks/outlinks - represents either root or sink
            links[page_id] = set()
        else:
            for i in range(1, count_other_links + 1):
                if page_id not in ['']:
                    if page_id in links:
                        links[page_id].add(all_links_in_line[i].strip())
                    else:
                        links[page_id] = set()
                        links[page_id].add(all_links_in_line[i].strip())
    return links


def getInitialQueryResult():
    rootset = dict()
    query_body = {
        "query": {
            "match": {
                "text": {
                    "query": "maritime accident"
                }
            }
        },
        "track_total_hits": False,
        "track_scores": True,
        "size": 1000,
        "_source": ["id"],
        "explain": False
    }
    maritime_index = Index()
    res = maritime_index.es.search(index=maritime_index.INDEX_NAME, body=query_body)

    for i in range(0, 1000):
        rootset[res["hits"]["hits"][i]["_source"]["id"]] = 1
    # print(rootset["http://en.wikipedia.org/wiki/Accident_Investigation_Board_Norway"])

    return rootset


def expandRootSet(inlinks, outlinks, rootSet):
    for i in range(2, 3):
        tempRootSet = dict()
        for page_id in rootSet:
            if rootSet[page_id] == i - 1:
                if page_id in outlinks:
                    for outgoing_link in outlinks[page_id]:
                        if outgoing_link in outlinks:  # indicates crawled
                            tempRootSet[outgoing_link] = i
                else:
                    print(page_id + " does not exist in outlinks")
        # print("Added " + str(len(tempRootSet)) + " outlinks for all pages in rootset")

        countPages = 0
        for page_id in rootSet:
            DInlinkstempRootSet = dict()
            countPages += 1
            if rootSet[page_id] == i - 1:
                if page_id in inlinks:
                    for incoming_link in inlinks[page_id]:
                        if incoming_link in outlinks:  # check if crawled
                            DInlinkstempRootSet[incoming_link] = 1
                else:
                    print(page_id + " does not exist in inlinks")

            # print("length of new links is " + str(len(DInlinkstempRootSet)))
            if len(DInlinkstempRootSet) <= 200:
                tempRootSet.update(DInlinkstempRootSet)
            else:
                randomSet = set()
                while len(randomSet) <= 200:
                    random_key = random.choice(list(DInlinkstempRootSet.keys()))
                    if random_key in outlinks:  # crawled
                        randomSet.add(random_key)
                for link in randomSet:
                    tempRootSet[link] = 1

        # print("Added 200 inlinks for each page in rootset")
        rootSet.update(tempRootSet)
        print("After iteration " + str(i) + " length of root set is " + str(len(rootSet)))
        if len(rootSet) > 20000:
            break
        i += 1
    return rootSet


def compute_HITS(inlinks, outlinks, rootSet):
    authority_scores = dict()
    hub_scores = dict()

    for page_id in rootSet:
        authority_scores[page_id] = 1
        hub_scores[page_id] = 1

    prevPerpAuth = getPerplexity(authority_scores)
    prevPerpHub = getPerplexity(hub_scores)
    print("Perplexity of initial auth scores is " + str(prevPerpAuth))
    print("Perplexity of initial hub scores is "+str(prevPerpHub))

    for iter in range(1, 11):
        print("Round "+str(iter))
        normalization = 0
        for page_id in rootSet:
            authority_scores[page_id] = 0
            if page_id in inlinks:
                for incoming_link in inlinks[page_id]:
                    if incoming_link in hub_scores:
                        authority_scores[page_id] += hub_scores[incoming_link]
                    # else:
                    #     print(page_id + " not in hub score dict")
            else:
                print(page_id + " not in inlinks dict")
            normalization += pow(authority_scores[page_id], 2)
        normalization = math.sqrt(normalization)
        for page_id in rootSet:
            authority_scores[page_id] = authority_scores[page_id] / normalization

        normalization = 0
        for page_id in rootSet:
            hub_scores[page_id] = 0
            if page_id in outlinks:
                for outgoing_link in outlinks[page_id]:
                    if outgoing_link in authority_scores:
                        hub_scores[page_id] += authority_scores[outgoing_link]
                    # else:
                    #     print(outgoing_link + " not in auth score dict")
            else:
                print(page_id + " not in outloinks dict")
            normalization += pow(hub_scores[page_id], 2)
        normalization = math.sqrt(normalization)
        for page_id in rootSet:
            hub_scores[page_id] = hub_scores[page_id] / normalization

        newPerpAuth = getPerplexity(authority_scores)
        newPerpHub = getPerplexity(hub_scores)
        print("Perplexity of new auth scores is "+str(newPerpAuth))
        print("Perplexity of new hub scores is "+str(newPerpHub))

        if abs(prevPerpAuth - newPerpAuth) < 0.000001 and abs(prevPerpHub - newPerpHub) < 0.000001:
            iter += 1
        prevPerpAuth = newPerpAuth
        prevPerpHub = newPerpHub

    return authority_scores, hub_scores


def write_top500_score(score_dict, output_file):
    print("Writing results for " + output_file)
    sorted_score_dict = sorted(score_dict.items(), reverse=True, key=lambda x: x[1])[:500]
    with open(output_file, mode='w') as file:
        for page_id in sorted_score_dict:
            file.write(str(page_id) + "\n")  # + "\t" + str(sorted_score_dict[page_id]
    file.close()


def multi_scoring(page_list, inlinks, inlinks_count, outlinks, outlinks_count, temp_salsa_hub_scores, salsa_hub_scores):
    for page_id in page_list:
        for v in outlinks[page_id]:
            if v in inlinks and inlinks_count[v] > 0:
                # print(inlinks_count[v])
                for w in inlinks[v]:
                    if w in outlinks and w in salsa_hub_scores and outlinks_count[w] > 0:
                        # print(outlinks_count[w])
                        # print(salsa_hub_scores[w])
                        if page_id not in temp_salsa_hub_scores:
                            temp_salsa_hub_scores[page_id] = salsa_hub_scores[w] / (
                                    inlinks_count[v] * outlinks_count[w])
                        else:
                            temp_salsa_hub_scores[page_id] += salsa_hub_scores[w] / (
                                    inlinks_count[v] * outlinks_count[w])


def compute_hub_SALSA(inlinks, inlinks_count, outlinks, outlinks_count, rootSet):
    print("Size of rootset is " + str(len(rootSet)))
    # computing hub scores
    salsa_hub_scores = dict()

    countBaseSetWithOutlinks = 0
    for page_id in rootSet:
        if page_id in outlinks:
            if len(outlinks[page_id]) > 0:
                countBaseSetWithOutlinks += 1
    print(countBaseSetWithOutlinks)

    for page_id in rootSet:
        if page_id in outlinks:
            if len(outlinks[page_id]) > 0:
                salsa_hub_scores[page_id] = 1 / countBaseSetWithOutlinks
            else:
                salsa_hub_scores[page_id] = 0
        else:
            salsa_hub_scores[page_id] = 0

    for iter in range(0, 3):
        print(iter)
        global temp_salsa_hub_scores
        count = 0

        page_list = list()
        procs = list()
        for page_id in rootSet:
            page_list.append(page_id)
            count += 1
            if count % 100 == 0:
                print(count)
                proc = Process(target=multi_scoring, args=(
                page_list, inlinks, inlinks_count, outlinks, outlinks_count, temp_salsa_hub_scores, salsa_hub_scores,))
                page_list = list()
                procs.append(proc)
                proc.start()

        print("Total processes  " + str(len(procs)))
        for proc in procs:
            proc.join()

        for page_id in temp_salsa_hub_scores:
            salsa_hub_scores[page_id] = temp_salsa_hub_scores[page_id]
        temp_salsa_hub_scores = dict()
    return salsa_hub_scores


def compute_auth_SALSA(inlinks, outlinks, rootSet):
    # computing auth scores
    salsa_auth_scores = dict()

    countBaseSetWithInlinks = 0
    for page_id in rootSet:
        if page_id in inlinks:
            if len(inlinks[page_id]) > 0:
                countBaseSetWithInlinks += 1

    for page_id in rootSet:
        if page_id in inlinks:
            if len(inlinks[page_id]) > 0:
                salsa_auth_scores[page_id] = 1 / countBaseSetWithInlinks
            else:
                salsa_auth_scores[page_id] = 0
        else:
            salsa_auth_scores[page_id] = 0

    for iter in range(0, 5):
        print(iter)
        temp_salsa_auth_scores = dict()
        for page_id in rootSet:
            if page_id in inlinks:
                for v in inlinks[page_id]:
                    if v in outlinks:
                        len_outlinks_v = len(outlinks[v])
                        print(len_outlinks_v)
                        if len_outlinks_v != 0:
                            for w in outlinks[v]:
                                if w in inlinks:
                                    len_inlinks_w = len(inlinks[w])
                                    print(len_inlinks_w)
                                    if len_inlinks_w != 0:
                                        if w not in salsa_auth_scores:
                                            salsa_auth_scores[w] = 0
                                        if page_id in temp_salsa_auth_scores:
                                            temp_salsa_auth_scores[page_id] += salsa_auth_scores[w] / (
                                                        len_outlinks_v * len_inlinks_w)
                                        else:
                                            temp_salsa_auth_scores[page_id] = salsa_auth_scores[w] / (
                                                        len_outlinks_v * len_inlinks_w)
        print(len(temp_salsa_auth_scores))

        for page_id in rootSet:
            if page_id in temp_salsa_auth_scores:
                salsa_auth_scores[page_id] = temp_salsa_auth_scores[page_id]
    return salsa_auth_scores


if __name__ == '__main__':
    rootSet = getInitialQueryResult()  # According to Kleinberg the reason for constructing a base set is to ensure that most (or many) of the strongest authorities are included.
    print(len(rootSet))

    inlinks = read_file(MARITIME_INLINKS)
    outlinks = read_file(MARITIME_OUTLINKS)

    print(len(inlinks))
    print(len(outlinks))

    rootSet = expandRootSet(inlinks, outlinks, rootSet)

    authority_scores, hub_scores = compute_HITS(inlinks, outlinks, rootSet)
    write_top500_score(authority_scores, TOP500_AUTHORITY_PAGES)
    write_top500_score(hub_scores, TOP500_HUB_PAGES)

    # inlinks_count = dict()
    # outlinks_count = dict()
    # for page_id in inlinks:
    #     inlinks_count[page_id] = len(inlinks[page_id])
    # for page_id in outlinks:
    #     outlinks_count[page_id] = len(outlinks[page_id])
    #
    # salsa_hub_scores = compute_hub_SALSA(inlinks, inlinks_count, outlinks, outlinks_count, rootSet)
    # write_top500_score(salsa_hub_scores, TOP500_SALSA_HUB_PAGES)
    # salsa_authority_scores = compute_auth_SALSA(inlinks, outlinks, rootSet)
    # write_top500_score(salsa_authority_scores, TOP500_SALSA_AUTH_PAGES)
