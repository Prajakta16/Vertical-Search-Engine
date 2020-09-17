import math
import pickle

from EsIndex import MARITIME_INLINKS, MARITIME_OUTLINKS, MARITIME_CORPUS

W2G_INLINKS = "./input/wt2g_inlinks.txt"
PAGE_RANK_CRAWL = "./results/pageRankcrawl"
PAGE_RANK_WT2G = "./results/pageRankWT2G"
w2gFlag = False


def read_file(file):
    links = dict()

    print("Reading file " + file)
    with open(file, mode='r') as filee:
        all_lines = filee.read().split("\n")
    filee.close()

    count = 0
    for line in all_lines:
        all_links_in_line = line.split(" ")
        count_other_links = len(all_links_in_line) - 1
        page_id = all_links_in_line[0].strip()

        if count_other_links == 0:  # no inlinks/outlinks - represents either root or sink
            count += 1
            links[page_id] = set()
        else:
            for i in range(1, count_other_links + 1):
                if page_id not in ['']:
                    if page_id not in links:
                        links[page_id] = set()
                    links[page_id].add(all_links_in_line[i].strip())
    # print("There are count = "+str(count)+" pages with no neighbouring links")
    return links


def getPerplexity(PR):
    """
    entropy H(x) = summation_(i=0 to n-1) (p_i log(p_i))
    """
    entropy = 0
    try:
        for value in PR.values():  # Finding the Shannon entropy for each inlink
            entropy += - (value * math.log(value, 2))
        perplexity = math.pow(2, entropy)  # Calulating the perplexity
        return perplexity
    except ValueError:
        print("Error while calculating perplexity")
        return 0


def getOutlinksFromInlinks(inlinks):
    count = 0
    new_outlinks = dict()

    for page_id in inlinks:
        if len(inlinks[page_id]) > 0:
            for incoming_link in inlinks[page_id]:
                if incoming_link not in inlinks:
                    count += 1
                if incoming_link not in new_outlinks:
                    new_outlinks[incoming_link] = set()
                new_outlinks[incoming_link].add(page_id)
    # print(len(new_outlinks))
    # print("There are " + str(count) + " outlinks which are not page ids in the corpus")

    for page_id in inlinks:
        if page_id not in new_outlinks:
            new_outlinks[page_id] = set()
    # print(len(new_outlinks))
    return new_outlinks


def getCorpusFromInlinks(inlinks):
    new_corpus = dict()
    for page_id in inlinks.keys():
        new_corpus[page_id] = 1
    return new_corpus


def write_top500_score(score_dict, output_file, inlink_dict, outlink_dict):
    sorted_score_dict = sorted(score_dict.items(), reverse=True, key=lambda x: x[1])[:500]
    with open(output_file, mode='w') as file:
        for page_id in sorted_score_dict:
            if w2gFlag:
                if page_id[0] not in outlink_dict:
                    outlink_dict[page_id[0]] = set()
                if page_id[0] not in inlink_dict:
                    inlink_dict[page_id[0]] = set()
                file.write(page_id[0] + "\t" + str(page_id[1]) + "\tinlinks " + str(
                    len(inlink_dict[page_id[0]])) + "\toutlinks " + str(len(outlink_dict[page_id[0]])) + "\n")
            else:
                file.write(page_id[0] + "\t" + str(page_id[1]) + "\n")
    file.close()


if __name__ == '__main__':
    PR = dict()
    sink = dict()
    new_PR = dict()
    d = 0.85
    i = 0
    convergenceIteration = 8

    if w2gFlag:
        inlinks = read_file(W2G_INLINKS)
        outlinks = getOutlinksFromInlinks(inlinks)
        corpus = getCorpusFromInlinks(inlinks)
    else:
        corpus = dict()
        with open(MARITIME_CORPUS, mode='r') as file:
            content = file.read().split("\n")
        file.close()
        for line in content:
            corpus[line.strip()] = 1
        inlinks = read_file(MARITIME_INLINKS)
        outlinks = read_file(MARITIME_OUTLINKS)
        # outlinks2 = getOutlinksFromInlinks(inlinks)
        # outlinks.update(outlinks2)

    print("corpus "+str(len(corpus)))
    print(len(inlinks))
    print(len(outlinks))

    # pickle.dump(inlinks, open("./input/inlinks.pickle", "wb"))
    # pickle.dump(outlinks, open("./input/outlinks.pickle", "wb"))

    # for link in inlinks:
    #     if link not in corpus:
    #         print(link+" does not exist in corpus dict")
    #     if link not in outlinks:
    #         print(link+" does not exist in outlink dict")
    # print("-----check 1 -------")
    # for link in outlinks:
    #     if link not in corpus:
    #         print(link+" does not exist in corpus dict")
    #     if link not in inlinks:
    #         print(link+" does not exist in inlink dict")
    # print("-----check 2 -------")
    # for link in corpus:
    #     if link not in outlinks:
    #         print(link + " does not exist in outlink dict")
    #     if link not in inlinks:
    #         print(link + " does not exist in inlink dict")
    # print("-----check 3 -------")
    #
    # count = 0
    # for link in inlinks:
    #     for incoming_link in inlinks[link]:
    #         if incoming_link not in corpus:
    #             count += 1
    #             print(link+" does not exist in corpus dict")
    # print("-----check 4 -------")
    # print(count)
    # exit()

    # inlinks = pickle.load(open("./inlinks.pickle", "rb"))
    # print(len(inlinks))
    # outlinks = pickle.load(open("outlinks.pickle", "rb"))
    # print(len(outlinks))

    for page_id in corpus.keys():
        if len(outlinks[page_id]) == 0:
            sink[page_id] = 1
    print("Number of sink nodes = " + str(len(sink)))

    N = len(corpus)
    initial_PR = float(1 / N)
    print("Initial page rank value = " + str(initial_PR))

    for page_id in corpus.keys():
        PR[page_id] = initial_PR

    prevPerplexity = getPerplexity(PR)
    print("Initial perplexity = " + str(prevPerplexity))

    while i < convergenceIteration:
        print("iter " + str(i))
        sink_PR = 0
        for page_id in sink.keys():
            sink_PR += PR[page_id]

        for page_id in corpus.keys():
            new_PR[page_id] = float((1 - d) / N + (d * sink_PR) / N)
            if page_id not in inlinks:
                print("No inlink exists for " + page_id)
            else:
                for link in inlinks[page_id]:
                    if link not in [''] and link not in sink.keys():
                        if link in outlinks:
                            new_PR[page_id] += d * PR[link] / len(outlinks[link])
                        # else:
                        #     print(link+" does not exist in outlinks")

        for page_id in new_PR.keys():
            if page_id in PR:
                PR[page_id] = new_PR[page_id]
            else:
                print("Error for " + page_id)

        newPerplexity = getPerplexity(PR)
        print(newPerplexity)
        print("perplexity for i= " + str(i) + " = " + str(newPerplexity))

        # Check if the change in the perplexity is less than 1
        if abs(prevPerplexity - newPerplexity) < 1:
            i += 1
        prevPerplexity = newPerplexity  # Update the prev perplexity

    print("Writing results")
    if w2gFlag:
        write_top500_score(PR, PAGE_RANK_WT2G, inlinks, outlinks)
    else:
        write_top500_score(PR, PAGE_RANK_CRAWL, inlinks, outlinks)
