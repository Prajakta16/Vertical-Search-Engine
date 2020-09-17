import os
import pickle

from bs4 import BeautifulSoup
from html_reader import get_canonical_form, get_base_url
from mergeIndex import read_data

FINAL_INLINKS_FILE = "./final_inlinks"
FINAL_OUTLINKS_FILE = "./final_outlinks"
FINAL_OUTLINKS_FILE2 = "./final_outlinks2"
PARTIAL_INDEXING_FOLDER = "./partial_indexing_data/"
TEMP_FOLDER = "./temp_data/"
SYNCED_INDEXING_FOLDER = "./synced_indexed_folder/"
CRAWLED_URLS_FILE = "./crawled_urls"
SJ_CRAWL_URLS_FILE = "./sj-crawled"
PS_CRAWL_URLS_FILE = "./info.pickle"


def read_outlinks(batch_no, batch_size, outlinks):
    start = ((batch_no - 1) * batch_size / 100) + 1  # docs crawled in last batch + 1
    end = start + batch_size / 100
    while start < end:
        # print("Reading from file " + str(start))
        filepath = open(os.path.join(PARTIAL_INDEXING_FOLDER, str(start)), 'rb')
        print(filepath)
        new_dict = pickle.load(filepath)
        filepath.close()

        for doc in new_dict:
            outlinks[doc] = set()
            if "raw_html" in new_dict[doc]:
                rawhtml = new_dict[doc]["raw_html"]
                soup = BeautifulSoup(rawhtml, "html.parser")
                unique_outlinks = set(soup.find_all('a'))
                for link in unique_outlinks:
                    if link.get('href') and not link.get('href').startswith("#"):
                        web_link = link.get('href')
                        web_link = get_canonical_form(web_link, doc)
                        outlinks[doc].add(web_link)
            else:
                print("Unable to access raw html for " + doc)

        for doc in new_dict:
            new_dict[doc]["outlinks"] = outlinks[doc]
            if "raw_html" in new_dict[doc]:
                del new_dict[doc]["raw_html"]

        print(TEMP_FOLDER + "" + str(start) + ".0")
        with open(TEMP_FOLDER + "" + str(start) + ".0", mode='wb') as file:
            pickle.dump(new_dict, file)
        file.close()

        filepath = open(FINAL_OUTLINKS_FILE2, 'wb')
        pickle.dump(outlinks, filepath)
        filepath.close()

        # data_to_be_indexed.update(new_dict)  # merging existing data with new data
        start += 1
    return outlinks


def sync_inlinks_outlinks(inlinks, crawl_hist_dict, sj_crawl_hist_dict, ps_crawl_hist_dict):
    batch_size = 100
    total_batches = int(40000 / batch_size)

    count = 0
    for i in range(1, total_batches + 2):  # total_batches + 1
        print("Processing batch " + str(i) + "of docs ")
        data = read_data(i, batch_size, TEMP_FOLDER, ".0.0")

        for doc in data:
            if "headers" in data[doc]:
                del data[doc]["headers"]
            try:
                if doc in inlinks and doc in crawl_hist_dict:  # check doc exists in inlinks dict, otherwise do nothing
                    data[doc]["inlinks"] = inlinks[doc]
                else:
                    print(""+doc+" does not exist")
            except Exception:
                print("Error to sync inlinks")
                pass
            # Conversion to domain names for uncrawled urls
            # print(data[doc]["outlinks"]) #set
            # curr_outlinks = data[doc]["outlinks"]
            new_domains = set()
            red_urls = set()
            # print("---------------- Converting redundant urls to domain names ----------")
            # print("For " + doc + " "+str(len(data[doc]["outlinks"])))
            for link in data[doc]["outlinks"]:
                try:
                    if link not in crawl_hist_dict:
                        if link not in sj_crawl_hist_dict:
                            if link not in ps_crawl_hist_dict:
                                domain_url = get_base_url(link)
                                # print("Converting " + link + " to domain " + domain_url)
                                red_urls.add(link)
                                new_domains.add(domain_url)
                except Exception:
                    print("Error to convert unused outlink " + link + " to domain name")
                    pass
            # print(red_urls)
            # print(new_domains)
            # print("Red urls for "+doc+" = "+str(len(red_urls)) +" out of total of "+str(len(data[doc]["outlinks"])))
            for url in red_urls:
                data[doc]["outlinks"].remove(url)
            # print("Remove red -- "+str(len(data[doc]["outlinks"])))
            data[doc]["outlinks"].update(new_domains)
            # print("For " + doc + " "+str(len(data[doc]["outlinks"])))

        check = "http://en.wikipedia.org/wiki/List_of_maritime_disasters"
        if check in data:
            print(data[check]["inlinks"])
            print(data[check]["outlinks"])

        with open(SYNCED_INDEXING_FOLDER + "" + str(i), mode='wb') as file:
            print("------Writing data with synced inlinks to be indexed in file------")
            print(len(data))
            pickle.dump(data, file)
        file.close()

        if len(data[doc]["inlinks"]) > 800:
            count += 1

    print(count)
    print("----------Sync complete-----------")


def get_all_inlinks(crawl_hist_dict, outlinks):
    batch_size = 1000
    total_batches = int(40000 / batch_size)
    in_links = {}

    print("------Acquiring newly added inlinks for an url after it was crawled------")
    count = 0
    for i in range(1, total_batches + 2):  # total_batches + 1
        print("Processing batch " + str(i))
        indexed_data_dict = read_data(i, batch_size, TEMP_FOLDER, ".0.0")
        print("Read " + str(len(indexed_data_dict)) + " docs for process")
        try:
            for doc in indexed_data_dict:
                try:
                    for link in indexed_data_dict[doc]["outlinks"]:
                        if link in outlinks[doc]:
                            if len(indexed_data_dict[doc]["outlinks"]) != len(outlinks[doc]):
                                print("Lenths do not sync")
                                print(len(indexed_data_dict[doc]["outlinks"]))
                                print(len(outlinks[link]))
                            else:
                                if link in crawl_hist_dict.keys() and link in outlinks.keys():  # crawled
                                    if link in in_links.keys():
                                        if doc in crawl_hist_dict.keys():
                                            in_links[link].add(doc)
                                            # print("Adding " + doc + " to inlinks of " + link)
                                        else:
                                            print(doc + " not in crawl his, cannot be added to inlinks of " + link)
                                    else:
                                        # print("Adding "+link+" to inlinks dictionary")
                                        in_links[link] = set()
                                        in_links[link].add(doc)
                                else:
                                    count += 1
                except Exception:
                    pass
        except Exception:
            pass
        print("Inlinks processed " + str(len(in_links)))
        print("Ignored " + str(count) + " inlinks as they were not crawled")
    # Backup
    with open(FINAL_INLINKS_FILE, mode='wb') as file:
        print("------Writing final inlinks ------")
        pickle.dump(in_links, file)
    file.close()
    return in_links


def verify_sync(inlinks):
    batch_size = 100
    total_batches = int(40000 / batch_size)

    count = 0
    for i in range(1, total_batches + 2):  # total_batches + 1
        print("Processing batch " + str(i) + "of docs ")
        data = read_data(i, batch_size, SYNCED_INDEXING_FOLDER, "")

        for doc in data:
            try:
                if doc in inlinks and doc in crawl_hist_dict:  # check doc exists in inlinks dict, otherwise do nothing
                    if len(data[doc]["inlinks"]) != len(inlinks[doc]):
                        print("Inlink count does not match for "+doc)
                else:
                    print("" + doc + " does not exist")
                    print(data[doc]["inlinks"])
            except Exception:
                print("Error to sync inlinks")
                pass
        #     finally:
        #         if len(data[doc]["inlinks"]) > 800:
        #             # print(doc)
        #             count += 1
        # print(count)
    print("----------verify Sync complete-----------")


if __name__ == '__main__':
    batch_size = 100
    total_batches = int(40000 / batch_size)

    # 1. As outlinks were not accounted initially, this code parsed the war_html and identified outlinks
    # print("------Get outlinks ------")
    # outlinks = {}
    # for i in range(1, total_batches+2):
    #     print("Processing batch " + str(i))
    #     outlinks = read_outlinks(i, batch_size, outlinks)
    #     print(len(outlinks))
    #
    # print("------Writing final outlinks ------")
    # filepath = open(FINAL_OUTLINKS_FILE, 'wb')
    # pickle.dump(outlinks, filepath)
    # filepath.close()

    # While crawling the web, few documents which were later crawled and had outlinks
    # as prev crawled docs were missed as inlinks in prev crawled doc. This function helps to sync all the outlinks with the inlink

    with open(CRAWLED_URLS_FILE, mode='r') as file:
        crawl_history = file.readlines()
    file.close()

    crawl_hist_dict = {}
    for line in crawl_history:
        crawl_hist_dict[line.split(" ")[0]] = 1
    print("Length of crawl hist " + str(len(crawl_hist_dict)))
    print(crawl_hist_dict["https://www.bbc.com/news/world-asia-39361944"])

    filepath = open(FINAL_OUTLINKS_FILE, 'rb')
    outlinks = pickle.load(filepath)
    filepath.close()
    print("Length of outlinks " + str(len(outlinks)))

    # count = 0
    # for link in outlinks:
    #     for outgoing_link in outlinks[link]:
    #         if outgoing_link == "https://www.nytimes.com/2019/06/10/world/asia/sewol-ferry-accident.html":
    #             print(link + "has outgoin link to bbc")
    # exit()

    print("------Get inlinks ------")
    inlinks = get_all_inlinks(crawl_hist_dict, outlinks)

    # filepath = open(FINAL_INLINKS_FILE, 'rb')
    # inlinks = pickle.load(filepath)
    # filepath.close()
    print("Length of inlinks " + str(len(inlinks)))  # check
    print(inlinks["http://en.wikipedia.org/wiki/List_of_maritime_disasters"])  # check
    # print(len(inlinks["http://en.wikipedia.org/wiki/France"]))
    # for key in inlinks:
    #     if len(inlinks[key]) > 3000:
    #         print(key)
    # count = 0
    # for url in inlinks:
    #     if len(inlinks[url]) > 800:
    #         count += 1
    #         print(url)
    # print(count)
    # exit()
    # print(inlinks["http://en.wikipedia.org/wiki/Buddy_diving"])  # check
    # print(inlinks["http://en.wikipedia.org/wiki/Apostles"])
    #
    # for incoming_link in inlinks["http://en.wikipedia.org/wiki/Apostles"]:
    #     print(incoming_link+"\n")
    # countCorrect = 0
    # countWrong = 0
    # for url in inlinks:
    #     for incoming_link in inlinks[url]:
    #         if incoming_link in crawl_hist_dict:
    #             if incoming_link in outlinks.keys():
    #                 if url in outlinks[incoming_link]:
    #                     countCorrect += 1
    #                 else:
    #                     countWrong += 1
    #                     print(url)
    #                     print(incoming_link)
    #             else:
    #                 countWrong += 1
    #                 print(url)
    #                 print(incoming_link)
    #         else:
    #             print(""+incoming_link+" not crawled")
    # print(countCorrect)
    # print(countWrong)
    # exit()

    filepath = open(SJ_CRAWL_URLS_FILE, 'rb')
    content = pickle.load(filepath)
    filepath.close()

    sj_crawl_hist_dict = {}
    for url in content["crawled"]:
        sj_crawl_hist_dict[url.replace("https", "http")] = 1
    print("SJ history "+str(len(sj_crawl_hist_dict)))

    filepath = open(PS_CRAWL_URLS_FILE, 'rb')
    content = pickle.load(filepath)
    filepath.close()

    ps_crawl_hist_dict = {}
    for entry in content:
        ps_crawl_hist_dict[entry[0].replace("https", "http")] = 1
    print("PS history "+str(len(ps_crawl_hist_dict)))

    print("------Sync all inlink outlinks------")
    sync_inlinks_outlinks(inlinks, crawl_hist_dict, sj_crawl_hist_dict, ps_crawl_hist_dict)

    # print("------Verify sync------")
    # verify_sync(inlinks)

    # count = 0
    # for url in sj_crawl_hist_dict.keys():
    #     if url in crawl_hist_dict or url in ps_crawl_hist_dict:
    #         count += 1
    #         print(url)
    # print(count)

