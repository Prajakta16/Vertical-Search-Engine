import os
import pickle
import time

from CustomTimeoutRobotFileParser import CustomRobotFileParser
from helper import preprocessing
from html_reader import HtmlReader, get_base_url, get_canonical_form
from pqueue import PQueue

PARTIAL_INDEXING_FOLDER = "./partial_indexing_data/"
FINAL_INDEX_FILE = "./final_indexed_file"
FRONTIER_ENTRY_FINDER_DICT_FOLDER = "./frontier_entry_finder/"
FRONTIER_PQ_FOLDER = "./frontier_pqueue/"
CRAWLED_URLS_FILE = "./crawled_urls"
inlinks = {}
wave = {}
data_to_be_indexed = {}
maritime_keyword_list = []
stopword_list = []


def isAllowedByRobot(base_url):
    robotUrl = base_url + "/robots.txt"
    try:
        # print("Robot URL: {0}".format(robotUrl))
        rp = CustomRobotFileParser(robotUrl)
        rp.read()
        return rp.can_fetch("*", base_url.encode('utf-8'))
    except Exception:
        return True


def crawl_web(frontierManager):
    global wave, inlinks, data_to_be_indexed
    crawling_history = {}
    robot_allowance = {}

    count = 0
    with open(CRAWLED_URLS_FILE, mode='a') as result_file:
        while count < 40100:
            try:
                top_url, score = frontierManager.pop_task()  # get topmost element from frontier
                print("Topmost url received - " + str(top_url) + " score = " + str(score))

                if top_url not in crawling_history:
                    crawling_history[top_url] = count
                    base_url = get_base_url(top_url)
                    if base_url not in robot_allowance:
                        # print("Robot " + str(is_allowed_by_robot(base_url)))
                        robot_allowance[base_url] = isAllowedByRobot(base_url)
                    if bool(robot_allowance[
                                base_url]):  # read html file if allowed by a robot. Maintain the politeness policy
                        hr = HtmlReader(top_url)
                        try:
                            info, wave, inlinks, frontierManager = hr.read_page(wave, frontierManager, score, inlinks,
                                                                                crawling_history, maritime_keyword_list,
                                                                                stopword_list)
                            if info is not None and info is not '':  # if there is no error while reading the web page
                                data_to_be_indexed[top_url] = info  # insert url in data to be indexed
                                # Write contents into a file
                                result_file.write(top_url + " " + str(score) + "\n")
                                count += 1
                                if count % 10 == 0:
                                    print("Crawled " + str(count) + " docs")
                                    print("--- %s seconds ---" % (time.time() - start_time))
                                    if count % 100 == 0:
                                        print("Frontier manager length = " + str(
                                            len(frontierManager.pq)) + " has potential duplicates")
                                        with open(PARTIAL_INDEXING_FOLDER + "" + str(count / 100), mode='wb') as file:
                                            print("------Writing data to be indexed in file------")
                                            print(len(data_to_be_indexed))
                                            pickle.dump(data_to_be_indexed, file)
                                            data_to_be_indexed = {}
                                        file.close()
                                        with open(FRONTIER_ENTRY_FINDER_DICT_FOLDER + "" + str(count / 100),
                                                  mode='wb') as file:
                                            print("------Writing existing frontier------")
                                            print(len(frontierManager.entry_finder))
                                            pickle.dump(frontierManager.entry_finder, file)
                                        file.close()
                                        with open(FRONTIER_PQ_FOLDER + "" + str(count / 100), mode='wb') as file:
                                            print("------Writing existing frontier------")
                                            print(len(frontierManager.pq))
                                            pickle.dump(frontierManager.pq, file)
                                        file.close()
                                    time.sleep(0.5)
                            else:
                                print("Error while reading web page " + top_url)
                        except Exception:
                            print("Error while crawling url")
                            pass
                    else:
                        print("Not allowed by robot " + base_url)
                else:
                    print("Url already crawled")
            except Exception:
                pass
    result_file.close()


if __name__ == '__main__':
    start_time = time.time()
    maritime_keyword_list, stopword_list, aircraft_list = preprocessing()

    seed_urls = ['http://en.wikipedia.org/wiki/List_of_maritime_disasters',
                 'http://en.wikipedia.org/wiki/Sinking_of_the_MV_Sewol',
                 'https://www.nytimes.com/2019/06/10/world/asia/sewol-ferry-accident.html',
                 'https://www.bbc.com/news/world-asia-39361944',
                 'https://www.history.com/news/5-maritime-disasters-you-might-not-know-about']

    frontierManager = PQueue()
    score = 4
    for url in seed_urls:
        frontierManager.add_task(url, priority=-score)
        wave[url] = 1
        inlinks[url] = []

    crawl_web(frontierManager)
    print("-------------Finished crawling----------------")
    print("Crawled " + str(len(data_to_be_indexed)) + " docs")
