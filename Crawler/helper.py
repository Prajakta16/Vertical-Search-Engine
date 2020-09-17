import re
import time
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from nltk.stem.porter import PorterStemmer

import pycountry
import wikipedia

MARITIME_KEYWORDS_FILE = "topic_keywords/maritime_keywords"
MARITIME_KEYWORDS_FILE_LIST = "topic_keywords/maritime_keywords_list"
AIRCRAFT_KEYWORDS_FILE_LIST = "topic_keywords/aircraft_keywords_list"
STOPWORDS_FILE = "topic_keywords/stoplist.txt"
COUNTRY_FILE = "topic_keywords/country.txt"
stemming_flag = True
stemmer = PorterStemmer()


def preprocessing():
    with open(MARITIME_KEYWORDS_FILE, mode='r') as file:
        maritime_keyword_list = file.read().split("\n")
    file.close()

    with open(STOPWORDS_FILE, 'r', encoding='ISO-8859-1') as file:
        stopword_list = file.read().split("\n")
    file.close()

    with open(AIRCRAFT_KEYWORDS_FILE_LIST, mode='r') as file3:
        aircraft_list = file3.read().split("\n")
        aircraft_list = analyze_query(aircraft_list)
    file3.close()

    return maritime_keyword_list, stopword_list, aircraft_list


def remove_stopwords(terms):
    with open(STOPWORDS_FILE, 'r', encoding='ISO-8859-1') as file:
        stopword_list = file.read().split("\n")
    file.close()
    for j in stopword_list:
        while j in terms:
            terms.remove(j)
    return terms


def analyze_query(query):
    terms = remove_stopwords(query)
    if stemming_flag:  # stemming the terms
        stemmed_terms = []
        for t in terms:
            stemmed_term = stemmer.stem(t)
            stemmed_terms.append(stemmed_term)
        terms = stemmed_terms
    return terms


def get_url_relevance(weblink, keywords, query, maritime_keyword_list, wave_no):
    relevance_score = 1
    maritime_flag = False
    disaster_flag = False

    ship_list = ['uss', 'hms', 'rms', 'ms', 'titan', 'titanic', 'armada', 'submarin', 'ship', 'warship', 'boat',
                 'maritim', 'marin', 'cruis', 'vessel', 'seoul', 'shipwreck', 'wreckag', 'wreck', 'sink', 'sunk', 'lifeboat',
                 'submerg', 'fog', 'rocki', 'sewol', 'mv', 'naval', 'fleet', 'navy', 'liner', 'starship', 'battleship',
                 'passeng', 'voyag', 'sea', 'nautic', 'fisherman', 'distress']
    disaster_list = ['earthquak', 'fatal', 'accid', 'disast', 'death', 'rescu', 'survivor', 'rediscov', 'excav',
                     'tragedi', 'tragic', 'unidentifi', 'recov', 'helicopt', 'doom', 'collid', 'overturn', 'inquiri',
                     'catastroph', 'danger', 'lost', 'save']
    aircraft_railway_list = ['aviat', 'airbu', 'boe', 'takeoff', 'aviat', 'jet', 'engin', 'land', 'airlin', 'aerial',
                             'pilot', 'airplan', 'airway', 'malfunct', 'runway', 'aeroplan', 'plane', 'aircraft',
                             'airport', 'turbul', 'cockpit', 'debri', 'crash', 'explos', 'rail', 'railway', 'train',
                             'line', 'railroad', 'tram', 'electr', 'derail', 'wagon', 'coach']
    for word in keywords:
        if word in ship_list:
            maritime_flag = True
            relevance_score = relevance_score + 1
        elif word in maritime_keyword_list:
            maritime_flag = True
            relevance_score = relevance_score + 1
        elif word in aircraft_railway_list:
            relevance_score = relevance_score - 1

    if maritime_flag:
        for word in keywords:
            if word in disaster_list:
                disaster_flag = True
                relevance_score = relevance_score + 1.5
        if disaster_flag and keywords[0].lower() in ['category', 'list']:
            relevance_score = relevance_score + 2
    # else:
    #     if wave_no < 3:
    #         try:
    #             # summary = get_summary(weblink)
    #             # print("Reading page to check relevance for " + weblink)
    #             summ = get_wiki_summary(weblink, query)
    #             print(summ)
    #             for word in summ:
    #                 if word in ship_list:
    #                     relevance_score = relevance_score + 1
    #                 elif word in maritime_keyword_list:
    #                     relevance_score = relevance_score + 1
    #                 elif word in aircraft_railway_list:
    #                     relevance_score = relevance_score - 1
    #         except:
    #             # print("Error while getting summary")
    #             pass
    return relevance_score


def get_score(wave_no, parent_score, weblink, maritime_keyword_list, stopword_list):
    keywords, query = get_keywords_from_url(weblink, stopword_list)
    relevance_score = get_url_relevance(weblink, keywords, query, maritime_keyword_list, wave_no)

    if len(keywords) <= 2:
        try:
            country_name = keywords[0]
            if len(keywords) == 2:
                country_name = country_name + " " + keywords[1]
            if bool(pycountry.countries.search_fuzzy(country_name)):
                relevance_score = relevance_score - 1
        except Exception:
            pass

    score = 0.4 * relevance_score + 0.3 * parent_score + 0.35 * 1 / wave_no  # not considering inlnks because inlink = 1the first time we encounter a new link
    score = float("%.2f" % round(score, 2))
    # print("Relevance " + str(relevance_score) + " Parent score " + str(parent_score) + "in links " + str(
    #     no_inlinks) + "wave " + str(wave_no))
    # print("Score for " + weblink + " = " + str(score))
    return score


def get_keywords_from_url(url, stoplist):
    keywords = []
    url_text = ""

    try:
        url_array = urlparse(url)
        # ParseResult(scheme='http', netloc='www.browse-tutorials.net', path='/tutorial/get-self-base-url-appengine-urlparse',
        #             params='', query='', fragment='')
        if url_array.path not in ['', '/']:
            if url_array.path.rsplit('/', 1)[1] in ['']:
                path = url_array.path.rsplit('/', 1)[0].rsplit('/', 1)[1].rsplit('.')[0]
            else:
                path = url_array.path.replace("(", "").replace(")", "").replace("'", "").rsplit('/', 1)[1].rsplit('.')[
                    0]
        else:
            path = url_array.netloc.rsplit('.', 1)[0].split('.', 2)[1]

        if len(url_array.path.split("Category:", 1)) > 1:
            path = url_array.path.split("Category:", 1)[1]
            keywords = ['Category']
        if len(path.split('_')) > 1:
            keywords = keywords + path.split('_')
        elif len(path.split('-')) > 1:
            keywords = keywords + path.split('-')
        else:
            keywords = [path]
        keywords = analyze_query(keywords)
        for word in keywords:
            url_text = url_text + " " + word
    except Exception:
        pass
    return keywords, url_text


def get_summary(weblink):
    ten_words = []
    try:
        header = requests.get(weblink, timeout=3).headers["Content-Type"]
        if header in ['text/html', 'text/html; charset=UTF-8',
                      'text/html; charset=utf-8',
                      'text/html;charset=utf-8']:  # check if url refers to text and html docs
            try:
                time.sleep(1)
                raw_html = requests.get(weblink, timeout=2).text  # get html page content
                soup = BeautifulSoup(raw_html, features="html.parser")
                first_para = str(soup.find_all("p")[0]).replace("<p>", "").replace("</p>", "").split(
                    " ")  # finds table or paragraph
                print(first_para)
                ten_words = first_para[:10]
                ten_words = analyze_query(ten_words)
                print(ten_words)
            except Exception:
                print("Cannot read page")
                pass
    except Exception:
        print("Cannot access headers")
        pass
    return ten_words


def get_wiki_summary(weblink, query):
    try:
        summary = wikipedia.summary(query)[0:100]  # take first 100 chars
        # print(summary)
        summary = re.sub("\(.+\)", "", summary).replace(",", "").replace('-', ' ').replace('.', '').replace('"',
                                                                                                            '').lower().split(
            " ")
        summary = analyze_query(summary)
        # print(summary)
    except Exception:
        summary = []
        print("Cannot access summary for " + weblink)
    finally:
        return summary
