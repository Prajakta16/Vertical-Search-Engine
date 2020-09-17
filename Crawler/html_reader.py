import wikipedia
import re
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from url_normalize import url_normalize
from helper import get_score

CRAWLED_FOLDER = './CRAWLED_FOLDER/'


def get_base_url(url):
    try:
        url_array = urlparse(url)
        # ParseResult(scheme='http', netloc='www.browse-tutorials.net', path='/tutorial/get-self-base-url-appengine-urlparse',
        #             params='', query='', fragment='')
        base_url = url_array.scheme + "://" + url_array.netloc
    except Exception:
        return ""
    return base_url


def get_canonical_form(url, parent_url):
    try:
        can_url = url_normalize(url)

        can_url = str(can_url).replace(":80", "").replace(":443", "").replace("https", "http")
        if can_url.__contains__("#"):
            can_url = can_url.split("#")[0]

        split_array = re.split("^\.\./", can_url)
        if len(split_array) > 1:  # will execute if '../' is present at the url start
            extension = split_array[1]
            can_url = urljoin(parent_url, extension)

        if can_url.startswith("/"):
            can_url = urljoin(parent_url, can_url)

        url_array = urlparse(can_url)
        # ParseResult(scheme='http', netloc='www.browse-tutorials.net', path='/tutorial/get-self-base-url-appengine-urlparse',
        #             params='', query='', fragment='')
        base = url_array.scheme + "://" + url_array.netloc
        can_url = can_url[:len(base)].lower() + can_url[len(base):]
    except Exception:
        return ""
    return can_url


def check_link_usefulness(web_link):
    bad_re_list = ['^http://en\.wikipedia\.org/w/index\.php', '^http://en\.wikipedia\.org/wiki/Main_Page',
                   '.+wiki/Help',
                   '^http://www\.cruiselinefans.+', '.*facebook.*', '.*wikimedia.*',
                   '.+share.+', '.+/shop.*', '.+license.*', '.+/blog.*', '.+/copyright.*',
                   '^http://en\.wikipedia\.org/wiki/Wikipedia:', '.*mediawiki.*',
                   '.*language.*', '.+/wiki.+Template:.+', '.+/wiki/Special:.+', '.+wiki/Portal:.+',
                   '.+wiki/Talk:.+', '.+wiki.+talk:.+', '.*donate.*', '.*music.*', '.*login.*', '.*food.*',
                   '.*garden.*',
                   '.*defence-news.*', '.*exhibitions.*', '.*privacy.*', '.*finance.*', '.*sport.*', '.*horoscope.*',
                   '.*entertainment.*',
                   '.*user.*', '.*advertise.*', '.*watsapp.*', '.*book.*', '.*/science/.*', '.*celebrity.*',
                   '.*/travel/.*',
                   '.*style.*', '.*corona.*', '.*art.*', '.*literature.*', '.*religion.*', '.*law.*', '.*politics.*',
                   '^http://en\.m', '^http://www\.yonhapnews\.co\.kr', '^http://www\.ytn\.co\.kr/', '.*\.jpg$',
                   '.*\.JPG$',
                   '.*\.svg$', '.*\.asp$', '.*\.png$']

    if re.match('^http://www', web_link) or re.match('^http://en', web_link):
        for r in bad_re_list:
            if re.match(r, web_link):
                # print(web_link + "is a bad link")
                return False
    else:
        # print(web_link + "is a bad link")
        return False
    return True


class HtmlReader(object):

    def __init__(self, url):
        self.url = url
        self.header = ""
        self.title = ""
        self.raw_html = ""
        self.text = ""
        self.inlinks = []
        self.outlinks = []

    def read_page(self, wave, frontierManager, crawled_score, inlinks, crawling_history, maritime_keyword_list,
                  stopword_list):
        info = ''
        parent_wave = wave[self.url]
        try:
            self.header = requests.get(self.url, timeout=4).headers["Content-Type"]
            if self.header in ['text/html', 'text/html; charset=UTF-8',
                               'text/html; charset=utf-8',
                               'text/html;charset=utf-8']:  # check if url refers to text and html docs
                try:
                    self.raw_html = requests.get(self.url, timeout=4).text  # get html page content

                    soup = BeautifulSoup(self.raw_html, features="html.parser")
                    self.raw_html = soup.prettify()

                    if bool(soup.title):
                        if bool(soup.title.string):
                            self.title = soup.title.string
                            # print(self.title)

                    page_content = soup.find_all(
                        ["table", {"class": "wikitable sortable"}, "p"])  # finds table or paragraph
                    text = ""
                    for content in page_content:
                        if str(content).startswith('<p'):  # page found
                            text = text + "" + content.text
                        else:  # table found
                            table_text = ""
                            headers = content.findAll('th')
                            head_cells = []
                            for i in range(0, len(headers)):
                                head_cells.append(headers[i].text + "".lstrip().rstrip())
                            for row in content.findAll('tr'):
                                cells = row.findAll('td')
                                for i in range(0, len(cells)):
                                    if len(headers) == len(cells):
                                        table_text = table_text + head_cells[i] + ":" + (
                                                cells[i].text + "".lstrip().rstrip())
                                    else:
                                        table_text = table_text + (cells[i].text + "".lstrip().rstrip())
                            table_text = table_text.replace("\n", " ")
                            text = text + table_text + "\n "
                    self.text = text

                    formatted_output = "<DOC>\n<DOCNO>" + self.url + "</DOCNO>\n<HEAD>" + self.title + "</HEAD>\n<TEXT>\n" + self.text + "\n</TEXT>\n</DOC>\n"
                    try:
                        with open(CRAWLED_FOLDER + "" + str(parent_wave), mode='a') as result_file:
                            result_file.write(formatted_output)
                        result_file.close()
                    except IOError:
                        print("Cannot write " + self.url)

                    unique_outlinks = set(soup.find_all('a'))
                    # print("outlinks " + str(len(unique_outlinks)))

                    for link in unique_outlinks:
                        if link.get('href') and not link.get('href').startswith("#"):
                            web_link = link.get('href')
                            web_link = get_canonical_form(web_link, self.url)
                            if web_link not in self.outlinks:
                                self.outlinks.append(web_link)
                            if web_link not in inlinks:
                                inlinks[web_link] = [self.url]
                            else:
                                if self.url not in inlinks[web_link]:
                                    inlinks[web_link].append(self.url)
                            if check_link_usefulness(web_link):
                                if web_link not in crawling_history:  # not crawled before
                                    if web_link not in frontierManager.entry_finder:  # not in the frontier

                                        if web_link not in wave:
                                            wave[web_link] = parent_wave + 1
                                        if len(
                                                crawling_history) < 10000:  # ignore outlinks insertion into frontier if page appearing post 5k
                                            score = get_score(wave[web_link], -crawled_score, web_link,
                                                              maritime_keyword_list, stopword_list)
                                            # Adding to the frontier
                                            frontierManager.add_task(web_link, priority=-score)
                                    else:
                                        # print(web_link + " weblink already accessed and is present in frontier, updating score")
                                        if len(inlinks[web_link]) < 10:
                                            curr_score = frontierManager.get_task_priority(web_link)
                                            curr_score = float("%.2f" % round(-curr_score + 0.0125, 2))
                                            frontierManager.add_task(web_link,
                                                                     priority=-curr_score)  # since each inlink has 0.1 * (no_inlinks / 4) weightage
                                # else:
                                #     print(web_link + " weblink already crawled, not adding to frontier")
                    # print("Added " + str(count) + "links to frontier, new length "+str(len(frontierManager.entry_finder)))
                    if self.url not in inlinks:  # although this situation might never arise
                        inlinks[self.url] = []
                    info = {"id": self.url, "headers": self.header, "raw_html": self.raw_html, "text": self.text,
                            "inlinks": inlinks[self.url], "outlinks": self.outlinks}
                except requests.exceptions.RequestException:
                    print("Unable to read web page")
                    pass
            else:
                print("Unable to get headers of web page")
        except requests.exceptions.RequestException:  # This is the correct syntax
            print("Unable to get headers of web page")
            pass
        finally:
            return info, wave, inlinks, frontierManager  # do nothing
