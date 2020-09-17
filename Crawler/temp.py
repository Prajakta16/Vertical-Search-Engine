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
        # data_to_be_indexed.update(new_dict)  # merging existing data with new data
        start += 1
    return outlinks


def sync_inlinks_outlinks(inlinks):
    batch_size = 100
    no_batches = int(40000 / batch_size)

    try:
        for i in range(1, no_batches + 1):
            print("Processing batch " + str(i))
            data = read_data(i, batch_size)

            for doc in data:
                if doc in inlinks:  # check doc exists in inlinks dict, otherwise do nothing
                    for link in inlinks[doc]:  # get all inlinks for a particular link
                        if link in data:  # check if this url is crawled too in order to receive the inlink
                            data[doc]["inlinks"] = list(set(data[doc]["inlinks"].append(link)))

            check = "http://en.wikipedia.org/wiki/List_of_maritime_disasters"
            if check in data:
                print(data[check][inlinks])

            try:
                with open(SYNCED_INDEXING_FOLDER + "" + str(i), mode='wb') as file:
                    print("------Writing data with synced inlinks to be indexed in file------")
                    print(len(data))
                    pickle.dump(data, file)
                file.close()
            except IOError:
                print("Dump failed")
        print("----------Sync complete-----------")
    except Exception:
        print("Error")
        pass
        # Dump final inlinks


def get_all_inlinks():
    batch_size = 100
    total_batches = int(40000 / batch_size)
    in_links = {}

    print("------Acquiring newly added inlinks for an url after it was crawled------")
    for i in range(1, total_batches + 1):  # total_batches + 1
        print("Processing batch " + str(i))
        indexed_data_dict = read_data(i, batch_size)
        print(len(indexed_data_dict))
        try:
            for doc in indexed_data_dict:
                try:
                    for link in indexed_data_dict[doc]["outlinks"]:
                        # if href not in inLinks.keys():
                        #     inLinks[href] = set()
                        #     inLinks[href].add(url)
                        # else:
                        #     inLinks[href].add(url)

                        if link in in_links.keys():
                            in_links[link].add(doc)
                            print("Adding "+doc+" to inlinks of "+link)
                        else:
                            # print("Adding "+link+" to inlinks dictionary")
                            in_links[link] = set()
                            in_links[link].add(doc)
                except Exception:
                    pass
        except Exception:
            pass
        print(len(in_links))
    # Backup
    with open(FINAL_INLINKS_FILE, mode='wb') as file:
        print("------Writing final inlinks ------")
        pickle.dump(in_links, file)
    file.close()
    return in_links


def get_inlinks_from_outlinks():
    batch_size = 100
    total_batches = int(40000 / batch_size)
    in_links = {}

    print("------Acquiring newly added inlinks for an url after it was crawled------")
    for i in range(1, 2):  # total_batches + 1
        print("Processing batch " + str(i))
        filepath = open(FINAL_OUTLINKS_FILE, 'rb')
        outlinks = pickle.load(filepath)
        filepath.close()
        print(len(outlinks))
        for doc in outlinks:
            try:
                for link in outlinks[doc]:
                    if link in in_links.keys():
                        in_links[link].add(doc)
                        print("Adding " + doc + " to inlinks of " + link)
                    else:
                        print("Adding new " + link + " to inlinks dictionary")
                        in_links[link] = set()
                        in_links[link].add(doc)
            except Exception:
                print("Error for " + doc)
                pass
        print(len(in_links))
    # Backup
    with open(FINAL_INLINKS_FILE, mode='wb') as file:
        print("------Writing final inlinks ------")
        pickle.dump(in_links, file)
    file.close()
    return in_links

if __name__ == '__main__':
    batch_size = 100
    total_batches = int(40000 / batch_size)
    outlinks = {}
    for i in range(1, total_batches + 1):
        print("Processing batch " + str(i))
        outlinks = read_outlinks(i, batch_size, outlinks)
        print(len(outlinks))
    # Backup
    with open(FINAL_OUTLINKS_FILE, mode='wb') as file:
        print("------Writing final outlinks ------")
        pickle.dump(outlinks, file)
    file.close()

    inlinks = get_all_inlinks()
    print(inlinks["http://en.wikipedia.org/wiki/List_of_maritime_disasters"])
    exit()

    # filepath = open(FINAL_INLINKS_FILE, 'rb')
    # inlinks = pickle.load(filepath)
    # filepath.close()

    sync_inlinks_outlinks(inlinks)

    # ----------
    # 1. As outlinks were not accounted initially, this code parsed the war_html and identified outlinks
    print("------Get outlinks ------")
    outlinks = {}
    for i in range(1, 149):
        print("Processing batch " + str(i))
        outlinks = read_outlinks(i, batch_size, outlinks)
        print(len(outlinks))

    print("------Writing final outlinks ------")
    filepath = open(FINAL_OUTLINKS_FILE2, 'wb')
    pickle.dump(outlinks, filepath)
    filepath.close()

    filepath = open(FINAL_OUTLINKS_FILE, 'rb')
    outlinks = pickle.load(filepath)
    filepath.close()

    print(len(outlinks))