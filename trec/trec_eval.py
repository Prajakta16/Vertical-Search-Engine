import math
import matplotlib.pyplot as plt

HW1_flag = False
detail_flag = False
total_docs = 0
RESULT_ESBUILTIN_FILE = "./result/esbuiltin.txt"
RESULT_BM25_FILE = "./result/result_bm25.txt"

precision_k = dict()
recall_k = dict()
f1_k = dict()
avg_precision = dict()
r_precision = dict()
prec_at_recall_cutoffs = dict()
prec_at_precision_cutoffs = dict()
nDCG = dict()
relevance = dict()

recall_cutoffs = tuple()
prec_cutoffs = tuple()


def readQrelFile(file):
    file_content = dict()
    with open(file, mode='r') as eval_file:
        content = eval_file.read().split("\n")
    eval_file.close()

    for line in content:
        if line not in ['']:
            try:
                fields = line.split(" ")
                query_id = fields[0]
                assessor_id = fields[1]
                doc_id = fields[2]
                grade = int(fields[3])
                if query_id not in file_content:
                    file_content[query_id] = dict()
                # if doc_id in file_content[query_id]:
                #     print(doc_id)
                file_content[query_id][doc_id] = grade
            except Exception:
                # print("Error parsing line " + line)
                pass

    return file_content


def readResultFile(file, doc_col, grade_col):
    file_content = dict()
    with open(file, mode='r') as eval_file:
        content = eval_file.read().split("\n")
    eval_file.close()

    for line in content:
        if line not in ['']:
            try:
                fields = line.split(" ")
                # print(fields)
                query_id = fields[0]
                # print(query_id)
                doc_id = fields[doc_col]
                # print(doc_id)
                grade = float(fields[grade_col])
                # print(grade)
                # exit()
                if query_id not in file_content:
                    file_content[query_id] = dict()
                # if doc_id in file_content[query_id]:
                #     print(doc_id)
                file_content[query_id][doc_id] = grade
            except Exception:
                print("Error parsing line " + line)
                pass
    print(file_content)
    return file_content


def printEvaluations(count_ret, count_rel, count_rel_ret, prec_at_recall_cutoffs, avg_precision, prec_at_prec_cutoffs,
                     r_precision, ndcg, f1_k):
    f1_cutoffs = (5, 10, 20, 50, 100)
    print("Documents retrieved: " + str(count_ret))
    print("Existing relevant documents: " + str(count_rel))
    print("Relevant documents retrieved: " + str(count_rel_ret))
    print("Interpolated Recall - Precision Averages(Precision at recall cutoffs):")
    for cutoff in recall_cutoffs:
        print("At " + str(cutoff) + "\t" + str(prec_at_recall_cutoffs[cutoff]))
    print("Average precision: " + str(avg_precision)+"\n")
    print("Precision at cutoffs:")
    for cutoff in prec_at_prec_cutoffs:
        print("At " + str(cutoff) + "\t" + str(prec_at_prec_cutoffs[cutoff]))
    print("R precision: " + str(r_precision))
    if not HW1_flag:
        print("\nF1 at cutoff: ")
        for cutoff in f1_cutoffs:
            if cutoff in f1_k:
                if f1_k != 999:
                    print("At "+str(cutoff)+" "+str(f1_k[cutoff]))
                else:
                    print("At "+str(cutoff)+" undefined/infinity")
    print("nDCG: " + str(ndcg))
    print("--------------------------------------------------------------")


def calculate_dcg(doc_relevance, query_id):
    dcg = dict()
    i = 0
    for doc_id in doc_relevance:
        i = i + 1
        if i == 1:
            dcg = relevance[query_id][doc_id]  # relevance of the first doc
        else:
            dcg += relevance[query_id][doc_id] / math.log(i, 2)
    return dcg


def calculate_eval_for_query(query_id, qrel_dict, result_dict, c_relevant):
    precision_k[query_id] = dict()
    recall_k[query_id] = dict()
    f1_k[query_id] = dict()
    prec_at_recall_cutoffs[query_id] = dict()
    prec_at_precision_cutoffs[query_id] = dict()
    relevance[query_id] = dict()
    count_retrieved = 0
    count_rel_retrieved = 0
    sum_prec = 0

    # print("Accessing docs "+str(len(result_dict[query_id])))
    for doc_id in result_dict[query_id]:  # doc_ids will be sorted in terms of score
        count_retrieved += 1
        if doc_id in qrel_dict[query_id]:
            doc_relevance_for_query = qrel_dict[query_id][doc_id]
        else:
            doc_relevance_for_query = 0

        if doc_relevance_for_query > 0:  # if the doc is not marked irrelevant
            count_rel_retrieved += 1
            sum_prec += doc_relevance_for_query * count_rel_retrieved / count_retrieved  # doc_relevance_for_query only significant when relevance is scaled and not binary
            # sum of precision for all the relevant docs retrieved till now

        prec = count_rel_retrieved / count_retrieved
        precision_k[query_id][count_retrieved] = prec

        rec = count_rel_retrieved / c_relevant
        recall_k[query_id][count_retrieved] = rec

        if prec + rec != 0:
            f1_k[query_id][count_retrieved] = (2 * prec * rec) / (prec + rec)
        else:
            # print("Prec + recall = 0 for "+str(query_id)+" "+str(count_retrieved))
            f1_k[query_id][count_retrieved] = 999

        if count_retrieved > total_docs:
            break

    # filling in the leftover values upto doc count = 1000
    final_recall = count_rel_retrieved / count_retrieved
    for k in range(count_retrieved+1, 1001):
        precision_k[query_id][k] = count_rel_retrieved/k
        recall_k[query_id][k] = final_recall
    # print(len(precision_k[query_id]))

    # avg precision combines recall and precision at relevant docs
    avg_precision[query_id] = sum_prec / c_relevant

    # get precision at cutoffs
    for cutoff in prec_cutoffs:
        prec_at_precision_cutoffs[query_id][cutoff] = precision_k[query_id][cutoff]

    # r_precision is the recall at the Rth position where R(c_relevant) is the total relevant docs for the query
    if c_relevant > count_retrieved:
        r_precision[query_id] = count_rel_retrieved / c_relevant
    else:
        r_precision[query_id] = precision_k[query_id][c_relevant]

    # calculate prec at recall_cutoffs.. As you move down the ranked list, recall increases monotonically, whenever recall matches our cutoffs we store the precision values at till that point
    max_prec = 0
    j = 1000
    while j >= 1:
        if precision_k[query_id][j] >= max_prec:
            max_prec = precision_k[query_id][j]
        else:
            precision_k[query_id][j] = max_prec
        j = j - 1

    i = 1  # i indicates the doc which we are at
    for cutoff in recall_cutoffs:
        while i <= total_docs and recall_k[query_id][i] < cutoff:
            i += 1
        if i <= total_docs:  # valid
            prec_at_recall_cutoffs[query_id][cutoff] = precision_k[query_id][i]
        else:
            prec_at_recall_cutoffs[query_id][cutoff] = 0

    # Calculating dgc
    i = 0
    nDCG[query_id] = 0
    for doc_id in result_dict[query_id]:  # check relevance and add to the formula
        if doc_id in qrel_dict[query_id]:
            relevance[query_id][doc_id] = qrel_dict[query_id][doc_id]
        else:
            relevance[query_id][doc_id] = 0

    DCG = calculate_dcg(result_dict[query_id], query_id)
    sorted_docs_by_relevance = {k: v for k, v in
                                sorted(relevance[query_id].items(), reverse=True, key=lambda item: item[1])}
    iDCG = calculate_dcg(sorted_docs_by_relevance, query_id)
    nDCG[query_id] = DCG / iDCG

    print("Query " + query_id)
    if detail_flag:
        printEvaluations(count_retrieved, c_relevant, count_rel_retrieved, prec_at_recall_cutoffs[query_id],
                         avg_precision[query_id], prec_at_precision_cutoffs[query_id], r_precision[query_id], nDCG[query_id], f1_k[query_id])
    return count_retrieved, count_rel_retrieved


def trec_eval(qrel_dict, result_dict, count_relevant):
    num_queries = len(result_dict)
    total_retrieved = 0
    total_relevant_retrieved = 0
    total_relevant_across_queries = 0

    count = 1
    for query_id in result_dict:
        print("Queries accessed till now"+str(count))
        count += 1
        ret, rel_ret = calculate_eval_for_query(query_id, qrel_dict, result_dict, count_relevant[query_id])
        total_retrieved += ret
        total_relevant_retrieved += rel_ret
        total_relevant_across_queries += count_relevant[query_id]

    # calculate summary for all queries
    sum_prec_at_prec_cutoff = dict()
    sum_prec_at_recall_cutoff = dict()
    sum_f1_at_f1_cutoffs = dict()

    sum_avg_precision = 0
    sum_r_precision = 0
    sum_ndgc = 0
    for cutoff in f1_cutoff:
        sum_f1_at_f1_cutoffs[cutoff] = 0
    for cutoff in prec_cutoffs:
        sum_prec_at_prec_cutoff[cutoff] = 0
    for cutoff in recall_cutoffs:
        sum_prec_at_recall_cutoff[cutoff] = 0

    for query_id in result_dict:
        sum_avg_precision += avg_precision[query_id]
        sum_r_precision += r_precision[query_id]
        sum_ndgc += nDCG[query_id]

        for cutoff in f1_cutoff:
            if f1_k[query_id][cutoff] != 999:
                sum_f1_at_f1_cutoffs[cutoff] += f1_k[query_id][cutoff]

        for cutoff in prec_cutoffs:
            sum_prec_at_prec_cutoff[cutoff] += prec_at_precision_cutoffs[query_id][cutoff]

        for cutoff in recall_cutoffs:
            sum_prec_at_recall_cutoff[cutoff] += prec_at_recall_cutoffs[query_id][cutoff]

    for cutoff in f1_cutoff:
        sum_f1_at_f1_cutoffs[cutoff] = sum_f1_at_f1_cutoffs[cutoff] / num_queries

    for cutoff in prec_cutoffs:
        sum_prec_at_prec_cutoff[cutoff] = sum_prec_at_prec_cutoff[cutoff] / num_queries

    for cutoff in recall_cutoffs:
        sum_prec_at_recall_cutoff[cutoff] = sum_prec_at_recall_cutoff[cutoff] / num_queries

    print("Total Queries " + str(num_queries))
    printEvaluations(total_retrieved, total_relevant_across_queries, total_relevant_retrieved,
                     sum_prec_at_recall_cutoff, sum_avg_precision / num_queries, sum_prec_at_prec_cutoff,
                     sum_r_precision / num_queries, sum_ndgc / num_queries, sum_f1_at_f1_cutoffs)


def count_rel(qrel_dict):
    count_rele = dict()
    for query_id in qrel_dict:
        for doc_id in qrel_dict[query_id]:
            if qrel_dict[query_id][doc_id] > 0:  # indicates that the doc is relevant for the query
                if query_id not in count_rele:
                    count_rele[query_id] = 0
                count_rele[query_id] += 1
    return count_rele


def plot_prec_recall_curve():
    for queryId in result_dict:
        plt.plot(recall_k[queryId].values(), precision_k[queryId].values())
        plt.xlabel('Recall')
        plt.ylabel('Precision')
        plt.title('Precision recall graph for query '+queryId)
        plt.show()


if __name__ == '__main__':
    if HW1_flag:
        total_docs = 1000
        recall_cutoffs = (0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0)
        prec_cutoffs = (5, 10, 15, 20, 30, 100, 200, 500, 1000)
        qrel_file = "/Users/prajakta/PycharmProjects/HW1IR/result/basic/qrels.txt"
        result_file = "/Users/prajakta/PycharmProjects/HW1IR/result/basic/esbuiltin.txt"
        print("Reading eval files..")
        qrel_dict = readQrelFile(qrel_file)
    else:
        total_docs = 200
        recall_cutoffs = (0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0)
        prec_cutoffs = (5, 10, 15, 20, 30, 100, 200)
        print("Reading eval files..")
        qrel_dictps = readQrelFile("./eval_files/qrelPraharsha.txt")
        qrel_dictpd = readQrelFile("./eval_files/qrelPrajakta.txt")
        qrel_dictsj = readQrelFile("./eval_files/qrelsaiesh.txt")

        for query_id in qrel_dictsj:
            for doc in qrel_dictsj[query_id]:
                try:
                    if doc in qrel_dictps[query_id]:
                        if qrel_dictsj[query_id][doc] != qrel_dictps[query_id][doc]:
                            if doc in qrel_dictpd[query_id]:
                                if qrel_dictsj[query_id][doc] != qrel_dictpd[query_id][doc]:
                                    if qrel_dictpd[query_id][doc] != qrel_dictsj[query_id][doc]:
                                        qrel_dictsj[query_id][doc] = max(qrel_dictpd[query_id][doc], qrel_dictps[query_id][doc], qrel_dictsj[query_id][doc])
                                    else:
                                        qrel_dictsj[query_id][doc] = qrel_dictpd[query_id][doc]
                            else:
                                # print("Not in pd" + doc)
                                qrel_dictsj[query_id][doc] = max(qrel_dictps[query_id][doc], qrel_dictsj[query_id][doc])
                    else:
                        # print("Not in ps "+doc)
                        if doc in qrel_dictpd[query_id]:
                            qrel_dictsj[query_id][doc] = max(qrel_dictpd[query_id][doc], qrel_dictsj[query_id][doc])
                except Exception:
                    pass
        qrel_dict = qrel_dictsj

        with open("./eval_files/combined_qrel", mode='w') as combined_qrel:
            for query_id in qrel_dict:
                for doc in qrel_dict[query_id]:
                    combined_qrel.write(doc+" "+str(qrel_dict[query_id][doc])+"\n")
                    if qrel_dict[query_id][doc] == 2:
                        qrel_dict[query_id][doc] = 1
        combined_qrel.close()

        result_file = RESULT_BM25_FILE
        # result_file = RESULT_ESBUILTIN_FILE
    f1_cutoff = (5, 10, 20, 50, 100)

    count_relevant = count_rel(qrel_dict)
    print("Number of queries in qrel " + str(len(qrel_dict)))
    if HW1_flag:
        result_dict = readResultFile(result_file, 2, 4)
    else:
        result_dict = readResultFile(result_file, 1, 3)
    print(len(result_dict))

    ranked_docs = dict()
    for queryId in result_dict:  # for each query we will have a doc with score
        ranked_docs[queryId] = {k: v for k, v in
                                sorted(result_dict[queryId].items(), reverse=True, key=lambda item: item[1])}
    result_dict = ranked_docs
    print("Number of queries in result " + str(len(result_dict)))

    trec_eval(qrel_dict, result_dict, count_relevant)

    plot_prec_recall_curve()
