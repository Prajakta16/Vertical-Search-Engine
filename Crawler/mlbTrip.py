import pprint

import numpy


def getBestRoute(G, S, T, B, k):
    n = len(G)
    # DP table would be a 3D table with dimensions as vertex, hops, and num of ballpark nodes
    DP = [[[8888 for ll in range(0, k+1)] for j in range(0, n - 1)] for v in range(0, n)]
    # DP = numpy.empty((n, n-1, k+1), dtype=int)
    pprint.pprint(DP)

    print("-------------Define base cases--------------")
    #  base cases
    for v in range(0, n):
        # print("Processing node " + str(v))
        for x in range(0, k+1):
            DP[v][0][x] = 999

        if v in B:
            for j in range(0, n - 1):
                DP[v][j][0] = 999  # if allowed ballpark nodes is 0, and if v is a ballpark node then no path exists
    DP[S][0][0] = 0
    pprint.pprint(DP)

    print("-----------DP---------")

    # iteration
    for l in range(0, k+1):
        for j in range(1, n - 1):
            for v in range(0, n):
                print("Updating v = "+str(v)+" j = "+str(j)+" l = "+str(l))
                print(DP[v][j][l])
                DP[v][j][l] = 999
                print(DP[v][j][l])

                if v in B:
                    for u in range(0, n):
                        if DP[u][j - 1][l - 1] == 9999:
                            print(+str(u)+" unexplored")
                        if G[u][v] != 0:  # indicates that u is an incoming node for v
                            if DP[v][j][l] > DP[u][j - 1][l - 1] + G[u][v]:
                                DP[v][j][l] = DP[u][j - 1][l - 1] + G[u][v]
                else:
                    for u in range(0, n):
                        if DP[u][j - 1][l - 1] == 9999:
                            print(+str(u)+" unexplored")
                        if G[u][v] != 0:  # indicates that u is an incoming node for v
                            if DP[v][j][l] > DP[u][j - 1][l] + G[u][v]:
                                DP[v][j][l] = DP[u][j - 1][l] + G[u][v]

                if j - 1 >= 0:
                    DP[v][j][l] = min(DP[v][j][l], DP[v][j - 1][l])
                    if l - 1 >= 0:
                        DP[v][j][l] = min(DP[v][j][l], DP[v][j][l - 1])
                if l - 1 >= 0:
                    DP[v][j][l] = min(DP[v][j][l], DP[v][j][l - 1])
                    if j - 1 >= 0:
                        DP[v][j][l] = min(DP[v][j][l], DP[v][j - 1][l])
                print(DP[v][j][l])
    pprint.pprint(DP)

    # analyze the DP table to find best path
    min_dist = 9999
    for x in range(0, k+1):
        for j in range(0, n-1):
            if DP[T][j][x] < min_dist:
                min_dist = DP[T][j][x]
                hops = j
                ballparks = x
    print(min_dist)
    print(hops)
    print(ballparks)


if __name__ == '__main__':
    G = [[0, 1, 0, 0, 0, 0],
         [0, 0, 3, 4, 2, 0],
         [0, 0, 0, 0, 0, -2],
         [0, 0, 8, 0, 3, -2],
         [0, 0, 0, 0, 0, 6],
         [0, 0, 0, 0, -1, 0]]

    S = 0  # source node
    T = 4  # terminal node
    B = [2, 3, 5]  # ballpark nodes
    k = 3  # trip can be through at max k ballpark nodes

    getBestRoute(G, S, T, B, k)
