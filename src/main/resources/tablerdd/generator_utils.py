#!/usr/bin/python
import sys

# e.g. createList(1,3, "T[", "]", ",") gives T[1],T[2],T[3]
def createList(start, stop, prefix, suffix="", sep = ",", newlineAfter = 70, indent = 0):
    res = ""
    oneLine = res
    for y in range(start,stop+1):
        res     += prefix + str(y) + suffix
        oneLine += prefix + str(y) + suffix
        if y != stop:
            res     += sep
            oneLine += sep
            if len(oneLine) > newlineAfter:
                res += "\n" + " "*indent
                oneLine = ""
    return res

