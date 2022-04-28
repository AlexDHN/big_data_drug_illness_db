import re


def loading_stitch_chemical_sources(path):
    print("Start loading Stitch Chemical Sources\n")
    tuple_chemical = []  # Tuple in which there will be CID with corresponding ATC
    with open(path + '/STITCH - ATC/chemical.sources.v5.0.tsv') as f:
        for line in f:
            if line[26:29] == "ATC":  # Interesting part of the doc
                cidm = re.search(r"CIDm[0-9]*", line).group()
                cidm = cidm[:3] + "1" + cidm[4:]  # to be identical to sider cid
                cids = re.search(r"CIDs[0-9]*", line).group()
                cids = cids[:3] + "0" + cids[4:]  # to be identical to sider cid
                tuple_chemical.append((cidm + " " + cids, line[30:][:-1]))
            elif line[0] == "#" or line[:8] == "chemical":  # Beginning of the doc
                pass
            else:  # Uninteresting part of the doc
                print("End loading Stitch Chemical Sources\n")
                return tuple_chemical
    return tuple_chemical


def matching(data):  # "Eliglustat [DG:DG00148]" return Eliglustat
    n = len(data)
    for i in range(n):
        if data[i] == '[':
            return data[:i-1]
    return data


def loading_stitch_br(path):
    print("Start loading Stitch br08303\n")
    tuple_br = []  # Tuple in which there will be CID with corresponding ATC
    with open(path + '/STITCH - ATC/br08303.keg') as f:
        for line in f:
            if line[0] == "E":  # Interesting part of the doc
                # tuple_br.append((re.search(r"[A-Z]([0-9]|[A-Z]){6}", line).group(),re.search(r"(?<=[A-Z]([0-9]|[A-Z]){6} ).*$", line).group()))
                tuple_br.append((line[9:16], matching(line[17:][:-1])))
            else:
                pass
    print("End loading Stitch br08303\n")
    return tuple_br
