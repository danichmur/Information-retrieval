from pyspark import SparkConf, SparkContext, AccumulatorParam, StorageLevel
from bs4 import BeautifulSoup
import requests
import gzip
import os
import pickle
import re
from operator import add


STORAGE = os.path.join(os.getcwd(), "wiki")
DOMEN = "https://simple.wikipedia.org"
URL_NAME = "pagerank_data.txt"
prohibited = {'/', ':'}
wiki_re = re.compile("^/wiki")


class DictAccumulator(AccumulatorParam):

    def zero(self, initial_value=None):
        return initial_value or {}

    def addInPlace(self, v1, v2):
        v1.update(v2)
        return v1


def url2path(url):
    path = ''.join(c for c in url if c not in prohibited)
    path = path.replace('wiki', '')
    if len(path) > 2:
        return os.path.join(STORAGE, path[0], path[1], path)
    else:
        return ""


def save_file(url, html, url_path_map):
    path = url2path(url)
    url_path_map += {url: path}
    if path != "" and not os.path.isfile(path):
        directory, _ = os.path.split(path)
        if not os.path.exists(directory):
            os.makedirs(directory)
        try:
            with gzip.open(path, 'wb') as f:
                f.write(gzip.compress(bytes(html, 'utf-8')))
        except Exception as e:
            print("Wow", str(e))


def url2links(url, url_path_map):
    try:
        page = requests.get(DOMEN + url)
    except Exception as e:
        print("Wow", str(e))
        return []

    html = page.text
    save_file(url, html, url_path_map)
    soup = BeautifulSoup(html, 'html.parser')
    return [
        (url, to) for to
        in [
            a["href"] for a in soup.findAll('a', attrs={'href': wiki_re})
            if not ('#' in a["href"] or re.search(r':(?!_)', a["href"]) is not None)
        ]
    ]


def num2file(num):
    return os.path.join("edges", str(num) + ".p")


def give_me_sc():
    conf = SparkConf()\
        .setAppName("PySparkler") \
        .set("spark.executor.memory", "4g") \
        .set("spark.driver.memory", "15g") \
        .set("spark.cores.max", "4")

    return SparkContext(conf=conf)


def load_wiki(sc):
    read_backup = False
    start_urls = ["/wiki/Main_Page"]
    url_path_map_value = {}
    visited_urls = set(start_urls)

    if read_backup:
        with open('cur_links.p', 'rb') as f:
            start_urls = pickle.load(f)
        with open('visited_urls.p', 'rb') as f:
            visited_urls = pickle.load(f)
        with open('url_path_map.p', 'rb') as f:
            url_path_map_value = pickle.load(f)

    url_path_map = sc.accumulator(url_path_map_value, DictAccumulator())
    urls_rdd = sc.parallelize(start_urls).repartition(4)

    total_num = 0
    acc = 1
    i = 0

    while acc > 0:
        url_pairs_rdd = urls_rdd.flatMap(lambda url: url2links(url, url_path_map)).distinct()
        url_pairs = url_pairs_rdd.collect()
        pickle.dump(url_pairs, open(num2file(i), "wb"))
        i += 1

        visited_urls.update(set([j for j, _ in url_pairs]))
        urls = [j for _, j in url_pairs if j not in visited_urls]
        urls_rdd = sc.parallelize(urls).repartition(4).distinct().persist(StorageLevel.MEMORY_AND_DISK)

        acc = len(urls)
        total_num += acc
        print(f"Get: {total_num} links")
        pickle.dump(url_path_map.value, open("url_path_map.p", "wb"))
        pickle.dump(urls, open("cur_links.p", "wb"))
        pickle.dump(visited_urls, open("visited_urls.p", "wb"))


def rw(i):
    with open('edges/' + str(i) + '.p', 'rb') as f:
        url_path_map_value = pickle.load(f)

    with open(URL_NAME, 'a') as f:
        for key, value in url_path_map_value:
            f.write(key + " " + value + "\n")


def pickle2adj_list():
    for i in range(12):
        rw(i)


def line2links(urls):
    parts = urls.split()
    return parts[0], parts[1]


def compute_rank(urls, rank):
    num_urls = len(urls)
    return [(url, rank / num_urls) for url in urls]


def pagerank(sc, iterations=20, delta=0.85):
    lines = sc.textFile(URL_NAME)
    links = lines.map(line2links).groupByKey(numPartitions=4).cache()
    ranks = links.map(lambda x: (x[0], 1.0))

    for _ in range(iterations):
        contribs = links.join(ranks).flatMap(lambda x: compute_rank(x[1][0], x[1][1]))
        ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * delta + 1 - delta)

    # count = ranks.count()
    count = 188993

    print(count)
    for url, rank in ranks.top(10, key=lambda x: x[1]):
        print(url, rank / count)

    # sum = ranks.map(lambda x: x[1]).sum()
    # print(sum / ranks.count())


if __name__ == "__main__":
    # check_link("/wiki/List_of_S-phrases")
    # print("hello")
    sc = give_me_sc()

    # load_wiki(sc)
    pagerank(sc, delta=0.3)  # delta 0.95, 0.5, 0.3.
    # filter_pages(sc)

    sc.stop()

