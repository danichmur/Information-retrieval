from bs4 import BeautifulSoup
import requests
import gzip
import os
from pyspark.sql import Row
import pickle

STORAGE = os.path.join(os.getcwd(), "wiki")
DOMEN = "https://simple.wikipedia.org"
ignore_values = {"", "/"}
prohibited = {'/', ':'}


def desired_links(href):
    if not href:
        return False

    if href.startswith("#"):
        return False

    if href in ignore_values:
        return False

    if not (href.startswith('/wiki')):
        return False

    if "File:" in href:
        return False

    return True


def url2path(url):
    path = ''.join(c for c in url if c not in prohibited)
    path = path.replace('wiki', '')
    if len(path) > 2:
        return os.path.join(STORAGE, path[0], path[1], path)
    else:
        return ""


def save_file(url, html, url_path_map):
    path = url2path(url)
    if path != "":
        directory, _ = os.path.split(path)
        if not os.path.exists(directory):
            os.makedirs(directory)
        try:
            with gzip.open(path, 'wb') as f:
                url_path_map += {url: path}
                compressed_value = gzip.compress(bytes(html, 'utf-8'))
                f.write(compressed_value)
        except Exception as e:
            print(str(e))


def url2links(url, url_path_map):
    page = requests.get(DOMEN + url)
    html = page.text
    save_file(url, html, url_path_map)
    soup = BeautifulSoup(html, 'html.parser')
    return set([a["href"] for a in soup.find_all("a", href=desired_links)])


def urls2dataframe(spark, urls):
    return spark.createDataFrame([Row(url=u) for u in urls])


def dump_dict(url_path_map):
    pickle.dump(url_path_map, open("url_path_map.p", "wb"))