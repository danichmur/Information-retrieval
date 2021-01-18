from tqdm import tqdm
import os.path

from typing import Dict, List, Tuple

ArticleName = str
Text = str
Term = str
CollectionData = Dict[str, Dict[str,  float]]
RankingParams = {}

PREFIX = '/Users/danielmuraveyko/playground/ir/wiki'
AVERAGE_KEY = 'avg_len'
ID_TO_TITLE_KEY = 'id2title'
DOC_TO_LEN_KEY = 'doc2len'
POSTINGS_LEN_KEY = 'postings_len'

DocId = int
TermFreq = int

RelevInfo = float

Posting = List[Tuple[DocId, RelevInfo]]


import pickle
import gzip

url_path_map_value = {}

with open('/Users/danielmuraveyko/Desktop/url_path_map.p', 'rb') as f:
    url_path_map_value = pickle.load(f)

import sys

letter = sys.argv[1].lower()

from bs4 import BeautifulSoup
import re 
import itertools  

def html2text(html):
    soup = BeautifulSoup(html)
    paras = []
    for paragraph in soup.find_all('p'):
        paras.append(str(paragraph.text))
    heads = []
    for head in soup.find_all('span', attrs={'mw-headline'}):
        heads.append(str(head.text))
    text = [val for pair in itertools.zip_longest(paras, heads, fillvalue =' ' ) for val in pair]
    text = ' '.join(text)
    text = re.sub(r"\[.*?\]+", '', text)

    text = text.replace('\n', '')[:-11]
    return text


def get_article_text(article_name: ArticleName) -> Text:
    text = ""
    key = article_name
    path = url_path_map_value[key]
    if path != '':
        if os.path.isfile(path):
            with gzip.open(path, 'rb') as f:
                html = gzip.decompress(f.read()).decode('utf-8')
                title = article_name.replace('_', ' ')
                text = html2text(html)
        else:
            text = "NOPE"
    else:
        text = "NOPE"
    return text

import string

def make_terms(text: Text) -> List[Term]:
    words = text.lower().translate(str.maketrans('', '', string.punctuation)).split()
    return words


def load_docs(url_path_map) -> Dict[ArticleName, Text]:    
    docs = {}
    nope = []
    for article_name in tqdm(url_path_map.keys()):
        if article_name != '/wiki/':
            first_letter = article_name.split("wiki/",1)[1][0].lower()
            if first_letter == letter:
                text = get_article_text(article_name)
                if text == "NOPE":
                    nope.append(article_name)
                else:
                    docs[article_name] = text
    return docs, nope
    
docs, nope = load_docs(url_path_map_value)
print(f'{len(docs)} docs loaded')
print(f'{len(nope)} nope found')
pickle.dump(docs, open(f"{letter}_docs.p", "wb"))
pickle.dump(nope, open(f"{letter}_nope.p", "wb"))
