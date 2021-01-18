from utils import url2links
from utils import urls2dataframe


class PageParser:

    def __init__(self, url):
        self.url = url
        # self._config = config

    def produce(self):
        links = url2links(self.url)
        return [PageParser(link) for link in links]
