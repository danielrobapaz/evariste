import requests
import pandas as pd
from lxml import html
from bs4 import BeautifulSoup
import re
import random

TAGS = ['title', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'p', 'a','span', 'label', 'li']
TABLE_TAGS = ['tr', 'td', 'th', 'tbody']
HEADER_TAGS = ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']
BASE_DOMAIN = 'https://en.wikipedia.org'

class ExtractContentFromWikipedia:
    def request_maker(self,
                      url: str) -> None:
        try:
            response = requests.get(url)

            if response.status_code == 200:
                html = response.text
                url = response.url

                links = self.parse_links(html, url)
                text = self.extract_text(html, '//div[@id="bodyContent"]')
                return (links, text)

        except Exception as e:
            print(e)
            return (set(), f"Could not get the HTML of the page {url}.")
            
    def parse_links(self, 
                    html: str,
                    page_url: str):
        soup = BeautifulSoup(html, 'html.parser')
        links = soup.find_all('a')

        pattern = re.compile(r'^/wiki/(?!.*(?:Category|Wikipedia|Help|Portal|File|Special)).*')
        valid_links = [link.get('href') for link in links if pattern.match(str(link.get('href')))]
        
        return set([f'{BASE_DOMAIN}{link}' for link in valid_links])

    def extract_html(sefl,
                     url: str) -> str | None:
        """Extracts html code from a page

        Args:
            url (string): Page's link

        Returns:
            string: Page's html code
        """
        request = requests.get(url)

        if request.status_code == 200:
            return request.text
        
        return None

    def extract_text(self,
                     html_str: str,
                     content_xpath: str) -> list[str]:
        """Extracts text contents from a html code

        Args:
            html (string): Html code
            
            content_xpath: A string with the xpath to the content main
                        label

        Returns:
            list: List of text contents
        """
        root = html.fromstring(html_str)

        content = []
        stack = [root.xpath(content_xpath)[0]]
        
        while stack:
            node = stack.pop()
            if node.tag in TABLE_TAGS:
                continue
            if node.tag in TAGS:
                if node.tag in HEADER_TAGS:
                    content.append('\n')
                    content.append(node.text_content())
                    content.append('\n')
                else: 
                    content.append(node.text_content())

                continue

            stack.extend(reversed(node))
        
        return ' '.join(content)

    def extract_pages_info(self,
                           filename: str, 
                           content_xpath:str = '//div[@id="bodyContent"]') -> dict[str, str]:
        """Extracts information from a page

        Args:
            filename (string): Path of the CSV with countries and pages

            content_xpath: A string with the xpath to the content main
                        label

        Returns:
            dictionary: Dictionary with countries as keys and information as value
        """

        countries = pd.read_csv(filename, sep=';')
        countries_dic = countries.set_index(countries.columns[0])[countries.columns[1]].to_dict()
        countries_info = {}

        for country in countries_dic.keys():
            url = countries_dic[country]  
            html = self.extract_html(url)

            countries_info[country] = self.extract_text(html, content_xpath) if html is not None else f"Could not get the HTML of the page {url}."

        return countries_info