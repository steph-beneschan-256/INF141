import logging
import re
from urllib.parse import urlparse

from lxml import etree as etree #Would it be better to use BeautifulSoup?
from lxml import html
from lxml.html import fromstring

logger = logging.getLogger(__name__)

class Crawler:
    """
    This class is responsible for scraping urls from the next available link in frontier and adding the scraped links to
    the frontier
    """

    def __init__(self, frontier, corpus):
        self.frontier = frontier
        self.corpus = corpus

    def start_crawling(self):
        """
        This method starts the crawling process which is scraping urls from the next available link in frontier and adding
        the scraped links to the frontier
        """
        while self.frontier.has_next_url():
            url = self.frontier.get_next_url()
            logger.info("Fetching URL %s ... Fetched: %s, Queue size: %s", url, self.frontier.fetched, len(self.frontier))
            url_data = self.corpus.fetch_url(url)

            for next_link in self.extract_next_links(url_data):
                if self.is_valid(next_link):
                    if self.corpus.get_file_name(next_link) is not None:
                        self.frontier.add_url(next_link)

    def extract_next_links(self, url_data):
        """
        The url_data coming from the fetch_url method will be given as a parameter to this method. url_data contains the
        fetched url, the url content in binary format, and the size of the content in bytes. This method should return a
        list of urls in their absolute form (some links in the content are relative and needs to be converted to the
        absolute form). Validation of links is done later via is_valid method. It is not required to remove duplicates
        that have already been fetched. The frontier takes care of that.


        Suggested library: lxml
        """
        outputLinks = []

        '''
        The value of url_data["content_type"] is usually "b'text/html; charset=UTF-8'"
        This includes the "b'" within the string for some reason (something to do with converting from bytes to str?)
        Sometimes the encoding isn't present
        '''
        if(url_data["content_type"] == None):
            return outputLinks

        content_type = url_data["content_type"].removeprefix("b\'") #https://docs.python.org/3.9/library/stdtypes.html?highlight=removeprefix#str.removeprefix
        file_type = content_type.split(';')[0] #usually content_type has both a filetype and an encoding, but sometimes the encoding is absent...
        
        #Probably not the best way to check the file type of these checks, but I don't know what else to do...
        #Also, I think that we might be supposed to handle .txt files too, but I'm not sure if lxml has any tools to parse those
        doc = None
        if (file_type == 'text/html'):
            doc = html.fromstring(url_data["content"]) #Creates an lxml.html.HtmlElement object
        elif (file_type == 'text/xml'):
            try:
                doc = etree.fromstring(url_data["content"], base_url=url_data["url"]) #I'm not entirely sure whether the base_url part helps or not
            except etree.XMLSyntaxError: #this error seems to come up every time we try to parse an XML file
                print("failed to parse XML document") #Apparently the lxml parser doesn't work if the XML file has the "XML Prolog" at the beginning
        
        if(doc == None):
            #print("Could not parse {} with content type {}\n".format(url_data["url"], content_type))
            return outputLinks

        doc.make_links_absolute(url_data["url"])

        #Maybe this could be where we analyze the document for the analytics?
        #But where do we keep that data? In the frontier? But he said that we don't have to edit frontier.py, right?
        #If we just stored the data here, then wouldn't it be lost if we had to pause the crawl and then run the program again?

        for link in doc.iterlinks():
            link_url = link[2] #According to the lxml documentation, link_url should be a tuple: (element, attribute, link, pos)
            if((link != None) and self.is_valid(link_url)):
                outputLinks.append(link_url)

        if(file_type == 'text/xml'):
            print("Successfully parsed an XML file!\n")
        else:
            print("Successfully parsed the file")

        return outputLinks

    def is_valid(self, url):
        """
        Function returns True or False based on whether the url has to be fetched or not. This is a great place to
        filter out crawler traps. Duplicated urls will be taken care of by frontier. You don't need to check for duplication
        in this method
        """
        parsed = urlparse(url)

        #How do we detect calendar traps???

        #original code from skeleton below:
        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            return False
        try:
            return ".ics.uci.edu" in parsed.hostname \
                   and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4" \
                                    + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
                                    + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
                                    + "|thmx|mso|arff|rtf|jar|csv" \
                                    + "|rm|smil|wmv|swf|wma|zip|rar|gz|pdf)$", parsed.path.lower())

        except TypeError:
            print("TypeError for ", parsed)
            return False

        
