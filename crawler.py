import logging
import re
from urllib.parse import urlparse, urljoin

#Additional libraries
import os
from lxml import etree as etree
from lxml import html
from bs4 import BeautifulSoup
from lxml.html import soupparser
import pickle
from fingerprinter import get_fingerprints
from collections import defaultdict

#Import a couple of custom classes
from analytics_data import Analytics_Data
from string_tokenizer import tokenize

# Configures logging and outputting to file
logging.basicConfig(filename="./history.log", filemode='w', format='%(asctime)s (%(name)s) %(levelname)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',
                    level=logging.INFO)

logger = logging.getLogger(__name__)

class Crawler:
    """
    This class is responsible for scraping urls from the next available link in frontier and adding the scraped links to
    the frontier
    """

    URL_SIZE_LIMIT = 200 #Is this too high?
    #Directory/file name for analytics file
    #It's probably fastest to use a single file for the analytics data, right?
    #(This is not the file where the final analytics data will be written to once the crawl is finished)
    ANALYTICS_DIR_NAME = "analytics"
    ANALYTICS_FILE_NAME = os.path.join(".", ANALYTICS_DIR_NAME, "analytics_data.pkl")
    FETCH_LIMIT = 0 #Will only crawl this many URLs; should be useful for testing the analytics functionality

    def __init__(self, frontier, corpus):
        self.frontier = frontier
        self.corpus = corpus
        self.counter_links_crawled = 0
        self.counter_domain = defaultdict(int)
        #self.traps = []


    '''
    Save analytics data
    (not the same as writing the final analytics file at the end)
    This method is based on the save_frontier method in frontier.py
    '''
    def save_analytics_data(self):
        if(not os.path.exists(self.ANALYTICS_DIR_NAME)):
            os.makedirs(self.ANALYTICS_DIR_NAME)

        analytics_file = open(self.ANALYTICS_FILE_NAME, "wb")
        pickle.dump(self.analytics_data, analytics_file)

    '''
    Load analytics data from an existing file.
    This method is based on the load_frontier method in frontier.py
    '''
    def load_analytics_data(self):
        if os.path.isfile(self.ANALYTICS_FILE_NAME):
            analytics_file = open(self.ANALYTICS_FILE_NAME, "rb")
            self.analytics_data = pickle.load(analytics_file)
            logger.info("Loaded previous analytics data into memory.")
        else:
            logger.info("No previous analytics data found. Recording new data ...")
            self.analytics_data = Analytics_Data()

    def start_crawling(self):
        """
        This method starts the crawling process which is scraping urls from the next available link in frontier and adding
        the scraped links to the frontier
        """

        #Note: need to reset analytics data when starting a new crawl
        #TODO: Add reset function to the analytics data object, and call that here
        self.load_analytics_data()

        while self.frontier.has_next_url() and ((self.FETCH_LIMIT <= 0) or (self.frontier.fetched < self.FETCH_LIMIT)):
        #while self.frontier.has_next_url():
            url = self.frontier.get_next_url()

            #added code to check validity before fetching
            if not self.is_valid(url):
                continue

            logger.info("Fetching URL %s ... Fetched: %s, Queue size: %s", url, self.frontier.fetched, len(self.frontier))
            print("Fetching URL {} ... Fetched: {}, Queue size: {}".format(url, self.frontier.fetched, len(self.frontier)))
            url_data = self.corpus.fetch_url(url)

            #logger.info(url_data["content"].decode("utf-8"))
            # logger.info(url_data["content"].find(b'\0'))
            # logger.info(url_data["content"]) #for debugging
            # logger.info(url_data["content"].decode("utf-8")) #the string with all the null characters
            # a = url_data["content"].replace(b"\x00", b"")
            # logger.info(a.decode("utf-8"))

            for next_link in self.extract_next_links(url_data):
                if self.is_valid(next_link):
                    if self.corpus.get_file_name(next_link) is not None:
                        self.frontier.add_url(next_link)

        print("Crawling complete.")
        self.analytics_data.log_analytics(self.frontier.fetched, self.frontier.get_traps())

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

        if(url_data["content_type"] == None):
            return []

        '''
        Determine the whether the page is an HTML or XML document.
        Due to a presumed error in converting from bytes to str, content_type is usually prefixed by "b'" so we need to remove this.

        Sometimes the encoding isn't present
        '''
        content_type = url_data["content_type"].removeprefix("b\'") #https://docs.python.org/3.9/library/stdtypes.html?highlight=removeprefix#str.removeprefix
        file_type = content_type.split(';')[0] #usually content_type has both a filetype and an encoding, but sometimes the encoding is absent...
        #logger.info(url_data["content_type"])

        if(url_data["content"][:5] == b"<?xml"):
            logger.info("XML Prolog found")

        '''
        Try to parse the document content using lxml.
        If that does not work, try BeautifulSoup instead.
        '''
        doc = None
        try:
            #Try the HTML and XML parsers
            if(file_type == 'text/html'):
                doc = html.fromstring(url_data["content"])
            elif(file_type == 'text/xml'):
                doc = etree.fromstring(url_data["content"])
        except:
            try:
                #If the other parsers failed, then try the Beautiful Soup parser.
                doc = soupparser.fromstring(url_data["content"]) 
            except:
                try:
                    #Check if the bytes contain excessive null terminators.
                    #If so, eliminate them.
                    if(url_data["content"].count(b'\x00') > 0):
                        logger.info("Excess null terminators found. Eliminating now.")
                        content_cleaned = url_data["content"].replace(b'\x00', b'')
                        doc = soupparser.fromstring(url_data[content_cleaned])
                except:
                    logger.info("Failed to parse the document.")
                    return []
            
        if(doc == None):
            #Could not parse the document
            return outputLinks

        url = (url_data["final_url"] if (url_data["final_url"] != None) else url_data["url"])

        self.analytics_data.new_url_downloaded(url)

        #For analytics
        subdomains = self.extract_subdomains(url)
        for subdomain in subdomains:
            self.analytics_data.update_subdomain_url_count(subdomain)

        doc.make_links_absolute(url)

        text_no_markup = doc.text_content() #returns object of type lxml.etree._ElementUnicodeResult
        #text_no_markup = str(html.tostring(doc)) #Uncomment this line to include HTML markup is in the analytics, for testing

        '''
        Check if this page is a near-duplicate of a previously-examined page.
        If so, DO NOT assume that it is a trap,
        but don't return any of its outlinks or count it in the analytics
        '''
        fingerprints = get_fingerprints(text_no_markup)
        logger.info("\tchecking duplication for {}".format(url))
        #The is_near_duplicate method should handle detecting and registering traps
        #But maybe it should be split into multiple functions?
        if(self.frontier.is_near_duplicate(url, fingerprints)):
            return []

        '''
        Determine the frequency of each word in the page, as well
        as the overall word count.
        Then, update the analytics data accordingly.
        '''
        page_token_dict = tokenize(text_no_markup)
        word_count = 0
        for token in page_token_dict.keys():
            token_instances = page_token_dict[token]
            word_count += token_instances
            self.analytics_data.update_word_frequency(token, token_instances)
        self.analytics_data.update_longest_page(url, word_count) #If this page doesn't break the record, then nothing will change
        
        '''
        Find any and all valid outlinks within the page.
        Also, update the analytics data if this page breaks
        the record for most valid outlinks
        '''
        valid_links = 0 #For the analytics
        for link in doc.iterlinks():
            link_url = link[2] #According to the lxml documentation, link_url should be a tuple: (element, attribute, link, pos)
            #Is it possible to detect duplication of each new link here?
            if(link != None):
                self.counter_links_crawled += 1
                if(self.is_valid(link_url)):
                    outputLinks.append(link_url)
                    valid_links += 1
        self.analytics_data.update_most_valid_outlinks(url, valid_links) #If this page doesn't break the record, then nothing will change

        return outputLinks

    '''
    Helper function to return list of subdomains in a given URL
    '''
    def extract_subdomains(self, url):
        parsed = urlparse(url)
        netloc = parsed.netloc
        domain = netloc.replace('www.','')
        domain_split = domain.split('.')
        subdomains = []
        for i in range(len(domain_split) - 2):                          # exlude the 2 root domains: uci.edu and edu
            subdomains.append('.'.join(domain_split[i:]))
        return subdomains

    def is_valid(self, url):
        """
        Function returns True or False based on whether the url has to be fetched or not. This is a great place to
        filter out crawler traps. Duplicated urls will be taken care of by frontier. You don't need to check for duplication
        in this method
        """
        if(len(url) > self.URL_SIZE_LIMIT):
            #print("trap detected in {}; url too long".format(url))
            return False

        #Using the frontier to store trap data
        trimmed = self.frontier.trim_url(url)
        if(trimmed in self.frontier.traps):
            logger.info("\tTrap found. Ignoring.")
            return False

        #TODO: Check for repeating subdomains (maybe if a subdomain appears 3+ times in the URL, declare it invalid?)
        #Cora said that it should be fine to use regex for this
        subdomains = self.extract_subdomains(url)
        
        #original code from skeleton below:
        parsed = urlparse(url)
        domain = parsed.netloc + parsed.path

        self.counter_domain[domain] += 1

        # to avoid calendar trap, track the access amounts
        # the arbitrary and intuitive number here I put 20
        if self.counter_domain[domain] >= 20:
            self.frontier.traps.add(domain)
        
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
            logger.info("TypeError for ", parsed)
            #print("TypeError for ", parsed)
            return False
