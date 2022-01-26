import logging
import re
from urllib.parse import urlparse

#It should be okay to import these, right...?
import os
from urllib.parse import urldefrag
from lxml import etree as etree #Would it be better to use BeautifulSoup?
from lxml import html
from lxml.html import fromstring
from lxml.html import soupparser
import pickle
from fingerprinter import get_fingerprints

#Import a couple of custom classes
from analytics_data import Analytics_Data
from frontier import Frontier
from string_tokenizer import tokenize


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
    FETCH_LIMIT = 100000 #Will only crawl this many URLs; should be useful for testing the analytics functionality

    def __init__(self, frontier, corpus):
        self.frontier = frontier
        self.corpus = corpus

    '''
    Save analytics data
    (not the same as writing the final analytics file at the end)
    This method is based on the save_frontier method in frontier.py
    
    (Note to self: confirm that we are allowed to write additional classes)
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

    '''
    Call this method when the crawling is complete
    (For testing, call it whenever the program exits)
    '''
    def log_analytics_data(self):
        self.analytics_data.log_analytics(self.frontier.fetched, self.frontier.traps)

    def start_crawling(self):
        """
        This method starts the crawling process which is scraping urls from the next available link in frontier and adding
        the scraped links to the frontier
        """

        #Note: need to reset analytics data when starting a new crawl
        self.load_analytics_data()

        while self.frontier.has_next_url() and ((self.FETCH_LIMIT <= 0) or (self.frontier.fetched < self.FETCH_LIMIT)):
        #while self.frontier.has_next_url():
            url = self.frontier.get_next_url()
            #logger.info("Fetching URL %s ... Fetched: %s, Queue size: %s", url, self.frontier.fetched, len(self.frontier))
            url_data = self.corpus.fetch_url(url)

            for next_link in self.extract_next_links(url_data):
                if self.is_valid(next_link):
                    if self.corpus.get_file_name(next_link) is not None:
                        self.frontier.add_url(next_link)

        self.analytics_data.log_analytics()

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

        '''
        Misc. notes on file parsing:
        - We can assume that we only need to decode using UTF-8 
            https://piazza.com/class/kxzmqq02jne6go?cid=78
        - 
        '''

        #Probably not the best way to check the file type of these checks, but I don't know what else to do...
        #Also, I think that we might be supposed to handle .txt files too, but I'm not sure if lxml has any tools to parse those
        doc = None
        if (file_type == 'text/html'):
            try:
                doc = html.fromstring(url_data["content"]) #Creates an lxml.html.HtmlElement object
            except:
                #apparently soup is slower than lxml?
                pass #TODO: handle HTML parsing errors somehow
        elif (file_type == 'text/xml'):
            #print(url_data["content"])
            try:
                doc = etree.fromstring(url_data["content"])
            except:
                #Getting lxml.etree.XMLSyntaxError
                pass #TODO: handle XML parsing errors
            
        if(doc == None):
            #Could not parse the document
            return outputLinks

        url = (url_data["final_url"] if (url_data["final_url"] != None) else url_data["url"])

        doc.make_links_absolute(url)

        text_no_markup = doc.text_content() #returns object of type lxml.etree._ElementUnicodeResult
        #text_no_markup = str(html.tostring(doc)) #Uncomment this line to include HTML markup is in the analytics, for testing

        '''
        Check if this page is a near-duplicate of a previously-examined page.
        If so, DO NOT assume that it is a trap,
        but don't return any of its outlinks or count it in the analytics
        '''
        fingerprints = get_fingerprints(text_no_markup)
        print("\tdetecting for duplication for {}".format(url))
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
            if((link != None) and self.is_valid(link_url)):
                outputLinks.append(link_url)
                valid_links += 1
        self.analytics_data.update_most_valid_outlinks(url, valid_links) #If this page doesn't break the record, then nothing will change

        return outputLinks

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
            print("help")
            return False

        #TODO: Check for repeating subdomains (maybe if a subdomain appears 3+ times in the URL, declare it invalid?)

        
        #How do we detect calendar traps???


        #https://docs.python.org/3/library/urllib.parse.html#urllib.parse.urlparse

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
