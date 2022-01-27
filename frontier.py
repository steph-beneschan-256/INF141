import logging
import os
from collections import deque
import pickle

import fingerprinter #Custom line
from collections import defaultdict #Custom line (Why is it always defaultdict?)
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

class Frontier:
    """
    This class acts as a representation of a frontier. It has method to add a url to the frontier, get the next url and
    check if the frontier has any more urls. Additionally, it has methods to save the current state of the frontier and
    load existing state

    Attributes:
        urls_queue: A queue of urls to be download by crawlers
        urls_set: A set of urls to avoid duplicated urls
        fetched: the number of fetched urls so far
    """

    # File names to be used when loading and saving the frontier state
    FRONTIER_DIR_NAME = "frontier_state"
    URL_QUEUE_FILE_NAME = os.path.join(".", FRONTIER_DIR_NAME, "url_queue.pkl")
    URL_SET_FILE_NAME = os.path.join(".", FRONTIER_DIR_NAME, "url_set.pkl")
    FETCHED_FILE_NAME = os.path.join(".", FRONTIER_DIR_NAME, "fetched.pkl")

    FINGERPRINT_FILE_NAME = os.path.join(".", FRONTIER_DIR_NAME, "fingerprints.pkl") #Custom line
    FINGERPRINT_OVERLAP_THRESHOLD = 0.99 #Custom line
    MAX_DUPES_ALLOWED = 50 #Number of near-duplicates permitted before a URL is deemed a trap


    def __init__(self):
        self.urls_queue = deque()
        self.urls_set = set()
        self.fetched = 0

        #Record the fingerprints for each crawled page, for each trimmed URL
        self.fingerprint_list = defaultdict(list)
        #Track the number of near-duplicates found for a given, trimmed URL
        self.near_dupes = defaultdict(int)
        #Keep a set of traps (stored as URLs without a query or fragment ID)
        self.traps = set()

    def add_url(self, url):
        """
        Adds a url to the urls queue
        :param url: the url to be added
        """
        if not self.is_duplicate(url):
            self.urls_queue.append(url)
            self.urls_set.add(url)

    def is_duplicate(self, url):
        return url in self.urls_set

    '''
    Custom function to remove query and fragment from URL
    '''
    def trim_url(self, url):
        parsed = urlparse(url)
        trimmed = parsed._replace(params="", query="", fragment="")
        return trimmed.geturl()

    '''
    Custom function to check whether a given set of fingerprints was already found in a different page
    '''
    def is_near_duplicate(self, url, prints: set):
        #This is definitely inefficient, but for early testing it should suffice, right...?
        #Maybe have it be one list of fingerprint sets per trimmed URL
        trimmed = self.trim_url(url)
        for saved_prints in self.fingerprint_list[trimmed]:
            if(fingerprinter.compare_prints(prints, saved_prints)):
                self.near_dupes[trimmed] += 1
                if(self.near_dupes[trimmed] > self.MAX_DUPES_ALLOWED):
                    #If it has too many near-duplicate pages, then consider it a trap
                    self.traps.add(trimmed)
                    #For some reason, this line is getting printed 2+ times for the exact same URL. Why?!?!?!
                    print("Trap detected in {}; too many near-duplicates".format(trimmed))
                self.register_fingerprints(url, prints)
                return True
        self.register_fingerprints(url, prints)
        return False

    '''
    Custom function to register a new set of fingerprints (from an individual document/web page)
    '''
    def register_fingerprints(self, new_url, new_prints: set):
        trimmed = self.trim_url(new_url)
        self.fingerprint_list[trimmed].append(new_prints)

    def get_next_url(self):
        """
        Returns the next url to be fetched
        """
        if self.has_next_url():
            self.fetched += 1
            return self.urls_queue.popleft()

    def has_next_url(self):
        """
        Returns true if there are more urls in the queue, otherwise false
        """
        return len(self.urls_queue) != 0

    def save_frontier(self):
        """
        saves the current state of the frontier in two files using pickle
        """
        if not os.path.exists(self.FRONTIER_DIR_NAME):
            os.makedirs(self.FRONTIER_DIR_NAME)

        url_queue_file = open(self.URL_QUEUE_FILE_NAME, "wb")
        url_set_file = open(self.URL_SET_FILE_NAME, "wb")
        fetched_file = open(self.FETCHED_FILE_NAME, "wb")
        fingerprint_file = open(self.FINGERPRINT_FILE_NAME, "wb") #Custom line
        pickle.dump(self.urls_queue, url_queue_file)
        pickle.dump(self.urls_set, url_set_file)
        pickle.dump(self.fetched, fetched_file)
        pickle.dump(self.fingerprint_list, fingerprint_file) #Custom line

    def load_frontier(self):
        """
        loads the previous state of the frontier into memory, if exists
        """
        if os.path.isfile(self.URL_QUEUE_FILE_NAME) and os.path.isfile(self.URL_SET_FILE_NAME) and\
                os.path.isfile(self.FETCHED_FILE_NAME):
            try:
                self.urls_queue = pickle.load(open(self.URL_QUEUE_FILE_NAME, "rb"))
                self.urls_set = pickle.load(open(self.URL_SET_FILE_NAME, "rb"))
                self.fetched = pickle.load(open(self.FETCHED_FILE_NAME, "rb"))
                self.fingerprint_list = pickle.load(open(self.FINGERPRINT_FILE_NAME, "rb")) #Custom line
                logger.info("Loaded previous frontier state into memory. Fetched: %s, Queue size: %s", self.fetched,
                            len(self.urls_queue))
            except:
                pass
        else:
            logger.info("No previous frontier state found. Starting from the seed URL ...")
            self.add_url("http://www.ics.uci.edu/")

    def __len__(self):
        return len(self.urls_queue)

