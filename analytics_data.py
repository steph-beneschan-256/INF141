import os
from collections import defaultdict

'''
This class is meant to help manage data for the analytics,
and then print the analytics after crawling is finished
'''

class Analytics_Data:

    ANALYTICS_FILE_NAME = os.path.join(".", "analytics.txt")
    # set of stop words, since set has a O(1) time complexity when indexing
    STOP_WORDS = {"a", "able", "about", "above", "abst", "accordance", "according", "accordingly", "across", "act", "actually", "added", "adj", "affected", "affecting", "affects", "after", "afterwards", "again", "against", "ah", "all", "almost", "alone", "along", "already", "also", "although", "always", "am", "among", "amongst", "an", "and", "announce", "another", "any", "anybody", "anyhow", "anymore", "anyone", "anything", "anyway", "anyways", "anywhere", "apparently", "approximately", "are", "aren", "arent", "arise", "around", "as", "aside", "ask", "asking", "at", "auth", "available", "away", "awfully", "b", "back", "be", "became", "because", "become", "becomes", "becoming", "been", "before", "beforehand", "begin", "beginning", "beginnings", "begins", "behind", "being", "believe", "below", "beside", "besides", "between", "beyond", "biol", "both", "brief", "briefly", "but", "by", "c", "ca", "came", "can", "cannot", "can't", "cause", "causes", "certain", "certainly", "co", "com", "come", "comes", "contain", "containing", "contains", "could", "couldnt", "d", "date", "did", "didn't", "different", "do", "does", "doesn't", "doing", "done", "don't", "down", "downwards", "due", "during", "e", "each", "ed", "edu", "effect", "eg", "eight", "eighty", "either", "else", "elsewhere", "end", "ending", "enough", "especially", "et", "et-al", "etc", "even", "ever", "every", "everybody", "everyone", "everything", "everywhere", "ex", "except", "f", "far", "few", "ff", "fifth", "first", "five", "fix", "followed", "following", "follows", "for", "former", "formerly", "forth", "found", "four", "from", "further", "furthermore", "g", "gave", "get", "gets", "getting", "give", "given", "gives", "giving", "go", "goes", "gone", "got", "gotten", "h", "had", "happens", "hardly", "has", "hasn't", "have", "haven't", "having", "he", "hed", "hence", "her", "here", "hereafter", "hereby", "herein", "heres", "hereupon", "hers", "herself", "hes", "hi", "hid", "him", "himself", "his", "hither", "home", "how", "howbeit", "however", "hundred", "i", "id", "ie", "if", "i'll", "im", "immediate", "immediately", "importance", "important", "in", "inc", "indeed", "index", "information", "instead", "into", "invention", "inward", "is", "isn't", "it", "itd", "it'll", "its", "itself", "i've", "j", "just", "k", "keep", "keeps", "kept", "kg", "km", "know", "known", "knows", "l", "largely", "last", "lately", "later", "latter", "latterly", "least", "less", "lest", "let", "lets", "like", "liked", "likely", "line", "little", "'ll", "look", "looking", "looks", "ltd", "m", "made", "mainly", "make", "makes", "many", "may", "maybe", "me", "mean", "means", "meantime", "meanwhile", "merely", "mg", "might", "million", "miss", "ml", "more", "moreover", "most", "mostly", "mr", "mrs", "much", "mug", "must", "my", "myself", "n", "na", "name", "namely", "nay", "nd", "near", "nearly", "necessarily", "necessary", "need", "needs", "neither", "never", "nevertheless", "new", "next", "nine", "ninety", "no", "nobody", "non", "none", "nonetheless", "noone", "nor", "normally", "nos", "not", "noted", "nothing", "now", "nowhere", "o", "obtain", "obtained", "obviously", "of", "off", "often", "oh", "ok", "okay", "old", "omitted", "on", "once", "one", "ones", "only", "onto", "or", "ord", "other", "others", "otherwise", "ought", "our", "ours", "ourselves", "out", "outside", "over", "overall", "owing", "own", "p", "page", "pages", "part", "particular", "particularly", "past", "per", "perhaps", "placed", "please", "plus", "poorly", "possible", "possibly", "potentially", "pp", "predominantly", "present", "previously", "primarily", "probably", "promptly", "proud", "provides", "put", "q", "que", "quickly", "quite", "qv", "r", "ran", "rather", "rd", "re", "readily", "really", "recent", "recently", "ref", "refs", "regarding", "regardless", "regards", "related", "relatively", "research", "respectively", "resulted", "resulting", "results", "right", "run", "s", "said", "same", "saw", "say", "saying", "says", "sec", "section", "see", "seeing", "seem", "seemed", "seeming", "seems", "seen", "self", "selves", "sent", "seven", "several", "shall", "she", "shed", "she'll", "shes", "should", "shouldn't", "show", "showed", "shown", "showns", "shows", "significant", "significantly", "similar", "similarly", "since", "six", "slightly", "so", "some", "somebody", "somehow", "someone", "somethan", "something", "sometime", "sometimes", "somewhat", "somewhere", "soon", "sorry", "specifically", "specified", "specify", "specifying", "still", "stop", "strongly", "sub", "substantially", "successfully", "such", "sufficiently", "suggest", "sup", "sure", "t", "take", "taken", "taking", "tell", "tends", "th", "than", "thank", "thanks", "thanx", "that", "that'll", "thats", "that've", "the", "their", "theirs", "them", "themselves", "then", "thence", "there", "thereafter", "thereby", "thered", "therefore", "therein", "there'll", "thereof", "therere", "theres", "thereto", "thereupon", "there've", "these", "they", "theyd", "they'll", "theyre", "they've", "think", "this", "those", "thou", "though", "thoughh", "thousand", "throug", "through", "throughout", "thru", "thus", "til", "tip", "to", "together", "too", "took", "toward", "towards", "tried", "tries", "truly", "try", "trying", "ts", "twice", "two", "u", "un", "under", "unfortunately", "unless", "unlike", "unlikely", "until", "unto", "up", "upon", "ups", "us", "use", "used", "useful", "usefully", "usefulness", "uses", "using", "usually", "v", "value", "various", "'ve", "very", "via", "viz", "vol", "vols", "vs", "w", "want", "wants", "was", "wasnt", "way", "we", "wed", "welcome", "we'll", "went", "were", "werent", "we've", "what", "whatever", "what'll", "whats", "when", "whence", "whenever", "where", "whereafter", "whereas", "whereby", "wherein", "wheres", "whereupon", "wherever", "whether", "which", "while", "whim", "whither", "who", "whod", "whoever", "whole", "who'll", "whom", "whomever", "whos", "whose", "why", "widely", "willing", "wish", "with", "within", "without", "wont", "words", "world", "would", "wouldnt", "www", "x", "y", "yes", "yet", "you", "youd", "you'll", "your", "youre", "yours", "yourself", "yourselves", "you've", "z", "zero"}

    def __init__(self):
        self.subdomain_url_count = defaultdict(int) #URLs and subdomain counts???
        self.most_valid_outlinks_url = "unknwown"
        self.most_valid_outlinks_count = 0
        self.urls_downloaded = set()
        self.longest_page_url = "unknown"
        self.longest_page_length = 0
        self.word_frequencies = defaultdict(int) #Words and their frequencies (across the entire corpus)

    '''
    Update the number of URLs processed for a given subdomain
    (Assuming that duplicate URLs are not an issue...)
    '''
    def update_subdomain_url_count(self, subdomain):
        if (subdomain not in self.subdomain_url_count.keys()):
            self.subdomain_url_count[subdomain] = 0
        self.subdomain_url_count[subdomain] += 1

    '''
    Take the URL of a page, and that page's number of valid outlinks
    If it breaks the record for most valid outlinks, then record the URL and outlink count
    Otherwise, do nothing
    '''
    def update_most_valid_outlinks(self, new_url, outlink_count):
        if(outlink_count > self.most_valid_outlinks_count):
            self.most_valid_outlinks_count = outlink_count
            self.most_valid_outlinks_url = new_url

    def new_url_downloaded(self, new_url):
        self.urls_downloaded.add(new_url)

    '''
    Register the URL of a newly-identified trap
    '''
    def update_traps(self, trap_url):
        self.traps.append(trap_url)

    '''
    Take the URL of a page, and that page's word count.
    If it breaks the record for longest page, then record the URL and word count
    Otherwise, do nothing
    '''
    def update_longest_page(self, new_url, length):
        if(length > self.longest_page_length):
            self.longest_page_length = length
            self.longest_page_url = new_url

    '''
    Update the word frequency of a single word by a specific amount
    '''
    ## words still tend to represent crawler traps i.e numbers, months, etc.
    def update_word_frequency(self, word, amount=1):
        if(word not in self.STOP_WORDS): #  will not add if word is a stop word
            self.word_frequencies[word] += amount

    def log_analytics(self, fetched, traps):

        output_file = open(self.ANALYTICS_FILE_NAME, 'w')
    
        subdomains = list(self.subdomain_url_count.keys())
        sort_key = lambda x: (-self.subdomain_url_count[x], x)
        subdomains.sort(key=sort_key)
        output_file.write("Number of URLs processed for each subdomain:")
        for subdomain in subdomains:
            output_file.write("\n\t{0:20}{1}".format(subdomain + ':', self.subdomain_url_count[subdomain]))

        output_file.write("\nPage with most valid outlinks:")
        output_file.write("\n\tURL: {}".format(self.most_valid_outlinks_url))
        output_file.write("\n\tNumber of Outlinks: {}".format(self.most_valid_outlinks_count))

        output_file.write("\nURLs Downloaded:")
        for url in self.urls_downloaded:
            try:
                output_file.write("\n\t{}".format(url))
            except UnicodeEncodeError:
                output_file.write("\n\t{}".format(url.encode("utf-8")))

        output_file.write("\nIdentified Traps:")
        for trap in traps:
            try:
                output_file.write("\n\t{}".format(trap))
            except UnicodeEncodeError:
                output_file.write("\n\t{}".format(trap.encode("utf-8")))

        output_file.write("\nPage with highest word count:")
        output_file.write("\n\tURL: {}".format(self.longest_page_url))
        output_file.write("\n\tWord Count: {}".format(self.longest_page_length))

        output_file.write("\nNumber of URLs fetched:")
        output_file.write("\n\t{}".format(fetched))

        #Reused code from Assignment 1A
        #As before, the idea to use tuples for tie-breaking came from this resource: https://stackoverflow.com/a/54396160
        words = list(self.word_frequencies.keys())
        sort_key = lambda x: (-self.word_frequencies[x], x)
        words.sort(key=sort_key)
        words = words[:50]
        output_file.write("\nFifty most common words:")
        for word in words:
            output_file.write("\n\t{0:20}{1}".format(word + ':', self.word_frequencies[word]))

        output_file.close()
        
