import os

'''
This class is meant to help manage data for the analytics,
and then print the analytics after crawling is finished
'''

class Analytics_Data:

    ANALYTICS_FILE_NAME = os.path.join(".", "analytics.txt")

    def __init__(self):
        self.subdomain_url_count = dict() #URLs and subdomain counts???
        self.most_valid_outlinks_url = "unknwown"
        self.most_valid_outlinks_count = 0
        self.longest_page_url = "unknown"
        self.longest_page_length = 0
        self.word_frequencies = dict() #Words and their frequencies (across the entire corpus)


    '''
    Take the URL of a page, and that page's number of valid outlinks
    If it breaks the record for most valid outlinks, then record the URL and outlink count
    Otherwise, do nothing
    '''
    def update_most_valid_outlinks(self, new_url, outlink_count):
        if(outlink_count > self.most_valid_outlinks_count):
            self.most_valid_outlinks_count = outlink_count
            self.most_valid_outlinks_url = new_url

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
    def update_word_frequency(self, word, amount=1):
        if(word not in self.word_frequencies.keys()):
            self.word_frequencies[word] = 0
        self.word_frequencies[word] += amount

    def log_analytics(self, fetched=0, traps=set()):

        output_file = open(self.ANALYTICS_FILE_NAME, 'w')

        output_file.write("subdomain data placeholder")

        output_file.write("\nPage with most valid outlinks:")
        output_file.write("\n\tURL: {}".format(self.most_valid_outlinks_url))
        output_file.write("\n\tNumber of Outlinks: {}".format(self.most_valid_outlinks_count))
        
        output_file.write("\nIdentified Traps:")
        for trap in traps:
            output_file.write("\n\t{}".format(trap))

        output_file.write("\nPage with highest word count:")
        output_file.write("\n\tURL: {}".format(self.longest_page_url))
        output_file.write("\n\tWord Count: {}".format(self.longest_page_length))

        #Time to calculate the goddamn word frequencies
        #We can add the stopword checking later, can't we...?

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
        
