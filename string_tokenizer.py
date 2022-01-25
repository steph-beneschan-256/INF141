from collections import defaultdict

'''
A basic tokenizer repurposed from Assignment 1
Take a string and tokenize it
'''
def tokenize(text) -> defaultdict:
    #Use token_dict to store frequency of each token in the input string
    token_dict = defaultdict(int)
    token = ''  #Current token being read

    for char in text:
        if(not char.isascii()):
            #Ignore non-ASCII characters; this ensures that all non-English characters are excluded
            continue
        if(char.isalnum()):
            token += char.lower()
        #a non-alphanumeric character denotes the end of a token
        elif (token != ''):
            token_dict[token] += 1
            token = ''

    #Be sure to process any tokens at the very end of the string
    if (token != ''):
        token_dict[token] += 1

    return token_dict