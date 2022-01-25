from collections import defaultdict, deque

from sys import argv #for testing

N = 3 #n value for the n-grams
X = 4 #x value for 0 mod x; set to 1 to include all prints

'''
Find n-grams
Repurposed from the tokenizer from Assignment 1
'''
def find_grams(text: str) -> defaultdict:
    ngrams = []
    window = deque(maxlen=N)
    token = ''  #Current token being read

    for char in text:
        if(not char.isascii()):
            #Ignore non-ASCII characters
            continue
        if(char.isalnum()):
            token += char.lower()
        #a non-alphanumeric character denotes the end of a token
        elif (token != ''):
            window.append(token)
            token = ''
            if(len(window) >= N):
                ngrams.append(" ".join(window))

    #Be sure to process any tokens at the very end of the string
    if (token != ''):
        window.append(token)
        if(len(window) >= N):
            ngrams.append(" ".join(window))

    return ngrams

'''
Generate fingerprints
'''
def get_fingerprints(ngrams: list) -> set:
    fingerprints = set()
    for gram in ngrams:
        f = hash(gram)
        if(f % X == 0): #What happens if none of the prints qualify?
            fingerprints.add(f)
    return fingerprints

'''
Compare fingerprints
'''
def compare_prints(prints1: set, prints2: set) -> float:
    shared_prints = prints1.intersection(prints2)
    all_prints = prints1.union(prints2)
    return len(shared_prints) / len(all_prints)

'''
For testing
'''
if __name__ == "__main__":
    input_str = argv[1]
    grams = find_grams(input_str)
    print(grams)
    fingerprints1 = get_fingerprints(grams)
    print(fingerprints1)

    if(len(argv) > 2):
        input_str = argv[2]
        grams = find_grams(input_str)
        print(grams)
        fingerprints2 = get_fingerprints(grams)
        print(fingerprints2)
        print(compare_prints(fingerprints1, fingerprints2))

