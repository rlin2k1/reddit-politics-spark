#!/usr/bin/env python3

"""Clean comment text for easier parsing."""

from __future__ import print_function

import re
import string
import argparse


__author__ = ""
__email__ = ""

# Depending on your implementation,
# this data may or may not be useful.
# Many students last year found it redundant.
_CONTRACTIONS = {
    "tis": "'tis",
    "aint": "ain't",
    "amnt": "amn't",
    "arent": "aren't",
    "cant": "can't",
    "couldve": "could've",
    "couldnt": "couldn't",
    "didnt": "didn't",
    "doesnt": "doesn't",
    "dont": "don't",
    "hadnt": "hadn't",
    "hasnt": "hasn't",
    "havent": "haven't",
    "hed": "he'd",
    "hell": "he'll",
    "hes": "he's",
    "howd": "how'd",
    "howll": "how'll",
    "hows": "how's",
    "id": "i'd",
    "ill": "i'll",
    "im": "i'm",
    "ive": "i've",
    "isnt": "isn't",
    "itd": "it'd",
    "itll": "it'll",
    "its": "it's",
    "mightnt": "mightn't",
    "mightve": "might've",
    "mustnt": "mustn't",
    "mustve": "must've",
    "neednt": "needn't",
    "oclock": "o'clock",
    "ol": "'ol",
    "oughtnt": "oughtn't",
    "shant": "shan't",
    "shed": "she'd",
    "shell": "she'll",
    "shes": "she's",
    "shouldve": "should've",
    "shouldnt": "shouldn't",
    "somebodys": "somebody's",
    "someones": "someone's",
    "somethings": "something's",
    "thatll": "that'll",
    "thats": "that's",
    "thatd": "that'd",
    "thered": "there'd",
    "therere": "there're",
    "theres": "there's",
    "theyd": "they'd",
    "theyll": "they'll",
    "theyre": "they're",
    "theyve": "they've",
    "wasnt": "wasn't",
    "wed": "we'd",
    "wedve": "wed've",
    "well": "we'll",
    "were": "we're",
    "weve": "we've",
    "werent": "weren't",
    "whatd": "what'd",
    "whatll": "what'll",
    "whatre": "what're",
    "whats": "what's",
    "whatve": "what've",
    "whens": "when's",
    "whered": "where'd",
    "wheres": "where's",
    "whereve": "where've",
    "whod": "who'd",
    "whodve": "whod've",
    "wholl": "who'll",
    "whore": "who're",
    "whos": "who's",
    "whove": "who've",
    "whyd": "why'd",
    "whyre": "why're",
    "whys": "why's",
    "wont": "won't",
    "wouldve": "would've",
    "wouldnt": "wouldn't",
    "yall": "y'all",
    "youd": "you'd",
    "youll": "you'll",
    "youre": "you're",
    "youve": "you've"
}

# You may need to write regular expressions.

# Task 1
def newlines_and_tabs_to_spaces(text):
    pattern = '[\n\t]+'
    replace = ' '
    new_text = re.sub(pattern, replace, text)
    return new_text
    # print(newlines_and_tabs_to_spaces("hey\n\t\n\twassup\n"))

# Task 5
def split_single_space(text):
    tokens = re.split(' ', text)
    # Remove empty tokens
    # Source: https://stackoverflow.com/questions/16099694/how-to-remove-empty-string-in-a-list/16099706
    tokens = list(filter(None, tokens))
    return tokens
    # print(split_single_space("get some   tokens     man"))

# Task 7
def remove_punctuation(text):
    # pattern = '(?![a-zA-Z0-9]+)[^a-zA-Z0-9 ]'
    # pattern = '(?![a-zA-Z0-9])[^a-zA-Z0-9 ]+(?![ ]|$)'
    # pattern = '(?![a-zA-Z0-9])[^a-zA-Z0-9 ](?![a-zA-Z0-9]| |$)'
    # pattern = ' [^a-zA-Z0-9 ]+|^[^a-zA-Z0-9 ]+'
    pattern = ' [^a-zA-Z0-9 ] '
    replace = ' '
    new_text = re.sub(pattern, replace, text)
    return new_text

def sanitize(text):
    """Do parse the text in variable "text" according to the spec, and return
    a LIST containing FOUR strings 
    1. The parsed text.
    2. The unigrams
    3. The bigrams
    4. The trigrams
    """

    # YOUR CODE GOES BELOW:

    # TODO: Order of tasks: 8, 7, 6, 5 ?? <-- WRONG!
    # TODO: Remove special characters (keep other punctuation)
    # TODO: Double check punctuation symbols for separation

    return [parsed_text, unigrams, bigrams, trigrams]


if __name__ == "__main__":
    # This is the Python main function.
    # You should be able to run
    # python cleantext.py <filename>
    # and this "main" function will open the file,
    # read it line by line, extract the proper value from the JSON,
    # pass to "sanitize" and print the result as a list.

    # YOUR CODE GOES BELOW.

    # We are "requiring" your write a main function so you can
    # debug your code. It will not be graded.

    # print(newlines_and_tabs_to_spaces("hey\n\t\n\twassup\n"))
    # print(split_single_space("get some   tokens     man"))
    print(remove_punctuation(".. ,yo ;hey ! #$@ how are you?? app-le?!"))
    print(remove_punctuation(". . , yo ; hey ! #$ how are you ? ? app-le ? ! ."))

