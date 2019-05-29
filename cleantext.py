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

# Task 2
def remove_url(text):
    """
    4 Regex Expressions Separated By '|'
    1. Matches Brackets and (http://) Replaces With String in Brackets
    2. Matches Brackets and (https://) Replaces with String in Brackets
    3. Matches https:// Replaces With Empty String
    4. Matches http:// Replaces With Empty String
    """
    return re.sub(r'\[([^)]*)]\([^)]*\)|https://([^\s]+)|http://([^\s]+)', r'\1', text)

# Task 5
def split_single_space(text):
    tokens = re.split(' ', text)
    # Remove empty tokens
    # Source: https://stackoverflow.com/questions/16099694/how-to-remove-empty-string-in-a-list/16099706
    tokens = list(filter(None, tokens))
    return tokens
    # print(split_single_space("get some   tokens     man"))

# Task 6
def separate_punctuation(tokens_list):
    new_tokens_list = []
    for token in tokens_list:
        length = len(token)
        left_substring = ""
        middle_substring = ""
        right_substring = ""
        # Iterate left to right until we hit an alphanumeric char
        for index in range(0, length):
            if token[index].isalnum(): # Found alphanumeric char
                middle_substring = token[index::] # WE WILL USE THIS FOR THE NEXT LOOP.
                break
            else:
                left_substring += token[index]
        # Iterate right to left using middle_substring as the new "token"
        length = len(middle_substring)
        for index in range(length-1, -1, -1): # Iterate in reverse
            if middle_substring[index].isalnum():
                middle_substring = middle_substring[0:index+1]
                break
            else:
                right_substring = middle_substring[index] + right_substring
        for char in left_substring:
            new_tokens_list.append(char)
        new_tokens_list.append(middle_substring)
        for char in right_substring:
            new_tokens_list.append(char)
    new_tokens_list = list(filter(None, new_tokens_list)) # Remove empty tokens
    return new_tokens_list


# Task 7
def remove_punctuation(tokens_list):
    punctuations = ['.', ',', '!', '?', ';', ':']
    new_tokens_list = []
    for token in tokens_list:
        length = len(token)
        is_alphanumeric = token.isalnum()
        if length == 1 and not is_alphanumeric and token not in punctuations:
            continue
        else:
            new_tokens_list.append(token)
    return new_tokens_list

def make_unigrams(text):
    remove = string.punctuation
    remove = remove.replace("#", "")
    remove = remove.replace("$", "")
    return ' '.join(filter(lambda x: x not in remove, text))

def make_bigrams(tokens_list):
    punctuations = ['.', ',', '!', '?', ';', ':']
    size = len(tokens_list)
    bigrams = ""
    if size < 2:
        return bigrams
    add_space_to_front = False
    for i in range(0, size-1): # First index to second-to-last index
        if tokens_list[i] in punctuations or tokens_list[i+1] in punctuations:
            continue # Don't add punctuations

        if add_space_to_front:
            bigrams += " "
        bigrams += tokens_list[i] + "_" + tokens_list[i+1]
        add_space_to_front = True
    return bigrams

def make_trigrams(tokens_list):
    punctuations = ['.', ',', '!', '?', ';', ':']
    size = len(tokens_list)
    trigrams = ""
    if size < 3:
        return trigrams
    add_space_to_front = False
    for i in range(0, size-2): # First index to third-to-last index
        if tokens_list[i] in punctuations or tokens_list[i+1] in punctuations or tokens_list[i+2] in punctuations:
            continue # Don't add punctuations

        if add_space_to_front:
            trigrams += " "
        trigrams += tokens_list[i] + "_" + tokens_list[i+1] + "_" + tokens_list[i+2]
        add_space_to_front = True
    return trigrams

def sanitize(text):
    """Do parse the text in variable "text" according to the spec, and return
    a LIST containing FOUR strings 
    1. The parsed text.
    2. The unigrams
    3. The bigrams
    4. The trigrams
    """
    # YOUR CODE GOES BELOW:
    text = newlines_and_tabs_to_spaces(text)
    text = remove_url(text)
    text = text.lower()
    tokens_list = split_single_space(text)
    tokens_list = separate_punctuation(tokens_list)
    tokens_list = remove_punctuation(tokens_list)
    parsed_text = " ".join(tokens_list) # Makes List to String
    unigrams = make_unigrams(tokens_list)
    bigrams = make_bigrams(tokens_list)
    trigrams = make_trigrams(tokens_list)
    return unigrams.split(" ") + bigrams.split(" ") + trigrams.split(" ")
    # return [parsed_text, unigrams, bigrams, trigrams]

########################################################################################################################################################
########################################################################################################################################################

def test_sanitize():
    num_tests = 0

    arg = "I'm afraid I can't explain myself, sir. Because I am not myself, you see?"
    my_out = sanitize(arg)
    tool_out1 = "i'm afraid i can't explain myself , sir . because i am not myself , you see ?"
    tool_out2 = "i'm afraid i can't explain myself sir because i am not myself you see"
    tool_out3 = "i'm_afraid afraid_i i_can't can't_explain explain_myself because_i i_am am_not not_myself you_see"
    tool_out4 = "i'm_afraid_i afraid_i_can't i_can't_explain can't_explain_myself because_i_am i_am_not am_not_myself"
    tool_out = [tool_out1, tool_out2, tool_out3, tool_out4]
    if tool_out != my_out:
        print("TEST 1 FAILED...\n")
        print("INPUT: " + arg)
        print("Parsed: " + my_out[0])
        print("Unigrams: " + my_out[1])
        print("Bigrams: " + my_out[2])
        print("Trigrams: " + my_out[3])
        return
    num_tests += 1

    arg = "! ;h.o,w_ are you?? app-le?!"
    my_out = sanitize(arg)
    tool_out1 = "! ; h.o,w are you ? ? app-le ? !"
    tool_out2 = "h.o,w are you app-le"
    tool_out3 = "h.o,w_are are_you"
    tool_out4 = "h.o,w_are_you"
    tool_out = [tool_out1, tool_out2, tool_out3, tool_out4]
    if tool_out != my_out:
        print("TEST 2 FAILED...\n")
        print("INPUT: " + arg)
        print("Parsed: " + my_out[0])
        print("Unigrams: " + my_out[1])
        print("Bigrams: " + my_out[2])
        print("Trigrams: " + my_out[3])
        return
    num_tests += 1

    arg = "[Let](https://www.merriam-webster.com/dictionary/let) could mean loads of things, including \"to give opportunity to *or failure to prevent*.\" It's ambiguous at best. I mean, those women *let* CK jack off in front of them. You're right that the rules are different if you're wealthy, but I'm not sure I'm okay with that. "
    my_out = sanitize(arg)
    tool_out1 = "let could mean loads of things , including to give opportunity to or failure to prevent . it's ambiguous at best . i mean , those women let ck jack off in front of them . you're right that the rules are different if you're wealthy , but i'm not sure i'm okay with that ."
    tool_out2 = "let could mean loads of things including to give opportunity to or failure to prevent it's ambiguous at best i mean those women let ck jack off in front of them you're right that the rules are different if you're wealthy but i'm not sure i'm okay with that"
    tool_out3 = "let_could could_mean mean_loads loads_of of_things including_to to_give give_opportunity opportunity_to to_or or_failure failure_to to_prevent it's_ambiguous ambiguous_at at_best i_mean those_women women_let let_ck ck_jack jack_off off_in in_front front_of of_them you're_right right_that that_the the_rules rules_are are_different different_if if_you're you're_wealthy but_i'm i'm_not not_sure sure_i'm i'm_okay okay_with with_that"
    tool_out4 = "let_could_mean could_mean_loads mean_loads_of loads_of_things including_to_give to_give_opportunity give_opportunity_to opportunity_to_or to_or_failure or_failure_to failure_to_prevent it's_ambiguous_at ambiguous_at_best those_women_let women_let_ck let_ck_jack ck_jack_off jack_off_in off_in_front in_front_of front_of_them you're_right_that right_that_the that_the_rules the_rules_are rules_are_different are_different_if different_if_you're if_you're_wealthy but_i'm_not i'm_not_sure not_sure_i'm sure_i'm_okay i'm_okay_with okay_with_that"
    tool_out = [tool_out1, tool_out2, tool_out3, tool_out4]
    if tool_out != my_out:
        print("TEST 3 FAILED...\n")
        print("INPUT: " + arg)
        print("Parsed: " + my_out[0])
        print("Unigrams: " + my_out[1])
        print("Bigrams: " + my_out[2])
        print("Trigrams: " + my_out[3])
        return
    num_tests += 1

    arg = ".,1!2@3#4$5%6^7&8*9(0.,"
    my_out = sanitize(arg)
    tool_out1 = ". , 1!2@3#4$5%6^7&8*9(0 . ,"
    tool_out2 = "1!2@3#4$5%6^7&8*9(0"
    tool_out3 = ""
    tool_out4 = ""
    tool_out = [tool_out1, tool_out2, tool_out3, tool_out4]
    if tool_out != my_out:
        print("TEST 4 FAILED...\n")
        print("INPUT: " + arg)
        print("Parsed: " + my_out[0])
        print("Unigrams: " + my_out[1])
        print("Bigrams: " + my_out[2])
        print("Trigrams: " + my_out[3])
        return
    num_tests += 1

    arg = ""
    my_out = sanitize(arg)
    tool_out1 = ""
    tool_out2 = ""
    tool_out3 = ""
    tool_out4 = ""
    tool_out = [tool_out1, tool_out2, tool_out3, tool_out4]
    if tool_out != my_out:
        print("TEST 5 FAILED...\n")
        print("INPUT: " + arg)
        print("Parsed: " + my_out[0])
        print("Unigrams: " + my_out[1])
        print("Bigrams: " + my_out[2])
        print("Trigrams: " + my_out[3])
        return
    num_tests += 1

    arg = "*let* *he*y*"
    my_out = sanitize(arg)
    tool_out1 = "let he*y"
    tool_out2 = "let he*y"
    tool_out3 = "let_he*y"
    tool_out4 = ""
    tool_out = [tool_out1, tool_out2, tool_out3, tool_out4]
    if tool_out != my_out:
        print("TEST 6 FAILED...\n")
        print("INPUT: " + arg)
        print("Parsed: " + my_out[0])
        print("Unigrams: " + my_out[1])
        print("Bigrams: " + my_out[2])
        print("Trigrams: " + my_out[3])
        return
    num_tests += 1

    arg = "*let* *he*y* h*i*\t\n\thoo*"
    my_out = sanitize(arg)
    tool_out1 = "let he*y h*i hoo"
    tool_out2 = "let he*y h*i hoo"
    tool_out3 = "let_he*y he*y_h*i h*i_hoo"
    tool_out4 = "let_he*y_h*i he*y_h*i_hoo"
    tool_out = [tool_out1, tool_out2, tool_out3, tool_out4]
    if tool_out != my_out:
        print("TEST 7 FAILED...\n")
        print("INPUT: " + arg)
        print("Parsed: " + my_out[0])
        print("Unigrams: " + my_out[1])
        print("Bigrams: " + my_out[2])
        print("Trigrams: " + my_out[3])
        return
    num_tests += 1

    arg = "It's not *something for nothing*. It's relatively healthy people who NEVER see the need to see a doctor, and are perfectly fine with the risk of dolling out 10 grand IF they get very sick. It should be our choice to make.  \n  \nAnd it *was* implemented overnight. We were promised that we could keep our policies, keep out doctors. I, for one, was allowed to keep neither."
    my_out = sanitize(arg)
    tool_out1 = "it's not something for nothing . it's relatively healthy people who never see the need to see a doctor , and are perfectly fine with the risk of dolling out 10 grand if they get very sick . it should be our choice to make . and it was implemented overnight . we were promised that we could keep our policies , keep out doctors . i , for one , was allowed to keep neither ."
    tool_out2 = "it's not something for nothing it's relatively healthy people who never see the need to see a doctor and are perfectly fine with the risk of dolling out 10 grand if they get very sick it should be our choice to make and it was implemented overnight we were promised that we could keep our policies keep out doctors i for one was allowed to keep neither"
    tool_out3 = "it's_not not_something something_for for_nothing it's_relatively relatively_healthy healthy_people people_who who_never never_see see_the the_need need_to to_see see_a a_doctor and_are are_perfectly perfectly_fine fine_with with_the the_risk risk_of of_dolling dolling_out out_10 10_grand grand_if if_they they_get get_very very_sick it_should should_be be_our our_choice choice_to to_make and_it it_was was_implemented implemented_overnight we_were were_promised promised_that that_we we_could could_keep keep_our our_policies keep_out out_doctors for_one was_allowed allowed_to to_keep keep_neither"
    tool_out4 = "it's_not_something not_something_for something_for_nothing it's_relatively_healthy relatively_healthy_people healthy_people_who people_who_never who_never_see never_see_the see_the_need the_need_to need_to_see to_see_a see_a_doctor and_are_perfectly are_perfectly_fine perfectly_fine_with fine_with_the with_the_risk the_risk_of risk_of_dolling of_dolling_out dolling_out_10 out_10_grand 10_grand_if grand_if_they if_they_get they_get_very get_very_sick it_should_be should_be_our be_our_choice our_choice_to choice_to_make and_it_was it_was_implemented was_implemented_overnight we_were_promised were_promised_that promised_that_we that_we_could we_could_keep could_keep_our keep_our_policies keep_out_doctors was_allowed_to allowed_to_keep to_keep_neither"
    tool_out = [tool_out1, tool_out2, tool_out3, tool_out4]
    if tool_out != my_out:
        print("TEST 8 FAILED...\n")
        print("INPUT: " + arg)
        print("Parsed: " + my_out[0])
        print("Unigrams: " + my_out[1])
        print("Bigrams: " + my_out[2])
        print("Trigrams: " + my_out[3])
        return
    num_tests += 1

    arg = "&gt; You shouldn't feel proud or guilty about being white. Just like you shouldn't feel proud or guilty of any aspect of your identity that you cannot control. \n\nshould I be proud of my ww2 veteran grandfather?"
    my_out = sanitize(arg)
    tool_out1 = "gt ; you shouldn't feel proud or guilty about being white . just like you shouldn't feel proud or guilty of any aspect of your identity that you cannot control . should i be proud of my ww2 veteran grandfather ?"
    tool_out2 = "gt you shouldn't feel proud or guilty about being white just like you shouldn't feel proud or guilty of any aspect of your identity that you cannot control should i be proud of my ww2 veteran grandfather"
    tool_out3 = "you_shouldn't shouldn't_feel feel_proud proud_or or_guilty guilty_about about_being being_white just_like like_you you_shouldn't shouldn't_feel feel_proud proud_or or_guilty guilty_of of_any any_aspect aspect_of of_your your_identity identity_that that_you you_cannot cannot_control should_i i_be be_proud proud_of of_my my_ww2 ww2_veteran veteran_grandfather"
    tool_out4 = "you_shouldn't_feel shouldn't_feel_proud feel_proud_or proud_or_guilty or_guilty_about guilty_about_being about_being_white just_like_you like_you_shouldn't you_shouldn't_feel shouldn't_feel_proud feel_proud_or proud_or_guilty or_guilty_of guilty_of_any of_any_aspect any_aspect_of aspect_of_your of_your_identity your_identity_that identity_that_you that_you_cannot you_cannot_control should_i_be i_be_proud be_proud_of proud_of_my of_my_ww2 my_ww2_veteran ww2_veteran_grandfather"
    tool_out = [tool_out1, tool_out2, tool_out3, tool_out4]
    if tool_out != my_out:
        print("TEST 9 FAILED...\n")
        print("INPUT: " + arg)
        print("Parsed: " + my_out[0])
        print("Unigrams: " + my_out[1])
        print("Bigrams: " + my_out[2])
        print("Trigrams: " + my_out[3])
        return
    num_tests += 1

    arg = "https://www.youtube.com/watch?v=X3W2GIZ8rN0\n\nyou should watch this. It is easier to be fooled, than to admit that you've been I know. Not superdelegate related. But you should probably have more information about what Hilary was doing with the DNC"
    my_out = sanitize(arg)
    tool_out1 = "you should watch this . it is easier to be fooled , than to admit that you've been i know . not superdelegate related . but you should probably have more information about what hilary was doing with the dnc"
    tool_out2 = "you should watch this it is easier to be fooled than to admit that you've been i know not superdelegate related but you should probably have more information about what hilary was doing with the dnc"
    tool_out3 = "you_should should_watch watch_this it_is is_easier easier_to to_be be_fooled than_to to_admit admit_that that_you've you've_been been_i i_know not_superdelegate superdelegate_related but_you you_should should_probably probably_have have_more more_information information_about about_what what_hilary hilary_was was_doing doing_with with_the the_dnc"
    tool_out4 = "you_should_watch should_watch_this it_is_easier is_easier_to easier_to_be to_be_fooled than_to_admit to_admit_that admit_that_you've that_you've_been you've_been_i been_i_know not_superdelegate_related but_you_should you_should_probably should_probably_have probably_have_more have_more_information more_information_about information_about_what about_what_hilary what_hilary_was hilary_was_doing was_doing_with doing_with_the with_the_dnc"
    tool_out = [tool_out1, tool_out2, tool_out3, tool_out4]
    if tool_out != my_out:
        print("TEST 10 FAILED...\n")
        print("INPUT: " + arg)
        print("Parsed: " + my_out[0])
        print("Unigrams: " + my_out[1])
        print("Bigrams: " + my_out[2])
        print("Trigrams: " + my_out[3])
        return
    num_tests += 1

    # INSERT MORE TESTS USING THIS:

    # arg = ""
    # my_out = sanitize(arg)
    # tool_out1 = ""
    # tool_out2 = ""
    # tool_out3 = ""
    # tool_out4 = ""
    # tool_out = [tool_out1, tool_out2, tool_out3, tool_out4]
    # if tool_out != my_out:
    #     print("TEST 5 FAILED...\n")
    #     print("INPUT: " + arg)
    #     print("Parsed: " + my_out[0])
    #     print("Unigrams: " + my_out[1])
    #     print("Bigrams: " + my_out[2])
    #     print("Trigrams: " + my_out[3])
    #     return
    # num_tests += 1

    print("* * All %d tests passed. * *" % num_tests) # DO NOT REMOVE THIS LINE



########################################################################################################################################################
########################################################################################################################################################
    

#if __name__ == "__main__":
    # This is the Python main function.
    # You should be able to run
    # python cleantext.py <filename>
    # and this "main" function will open the file,
    # read it line by line, extract the proper value from the JSON,
    # pass to "sanitize" and print the result as a list.

    # YOUR CODE GOES BELOW.

    # We are "requiring" your write a main function so you can
    # debug your code. It will not be graded.
    