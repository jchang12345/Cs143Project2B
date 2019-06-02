#!/usr/bin/env python

"""Clean comment text for easier parsing."""

from __future__ import print_function

import re
import string
import argparse
import json


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

    





def sanitize(text):
    """Do parse the text in variable "text" according to the spec, and return
    a LIST containing FOUR strings 
    1. The parsed text.
    2. The unigrams
    3. The bigrams
    4. The trigrams
    """
    # ONLY SANITIZE IS BEING GRADED!!!!!

    # YOUR CODE GOES BELOW:

    _PUNCTUATION = frozenset(string.punctuation.replace('$','').replace('#','').replace('%','').replace(',','').replace('.','').replace('?','').replace('!','')) #% , # 

    def _remove_punc(token):
        """Removes punctuation from start/end of token."""
        i = 0
        j = len(token) - 1
        idone = False
        jdone = False
        while i <= j and not (idone and jdone):
            if token[i] in _PUNCTUATION and not idone:
                i += 1
            else:
                idone = True
            if token[j] in _PUNCTUATION and not jdone:
                j -= 1
            else:
                jdone = True
        return "" if i > j else token[i:(j+1)]

    parsed_text=""
    unigrams=""
    bigrams=""
    trigrams=""

    stripnt=text.replace('\n',' ')
    stripnt=stripnt.replace('\t',' ')

    stripurls=re.sub(r'http://\S+','', stripnt)
    stripurls=re.sub(r'https://\S+','', stripurls)

    stripusrs=re.sub(r'/r/\S+','', stripurls)
    stripusrs=re.sub(r'/u/\S+','', stripusrs)

    #REEEEEE
    stripextraspaces=stripusrs
    stripextraspaces=re.split('\s+',stripextraspaces)

    reconstructedstring=""
    for i in stripextraspaces:
        reconstructedstring=reconstructedstring+i+" "
    #print("reconstructing that string")
    #print(reconstructedstring)



    externaltokenstring =re.sub(r',',',', reconstructedstring)

    externaltokenstringslist=externaltokenstring.split(' ')
    #print(externaltokenstringslist)

    for i in range(len(externaltokenstringslist)-1):
        #print(externaltokenstringslist[i])
        externaltokenstringslist[i]=_remove_punc(externaltokenstringslist[i])
   # print(externaltokenstringslist)

    reconstructedexternaltokenstring=""
    for i in externaltokenstringslist:
        #if the last character in the string is a '.' or '?' or '!' or ',' append space b/w
        
        if len(i)>1 and (i[len(i)-1] == ',' or i[len(i)-1]=='.' or i[len(i)-1]=='?' or i[len(i)-1]=='!'):
            #space it out
            reconstructedexternaltokenstring=reconstructedexternaltokenstring+i[0:len(i)-1]+" "+i[len(i)-1]+" "
        if len(i)>1 and not (i[len(i)-1] == ',' or i[len(i)-1]=='.' or i[len(i)-1]=='?' or i[len(i)-1]=='!'):
            reconstructedexternaltokenstring=reconstructedexternaltokenstring+i+" "
        if len(i)==1 and not (i[len(i)-1] == ',' or i[len(i)-1]=='.' or i[len(i)-1]=='?' or i[len(i)-1]=='!'):
            reconstructedexternaltokenstring=reconstructedexternaltokenstring+i+" "

    #print("reconstructing that string")
    #print(reconstructedexternaltokenstring)


    lowercasedstring=reconstructedexternaltokenstring.lower()


    lowercasestringslist=lowercasedstring.split()

    a=""
    parsed_text=""
    for i in range(len(lowercasestringslist)):
        if i!=len(lowercasestringslist)-1:
            if lowercasestringslist[i+1]==';' or lowercasestringslist[i+1].startswith('\''):
                parsed_text=parsed_text+lowercasestringslist[i]+lowercasestringslist[i+1]+" "
            if lowercasestringslist[i]!=a and lowercasestringslist[i+1]!=';'and not lowercasestringslist[i+1].startswith('\'') and not lowercasestringslist[i].startswith('\'')and lowercasestringslist[i]!=';':
                parsed_text=parsed_text+lowercasestringslist[i]+" "
        if i==len(lowercasestringslist)-1:
            if lowercasestringslist[i]!=a and lowercasestringslist[i]!=';'and not lowercasestringslist[i].startswith('\''):
                parsed_text=parsed_text+lowercasestringslist[i]

    #unigrams =re.sub(r'\.','', lowercasedstring)
    #unigrams =re.sub(r',','', unigrams)
    #unigrams =re.sub(r'\!','', unigrams)
    #unigrams =re.sub(r'\?','', unigrams)
    #print(parsed_text)
    #parsed_text=lowercasedstring

    for i in range(len(lowercasestringslist)):
        if i!=len(lowercasestringslist)-1:
            if lowercasestringslist[i+1]==';' or lowercasestringslist[i+1].startswith('\'') and lowercasestringslist[i] not in string.punctuation:
                unigrams=unigrams+lowercasestringslist[i]+lowercasestringslist[i+1]+" "
            if lowercasestringslist[i]!=a and lowercasestringslist[i+1]!=';'and not lowercasestringslist[i+1].startswith('\'') and not lowercasestringslist[i].startswith('\'')and lowercasestringslist[i]!=';' and lowercasestringslist[i] not in string.punctuation:
                unigrams=unigrams+lowercasestringslist[i]+" "

        if i==len(lowercasestringslist)-1:
            if lowercasestringslist[i] not in string.punctuation and lowercasestringslist[i]!=';' and not lowercasestringslist[i].startswith('\''):
                unigrams=unigrams+lowercasestringslist[i]+" "

    unigrams=unigrams.replace("  "," ")
    #unigrams=unigrams.replace(""," ")
    #print("unigrams")
    #print(unigrams)

    #print('\n')


    bigrams=parsed_text
    output=[]
    a=[]
    input=bigrams.split(' ')
    for i in range(len(input)-1):
        if input[i:i+2] !=["",""]:
            if input[i:i+2] !=['']:
                if input[i:i+2]!= a:
                    if input[i:i+2]!=[input[i],""] and input[i] not in string.punctuation and input[i+1] not in string.punctuation:
                        output.append(input[i:i+2])  

    #print(output)
    prep=['_'.join(x) for x in output]
    bigrams=""
    #print(prep)
    #bigrams=prep
    for c in range(len(prep)):
        bigrams=bigrams+prep[c]+" "
    #then clean up the last guy.
    #text.rsplit(' ', 1)[0]
    #bigrams=bigrams.rsplit(' ',20)[0]

   # print("bigrams")
   # print(bigrams)

    #print('\n')

    outputtri=[]
    trigrams=parsed_text
    #a=[]
    input=trigrams.split(' ')
    for i in range(len(input)-1):
        #print(len(input))
        if i+2<(len(input)-1):
            if input[i:i+3] !=["","",""]:
                if input[i:i+3] !=['']:
                    if input[i:i+3] !=['','']:
                        if input[i:i+3]!= a:
                            if input[i:i+3]!=[input[i],"",""]:
                                    if input[i:i+3]!=[input[i],input[i+1],""]and input[i] not in string.punctuation and input[i+1] not in string.punctuation and input[i+2] not in string.punctuation:
                                        outputtri.append(input[i:i+3])  

    #print(outputtri)
    prep=['_'.join(x) for x in outputtri]
    trigrams=""
    #print(prep)
    #bigrams=prep
    for c in range(len(prep)):
        trigrams=trigrams+prep[c]+" "

    #print("trigrams")
    #print(trigrams)

   # print('\n')



    return [parsed_text, unigrams, bigrams, trigrams]


if __name__ == "__main__":#bruh this is bugged needs 2 ==
    # This is the Python main function.
    # You should be able to run
    # python cleantext.py <filename>
    # and this "main" function will open the file,
    # read it line by line, extract the proper value from the JSON,
    # pass to "sanitize" and print the result as a list.
    #using argparse 


    # YOUR CODE GOES BELOW.
    parser=argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)
#        parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)

    parser.add_argument('--f', dest="add", nargs=1, help="JSON file to be processed", type=str)
    args=parser.parse_args()
    #d=json.load(args.infile[0])
    addfile=args.add
    #print(addfile)
    with open(addfile[0]) as f:
    #    json.dumps(f.readlines())
        content=f.readlines()
    #print(content)

   
    for i in range(0,len(content)):
        print('\n')#debugging purposes
        
        pythonobj=json.loads(content[i])
        stringtoproc=pythonobj["body"]
        p,u,b,t=sanitize(stringtoproc) #its going to be content sub everything eventually, just 
        print(p)
        print('\n')
        print(u)
        print('\n')
        print(b)
        print('\n')
        print(t)


