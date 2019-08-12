import nltk,operator,math,pymongo,functools
import data,keywords,utils


# TODO make parameters generic
RAWDATABASE='redbook'
RAWCOLLECTION='raw'
RESDATABASE = 'patent'
KWCOLLECTION = 'keywords'
STEMCOLLECTION = 'stems'

TEXTFIELDS={"id":1,"title":1,"abstract":1}

# TODO generic parametrization for rule
def stemgrammar(tag):
    return((tag[1]=='NN' or tag[1]=='NNP' or tag[1] == 'VBG' or tag[1] =='NNS'or tag[1] =='JJ' or tag[1] =='JJR'))


##
# Extract keywords for a given year
def extract_keywords_year(year):
    """
    Extract potential multi-stems for records of a given year

    :year: year
    """
    corpus = data.get_patent_data(RAWDATABASE,RAWCOLLECTION,[year],"year",0,full=True,textfields=TEXTFIELDS)
    print('corpus size : '+str(len(corpus)))
    [p_kw_dico,kw_p_dico,stem_dico] = construct_occurrence_dico(corpus)
    data.export_kw_dico(RESDATABASE,KWCOLLECTION,p_kw_dico,year)
    data.export_set_dico(RESDATABASE,STEMCOLLECTION,stem_dico,['stem','keywords'])






def construct_occurrence_dico(data) :
    """
    Constructs occurrence dicos from raw data

    :data: dataset
    """
    print('Constructing occurence dictionnaries...')

    p_kw_dico = dict()
    kw_p_dico = dict()
    full_stem_dico = {}
    for patent in data :
        patent_id = patent['id']
        #[keywords,stem_dico] = extract_keywords(patent[1]+". "+patent[2],patent_id)
        [keywords,stem_dico] = extract_keywords(patent['title']+". "+patent['abstract'],patent_id)
        #print(keywords)

        for k in keywords :
            # add to p_kw dico
            if k in kw_p_dico :
                kw_p_dico[k].append(patent_id)
            else :
                kw_p_dico[k]= [patent_id]
            #
            if patent_id in p_kw_dico :
                p_kw_dico[patent_id].append(k)
            else :
                p_kw_dico[patent_id] = [k]

        for k in stem_dico.keys():
            if k in full_stem_dico :
                full_stem_dico[k]=full_stem_dico[k].union(stem_dico[k])
            else :
                full_stem_dico[k] = stem_dico[k]

    return([p_kw_dico,kw_p_dico,full_stem_dico])



def potential_multi_term(tagged) :
    """
    Rule for potentially relevant multi stem
     tagged of the form (word,TAG)
    """
    res = True
    for tag in tagged :
        res = res and stemgrammar(tag)
    return res



def extract_keywords(raw_text,id):
    """
    Extract multi-stem from a text.

    :raw_text: unparsed text (string)
    :id: id of record
    """

    print("Extracting keywords for "+id)

    stemmer = nltk.PorterStemmer()

    # Construct text

    # Tokens
    tokens = nltk.word_tokenize(raw_text)
    # filter undesirable words and format
    words = [w.replace('\'','') for w in tokens if len(w)>=3]
    text = nltk.Text(words)

    tagged_text = nltk.pos_tag(text)
    #nouns = [tg[0] for tg in tagged_text if tg[1]=='NN' or tg[1]=='NNP' ]
    #print(nouns)

    # multi-term
    multiterms = set()
    stem_dico = {}
    for i in range(len(tagged_text)) :
        # max length 4 for multi-terms ==> 3
        for l in range(1,4) :
            if i+l < len(tagged_text) :
                tags = [tagged_text[k] for k in range(i,i+l)]
                if potential_multi_term(tags) :
                    multistemlist = [str.lower(stemmer.stem(tagged_text[k][0])) for k in range(i,i+l)]
                    #multistem.sort(key=str.lower)
		    #python 3 : remove .encode('ascii','ignore')
                    multistem = functools.reduce(lambda s1,s2 : s1+' '+s2,multistemlist)
                    rawtext = functools.reduce(lambda s1,s2 : s1+' '+s2,[str.lower(tagged_text[k][0]) for k in range(i,i+l)])
                    multiterms.add(multistem)
                    if multistem in stem_dico :
                        stem_dico[multistem].add(rawtext)
                    else :
                        stem_dico[multistem] = set([rawtext])

    return [list(multiterms),stem_dico]
