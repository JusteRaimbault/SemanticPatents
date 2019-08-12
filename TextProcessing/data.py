import pymongo


MONGOPATH = open('.mongopath').readlines()[0]

# construct a unique mongo client
mongo = pymongo.MongoClient(MONGOPATH)



#
#  Given large occurence dico, extracts corresponding subdico
#  assumes large dicos contains all subcorpus.
#  @returns [p_kw_dico,kw_p_dico]
def extract_sub_dicos(corpus,occurence_dicos) :
    p_kw_dico_all = occurence_dicos[0]
    kw_p_dico_all = occurence_dicos[1]

    p_kw_dico = dict()
    kw_p_dico = dict()

    for patent in corpus :
        #patent_id = data.get_patent_id(patent)
        patent_id=patent[0]
        keywords = []
        if patent_id in p_kw_dico_all : keywords = p_kw_dico_all[patent_id]
        p_kw_dico[patent_id] = keywords
        for k in keywords :
            if k not in kw_p_dico : kw_p_dico[k] = []
            kw_p_dico[k].append(patent_id)

    return([p_kw_dico,kw_p_dico])







##
#  export to mongo
def export_kw_dico(database,collection,p_kw_dico,year,keywordfield="keywords"):
    """
    Exports potential keywords to database, for each record.

    :database: name of the database in which the export is done
    :collection: name of the collection in which the export is done
    :p_kw_dico: dictionary to be exported, keys are records id, values are string lists (multi-stems)
    :year:
    """
    database = mongo[database]
    col = database[collection]
    col.create_index("id")

    data = []

    for p in p_kw_dico.keys() :
        data.append({"id":p,keywordfield:p_kw_dico[p],"year":str(year)})

    col.insert_many(data)


def export_set_dico(database,collection,dico,fields):
    database = mongo[database]
    col = database[collection]
    col.create_index(fields[0])
    data = []
    for p in dico.keys() :
        data.append({fields[0]:p,fields[1]:list(dico[p])})
    col.insert_many(data)

def import_kw_dico(database,collection,years,yearfield):
    database = mongo[database]
    col = database[collection]

    data = col.find({yearfield:{"$in":years}})
    p_kw_dico={}
    kw_p_dico={}

    for row in data:
        keywords = row['keywords'];patent_id=row['id']
        p_kw_dico[patent_id] = keywords
        for kw in keywords :
            if kw not in kw_p_dico : kw_p_dico[kw] = []
            kw_p_dico[kw].append(kw)

    return([p_kw_dico,kw_p_dico])



# get patent id -> not needed in python 3
#def get_patent_id(cursor_raw):
#    return(cursor_raw[0].encode('ascii','ignore'))



def get_patent_data(db,collection,years,yearfield,limit,full=True,abstractfield="abstract",textfields={"id":1,"title":1,"abstract":1}):
    """
    Requires the id field (and to be a number)
    """
    database = mongo[db]
    col = database[collection]
    if full :
        data = col.find({yearfield:{"$in":years},"id":{"$regex":r'^[0-9]'},abstractfield:{"$regex":r'.'}},textfields)#.limit(limit)
    else :
        data = col.find({yearfield:{"$in":years},"id":{"$regex":r'^[0-9]'}},{"id":1})
    res=[]
    for row in data :
        #print row
        rec={}
        for field in textfields.keys():
            if field in row :
                #rec.append(row[field])
                rec[field]=row[field]
            else :
                #rec.append("")
                rec[field]=""
        res.append(rec)
    return(res)
