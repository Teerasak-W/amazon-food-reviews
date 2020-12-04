from flask import Flask, jsonify, request  
from flask_cors import CORS , cross_origin
from functools import reduce
from datetime import datetime
import pymongo
ts = datetime.now()

# Replace your URL here. Don't forget to replace the password. 
connection_url = 'mongodb+srv://ADMIN01:admin01@cluster0.hq53d.mongodb.net/PROJECT_BIG_DATA?retryWrites=true&w=majority'
app = Flask(__name__) 
client = pymongo.MongoClient(connection_url) 

# Database 
Database = client.get_database('PROJECT_BIG_DATA')

Database_Main_Collections = client['PROJECT_BIG_DATA']
Database_Update_ANALYSIS_WORDS_AMAZON = Database_Main_Collections['ANALYSIS_WORDS_AMAZON']
Database_Update_ANALYSIS_WORDS_AMAZON_PAIRS = Database_Main_Collections['ANALYSIS_WORDS_AMAZON_ORDERED_PAIRS']
# Table 
SampleTable_MainData = Database.AMAZON_FOOD_REVIEWS
SampleTable_AnalysisData = Database.ANALYSIS_WORDS_AMAZON
SampleTable_AnalysisData_Pairs = Database.ANALYSIS_WORDS_AMAZON_ORDERED_PAIRS

@app.route('/')
@cross_origin()
def helloOpen():
    main = 'WELCOME_AMAZON_FOOD_REVIEWS_API <br><br>/find/{จำนวนที่จะดึงมาดู}/\
<br>/find/HelpfulnessDenominator/{เลข/max/min}/ = ความเป็นประโยชน์ <br>/find/HelpfulnessNumerator/{เลข/max/min}/ = ความมีประโยชน์ \
<br>/find/Score/{เลข1ถึง5}/ = คะแนน<br>/find/pairs/score/{เลข1-5}/ = คู่อันดับคำกับจำนวน<br><br>\
/analysis_update/by_Score/ = ใช้อัพเดทข้อมูล(ไม่จำเป็นไม่ต้อง)<br>/analysis_pairs_update/ = ใช้อัพเดทข้อมูล(ไม่จำเป็นไม่ต้อง)'
    return main

@app.route('/find/<value>/')
@cross_origin()
def findAll(value): 
    output = {}
    i = 0
    for x in SampleTable_MainData.find().limit(int(value)): 
        output[i] = x 
        output[i].pop('_id')
        i += 1
    return jsonify(output)

@app.route('/find/<variables>/<value>/')
@cross_origin()
def findVariablesByValue(variables,value): 
    output = {}
    i = 0
    for x in SampleTable_MainData.find({variables: value}).limit(5000): 
        output[i] = x 
        output[i].pop('_id')
        i += 1
    return jsonify(output)

@app.route('/find/<variables>/min/')
@cross_origin()
def findVariablesAllMin(variables): 
    output = {}
    i = 0
    for x in SampleTable_MainData.find().sort(variables).limit(5000): 
        output[i] = x 
        output[i].pop('_id')
        i += 1
    return jsonify(output)

@app.route('/find/<variables>/max/')
@cross_origin()
def findVariablesAllMax(variables): 
    output = {}
    i = 0
    for x in SampleTable_MainData.find().sort(variables,-1).limit(5000): 
        output[i] = x 
        output[i].pop('_id')
        i += 1
    return jsonify(output)

@app.route('/find/pairs/score/<value>/')
@cross_origin()
def findPairs(value): 
    query = SampleTable_AnalysisData_Pairs.find({"Score":int(value)+1})
    output = list(map(lambda y:y["Pairs"], query))
    return jsonify(output)

@app.route('/analysis_update/by_Score/')
@cross_origin()
def Analysis_Update():
    for value in range(5):
        query = SampleTable_MainData.find({"Score": str(value+1)}).limit(5000)
        wordlistALL = []
        wordlistALL = list(map(lambda y:y['Text'].lower().split(), query))
        wordlistALL = reduce(lambda a,b:a+b, wordlistALL)
        wordlistALL = [x.replace('<br', '') for x in wordlistALL]
        removetable = str.maketrans('', '', '\'|,@#%".()~!?/<>-$%&^*\=:[]+{};`_0123456789 ')
        wordlistALL = [x.translate(removetable) for x in wordlistALL]
        wordlistALL.sort()
        Data_now = { "$set": {"Timestamp":ts, "Wordlist_RAW":wordlistALL}}
        filter_D = { "Score": value+1 }
        Database_Update_ANALYSIS_WORDS_AMAZON.update_one(filter_D, Data_now)  
        print(value+1)

    return "Update Success"

@app.route('/analysis_pairs_update/')
@cross_origin()
def Analysis_Update_Pairs(): 
    for value in range(5):
        query = SampleTable_AnalysisData.find({"Score": value+1})
        wordlistALL = list(map(lambda y:y["Wordlist_RAW"], query))
        wordlistALL_remove_duplicates = list(dict.fromkeys(wordlistALL[0]))
        wordlistALL_remove_duplicates.remove("")
        wordfreq = [wordlistALL[0].count(w) for w in wordlistALL_remove_duplicates]
        wordlistfreq = dict(zip(wordlistALL_remove_duplicates, wordfreq))
        mylist = []
        for x, y in wordlistfreq.items():
            if x in ['is', 'in', 'but', 'of', 'to', 'the', 'have', 'are', 'so', 'was', 'a', 'for', 'and', 'is', 'that', 'its', 'get', 'more', 'for', 'will', 'from', 'me', 'too', 'as', 'it', 'like', 'that', 'was', 'not', 'so', 'be', 'i', 'my', 'are', 'do', 'or', 'this', 'had', 'other', 'by', 'am', 'all', 'an', 'on', 'can', 'as', 'how', 'did', 'about', 'me', 'too', 'if', 'id', 'ok', 'no', 'that', 'like', 'what', 'even', 'do', 'we', '-------------', 'you', 'with', 'dont', 'one', 'got', 'then', 'at', 'any', 'where', 'im', 'does', 'who', 'too', 'on', 'much', 'she', 'he', 'just', 'when', 'would', 'some', 'what']:
                continue
            else:
                dict_set = {"text":x, "value":y}
                mylist.append(dict_set)
        Data_now = { "$set": {"Pairs":mylist}}
        filter_D = { "Score": value+1 }
        Database_Update_ANALYSIS_WORDS_AMAZON_PAIRS.update_one(filter_D, Data_now)
        print(value+1) 
    return "Update Success"

if __name__ == '__main__': 
	app.run(debug=True) 
