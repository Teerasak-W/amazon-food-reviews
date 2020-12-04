from flask import Flask, jsonify, request  
from flask_cors import CORS 
from functools import reduce
import pymongo
from datetime import datetime
ts = datetime.now()

# Replace your URL here. Don't forget to replace the password. 
connection_url = 'mongodb+srv://ADMIN01:admin01@cluster0.hq53d.mongodb.net/PROJECT_BIG_DATA?retryWrites=true&w=majority'
app = Flask(__name__) 
client = pymongo.MongoClient(connection_url) 

# Database 
Database = client.get_database('PROJECT_BIG_DATA')

Database_Collections = client['PROJECT_BIG_DATA']
Database_Update = Database_Collections['ANALYSIS_WORDS_AMAZON']
# Table 
SampleTable_MainData = Database.AMAZON_FOOD_REVIEWS
SampleTable_AnalysisData = Database.ANALYSIS_WORDS_AMAZON

@app.route('/', methods=['GET']) 
def helloOpen(): 
    return 'WELCOME_AMAZON_FOOD_REVIEWS_API <br><br>HelpfulnessDenominator = ความเป็นประโยชน์ <br>HelpfulnessNumerator = ความมีประโยชน์ <br>Score = คะแนน'

@app.route('/find/<variables>/<value>/', methods=['GET']) 
def findVariablesByValue(variables,value): 
    output = {}
    i = 0
    for x in SampleTable_MainData.find({variables: value}).limit(5000): 
        output[i] = x 
        output[i].pop('_id')
        i += 1
    return jsonify(output)

@app.route('/find/<variables>/min/', methods=['GET']) 
def findVariablesAllMin(variables): 
    output = {}
    i = 0
    for x in SampleTable_MainData.find().sort(variables).limit(5000): 
        output[i] = x 
        output[i].pop('_id')
        i += 1
    return jsonify(output)

@app.route('/find/<variables>/max/', methods=['GET']) 
def findVariablesAllMax(variables): 
    output = {}
    i = 0
    for x in SampleTable_MainData.find().sort(variables,-1).limit(5000): 
        output[i] = x 
        output[i].pop('_id')
        i += 1
    return jsonify(output)
    
# @app.route('/analysis_update/<value>/', methods=['GET']) 
# def findAnalysis(value): 
#     query = SampleTable_MainData.find().limit(int(value))

#     wordlistALL = []

#     wordlistALL = list(map(lambda y:y['Text'].lower().split(), query))

#     wordlistALL = reduce(lambda a,b:a+b, wordlistALL)

#     wordlistALL = [x.replace('<br', '') for x in wordlistALL]

#     removetable = str.maketrans('', '', '\',@#%".()~!?/<>-$%&^*\=:[]+{};`_0123456789 ')

#     wordlistALL = [x.translate(removetable) for x in wordlistALL]

#     wordlistALL.sort()

#     Data_now = {"Timestamp":ts, "Wordlist_RAW":wordlistALL}

#     Database_Update.insert_one(Data_now)

#     return "Update Success"

# @app.route('/analysis/', methods=['GET']) 
# def Analysis(): 
#     query = SampleTable_AnalysisData.find()
#     wordlistALL = list(map(lambda y:y["Wordlist_RAW"], query))
#     wordlistALL_remove_duplicates = list(dict.fromkeys(wordlistALL[0]))
#     wordfreq = [wordlistALL[0].count(w) for w in wordlistALL_remove_duplicates]
#     # wordfreq = list(map(lambda x,y : x.count(y), wordlistALL, wordlistALL_remove_duplicates))
#     wordlistfreq = list(set(zip(wordlistALL_remove_duplicates, wordfreq)))
#     wordlistfreq = list(dict.fromkeys(wordlistfreq))
#     wordlistfreq.sort(key = lambda x: x[1], reverse=True)
#     return jsonify(wordlistfreq)

if __name__ == '__main__': 
	app.run(debug=True) 
