from flask import Flask, jsonify, request  
from flask_cors import CORS , cross_origin
from functools import reduce
from datetime import datetime
import pymongo
ts = datetime.now()

# เชื่อมต่อกับ Cloud Mongo. 
connection_url = 'mongodb+srv://ADMIN01:admin01@cluster0.hq53d.mongodb.net/PROJECT_BIG_DATA?retryWrites=true&w=majority'
app = Flask(__name__) 
client = pymongo.MongoClient(connection_url)
cors = CORS(app, resources={r"/": {"origins": ""}})

# Database 
Database = client.get_database('PROJECT_BIG_DATA')
Database_Main_Collections = client['PROJECT_BIG_DATA']

# ดึง Table ไว้แก้ไข 
Database_Update_ANALYSIS_WORDS_AMAZON = Database_Main_Collections['ANALYSIS_WORDS_AMAZON']
Database_Update_ANALYSIS_WORDS_AMAZON_PAIRS = Database_Main_Collections['ANALYSIS_WORDS_AMAZON_ORDERED_PAIRS']

# ดึง Table ไว้ดู 
SampleTable_MainData = Database.AMAZON_FOOD_REVIEWS
SampleTable_AnalysisData = Database.ANALYSIS_WORDS_AMAZON
SampleTable_AnalysisData_Pairs = Database.ANALYSIS_WORDS_AMAZON_ORDERED_PAIRS

#หน้าแรกของ API (กรณีใช้ในการทดสอบ/ทดลองเล่น)
@app.route('/')
@cross_origin()
def helloOpen():
    main = 'WELCOME_AMAZON_FOOD_REVIEWS_API <br><br>/find/{จำนวนที่จะดึงมาดู}/\
<br>/find/HelpfulnessDenominator/{เลข/max/min}/ = ความเป็นประโยชน์ <br>/find/HelpfulnessNumerator/{เลข/max/min}/ = ความมีประโยชน์ \
<br>/find/Score/{เลข1ถึง5}/ = คะแนน<br>/find/pairs/score/{เลข1-5}/ = คู่อันดับคำกับจำนวน<br><br>\
/analysis_update/by_Score/ = ใช้อัพเดทข้อมูล(ไม่จำเป็นไม่ต้อง)<br>/analysis_pairs_update/ = ใช้อัพเดทข้อมูล(ไม่จำเป็นไม่ต้อง)'
    return main

#ส่วนทดสอบ
#--------------------------------------------------------------------------------------------------------------------------------------------------------
#ดูข้อมูลคร่าวๆ ตามจำนวนที่ใส่ไป(ยิ่งเยอะยิ่งช้า)
@app.route('/find/<value>/')
@cross_origin()
def findAll(value): 
    output = {}
    i = 0
    #loop สร้างชุดข้อมูล จำนวนตามที่กำหนด
    for x in SampleTable_MainData.find().limit(int(value)): 
        output[i] = x 
        output[i].pop('_id')
        i += 1
    return jsonify(output)

#ดูข้อมูลคร่าวๆ ตามเงื่อนไขที่ใส่ไป เช่น หา อันที่มี 5ดาว ก็ /find/Score/5/ จะได้ข้อมูล 5000 ชุดแรก ที่มี ดาว 5 ดาว
@app.route('/find/<variables>/<value>/')
@cross_origin()
def findVariablesByValue(variables,value): 
    output = {}
    i = 0
    #loop สร้างชุดข้อมูล เงื่อนไขตามที่กำหนด
    for x in SampleTable_MainData.find({variables: value}).limit(5000): 
        output[i] = x 
        output[i].pop('_id')
        i += 1
    return jsonify(output)

#ดูข้อมูลคร่าวๆ ตามเงื่อนไขที่ใส่ไป เช่น ใส่ไปว่า ดาว ก็ /find/Score/min/ จะได้ข้อมูล 10000 ชุดแรก เรียงจากดาวน้อยไปมาก(ใช้ทดสอบการดึงข้อมูล)
@app.route('/find/<variables>/min/')
@cross_origin()
def findVariablesAllMin(variables): 
    output = {}
    i = 0
    #loop สร้างชุดข้อมูล เรียงเงื่อนไขตามที่กำหนด(น้อยไปมาก)
    for x in SampleTable_MainData.find().sort(variables).limit(10000): 
        output[i] = x 
        output[i].pop('_id')
        i += 1
    return jsonify(output)

#ดูข้อมูลคร่าวๆ ตามเงื่อนไขที่ใส่ไป เช่น ใส่ไปว่า ดาว ก็ /find/Score/mฟป/ จะได้ข้อมูล 10000 ชุดแรก เรียงจากดาวมากไปน้อย(ใช้ทดสอบการดึงข้อมูล)
@app.route('/find/<variables>/max/')
@cross_origin()
def findVariablesAllMax(variables): 
    output = {}
    i = 0
    #loop สร้างชุดข้อมูล เรียงเงื่อนไขตามที่กำหนด(มากไปน้อย)
    for x in SampleTable_MainData.find().sort(variables,-1).limit(5000): 
        output[i] = x 
        output[i].pop('_id')
        i += 1
    return jsonify(output)

#ส่วนใช้งาน
#--------------------------------------------------------------------------------------------------------------------------------------------------------
#ส่งคู่อันดับ คำกับจำนวนครั้งที่พบ เช่น {"text": "aa", "value": 4} ให้ทางฝั่ง web จัดการต่อ
@app.route('/find/pairs/score/<value>/')
@cross_origin()
def findPairs(value): 
    query = SampleTable_AnalysisData_Pairs.find({"Score":int(value)})
    output = list(map(lambda y:y["Pairs"], query))
    return jsonify(output)

#ส่วนใช้ในการอัพเดทข้อมูล
#--------------------------------------------------------------------------------------------------------------------------------------------------------
#ดึงชุดข้อมูลมาอัพเดทโดยแบ่งเป็น5ชุด(ตามดาว)5000ชุดแรก และนำในส่วนของ Text มาทำเป็น list คำต่างๆ แล้ว ตัดตัวอักษรพิเศษกับตัวเลขออกส่งขึ้นไปเก็บอีก Table นึง(ANALYSIS_WORDS_AMAZON) 
@app.route('/analysis_update/by_Score/')
@cross_origin()
def Analysis_Update():
    for value in range(5):
        query = SampleTable_MainData.find({"Score": str(value+1)}).limit(5000)#ดึงข้อมูลมา5000ชุด
        wordlistALL = []
        wordlistALL = list(map(lambda y:y['Text'].lower().split(), query))#ดึงข้อมูลแค่ส่วน Text มาทำ lower case และแบ่งเป็นคำๆ
        wordlistALL = reduce(lambda a,b:a+b, wordlistALL) #รวมชุดlistทั้งหมดให้เป็น list เดียว
        wordlistALL = [x.replace('<br', '') for x in wordlistALL] # ตัดคำที่เป็น <br
        removetable = str.maketrans('', '', '\'-|,@#%".()~!?/<>-$%&^*\=:[]+{};`_0123456789 ') #ตัดตัวอักษรพิเศษกับตัวเลข
        wordlistALL = [x.translate(removetable) for x in wordlistALL] #ตัดตัวอักษรพิเศษกับตัวเลข
        wordlistALL.sort() #เรียงข้อมูล
        Data_now = { "$set": {"Timestamp":ts, "Wordlist_RAW":wordlistALL}} #เขียน value ที่จะไปอัพเดท
        filter_D = { "Score": value+1 } #เขียน key ที่จะใช้ในการอัพเดท
        Database_Update_ANALYSIS_WORDS_AMAZON.update_one(filter_D, Data_now)  #ส่งข้อมูลไปอัพเดท
        print(value+1) #นับว่า ส่วนไหนอัพเสร็จแล้ว มี 5 ส่วน

    return "Update Success"

#ดึงชุดข้อมูลมาจาก ANALYSIS_WORDS_AMAZON แล้วนับคำแล้วจับคู่ทำคู่อับดับข้อมูลเช่น aa มี 4 คำก็จะได้ {"text": "aa", "value": 4} ในลักษณะนี้
@app.route('/analysis_pairs_update/')
@cross_origin()
def Analysis_Update_Pairs(): 
    for value in range(5):
        query = SampleTable_AnalysisData.find({"Score": value+1})#ดึงข้อมูลจาก ANALYSIS_WORDS_AMAZON
        wordlistALL = list(map(lambda y:y["Wordlist_RAW"], query))#mapข้อมูลให้เป็น list
        wordlistALL_remove_duplicates = list(dict.fromkeys(wordlistALL[0]))#ตัดคำซ้ำ
        wordlistALL_remove_duplicates.remove("")#ตัดคำซ้ำ string เปล่า
        wordfreq = [wordlistALL[0].count(w) for w in wordlistALL_remove_duplicates]#นับจำนวนคำ(ความถี่ของคำนั้นๆ)
        wordlistfreq = dict(zip(wordlistALL_remove_duplicates, wordfreq))#จับคู่เป็นคู่อันดับ ลักษณะนี้ {"aa",4}
        mylist = []#listรับข้อมูล

        #คำที่อาจไม่มีประโยชน์
        unnecessary_text = ['is', 'in', 'but', 'of', 'to', 'the', 'have', 'are', 'so', 'was', 'a', 'for', 'and', 'is', 'that', 'its', 'get', 'more', 'for', 'will', 'from', 'me', 'too', 'as', 'it', 'like', 'that', 'was', 'not', 'so', 'be', 'i', 'my', 'are', 'do', 'or', 'this', 'had', 'other', 'by', 'am', 'all', 'an', 'on', 'can', 'as', 'how', 'did', 'about', 'me', 'too', 'if', 'id', 'ok', 'no', 'that', 'like', 'what', 'even', 'do', 'we', '-------------', 'you', 'with', 'dont', 'one', 'got', 'then', 'at', 'any', 'where', 'im', 'does', 'who', 'too', 'on', 'much', 'she', 'he', 'just', 'when', 'would', 'some', 'what']
        
        for x, y in wordlistfreq.items():
            if x in unnecessary_text:#ถ้าเจอคำที่อาจไม่มีประโยชน์ ให้ข้ามไป
                continue
            else:
                dict_set = {"text":x, "value":y}#สร้างคู่อันดับใหม่ ให้ฝั่งเว็บใช้ง่านง่ายขึ้น {"aa",4} => {"text": "aa", "value": 4}
                mylist.append(dict_set)#เพิ่มเข้า list
        Data_now = { "$set": {"Pairs":mylist}}#เขียน value ที่จะไปอัพเดท
        filter_D = { "Score": value+1 }#เขียน key ที่จะใช้ในการอัพเดท
        Database_Update_ANALYSIS_WORDS_AMAZON_PAIRS.update_one(filter_D, Data_now)#ส่งข้อมูลไปอัพเดท
        print(value+1)#นับว่า ส่วนไหนอัพเสร็จแล้ว มี 5 ส่วน
    return "Update Success"

if __name__ == '__main__': 
	app.run(port=3000, debug=True) 
