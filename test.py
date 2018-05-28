import pymongo
import motor

client = pymongo.MongoClient(
		host="mongodb://r0bocop:Robo69cop@cluster0-shard-00-00-f3vjs.mongodb.net/parser",
		connect=False,
	)

"""
client = pymongo.MongoClient(
		host="mongodb://apidev:apidev@192.168.65.243/parser",
		connect=False,
	)
"""

db = client.parser
print(db.gestori_rc.find_one())