# -*- coding: utf-8 -*-

import os
import re
import csv
import json
import pipes
import urllib.request, urllib.parse, urllib.error
import urllib.request, urllib.error, urllib.parse
import subprocess
import configparser
from flask import jsonify
from lib.utils import Utils
from datetime import datetime
from lib.filters import Filters
from bson.json_util import dumps
from bson.objectid import ObjectId
from flask_compress import Compress
from flask_cors import CORS, cross_origin
from werkzeug.utils import secure_filename
from pymongo import MongoClient, ReturnDocument
from flask_debugtoolbar import DebugToolbarExtension
from flask import Flask, flash, render_template, request, redirect, g



# current script dir
script_dir = os.path.dirname(os.path.abspath(__file__))
# global config (config.ini)
config = configparser.ConfigParser()
config.read(script_dir+'/api.py.ini')

# work database: linux environment or pd.py.ini ??
dbprefix = Utils.getDbprefix()
cpool = Utils.getCollectionPool(config, dbprefix)

app = Flask(__name__)
Compress(app)
CORS(app)

app.config['mongodb'] = config['mongodb']
app.config['coll'] = config['coll']
app.config['cpool'] = cpool



@app.after_request
def add_no_cache(response):

    """Ignore cache"""

    if request.endpoint != "static":
        response.headers["Cache-Control"] = "no-cache"
        response.headers["Pragma"] = "no-cache"
    return response



@app.route('/')
def index():
    return 'GA'



@app.route('/v1/ping')
def ping():

    """ PING PONG """

    pipeline = [
        {
            '$limit': 20
        }
    ]

    out = app.config['cpool']['collection_ilde_final'].aggregate(pipeline)
    if out is not None:
        return jsonify({'ping':'pong'})

    return False



@app.route('/v1/brands', methods=['GET', 'POST'])
def brands():

    """ brands """

    pipe = [
        {
            '$group': {
                '_id': {
                    'val': '$val',
                }
            }
        }
    ]

    provider = request.args.get('p')
    provider = str(provider.encode('utf8'))

    search = request.args.get('s')
    if search is None:
        search = ''

    search = str(search.encode('utf8'))

    out = app.config['cpool']['all_brands'].find(
            {'$and': [
                {'val':{'$regex':"^"+search, '$options': '-i'}},
                {'source': provider}
            ]}
        )

    return dumps(out)



@app.route('/v1/brands_letu', methods=['GET', 'POST'])
def brands_letu():

    """ get letu brands """

    out = app.config['cpool']['brands_letu'].distinct("name")
    return dumps(out)



@app.route('/v1/all_brands', methods=['GET', 'POST'])
def all_brands():

    """ merged letu + ilde brands """

    pipe = [
        {
            '$group': {
                '_id': {
                    'name': '$val',
                }
            }
        }
    ]

    search = str(request.args.get('search'))
    search = request.args.get('search')
    if search is None:
        search = ''

    search = search.encode('utf8').strip()

    regx = re.compile("^"+search, re.IGNORECASE)
    out = app.config['cpool']['all_brands'].find({'val': regx})

    return dumps(out)



@app.route('/v1/gestori_groups', methods=['GET', 'POST'])
def gestori_groups():

    """ get letu brands """

    search = request.args.get('search')
    if search is None:
        search = ''

    search = search.encode('utf8').strip()

    if search is not '':
        regx = re.compile("^"+search, re.IGNORECASE)
        out = app.config['cpool']['gestori_groups'].find({'name': regx})
    else:
        out = []

    return dumps(out)



#
# GESTORI
# products
#
@app.route('/v1/gestori_products', methods=['GET', 'POST'])
def gestori_products():

    # search by articul ?
    articul = request.args.get('a')
    page = int(request.args.get('p'))
    perPage = int(request.args.get('pP'))
    keyword = request.args.get('kw')
    search = request.args.get('s')

    # articul deprecated (??)
    # rules
    if articul == 'undefined' or articul == 'null' or articul is None:
        articul = False

    if search == 'undefined' or search == 'null' or search is None:
        search = False
    else:
        search = str(search.encode('utf8').strip())
        # set query string limit
        if len(search) > 60:
            return jsonify({'count': 0, 'data': []})
    if keyword == 'undefined' or keyword == 'null' or keyword is None or keyword == '':
        keyword = False

    # dbg params
    print(('articul: ', articul, 'keyword: ', keyword, 'search: ', search))

    if search is None or search == 'undefined' or search == '' or search == 'null':
        search = ''

    #total = app.config['cpool']['collection_gestori'].count()

    start = (page - 1) * perPage
    end = start + perPage

    # SEARCH BY BRAND
    if search is not False and articul is False and keyword is False:
        # need extra count
        total = app.config['cpool']['collection_gestori'].find({
            'name_brand': {
                '$regex': "^"+search, '$options': '-i'
            }
        }).count()
        pipe = [
            {
                '$match': {
                    'name_brand': {
                        '$regex': "^"+search, '$options': '-i'
                    }
                }
            },
            {
                '$skip': start
            },
            {
                '$limit': perPage
            },
            {
                '$sort': {
                    'Name_e': 1
                }
            },
            {
                '$group': {
                    '_id': {
                        'name': '$Name',
                        'brand': '$name_brand',
                        'artic': '$Artic',
                        'name_e': '$Name_e',
                        'cod_good': '$Cod_good',
                        'retail_price': '$Retail_price',
                        'barcod': '$Barcod',
                        'rive_match_code': '$rive_match_code',
                        'ilde_match_code': '$ilde_match_code',
                        'letu_match_code': '$letu_match_code',
                        'verified': '$verified',
                        'id': '$id'
                    }
                }
            }
        ]
        print('MATCH: ONLY BRAND')

    if search is False and keyword is False and articul is False:
        total = app.config['cpool']['collection_gestori'].find().count()
        pipe = [
            {
                '$skip': start
            },
            {
                '$limit': perPage
            },
            {
                '$sort': {
                    'Name_e': 1
                }
            },
            {
                '$group': {
                    '_id': {
                        'name': '$Name',
                        'brand': '$name_brand',
                        'artic': '$Artic',
                        'name_e': '$Name_e',
                        'cod_good': '$Cod_good',
                        'retail_price': '$Retail_price',
                        'barcod': '$Barcod',
                        'rive_match_code': '$rive_match_code',
                        'ilde_match_code': '$ilde_match_code',
                        'letu_match_code': '$letu_match_code',
                        'verified': '$verified',
                        'id': '$id'
                    }
                }
            }
        ]
        print('MATCH: ALL')

    # in case of articul is not empty
    if articul is not False:
        total = app.config['cpool']['collection_gestori'].find({
            'Artic': articul
        }).count()
        pipe = [
            {
                '$match': {'Artic': articul}
            },
            {
                '$skip': start
            },
            {
                '$limit': perPage
            },
            {
                '$sort': {
                    'Name_e': 1
                }
            },
            {
                '$group': {
                    '_id': {
                        'name': '$Name',
                        'brand': '$name_brand',
                        'artic': '$Artic',
                        'name_e': '$Name_e',
                        'cod_good': '$Cod_good',
                        'retail_price': '$Retail_price',
                        'barcod': '$Barcod',
                        'rive_match_code': '$rive_match_code',
                        'ilde_match_code': '$ilde_match_code',
                        'letu_match_code': '$letu_match_code',
                        'verified': '$verified',
                        'id': '$id'
                    }
                }
            }
        ]
        print('MATCH: ARTICUL')



    # keyword and brand
    if keyword is not False and search is not False and articul is False:

        # count before get
        pipe = [
            {
                '$match': {
                    '$text': {
                        '$search': keyword,
                    }
                }
            },
            {
                '$match': {
                    'name_brand': search
                }
            },
            {
                '$group': {
                    '_id': {
                        'name': '$Name'
                    }
                }
            }
        ]

        total = len(list(app.config['cpool']['collection_gestori'].aggregate(pipe)))

        pipe = [
            {
                '$match': {
                    '$text': {
                        '$search': keyword,
                    }
                }
            },
            {
                '$sort': { 'score': { '$meta': "textScore" } }
            },
            {
                '$match': {
                    'name_brand': search
                }
            },
            {
                '$skip': start
            },
            {
                '$limit': perPage
            },
            {
                '$group': {
                    '_id': {
                        'name': '$Name',
                        'brand': '$name_brand',
                        'artic': '$Artic',
                        'name_e': '$Name_e',
                        'cod_good': '$Cod_good',
                        'retail_price': '$Retail_price',
                        'barcod': '$Barcod',
                        'rive_match_code': '$rive_match_code',
                        'ilde_match_code': '$ilde_match_code',
                        'letu_match_code': '$letu_match_code',
                        'verified': '$verified',
                        'id': '$id'
                    }
                }
            }
        ]

        print(('KEYWORD + BRAND ' + keyword))

    # fulltext by keyword
    if keyword is not False and search is False and articul is False:
        total = app.config['cpool']['collection_gestori'].find({
            'Name': keyword
        }).count()
        print(('KEYWORD: ', keyword.encode('utf8').strip()))
        pipe = [
            {
                '$match': {
                    '$text': {
                        '$search': keyword,
                    }
                }
            },
            {
                '$sort': { 'score': { '$meta': "textScore" } }
            },
            {
                '$limit': 100
            },
            {
                '$group': {
                    '_id': {
                        'name': '$Name',
                        'brand': '$name_brand',
                        'artic': '$Artic',
                        'name_e': '$Name_e',
                        'cod_good': '$Cod_good',
                        'retail_price': '$Retail_price',
                        'barcod': '$Barcod',
                        'rive_match_code': '$rive_match_code',
                        'ilde_match_code': '$ilde_match_code',
                        'letu_match_code': '$letu_match_code',
                        'verified': '$verified',
                        'id': '$id'
                    }
                }
            }
        ]
        print('ONLY KEYWORD')

    out = app.config['cpool']['collection_gestori'].aggregate(pipe)
    counted = list(out)
    out_list = {
        'count': total,
        'data': counted
    }

    return jsonify(out_list)



@app.route('/v1/letu_products', methods=['GET', 'POST'])
def letu_products():

    """letoile products"""

    articul = request.args.get('art')
    search = request.args.get('search')
    keyword = request.args.get('kw')

    if search is None or search == 'undefined' or search == '' or search == 'null':
        search = False
    else:
        search = str(search.encode('utf8').strip())

    if keyword is None or keyword == 'undefined' or keyword == '' or keyword == 'null':
        keyword = False
    else:
        keyword = str(keyword.encode('utf8').strip())

    if articul is None or articul == 'undefined' or articul == '' or articul == 'null':
        articul = False
    else:
        articul = str(articul.encode('utf8').strip())

    page = int(request.args.get('page'))
    perPage = int(request.args.get('perPage'))

    start = (page - 1) * perPage
    end = start + perPage

    print(("kw", keyword, "s", search))

    # brand
    if search is not False and keyword is False:
        print("ONLY BRAND")
        total = app.config['cpool']['collection_letu_final'].find({
            'brand': search
        }).count()
        pipe = [
            {
                '$match': {
                    'brand': search
                }
            },
            {
                '$sort': {
                    'LastUpdate': -1
                }
            },
            {
                '$skip': start
            },
            {
                '$limit': perPage
            },
            {
                '$group': {
                    '_id': {
                        'name': '$name',
                        'name_e': '$name_e',
                        'brand': '$brand',
                        'artic': '$articul',
                        'desc': '$desc',
                        'id': '$id',
                        'img': '$img',
                        'volume': '$volume',
                        'listingprice': '$listingprice',
                        'oldprice': '$oldprice',
                        'LastUpdate': '$LastUpdate',
                        'Navi': '$Navi',
                        'url': '$url',
                        'gest_match_code': '$gest_match_code'
                    }
                }
            },
        ]

    # all
    if search is False and keyword is False:
        print("ALL")
        total = app.config['cpool']['collection_letu_final'].find().count()
        pipe = [
            {
                '$sort': {
                    'LastUpdate': -1
                }
            },
            {
                '$skip': start
            },
            {
                '$limit': perPage
            },
            {
                '$group': {
                    '_id': {
                        'name': '$name',
                        'name_e': '$name_e',
                        'brand': '$brand',
                        'artic': '$articul',
                        'desc': '$desc',
                        'id': '$id',
                        'img': '$img',
                        'volume': '$volume',
                        'listingprice': '$listingprice',
                        'oldprice': '$oldprice',
                        'LastUpdate': '$LastUpdate',
                        'Navi': '$Navi',
                        'url': '$url',
                        'gest_match_code': '$gest_match_code'
                    }
                }
            }
        ]

    # keyword
    if search is False and keyword is not False:
        print("KEYWORD")
        pipe = [
            {
                '$match': {
                    '$text': {
                        '$search': keyword
                    }
                }
            },
            {
                '$group': {
                    '_id': {
                        'id': '$id',
                    }
                }
            }
        ]

        total = app.config['cpool']['collection_letu_final'].aggregate(pipe)
        total = len(list(total))

        pipe = [
            {
                '$match': {
                    '$text': {
                        '$search': keyword
                    }
                }
            },
            {
                '$sort': { 'score': { '$meta': "textScore" } }
            },
            {
                '$skip': start
            },
            {
                '$limit': perPage
            },
            {
                '$group': {
                    '_id': {
                        'name': '$name',
                        'name_e': '$name_e',
                        'brand': '$brand',
                        'artic': '$articul',
                        'desc': '$desc',
                        'id': '$id',
                        'img': '$img',
                        'volume': '$volume',
                        'listingprice': '$listingprice',
                        'oldprice': '$oldprice',
                        'LastUpdate': '$LastUpdate',
                        'Navi': '$Navi',
                        'url': '$url',
                        'gest_match_code': '$gest_match_code'
                    }
                }
            }
        ]

    # keyword + search
    if keyword is not False and search is not False:
        print("KW + S")
        pipe = [
            {
                '$match': {
                    '$text': {
                        '$search': keyword
                    }
                }
            },
            {
                '$match': {
                    'brand': search
                }
            },
            {
                '$group': {
                    '_id': {
                        'id': '$id'
                    }
                }
            }
        ]

        total = app.config['cpool']['collection_letu_final'].aggregate(pipe)
        total = len(list(total))

        pipe = [
            {
                '$match': {
                    '$text': {
                        '$search': keyword
                    }
                }
            },
            {
                '$sort': { 'score': { '$meta': "textScore" } }
            },
            {
                '$match': {
                    'brand': search
                }
            },
            {
                '$skip': start
            },
            {
                '$limit': perPage
            },
            {
                '$group': {
                    '_id': {
                        'name': '$name',
                        'name_e': '$name_e',
                        'brand': '$brand',
                        'artic': '$articul',
                        'desc': '$desc',
                        'id': '$id',
                        'img': '$img',
                        'volume': '$volume',
                        'listingprice': '$listingprice',
                        'oldprice': '$oldprice',
                        'LastUpdate': '$LastUpdate',
                        'Navi': '$Navi',
                        'url': '$url',
                        'gest_match_code': '$gest_match_code'
                    }
                }
            }
        ]

    out = app.config['cpool']['collection_letu_final'].aggregate(pipe)
    out_list = {
        'count': total,
        'data': list(out)
    }
    return jsonify(out_list)



@app.route('/v1/ilde_products', methods=['GET', 'POST'])
def ilde_products():

    articul = request.args.get('art')
    search = request.args.get('search')
    keyword = request.args.get('kw')

    print(('articul: ', articul, 'keyword: ', keyword, 'search: ', search))

    if search is None or search == 'undefined' or search == '' or search == 'null':
        search = False
    else:
        search = str(search.encode('utf8').strip())

    if keyword is None or keyword == 'undefined' or keyword == '' or keyword == 'null':
        keyword = False
    else:
        keyword = str(keyword.encode('utf8').strip())

    if articul is None or articul == 'undefined' or articul == '' or articul == 'null':
        articul = False
    else:
        articul = str(articul.encode('utf8').strip())

    # dbg params
    print(('articul: ', articul, 'keyword: ', keyword, 'search: ', search))

    page = int(request.args.get('page'))
    perPage = int(request.args.get('perPage'))

    start = (page - 1) * perPage
    end = start + perPage

    # brand
    if search is not False and keyword is False:
        print("BRAND ONLY")
        pipe = [
            {
                '$match': {
                    'brand': search
                }
            },
            {
                '$group': {
                    '_id': {
                        'brand': '$brand',
                    }
                }
            },
            {
                '$sort': {
                    'name': 1
                }
            }
        ]
        total = len(list(app.config['cpool']['collection_ilde_final'].aggregate(pipe)))
        pipe = [
            {
                '$match': {
                    'brand': search
                }
            },
            {
                '$sort': {
                    'LastUpdate': -1
                }
            },
            {
                '$skip': start
            },
            {
                '$limit': perPage
            },
            {
                '$group': {
                    '_id': {
                        'name': '$pn',
                        'brand': '$brand',
                        'artic': '$articul',
                        'image': '$image',
                        'gestori': '$gestori',
                        'listingprice': '$listingprice',
                        'desc': '$desc',
                        'url': '$url',
                        'big_pic': '$big_pic',
                        'vip_price': '$vip_price',
                        'LastUpdate': '$LastUpdate',
                        'date': '$date',
                        'Navi': '$Navi',
                        'vol': '$vol',
                        'id': '$id',
                        'gest_match_code': '$gest_match_code'
                    }
                }
            }
        ]

    # keyword
    if search is False and keyword is not False:
        print("KEYWORD ONLY")
        pipe = [
            {
                '$match': {
                    '$text': {
                        '$search': keyword
                    }
                }
            },
            {
                '$group': {
                    '_id': {
                        'name': '$pn',
                    }
                }
            },
            {
                '$sort': {
                    'name': 1
                }
            }
        ]
        total = len(list(app.config['cpool']['collection_ilde_final'].aggregate(pipe)))
        pipe = [
            {
                '$match': {
                    '$text': {
                        '$search': keyword
                    }
                }
            },
            {
                '$sort': { 'score': { '$meta': "textScore" } }
            },
            {
                '$skip': start
            },
            {
                '$limit': perPage
            },
            {
                '$group': {
                    '_id': {
                        'name': '$pn',
                        'brand': '$brand',
                        'artic': '$articul',
                        'image': '$image',
                        'gestori': '$gestori',
                        'desc': '$desc',
                        'listingprice': '$listingprice',
                        'url': '$url',
                        'big_pic': '$big_pic',
                        'vip_price': '$vip_price',
                        'LastUpdate': '$LastUpdate',
                        'date': '$date',
                        'Navi': '$Navi',
                        'vol': '$vol',
                        'id': '$id',
                        'gest_match_code': '$gest_match_code'
                    }
                }
            }
        ]

    # all
    if search is False and keyword is False:
        print("ALL")
        total = app.config['cpool']['collection_ilde_final'].find().count()
        pipe = [
            {
                '$sort': {
                    'LastUpdate': -1
                }
            },
            {
                '$skip': start
            },
            {
                '$limit': perPage
            },
            {
                '$group': {
                    '_id': {
                        'name': '$pn',
                        'brand': '$brand',
                        'artic': '$articul',
                        'image': '$image',
                        'gestori': '$gestori',
                        'desc': '$desc',
                        'listingprice': '$listingprice',
                        'url': '$url',
                        'big_pic': '$big_pic',
                        'vip_price': '$vip_price',
                        'LastUpdate': '$LastUpdate',
                        'date': '$date',
                        'Navi': '$Navi',
                        'vol': '$vol',
                        'id': '$id',
                        'gest_match_code': '$gest_match_code'
                    }
                }
            }
        ]
        print('ILDE MATCH ALL')

    # keyword + brand
    if search is not False and keyword is not False:
        print("KEYWORD + BRAND")
        pipe = [
            {
                '$match': {
                    '$text': {'$search': keyword}
                }
            },
            {
                '$match': {
                    'brand': search.upper()
                }
            },
            {
                '$sort': { 'score': { '$meta': "textScore" } }
            },
            {
                '$group': {
                    '_id': {
                        'name': '$pn',
                    }
                }
            }
        ]
        total = len(list(app.config['cpool']['collection_ilde_final'].aggregate(pipe)))
        pipe = [
            {
                '$match': {
                    '$text': {
                            '$search': keyword
                        }
                }
            },
            {
                '$match': {
                    'brand': search.upper()
                }
            },
            {
                '$sort': { 'score': { '$meta': "textScore" } }
            },
            {
                '$skip': start
            },
            {
                '$limit': perPage
            },
            {
                '$group': {
                    '_id': {
                        'name': '$pn',
                        'brand': '$brand',
                        'artic': '$articul',
                        'image': '$image',
                        'gestori': '$gestori',
                        'desc': '$desc',
                        'listingprice': '$listingprice',
                        'url': '$url',
                        'big_pic': '$big_pic',
                        'vip_price': '$vip_price',
                        'LastUpdate': '$LastUpdate',
                        'date': '$date',
                        'Navi': '$Navi',
                        'vol': '$vol',
                        'id': '$id',
                        'gest_match_code': '$gest_match_code'
                    }
                }
            }
        ]

    out = app.config['cpool']['collection_ilde_final'].aggregate(pipe)
    out_list = {
        'count': total,
        'data': list(out)
    }

    return jsonify(out_list)



@app.route('/v1/rive_products', methods=['GET', 'POST'])
def rive_products():

    articul = request.args.get('art')
    search = request.args.get('search')
    keyword = request.args.get('kw')

    if search is None or search == 'undefined' or search == '' or search == 'null':
        search = False
    else:
        search = str(search.encode('utf8').strip())

    if articul is None or articul == 'undefined' or articul == '' or articul == 'null':
        articul = False
    else:
        articul = str(articul.encode('utf8').strip())

    if keyword is None or keyword == 'undefined' or keyword == '' or keyword == 'null':
        keyword = False
    else:
        keyword = str(keyword.encode('utf8').strip())



    print(('search:', search, 'articul:', articul, 'keyword:', keyword))

    page = int(request.args.get('page'))
    perPage = int(request.args.get('perPage'))

    start = (page - 1) * perPage
    end = start + perPage

    # only brand
    if search is not False and keyword is False and articul is False:
        total = app.config['cpool']['collection_rive_final'].find({
            'brand': search
        }).count()
        pipe = [
            {
                '$match': {
                    'brand': search
                }
            },
            {
                '$sort': {
                    'lastupdate': -1
                }
            },
            {
                '$skip': start
            },
            {
                '$limit': perPage
            },
            {
                '$group': {
                    '_id': {
                        'name': '$name',
                        'name_e': '$name_e',
                        'brand': '$brand',
                        'artic': '$articul',
                        'desc': '$desc',
                        'code': '$code',
                        'image_url': '$image_url',
                        'country': '$country',
                        'id': '$id',
                        'listingprice': '$listingprice',
                        'goldprice': '$goldprice',
                        'standardcardprice': '$standardcardprice',
                        'lastupdate': '$lastupdate',
                        'volume': '$volume',
                        'volumefieldname': '$volumefieldname',
                        'navi': '$navi',
                        'url': '$url',
                        'gest_match_code': '$gest_match_code'
                    }
                }
            }
        ]
        print('MATCH BRAND')

    # brand + keyword
    if search is not False and keyword is not False and articul is False:
        pipe = [
            {
                '$match': {
                    '$text': {
                        '$search': keyword
                    }
                }
            },
            {
                '$match': {
                    'brand': search
                }
            },
            {
                '$group': {
                    '_id': {
                        'name': '$name',
                    }
                }
            }
        ]

        total = len(list(app.config['cpool']['collection_rive_final'].aggregate(pipe)))

        pipe = [
            {
                '$match': {
                    '$text': {
                        '$search': keyword
                    }
                }
            },
            {
                '$sort': { 'score': { '$meta': "textScore" } }
            },
            {
                '$match': {
                    'brand': search
                }
            },
            {
                '$skip': start
            },
            {
                '$limit': perPage
            },
            {
                '$group': {
                    '_id': {
                        'name': '$name',
                        'name_e': '$name_e',
                        'brand': '$brand',
                        'artic': '$articul',
                        'desc': '$desc',
                        'code': '$code',
                        'image_url': '$image_url',
                        'country': '$country',
                        'id': '$id',
                        'listingprice': '$listingprice',
                        'goldprice': '$goldprice',
                        'standardcardprice': '$standardcardprice',
                        'lastupdate': '$lastupdate',
                        'volume': '$volume',
                        'volumefieldname': '$volumefieldname',
                        'navi': '$navi',
                        'url': '$url',
                        'gest_match_code': '$gest_match_code'
                    }
                }
            }
        ]

    # only keyword
    if search is False and keyword is not False and articul is False:
        print("ONLY KEYWORD")
        # first count
        pipe = [
            {
                '$match': {
                    '$text': {
                        '$search': keyword
                    }
                }
            },
            {
                '$sort': { 'score': { '$meta': "textScore" } }
            }
        ]
        total = app.config['cpool']['collection_rive_final'].aggregate(pipe)
        total = len(list(total))
        print(('TOTAL: ', total))

        # then a real results
        pipe = [
            {
                '$match': {
                    '$text': {
                        '$search': keyword
                    }
                }
            },
            {
                '$sort': { 'score': { '$meta': "textScore" } }
            },
            {
                '$skip': start
            },
            {
                '$limit': perPage
            },
            {
                '$group': {
                    '_id': {
                        'name': '$name',
                        'name_e': '$name_e',
                        'brand': '$brand',
                        'artic': '$articul',
                        'desc': '$desc',
                        'code': '$code',
                        'image_url': '$image_url',
                        'country': '$country',
                        'id': '$id',
                        'listingprice': '$listingprice',
                        'goldprice': '$goldprice',
                        'standardcardprice': '$standardcardprice',
                        'lastupdate': '$lastupdate',
                        'volume': '$volume',
                        'volumefieldname': '$volumefieldname',
                        'navi': '$navi',
                        'url': '$url',
                        'gest_match_code': '$gest_match_code'
                    }
                }
            }
        ]

    # in case of articul is not empty
    if articul is not False:
        total = app.config['cpool']['collection_rive_final'].find({
            'articul': articul
        }).count()
        pipe = [
            {
                '$match': {'code': articul}
            },
            {
                '$sort': {
                    'lastupdate': -1
                }
            },
            {
                '$skip': start
            },
            {
                '$limit': perPage
            },
            {
                '$group': {
                    '_id': {
                        'name': '$name',
                        'name_e': '$name_e',
                        'brand': '$brand',
                        'artic': '$articul',
                        'desc': '$desc',
                        'code': '$code',
                        'image_url': '$image_url',
                        'country': '$country',
                        'id': '$id',
                        'listingprice': '$listingprice',
                        'goldprice': '$goldprice',
                        'standardcardprice': '$standardcardprice',
                        'lastupdate': '$lastupdate',
                        'volume': '$volume',
                        'volumefieldname': '$volumefieldname',
                        'navi': '$navi',
                        'url': '$url',
                        'gest_match_code': '$gest_match_code'
                    }
                }
            }
        ]

    # all what we have
    if search is False and articul is False and keyword is False:
        total = app.config['cpool']['collection_rive_final'].find().count()
        pipe = [
            {
                '$sort': {
                    'lastupdate': -1
                }
            },
            {
                '$skip': start
            },
            {
                '$limit': perPage
            },
            {
                '$group': {
                    '_id': {
                        'name': '$name',
                        'name_e': '$name_e',
                        'brand': '$brand',
                        'artic': '$articul',
                        'desc': '$desc',
                        'code': '$code',
                        'image_url': '$image_url',
                        'country': '$country',
                        'id': '$id',
                        'listingprice': '$listingprice',
                        'goldprice': '$goldprice',
                        'standardcardprice': '$standardcardprice',
                        'lastupdate': '$lastupdate',
                        'volume': '$volume',
                        'volumefieldname': '$volumefieldname',
                        'navi': '$navi',
                        'url': '$url',
                        'gest_match_code': '$gest_match_code'
                    }
                }
            }
        ]

    out = app.config['cpool']['collection_rive_final'].aggregate(pipe)
    out_list = {
        'count': total,
        'data': list(out)
    }

    return dumps(out_list)



@app.route('/v1/podr_products', methods=['GET', 'POST'])
def podr_products():

    articul = request.args.get('art')
    search = request.args.get('search')
    keyword = request.args.get('kw')

    if search is None or search == 'undefined' or search == '' or search == 'null':
        search = False
    else:
        search = str(search.encode('utf8').strip())

    if articul is None or articul == 'undefined' or articul == '' or articul == 'null':
        articul = False
    else:
        articul = str(articul.encode('utf8').strip())

    if keyword is None or keyword == 'undefined' or keyword == '' or keyword == 'null':
        keyword = False
    else:
        keyword = str(keyword.encode('utf8').strip())



    print(('search:', search, 'articul:', articul, 'keyword:', keyword))

    page = int(request.args.get('page'))
    perPage = int(request.args.get('perPage'))

    start = (page - 1) * perPage
    end = start + perPage

    # only brand
    if search is not False and keyword is False and articul is False:
        total = app.config['cpool']['collection_podr_final'].find({
            'brand': search
        }).count()
        pipe = [
            {
                '$match': {
                    'brand': search
                }
            },
            {
                '$skip': start
            },
            {
                '$limit': perPage
            },
            {
                '$group': {
                    '_id': {
                        'name': '$name',
                        'brand': '$brand',
                        'country': '$country',
                        'articul': '$articul',
                        'img': '$img',
                        'price': '$price',
                        'navi': '$navi',
                        'url': '$url',
                        'date': '$date'
                    }
                }
            }
        ]
        print('MATCH BRAND')

    # brand + keyword
    if search is not False and keyword is not False and articul is False:
        pipe = [
            {
                '$match': {
                    '$text': {
                        '$search': keyword
                    }
                }
            },
            {
                '$match': {
                    'brand': search
                }
            },
            {
                '$group': {
                    '_id': {
                        'name': '$name',
                    }
                }
            }
        ]

        total = len(list(app.config['cpool']['collection_podr_final'].aggregate(pipe)))

        pipe = [
            {
                '$match': {
                    '$text': {
                        '$search': keyword
                    }
                }
            },
            {
                '$sort': { 'score': { '$meta': "textScore" } }
            },
            {
                '$match': {
                    'brand': search
                }
            },
            {
                '$skip': start
            },
            {
                '$limit': perPage
            },
            {
                '$group': {
                    '_id': {
                        'name': '$name',
                        'brand': '$brand',
                        'country': '$country',
                        'articul': '$articul',
                        'img': '$img',
                        'price': '$price',
                        'navi': '$navi',
                        'url': '$url',
                        'date': '$date'
                    }
                }
            }
        ]

    # only keyword
    if search is False and keyword is not False and articul is False:
        print("ONLY KEYWORD")
        # first count
        pipe = [
            {
                '$match': {
                    '$text': {
                        '$search': keyword
                    }
                }
            },
            {
                '$sort': { 'score': { '$meta': "textScore" } }
            }
        ]
        total = app.config['cpool']['collection_podr_final'].aggregate(pipe)
        total = len(list(total))
        print(('TOTAL: ', total))

        # then a real results
        pipe = [
            {
                '$match': {
                    '$text': {
                        '$search': keyword
                    }
                }
            },
            {
                '$sort': { 'score': { '$meta': "textScore" } }
            },
            {
                '$skip': start
            },
            {
                '$limit': perPage
            },
            {
                '$group': {
                    '_id': {
                        'name': '$name',
                        'brand': '$brand',
                        'country': '$country',
                        'articul': '$articul',
                        'img': '$img',
                        'price': '$price',
                        'navi': '$navi',
                        'url': '$url',
                        'date': '$date'
                    }
                }
            }
        ]

    # in case of articul is not empty
    if articul is not False:
        total = app.config['cpool']['collection_podr_final'].find({
            'articul': articul
        }).count()
        pipe = [
            {
                '$match': {'code': articul}
            },
            {
                '$sort': {
                    'lastupdate': -1
                }
            },
            {
                '$skip': start
            },
            {
                '$limit': perPage
            },
            {
                '$group': {
                    '_id': {
                        'name': '$name',
                        'brand': '$brand',
                        'country': '$country',
                        'articul': '$articul',
                        'img': '$img',
                        'price': '$price',
                        'navi': '$navi',
                        'url': '$url',
                        'date': '$date'
                    }
                }
            }
        ]

    # all what we have
    if search is False and articul is False and keyword is False:
        total = app.config['cpool']['collection_podr_final'].find().count()
        pipe = [
            {
                '$sort': {
                    'lastupdate': -1
                }
            },
            {
                '$skip': start
            },
            {
                '$limit': perPage
            },
            {
                '$group': {
                    '_id': {
                        'name': '$name',
                        'brand': '$brand',
                        'country': '$country',
                        'articul': '$articul',
                        'img': '$img',
                        'price': '$price',
                        'navi': '$navi',
                        'url': '$url',
                        'date': '$date'
                    }
                }
            }
        ]

    out = app.config['cpool']['collection_podr_final'].aggregate(pipe)
    out_list = {
        'count': total,
        'data': list(out)
    }

    return dumps(out_list)



"""
Find product prices
"""
@app.route('/v1/rive_product_price', methods=['GET'])
def rive_product_price():

    out_list = []
    articul = request.args.get('art')
    year = request.args.get('y')
    month = request.args.get('m')
    day = request.args.get('d')

    if articul is not None:
        pipe = [
            {
                '$match': {'code': articul}
            }
        ]

        coll = Utils.getPriceCollection(config, 'RIVE', year, month)
        print(coll)
        out = coll.aggregate(pipe)
        #out = list(out)

        #out = app.config['cpool']['collection_rive_price'].aggregate(pipe)

    return dumps(out)



@app.route('/v1/match', methods=['GET', 'POST'])
def match():

    pipe = []
    setter = {}
    match_doc = {}   # final match doc

    if 'gest' in request.json:
        #find_doc['gest'] = request.json['gest']['barcod']
        pipe.append({'$match': {'gest': {'barcod': request.json['gest']['barcod']}}})
        gest_barcode = request.json['gest']['barcod']
        match_doc['gest_match_code'] = request.json['gest']['barcod']
    if 'rive' in request.json:
        #find_doc['rive'] = request.json['rive']['code']
        pipe.append({'$match': {'rive': {'code':request.json['rive']['code']}}})
        setter['rive_match_code'] = request.json['rive']['code']
        match_doc['rive_match_code'] = request.json['rive']['code']
        # Set product matched
        app.config['cpool']['collection_rive_final'].update_many(
            {'code': request.json['rive']['code']},
            {
                '$set': {
                    'gest_match_code': gest_barcode
                }
            }
        )
    if 'letu' in request.json:
        #find_doc['letu'] = request.json['letu']['artic']
        pipe.append({'$match': {'letu': {'artic': request.json['letu']['artic']}}})
        setter['letu_match_code'] = request.json['letu']['artic']
        match_doc['letu_match_code'] = request.json['letu']['artic']
        app.config['cpool']['collection_letu_final'].update_many(
            {'articul': request.json['letu']['artic']},
            {
                '$set': {
                    'gest_match_code': gest_barcode
                }
            }
        )
    if 'ilde' in request.json:
        #find_doc['ilde'] = request.json['ilde']['artic']
        pipe.append({'$match': {'ilde': {'artic': request.json['ilde']['artic']}}})
        setter['ilde_match_code'] = request.json['ilde']['artic']
        match_doc['ilde_match_code'] = request.json['ilde']['artic']
        app.config['cpool']['collection_ilde_final'].update_many(
            {'articul': request.json['ilde']['artic']},
            {
                '$set': {
                    'gest_match_code': gest_barcode
                }
            }
        )
    # Matched in gest doc
    if len(setter) > 0:
        app.config['cpool']['collection_gestori'].update_many(
            {'Barcod': gest_barcode},
            {
                '$set': setter
            }
        )

    # check double
    #find = app.config['cpool']['matched'].find_one(find_doc)
    #pipe.append({'$count': 'passed_var'})
    find = app.config['cpool']['matched'].aggregate(pipe)
    find = list(find)

    match_doc['date'] = datetime.strftime(datetime.now(), "%d-%m-%Y")

    if len(find) == 0:
        ins = app.config['cpool']['matched'].insert_one(match_doc)
        return jsonify({'double': 0})
    else:
        return jsonify({'double': 1})

    #find = app.config['cpool']['matched']
    #ins = app.config['cpool']['matched'].insert_one(request.json)

    return jsonify(ins)



@app.route('/v1/getMatched', methods=['GET'])
def getMatched():

    search = request.args.get('search')
    if search is None or search == 'undefined':
        search = ''

    search = str(search.encode('utf8').strip())

    total = app.config['cpool']['matched'].count()
    page = int(request.args.get('page'))
    perPage = int(request.args.get('perPage'))

    start = (page - 1) * perPage
    end = start + perPage

    pipe = [
        {'$match': {'deleted': {'$exists': False}}},
        {
            '$lookup': {
               'from': 'RIVE_products_final',
               'localField': 'rive_match_code',
               'foreignField': 'code',
               'as': 'rive'
            }
        },
        {
            '$lookup': {
               'from': 'letu_products_final',
               'localField': 'letu_match_code',
               'foreignField': 'articul',
               'as': 'letu'
            }
        },
        {
            '$lookup': {
               'from': 'ILDE_products_final',
               'localField': 'ilde_match_code',
               'foreignField': 'articul',
               'as': 'ilde'
            }
        },
        {
            '$lookup': {
               'from': 'gestori_up',
               'localField': 'gest_match_code',
               'foreignField': 'Barcod',
               'as': 'gest'
            }
        },
        {
            '$sort': {'_id': -1}
        }
    ]

    res = app.config['cpool']['matched'].aggregate(pipe)
    res = list(res)

    return dumps(res)



@app.route('/v1/matchDelete', methods=['POST'])
def matchDelete():

    """
    Delete matched products
    """
    
    oid = request.data
    oid = json.loads(oid)
    oid = oid['oid']

    if oid is not None:
        res = app.config['cpool']['matched'].update(
                {"_id": ObjectId(oid)},
                {'$set': {'deleted': Utils.getDbprefix()['daily']}}
            )
        print(res)

    print(("OID: "+oid))
    return dumps({'status': 'ok'})



@app.route('/v1/gestMarkChecked', methods=['POST'])
def gestMarkChecked():

    """
    Mark product as verified
    """

    oid = request.data
    oid = json.loads(oid)
    oid = oid['oid']

    print(oid)

    if oid is not None:
        res = app.config['cpool']['collection_gestori'].update_many(
                {"Cod_good": oid},
                {'$set': {'verified': Utils.getDbprefix()['daily']}}
            )
        print(res)

    return dumps({'status': 'ok'})



@app.route('/v1/ft', methods=['GET', 'POST'])
def ft():

    """ FULL TXT """

    provider = request.args.get('p')
    search = request.args.get('s')
    brand = request.args.get('b')

    print(('BRAND:', brand, 'SEARCH:', search))

    # search param
    if search is None or search == 'undefined' or search == '' or search == 'null':
        search = False
    else:
        search = str(search.encode('utf8').strip())
        # set query string limit
        if len(search) > 40 or len(search) < 4:
            # fix ngFor array
            return jsonify([])

    # brand param
    if brand is None or brand == 'undefined' or brand == '' or brand == 'null':
        brand = False
    else:
        brand = str(brand.encode('utf8').strip())

    # only gestori has a upper B
    if 'gest' in provider:
        brand_field = 'name_brand'
    else:
        brand_field = 'brand'

    # keyword + brand
    if brand is not False and search is not False:
        pipe = [
            {
                '$match': {
                    '$text': {
                        '$search': search,
                    }
                }
            },
            {
                '$match': {
                    brand_field: brand
                }
            },
            {
                '$limit': 300
            },
            {
                '$sort': {
                    'Name': 1
                }
            }
        ]

    # only keyword
    if brand is False and search is not False:
        pipe = [
            {
                '$match': {
                    '$text': {
                        '$search': search,
                    }
                }
            },
            {
                '$sort': { 'score': { '$meta': "textScore" } }
            },
            {
                '$limit': 100
            }
        ]

    # only brand
    if brand is not False and search is False:
        pipe = [
            {
                '$match': {
                    brand_field: brand
                }
            },
            {
                '$limit': 300
            },
            {
                '$sort': {
                    'Name': 1
                }
            }
        ]

    # all false
    if brand is False and search is False:
        return jsonify([])

    if 'gest' in provider:
        out = app.config['cpool']['collection_gestori'].aggregate(pipe)
    if 'rive' in provider:
        out = app.config['cpool']['collection_rive_final'].aggregate(pipe)
    if 'ilde' in provider:
        out = app.config['cpool']['collection_ilde_final'].aggregate(pipe)
    if 'letu' in provider:
        out = app.config['cpool']['collection_letu_final'].aggregate(pipe)
    if 'podr' in provider:
        out = app.config['cpool']['collection_podr_final'].aggregate(pipe)
    return dumps(out)



if __name__ == "__main__":
    app.jinja_env.auto_reload = True
    app.config['TEMPLATES_AUTO_RELOAD'] = True
    app.run(host='0.0.0.0', threaded=True, debug=True)