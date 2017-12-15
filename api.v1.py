# -*- coding: utf-8 -*-

import os
import re
import csv
import json
import pipes
import urllib
import urllib2
import subprocess
import configparser
from datetime import datetime
from bson.objectid import ObjectId
from flask_compress import Compress
from werkzeug.utils import secure_filename
from pymongo import MongoClient, ReturnDocument
from flask_debugtoolbar import DebugToolbarExtension
from flask import Flask, flash, render_template, request, redirect, g
from flask import jsonify
from bson.json_util import dumps
from flask_cors import CORS, cross_origin

from lib.filters import Filters
from lib.utils import Utils



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



@app.route('/ping')
def ping():

    """ PING """

    pipeline = [
        {
            '$limit': 20
        }
    ]

    out = app.config['cpool']['collection_ilde_final'].aggregate(pipeline)
    if out is not None:
        return 'pong'

    return False



@app.route('/brands', methods=['GET', 'POST'])
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



@app.route('/brands_letu', methods=['GET', 'POST'])
def brands_letu():

    """ get letu brands """

    out = app.config['cpool']['brands_letu'].distinct("name")
    return dumps(out)



@app.route('/all_brands', methods=['GET', 'POST'])
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



@app.route('/gestori_groups', methods=['GET', 'POST'])
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
@app.route('/gestori_products', methods=['GET', 'POST'])
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
    print('articul: ', articul, 'keyword: ', keyword, 'search: ', search)

    if search is None or search == 'undefined' or search == '' or search == 'null':
        search = ''

    #total = app.config['cpool']['collection_gestori'].count()

    start = (page - 1) * perPage
    end = start + perPage

    # SEARCH BY BRAND
    if search is not False and articul is False and keyword is False:
        # need extra count
        total = app.config['cpool']['collection_gestori'].find({
            'Brand': {
                '$regex': "^"+search, '$options': '-i'
            }
        }).count()
        pipe = [
            {
                '$match': {
                    'Brand': {
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
                '$group': {
                    '_id': {
                        'name': '$Name',
                        'brand': '$Brand',
                        'artic': '$Artic',
                        'name_e': '$Name_e',
                        'cod_good': '$Cod_good',
                        'retail_price': '$Retail_price',
                        'barcod': '$Barcod',
                        'id': '$id'
                    }
                }
            },
            {
                '$sort': {
                    'name': 1
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
                '$group': {
                    '_id': {
                        'name': '$Name',
                        'brand': '$Brand',
                        'artic': '$Artic',
                        'name_e': '$Name_e',
                        'cod_good': '$Cod_good',
                        'retail_price': '$Retail_price',
                        'barcod': '$Barcod',
                        'id': '$id'
                    }
                }
            },
            {
                '$sort': {
                    'name': 1
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
                '$group': {
                    '_id': {
                        'name': '$Name',
                        'brand': '$Brand',
                        'artic': '$Artic',
                        'name_e': '$Name_e',
                        'cod_good': '$Cod_good',
                        'retail_price': '$Retail_price',
                        'barcod': '$Barcod',
                        'id': '$id'
                    }
                }
            },
            {
                '$sort': {
                    'name': 1
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
                    'Brand': search
                }
            },
            {
                '$group': {
                    '_id': {
                        'name': '$Name'
                    }
                }
            },
            {
                '$sort': {
                    'name': 1
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
                '$match': {
                    'Brand': search
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
                        'brand': '$Brand',
                        'artic': '$Artic',
                        'name_e': '$Name_e',
                        'cod_good': '$Cod_good',
                        'retail_price': '$Retail_price',
                        'barcod': '$Barcod',
                        'id': '$id'
                    }
                }
            },
            {
                '$sort': {
                    'name': 1
                }
            }
        ]

        print('KEYWORD + BRAND ' + keyword)

    # fulltext by keyword
    if keyword is not False and search is False and articul is False:
        total = app.config['cpool']['collection_gestori'].find({
            'Name': keyword
        }).count()
        print('KEYWORD: ', keyword.encode('utf8').strip())
        pipe = [
            {
                '$match': {
                    '$text': {
                        '$search': keyword,
                    }
                }
            },
            {
                '$limit': 300
            },
            {
                '$group': {
                    '_id': {
                        'name': '$Name',
                        'brand': '$Brand',
                        'artic': '$Artic',
                        'name_e': '$Name_e',
                        'cod_good': '$Cod_good',
                        'retail_price': '$Retail_price',
                        'barcod': '$Barcod',
                        'id': '$id'
                    }
                }
            },
            {
                '$sort': {
                    'name': 1
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



@app.route('/letu_products', methods=['GET', 'POST'])
def letu_products():

    """letoile products"""

    articul = request.args.get('art')
    search = request.args.get('search')
    keyword = request.args.get('keyword')

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

    # brand
    if search is not False and keyword is False:
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
                        'artic': '$articul',
                        'desc': '$desc',
                        'id': '$id',
                        'img': '$img',
                        'volume': '$volume',
                        'listingprice': '$listingprice',
                        'url': '$url'
                    }
                }
            },
            {
                '$sort': {
                    'name': 1
                }
            }
        ]

    # all
    if search is False and keyword is False:
        total = app.config['cpool']['collection_letu_final'].find().count()
        pipe = [
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
                        'artic': '$articul',
                        'desc': '$desc',
                        'id': '$id',
                        'img': '$img',
                        'volume': '$volume',
                        'listingprice': '$listingprice',
                        'url': '$url'
                    }
                }
            },
            {
                '$sort': {
                    'name': 1
                }
            }
        ]

    # keyword
    if search is False and keyword is not False:
        pipe = [
            {
                '$match': {
                    '$text': keyword
                }
            },
            {
                '$group': {
                    '_id': {
                        'name': '$name',
                        'brand': '$brand',
                        'artic': '$articul',
                        'desc': '$desc',
                        'id': '$id',
                        'img': '$img',
                        'volume': '$volume',
                        'listingprice': '$listingprice',
                        'url': '$url'
                    }
                }
            }
        ]

        total = app.config['cpool']['collection_letu_final'].aggregate(pipe)
        total = len(list(total))

        pipe = [
            {
                '$match': {
                    '$text': keyword
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
                        'artic': '$articul',
                        'desc': '$desc',
                        'id': '$id',
                        'img': '$img',
                        'volume': '$volume',
                        'listingprice': '$listingprice',
                        'url': '$url'
                    }
                }
            },
            {
                '$sort': {
                    'name': 1
                }
            }
        ]

    out = app.config['cpool']['collection_letu_final'].aggregate(pipe)
    out_list = {
        'count': total,
        'data': list(out)
    }
    return jsonify(out_list)



@app.route('/ilde_products', methods=['GET', 'POST'])
def ilde_products():

    articul = request.args.get('art')
    search = request.args.get('search')
    keyword = request.args.get('keyword')

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
    print('articul: ', articul, 'keyword: ', keyword, 'search: ', search)

    page = int(request.args.get('page'))
    perPage = int(request.args.get('perPage'))

    start = (page - 1) * perPage
    end = start + perPage

    # brand
    if search is not False and keyword is False:
        total = app.config['cpool']['collection_ilde_final'].find(
            {
                'brand': search
            }
        ).count()
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
                        'name': '$pn',
                        'brand': '$brand',
                        'artic': '$articul',
                        'image': '$image',
                        'gestori_match': '$gestori',
                        'listingprice': '$listingprice',
                        'id': '$id'
                    }
                }
            },
            {
                '$sort': {
                    'name': 1
                }
            }
        ]

    # keyword
    if search is False and keyword is not False:
        pipe = [
            {
                '$match': {
                    '$text': keyword
                }
            },
            {
                '$group': {
                    '_id': {
                        'name': '$pn',
                        'brand': '$brand',
                        'artic': '$articul',
                        'image': '$image',
                        'gestori_match': '$gestori',
                        'listingprice': '$listingprice',
                        'id': '$id'
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
                    '$text': keyword
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
                        'gestori_match': '$gestori',
                        'listingprice': '$listingprice',
                        'id': '$id'
                    }
                }
            },
            {
                '$sort': {
                    'name': 1
                }
            }
        ]

    # all
    if search is False and keyword is False:
        total = app.config['cpool']['collection_ilde_final'].find().count()
        pipe = [
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
                        'gestori_match': '$gestori',
                        'listingprice': '$listingprice',
                        'id': '$id'
                    }
                }
            },
            {
                '$sort': {
                    'name': 1
                }
            }
        ]
        print('ILDE MATCH ALL')

    out = app.config['cpool']['collection_ilde_final'].aggregate(pipe)
    out_list = {
        'count': total,
        'data': list(out)
    }

    return jsonify(out_list)



@app.route('/rive_products', methods=['GET', 'POST'])
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



    print('search:', search, 'articul:', articul, 'keyword:', keyword)

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
                        'volume': '$volume',
                        'volumefieldname': '$volumefieldname',
                        'url': '$url'
                    }
                }
            },
            {
                '$sort': {
                    'name': 1
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
                        'volume': '$volume',
                        'volumefieldname': '$volumefieldname',
                        'url': '$url'
                    }
                }
            },
            {
                '$sort': {
                    'name': 1
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
                        'volume': '$volume',
                        'volumefieldname': '$volumefieldname',
                        'url': '$url'
                    }
                }
            },
            {
                '$sort': {
                    'name': 1
                }
            }
        ]

    # only keyword
    if search is False and keyword is not False and articul is False:
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
                        'volume': '$volume',
                        'volumefieldname': '$volumefieldname',
                        'url': '$url'
                    }
                }
            }
        ]
        total = app.config['cpool']['collection_rive_final'].aggregate(pipe)
        total = len(list(total))
        print('TOTAL: ', total)

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
                        'volume': '$volume',
                        'volumefieldname': '$volumefieldname',
                        'url': '$url'
                    }
                }
            },
            {
                '$sort': {
                    'name': 1
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
                        'volume': '$volume',
                        'volumefieldname': '$volumefieldname',
                        'url': '$url'
                    }
                }
            },
            {
                '$sort': {
                    'name': 1
                }
            }
        ]

    # all what we have
    if search is False and articul is False and keyword is False:
        total = app.config['cpool']['collection_rive_final'].find().count()
        pipe = [
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
                        'volume': '$volume',
                        'volumefieldname': '$volumefieldname',
                        'url': '$url'
                    }
                }
            },
            {
                '$sort': {
                    'name': 1
                }
            }
        ]

    out = app.config['cpool']['collection_rive_final'].aggregate(pipe)
    out_list = {
        'count': total,
        'data': list(out)
    }
    for li, lv in enumerate(out_list['data']):
        for zi, zv in enumerate(out_list['data'][li]):
            # trim description
            if 'desc' in out_list['data'][li]['_id']:
                if out_list['data'][li]['_id']['desc'] is not None:
                    out_list['data'][li]['_id']['desc'] = out_list['data'][li]['_id']['desc'][:200]+" ..."

    return jsonify(out_list)



@app.route('/match', methods=['GET', 'POST'])
def match():

    pipe = []
    if 'rive' in request.json:
        #find_doc['rive'] = request.json['rive']['code']
        pipe.append({'$match': {'rive': {'code':request.json['rive']['code']}}})
    if 'gest' in request.json:
        #find_doc['gest'] = request.json['gest']['barcod']
        pipe.append({'$match': {'gest': {'barcod': request.json['gest']['barcod']}}})
    if 'letu' in request.json:
        #find_doc['letu'] = request.json['letu']['artic']
        pipe.append({'$match': {'letu': {'artic': request.json['letu']['artic']}}})
    if 'ilde' in request.json:
        #find_doc['ilde'] = request.json['ilde']['artic']
        pipe.append({'$match': {'ilde': {'artic': request.json['ilde']['artic']}}})

    # check double
    #find = app.config['cpool']['matched'].find_one(find_doc)
    #pipe.append({'$count': 'passed_var'})
    find = app.config['cpool']['matched'].aggregate(pipe)
    find = list(find)

    if len(find) == 0:
        ins = app.config['cpool']['matched'].insert_one(request.json)
        return jsonify({'double': 0})
    else:
        return jsonify({'double': 1})

    #find = app.config['cpool']['matched']
    #ins = app.config['cpool']['matched'].insert_one(request.json)

    return jsonify(ins)



@app.route('/getMatched', methods=['GET'])
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
        {
            '$sort': {'_id': -1}
        },
        {
            '$skip': start
        },
        {
            '$limit': perPage
        }
    ]

    res = app.config['cpool']['matched'].aggregate(pipe)
    res = list(res)

    return dumps(res)



@app.route('/ft', methods=['GET', 'POST'])
def ft():

    """ FULL TXT """

    provider = request.args.get('p')
    search = request.args.get('s')
    brand = request.args.get('b')

    print('BRAND:', brand, 'SEARCH:', search)

    # search param
    if search is None or search == 'undefined' or search == '' or search == 'null':
        search = False
    else:
        search = str(search.encode('utf8').strip())
        # set query string limit
        if len(search) > 40 or len(search) < 2:
            # fix ngFor array
            return jsonify([])

    # brand param
    if brand is None or brand == 'undefined' or brand == '' or brand == 'null':
        brand = False
    else:
        brand = str(brand.encode('utf8').strip())

    # only gestori has a upper B
    if 'gest' in provider:
        brand_field = 'Brand'
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
                '$limit': 300
            },
            {
                '$sort': {
                    'Name': 1
                }
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
    return dumps(out)



if __name__ == "__main__":
    app.jinja_env.auto_reload = True
    app.config['TEMPLATES_AUTO_RELOAD'] = True
    app.run(host='0.0.0.0', threaded=True)