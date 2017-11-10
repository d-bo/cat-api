# -*- coding: utf-8 -*-

import os
import re
import csv
import json
import pipes
import urllib
import subprocess
import pandas as pd
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

    return jsonify(dumps(out))



@app.route('/brands_letu', methods=['GET', 'POST'])
def brands_letu():

    """ get letu brands """

    out = app.config['cpool']['brands_letu'].distinct("name")
    return jsonify(dumps(out))



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

    return jsonify(dumps(out))



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

    return jsonify(dumps(out))



@app.route('/gestori_csv_put', methods=['GET', 'POST'])
def gestori_csv_put():

    """ save gestori csv to /mnt/boffice """

    group_id = request.args.get('group_id')
    barcode = request.args.get('barcode')
    name = request.args.get('name')
    artic = request.args.get('artic')
    measure = request.args.get('measure')

    with open('/mnt/from_api.csv', 'a') as file:
        file.write('input')

    return render_template('api.v1.html')



@app.route('/gestori_products', methods=['GET', 'POST'])
def gestori_products():

    # search by articul ?
    articul = request.args.get('art')
    # or by string ?
    search = request.args.get('search')
    if search is None or search == 'undefined':
        search = ''

    search = str(search.encode('utf8').strip())

    # set query string limit
    if len(search) > 15:
        return jsonify({'count': 0, 'data': []})

    total = app.config['cpool']['collection_gestori'].count()
    page = int(request.args.get('page'))
    perPage = int(request.args.get('perPage'))

    start = (page - 1) * perPage
    end = start + perPage

    if search != '' and articul is None:
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
            }
        ]
    else:
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
            }
        ]

    # in case of articul is not empty
    if articul is not None:
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
            }
        ]

    out = app.config['cpool']['collection_gestori'].aggregate(pipe)
    out_list = {
        'count': total,
        'data': list(out)
    }
    return jsonify(out_list)



@app.route('/letu_products', methods=['GET', 'POST'])
def letu_products():

    articul = request.args.get('art')
    search = request.args.get('search')
    if search is None or search == 'undefined':
        search = ''

    search = str(search.encode('utf8').strip())

    total = app.config['cpool']['collection_letu_final'].count()
    page = int(request.args.get('page'))
    perPage = int(request.args.get('perPage'))

    start = (page - 1) * perPage
    end = start + perPage

    if search != '':
        pipe = [
            {
                '$match': {
                    'brand': {
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
    else:
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
            }
        ]

    # autocomplete
    # in case of articul is not empty
    if articul is not None:
        pipe = [
            {
                '$match': {'articul': articul}
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
                        'name_e': '$desc',
                        'id': '$id'
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



@app.route('/ilde_products', methods=['GET', 'POST'])
def ilde_products():

    articul = request.args.get('art')
    search = request.args.get('search')
    if search is None or search == 'undefined':
        search = ''

    search = str(search.encode('utf8').strip())

    total = app.config['cpool']['collection_ilde_final'].count()
    page = int(request.args.get('page'))
    perPage = int(request.args.get('perPage'))

    start = (page - 1) * perPage
    end = start + perPage

    if search != '':
        pipe = [
            {
                '$match': {
                    'brand': {
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
                        'name': '$pn',
                        'brand': '$brand',
                        'artic': '$articul',
                        'image': '$image',
                        'gestori_match': '$gestori',
                        'listingprice': '$listingprice',
                        'id': '$id'
                    }
                }
            }
        ]
    else:
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
            }
        ]

    # in case of articul is not empty
    if articul is not None:
        pipe = [
            {
                '$match': {'articul': articul}
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
            }
        ]

    out = app.config['cpool']['collection_ilde_final'].aggregate(pipe)
    out_list = {
        'count': total,
        'data': list(out)
    }

    return jsonify(out_list)



@app.route('/rive_products', methods=['GET', 'POST'])
def rive_products():

    # search by articul ?
    articul = request.args.get('art')
    # or by string ?
    search = request.args.get('search')
    if search is None or search == 'undefined':
        search = ''

    search = str(search.encode('utf8').strip())

    total = app.config['cpool']['collection_rive_final'].count()
    page = int(request.args.get('page'))
    perPage = int(request.args.get('perPage'))

    start = (page - 1) * perPage
    end = start + perPage

    if search != '':
        pipe = [
            {
                '$match': {
                    'brand': {
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
                        'name': '$name',
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
    else:
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

    # in case of articul is not empty
    if articul is not None:
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

    out = app.config['cpool']['collection_rive_final'].aggregate(pipe)
    out_list = {
        'count': total,
        'data': list(out)
    }
    for li, lv in enumerate(out_list['data']):
        for zi, zv in enumerate(out_list['data'][li]):
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

    return jsonify(dumps(res))



@app.route('/ft', methods=['GET', 'POST'])
def ft():

    """ FULL TXT """

    provider = request.args.get('p')
    search = request.args.get('s')
    if search is None or search == 'undefined':
        search = ''
    search = str(search.encode('utf8').strip())

    # set query string limit
    if len(search) > 40 or len(search) < 2:
        return jsonify({'count': 0, data: []})

    if len(search) > 2:
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
            }
        ]
        
        if 'gest' in provider:
            out = app.config['cpool']['collection_gestori'].aggregate(pipe)
        if 'rive' in provider:
            out = app.config['cpool']['collection_rive_final'].aggregate(pipe)
        if 'ilde' in provider:
            out = app.config['cpool']['collection_ilde_final'].aggregate(pipe)
        if 'letu' in provider:
            out = app.config['cpool']['collection_letu_final'].aggregate(pipe)
        return jsonify(dumps(out))
    else:
        return jsonify({'count': 0, data: []})



if __name__ == "__main__":
    app.jinja_env.auto_reload = True
    app.config['TEMPLATES_AUTO_RELOAD'] = True
    app.run(host='0.0.0.0', threaded=True)
