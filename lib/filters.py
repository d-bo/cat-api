# -*- coding: utf-8 -*-

import jwt

class Filters:


    def __init__(self):
        module_version = '1.0'


    def document_rules(self, recipient_doc):

        """ Filter rules """

        # 3 empty rows -> abort
        if len(recipient_doc) < 3:
            return False

        return True


    @staticmethod
    def check_supplier_duplicate(cpool, doc, supplier_ObjectId, replace = False):

        """ duplicate logic check """

        # TODO: also update by supplier field

        # 1
        # no articul ??
        # check barcode
        if 'bar_code' in doc:
            doc_search = cpool['collection_supplier'].find_one(
                {'bar_code': doc['bar_code']},
                {'supplier_id': supplier_ObjectId}
            )
            # doc found -> upsert by index
            if doc_search:
                if replace is False:
                    cpool['collection_supplier'].update_one(
                        {
                            'bar_code': doc['bar_code'],
                            'supplier_id': supplier_ObjectId
                        },
                        {'$set': doc},
                        upsert=True
                    )
                else:
                    cpool['collection_supplier'].find_one_and_replace(
                        {
                            'bar_code': doc['bar_code'],
                            'supplier_id': supplier_ObjectId
                        },
                        doc
                    )
                return True

        # 2
        # check articul duplicate
        if 'articul' in doc:
            #doc_search = collection_supplier.find_one({'articul': doc['articul']}
            doc_search = cpool['collection_supplier'].find_one(
                {'articul': doc['articul']},
                {'supplier_id': supplier_ObjectId}
            )
            # doc found -> upsert by index
            if doc_search:
                if replace is False:
                    cpool['collection_supplier'].update_one(
                        {
                            'articul': doc['articul'],
                            'supplier_id': supplier_ObjectId
                        },
                        {'$set': doc},
                        upsert=True
                    )
                else:
                    cpool['collection_supplier'].find_one_and_replace(
                        {
                            'articul': doc['articul'],
                            'supplier_id': supplier_ObjectId
                        },
                        doc
                    )
                return True

        # 3
        # no articul and no supplier ?
        # try to match by name
        # TODO
        if 'pn' in doc:
            doc_search = cpool['collection_supplier'].find_one(
                {'pn': doc['pn']},
                {'supplier_id': supplier_ObjectId}
            )
            if doc_search:
                if replace is False:
                    cpool['collection_supplier'].update_one(
                        {
                            'pn': doc['pn'],
                            'supplier_id': supplier_ObjectId
                        },
                        {'$set': doc},
                        upsert=True
                    )
                else:
                    cpool['collection_supplier'].find_one_and_replace(
                        {
                            'pn': doc['pn'],
                            'supplier_id': supplier_ObjectId
                        },
                        doc
                    )
                return True

        return False      # no duplicate found



    @staticmethod
    def validate_auth(cpool, username, password):

        """
        Server side validate auth
        """

        errors = {
            'en': {
                'EMPTY_LGN_OR_PWD': 'Empty login or password'
            }
        }

        # Login or password empty
        if username is None or password is None:
            return {'error': 'EMPTY_LGN_OR_PWD', 'msg': errors['en']['EMPTY_LGN_OR_PWD']}

        # User exist ?
        user = cpool['users'].find_one({'username': username, "password": password})
        print("USER", user)

        if user is not None:
            token = jwt.encode(
                {
                    'username': user['username'],
                    'password': user['password']
                }, 'mysecret', algorithm='HS256')
            status = {'username': user['username'], 'token': token.decode('utf-8')}
        else:
            status = {'error': 'Not found'}

        return status