# -*- coding: utf-8 -*-

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
    def validate_auth(cpool, login, password):

        """
        Server side validate auth
        """

        return False