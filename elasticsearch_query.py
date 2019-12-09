from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from elasticsearch_dsl import Q


class ES(object):
    def __init__(self, connection, index, doc_type):
        self.connection = connection
        self.index = index
        self.doc_type = doc_type

    def retrieve_document(self, mdt_id):
        res = self.connection.get(index=self.index, doc_type=self.doc_type, id=mdt_id)
        return res['_source']

    def match_all_query(self):
        res = self.connection.search(index=self.index, body={"size": 1000, "_source": "Summary", 'query': {'match_all': {}}})
        print 'There are {} MDTs in total'.format(len(res['hits']['hits']))
        mdt_list = []
        for item in res['hits']['hits']:
            mdt_list.append(item['_id'])
        return mdt_list

    def customed_query(self, body):
        """Customed match query.
                        Parameters
                        ----------
                        body : str, mandatory
                        A constructed JSON str you want to query
                        Returns
                        ------
                        res : list
                        A list of all the matched hits
                        """

        res = self.connection.search(index=self.index, body=body)
        return res['hits']['hits']


class ES_dsl(object):
    def __init__(self, connection, index, doc_type):
        self.connection = connection
        self.index = index
        self.doc_type = doc_type

    def query_setup(self):
        search = Search(index=self.index).using(self.connection)
        return search

    def match_query(self, field, keyword, **kwargs):
        """Basic match query.
                        Parameters
                        ----------
                        field : str, mandatory
                            field you want to query.
                        keyword : str, mandatory
                            keyword you want to query of the field
                        Returns
                        ------
                        q : Q object of the constructed query
                        """

        q = Q('match', **{'{}'.format(field): '{}'.format(keyword)})
        results = self.scan_result(q, **kwargs)
        return results

    def multi_match_query(self, keyword, fields, **kwargs):
        """Multi-field match query.
                Parameters
                ----------
                keyword : str, mandatory
                    keyword you want to query.
                fields : list, mandatory
                    list of fields you want to query.
                Returns
                ------
                q : Q object of the constructed query
                """

        q = Q('multi_match', query=keyword, fields=fields)
        return self.scan_result(q, **kwargs)

    def combined_query(self, logic=None, dict1=None, dict2=None, dict3=None, **kwargs):
        """Combined match query, and, or, not action of match query.
                        Parameters
                        ----------
                        logic : str, mandatory
                            'should': or
                            'must': and
                            'must_not': not
                        dict1: dict
                            key-value of what you want to query
                        dict2: dict
                            another key-value of what you want to query
                        Returns
                        ------
                        q : Q object of the constructed query
                        """

        if logic == 'should':
            q = Q("match", **dict1) | Q("match", **dict2)

        elif logic == 'must':
            q = Q("match", **dict1) & Q("match", **dict2)

        elif logic == 'must_not':
            q = ~Q("match", **dict1)

        elif logic == "must-should":
            q = Q("bool", must=[Q("match", **dict1)], should=[Q("match", **dict2), Q("match", **dict3)], minimum_should_match=1)

        else:
            q = "Error, Couldn't find the logic method"
            print q

        return self.scan_result(q, **kwargs)

    def combined_query_with_term(self, logic=None, dict1=None, dict2=None, term_dict=None):
        """Combined match query but also with filter, others are just like combined query.
                                Parameters
                                ----------
                                term_dict : dict, mandatory
                                    key-value you want to filter
                                Returns
                                ------
                                scan : list
                                A list of all the hit results of the constructed query
                                """

        s = self.query_setup()
        combined_query = self.combined_query(logic, dict1, dict2)
        first_s = s.query(combined_query)
        second_s = first_s.filter('term', **term_dict)
        for hit in second_s.scan():
            print(hit.Summary)
            print(hit["Issue key"])
        return second_s.scan()

    def scan_result(self, query, **kwargs):
        s = self.query_setup()
        s = s.query(query).extra(**kwargs)
        '''
        for hit in s.execute():
            print(hit.Summary)
            print(hit["Issue key"])
        '''
        return s.execute()


if __name__ == '__main__':
    host = '10.84.108.114'
    port = 9200
    es = Elasticsearch([{'host': host, 'port': port}])
    index_name = 'test_index'
    doc_type = '_doc'
    mdt_id = 'MDT-115858'
    used_es = ES(es, index_name, doc_type)
    res = used_es.retrieve_document(mdt_id)
    print res

    mdt_list = used_es.match_all_query()
    print mdt_list

    body = '{"size":1000,"_source":"Description","query":{"match":{"Description":"fail to set Coldspellx speed"}}}'
    print used_es.customed_query(body)

    body = '{"size":1000,"query":{"bool":{"must":{"match":{"Summary":"fail"}},"should":{"match":{"Description":"fail"}}}}}'
    print used_es.customed_query(body)

    body = '{"size":1000,"query":{"match_phrase":{"Summary":"QLA Init FW"}}}'
    print used_es.customed_query(body)

    dsl = ES_dsl(es, index_name, doc_type)
    query_field = "Summary"
    query_keyword = "Init Firmware failed"
    dsl.match_query(query_field, query_keyword)

    field1 = 'Summary'
    field2 = 'Description'
    field3 = "Comment"
    keyword = 'QLA Init aaaa FW bbbb Failed cccc ccccc hhhhh host two fc ports  show online, but linkdown condition always show in our jenkins job'
    dsl.multi_match_query(keyword, [field1, field2, field3])

    dict1 = {'Summary': 'QLA Init aaaa FW bbbb Failed cccc'}
    dict2 = {'Description': 'ccccc hhhhh'}
    dict3 = {"Comment": "host two fc ports  show online, but linkdown condition always show in our jenkins job"}
    logic = 'must-should'
    dsl.combined_query(logic, dict1, dict2, dict3)

    dict1 = {'Summary': 'QLA Init aaaa FW bbbb Failed cccc'}
    dict2 = {'Description': 'ccccc hhhhh'}
    logic = 'must'
    term_dict = {'Custom field (Major Area and Component)': 'Platform -> FC IO Modules'}
    dsl.combined_query(logic, dict1, dict2)

    print '*****' * 50
    logic = 'should'
    dsl.combined_query(logic, dict1, dict2)

    print '~~~~~' * 50
    logic = 'must_not'
    dsl.combined_query(logic, dict1)

    print '#####' * 50
    dsl.combined_query_with_term('must', dict1, dict2, term_dict)
