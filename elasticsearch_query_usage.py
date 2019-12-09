from elasticsearch_query import ES_dsl
from elasticsearch import Elasticsearch
from get_known_issue import ExcelDealer

es_host = '10.84.108.114'
es_port = 9200
index_name = 'test_index'
doc_type = '_doc'
excel_path = "./known_issue_list.xlsx"
sheet_name = 'IO Modules Known MDT'


class Known_MDT_Triage(object):

    def __init__(self):
        self.es = Elasticsearch([{'host': es_host, 'port': es_port}])
        self.dsl = ES_dsl(self.es, index_name, doc_type)

    def get_new_mdt_list(self):
        pass

    def query_mdt_match_summary(self, query_keyword, size=1000, min_score=1):
        dsl = ES_dsl(self.es, index_name, doc_type)
        query_field = "Summary"
        results = dsl.match_query(query_field, query_keyword, size=size, min_score=min_score)
        return results

    def get_similar_summary_mdt_list(self, new_mdt_list):
        summary_most_like_mdt_list = {}
        size = 2
        for mdt in new_mdt_list:
            summary_most_like_mdt_list[mdt["Issue key"]] = []
            match = self.query_mdt_match_summary(mdt["Summary"], size)
            if match:
                for item in match:
                    summary_most_like_mdt_list[mdt["Issue key"]].append(item["Issue key"])
            else:
                print "No similar summary mdt found"
        print summary_most_like_mdt_list
        return summary_most_like_mdt_list

    def extract_root_cause_using_issue_key(self, similar_summary_mdt_list):
        dealer = ExcelDealer(excel_path)
        dealer.get_excel_book()
        known_root_cause_dict =  dealer.get_mdt_root_cause_content(sheet_name)
        possible_root_cause_dict = {}
        for key in similar_summary_mdt_list.keys():
            possible_root_cause_dict[key] = []
            for mdt in similar_summary_mdt_list[key]:
                possible_root_cause_dict[key].append(known_root_cause_dict[mdt])
        return possible_root_cause_dict

    def bool_query_get_most_similar(self, new_mdt_list, possible_root_cause):
        most_similar_mdt = {}
        logic = 'must-should'
        for mdt in new_mdt_list:
            mdt_key = mdt["Issue key"]
            summary = mdt["Summary"]
            dict1 = {'Summary': '{}'.format(summary)}
            most_similar_mdt[mdt_key] = []
            for root_cause in possible_root_cause[mdt_key]:
                dict2 = {'Description': '{}'.format(root_cause)}
                dict3 = {"Comment": '{}'.format(root_cause)}
                print "-----" * 40
                for hit in self.dsl.combined_query(logic, dict1, dict2, dict3):
                    print(hit["Issue key"], hit.meta.score)
                    most_similar_mdt[mdt_key].append((hit["Issue key"], hit.meta.score))
        return most_similar_mdt

    def multi_match_query_get_most_similar(self, new_mdt_list, possible_root_cause):
        most_similar_mdt = {}
        for mdt in new_mdt_list:
            mdt_key = mdt["Issue key"]
            summary = mdt["Summary"]
            field1 = 'Summary'
            field2 = 'Description'
            field3 = "Comment"
            most_similar_mdt[mdt_key] = []
            for root_cause in possible_root_cause[mdt_key]:
                keyword = '{} {}'.format(summary, root_cause)
                print "#####" * 40
                for hit in self.dsl.multi_match_query(keyword, [field1, field2, field3]):
                    print(hit["Issue key"], hit.meta.score)
                    most_similar_mdt[mdt_key].append((hit["Issue key"], hit.meta.score))
        return most_similar_mdt

    def remove_duplicated_result(self, most_similar_result):
        most_similar_result_dict = {}
        for key in most_similar_result.keys():
            mdt_dict = {}
            for item in most_similar_result[key]:
                if item[0] in mdt_dict:
                    pass
                else:
                    mdt_dict[item[0]] = item[1]

            mdt_list = sorted(mdt_dict.items(), key=lambda x: x[1], reverse=True)
            #print(mdt_list)
            most_similar_result_dict[key] = mdt_list
        #print most_similar_result_dict
        return most_similar_result_dict


def main():
    # step 1: get new mdt list
    new_mdt = Known_MDT_Triage()
    #new_mdt_list = new_mdt.get_new_mdt_list()
    new_mdt_list = [{"Issue key": "MDT-48997",
                     "Summary": "[WX-S6005][F8702][SAN]Incompatible ColdSpellX - could not write beacon to blink LED"},
                    {"Issue key": "MDT-50230",
                     "Summary": "CLONE - qla panic : NULL pointer dereference"}]
    # step 2: query mdt to match summary using new_mdt_summary
    summary_most_like_mdt_list = new_mdt.get_similar_summary_mdt_list(new_mdt_list)

    # step 3: read excel and extract root cause
    possible_root_cause = new_mdt.extract_root_cause_using_issue_key(summary_most_like_mdt_list)
    print possible_root_cause


    # step 4 option 1: query must summary and should description or comment
    similar_result = new_mdt.bool_query_get_most_similar(new_mdt_list, possible_root_cause)
    most_similar_result = {}
    for issue_key in similar_result.keys():
        similar_result[issue_key].sort(key=lambda x : x[1], reverse=True)
        most_similar_result[issue_key] = similar_result[issue_key]
    #print most_similar_result
    print new_mdt.remove_duplicated_result(most_similar_result)


    # step 4 option 2: multi_match query
    similar_result = new_mdt.multi_match_query_get_most_similar(new_mdt_list, possible_root_cause)
    most_similar_result = {}
    for issue_key in similar_result.keys():
        similar_result[issue_key].sort(key=lambda x: x[1], reverse=True)
        most_similar_result[issue_key] = similar_result[issue_key]
    #print most_similar_result
    print new_mdt.remove_duplicated_result(most_similar_result)


if __name__ == '__main__':
    main()
