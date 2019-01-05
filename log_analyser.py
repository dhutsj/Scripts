import re
import sys

def Jenkins_log_analysis(path):
    file_list = []
    with open(path, 'r') as f:
        for line in f.readlines():
            if line.__contains__('Running command -k'):
                matchObj = re.match(r'(.*) Running command -k (.*).py', line, re.M | re.I)
                test_case_name = matchObj.group(2)
                file_list.append(test_case_name)
    return file_list

def Write_lines_to_file(testcases, path1, path2):
    count = 1
    with open(path1, 'r+') as f1:
         case_num = 0
         for line in f1.readlines():
             if line.__contains__('Running command -k') and case_num == 0:
                f = open("{}\\{}".format(path2, testcases[case_num]), 'w+')
                f.write(line)
                case_num = case_num + 1
             elif line.__contains__('Running command -k'):
                 f = open("{}\\{}".format(path2, testcases[case_num]), 'w+')
                 f.write(line)
                 case_num = case_num + 1
                 count = count + 1
             elif case_num == count:
                f.write(line)

path1 = sys.argv[1]
path2 = sys.argv[2]
filelist = Jenkins_log_analysis(path1)
Write_lines_to_file(filelist, path1, path2)

