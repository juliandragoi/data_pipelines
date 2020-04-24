import os
import glob


'''
path needs to start from /Users/[username]
'''

home_path = '/Users/juliandragoi/Projects/DataMining/'

def change_path(path):
    os.chdir(path)


def get_file_with_json_ext():
    files = []
    for file in glob.glob("*.json"):
        files.append(file)

    for file in files:
        print(file)


change_path(home_path)

print(get_file_with_json_ext())





