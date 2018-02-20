# this file is to convert from spark output to input for javascript
# data analysis and visualization

import glob
import json

# if placing final data folder in this folder
data_location = '2015'

def clean_adj(adj_list):

    new_adj_list = []
    for adj_obj in adj_list:
        adj = adj_obj['adj']
        count = adj_obj['count']

        if count > 1 and adj != '<' and adj != '>':
            new_obj = {
                'adjective': adj,
                'count': count
            }
            new_adj_list.append(new_obj)

    return new_adj_list[0:10]

def sort_by_count(obj):
    return -obj['data']['count']

def word_count():

    file_path = data_location + '/word_count/orgs/part-*'

    file_paths = glob.glob(file_path)
    output_data = []
    id_num = 0

    for file_path in file_paths:
        print file_path
        with open(file_path, 'r') as data_file:
            for line in data_file:
                json_data = json.loads(line)

                noun = json_data['noun']
                subreddit = json_data['subreddit']
                num_nouns = json_data['numNoun']
                direct_adj = json_data['directAdj']
                connected_adj = json_data['connectedAdj']

                clean_direct_adj = clean_adj(direct_adj)
                clean_connected_adj = clean_adj(connected_adj)

                if num_nouns > 50 and (len(clean_direct_adj) > 0 or len(clean_connected_adj) > 0):
                    obj = {
                        'data': {
                            'noun': noun,
                            'subreddit': subreddit,
                            'count': num_nouns,
                            'directAdjectives': clean_direct_adj,
                            'connectedAdjectives': clean_connected_adj
                        },
                        'id': id_num
                    }
                    id_num += 1
                    output_data.append(obj)

    sorted_output_data = sorted(output_data, key=sort_by_count)

    final_output = {
        'items': sorted_output_data,
        'headers': [
            {
                'input': True,
                'display': 'Organization',
                'key': 'noun',
                'size': '8em'
            },
            {
                'input': True,
                'display': 'Subreddit',
                'key': 'subreddit',
                'size': '8em'
            },
            {
                'sort': True,
                'display': 'Count',
                'key': 'count',
                'size': '5em'
            },
            {
                'input': True,
                'display': 'Direct Adjectives',
                'key': 'directAdjectives',
                'size': '13em',
                'details': [
                    {
                        'input': True,
                        'display': 'Adjective',
                        'key': 'adjective',
                        'size': '8em'
                    },
                    {
                        'display': 'Count',
                        'key': 'count',
                        'size': '5em'
                    }
                ]
            },
            {
                'input': True,
                'display': 'Connected Adjectives',
                'key': 'connectedAdjectives',
                'size': '13em',
                'details': [
                    {
                        'input': True,
                        'display': 'Adjective',
                        'key': 'adjective',
                        'size': '8em'
                    },
                    {
                        'display': 'Count',
                        'key': 'count',
                        'size': '5em'
                    }
                ]
            }
        ]
    }

    with open('data-word-count.json', 'w') as output_file:
        output_json = json.dumps(final_output)
        output_file.write(output_json)

def pretty_print(input_dict):
    return json.dumps(input_dict, sort_keys=True, indent=4, separators=(',', ': '))

def word_to_vec():

    file_path_orgs = data_location + '/word_to_vec/orgSynonyms/*/part-*'
    file_path_all = data_location + '/word_to_vec/all/part-*'
    file_paths = glob.glob(file_path_orgs) + glob.glob(file_path_all)

    output_data = []
    id_num = 0

    for file_path in file_paths:
        print file_path
        with open(file_path, 'r') as data_file:
            for line in data_file:
                json_data = json.loads(line)

                org = json_data['org']
                subreddit = json_data['subreddit']
                count = json_data['count']
                synonymObjs = json_data['synonyms']

                if len(synonymObjs) > 0:

                    synonyms = []
                    for synonymObj in synonymObjs:
                        synonyms.append({
                            'word': synonymObj['word'],
                            'similarity': round(synonymObj['similarity'], 4)
                        })

                    obj = {
                        'data': {
                            'org': org,
                            'subreddit': subreddit,
                            'count': count,
                            'synonyms': synonyms
                        },
                        'id': id_num
                    }
                    id_num += 1
                    output_data.append(obj)

    sorted_output_data = sorted(output_data, key=sort_by_count)

    final_output = {
        'items': sorted_output_data,
        'headers': [
            {
                'input': True,
                'display': 'Organization',
                'key': 'org',
                'size': '10em'
            },
            {
                'input': True,
                'display': 'Subreddit',
                'key': 'subreddit',
                'size': '10em'
            },
            {
                'sort': True,
                'display': 'Count',
                'key': 'count',
                'size': '5em'
            },
            {
                'input': True,
                'display': 'Synonyms',
                'key': 'synonyms',
                'size': '20em',
                'details': [
                    {
                        'input': True,
                        'display': 'Word',
                        'key': 'word',
                        'size': '10em'
                    },
                    {
                        'display': 'Similarity',
                        'key': 'similarity',
                        'size': '10em'
                    }
                ]
            }
        ]
    }

    with open('data-word-to-vec.json', 'w') as output_file:
        output_json = json.dumps(final_output)
        output_file.write(output_json)

if __name__ == '__main__':

    word_count()
    word_to_vec()
