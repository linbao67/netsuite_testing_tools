import fnmatch
import json
import os
import shutil
import main.netsuite.endpoints as ep

from main.path import get_xml_path, get_json_path


def save_to_file(xml_data, endpoint, batch_id, page_number=0):
    file_path = get_xml_path() + '/' + endpoint + '_' + str(batch_id) + '_' + str(page_number) + '.xml'
    open(file_path, "w").write(xml_data)


def save_to_json(json_data, endpoint, batch_id, page_number=0):
    file_path = get_json_path() + '/' + endpoint + '_' + str(batch_id) + '_' + str(page_number) + '.json'
    open(file_path, "w").write(json.dumps(json_data))


def read_json_file(endpoint, batch_id, page_number=0):
    file_path = get_json_path() + '/' + endpoint + '_' + str(batch_id) + '_' + str(page_number) + '.json'
    return open(file_path, "r").read()


def read_xml_file(endpoint, batch_id, page_number):
    file_path = get_xml_path() + '/' + endpoint + '_' + str(batch_id) + '_' + str(page_number) + '.xml'
    return open(file_path, "r").read()

def merge_file(self, endpoint, batch_id):

    file_path = get_json_path()
    tmp_file_name = endpoint + ".tmp"
    target_file_name = endpoint + "_" + str(batch_id) + ".json"
    with open(os.path.join(file_path, tmp_file_name), "a") as destination_file:
        for _, _, file_name in os.walk(file_path):
            matched_file_name = endpoint + '_' + str(batch_id) + '_*.json'
            print(matched_file_name)
            file_name_list = fnmatch.filter(file_name, matched_file_name)
            if endpoint == ep.ITEM:
                file_name_list.append('predefined_items.json')
                file_name_list.append('expense_items.json')
                file_name_list.append('ship_items.json')
            for filename in file_name_list:
                with open(os.path.join(file_path, filename)) as src:
                    shutil.copyfileobj(src, destination_file)

    os.rename(os.path.join(file_path, tmp_file_name), os.path.join(file_path, target_file_name))


if __name__ == '__main__':
    xml = 'str(page_number)'
    save_to_json(xml, 'currency', 0, 1)
