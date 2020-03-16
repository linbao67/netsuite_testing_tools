import os

APP_PATH = os.path.dirname(__file__)


def get_project_folder():
    """
    :return:
    """
    project_path = os.path.abspath(os.path.join(APP_PATH, ".."))
    return project_path


def get_csv_path(folder_name='netsuite'):
    """
    generate the file name base for destination files
    :return:
    """
    source_file_path = os.path.abspath(os.path.join(APP_PATH, '../resources/{}/csv/'.format(folder_name)))

    return source_file_path


def get_model_path():
    """
    generate the file name base for destination files
    :return:
    """
    source_file_path = os.path.abspath(os.path.join(APP_PATH, '../resources/netsuite/model/'))

    return source_file_path


def get_xml_path():
    """
    generate the file name base for destination files
    :return:
    """
    source_file_path = os.path.abspath(os.path.join(APP_PATH, '../resources/netsuite/xml'))

    return source_file_path


def get_json_path():
    """
    generate the file name base for destination files
    :return:
    """
    source_file_path = os.path.abspath(os.path.join(APP_PATH, '../resources/netsuite/json'))

    return source_file_path


def get_pubsub_path():
    """
    return the file path for pubsub files
    :return:
    """
    pubsub_file_path = os.path.abspath(os.path.join(APP_PATH, '../resources/pubsub/csv'))

    return pubsub_file_path


if __name__ == '__main__':
    print(get_project_folder())
    print(get_xml_path())
