import os
from configparser import ConfigParser

cfg = ConfigParser()
path = os.path.join(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "..")), 'config', 'property.ini')
cfg.read(path)
cfg.sections()

system_config = ConfigParser()
path = os.path.join(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "..")), 'config', 'system.ini')
system_config.read(path)
system_config.sections()


def get_property(config_type, property_name):
    return cfg.get(config_type, property_name)


def set_property(config_type, property_name, property_value):
    cfg.set(config_type, property_name, property_value)
    write_property()


def update_property_list(property_list):
    for prop_item in property_list:
        cfg.set(prop_item.get('config_type'), prop_item.get('name'), prop_item.get('value'))
    write_property()


def set_user_id(user_id):
    system_config.set('system', 'user_id', user_id)
    write_config()


def get_url():
    return system_config.get('system', 'aiform_url')


def write_config():
    path = os.path.join(os.path.abspath(os.path.join(
        os.path.dirname(__file__), "..")), 'config', 'system.ini')
    with open(path, 'w') as configfile:
        system_config.write(configfile)


def write_property():
    path = os.path.join(os.path.abspath(os.path.join(
        os.path.dirname(__file__), "..")), 'config', 'property.ini')
    with open(path, 'w') as configfile:
        cfg.write(configfile)


def get_user_name():
    return system_config.get('system', 'user_name')


def get_password():
    return system_config.get('system', 'password')


def get_realm():
    return system_config.get('system', 'realm')


def is_end_with(config_type, content):
    if config_type not in cfg:
        return False
    for key in cfg[config_type]:
        if content.strip().endswith(cfg[config_type][key]):
            return True
    return False


def is_contain(config_type, content):
    if config_type not in cfg:
        return False
    for key in cfg[config_type]:
        if cfg[config_type][key] in content:
            return True
    return False


if __name__ == '__main__':
    print(cfg.sections())
    print(cfg['Exclude_Label']['Exclude_Follow'])
    is_contain('Exclude_Label', 'following:')
