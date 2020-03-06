from form.util.config_util import *
import re
from form import IConstants

def is_none_str(str):
    return str is None or str.strip() == ''


def validate_string(str):
    if(is_none_str(str)):
        return ''
    else:
        return str


def clear_string(p_str):
    p_str = validate_string(p_str)
    return p_str.strip().rstrip()


def is_dollar_str(str):
    return str is not None and str.strip() == '$'


def is_start_dollar(str):
    return str is not None and str.strip().startswith('$')


def is_colon_lable_table(str):
    return str.strip().endswith(':')


def is_label(str):
    if(str is None):
        return False
    # check specially list
    if(is_end_with('Exclude_Label', str)):
        return False
    return str.strip().endswith(':')


def is_excluded_line(line):
    if line[0] is None:
        return False
    elif line[0].text is None:
        return False
    return is_contain('Exclude_Line', line[0].text)


def count_char(text, char):
    count = 0
    for c in text:
        if c == char:
            count += 1
    return count


def upper_string(str):
    if str is None:
        return ""
    else:
        str = str.strip().upper()
        str = str.replace(" ", "_")
        str = str.replace("-", "_")
        while "__" in str:
            str = str.replace("__", "_")
        return str


def str_contains_ignore_space(total_str, sub_str):
    total_str = total_str.replace('\n', ' ')
    while '  ' in total_str:
        total_str = total_str.replace('  ',' ')
    sub_str = sub_str.strip().replace('\n', ' ')
    while '  ' in sub_str:
        sub_str = sub_str.replace('  ',' ')
    return sub_str in total_str


def transfer_xpath(xpath):
    if xpath.startswith('/'):
        xpath = xpath[1:len(xpath)]

    return xpath.replace('/', '.')


def compare_value(new_value, old_value):
    if float(new_value)> float(old_value):
        return True
    return False

def get_page_number_by_filename(filename):
    pattern = r".*"+IConstants.PAGE_NUMBER_PREFIX+r"(\d*\d).xml"
    result = re.search(pattern,filename)
    if result is not None:
        return result.group(1)