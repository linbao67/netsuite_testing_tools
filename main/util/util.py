import importlib
from form import IConstants
from form.util.string_util import count_char
from jpype import *

importlib.reload(sys)


def transfer_smartcomm_position(pageno, x0, y0, width, height, text):
    x0 = int(x0 + IConstants.PDF_TABLE_MOVE_RECT_X)
    y0 = int(y0 + IConstants.PDF_TABLE_MOVE_RECT_Y)
    width = int(width + IConstants.PDF_TABLE_RECT_ADD_WIDTH)
    height = int(height + IConstants.PDF_TABLE_RECT_ADD_HEIGHT)
    text = clean_text(text)
    return {'page': pageno, 'x0': x0, 'y0': y0, 'name': text,
            'width': width, 'height': height, 'value': 'TBD'}


def add_table_position_list(index, field, table_var_box_list_temp, table_var_positions_temp, is_dollar):
    field_width = field.x2 - field.x1
    field_x = field.x1 + 1
    if field_width > IConstants.CELL_MIN_WIDTH:
        if is_dollar:
            field_width = field_width - IConstants.DOLLAR_MIN_WIDTH
            field_x = field_x + IConstants.DOLLAR_MIN_WIDTH

        position = {"x1": field.x1, "x2": field.x2,
                    "y1": field.y1, "y2": field.y2}
        table_var_positions_temp.append(position)
        table_var_box_list_temp.append(
            transfer_smartcomm_position(index + 1, field_x,
                                        IConstants.PDF_HEIGHT - field.y2,
                                        field_width, field.y2 - field.y1, field.text))

    return table_var_box_list_temp, table_var_positions_temp


def transfer_label_to_variable_position(pageno, x1, y1, x2, y2, text, content_start, content_end, is_fixed_size,
                                        default_width=None, default_height=None, position='Right'):
    var_start = x2 + IConstants.PDF_MARGIN
    if (is_fixed_size):
        width = IConstants.FIXED_WIDTH
    else:
        width = content_end - var_start - IConstants.PDF_MARGIN
    if position == 'Right':
        x0 = int(var_start + IConstants.PDF_TEXT_MOVE_RECT_X)
        y0 = int(IConstants.PDF_HEIGHT - y2 + IConstants.PDF_TEXT_MOVE_RECT_Y)
    if position == 'Bottom':
        x0 = int(x1)
        y0 = int(IConstants.PDF_HEIGHT - y2 +
                 IConstants.PDF_TEXT_MOVE_RECT_Y + IConstants.PDF_MARGIN + (y2 - y1))
    if default_width is not None and default_width < width:
        width = default_width
    else:
        width = int(width + IConstants.PDF_TEXT_RECT_ADD_WIDTH)
    if default_height is not None:
        height = default_height
    else:
        height = int(y2 - y1 + IConstants.PDF_MARGIN +
                     IConstants.PDF_TEXT_RECT_ADD_HEIGHT)
    text = clean_text(text)
    return {'page': pageno + 1, 'x0': x0, 'y0': y0, 'name': text,
            'width': width, 'height': height, 'value': 'TBD'}


def transfer_dollar_label_to_variable_position(pageno, x1, y1, x2, y2, text):
    # start pos + "$" width + margin
    x0 = int(x1 + IConstants.PDF_MARGIN + IConstants.PDF_TEXT_MOVE_RECT_X)
    # pdf height -y2 (maybe need to use pdf_height- y1 - '$' height)
    y0 = int(IConstants.PDF_HEIGHT - y2 + IConstants.PDF_TEXT_MOVE_RECT_Y)
    width = int(IConstants.VAR_WIDTH + IConstants.PDF_TEXT_RECT_ADD_WIDTH)
    height = int(IConstants.VAR_HEIGHT + IConstants.PDF_TEXT_RECT_ADD_HEIGHT)
    text = clean_text(text)
    return {'page': pageno + 1, 'x0': x0, 'y0': y0, 'name': text,
            'width': width, 'height': height, 'value': 'TBD'}


def is_contain_dollor(x1, x2, y1, y2, dollor_positions):
    for position in dollor_positions:
        if (is_ranage(x1, y1, position) or
                (is_ranage(x2, y2, position))):
            return True
    return False


def is_ranage(x, y, position):
    return x >= position['x1'] and (x + IConstants.PDF_MARGIN) <= position['x2'] and y >= position[
        'y1'] and y + IConstants.PDF_MARGIN <= position['y2']


# when table variable have the label position, record the duplicate position.
def draw_before_and_removed(x1, x2, y1, y2, table_variables, remove_position_list):
    index = 0
    end_x = 0
    for position in table_variables:
        if (is_duplicate_part(x1, x2, y1, y2, position)):
            if index not in remove_position_list:
                remove_position_list.append(index)
            end_x = position['x2']
            break
        index = index + 1
    return remove_position_list, end_x


# delete the duplicate position item.
def merge_table_overlay_list(page_overlay_box_list, table_var_box_list, remove_position_list):
    remove_position_list.sort(reverse=True)
    for index in remove_position_list:
        table_var_box_list.remove(table_var_box_list[index])
    return page_overlay_box_list + table_var_box_list


def clean_text(text):
    text = text.replace(':', '')
    text = text.replace('\n', '')
    return text.strip()


# return all of $ cell position.
def check_multi_dollar(table):
    ROW = 0
    COL = 1
    COUNT = 2
    check_list = []
    all_dollar_draw_lists = []
    for row in range(0, len(table.cells)):
        line = table.cells[row]
        for col in range(0, len(line)):
            field = line[col]
            dollar_count = count_char(field.text, '$')
            if dollar_count >= 2:
                check_list.append([row, col, dollar_count])
    for cell_postion in check_list:
        start_row = cell_postion[ROW] + 1
        col = cell_postion[COL]
        count = cell_postion[COUNT]
        for row in range(start_row, start_row + count):
            if (start_row + count - 1) > (len(table.cells)) or row >= len(table.cells):
                break
            all_dollar_draw_lists.append([row, col])
            if table.cells[row][col].text != "":
                break
    return all_dollar_draw_lists


def is_draw_before(x1, x2, y1, y2, dollor_positions):
    for position in dollor_positions:
        if (is_duplicate_part(x1, x2, y1, y2, position)):
            return True
    return False


def is_duplicate_part(x1, x2, y1, y2, position):
    table_x1 = position['x1']
    table_x2 = position['x2']
    table_y1 = position['y1']
    table_y2 = position['y2']
    return abs(table_x2 + table_x1 - x2 - x1) + IConstants.DUP_DRAW_CHECK_DISTANCE < (x2 - x1 + table_x2 - table_x1) \
        and abs(table_y2 + table_y1 - y2 - y1) + IConstants.DUP_DRAW_CHECK_DISTANCE <= y2 - y1 + table_y2 - table_y1


if __name__ == '__main__':
    x1, y1, x2, y2 = 54, 558, 60, 90
    table_x1, table_y1, table_x2, table_y2 = 46, 80, 110, 300
    if (abs(x2 - x1) + IConstants.DUP_DRAW_CHECK_DISTANCE < (x2 - x1 + table_x2 - table_x1)
            and abs(y2 - y1) + IConstants.DUP_DRAW_CHECK_DISTANCE <= y2 - y1 + table_y2 - table_y1):
        print("contain")
    else:
        print("not contains")
