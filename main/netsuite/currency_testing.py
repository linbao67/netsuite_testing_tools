import fnmatch
import json
import os

import pandas as pd

import main.netsuite.endpoints as ep
from main.path import get_csv_path, get_json_path
from main.util.config_util import get_property
from main.util.file_utils import read_json_file


class NetsuiteClient(object):

    def __init__(self):
        self.batch_id = get_property('parameters', 'BATCH_ID')

    def convert_currency_to_csv(self):
        json_string = read_json_file(ep.CURRENCY, self.batch_id)
        items = json.loads(json_string)
        result = []
        for item in items:
            internal_id = item.get('internalId', '')
            name = item.get('name', '')
            symbol = item.get('symbol', '')
            is_base_currency = item.get('isBaseCurrency', '')
            is_inactive = item.get('isInactive', '')

            result.append(dict(internalId=internal_id,
                               name=name,
                               symbol=symbol,
                               isBaseCurrency=is_base_currency,
                               isInactive=is_inactive,
                               ))

        df = pd.DataFrame(result, columns=["internalId", "name",
                                           "symbol",
                                           "isBaseCurrency",
                                           "isInactive"
                                           ])
        csv_path = get_csv_path() + '/' + ep.CURRENCY + '_' + str(self.batch_id) + '.csv'
        df.to_csv(csv_path)

    def convert_item_to_csv(self):
        # items = self.read_json_files(ep.ITEM, self.batch_id)
        items = self.read_json_files(ep.ITEM, 0)
        result = []
        for item in items:
            internal_id = item.get('internalId', '')
            sales_description = item.get('salesDescription')
            display_name = item.get('displayName')
            name = item.get('salesDescription', item.get('displayName', ''))
            is_sold = True
            is_purchased = False
            is_pretax = item.get('isPreTax', '')
            item_type = item.get('type')
            reference = item.get('itemId', '')
            created_at = item.get('createdDate')
            updated_at = item.get('lastModifiedDate')
            print(updated_at)

            result.append(dict(internalId=internal_id,
                               salesDescription=sales_description,
                               displayName=display_name,
                               name=name,
                               is_sold=is_sold,
                               is_purchased=is_purchased,
                               itemId=reference,
                               reference=reference,
                               item_type=item_type,
                               isPreTax=is_pretax,
                               createdDate=created_at,
                               lastModifiedDate=updated_at
                               ))

        df = pd.DataFrame(result)
        csv_path = get_csv_path() + '/' + ep.ITEM + '_' + str(self.batch_id) + '.csv'
        df.to_csv(csv_path)
        # df.to_csv(csv_path, columns=['internalId', 'salesDescription', 'displayName', 'createdDate', 'lastModifiedDate','name', 'isPretax','is_sold','is_purchased'])

    def convert_credit_memo_to_csv(self):

        df_currency = pd.read_csv(os.path.join(get_csv_path(), ep.CURRENCY + '_1.csv'))
        print(df_currency)
        df_item = pd.read_csv(os.path.join(get_csv_path(), ep.ITEM + '_0.csv'))
        json_string = read_json_file(ep.CREDIT_MEMO, self.batch_id)
        records = json.loads(json_string)
        credit_memo = []
        credit_memo_lines = []
        for record in records:
            credit_memo_id = record.get('internalId', '')
            credit_note_number = record.get('tranId', '')
            amount = record.get('total', '')
            date = record.get('tranDate', '')
            currency_internal_id = record['currency']['internalId']
            print('currency internal id {}'.format(currency_internal_id))
            if currency_internal_id is not None:
                print(df_currency[str(df_currency['internalId']) == currency_internal_id])
                currency = df_currency.loc[df_currency['internalId']==currency_internal_id][0]['symbol']


            currency_rate = record['exchangeRate']
            created_at = record.get('createdDate')
            updated_at = record.get('lastModifiedDate')

            credit_memo.append(dict(id=credit_memo_id,
                                    credit_memo_id=credit_note_number,
                                    amount=amount,
                                    date=date,
                                    currency_internal_id=currency_internal_id,
                                    currency_rate=currency_rate,
                                    created_at=created_at,
                                    updated_at=updated_at
                                    ))

            if 'itemList' in records:
                item_lines = record['itemList']['item']
                for item_line in item_lines:
                    if 'item' in item_line:
                        item_id = item_line['item'].get('internalId', '')
                        name = item_line['item'].get('name')
                        df_tmp = df_item.loc[df_item['internalId'] == item_id]
                        item_type = df_tmp[0]['item_type']
                        is_preTax = df_tmp[0]['isPreTax']
                    if item_id == '0' or item_type == 'listAcct:ItemGroup' or item_type == \
                            'listAcct:DescriptionItem' or item_type == 'listAcct:SubtotalItem':
                        break
                    description = name
                    quantity = item_line.get('quantity')
                    rate = item_line.get('rate')
                    amount = item_line.get('amount')
                    print('quantity {} rate {} amount {}:'.format(quantity, rate, amount))
                    if quantity is not None:
                        calc_quantity = quantity
                    elif rate is not None and rate[-1] != '%' and float(rate) != 0.0:
                        calc_quantity = amount / rate
                    else:
                        calc_quantity = 1

                    if item_type == 'listAcct:DiscountItem':
                        calc_unit_price = 0
                    elif rate is not None and rate[-1] != '%' and float(rate) != 0.0:
                        calc_unit_price = rate
                    elif quantity is not None:
                        calc_unit_price = amount / quantity
                    else:
                        calc_unit_price = amount

                    if item_type == 'listAcct:DiscountItem':
                        calc_discount = amount
                    else:
                        calc_discount = 0

                    if item_type == 'listAcct:DiscountItem':
                        calc_net_total = 0
                    else:
                        calc_net_total = amount

                    tax_code = item_line['taxCode']['name']
                    tax_rate_1 = item_line.get('taxRate1', 0)

                    if is_preTax is None or is_preTax == 'false':
                        calc_tax_amount = amount * tax_rate_1
                    else:
                        calc_tax_amount = amount * calc_discount * tax_rate_1

                    calc_total = calc_net_total + calc_tax_amount - calc_net_total
                    credit_memo_lines.append(dict(credit_memo_id=credit_memo_id,
                                                  credit_note_number=credit_note_number,
                                                  internalId=item_id,
                                                  name=name,
                                                  item_type=item_type,
                                                  isPreTax=is_preTax,
                                                  quantity=quantity,
                                                  rate=rate,
                                                  amount=amount,
                                                  taxCode=tax_code,
                                                  taxRate1=tax_rate_1,
                                                  description=description,
                                                  calc_quantity=calc_quantity,
                                                  calc_unit_price=calc_unit_price,
                                                  calc_discount=calc_discount,
                                                  calc_net_total=calc_net_total,
                                                  calc_tax_amount=calc_tax_amount,
                                                  calc_total=calc_total))

            if 'shipMethod' in record:
                pass

        df_creditMemo = pd.DataFrame(credit_memo)
        df_credit_memo_line = pd.DataFrame(credit_memo_lines)
        raw_csv_path = get_csv_path() + '/' + ep.CREDIT_MEMO + '_' + str(self.batch_id) + '_raw.csv'
        line_csv_path = os.path.join(get_csv_path(),'credit_memo_line'+ '_' + str(self.batch_id) + '_raw.csv')

        df_creditMemo.to_csv(raw_csv_path,
                             columns=['id', 'credit_note_number', 'amount', 'date', 'currency_internal_id',
                                      'currency_rate', 'created_at', 'updated_at'],
                             header=['internalId', 'tranId', 'total', 'tranDate',
                                     'currency/internalId',
                                     'exchangeRate', 'createdDate', 'lastModifiedDate'])

        df_credit_memo_line.to_csv(line_csv_path)

    def read_json_files(self, endpoint, batch_id):

        file_path = get_json_path()
        tmp_file_name = endpoint + ".tmp"
        target_file_name = endpoint + "_" + str(batch_id) + ".json"

        for _, _, file_name in os.walk(file_path):
            matched_file_name = endpoint + '_' + str(batch_id) + '_*.json'
            print(matched_file_name)
            file_name_list = fnmatch.filter(file_name, matched_file_name)
            if endpoint == ep.ITEM:
                file_name_list.append('predefined_items.json')
                file_name_list.append('expense_items.json')
                # file_name_list.append('ship_items.json')
            items = []
            for filename in file_name_list:
                with open(os.path.join(file_path, filename)) as src:
                    items.extend(json.loads(src.read()))
        return items


if __name__ == '__main__':
    testing = NetsuiteClient()
    testing.convert_credit_memo_to_csv()
