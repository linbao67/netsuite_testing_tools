import fnmatch
import json
import os
from datetime import datetime

import pandas as pd

import main.netsuite.endpoints as ep
from main.path import get_csv_path, get_json_path
from main.util.config_util import get_property
from main.util.datetime_util import getUTCDateTime, add_days
from main.util.file_utils import read_json_file

DATE_FORMAT = '%Y-%m-%dT%H:%M:%S'  # 2019-09-01T00:00:00.000-07:00
DATE_TZONE_FORMAT = '%Y-%m-%dT%H:%M:%S%z'
DUE_DATE_FORMAT = '%Y-%m-%d'


class NetsuiteClient(object):

    def __init__(self):
        self.batch_id = get_property('parameters', 'BATCH_ID')

    def __cal_subtotal__(self, ):
        pass

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

    def convert_customer_to_csv(self):

        records = self.read_json_files(ep.CUSTOMER, self.batch_id)
        result = []
        for item in records:
            internal_id = item.get('internalId', '')
            name = item.get('entityId')
            created_at = item.get('createdDate')
            created_at = getUTCDateTime(created_at) if created_at else None
            updated_at = item.get('lastModifiedDate')
            updated_at = getUTCDateTime(updated_at) if updated_at else None
            print(updated_at)

            result.append(dict(internalId=internal_id,
                               name=name,
                               created_at=created_at,
                               updated_at=updated_at
                               ))

        df = pd.DataFrame(result)
        csv_path = get_csv_path() + '/' + ep.CUSTOMER + '_' + str(self.batch_id) + '.csv'
        df.to_csv(csv_path)
        customer_ref_path = os.path.join(get_csv_path(), 'customer_ref_' + str(self.batch_id) + '.csv')
        df.to_csv(customer_ref_path, columns=['internalId', 'name', 'updated_at'])

    def convert_item_to_csv(self):
        items = self.read_json_files(ep.ITEM, self.batch_id)
        # items = self.read_json_files(ep.ITEM, 0)
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
            if item_type is not None:
                item_type = item_type[item_type.index(':') + 1:]
            reference = item.get('itemId', '')
            created_at = item.get('createdDate')
            created_at = getUTCDateTime(created_at) if created_at else None
            updated_at = item.get('lastModifiedDate')
            updated_at = getUTCDateTime(updated_at) if updated_at else None
            print(updated_at)

            result.append(dict(internalId=internal_id,
                               salesDescription=sales_description,
                               displayName=display_name,
                               name=name,
                               is_sold=is_sold,
                               is_purchased=is_purchased,
                               itemId=reference,
                               reference=reference,
                               itemType=item_type,
                               isPreTax=is_pretax,
                               createdDate=created_at,
                               lastModifiedDate=updated_at
                               ))

        df = pd.DataFrame(result)
        csv_path = get_csv_path() + '/' + ep.ITEM + '_' + str(self.batch_id) + '.csv'
        df.to_csv(csv_path)
        item_ref_path = os.path.join(get_csv_path(), 'item_ref_' + str(self.batch_id) + '.csv')
        df.to_csv(item_ref_path, columns=['internalId', 'isPreTax', 'reference', 'itemType'])

    def convert_vendor_bill_to_csv(self):
        records = self.read_json_files(ep.VENDOR_BILL, self.batch_id)
        # items = self.read_json_files(ep.ITEM, 0)
        result = []
        for record in records:
            if 'itemList' in record:
                internal_id = record.get('internalId')
                for item in record['itemList']['item']:
                    item_internal_id = item['item'][0]['internalId']
                    line = item['line']
                    result.append(dict(internalId=internal_id,
                                       item_internalId=item_internal_id,
                                       line=line))
        df = pd.DataFrame(result)
        csv_path = get_csv_path() + '/' + ep.VENDOR_BILL + '_' + str(self.batch_id) + '.csv'
        df.to_csv(csv_path)

    def convert_time_bill_to_csv(self):
        records = self.read_json_files(ep.TIME_BILL, self.batch_id)
        result = []
        for record in records:
            internal_id = record.get('internalId')
            item_internal_id = record['item'][0]['internalId'] if 'item' in record else None
            item_name = record['item'][0]['name'] if 'item' in record else None
            result.append(dict(internalId=internal_id,
                               item_internalId=item_internal_id,
                               item_name=item_name))
        df = pd.DataFrame(result)
        csv_path = get_csv_path() + '/' + ep.TIME_BILL + '_' + str(self.batch_id) + '.csv'
        df.to_csv(csv_path)

    def convert_credit_memo_to_csv(self):

        df_currency = pd.read_csv(os.path.join(get_csv_path(), ep.CURRENCY + '_' + str(self.batch_id) + '.csv'))
        df_item = pd.read_csv(os.path.join(get_csv_path(), 'item_ref_' + str(self.batch_id) + '.csv'))
        records = self.read_json_files(ep.CREDIT_MEMO, self.batch_id)
        credit_memo = []
        credit_memo_lines = []
        for record in records:
            credit_memo_id = record.get('internalId', '')
            credit_note_number = record.get('tranId', '')
            print('credit memo internal id:{} credit memo number: {}'.format(credit_memo_id, credit_note_number))
            amount = record.get('total', '')
            date = record.get('tranDate', '')
            date = getUTCDateTime(date, DUE_DATE_FORMAT) if date else None
            currency_internal_id = record['currency']['internalId']
            if currency_internal_id is not None:
                currency = df_currency[df_currency['internalId'] == int(currency_internal_id)]['symbol'].values[0]

            currency_rate = record['exchangeRate']
            created_at = record.get('createdDate')
            created_at = getUTCDateTime(created_at) if created_at else None
            updated_at = record.get('lastModifiedDate')
            updated_at = getUTCDateTime(updated_at) if updated_at else None

            invoice_tax_rate = float(record.get('taxRate', 0))

            credit_memo.append(dict(id=credit_memo_id,
                                    credit_note_number=credit_note_number,
                                    amount=amount,
                                    date=date,
                                    currency_internal_id=currency_internal_id,
                                    currency=currency,
                                    currency_rate=currency_rate,
                                    created_at=created_at,
                                    updated_at=updated_at,
                                    ))

            discount_rate = record.get('discountRate')
            is_pre_tax = None
            if 'discountTotal' in record:
                discount_item = record['discountItem']
                discountItem_internalId = discount_item['internalId']
                discountItem_name = discount_item['name']
                description = 'transaction discount'
                discountTotal = float(record.get('discountTotal'))
                is_pre_tax = df_item[df_item['internalId'] == int(discountItem_internalId)]['isPreTax'].values[0]
                tgt_quantity = 1
                tgt_unit_price = 0
                tgt_discount = -1 * discountTotal
                tgt_net_total = 0
                tgt_tax_amount = 0
                tgt_total = tgt_net_total + tgt_tax_amount - tgt_discount

                credit_memo_lines.append(dict(credit_memo_id=credit_memo_id,
                                              credit_note_number=credit_note_number,
                                              src_internalId=None,
                                              src_discountItem_internalId=discountItem_internalId,
                                              src_shipMethod_internalId=None,
                                              tgt_internalId=discountItem_internalId,
                                              src_name=None,
                                              src_discountItem_name=discountItem_name,
                                              src_shipMethod_name=None,
                                              tgt_description=description,
                                              src_isPreTax=is_pre_tax,
                                              cal_item_type='DiscountItem',
                                              src_quantity=None,
                                              src_amount=None,

                                              src_rate=None,
                                              tgt_quantity=tgt_quantity,
                                              tgt_discount=tgt_discount,
                                              src_discountTotal=discountTotal,
                                              src_shippingCost=None,
                                              src_handlingCost=None,
                                              tgt_total=tgt_total,
                                              tgt_unit_price=tgt_unit_price,
                                              tgt_net_total=tgt_net_total,
                                              src_taxCode=None,
                                              src_shippingTaxCode=None,
                                              src_handlingTaxCode=None,
                                              tgt_tax_code=None,  # TODO
                                              src_taxRate1=None,
                                              src_discountRate=discount_rate,
                                              src_shippingTax1Rate=None,
                                              src_handlingTax1Rate=None,
                                              cal_sub_total=None,
                                              cal_discount_rate=None,
                                              tgt_tax_amount=tgt_tax_amount,
                                              updated_at=updated_at
                                              ))

            item_lines = record['itemList']['item']
            cal_sub_total = 0.0

            item_list_discount_apply_to = {}
            item_list_markup_apply_to = {}

            previous_subtotal_item_out_grp = 0
            previous_subtotal_item_in_grp = -1

            flag_inside_itemgroup = False
            flag_discountable_inc_markup = False
            flag_discountable_exc_markup = False

            flag_discountable_in_grp_inc_markup = False
            flag_discountable_in_grp_exc_markup = False

            count_item_list = 0

            previous_item_type = None
            last_discountable_item = None
            last_markupable_item = None
            item_list_published_discounts = []
            item_list_unknown_markup = []
            item_list_discount = []
            item_list_markup = []
            df_raw_invoice_line = pd.DataFrame.from_dict(item_lines)
            print(df_raw_invoice_line['item'])
            item_name_list = {}
            for line in item_lines:

                count_item_list = count_item_list + 1

                line_amount = float(line.get('amount')) if 'amount' in line else None

                if 'item' in line:
                    item_id = line['item'][0].get('internalId')
                    name = line['item'][0].get('name')

                    if item_id != '0':
                        item_type = df_item[df_item['internalId'] == int(item_id)]['itemType'].values[0]

                        print("items line no: {} item type {} amount {}".format(count_item_list, item_type,
                                                                                line_amount))

                        if item_type not in ['ItemGroup', 'DescriptionItem', 'SubtotalItem']:
                            item_name_list[count_item_list] = name
                            cal_sub_total = cal_sub_total + float(line.get('amount'))

                        if item_type == 'DiscountItem':

                            if 'tax1amt' in line:
                                tax1amt = float(line['tax1amt'])
                                if tax1amt == 0.0:
                                    tmp_tax_rate = 0
                                else:
                                    tmp_tax_rate = tax1amt / line_amount * 100
                            elif 'tax_rate_1' in line:
                                tax_rate_1 = line.get('tax_rate_1')
                                tmp_tax_rate = tax_rate_1
                            else:
                                tmp_tax_rate = 0

                            if last_discountable_item:
                                if 'rate' in line and line['rate'][-1] == '%':
                                    tmp_discount_rate = float(line['rate'][:-1]) * 0.01
                                elif line_amount != 0.0:
                                    tmp_discount_rate = line_amount / last_discountable_item['amount']

                                else:
                                    tmp_discount_rate = 0
                                discount_item = last_discountable_item
                                discount_item['discount_rate'] = tmp_discount_rate
                                discount_item['tax_rate'] = tmp_tax_rate
                                # only in this case, discount may apply to other discounts,
                                item_list_discount.append(count_item_list)
                                item_list_discount_apply_to[count_item_list] = discount_item

                            else:
                                item_list_published_discounts.append(count_item_list)


                        elif item_type == 'SubtotalItem':
                            end_subtotal_item = count_item_list - 1
                            if previous_item_type == 'SubtotalItem' or previous_item_type == 'ItemGroup':
                                last_discountable_item = None
                            elif end_subtotal_item != 0:
                                # if there is only description item, empty item group
                                if flag_inside_itemgroup:
                                    if flag_discountable_in_grp_inc_markup:
                                        pos_subtotal_items = {'start': previous_subtotal_item_in_grp + 1,
                                                              'end': end_subtotal_item,
                                                              'amount': line_amount}
                                        last_discountable_item = pos_subtotal_items
                                    else:
                                        last_discountable_item = None
                                    previous_subtotal_item_in_grp = count_item_list
                                else:
                                    if flag_discountable_inc_markup:
                                        pos_subtotal_items = {'start': previous_subtotal_item_out_grp + 1,
                                                              'end': end_subtotal_item,
                                                              'amount': line_amount}
                                        last_discountable_item = pos_subtotal_items
                                    else:
                                        last_discountable_item = None
                                    previous_subtotal_item_out_grp = count_item_list
                            if line_amount == 0.0:
                                print("subtotal with 0 :")
                                last_discountable_item = None

                            last_markupable_item = last_discountable_item

                            if flag_inside_itemgroup:
                                flag_discountable_in_grp_exc_markup = True
                                flag_discountable_in_grp_inc_markup = True
                            else:
                                flag_discountable_inc_markup = True
                                flag_discountable_exc_markup = True
                        elif item_type == 'ItemGroup':
                            flag_inside_itemgroup = True
                            previous_subtotal_item_in_grp = count_item_list
                            flag_discountable_inc_markup = True
                            flag_discountable_exc_markup = True
                            flag_discountable_in_grp_inc_markup = False
                            flag_discountable_in_grp_exc_markup = False
                            start_group_item = count_item_list + 1
                            last_discountable_item = None
                            last_markupable_item = last_discountable_item
                        elif item_type == 'MarkupItem':
                            if flag_inside_itemgroup:
                                flag_discountable_in_grp_inc_markup = True  # discountable item already set to True
                                # due to item group
                            else:
                                flag_discountable_inc_markup = True
                            if not flag_discountable_exc_markup or not flag_discountable_in_grp_exc_markup:
                                if line_amount != 0.0:
                                    last_discountable_item = {'start': count_item_list, 'end': count_item_list,
                                                              'amount': line_amount}
                                else:
                                    last_discountable_item = None

                            if last_markupable_item:
                                item_list_markup_apply_to[count_item_list] = last_markupable_item

                                item_list_markup.append(count_item_list)
                            else:
                                item_list_unknown_markup.append(count_item_list)
                                item_name_list[count_item_list] = 'Markup on-unknown item'

                        elif item_type == 'DescriptionItem':
                            # if not flag_discountable_inc_markup:
                            #     start_subtotal_item = start_subtotal_item + 1
                            # if flag_inside_itemgroup and (not flag_discountable_in_grp_inc_markup):
                            #     start_group_item = start_group_item + 1
                            # don't need to calculate the exact start item, those items will not be published or
                            # discountable item ( put off the calculation to next loop )
                            pass

                        else:
                            if flag_inside_itemgroup:
                                # already inside item group, no need to set the flag of discoutable item
                                flag_discountable_in_grp_exc_markup = True
                                flag_discountable_in_grp_inc_markup = True
                            else:
                                flag_discountable_inc_markup = True
                                flag_discountable_exc_markup = True

                            last_discountable_item = {'start': count_item_list, 'end': count_item_list,
                                                      'amount': line_amount}
                        previous_item_type = item_type
                        last_markupable_item = last_discountable_item
                    else:
                        end_group_item = count_item_list - 1
                        if end_group_item < start_group_item:
                            # group has no items
                            pos_group_items = None
                        else:
                            pos_group_items = {'start': start_group_item, 'end': end_group_item,
                                               'amount': line_amount}
                        previous_item_type = 'ItemGroupEnd'
                        last_discountable_item = pos_group_items
                        last_markupable_item = last_discountable_item
                        flag_inside_itemgroup = False
                else:
                    item_name_list[count_item_list] = ''
                    # in case there is no item name for the line
                    cal_sub_total = cal_sub_total + float(line.get('amount'))

                    last_discountable_item = {'start': count_item_list, 'end': count_item_list, 'amount':
                        line_amount}
                    previous_item_type = 'InventoryItem'
                    last_markupable_item = last_discountable_item

                    if flag_inside_itemgroup:
                        # already inside item group, no need to set the flag of discoutable item
                        flag_discountable_in_grp_exc_markup = True
                        flag_discountable_in_grp_inc_markup = True
                    else:
                        flag_discountable_inc_markup = True
                        flag_discountable_exc_markup = True

                if discount_rate is not None and discount_rate[-1] == '%':
                    cal_discount_rate = 1 - -1 * float(discount_rate[:-1]) * 0.01
                elif discount_rate is not None and cal_sub_total != 0.0:
                    cal_discount_rate = 1 - -1 * discountTotal / cal_sub_total
                else:
                    cal_discount_rate = 1

                print("credit_note_number {} published discount {}".format(credit_note_number, str(item_list_published_discounts)))
                item_list_discount_rate = {}
                item_list_tax_rate = {}
                print("credit_note_number {} dicount apply to {}".format(credit_note_number, str(item_list_discount_apply_to)))

                item_list_discount.reverse()
                item_list_discount_to_discount = {}


                print(item_list_discount)
                for discount in item_list_discount:
                    tmp_discount_rate = item_list_discount_apply_to[discount][
                                        'discount_rate'] * item_list_discount_to_discount.get(discount, 1)
                    start = item_list_discount_apply_to[discount]['start']
                    end = item_list_discount_apply_to[discount]['end'] + 1
                    for i in range(start, end):
                        if i in item_list_discount:
                            item_list_discount_to_discount[i] = item_list_discount_to_discount.get(i, 1) + \
                                                                tmp_discount_rate
                print(item_list_discount_to_discount)

                for line_no, discount in item_list_discount_apply_to.items():

                    cal_line_discount_rate = discount['discount_rate'] * item_list_discount_to_discount.get(line_no, 1)
                    cal_line_tax_rate = discount['tax_rate']  # already times 0.01
                    start = discount['start']
                    end = discount['end'] + 1

                    for i in range(start, end):
                        if i in item_list_discount_rate:
                            item_list_discount_rate[i] = item_list_discount_rate[i] + cal_line_discount_rate
                            item_list_tax_rate[i] = item_list_tax_rate[i] + cal_line_tax_rate * cal_line_discount_rate
                        else:
                            item_list_discount_rate[i] = cal_line_discount_rate
                            item_list_tax_rate[i] = cal_line_tax_rate
                print("credit_note_number {} discount rate {}".format(credit_note_number, str(item_list_discount_rate)))
                print("credit_note_number {} tax rate {}".format(credit_note_number, str(item_list_tax_rate)))

            count_item_list = 0
            for item_line in item_lines:
                count_item_list += 1
                if 'item' in item_line:
                    item_id = item_line['item'][0].get('internalId')
                    name = item_line['item'][0].get('name')
                    print(df_item[df_item['internalId'] == int(item_id)])
                    if item_id != '0':
                        df_tmp = df_item[df_item['internalId'] == int(item_id)]
                        item_type = df_tmp['itemType'].values[0]
                        item_name = df_tmp['reference'].values[0]

                        if item_type not in ['ItemGroup', 'DescriptionItem', 'SubtotalItem', 'DiscountItem'] or \
                                item_type == 'DiscountItem' and count_item_list in item_list_published_discounts:
                            description = name
                            quantity = float(item_line['quantity']) if 'quantity' in item_line else None
                            rate = item_line.get('rate')
                            amount = float(item_line.get('amount'))
                            print('quantity {} rate {} amount {}:'.format(quantity, rate, amount))
                            if quantity is not None:
                                tgt_quantity = quantity
                            elif rate is not None and rate[-1] != '%' and float(rate) != 0.0:
                                tgt_quantity = amount / float(rate)
                            else:
                                tgt_quantity = 1

                            if item_type == 'DiscountItem':
                                tgt_unit_price = 0
                            elif rate is not None and rate[-1] != '%' and float(rate) != 0.0:
                                tgt_unit_price = rate
                            elif quantity is not None:
                                tgt_unit_price = amount / quantity
                            else:
                                tgt_unit_price = amount

                            if item_type == 'DiscountItem':
                                tgt_discount = -1 * amount * item_list_discount_to_discount.get(count_item_list,1)
                            else:
                                tgt_discount = -1 * amount * item_list_discount_rate.get(count_item_list, 0)

                            if item_type == 'DiscountItem':
                                tgt_net_total = 0
                            else:
                                tgt_net_total = amount

                            if 'taxCode' in item_line:
                                tax_code = item_line['taxCode'].get('name')
                            else:
                                tax_code = None
                                print('Credit Memo {} Item {} doesnt have tax_code'.format(credit_note_number,
                                                                                           item_id))
                            if 'isTaxable' in item_line:
                                if item_line['isTaxable'].upper() == 'TRUE':
                                    tax_rate_1 = invoice_tax_rate
                                else:
                                    tax_rate_1 = 0
                            elif 'tax1Amt' in item_line:
                                tax1_amt = float(item_line['tax1Amt'])
                                tax_rate_1 = tax1_amt / amount * 100 if tax1_amt != 0.0 and amount != 0.0 else 0

                            else:
                                tax_rate_1 = float(item_line.get('taxRate1', 0))

                            if item_type == 'DiscountItem':
                                tax_rate_1 = tax_rate_1 * item_list_discount_to_discount.get(count_item_list, 1)
                            else:
                                tax_rate_1 = tax_rate_1 + item_list_tax_rate.get(count_item_list, 0)



                            if str(is_pre_tax).upper() == 'TRUE':
                                print('isPreTax {} is True'.format(is_pre_tax))
                                tgt_tax_amount = amount * cal_discount_rate * tax_rate_1 * 0.01
                            else:
                                tgt_tax_amount = amount * tax_rate_1 * 0.01

                            tgt_total = tgt_net_total + tgt_tax_amount - tgt_discount
                            credit_memo_lines.append(dict(credit_memo_id=credit_memo_id,
                                                          credit_note_number=credit_note_number,
                                                          src_internalId=item_id,
                                                          src_discountItem_internalId=None,
                                                          src_shipMethod_internalId=None,
                                                          tgt_internalId=item_id,
                                                          src_name=name,
                                                          src_discountItem_name=None,
                                                          src_shipMethod_name=None,
                                                          tgt_description=description,
                                                          src_isPreTax=is_pre_tax,
                                                          cal_item_type=item_type,
                                                          src_quantity=quantity,
                                                          src_amount=amount,
                                                          src_rate=rate,
                                                          tgt_quantity=tgt_quantity,
                                                          tgt_discount=tgt_discount,
                                                          src_discountTotal=None,
                                                          src_shippingCost=None,
                                                          src_handlingCost=None,
                                                          tgt_total=tgt_total,
                                                          tgt_unit_price=tgt_unit_price,
                                                          tgt_net_total=tgt_net_total,
                                                          src_taxCode=tax_code,
                                                          src_shippingTaxCode=None,
                                                          src_handlingTaxCode=None,
                                                          tgt_tax_code=tax_code,
                                                          src_taxRate1=tax_rate_1,
                                                          src_shippingTax1Rate=None,
                                                          src_handlingTax1Rate=None,
                                                          src_discountRate=discount_rate,
                                                          cal_sub_total=cal_sub_total,
                                                          cal_discount_rate=cal_discount_rate,
                                                          tgt_tax_amount=tgt_tax_amount,
                                                          updated_at=updated_at))
                else:

                    quantity = float(item_line['quantity']) if 'quantity' in item_line else None
                    rate = item_line.get('rate')
                    amount = float(item_line.get('amount'))
                    print('quantity {} rate {} amount {}:'.format(quantity, rate, amount))
                    if quantity is not None:
                        tgt_quantity = quantity
                    elif rate is not None and rate[-1] != '%' and float(rate) != 0.0:
                        tgt_quantity = amount / float(rate)
                    else:
                        tgt_quantity = 1

                    if rate is not None and rate[-1] != '%' and float(rate) != 0.0:
                        tgt_unit_price = rate
                    elif quantity is not None:
                        tgt_unit_price = amount / quantity
                    else:
                        tgt_unit_price = amount

                    tgt_discount = 0

                    tgt_net_total = amount

                    if 'taxCode' in item_line:
                        tax_code = item_line['taxCode'].get('name')
                    else:
                        tax_code = None
                        print('Credit Memo {} Item {} doesnt have tax_code'.format(credit_note_number,
                                                                                   item_id))
                    tax_rate_1 = float(item_line.get('taxRate1', 0))

                    # if discount_rate is not None and discount_rate[-1] == '%':
                    #     cal_discount_rate = 1 - -1 * float(discount_rate[:-1]) * 0.01
                    # elif discount_rate is not None and cal_sub_total != 0.0:
                    #     cal_discount_rate = 1 - -1 * discountTotal / cal_sub_total
                    # else:
                    #     cal_discount_rate = 1

                    if str(is_pre_tax).upper() == 'TRUE':
                        print('isPreTax {} is True'.format(is_pre_tax))
                        tgt_tax_amount = amount * cal_discount_rate * tax_rate_1 * 0.01
                    else:
                        tgt_tax_amount = amount * tax_rate_1 * 0.01

                    tgt_total = tgt_net_total + tgt_tax_amount - tgt_discount
                    credit_memo_lines.append(dict(credit_memo_id=credit_memo_id,
                                                  credit_note_number=credit_note_number,
                                                  src_internalId=None,
                                                  src_discountItem_internalId=None,
                                                  src_shipMethod_internalId=None,
                                                  tgt_internalId=None,
                                                  src_name=None,
                                                  src_discountItem_name=None,
                                                  src_shipMethod_name=None,
                                                  tgt_description=None,
                                                  src_isPreTax=is_pre_tax,
                                                  cal_item_type=None,
                                                  src_quantity=quantity,
                                                  src_amount=amount,
                                                  src_rate=rate,
                                                  tgt_quantity=tgt_quantity,
                                                  tgt_discount=tgt_discount,
                                                  src_discountTotal=None,
                                                  src_shippingCost=None,
                                                  src_handlingCost=None,
                                                  tgt_total=tgt_total,
                                                  tgt_unit_price=tgt_unit_price,
                                                  tgt_net_total=tgt_net_total,
                                                  src_taxCode=tax_code,
                                                  src_shippingTaxCode=None,
                                                  src_handlingTaxCode=None,
                                                  tgt_tax_code=tax_code,
                                                  src_taxRate1=tax_rate_1,
                                                  src_shippingTax1Rate=None,
                                                  src_handlingTax1Rate=None,
                                                  src_discountRate=discount_rate,
                                                  cal_sub_total=cal_sub_total,
                                                  cal_discount_rate=cal_discount_rate,
                                                  tgt_tax_amount=tgt_tax_amount,
                                                  updated_at=updated_at))

            if 'shippingCost' in record:
                ship_method = record['shipMethod']
                shipMethod_internalId = ship_method['internalId']
                shipMethod_name = ship_method['name']
                description = shipMethod_name
                shipping_cost = float(record.get('shippingCost'))
                shipping_tax_code = record['shippingTaxCode']['name'] if 'shippingTaxCode' in record else None
                shipping_tax_rate = record.get('shippingTax1Rate')
                tgt_quantity = 1
                tgt_unit_price = shipping_cost
                tgt_discount = 0
                tgt_net_total = shipping_cost
                if shipping_tax_rate:
                    tgt_tax_amount = shipping_cost * float(shipping_tax_rate) * 0.01
                else:
                    tgt_tax_amount = 0
                tgt_total = tgt_net_total + tgt_tax_amount - tgt_discount

                credit_memo_lines.append(dict(credit_memo_id=credit_memo_id,
                                              credit_note_number=credit_note_number,
                                              src_internalId=None,
                                              src_discountItem_internalId=None,
                                              src_shipMethod_internalId=shipMethod_internalId,
                                              tgt_internalId=shipMethod_internalId,
                                              src_name=None,
                                              src_discountItem_name=None,
                                              src_shipMethod_name=shipMethod_name,
                                              tgt_description=description,
                                              src_isPreTax=None,
                                              cal_item_type='ShipItem',
                                              src_quantity=None,
                                              src_amount=None,
                                              src_rate=None,
                                              tgt_quantity=tgt_quantity,
                                              tgt_discount=tgt_discount,
                                              src_discountTotal=None,
                                              src_shippingCost=shipping_cost,
                                              src_handlingCost=None,
                                              tgt_total=tgt_total,
                                              tgt_unit_price=tgt_unit_price,
                                              tgt_net_total=tgt_net_total,
                                              src_taxCode=None,
                                              src_shippingTaxCode=shipping_tax_code,
                                              src_handlingTaxCode=None,
                                              tgt_tax_code=shipping_tax_code,
                                              src_taxRate1=None,
                                              src_shippingTax1Rate=shipping_tax_rate,
                                              src_handlingTax1Rate=None,
                                              src_discountRate=None,
                                              cal_sub_total=None,
                                              cal_discount_rate=None,
                                              tgt_tax_amount=tgt_tax_amount,
                                              updated_at=updated_at

                                              ))

            if 'handlingCost' in record:
                description = 'handling cost'
                handling_cost = float(record.get('handlingCost'))
                handling_tax_code = record['handlingTaxCode']['name'] if 'handlingTaxCode' in record else None
                handling_tax_rate = record.get('handlingTax1Rate')
                tgt_quantity = 1
                tgt_unit_price = handling_cost
                tgt_discount = 0
                tgt_net_total = handling_cost
                if handling_tax_rate:
                    tgt_tax_amount = handling_cost * float(handling_tax_rate) * 0.01
                else:
                    tgt_tax_amount = 0
                tgt_total = tgt_net_total + tgt_tax_amount - tgt_discount

                credit_memo_lines.append(dict(credit_memo_id=credit_memo_id,
                                              credit_note_number=credit_note_number,
                                              src_internalId=None,
                                              src_discountItem_internalId=None,
                                              src_shipMethod_internalId=None,
                                              tgt_internalId=None,
                                              src_name=None,
                                              src_discountItem_name=None,
                                              src_shipMethod_name=None,
                                              tgt_description=description,
                                              src_isPreTax=None,
                                              cal_item_type=None,
                                              src_quantity=None,
                                              src_amount=None,
                                              src_rate=None,
                                              tgt_quantity=tgt_quantity,
                                              tgt_discount=tgt_discount,
                                              src_discountTotal=None,
                                              src_shippingCost=None,
                                              src_handlingCost=handling_cost,
                                              tgt_total=tgt_total,
                                              tgt_unit_price=tgt_unit_price,
                                              tgt_net_total=tgt_net_total,
                                              src_taxCode=tax_code,
                                              src_shippingTaxCode=None,
                                              src_handlingTaxCode=handling_tax_code,
                                              tgt_tax_code=handling_tax_code,
                                              src_taxRate1=None,
                                              src_shippingTax1Rate=None,
                                              src_handlingTax1Rate=handling_tax_rate,
                                              src_discountRate=None,
                                              cal_sub_total=None,
                                              cal_discount_rate=None,
                                              tgt_tax_amount=tgt_tax_amount,
                                              updated_at=updated_at))

        df_creditMemo = pd.DataFrame(credit_memo)
        df_credit_memo_line = pd.DataFrame(credit_memo_lines)
        raw_csv_path = get_csv_path() + '/' + ep.CREDIT_MEMO + '_' + str(self.batch_id) + '_raw.csv'
        line_csv_path = os.path.join(get_csv_path(), ep.CREDIT_MEMO + '_line_' + str(self.batch_id) + '_raw.csv')

        df_creditMemo.to_csv(raw_csv_path,
                             columns=['id', 'credit_note_number', 'amount', 'date', 'currency_internal_id',
                                      'currency_rate', 'created_at', 'updated_at', 'currency'],
                             header=['internalId', 'tranId', 'total', 'tranDate',
                                     'currency/internalId',
                                     'exchangeRate', 'createdDate', 'lastModifiedDate', 'currency'])

        df_credit_memo_line.to_csv(line_csv_path)

    def convert_customer_payment_apply_to_csv(self):
        records = self.read_json_files(ep.CUSTOMER_PAYMENT, self.batch_id)
        # items = self.read_json_files(ep.ITEM, 0)
        result = []
        for record in records:
            if 'applyList' in record:
                internal_id = record.get('internalId')

                records = record['applyList']['apply']
                if isinstance(records, list):
                    for item in record['applyList']['apply']:
                        if item.get('apply', 'false').lower() == 'true' and item.get('type', 'other').lower() == \
                                'invoice':
                            doc = item['doc']
                            line = item['line']
                            amount = item['amount']
                            result.append(dict(internalId=internal_id,
                                               doc=doc,
                                               line=line,
                                               amount=amount))
                else:
                    doc = records['doc']
                    line = records['line']
                    amount = records['amount']
                    result.append(dict(internalId=internal_id,
                                       doc=doc,
                                       line=line,
                                       amount=amount))
        df = pd.DataFrame(result)
        csv_path = get_csv_path() + '/' + ep.CUSTOMER_PAYMENT + '_apply_' + str(self.batch_id) + '.csv'
        df.to_csv(csv_path)

    def convert_credit_memo_apply_to_csv(self):

        records = self.read_json_files(ep.CREDIT_MEMO, self.batch_id)
        # items = self.read_json_files(ep.ITEM, 0)
        result = []
        for record in records:
            if 'applyList' in record:
                internal_id = record.get('internalId')
                records = record['applyList']['apply']
                if isinstance(records, list):
                    for item in record['applyList']['apply']:
                        if item.get('apply', 'false').lower() == 'true' and item.get('type', 'other').lower() == \
                                'invoice':
                            doc = item['doc']
                            line = item['line']
                            amount = item['amount']
                            result.append(dict(internalId=internal_id,
                                               doc=doc,
                                               line=line,
                                               amount=amount))
                else:
                    doc = records['doc']
                    line = records['line']
                    amount = records['amount']
                    result.append(dict(internalId=internal_id,
                                       doc=doc,
                                       line=line,
                                       amount=amount))

        df = pd.DataFrame(result)
        csv_path = get_csv_path() + '/' + ep.CREDIT_MEMO + '_apply_' + str(self.batch_id) + '.csv'
        df.to_csv(csv_path)

    def convert_invoice_to_csv(self):

        df_currency = pd.read_csv(os.path.join(get_csv_path(), ep.CURRENCY + '_' + str(self.batch_id) + '.csv'))
        df_item = pd.read_csv(os.path.join(get_csv_path(), 'item_ref_' + str(self.batch_id) + '.csv'))
        df_vendor_bill = pd.read_csv(os.path.join(get_csv_path(), ep.VENDOR_BILL + '_' + str(self.batch_id) + '.csv'))
        df_time_bill = pd.read_csv(os.path.join(get_csv_path(), ep.TIME_BILL + '_' + str(self.batch_id) + '.csv'))
        df_customer_payment_apply = pd.read_csv(
            os.path.join(get_csv_path(), 'customerPayment_apply_' + str(self.batch_id) + '.csv'))
        df_credit_memo_apply = pd.read_csv(
            os.path.join(get_csv_path(), 'creditMemo_apply_' + str(self.batch_id) + '.csv'))
        df_customer = pd.read_csv(os.path.join(get_csv_path(), ep.CUSTOMER + '_ref_' + str(self.batch_id) + '.csv'))
        df_customer = df_customer[pd.to_datetime(df_customer.updated_at) < datetime.utcnow()]

        records = self.read_json_files(ep.INVOICE, self.batch_id)
        invoice = []
        invoice_lines = []

        for record in records:

            invoice_id = record.get('internalId', '')
            invoice_number = record.get('tranId', 'sales_invoice' + str(invoice_id))
            print('Invoice internal id:{}  number: {}'.format(invoice_id, invoice_number))

            billing_name = None
            billing_address_line_1 = None
            billing_address_line_2 = None
            billing_city = None
            billing_region = None
            billing_postcode = None
            billing_country = None
            shipping_name = None
            shipping_address_line_1 = None
            shipping_address_line_2 = None
            shipping_city = None
            shipping_region = None
            shipping_postcode = None
            shipping_country = None

            if 'billingAddress' in record:
                billing_name = record['billingAddress'].get('addressee')
                if billing_name is None:
                    billing_name = record['billingAddress'].get('attention')
                billing_address_line_1 = record['billingAddress'].get('addr1')
                billing_address_line_2 = record['billingAddress'].get('addr2')
                billing_city = record['billingAddress'].get('city')
                billing_region = record['billingAddress'].get('state')
                billing_postcode = record['billingAddress'].get('zip')
                billing_country = record['billingAddress'].get('country')

            if 'shippingAddress' in record:
                shipping_name = record['shippingAddress'].get('addressee')
                if shipping_name is None:
                    shipping_name = record['shippingAddress'].get('attention')
                shipping_address_line_1 = record['shippingAddress'].get('addr1')
                shipping_address_line_2 = record['shippingAddress'].get('addr2')
                shipping_city = record['shippingAddress'].get('city')
                shipping_region = record['shippingAddress'].get('state')
                shipping_postcode = record['shippingAddress'].get('zip')
                shipping_country = record['shippingAddress'].get('country')

            if 'entity' in record:
                contact_internal_id = record['entity']['internalId']
                contact_name = record['entity']['name']
                lookup_contact_name = df_customer[df_customer['internalId'] == int(contact_internal_id)]['name']
                r = lookup_contact_name.shape[0]
                if r > 0:
                    lookup_contact_name = lookup_contact_name.values[0]
                else:
                    lookup_contact_name = None
            tax_item_name = record['taxItem']['name'] if 'taxItem' in record else None
            invoice_tax_rate = float(record.get('taxRate', 0))

            amount = record.get('total', '')
            date = record.get('tranDate')
            due_date = record.get('dueDate')

            terms = record.get('terms')
            terms_name = terms.get('name') if terms else None
            if due_date is None:
                if terms_name:
                    if terms_name.startswith('Net'):
                        no_of_days = int(terms_name[4:])
                    else:
                        no_of_days = 0
                    tgt_due_date = add_days(date, no_of_days, DUE_DATE_FORMAT)
                else:
                    tgt_due_date = add_days(date, 30, DUE_DATE_FORMAT)
            else:
                tgt_due_date = getUTCDateTime(due_date, DUE_DATE_FORMAT)

            src_invoice_sub_total = float(record.get('subTotal', 0.0))

            # sum of line.net_total except giftCertRedemption
            tgt_invoice_net_total = 0.0

            tgt_invoice_tax_total = float(record.get('taxTotal', 0))
            # cal_sum_discount for discount_total sum of line.discount
            cal_sum_discount = 0.0
            tgt_invoice_outstanding_total = 0.0  # todo
            cal_redemption_total = 0.0
            # sum of line.total where tax_amount = 0 except paymentItem and giftCertRedemption
            tgt_invoice_tax_exempt_total = 0.0

            currency_internal_id = record['currency']['internalId']
            if currency_internal_id is not None:
                currency = df_currency[df_currency['internalId'] == int(currency_internal_id)]['symbol'].values[0]
            currency_rate = record['exchangeRate']
            created_date = record.get('createdDate')
            created_at = getUTCDateTime(created_date) if created_date else None

            last_modified_date = record.get('lastModifiedDate')
            updated_at = getUTCDateTime(last_modified_date) if last_modified_date else None

            # Section 1: handling transaction discount if exists for sub total
            discount_item = record.get('discountItem')
            discountItem_internalId = discount_item.get('internalId') if discount_item else None
            discount_total = float(record.get('discountTotal', 0.0))

            if discountItem_internalId:
                is_pre_tax = df_item[df_item['internalId'] == int(discountItem_internalId)]['isPreTax'].values[0]
            else:
                is_pre_tax = None

            # validation of sub total logic:
            if 'itemList' in record:
                item_lines = record['itemList']['item']
                cal_sub_total = 0.0

                item_list_discount_apply_to = {}
                item_list_markup_apply_to = {}

                previous_subtotal_item_out_grp = 0
                previous_subtotal_item_in_grp = -1

                flag_inside_itemgroup = False
                flag_discountable_inc_markup = False
                flag_discountable_exc_markup = False

                flag_discountable_in_grp_inc_markup = False
                flag_discountable_in_grp_exc_markup = False

                count_item_list = 0

                previous_item_type = None
                last_discountable_item = None
                last_markupable_item = None
                item_list_published_discounts = []
                item_list_unknown_markup = []
                item_list_discount = []
                item_list_markup = []
                df_raw_invoice_line = pd.DataFrame.from_dict(item_lines)
                print(df_raw_invoice_line['item'])
                item_name_list = {}
                for line in item_lines:

                    count_item_list = count_item_list + 1

                    line_amount = float(line.get('amount')) if 'amount' in line else None

                    if 'item' in line:
                        item_id = line['item'][0].get('internalId')
                        name = line['item'][0].get('name')

                        if item_id != '0':
                            item_type = df_item[df_item['internalId'] == int(item_id)]['itemType'].values[0]

                            print("items line no: {} item type {} amount {}".format(count_item_list, item_type,
                                                                                    line_amount))

                            if item_type not in ['ItemGroup', 'DescriptionItem', 'SubtotalItem']:
                                item_name_list[count_item_list] = name
                                if item_type == 'PaymentItem':
                                    src_invoice_sub_total = src_invoice_sub_total - float(line.get('amount'))
                                else:
                                    cal_sub_total = cal_sub_total + float(line.get('amount'))

                            if item_type == 'DiscountItem':

                                if 'tax1amt' in line:
                                    tax1amt = float(line['tax1amt'])
                                    if tax1amt == 0.0:
                                        tmp_tax_rate = 0
                                    else:
                                        tmp_tax_rate = tax1amt / line_amount * 100
                                elif 'tax_rate_1' in line:
                                    tax_rate_1 = line.get('tax_rate_1')
                                    tmp_tax_rate = tax_rate_1
                                else:
                                    tmp_tax_rate = 0

                                if last_discountable_item:
                                    if 'rate' in line and line['rate'][-1] == '%':
                                        tmp_discount_rate = float(line['rate'][:-1])* 0.01
                                    elif line_amount != 0.0:
                                        tmp_discount_rate = line_amount / last_discountable_item['amount']

                                    else:
                                        tmp_discount_rate = 0
                                    discount_item = last_discountable_item
                                    discount_item['discount_rate'] = tmp_discount_rate
                                    discount_item['tax_rate'] = tmp_tax_rate
                                    # only in this case, discount may apply to other discounts,
                                    item_list_discount.append(count_item_list)
                                    item_list_discount_apply_to[count_item_list] = discount_item

                                else:
                                    item_list_published_discounts.append(count_item_list)


                            elif item_type == 'SubtotalItem':
                                end_subtotal_item = count_item_list - 1
                                if previous_item_type == 'SubtotalItem' or previous_item_type == 'ItemGroup':
                                    last_discountable_item = None
                                elif end_subtotal_item != 0:
                                    # if there is only description item, empty item group
                                    if flag_inside_itemgroup:
                                        if flag_discountable_in_grp_inc_markup:
                                            pos_subtotal_items = {'start': previous_subtotal_item_in_grp + 1,
                                                                  'end': end_subtotal_item,
                                                                  'amount': line_amount}
                                            last_discountable_item = pos_subtotal_items
                                        else:
                                            last_discountable_item = None
                                        previous_subtotal_item_in_grp = count_item_list
                                    else:
                                        if flag_discountable_inc_markup:
                                            pos_subtotal_items = {'start': previous_subtotal_item_out_grp + 1,
                                                                  'end': end_subtotal_item,
                                                                  'amount': line_amount}
                                            last_discountable_item = pos_subtotal_items
                                        else:
                                            last_discountable_item = None
                                        previous_subtotal_item_out_grp = count_item_list
                                if line_amount == 0.0:
                                    print("subtotal with 0 :")
                                    last_discountable_item = None

                                last_markupable_item = last_discountable_item

                                if flag_inside_itemgroup:
                                    flag_discountable_in_grp_exc_markup = True
                                    flag_discountable_in_grp_inc_markup = True
                                else:
                                    flag_discountable_inc_markup = True
                                    flag_discountable_exc_markup = True
                            elif item_type == 'ItemGroup':
                                flag_inside_itemgroup = True
                                previous_subtotal_item_in_grp = count_item_list
                                flag_discountable_inc_markup = True
                                flag_discountable_exc_markup = True
                                flag_discountable_in_grp_inc_markup = False
                                flag_discountable_in_grp_exc_markup = False
                                start_group_item = count_item_list + 1
                                last_discountable_item = None
                                last_markupable_item = last_discountable_item
                            elif item_type == 'MarkupItem':
                                if flag_inside_itemgroup:
                                    flag_discountable_in_grp_inc_markup = True  # discountable item already set to True
                                    # due to item group
                                else:
                                    flag_discountable_inc_markup = True
                                if not flag_discountable_exc_markup or not flag_discountable_in_grp_exc_markup:
                                    if line_amount != 0.0:
                                        last_discountable_item = {'start': count_item_list, 'end': count_item_list,
                                                                  'amount': line_amount}
                                    else:
                                        last_discountable_item = None

                                if last_markupable_item:
                                    item_list_markup_apply_to[count_item_list] = last_markupable_item

                                    item_list_markup.append(count_item_list)
                                else:
                                    item_list_unknown_markup.append(count_item_list)
                                    item_name_list[count_item_list] = 'Markup on-unknown item'

                            elif item_type == 'DescriptionItem':
                                # if not flag_discountable_inc_markup:
                                #     start_subtotal_item = start_subtotal_item + 1
                                # if flag_inside_itemgroup and (not flag_discountable_in_grp_inc_markup):
                                #     start_group_item = start_group_item + 1
                                # don't need to calculate the exact start item, those items will not be published or
                                # discountable item ( put off the calculation to next loop )
                                pass

                            else:
                                if flag_inside_itemgroup:
                                    # already inside item group, no need to set the flag of discoutable item
                                    flag_discountable_in_grp_exc_markup = True
                                    flag_discountable_in_grp_inc_markup = True
                                else:
                                    flag_discountable_inc_markup = True
                                    flag_discountable_exc_markup = True

                                last_discountable_item = {'start': count_item_list, 'end': count_item_list,
                                                          'amount': line_amount}
                            previous_item_type = item_type
                            last_markupable_item = last_discountable_item
                        else:
                            end_group_item = count_item_list - 1
                            if end_group_item < start_group_item:
                                # group has no items
                                pos_group_items = None
                            else:
                                pos_group_items = {'start': start_group_item, 'end': end_group_item,
                                                   'amount': line_amount}
                            previous_item_type = 'ItemGroupEnd'
                            last_discountable_item = pos_group_items
                            last_markupable_item = last_discountable_item
                            flag_inside_itemgroup = False
                    else:
                        item_name_list[count_item_list] = ''
                        # in case there is no item name for the line
                        cal_sub_total = cal_sub_total + float(line.get('amount'))

                        last_discountable_item = {'start': count_item_list, 'end': count_item_list, 'amount':
                            line_amount}
                        previous_item_type = 'InventoryItem'
                        last_markupable_item = last_discountable_item

                        if flag_inside_itemgroup:
                            # already inside item group, no need to set the flag of discoutable item
                            flag_discountable_in_grp_exc_markup = True
                            flag_discountable_in_grp_inc_markup = True
                        else:
                            flag_discountable_inc_markup = True
                            flag_discountable_exc_markup = True

            item_cost_discount_item_type = None
            if 'itemCostDiscount' in record:
                item_cost_discount = record['itemCostDiscount']
                itemCostDiscount_internalId = item_cost_discount['internalId']
                df_tmp = df_item[df_item['internalId'] == int(itemCostDiscount_internalId)]
                item_cost_discount_item_type = df_tmp['itemType'].values[0]
                item_cost_discount_item_name = df_tmp['reference'].values[0]

                item_cost_discount_amount = float(record.get('itemCostDiscAmount'))
                cal_sub_total = cal_sub_total + item_cost_discount_amount

                if 'itemCostDiscTax1Amt' in record:
                    item_cost_discount_tax = float(record.get('itemCostDiscTax1Amt'))
                    item_cost_discount_tax_rate = item_cost_discount_tax / item_cost_discount_amount * 100
                elif 'itemCostTaxRate1' in record:
                    item_cost_discount_tax_rate = float(record.get('itemCostTaxRate1'))
                    item_cost_discount_tax = item_cost_discount_amount * item_cost_discount_tax_rate * 0.01
                else:
                    item_cost_discount_tax = 0
                    item_cost_discount_tax_rate = 0

            exp_cost_discount_item_type = None
            if 'expCostDiscount' in record:
                exp_cost_discount = record['expCostDiscount']
                expCostDiscount_internalId = exp_cost_discount['internalId']

                df_tmp = df_item[df_item['internalId'] == int(expCostDiscount_internalId)]
                exp_cost_discount_item_type = df_tmp['itemType'].values[0]
                exp_cost_discount_item_name = df_tmp['reference'].values[0]

                exp_cost_discount_amount = float(record.get('expCostDiscAmount'))
                cal_sub_total = cal_sub_total + exp_cost_discount_amount

                if 'expCostDiscTax1Amt' in record:
                    exp_cost_discount_tax = float(record.get('expCostDiscTax1Amt'))
                    exp_cost_discount_tax_rate = exp_cost_discount_tax / exp_cost_discount_amount * 100
                elif 'expCostTaxRate1' in record:
                    exp_cost_discount_tax_rate = float(record.get('expCostTaxRate1'))
                    exp_cost_discount_tax = exp_cost_discount_amount * exp_cost_discount_tax_rate * 0.01
                else:
                    exp_cost_discount_tax = 0
                    exp_cost_discount_tax_rate = 0

            time_discount_item_type = None
            if 'timeDiscount' in record:
                time_discount = record['timeDiscount']
                timeDiscount_internalId = time_discount['internalId']
                df_tmp = df_item[df_item['internalId'] == int(timeDiscount_internalId)]
                time_discount_item_name = df_tmp['reference'].values[0]
                time_discount_item_type = df_tmp['itemType'].values[0]
                print('time discount item type {}'.format(time_discount_item_type))

                time_discount_amount = float(record.get('timeDiscAmount'))
                if 'timeDiscTax1Amt' in record:
                    time_discount_tax = float(record.get('timeDiscTax1Amt'))
                    time_discount_tax_rate = time_discount_tax / time_discount_amount * 100
                elif 'timeDiscTax1Rate' in record:
                    time_discount_tax_rate = float(record.get('timeTaxRate1'))
                    time_discount_tax = time_discount_amount * time_discount_tax_rate * 0.01
                else:
                    time_discount_tax = 0
                    time_discount_tax_rate = 0

                cal_sub_total = cal_sub_total + time_discount_amount

            if 'timeList' in record:
                time_list = record['timeList']['time']
                time_list_total = 0.0
                time_list_name = ''
                for time_line in time_list:

                    if time_line['apply'].upper() == 'TRUE':
                        itemDisp = time_line.get('itemDisp')
                        amount = float(time_line.get('amount'))
                        cal_sub_total = cal_sub_total + amount
                        time_list_total = time_list_total + amount
                        time_list_name = time_list_name + ',' + itemDisp
                time_list_name = time_list_name[1:]

            if 'itemCostList' in record:
                item_cost_lines = record['itemCostList']['itemCost']
                item_cost_list_total = 0.0
                item_cost_list_name = ''
                for item_cost_line in item_cost_lines:
                    apply = item_cost_line['apply'].upper() == 'TRUE'  # make sure apply exists in itemCost
                    if apply:
                        doc = item_cost_line.get('doc')
                        line = item_cost_line.get('line')
                        df_tmp_vb = df_vendor_bill[(df_vendor_bill['internalId'] == int(doc)) & (df_vendor_bill['line']
                                                                                                 == int(line))]
                        item_internal_id = df_tmp_vb['item_internalId'].values[0]
                        if item_internal_id != '0':
                            df_tmp = df_item[df_item['internalId'] == int(item_internal_id)]
                            item_type = df_tmp['itemType'].values[0]
                            item_name = df_tmp['reference'].values[0]
                            if item_type not in ['ItemGroup', 'DescriptionItem', 'SubtotalItem']:
                                amount = float(item_cost_line.get('amount', 0))
                                cal_sub_total = cal_sub_total + amount
                                item_cost_list_total = item_cost_list_total + amount
                                item_cost_list_name = item_cost_list_name + ',' + item_name
                item_cost_list_name = item_cost_list_name[1:]

            if 'expCostList' in record:
                exp_cost_list = record['expCostList']['expCost']
                exp_cost_list_total = 0.0
                exp_cost_list_name = ''
                for exp_cost in exp_cost_list:
                    if exp_cost['apply'].upper() == 'TRUE':
                        amount = float(exp_cost.get('amount'))
                        memo = exp_cost.get('memo', '')
                        cal_sub_total = cal_sub_total + amount
                        exp_cost_list_total = exp_cost_list_total + amount
                        exp_cost_list_name = exp_cost_list_name + ',' + memo
                exp_cost_list_name = exp_cost_list_name[1:]

            discount_rate = record.get('discountRate')
            if discount_rate is not None and discount_rate[-1] == '%':
                cal_transaction_discount_rate = 1 - -1 * float(discount_rate[:-1]) * 0.01
            elif discount_rate is not None and cal_sub_total != 0.0:
                cal_transaction_discount_rate = 1 - -1 * discount_total / cal_sub_total
            else:
                cal_transaction_discount_rate = 1

            print("Invoice {} published discount {}".format(invoice_id, str(item_list_published_discounts)))
            item_list_discount_rate = {}
            item_list_tax_rate = {}
            print("invoice {} dicount apply to {}".format(invoice_id, str(item_list_discount_apply_to)))

            item_list_discount.reverse()
            item_list_discount_to_discount = {}

            print(item_list_discount)
            for discount in item_list_discount:
                discount_rate = item_list_discount_apply_to[discount][
                                    'discount_rate']*item_list_discount_to_discount.get(discount, 1)
                start = item_list_discount_apply_to[discount]['start']
                end = item_list_discount_apply_to[discount]['end'] + 1
                for i in range(start, end):
                    if i in item_list_discount:
                        item_list_discount_to_discount[i] = item_list_discount_to_discount.get(i, 1) + discount_rate
            print(item_list_discount_to_discount)

            for line_no, discount in item_list_discount_apply_to.items():

                cal_line_discount_rate = discount['discount_rate'] * item_list_discount_to_discount.get(line_no,
                                                                                                        1) * cal_transaction_discount_rate
                cal_line_tax_rate = discount['tax_rate']  # already times 0.01
                start = discount['start']
                end = discount['end'] + 1

                for i in range(start, end):
                    if i in item_list_discount_rate:
                        item_list_discount_rate[i] = item_list_discount_rate[i] + cal_line_discount_rate
                        item_list_tax_rate[i] = item_list_tax_rate[i] + cal_line_tax_rate * cal_line_discount_rate
                    else:
                        item_list_discount_rate[i] = cal_line_discount_rate
                        item_list_tax_rate[i] = cal_line_tax_rate

            print("invoice {} discount rate {}".format(invoice_id, str(item_list_discount_rate)))
            print("invoice {} tax rate {}".format(invoice_id, str(item_list_tax_rate)))

            print("item name list {}".format(str(item_name_list)))
            print("item markup apply to {}".format(str(item_list_markup_apply_to)))
            for markup in item_list_markup:
                start = item_list_markup_apply_to[markup]['start']
                end = item_list_markup_apply_to[markup]['end'] + 1
                name = ''
                for i in range(start, end):
                    if i in item_name_list:
                        name = name + ',' + item_name_list[i]
                name = 'Markup on-' + name[1:]
                print(name)

                item_name_list[markup] = name

            # section 2: itemList
            if 'itemList' in record:
                item_lines = record['itemList']['item']
                cal_sub_total = 0.0

                count_item_list = 0
                for item_line in item_lines:
                    count_item_list = count_item_list + 1
                    if 'item' in item_line:
                        item_id = item_line['item'][0].get('internalId')
                        name = item_line['item'][0].get('name')

                        if item_id != '0':
                            df_tmp = df_item[df_item['internalId'] == int(item_id)]
                            item_type = df_tmp['itemType'].values[0]
                            item_name = df_tmp['reference'].values[0]

                            if item_type == 'MarkupItem':
                                name = item_name_list[count_item_list]

                            if item_type not in ['ItemGroup', 'DescriptionItem', 'SubtotalItem', 'DiscountItem'] or \
                                    item_type == 'DiscountItem' and count_item_list in item_list_published_discounts:
                                description = name
                                quantity = float(item_line['quantity']) if 'quantity' in item_line else None
                                rate = item_line.get('rate')
                                amount = float(item_line.get('amount'))
                                if quantity is not None:
                                    tgt_quantity = quantity
                                elif rate is not None and rate[-1] != '%' and float(rate) != 0.0:
                                    tgt_quantity = amount / float(rate)
                                else:
                                    tgt_quantity = 1

                                if item_type == 'DiscountItem':
                                    tgt_unit_price = 0
                                elif rate is not None and rate[-1] != '%' and float(rate) != 0.0:
                                    tgt_unit_price = rate
                                elif quantity is not None:
                                    tgt_unit_price = amount / quantity
                                else:
                                    tgt_unit_price = amount

                                if item_type == 'DiscountItem':
                                    tgt_discount = -1 * amount * item_list_discount_to_discount.get(count_item_list,1) * \
                                                   cal_line_discount_rate + amount*(1-cal_line_discount_rate)


                                else:
                                    print(item_list_discount_rate.get(count_item_list, 0))
                                    tgt_discount = -1 * amount * round(item_list_discount_rate.get(count_item_list,
                                                                                                   0),2) + amount*(1-cal_line_discount_rate)

                                cal_sum_discount = cal_sum_discount + tgt_discount

                                if item_type == 'DiscountItem':
                                    tgt_net_total = 0
                                else:
                                    tgt_net_total = amount

                                tgt_invoice_net_total = tgt_invoice_net_total + tgt_net_total

                                # requirements updated on Apr

                                #     If itemList.item.isTaxable is found in this
                                #     record,
                                #     if itemList.item.isTaxable = true
                                #     = taxItem.name
                                #     else if itemList.item.isTaxable = false
                                #     = null
                                # else =itemList.item.taxCode.name

                                if 'isTaxable' in item_line:
                                    if item_line['isTaxable'].upper() == 'TRUE':
                                        tax_code = tax_item_name
                                    else:
                                        tax_code = None
                                else:
                                    if 'taxCode' in item_line:
                                        tax_code = item_line['taxCode'].get('name')
                                    else:
                                        tax_code = None

                                if 'isTaxable' in item_line:
                                    if item_line['isTaxable'].upper() == 'TRUE':
                                        tax_rate_1 = invoice_tax_rate
                                    else:
                                        tax_rate_1 = 0
                                elif 'tax1Amt' in item_line:
                                    tax1_amt = float(item_line['tax1Amt'])
                                    tax_rate_1 = tax1_amt / amount * 100 if tax1_amt != 0.0 else 0

                                else:
                                    tax_rate_1 = float(item_line.get('taxRate1', 0))

                                if item_type == 'DiscountItem':
                                    tax_rate_1 = tax_rate_1 * item_list_discount_to_discount.get(count_item_list,1)
                                else:
                                    tax_rate_1 = tax_rate_1 + item_list_tax_rate.get(count_item_list, 0)

                                if str(is_pre_tax).upper() == 'TRUE':
                                    print('isPreTax {} is TRUE'.format(is_pre_tax))
                                    tgt_tax_amount = amount * cal_transaction_discount_rate * tax_rate_1 * 0.01
                                else:
                                    tgt_tax_amount = amount * tax_rate_1 * 0.01
                                tgt_total = tgt_net_total + tgt_tax_amount - tgt_discount
                                if item_type != 'PaymentItem' and tgt_tax_amount == 0.0:
                                    tgt_invoice_tax_exempt_total = tgt_invoice_tax_exempt_total + tgt_total
                                invoice_lines.append(dict(invoice_id=invoice_id,
                                                          invoice_number=invoice_number,
                                                          src_internalId=item_id,
                                                          tgt_description=description,
                                                          tgt_item_name=item_name,
                                                          tgt_quantity=tgt_quantity,
                                                          tgt_discount=tgt_discount,
                                                          tgt_total=round(tgt_total, 2),
                                                          tgt_unit_price=tgt_unit_price,
                                                          tgt_net_total=tgt_net_total,
                                                          tgt_tax_code=tax_code,
                                                          tgt_tax_amount=round(tgt_tax_amount, 2),
                                                          cal_sub_total=cal_sub_total,
                                                          cal_discount_rate=cal_transaction_discount_rate,
                                                          cal_item_type=item_type,
                                                          isPreTax=is_pre_tax,
                                                          src_quantity=quantity,
                                                          src_amount=amount,
                                                          src_rate=rate,
                                                          src_taxRate1=tax_rate_1,
                                                          updated_at=updated_at))
                    else:

                        quantity = float(item_line['quantity']) if 'quantity' in item_line else None
                        rate = item_line.get('rate')
                        amount = float(item_line.get('amount'))
                        print('quantity {} rate {} amount {}:'.format(quantity, rate, amount))
                        if quantity is not None:
                            tgt_quantity = quantity
                        elif rate is not None and rate[-1] != '%' and float(rate) != 0.0:
                            tgt_quantity = amount / float(rate)
                        else:
                            tgt_quantity = 1

                        if rate is not None and rate[-1] != '%' and float(rate) != 0.0:
                            tgt_unit_price = rate
                        elif quantity is not None:
                            tgt_unit_price = amount / quantity
                        else:
                            tgt_unit_price = amount

                        tgt_discount = -1 * amount * round(item_list_discount_rate.get(count_item_list,
                                                                                       0), 2) + amount * (
                                                   1 - cal_line_discount_rate)
                        cal_sum_discount = cal_sum_discount + tgt_discount

                        tgt_net_total = amount

                        tgt_invoice_net_total = tgt_invoice_net_total + tgt_net_total

                        if 'isTaxable' in item_line:
                            if item_line['isTaxable'].upper() == 'TRUE':
                                tax_code = tax_item_name
                            else:
                                tax_code = None
                        else:
                            if 'taxCode' in item_line:
                                tax_code = item_line['taxCode'].get('name')
                            else:
                                tax_code = None
                                print('Credit Memo {} Item {} doesnt have tax_code'.format(invoice_number,
                                                                                           item_id))

                        if 'isTaxable' in item_line:
                            if item_line['isTaxable'].upper() == 'TRUE':
                                tax_rate_1 = invoice_tax_rate
                            else:
                                tax_rate_1 = 0
                        elif 'tax1Amt' in item_line:
                            tax1_amt = float(time_line['tax1Amt'])
                            tax_rate_1 = tax1_amt / amount * 100 if tax1_amt != 0.0 else 0

                        else:
                            tax_rate_1 = float(item_line.get('taxRate1', 0))

                        tax_rate_1 = tax_rate_1 + item_list_tax_rate.get(count_item_list, 0)

                        if str(is_pre_tax).upper() == 'TRUE':
                            print('isPreTax {} is TRUE'.format(is_pre_tax))
                            tgt_tax_amount = amount * cal_transaction_discount_rate * tax_rate_1 * 0.01
                        else:
                            tgt_tax_amount = amount * tax_rate_1 * 0.01
                        tgt_total = tgt_net_total + tgt_tax_amount - tgt_discount
                        if item_type != 'PaymentItem' and tgt_tax_amount == 0.0:
                            tgt_invoice_tax_exempt_total = tgt_invoice_tax_exempt_total + tgt_total
                        invoice_lines.append(dict(invoice_id=invoice_id,
                                                  invoice_number=invoice_number,
                                                  src_internalId=None,
                                                  tgt_description=None,
                                                  tgt_item_name=None,
                                                  tgt_quantity=tgt_quantity,
                                                  tgt_discount=tgt_discount,
                                                  tgt_total=round(tgt_total, 2),
                                                  tgt_unit_price=tgt_unit_price,
                                                  tgt_net_total=tgt_net_total,
                                                  tgt_tax_code=tax_code,
                                                  tgt_tax_amount=round(tgt_tax_amount, 2),
                                                  cal_sub_total=cal_sub_total,
                                                  cal_discount_rate=cal_transaction_discount_rate,
                                                  cal_item_type=item_type,
                                                  isPreTax=is_pre_tax,
                                                  src_quantity=quantity,
                                                  src_amount=amount,
                                                  src_rate=rate,
                                                  src_taxRate1=tax_rate_1,
                                                  updated_at=updated_at))

            # section 3: timeList
            if 'timeList' in record:
                time_list = record['timeList']['time']
                for time_line in time_list:

                    if time_line['apply'].upper() == 'TRUE':
                        doc = time_line.get('doc')
                        itemDisp = time_line.get('itemDisp')
                        description = itemDisp
                        quantity = time_line.get('quantity')
                        line = time_line.get('line')

                        df_tmp_item = df_time_bill[(df_time_bill['internalId'] == int(doc))]
                        item_internal_id = df_tmp_item['item_internalId'].values[0]

                        df_tmp = df_item[df_item['internalId'] == int(item_internal_id)]
                        item_type = df_tmp['itemType'].values[0]
                        item_name = df_tmp['reference'].values[0]

                        rate = time_line.get('rate')
                        amount = float(time_line.get('amount'))
                        print('quantity {} rate {} amount {}:'.format(quantity, rate, amount))
                        pos = quantity.index(':')
                        tgt_quantity = round(int(quantity[:pos]) + int(quantity[pos + 1:]) / 60, 2)
                        tgt_unit_price = rate
                        if time_discount_item_type == 'DiscountItem':
                            tgt_discount = amount * time_discount_amount / time_list_total
                        else:
                            tgt_discount = 0
                        tgt_net_total = amount
                        tgt_invoice_net_total = tgt_invoice_net_total + tgt_net_total

                        if 'taxCode' in time_line:
                            tax_code = time_line['taxCode'].get('name')
                        else:
                            tax_code = None
                            print(
                                'Invoice {} Item {} doesnt have tax_code'.format(invoice_number,
                                                                                 item_id))
                        if 'tax1Amt' in time_line:
                            tax1_amt = float(time_line['tax1Amt'])
                            tax_rate_1 = tax1_amt / amount * 100 if tax1_amt != 0.0 else 0

                        else:
                            tax_rate_1 = float(time_line.get('taxRate1', 0))

                        if time_discount_item_type == 'DiscountItem':
                            tax_rate_1 = tax_rate_1 + time_discount_tax_rate * time_discount_amount / time_list_total

                        if str(is_pre_tax).upper() == 'TRUE':
                            print('isPreTax {} is True'.format(is_pre_tax))
                            tgt_tax_amount = amount * tax_rate_1 * cal_transaction_discount_rate * 0.01
                        else:
                            tgt_tax_amount = amount * tax_rate_1 * 0.01

                        tgt_total = tgt_net_total + tgt_tax_amount - tgt_discount
                        if tax_rate_1 == 0.0:
                            tgt_invoice_tax_exempt_total = tgt_invoice_tax_exempt_total + tgt_total
                        invoice_lines.append(dict(invoice_id=invoice_id,
                                                  invoice_number=invoice_number,
                                                  src_internalId=doc,
                                                  tgt_description=description,
                                                  tgt_item_name=item_name,
                                                  tgt_quantity=tgt_quantity,
                                                  tgt_discount=tgt_discount,
                                                  tgt_total=round(tgt_total, 2),
                                                  tgt_unit_price=tgt_unit_price,
                                                  tgt_net_total=tgt_net_total,
                                                  tgt_tax_code=tax_code,
                                                  tgt_tax_amount=round(tgt_tax_amount, 2),
                                                  cal_sub_total=cal_sub_total,
                                                  cal_discount_rate=cal_transaction_discount_rate,
                                                  cal_item_type=item_type,
                                                  isPreTax=is_pre_tax,
                                                  src_quantity=quantity,
                                                  src_amount=amount,
                                                  src_rate=rate,
                                                  src_taxRate1=tax_rate_1,
                                                  updated_at=updated_at))

            # section 4: itemCostList
            if 'itemCostList' in record:
                item_cost_lines = record['itemCostList']['itemCost']
                for item_cost_line in item_cost_lines:
                    apply = item_cost_line['apply'].upper() == 'TRUE'  # make sure apply exists in itemCost
                    if apply:
                        doc = item_cost_line.get('doc')
                        line = item_cost_line.get('line')
                        name = item_cost_line.get('itemDisp')
                        df_tmp_item = df_vendor_bill[(df_vendor_bill['internalId'] == int(doc)) & (df_vendor_bill[
                                                                                                       'line'] ==
                                                                                                   int(line))]
                        item_internal_id = df_tmp_item['item_internalId'].values[0]
                        if item_internal_id != '0':
                            df_tmp = df_item[df_item['internalId'] == int(item_internal_id)]
                            item_type = df_tmp['itemType'].values[0]
                            item_name = df_tmp['reference'].values[0]

                            if item_type not in ['ItemGroup', 'DescriptionItem', 'SubtotalItem', 'DiscountItem',
                                                 'MarkupItem']:

                                description = name
                                quantity = float(
                                    item_cost_line['itemCostCount']) if 'itemCostCount' in item_cost_line else None
                                cost = item_cost_line.get('cost')
                                amount = float(item_cost_line.get('amount', 0))
                                print('quantity {} rate {} amount {}:'.format(quantity, rate, amount))
                                if quantity is not None:
                                    tgt_quantity = quantity
                                elif cost is not None and cost[-1] != '%' and float(cost) != 0.0:
                                    tgt_quantity = amount / float(cost)
                                    if tgt_quantity == -0:
                                        tgt_quantity = 0
                                else:
                                    tgt_quantity = 1

                                if cost is not None and cost[-1] != '%' and float(cost) != 0.0:
                                    tgt_unit_price = cost
                                elif quantity is not None:
                                    tgt_unit_price = amount / quantity
                                else:
                                    tgt_unit_price = amount

                                if item_cost_discount_item_type == 'DiscountItem':
                                    tgt_discount = amount * item_cost_discount_amount / item_cost_list_total
                                else:
                                    tgt_discount = 0

                                tgt_net_total = amount

                                tgt_invoice_net_total = tgt_invoice_net_total + tgt_net_total

                                if 'taxCode' in item_cost_line:
                                    tax_code = item_cost_line['taxCode'].get('name')
                                else:
                                    tax_code = None
                                    print('Credit Memo {} Item {} doesnt have tax_code'.format(invoice_number,
                                                                                               item_id))
                                if 'tax1Amt' in item_cost_line:
                                    tax1_amt = float(item_cost_line['tax1Amt'])
                                    tax_rate_1 = tax1_amt / amount * 100 if tax1_amt != 0.0 else 0

                                else:
                                    tax_rate_1 = float(item_cost_line.get('taxRate1', 0))

                                if item_cost_discount_item_type == 'DiscountItem':
                                    tax_rate_1 = tax_rate_1 + item_cost_discount_tax_rate * item_cost_discount_amount \
                                                 / item_cost_list_total

                                if str(is_pre_tax).upper() == 'TRUE':
                                    print('isPreTax {} is True'.format(is_pre_tax))
                                    tgt_tax_amount = amount * cal_transaction_discount_rate * tax_rate_1 * 0.01
                                else:
                                    tgt_tax_amount = amount * tax_rate_1 * 0.01

                                tgt_total = tgt_net_total + tgt_tax_amount - tgt_discount
                                if tgt_tax_amount == 0.0:
                                    tgt_invoice_tax_exempt_total = tgt_invoice_tax_exempt_total + tgt_total
                                invoice_lines.append(dict(invoice_id=invoice_id,
                                                          invoice_number=invoice_number,
                                                          src_internalId='{}_{}'.format(doc, line),
                                                          tgt_description=description,
                                                          tgt_item_name=item_name,
                                                          tgt_quantity=tgt_quantity,
                                                          tgt_discount=tgt_discount,
                                                          tgt_total=round(tgt_total, 2),
                                                          tgt_unit_price=tgt_unit_price,
                                                          tgt_net_total=tgt_net_total,
                                                          tgt_tax_code=tax_code,
                                                          tgt_tax_amount=round(tgt_tax_amount, 2),
                                                          cal_sub_total=cal_sub_total,
                                                          cal_discount_rate=cal_transaction_discount_rate,
                                                          cal_item_type=item_type,
                                                          isPreTax=is_pre_tax,
                                                          src_quantity=None,
                                                          src_amount=amount,
                                                          src_cost=cost,
                                                          src_rate=None,
                                                          src_taxRate1=tax_rate_1,
                                                          updated_at=updated_at))
            # section 5: expCostList
            if 'expCostList' in record:
                exp_cost_list = record['expCostList']['expCost']
                for exp_cost in exp_cost_list:

                    if exp_cost['apply'].upper() == 'TRUE':
                        doc = exp_cost.get('doc')
                        memo = exp_cost.get('memo')
                        description = memo
                        tgt_quantity = 1
                        amount = float(exp_cost.get('amount'))
                        tgt_unit_price = amount
                        if exp_cost_discount_item_type == 'DiscountItem':
                            tgt_discount = amount * exp_cost_discount_amount / exp_cost_list_total
                        else:
                            tgt_discount = 0
                        tgt_net_total = amount
                        tgt_invoice_net_total = tgt_invoice_net_total + tgt_net_total

                        tax_code = exp_cost['taxCode'].get('name') if 'taxCode' in exp_cost else None
                        if 'tax1Amt' in exp_cost:
                            tax1_amt = float(exp_cost['tax1Amt'])
                            tax_rate_1 = tax1_amt / amount * 100 if tax1_amt != 0.0 else 0

                        else:
                            tax_rate_1 = float(exp_cost.get('taxRate1', 0))

                        if item_cost_discount_item_type == 'DiscountItem':
                            tax_rate_1 = tax_rate_1 + exp_cost_discount_tax_rate * exp_cost_discount_amount / \
                                         exp_cost_list_total

                        if str(is_pre_tax).upper() == 'TRUE':
                            print('isPreTax {} is True'.format(is_pre_tax))
                            tgt_tax_amount = amount * cal_transaction_discount_rate * tax_rate_1 * 0.01
                        else:
                            tgt_tax_amount = amount * tax_rate_1 * 0.01

                        tgt_total = tgt_net_total + tgt_tax_amount - tgt_discount
                        if tgt_tax_amount == 0.0:
                            tgt_invoice_tax_exempt_total = tgt_invoice_tax_exempt_total + tgt_total
                        invoice_lines.append(dict(invoice_id=invoice_id,
                                                  invoice_number=invoice_number,
                                                  src_internalId=doc,
                                                  tgt_description=description,
                                                  tgt_item_name=None,
                                                  tgt_quantity=tgt_quantity,
                                                  tgt_discount=tgt_discount,
                                                  tgt_total=round(tgt_total, 2),
                                                  tgt_unit_price=tgt_unit_price,
                                                  tgt_net_total=tgt_net_total,
                                                  tgt_tax_code=tax_code,
                                                  tgt_tax_amount=round(tgt_tax_amount, 2),
                                                  cal_sub_total=cal_sub_total,
                                                  cal_discount_rate=cal_transaction_discount_rate,
                                                  cal_item_type=None,
                                                  isPreTax=is_pre_tax,
                                                  src_quantity=None,
                                                  src_amount=amount,
                                                  src_rate=None,
                                                  src_taxRate1=tax_rate_1,
                                                  updated_at=updated_at
                                                  ))
            # section 6 itemCostDiscount
            if item_cost_discount_item_type == 'MarkupItem':

                description = "Markup on-" + item_cost_list_name

                tgt_quantity = 1
                tgt_discount = 0
                tgt_unit_price = item_cost_discount_amount
                tgt_net_total = item_cost_discount_amount

                item_cost_tax_code = record['itemCostTaxCode'][
                    'name'] if 'itemCostTaxCode' in record else None  #

                item_cost_tax_rate = float(record.get('itemCostTaxRate1', 0))

                tgt_invoice_net_total = tgt_invoice_net_total + tgt_net_total

                if str(is_pre_tax).upper() == 'TRUE':
                    print('isPreTax {} is True'.format(is_pre_tax))
                    tgt_tax_amount = item_cost_discount_amount * cal_transaction_discount_rate * item_cost_tax_rate * 0.01
                else:
                    tgt_tax_amount = item_cost_discount_amount * item_cost_tax_rate * 0.01
                tgt_total = tgt_net_total + tgt_tax_amount - tgt_discount
                if tgt_tax_amount == 0.0:
                    tgt_invoice_tax_exempt_total = tgt_invoice_tax_exempt_total + tgt_total
                invoice_lines.append(dict(invoice_id=invoice_id,
                                          invoice_number=invoice_number,
                                          src_internalId=itemCostDiscount_internalId,
                                          tgt_description=description,
                                          tgt_item_name=item_cost_discount_item_name,
                                          tgt_quantity=tgt_quantity,
                                          tgt_discount=tgt_discount,
                                          tgt_total=round(tgt_total, 2),
                                          tgt_unit_price=tgt_unit_price,
                                          tgt_net_total=tgt_net_total,
                                          tgt_tax_code=item_cost_tax_code,
                                          tgt_tax_amount=round(tgt_tax_amount, 2),
                                          cal_sub_total=cal_sub_total,
                                          cal_discount_rate=cal_transaction_discount_rate,
                                          cal_item_type=item_cost_discount_item_type,
                                          isPreTax=is_pre_tax,
                                          src_expDiscountAmount=item_cost_discount_amount,
                                          src_timeTaxRate1=item_cost_tax_rate,
                                          src_quantity=None,
                                          src_amount=None,
                                          src_rate=None,
                                          updated_at=updated_at
                                          ))

            # section 7 expCostDiscount
            if exp_cost_discount_item_type == 'MarkupItem':

                description = "Markup on-" + exp_cost_list_name

                tgt_quantity = 1
                tgt_discount = 0
                tgt_unit_price = exp_cost_discount_amount

                tgt_net_total = exp_cost_discount_amount

                exp_cost_tax_code = record['expCostTaxCode'][
                    'name'] if 'expCostTaxCode' in record else None  #

                exp_cost_tax_rate = float(record.get('expCostTaxRate1', 0))

                tgt_invoice_net_total = tgt_invoice_net_total + tgt_net_total

                if str(is_pre_tax).upper() == 'TRUE':
                    print('isPreTax {} is True'.format(is_pre_tax))
                    tgt_tax_amount = exp_cost_discount_amount * cal_transaction_discount_rate * exp_cost_tax_rate * 0.01
                else:
                    tgt_tax_amount = exp_cost_discount_amount * exp_cost_tax_rate * 0.01
                tgt_total = tgt_net_total + tgt_tax_amount - tgt_discount

                if tgt_tax_amount == 0.0:
                    tgt_invoice_tax_exempt_total = tgt_invoice_tax_exempt_total + tgt_total

                invoice_lines.append(dict(invoice_id=invoice_id,
                                          invoice_number=invoice_number,
                                          src_internalId=expCostDiscount_internalId,
                                          tgt_description=description,
                                          tgt_item_name=exp_cost_discount_item_name,
                                          tgt_quantity=tgt_quantity,
                                          tgt_discount=tgt_discount,
                                          tgt_total=round(tgt_total, 2),
                                          tgt_unit_price=tgt_unit_price,
                                          tgt_net_total=tgt_net_total,
                                          tgt_tax_code=exp_cost_tax_code,
                                          tgt_tax_amount=round(tgt_tax_amount, 2),
                                          cal_sub_total=cal_sub_total,
                                          cal_discount_rate=cal_transaction_discount_rate,
                                          cal_item_type=exp_cost_discount_item_type,
                                          isPreTax=is_pre_tax,
                                          src_expDiscountAmount=exp_cost_discount_amount,
                                          src_timeTaxRate1=exp_cost_tax_rate,
                                          src_quantity=None,
                                          src_amount=None,
                                          src_rate=None,
                                          updated_at=updated_at
                                          ))
            # section 8 timeDiscount
            if time_discount_item_type == 'MarkupItem':
                description = "Markup on-" + time_list_name

                time_discount_amount = float(record.get('timeDiscAmount'))
                tgt_quantity = 1
                tgt_discount = 0

                tgt_unit_price = time_discount_amount
                tgt_net_total = time_discount_amount

                time_tax_code = record['timeTaxCode']['name'] if 'timeTaxCode' in record else None  #

                time_tax_rate = float(record.get('timeTaxRate1', 0))

                tgt_invoice_net_total = tgt_invoice_net_total + tgt_net_total

                if str(is_pre_tax).upper() == 'TRUE':
                    print('isPreTax {} is True'.format(is_pre_tax))
                    tgt_tax_amount = time_discount_amount * cal_transaction_discount_rate * time_tax_rate * 0.01
                else:
                    tgt_tax_amount = time_discount_amount * time_tax_rate * 0.01
                tgt_total = tgt_net_total + tgt_tax_amount - tgt_discount
                if tgt_tax_amount == 0.0:
                    tgt_invoice_tax_exempt_total = tgt_invoice_tax_exempt_total + tgt_total

                invoice_lines.append(dict(invoice_id=invoice_id,
                                          invoice_number=invoice_number,
                                          src_internalId=timeDiscount_internalId,
                                          tgt_description=description,
                                          tgt_item_name=time_discount_item_name,
                                          tgt_quantity=tgt_quantity,
                                          tgt_discount=tgt_discount,
                                          tgt_total=round(tgt_total, 2),
                                          tgt_unit_price=tgt_unit_price,
                                          tgt_net_total=tgt_net_total,
                                          tgt_tax_code=time_tax_code,
                                          tgt_tax_amount=round(tgt_tax_amount, 2),
                                          cal_sub_total=cal_sub_total,
                                          cal_discount_rate=cal_transaction_discount_rate,
                                          cal_item_type=time_discount_item_type,
                                          isPreTax=is_pre_tax,
                                          src_timeDiscountAmount=time_discount_amount,
                                          src_taxRate1=time_tax_rate,
                                          updated_at=updated_at
                                          ))

            # section 9 shippingCost
            if 'shippingCost' in record:
                ship_method = record['shipMethod']
                shipMethod_internalId = ship_method['internalId']
                shipMethod_name = ship_method['name']
                description = shipMethod_name
                shipping_cost = float(record.get('shippingCost'))

                # TODO shipping tax code may exists in ShipGroup!!
                shipping_tax_code = record['shippingTaxCode']['name'] if 'shippingTaxCode' in record else None  #

                shipping_tax_rate = record.get('shippingTax1Rate')
                tgt_quantity = 1
                tgt_unit_price = shipping_cost
                tgt_discount = 0
                tgt_net_total = shipping_cost
                tgt_invoice_net_total = tgt_invoice_net_total + tgt_net_total

                if shipping_tax_rate:
                    tgt_tax_amount = shipping_cost * float(shipping_tax_rate) * 0.01
                else:
                    tgt_tax_amount = 0
                tgt_total = tgt_net_total + tgt_tax_amount - tgt_discount
                if tgt_tax_amount == 0.0:
                    tgt_invoice_tax_exempt_total = tgt_invoice_tax_exempt_total + tgt_total

                invoice_lines.append(dict(invoice_id=invoice_id,
                                          invoice_number=invoice_number,
                                          src_internalId=shipMethod_internalId,
                                          tgt_description=description,
                                          tgt_item_name=None,
                                          tgt_quantity=tgt_quantity,
                                          tgt_discount=tgt_discount,
                                          tgt_total=round(tgt_total, 2),
                                          tgt_unit_price=tgt_unit_price,
                                          tgt_net_total=tgt_net_total,
                                          tgt_tax_code=shipping_tax_code,
                                          tgt_tax_amount=round(tgt_tax_amount, 2),
                                          cal_sub_total=None,
                                          cal_discount_rate=None,
                                          cal_item_type='ShipItem',
                                          isPreTax=is_pre_tax,
                                          src_shippingCost=shipping_cost,
                                          src_shippingTax1Rate=shipping_tax_rate,
                                          updated_at=updated_at

                                          ))

            # section 10 handlingCost
            if 'handlingCost' in record:
                description = 'handling cost'
                handling_cost = float(record.get('handlingCost'))
                handling_tax_code = record['handlingTaxCode']['name'] if 'handlingTaxCode' in record else None
                handling_tax_rate = record.get('handlingTax1Rate')
                tgt_quantity = 1
                tgt_unit_price = handling_cost
                tgt_discount = 0
                tgt_net_total = handling_cost
                tgt_invoice_net_total = tgt_invoice_net_total + tgt_net_total

                if handling_tax_rate:
                    tgt_tax_amount = handling_cost * float(handling_tax_rate) * 0.01
                else:
                    tgt_tax_amount = 0
                tgt_total = tgt_net_total + tgt_tax_amount - tgt_discount
                if tgt_tax_amount == 0:
                    tgt_invoice_tax_exempt_total = tgt_invoice_tax_exempt_total + tgt_total

                invoice_lines.append(dict(invoice_id=invoice_id,
                                          invoice_number=invoice_number,
                                          src_internalId=None,
                                          tgt_description=description,
                                          tgt_item_name=None,
                                          tgt_quantity=tgt_quantity,
                                          tgt_discount=tgt_discount,
                                          tgt_total=round(tgt_total, 2),
                                          tgt_unit_price=tgt_unit_price,
                                          tgt_net_total=tgt_net_total,
                                          tgt_tax_code=handling_tax_code,
                                          tgt_tax_amount=round(tgt_tax_amount, 2),
                                          cal_sub_total=None,
                                          cal_discount_rate=None,
                                          cal_item_type=None,
                                          isPreTax=is_pre_tax,
                                          src_handlingCost=handling_cost,
                                          src_handlingTax1Rate=handling_tax_rate,
                                          updated_at=updated_at))

            # section 11 redemptionList
            if 'giftCertRedemptionList' in record:
                redemption_list = record['giftCertRedemptionList']['giftCertRedemption']
                for redemption in redemption_list:
                    name = redemption['authCode']['name']
                    description = name
                    applied_amount = float(redemption.get('authCodeApplied'))
                    cal_redemption_total = cal_redemption_total + applied_amount
                    tgt_quantity = 1
                    tgt_unit_price = applied_amount
                    tgt_discount = 0
                    tgt_net_total = applied_amount
                    tgt_tax_amount = 0
                    tgt_total = tgt_net_total + tgt_tax_amount - tgt_discount

                    invoice_lines.append(dict(invoice_id=invoice_id,
                                              invoice_number=invoice_number,
                                              src_internalId=None,
                                              tgt_description=description,
                                              tgt_quantity=tgt_quantity,
                                              tgt_discount=tgt_discount,
                                              tgt_total=round(tgt_total, 2),
                                              tgt_unit_price=tgt_unit_price,
                                              tgt_net_total=tgt_net_total,
                                              tgt_tax_code=None,
                                              tgt_tax_amount=round(tgt_tax_amount, 2),
                                              cal_sub_total=None,
                                              cal_discount_rate=None,
                                              cal_item_type=None,
                                              isPreTax=is_pre_tax,
                                              src_amount=applied_amount,
                                              updated_at=updated_at))

            tgt_invoice_discount_total = -1 * discount_total + cal_sum_discount
            # sum of line.net_total - sum(line.discountTotal) + sum(line.

            tgt_invoice_due_total = tgt_invoice_net_total - tgt_invoice_discount_total + tgt_invoice_tax_total

            df_tmp_1 = df_customer_payment_apply[df_customer_payment_apply['doc'] == int(invoice_id)][[
                'amount']]
            cal_customer_payment_apply = df_tmp_1.sum().values[0]

            cal_credit_memo_apply = df_credit_memo_apply[df_credit_memo_apply['doc'] == int(invoice_id)][[
                'amount']].sum().values[0]

            # print('customer payment {} credit memo apply {}'.format(cal_customer_payment_apply, cal_credit_memo_apply))
            tgt_invoice_outstanding_total = tgt_invoice_due_total + cal_redemption_total - cal_credit_memo_apply - cal_customer_payment_apply
            tgt_invoice_apply_tax_after_discount = is_pre_tax
            invoice.append(dict(id=invoice_id,
                                invoice_number=invoice_number,
                                amount=amount,
                                date=date,
                                dueDate=due_date,
                                terms=terms_name,
                                tgt_due_date=tgt_due_date,
                                currency_internal_id=currency_internal_id,
                                currency=currency,
                                currency_rate=currency_rate,
                                tgt_net_total=tgt_invoice_net_total,
                                tgt_tax_total=tgt_invoice_tax_total,
                                tgt_due_total=tgt_invoice_due_total,
                                tgt_discount_total=tgt_invoice_discount_total,
                                tgt_outstanding_total=round(tgt_invoice_outstanding_total, 2),
                                tgt_tax_exempt_total=tgt_invoice_tax_exempt_total,
                                tgt_apply_tax_after_discount=tgt_invoice_apply_tax_after_discount,
                                src_discountTotal=discount_total,
                                billing_name=billing_name,
                                billing_address_line_1=billing_address_line_1,
                                billing_address_line_2=billing_address_line_2,
                                billing_city=billing_city,
                                billing_region=billing_region,
                                billing_postcode=billing_postcode,
                                billing_country=billing_country,
                                shipping_name=shipping_name,
                                shipping_address_line_1=shipping_address_line_1,
                                shipping_address_line_2=shipping_address_line_2,
                                shipping_city=shipping_city,
                                shipping_region=shipping_region,
                                shipping_postcode=shipping_postcode,
                                shipping_country=shipping_country,
                                src_contact=contact_name,
                                tgt_contact=lookup_contact_name,
                                created_at=created_at,
                                updated_at=updated_at
                                ))

        df_invoice = pd.DataFrame(invoice)
        df_invoice_line = pd.DataFrame(invoice_lines)
        raw_csv_path = get_csv_path() + '/' + ep.INVOICE + '_' + str(self.batch_id) + '_raw.csv'
        line_csv_path = os.path.join(get_csv_path(), 'invoice_line' + '_' + str(self.batch_id) + '_raw.csv')

        df_invoice.to_csv(raw_csv_path)

        df_invoice_line.to_csv(line_csv_path)

    def convert_invoice_to_csv_v2(self):

        #     'internalId', 'type', 'createdDate', 'lastModifiedDate'
        #     'entity'
        #
        # 'tranDate' 'tranId' 'createdFrom' 'postingPeriod' 'location' 'subsidiary'
        # 'currency' 'dueDate' 'salesRep' 'totalCostEstimate' 'estGrossProfit'
        # 'estGrossProfitPercent' 'account' 'exchangeRate' 'currencyName'
        # 'toBePrinted' 'toBeEmailed' 'toBeFaxed' 'billingAddress'
        # 'billAddressList' 'shippingAddress' 'shipIsResidential' 'subTotal'
        # 'canHaveStackable' 'taxTotal' 'total' 'status' 'email'
        # 'itemCostDiscPrint' 'expCostDiscPrint' 'timeDiscPrint' 'itemList'
        # 'customFieldList' 'itemCostList' 'memo' 'fax' 'terms' 'startDate'
        # 'endDate' 'salesEffectiveDate' 'discountItem' 'discountRate' 'shipDate'
        # 'discountTotal' 'shipGroupList' 'shipAddressList' 'shipMethod'
        # 'shippingCost' 'shippingTax1Rate' 'shippingTaxCode' 'handlingTaxCode'
        # 'handlingTax1Rate' 'handlingCost' 'altShippingCost' 'altHandlingCost'
        # 'expCostDiscount' 'expCostDiscRate' 'expCostDiscAmount' 'expCostTaxRate1'
        # 'expCostTaxCode' 'expCostDiscTax1Amt' 'expCostList' 'giftCertApplied'
        # 'giftCertRedemptionList' 'leadSource' 'itemCostDiscount'
        # 'itemCostDiscRate' 'itemCostDiscAmount' 'itemCostTaxRate1'
        # 'itemCostTaxCode' 'itemCostDiscTax1Amt' 'timeDiscount' 'timeDiscRate'
        # 'timeDiscAmount' 'timeTaxRate1' 'timeTaxCode' 'timeDiscTax1Amt'
        # 'timeList'
        records = self.read_json_files(ep.INVOICE, self.batch_id)
        df_raw_invoice = pd.DataFrame.from_dict(records,
                                                columns=['internalId', 'type', 'createdDate', 'lastModifiedDate',
                                                         'tranDate', 'tranId', 'currency', 'dueDate', 'exchangeRate',
                                                         'billingAddress',
                                                         'billAddressList', 'shippingAddress', 'subTotal',
                                                         'canHaveStackable', 'taxTotal', 'total', 'status', 'terms',
                                                         'discountItem', 'discountRate',
                                                         'discountTotal', 'shipAddressList'])
        print(df_raw_invoice.columns.values)

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
                file_name_list.append('expense_items_1.json')
                # file_name_list.append('ship_items.json')
            items = []
            for filename in file_name_list:
                with open(os.path.join(file_path, filename)) as src:
                    items.extend(json.loads(src.read()))
        return items


if __name__ == '__main__':
    testing = NetsuiteClient()

    # testing.convert_currency_to_csv()
    # testing.convert_item_to_csv()
    # testing.convert_customer_to_csv()
    # testing.convert_vendor_bill_to_csv()
    # testing.convert_time_bill_to_csv()
    testing.convert_credit_memo_to_csv()
    # testing.convert_credit_memo_apply_to_csv()
    # testing.convert_customer_payment_apply_to_csv()
    # testing.convert_invoice_to_csv()
