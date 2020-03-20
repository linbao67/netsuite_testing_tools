import os

import pandas as pd
from datetime import datetime

import pytz

import main.netsuite.endpoints as ep
from main.path import get_csv_path
from main.util.config_util import get_property


class NetsuiteTestingClient(object):

    def __init__(self):
        self.batch_id = get_property('parameters', 'BATCH_ID')

    def generate_invoice_testing_report(self):
        df_pubsub = pd.read_csv(os.path.join(get_csv_path('pubsub'), ep.INVOICE + '.csv'))
        df_source = pd.read_csv(os.path.join(get_csv_path(), ep.INVOICE + '_' + str(self.batch_id) + '_raw.csv'))
        df_source = df_source[pd.to_datetime(df_source.updated_at) < datetime.utcnow()]

        count_pubsub = df_pubsub.iloc[:, 0].size
        count_source = df_source.iloc[:, 0].size

        if count_pubsub != count_source:
            print(color.BOLD + color.RED +
                'The number of invoices are different, {} from pubsub {} from source'.format(count_pubsub,
                                                                                             count_source))
            print('\033[0m')
        else:
            print("Source and Pubsub count are the same.")

        df_tmp = df_pubsub[df_pubsub['attributes_invoice_number'].duplicated()]['attributes_invoice_number']
        r = df_tmp.shape[0]
        if r != 0:
            print(color.BOLD + 'Invoice in pubsub have duplicated values:' + color.RED)
            print("Totally got {} invoices".format(df_tmp.shape[0]))
            print(df_tmp.values)
            print('\033[0m')



        df_tmp = df_pubsub[~df_pubsub['attributes_invoice_number'].isin(df_source['invoice_number'])][
                  'attributes_invoice_number']
        r = df_tmp.shape[0]
        if r != 0:
            print(color.BOLD + 'Invoice CAN NOT FOUND in source:' + color.RED)
            print("Totally got {} invoices".format(df_tmp.shape[0]))
            print(df_tmp.values)
            print('\033[0m')



        df_tmp = df_source[~df_source['invoice_number'].isin(df_pubsub['attributes_invoice_number'])][
                  'invoice_number']
        r = df_tmp.shape[0]
        if r != 0:
            print(color.BOLD + 'Invoice are missing in pubsub:' + color.RED)
            print("Totally got {} invoices".format(df_tmp.shape[0]))
            print(df_tmp.values)
            print('\033[0m')
        else:
            print("No Invoice missing in pubsub")

        df_merge = pd.merge(df_pubsub, df_source, how='left', left_on=['attributes_invoice_number'],
                            right_on=['invoice_number'])

        df_tmp = df_merge[df_merge['attributes_net_total'].round(2) !=
                          df_merge['tgt_net_total'].round(2)][['attributes_invoice_number', 'attributes_net_total', 'tgt_net_total']]
        r = df_tmp.shape[0]
        if r != 0:
            print(color.BOLD + 'attributes_net_total failed' + color.RED)
            print("Totally got {} invoices".format(df_tmp.shape[0]))
            print(df_tmp.values)
            print('\033[0m')
        else:
            print("attributes_net_total passed")


        df_tax_total = df_merge[df_merge['attributes_tax_total'].round(2) != df_merge['tgt_tax_total'].round(2)][
            ['attributes_invoice_number', 'attributes_tax_total', 'tgt_tax_total']]
        r = df_tax_total.shape[0]
        if r != 0:
            print(color.BOLD + 'attributes_tax_total failed' + color.RED)
            print("Totally got {} invoices".format(df_tax_total.shape[0]))
            print(df_tax_total.values)
            print('\033[0m')
        else:
            print("attributes_tax_total passed")


        df_due_total = df_merge[(df_merge['attributes_due_total'].round(2) != df_merge['tgt_due_total'].round(2))][
            ['attributes_invoice_number', 'attributes_due_total', 'tgt_due_total']]
        r = df_due_total.shape[0]
        if r != 0:
            print(color.BOLD + 'attributes_due_total failed' + color.RED)
            print("Totally got {} invoices".format(df_due_total.shape[0]))
            print(df_due_total.values)
            df_tmp = df_due_total[
                ~df_due_total['attributes_invoice_number'].isin(df_tax_total['attributes_invoice_number'])]
            print("Due Total Error are not caused by tax total")
            print(df_tmp.values)
            print('\033[0m')
        else:
            print("attributes_due_total passed")

        df_tmp = df_merge[(df_merge[
                    'attributes_apply_tax_after_discount'].fillna(value=False) != df_merge[
            'tgt_apply_tax_after_discount'].fillna(value=False))]['attributes_invoice_number']

        r = df_tmp.shape[0]
        if r != 0:
            print(color.BOLD + 'attributes_apply_tax_after_discount failed' + color.RED)
            print("Totally got {} invoices".format(df_tmp.shape[0]))
            print(df_tmp.values)
            print('\033[0m')
        else:
            print("attributes_apply_tax_after_discount passed")


        df_tmp = df_merge[(df_merge[
                    'attributes_discount_total'].round(2) != df_merge['tgt_discount_total'].round(2))][
                  'attributes_invoice_number']

        r = df_tmp.shape[0]
        if r != 0:
            print(color.BOLD + 'attributes_discount_total failed' + color.RED)
            print("Totally got {} invoices".format(df_tmp.shape[0]))
            print(df_tmp.values)
            print('\033[0m')
        else:
            print("attributes_discount_total passed")


        df_tmp = df_merge[(
                df_merge[
                    'attributes_outstanding_total'].round(2) != df_merge['tgt_outstanding_total'].round(2))][
                  'attributes_invoice_number']

        r = df_tmp.shape[0]
        if r != 0:
            print(color.BOLD + 'attributes_outstanding_total failed' + color.RED)
            print("Totally got {} invoices".format(df_tmp.shape[0]))
            print(df_tmp.values)
            print('\033[0m')
        else:
            print("attributes_outstanding_total passed")


        df_tmp = df_merge[(df_merge['attributes_tax_exempt_total'].round(2)
                           != df_merge['tgt_tax_exempt_total'].round(2))]['attributes_invoice_number']

        r = df_tmp.shape[0]
        if r != 0:
            print(color.BOLD + 'attributes_tax_exempt_total failed' + color.RED)
            print("Totally got {} invoices".format(df_tmp.shape[0]))
            print(df_tmp.values)
            print('\033[0m')
        else:
            print("attributes_tax_exempt_total passed")


        df_tmp = df_merge[(df_merge['attributes_due_date'] != df_merge['tgt_due_date'])][['attributes_invoice_number',
                                                                                       'attributes_due_date',
                                                                                       'tgt_due_date']]

        r = df_tmp.shape[0]
        if r != 0:
            print(color.BOLD + 'attributes_due_date failed' + color.RED)
            print("Totally got {} invoices".format(df_tmp.shape[0]))
            print(df_tmp.values)
            print('\033[0m')
        else:
            print("attributes_due_date passed")


        df_tmp = df_merge[(df_merge['attributes_created_at']
                           != df_merge['created_at'])][['attributes_invoice_number','attributes_created_at','created_at']]

        r = df_tmp.shape[0]
        if r != 0:
            print(color.BOLD + 'attributes_created_at failed' + color.RED)
            print("Totally got {} invoices".format(df_tmp.shape[0]))
            print(df_tmp.values)
            print('\033[0m')
        else:
            print("attributes_created_at passed")



        df_tmp = df_merge[(df_merge['attributes_updated_at'] != df_merge['updated_at'])]
        [['attributes_invoice_number','attributes_updated_at','updated_at']]

        r = df_tmp.shape[0]
        if r != 0:
            print(color.BOLD + 'attributes_updated_at failed' + color.RED)
            print("Totally got {} invoices".format(df_tmp.shape[0]))
            print(df_tmp.values)
            print('\033[0m')
        else:
            print("attributes_updated_at passed")


        df_tmp = df_merge[(df_merge['attributes_currency'] != df_merge['currency'])]['attributes_invoice_number']

        r = df_tmp.shape[0]
        if r != 0:
            print(color.BOLD+'attributes_currency failed'+color.RED)
            print("Totally got {} invoices".format(df_tmp.shape[0]))
            print(df_tmp.values)
            print('\033[0m')
        else:
            print("attributes_currency passed")



        df_tmp = df_merge[(df_merge['attributes_billing_address_name'].fillna(value='Unknown')
                        != df_merge['billing_name'].fillna(value='Unknown'))]['attributes_invoice_number']

        if df_tmp.shape[0] != 0:
            print(color.BOLD+'attributes_billing_address_name failed'+color.RED)
            print("Totally got {} invoices".format(df_tmp.shape[0]))
            print(df_tmp.values)
            print('\033[0m')
        else:
            print("attributes_billing_address_name passed")


        df_tmp = df_merge[(df_merge['attributes_billing_address_city'].fillna(value='Unknown')
                        != df_merge['billing_city'].fillna(value='Unknown'))]['attributes_invoice_number']
        if df_tmp.shape[0] != 0:
            print('attributes_billing_address_city failed'+color.RED)
            print("Totally got {} invoices".format(df_tmp.shape[0]))
            print(df_tmp.values)
            print('\033[0m')
        else:
            print("attributes_billing_address_city passed")


        df_tmp = df_merge[(df_merge['attributes_billing_address_country'].fillna(value='Unknown')
                        != df_merge['billing_country'].fillna(value='Unknown'))]['attributes_invoice_number']

        r = df_tmp.shape[0]
        if r != 0:
            print('attributes_billing_address_country failed'+color.RED)
            print("Totally got {} invoices".format(df_tmp.shape[0]))
            print(df_tmp.values)
            print('\033[0m')
        else:
            print("attributes_billing_address_country passed")

        df_tmp = df_merge[(df_merge['attributes_billing_address_address_line_1'].fillna(value='Unknown')
                           != df_merge['billing_address_line_1'].fillna(value='Unknown'))]['attributes_invoice_number']



        r = df_tmp.shape[0]
        if r != 0:
            print('attributes_billing_address_address_line_1 failed'+color.RED)
            print("Totally got {} invoices".format(df_tmp.shape[0]))
            print(df_tmp.values)
            print('\033[0m')
        else:
            print("attributes_billing_address_address_line_1 passed")

        df_tmp = df_merge[(df_merge['attributes_billing_address_address_line_2'].fillna(value='Unknown')
                        != df_merge['billing_address_line_2'].fillna(value='Unknown'))]['attributes_invoice_number']

        r = df_tmp.shape[0]
        if r != 0:
            print('attributes_billing_address_address_line_2 failed'+color.RED)
            print("Totally got {} invoices".format(df_tmp.shape[0]))
            print(df_tmp.values)
            print('\033[0m')
        else:
            print("attributes_billing_address_address_line_2 passed")

        df_tmp = df_merge[pd.notnull(df_merge['attributes_billing_address_address_line_3'])]
        r = df_tmp.shape[0]
        if r != 0:
            print('attributes_billing_address_address_line_3 failed'+color.RED)
            print("Totally got {} invoices".format(df_tmp.shape[0]))
            print(df_tmp.values)
            print('\033[0m')
        else:
            print("attributes_billing_address_address_line_3 passed")

        df_tmp = df_merge[(df_merge['attributes_billing_address_region'].fillna(value='Unknown')
                        != df_merge['billing_region'].fillna(value='Unknown'))]['attributes_invoice_number']
        r = df_tmp.shape[0]
        if r != 0:
            print('attributes_billing_address_region failed'+color.RED)
            print("Totally got {} invoices".format(df_tmp.shape[0]))
            print(df_tmp.values)
            print('\033[0m')
        else:
            print("attributes_billing_address_region passed")

        df_tmp = df_merge[(df_merge['attributes_billing_address_postcode'].fillna(value='Unknown')
                        != df_merge['billing_postcode'].fillna(value='Unknown'))]['attributes_invoice_number']
        r = df_tmp.shape[0]
        if r != 0:
            print('attributes_billing_address_postcode failed'+color.RED)
            print("Totally got {} invoices".format(df_tmp.shape[0]))
            print(df_tmp.values)
            print('\033[0m')
        else:
            print("attributes_billing_address_postcode passed")

        df_tmp = df_merge[(df_merge['attributes_shipping_address_name'].fillna(value='Unknown')
                           != df_merge['shipping_name'].fillna(value='Unknown'))]['attributes_invoice_number']

        r = df_tmp.shape[0]
        if r != 0:
            print('attributes_shipping_address_name failed'+color.RED)
            print("Totally got {} invoices".format(df_tmp.shape[0]))
            print(df_tmp.values)
            print('\033[0m')
        else:
            print("attributes_shipping_address_name passed")

        df_tmp = df_merge[(df_merge['attributes_shipping_address_city'].fillna(value='Unknown')
                           != df_merge['shipping_city'].fillna(value='Unknown'))]['attributes_invoice_number']
        r = df_tmp.shape[0]
        if r != 0:
            print('attributes_shipping_address_city failed'+color.RED)
            print("Totally got {} invoices".format(df_tmp.shape[0]))
            print(df_tmp.values)
            print('\033[0m')
        else:
            print("attributes_shipping_address_city passed")

        df_tmp = df_merge[(df_merge['attributes_shipping_address_country'].fillna(value='Unknown')
                           != df_merge['shipping_country'].fillna(value='Unknown'))]['attributes_invoice_number']

        r = df_tmp.shape[0]
        if r != 0:
            print('attributes_shipping_address_country failed'+color.RED)
            print("Totally got {} invoices".format(df_tmp.shape[0]))
            print(df_tmp.values)
            print('\033[0m')
        else:
            print("attributes_shipping_address_country passed")

        df_tmp = df_merge[(df_merge['attributes_shipping_address_address_line_1'].fillna(value='Unknown')
                           != df_merge['shipping_address_line_1'].fillna(value='Unknown'))]['attributes_invoice_number']

        r = df_tmp.shape[0]
        if r != 0:
            print('attributes_shipping_address_address_line_1 failed'+color.RED)
            print("Totally got {} invoices".format(df_tmp.shape[0]))
            print(df_tmp.values)
            print('\033[0m')
        else:
            print("attributes_shipping_address_address_line_1 passed")

        df_tmp = df_merge[(df_merge['attributes_shipping_address_address_line_2'].fillna(value='Unknown')
                           != df_merge['shipping_address_line_2'].fillna(value='Unknown'))]['attributes_invoice_number']

        r = df_tmp.shape[0]
        if r != 0:
            print(color.RED+'attributes_shipping_address_address_line_2 failed'+color.RED)
            print("Totally got {} invoices".format(df_tmp.shape[0]))
            print(df_tmp.values)
            print('\033[0m')
        else:
            print("attributes_shipping_address_address_line_2 passed")

        df_tmp = df_merge[pd.notnull(df_merge['attributes_shipping_address_address_line_3'])]
        r = df_tmp.shape[0]
        if r != 0:
            print('attributes_shipping_address_address_line_3 failed'+color.RED)
            print("Totally got {} invoices".format(df_tmp.shape[0]))
            print(df_tmp.values)
            print('\033[0m')
        else:
            print("attributes_shipping_address_address_line_3 passed")

        df_tmp = df_merge[(df_merge['attributes_shipping_address_region'].fillna(value='Unknown')
                           != df_merge['shipping_region'].fillna(value='Unknown'))]['attributes_invoice_number']
        r = df_tmp.shape[0]
        if r != 0:
            print('attributes_shipping_address_region failed'+color.RED)
            print("Totally got {} invoices".format(df_tmp.shape[0]))
            print(df_tmp.values)
            print('\033[0m')
        else:
            print("attributes_shipping_address_region passed")

        df_tmp = df_merge[(df_merge['attributes_shipping_address_postcode'].fillna(value='Unknown')
                           != df_merge['shipping_postcode'].fillna(value='Unknown'))]['attributes_invoice_number']
        r = df_tmp.shape[0]
        if r != 0:
            print('attributes_shipping_address_postcode failed'+color.RED)
            print("Totally got {} invoices".format(df_tmp.shape[0]))
            print(df_tmp.values)
            print('\033[0m')
        else:
            print("attributes_shipping_address_postcode passed")

    def generate_invoice_line_testing_report(self):
        df_pubsub = pd.read_csv(os.path.join(get_csv_path('pubsub'), 'invoice_line.csv'))
        df_source = pd.read_csv(os.path.join(get_csv_path(), 'invoice_line_' + str(self.batch_id) + '_raw.csv'))
        df_source = df_source[pd.to_datetime(df_source.updated_at) < datetime.utcnow()]

        count_pubsub = df_pubsub.iloc[:, 0].size
        count_source = df_source.iloc[:, 0].size

        if count_pubsub != count_source:
            print( color.BOLD + color.RED +
                'The records number are not matched, {} from pubsub {} from source'.format(count_pubsub,
                                                                                            count_source))

        # print(df_pubsub[df_pubsub[['relationships_invoice_number', 'attributes_description']].duplicated()]
        #       [['relationships_invoice_number', 'description']])

        df_merge_source_missing = pd.merge(df_pubsub, df_source, how='left', left_on=['relationships_invoice_number',
                                                                        'attributes_description'],
                            right_on=['invoice_number', 'tgt_description'])

        df_tmp = df_merge_source_missing[pd.isnull(df_merge_source_missing['invoice_number'])][[
            'relationships_invoice_number','attributes_description']]

        r = df_tmp.shape[0]
        if r != 0:
            print(color.BOLD + 'Invoice Line are not found in Source:'+ color.RED )
            print("Totally got {} invoice lines".format(df_tmp.shape[0]))
            print(df_tmp.values)
            print('\033[0m')

        df_merge_pubsub_missing = pd.merge(df_pubsub, df_source, how='right', left_on=['relationships_invoice_number',
                                                                                      'attributes_description'],
                                           right_on=['invoice_number', 'tgt_description'])

        df_tmp = df_merge_pubsub_missing[pd.isnull(df_merge_pubsub_missing['relationships_invoice_number'])][['invoice_number', 'tgt_description']]
        r = df_tmp.shape[0]
        if r != 0:
            print(color.BOLD + 'Invoice line are missing in pubsub:'+ color.RED )
            print("Totally got {} invoice lines".format(df_tmp.shape[0]))
            print(df_tmp.values)
            print('\033[0m')

        df_merge = pd.merge(df_pubsub, df_source, how='inner', left_on=['relationships_invoice_number',
                                                                        'attributes_description'],
                            right_on=['invoice_number', 'tgt_description'])


        print('net total failed'+color.RED)
        print(df_merge[df_merge['attributes_net_total'].round(2) != df_merge['tgt_net_total'].round(2)]
              [['relationships_invoice_number', 'attributes_description', 'attributes_net_total',
                'tgt_net_total']].values)

        df_tmp = df_merge[df_merge['attributes_net_total'].round(2)
                          != df_merge['tgt_net_total'].round(2)][['relationships_invoice_number',
                        'attributes_description', 'attributes_net_total', 'tgt_net_total']]

        r = df_tmp.shape[0]
        if r != 0:
            print(color.BOLD + color.RED + 'attributes_net_total failed:')
            print("Totally got {} invoice lines".format(df_tmp.shape[0]))
            print(df_tmp.values)
            print('\033[0m')
        else:
            print("attributes_net_total passed")



        df_tmp = df_merge[df_merge['attributes_total'].round(2) != df_merge['tgt_total'].round(2)][
            ['relationships_invoice_number', 'attributes_description', 'attributes_total', 'tgt_total']]
        r = df_tmp.shape[0]
        if r != 0:
            print(color.BOLD + color.RED + 'attributes_total failed:')
            print("Totally got {} invoice lines".format(df_tmp.shape[0]))
            print(df_tmp.values)
            print('\033[0m')
        else:
            print("attributes_total passed")


        df_tmp = df_merge[(df_merge['attributes_quantity'] != df_merge['tgt_quantity'])][
            ['relationships_invoice_number', 'attributes_description', 'attributes_quantity', 'tgt_quantity']]

        r = df_tmp.shape[0]
        if r != 0:
            print(color.BOLD + 'attributes_quantity failed:'+ color.RED )
            print("Totally got {} invoice lines".format(df_tmp.shape[0]))
            print(df_tmp.values)
            print('\033[0m')
        else:
            print("attributes_quantity passed")



        df_tmp = df_merge[(df_merge['attributes_discount'].round(2) != df_merge['tgt_discount'].round(2))][[
            'relationships_invoice_number', 'attributes_description', 'attributes_discount',
            'tgt_discount']]

        r = df_tmp.shape[0]
        if r != 0:
            print(color.BOLD + 'attributes_discount failed:' + color.RED)
            print("Totally got {} invoice lines".format(df_tmp.shape[0]))
            print(df_tmp.values)
            print('\033[0m')
        else:
            print("attributes_discount passed")


        df_tmp = df_merge[(df_merge['attributes_unit_price'].round(2) != df_merge['tgt_unit_price'].round(2))][[
            'relationships_invoice_number', 'attributes_description', 'attributes_unit_price',
                'tgt_unit_price']]

        r = df_tmp.shape[0]
        if r != 0:
            print(color.BOLD + color.RED + 'attributes_unit_price failed:')
            print("Totally got {} invoice lines".format(df_tmp.shape[0]))
            print(df_tmp.values)
            print('\033[0m')
        else:
            print("attributes_unit_price passed")


        df_tmp = df_merge[(df_merge['attributes_tax_code'].fillna(value='Unknown') != df_merge['tgt_tax_code'].fillna(
                      value='Unknown'))][['relationships_invoice_number', 'attributes_description', 'attributes_tax_code',
                'tgt_tax_code']]

        r = df_tmp.shape[0]
        if r != 0:
            print(color.BOLD + color.RED + 'attributes_tax_code failed:')
            print("Totally got {} invoice lines".format(df_tmp.shape[0]))
            print(df_tmp.values)
            print('\033[0m')
        else:
            print("attributes_tax_code passed")

        df_tmp = df_merge[(df_merge['attributes_tax_amount'].round(2) != df_merge['tgt_tax_amount'].round(2))][[
            'relationships_invoice_number', 'attributes_description',
                'attributes_tax_amount',
                'tgt_tax_amount']]

        r = df_tmp.shape[0]
        if r != 0:
            print(color.BOLD + color.RED + 'attributes_tax_amount failed:')
            print("Totally got {} invoice lines".format(df_tmp.shape[0]))
            print(df_tmp.values)
            print('\033[0m')
        else:
            print("attributes_tax_amount passed")

    def generate_credit_memo_testing_report(self):
        df_pubsub = pd.read_csv(os.path.join(get_csv_path('pubsub'),ep.CREDIT_MEMO + '_' + str(self.batch_id) +
                                             '.csv'))
        df_source = pd.read_csv(os.path.join(get_csv_path(), ep.CREDIT_MEMO + '_' + str(self.batch_id) +
                                             '_raw.csv'))
        df_source = df_source[pd.to_datetime(df_source.lastModifiedDate) < datetime.utcnow()]

        count_pubsub = df_pubsub.iloc[:, 0].size
        count_source = df_source.iloc[:, 0].size

        if count_pubsub != count_source:
            print(color.BOLD + color.RED +
                  'The number of credit notes are different, {} from pubsub {} from source'.format(count_pubsub,
                                                                                                   count_source))
            print('\033[0m')
        else:
            print("Source and Pubsub count are the same.")

        df_tmp = df_pubsub[df_pubsub['attributes_credit_note_number'].duplicated()]['attributes_credit_note_number']
        r = df_tmp.shape[0]
        if r != 0:
            print(color.BOLD + 'Credit note in pubsub have duplicated values:' + color.RED)
            print("Totally got {} credit notes".format(df_tmp.shape[0]))
            print(df_tmp.values)
            print('\033[0m')

        df_tmp = df_pubsub[~df_pubsub['attributes_credit_note_number'].isin(df_source['tranId'])][
            'attributes_credit_note_number']
        r = df_tmp.shape[0]
        if r != 0:
            print(color.BOLD + 'Credit note CAN NOT FOUND in source:' + color.RED)
            print("Totally got {} credit notes".format(df_tmp.shape[0]))
            print(df_tmp.values)
            print('\033[0m')

        df_tmp = df_source[~df_source['tranId'].isin(df_pubsub['attributes_credit_note_number'])][
            'tranId']
        r = df_tmp.shape[0]
        if r != 0:
            print(color.BOLD + 'Credit Notes are missing in pubsub:' + color.RED)
            print("Totally got {} credit notes".format(df_tmp.shape[0]))
            print(df_tmp.values)
            print('\033[0m')
        else:
            print("No credit note missing in pubsub")

        df_merge = pd.merge(df_pubsub, df_source, how='left', left_on=['attributes_credit_note_number'],
                            right_on=['tranId'])

        df_tmp = df_merge[df_merge['attributes_amount'].round(2) !=
                          df_merge['total'].round(2)][
            ['attributes_credit_note_number', 'attributes_amount', 'total']]
        r = df_tmp.shape[0]
        if r != 0:
            print(color.BOLD + 'attributes_amount failed' + color.RED)
            print("Totally got {} credit notes".format(df_tmp.shape[0]))
            print(df_tmp.values)
            print('\033[0m')
        else:
            print("attributes_amount passed")

        df_tmp = df_merge[(df_merge[
                               'attributes_currency_rate'].fillna(value=False) != df_merge[
                               'exchangeRate'].fillna(value=False))]['attributes_credit_note_number']

        r = df_tmp.shape[0]
        if r != 0:
            print(color.BOLD + 'attributes_currency_rate failed' + color.RED)
            print("Totally got {} credit notes".format(df_tmp.shape[0]))
            print(df_tmp.values)
            print('\033[0m')
        else:
            print("attributes_currency_rate passed")

        df_tmp = df_merge[(df_merge['attributes_date'] != df_merge['tranDate'])][
            ['attributes_credit_note_number',
             'attributes_date',
             'tranDate']]

        r = df_tmp.shape[0]
        if r != 0:
            print(color.BOLD + 'attributes_due_date failed' + color.RED)
            print("Totally got {} credit notes".format(df_tmp.shape[0]))
            print(df_tmp.values)
            print('\033[0m')
        else:
            print("attributes_due_date passed")

        df_tmp = df_merge[(df_merge['attributes_created_at']
                           != df_merge['createdDate'])][
            ['attributes_credit_note_number', 'attributes_created_at', 'createdDate']]

        r = df_tmp.shape[0]
        if r != 0:
            print(color.BOLD + 'attributes_created_at failed' + color.RED)
            print("Totally got {} credit notes".format(df_tmp.shape[0]))
            print(df_tmp.values)
            print('\033[0m')
        else:
            print("attributes_created_at passed")

        df_tmp = df_merge[(df_merge['attributes_updated_at'] != df_merge['lastModifiedDate'])]
        [['attributes_credit_note_number', 'attributes_updated_at', 'updated_at']]

        r = df_tmp.shape[0]
        if r != 0:
            print(color.BOLD + 'attributes_updated_at failed' + color.RED)
            print("Totally got {} credit notes".format(df_tmp.shape[0]))
            print(df_tmp.values)
            print('\033[0m')
        else:
            print("attributes_updated_at passed")

        df_tmp = df_merge[(df_merge['attributes_currency'] != df_merge['currency'])]['attributes_credit_note_number']

        r = df_tmp.shape[0]
        if r != 0:
            print(color.BOLD + 'attributes_currency failed' + color.RED)
            print("Totally got {} credit notes".format(df_tmp.shape[0]))
            print(df_tmp.values)
            print('\033[0m')
        else:
            print("attributes_currency passed")

    def generate_credit_note_line_testing_report(self):
        df_pubsub = pd.read_csv(os.path.join(get_csv_path('pubsub'), ep.CREDIT_MEMO+'_line_' + str(self.batch_id) +
                                             '.csv'))
        df_source = pd.read_csv(os.path.join(get_csv_path(), ep.CREDIT_MEMO + '_line_' + str(self.batch_id) +
                                             '_raw.csv'))
        df_source = df_source[pd.to_datetime(df_source.updated_at) < datetime.utcnow()]

        count_pubsub = df_pubsub.iloc[:, 0].size
        count_source = df_source.iloc[:, 0].size

        if count_pubsub != count_source:
            print(color.BOLD + color.RED +
                  'The records number are not matched, {} from pubsub {} from source'.format(count_pubsub,
                                                                                             count_source))
        else:
            print('The records of credit note line are matched')

        # print(df_pubsub[df_pubsub[['attributes_credit_note_number', 'attributes_description']].duplicated()]
        #       [['attributes_credit_note_number', 'description']])

        df_merge_source_missing = pd.merge(df_pubsub, df_source, how='left',
                                           left_on=['attributes_credit_note_number',
                                                    'attributes_description'],
                                           right_on=['credit_note_number', 'tgt_description'])

        df_tmp = df_merge_source_missing[pd.isnull(df_merge_source_missing['credit_note_number'])][[
            'attributes_credit_note_number', 'attributes_description']]

        r = df_tmp.shape[0]
        if r != 0:
            print(color.BOLD + 'credit note Line are not found in Source:' + color.RED)
            print("Totally got {} credit note lines".format(df_tmp.shape[0]))
            print(df_tmp.values)
            print('\033[0m')

        df_merge_pubsub_missing = pd.merge(df_pubsub, df_source, how='right',
                                           left_on=['attributes_credit_note_number',
                                                    'attributes_description'],
                                           right_on=['credit_note_number', 'tgt_description'])

        df_tmp = df_merge_pubsub_missing[pd.isnull(df_merge_pubsub_missing['attributes_credit_note_number'])][
            ['credit_note_number', 'tgt_description']]
        r = df_tmp.shape[0]
        if r != 0:
            print(color.BOLD + 'credit note line are missing in pubsub:' + color.RED)
            print("Totally got {} credit note lines".format(df_tmp.shape[0]))
            print(df_tmp.values)
            print('\033[0m')

        df_merge = pd.merge(df_pubsub, df_source, how='inner', left_on=['attributes_credit_note_number',
                                                                        'attributes_description'],
                            right_on=['credit_note_number', 'tgt_description'])

        print('net total failed' + color.RED)
        print(df_merge[df_merge['attributes_net_total'].round(2) != df_merge['tgt_net_total'].round(2)]
              [['attributes_credit_note_number', 'attributes_description', 'attributes_net_total',
                'tgt_net_total']].values)

        df_tmp = df_merge[df_merge['attributes_net_total'].round(2)
                          != df_merge['tgt_net_total'].round(2)][['attributes_credit_note_number',
                                                                  'attributes_description', 'attributes_net_total',
                                                                  'tgt_net_total']]

        r = df_tmp.shape[0]
        if r != 0:
            print(color.BOLD + color.RED + 'attributes_net_total failed:')
            print("Totally got {} credit note lines".format(df_tmp.shape[0]))
            print(df_tmp.values)
            print('\033[0m')
        else:
            print("attributes_net_total passed")

        df_tmp = df_merge[df_merge['attributes_total'].round(2) != df_merge['tgt_total'].round(2)][
            ['attributes_credit_note_number', 'attributes_description', 'attributes_total', 'tgt_total']]
        r = df_tmp.shape[0]
        if r != 0:
            print(color.BOLD + color.RED + 'attributes_total failed:')
            print("Totally got {} credit note lines".format(df_tmp.shape[0]))
            print(df_tmp.values)
            print('\033[0m')
        else:
            print("attributes_total passed")

        df_tmp = df_merge[(df_merge['attributes_quantity'] != df_merge['tgt_quantity'])][
            ['attributes_credit_note_number', 'attributes_description', 'attributes_quantity', 'tgt_quantity']]

        r = df_tmp.shape[0]
        if r != 0:
            print(color.BOLD + 'attributes_quantity failed:' + color.RED)
            print("Totally got {} credit note lines".format(df_tmp.shape[0]))
            print(df_tmp.values)
            print('\033[0m')
        else:
            print("attributes_quantity passed")

        df_tmp = df_merge[(df_merge['attributes_discount'].round(2) != df_merge['tgt_discount'].round(2))][[
            'attributes_credit_note_number', 'attributes_description', 'attributes_discount',
            'tgt_discount']]

        r = df_tmp.shape[0]
        if r != 0:
            print(color.BOLD + 'attributes_discount failed:' + color.RED)
            print("Totally got {} credit note lines".format(df_tmp.shape[0]))
            print(df_tmp.values)
            print('\033[0m')
        else:
            print("attributes_discount passed")

        df_tmp = df_merge[(df_merge['attributes_unit_price'].round(2) != df_merge['tgt_unit_price'].round(2))][[
            'attributes_credit_note_number', 'attributes_description', 'attributes_unit_price',
            'tgt_unit_price']]

        r = df_tmp.shape[0]
        if r != 0:
            print(color.BOLD + color.RED + 'attributes_unit_price failed:')
            print("Totally got {} credit note lines".format(df_tmp.shape[0]))
            print(df_tmp.values)
            print('\033[0m')
        else:
            print("attributes_unit_price passed")

        df_tmp = \
        df_merge[(df_merge['attributes_tax_code'].fillna(value='Unknown') != df_merge['tgt_tax_code'].fillna(
            value='Unknown'))][['attributes_credit_note_number', 'attributes_description', 'attributes_tax_code',
                                'tgt_tax_code']]

        r = df_tmp.shape[0]
        if r != 0:
            print(color.BOLD + color.RED + 'attributes_tax_code failed:')
            print("Totally got {} credit note lines".format(df_tmp.shape[0]))
            print(df_tmp.values)
            print('\033[0m')
        else:
            print("attributes_tax_code passed")

        df_tmp = df_merge[(df_merge['attributes_tax_amount'].round(2) != df_merge['tgt_tax_amount'].round(2))][[
            'attributes_credit_note_number', 'attributes_description',
            'attributes_tax_amount',
            'tgt_tax_amount']]

        r = df_tmp.shape[0]
        if r != 0:
            print(color.BOLD + color.RED + 'attributes_tax_amount failed:')
            print("Totally got {} credit note lines".format(df_tmp.shape[0]))
            print(df_tmp.values)
            print('\033[0m')
        else:
            print("attributes_tax_amount passed")


class color:
   PURPLE = '\033[95m'
   CYAN = '\033[96m'
   DARKCYAN = '\033[36m'
   BLUE = '\033[94m'
   GREEN = '\033[92m'
   YELLOW = '\033[93m'
   RED = '\033[91m'
   BOLD = '\033[1m'
   UNDERLINE = '\033[4m'
   END = '\033[0m'

if __name__ == '__main__':
    client = NetsuiteTestingClient()
    client.generate_credit_memo_testing_report()
    client.generate_credit_note_line_testing_report()
    client.generate_invoice_line_testing_report()
    client.generate_invoice_testing_report()