import os

import pandas as pd
import main.netsuite.endpoints as ep

from main.path import get_csv_path
from main.util.config_util import get_property


class NetsuiteTestingClient(object):

    def __init__(self):
        self.batch_id = get_property('parameters', 'BATCH_ID')

    def generate_invoice_testing_report(self):

        df_pubsub = pd.read_csv(os.path.join(get_csv_path('pubsub'), ep.INVOICE + '.csv'))
        df_source = pd.read_csv(os.path.join(get_csv_path(), ep.INVOICE + '_' + str(self.batch_id) + '_raw.csv'))

        count_pubsub = df_pubsub.iloc[:, 0].size
        count_source = df_source.iloc[:, 0].size

        if count_pubsub != count_source:
            print('The records number are not the same, {} from pubsub {} from source'.format(count_pubsub,count_source))


        print('Invoice in pubsub not in source:')
        print(df_pubsub[~df_pubsub['attributes_invoice_number'].isin(df_source['invoice_number'])][
                  'attributes_invoice_number'].values)

        print('Invoice are missing in pubsub:')
        print(df_source[~df_source['invoice_number'].isin(df_pubsub['attributes_invoice_number'])][
                  'invoice_number'].values)

        df_merge = pd.merge(df_pubsub, df_source, how='left', left_on=['attributes_invoice_number'],
                            right_on=['invoice_number'])
        print('net total are not the same')
        print(df_merge[(
                df_merge[
            'attributes_net_total']!=df_merge['tgt_net_total'])]['attributes_invoice_number'].values)

        print('attributes_tax_total are not the same')
        print(df_merge[(
                df_merge[
                    'attributes_tax_total'] != df_merge['tgt_tax_total'])]['attributes_invoice_number'].values)

        print('attributes_due_total are not the same')
        print(df_merge[(
                df_merge[
                    'attributes_due_total'] != df_merge['tgt_due_total'])]['attributes_invoice_number'].values)

        # print('attributes_apply_tax_after_discount are not the same')
        # print(df_merge[(
        #         df_merge[
        #             'attributes_apply_tax_after_discount'] != df_merge['tgt_apply_tax_after_discount'])][
        #           'attributes_invoice_number'].values)

        print('attributes_discount_total are not the same')
        print(df_merge[(
                df_merge[
                    'attributes_discount_total'] != df_merge['tgt_discount_total'])][
                  'attributes_invoice_number'].values)

        print('attributes_outstanding_total are not the same')
        print(df_merge[(
                df_merge[
                    'attributes_outstanding_total'] != df_merge['tgt_outstanding_total'])][
                  'attributes_invoice_number'].values)

        print('attributes_tax_exempt_total are not the same')
        print(df_merge[(
                df_merge[
                    'attributes_tax_exempt_total'] != df_merge['tgt_tax_exempt_total'])][
                  'attributes_invoice_number'].values)

        print('attributes_due_date are not the same')
        print(df_merge[(
                df_merge[
                    'attributes_due_date'] != df_merge['tgt_due_date'])][
                  'attributes_invoice_number'].values)


if __name__ == '__main__':
    client = NetsuiteTestingClient()
    client.generate_invoice_testing_report()




