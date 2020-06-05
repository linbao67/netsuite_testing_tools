import json
import logging

import requests
import xmltodict

import main.netsuite.endpoints as ep
from main.util.config_util import get_property, get_realm, get_password, get_user_name, set_property, \
    update_property_list
from main.util.file_utils import save_to_json
from main.util.netsuite_utils import passport, create_advanced_search, search_more_with_id, delete_invoice


class NetsuiteClient(object):

    def __init__(self):
        self.batch_id = get_property('parameters', 'BATCH_ID')
        self.realm = get_realm()

    def get_and_delete_invoice_list(self, start_invoice_id, end_invoice_id):
        endpoint='invoice'
        page_index = int(get_property(endpoint, 'PAGE_INDEX'))
        print('endpoint {} page index {}'.format(endpoint, page_index))
        logging.info("Start ingest endpoint {} with page_index {}".format(endpoint, page_index))
        if page_index == 1:
            service = ep.SEARCH
            soap_service_xml = create_advanced_search(self.generate_passport(),start_invoice_id,end_invoice_id)
            print(soap_service_xml)
        else:
            search_id = get_property(endpoint, 'SEARCH_ID')
            soap_service_xml = search_more_with_id(searchId=search_id, pageIndex=page_index, _soapheaders={
                "passport": self.generate_passport()})
            service = ep.SEARCH_MORE_WITH_ID

        headers = {"Content-Type": "application/soap+xml; charset=UTF-8",
                   "Content-Length": str(len(soap_service_xml)),
                   "SOAPAction": service}

        response = requests.post(url='https://{}.suitetalk.api.netsuite.com/services/NetSuitePort_2019_2'.format(
            self.realm),
            headers=headers,
            data=soap_service_xml)
        print("Get xml file for endpoint {} with page index {}".format(endpoint, page_index))


        force_list = {'searchRow'}
        response_dict = xmltodict.parse(response.text, process_namespaces=True, namespaces=ep.ESCAPE_NAMESPACES,
                                        attr_prefix='', cdata_key='', force_list=force_list)
        print("XML to Json: {}".format(response_dict))

        response_json = json.loads(json.dumps(response_dict))
        response_field = ep.SERVICE_FIELD[service][ep.RESPONSE_FIELD]
        result_field = ep.SERVICE_FIELD[service][ep.RESULT_FIELD]
        if response_json['Envelope']['Body'][response_field][result_field]['totalRecords'] == '0':
            records = None
        else:
            records = response_json['Envelope']['Body'][response_field][result_field]['searchRowList']['searchRow']
            invoice_list = []
            for record in records:
                internal_id = record['basic']['internalId']['searchValue']['internalId']
                invoice_list.append(internal_id)

            deleted_list = list(set(invoice_list))

            delete_xml = delete_invoice(deleted_list,_soapheaders={"passport": self.generate_passport()})
            print("build delete xml: {}".format(delete_xml))
            headers = {"Content-Type": "application/soap+xml; charset=UTF-8",
                       "Content-Length": str(len(delete_xml)),
                       "SOAPAction": 'deleteList'}

            delete_response = requests.post(url='https://{'
                                             '}.suitetalk.api.netsuite.com/services/NetSuitePort_2019_2'.format(
                self.realm),
                headers=headers,
                data=delete_xml)
            print("delete_response: {}".format(delete_response.text))





        search_id, total_pages, records = self.get_sync_info(response_json, service)

        if total_pages != 0:
            save_to_json(records, endpoint, self.batch_id, page_index)
            if page_index == 1:
                self.udpate_sync_info(endpoint, search_id, total_pages, '2')

            if page_index == int(total_pages):
                property_list = [{'config_type': endpoint,
                                  'name': 'SEARCH_ID',
                                  'value': search_id},
                                 {'config_type': endpoint,
                                  'name': 'TOTAL_PAGES',
                                  'value': str(total_pages)},
                                 {'config_type': endpoint,
                                  'name': 'PAGE_INDEX',
                                  'value': '1'}]
                update_property_list(property_list)
                return False

            else:
                set_property(endpoint, 'page_index', str(page_index + 1))
                return True
        else:
            return False


    def udpate_sync_info(self, endpoint, search_id, total_pages, page_index=0):
        property_list = [{'config_type': endpoint,
                          'name': 'SEARCH_ID',
                          'value': search_id},
                         {'config_type': endpoint,
                          'name': 'TOTAL_PAGES',
                          'value': str(total_pages)},
                         {'config_type': endpoint,
                          'name': 'PAGE_INDEX',
                          'value': str(page_index)}]
        update_property_list(property_list)

    def get_sync_info(self, response_json, service):

        response_field = ep.SERVICE_FIELD[service][ep.RESPONSE_FIELD]
        result_field = ep.SERVICE_FIELD[service][ep.RESULT_FIELD]
        if response_json['Envelope']['Body'][response_field][result_field]['totalRecords'] == '0':
            records = None
        else:
            records = response_json['Envelope']['Body'][response_field][result_field]['searchRowList']['searchRow']

        return response_json['Envelope']['Body'][response_field][result_field]['searchId'], \
               int(response_json['Envelope']['Body'][response_field][result_field]['totalPages']), \
               records

    def generate_passport(self):
        email = get_user_name()
        password = get_password()
        realm = get_realm()
        return passport(email, password, realm)


if __name__ == '__main__':
    netsuite_client = NetsuiteClient()

    result = True
    while result:
        result = netsuite_client.get_and_delete_invoice_list(5735, 5739)



