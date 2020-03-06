import datetime
import json
import logging

import pytz
import requests
import xmltodict

import main.netsuite.endpoints as ep
from main.util.config_util import get_property, get_realm, get_password, get_user_name, set_property, \
    update_property_list
from main.util.file_utils import save_to_file, save_to_json, read_xml_file
from main.util.netsuite_utils import get_all, passport, create_soap_search_service, search_more_with_id


class NetsuiteClient(object):

    def __init__(self):
        self.batch_id = get_property('parameters', 'BATCH_ID')

    def generate_currency(self):
        endpoint = 'currency'

        soap_service_xml = get_all(ep.ENDPOINTS_FIELDS[endpoint][ep.RECORD_TYPE],
                                   _soapheaders={"passport": self.generate_passport()})
        service = ep.ENDPOINTS_FIELDS[endpoint][ep.SERVICE]

        headers = {"Content-Type": "application/soap+xml; charset=UTF-8",
                   "Content-Length": str(len(soap_service_xml)),
                   "SOAPAction": service}
        print(soap_service_xml)
        response = requests.post(url='https://tstdrv2177818.suitetalk.api.netsuite.com/services/NetSuitePort_2019_2',
                                 headers=headers,
                                 data=soap_service_xml)

        save_to_file(response.text, endpoint, self.batch_id)

    def generate_xml(self):
        sync_start_time = get_property('parameters', 'sync_start_time')
        sync_end_time = get_property('parameters', 'sync_end_time')

        endpoint_index = int(get_property('parameters', 'ENDPOINT_INDEX'))
        endpoint = ep.ENDPOINTS[endpoint_index]

        if sync_end_time is None:
            sync_end_time = datetime.datetime.utcnow().replace(microsecond=0).replace(
                tzinfo=pytz.utc).isoformat()

        page_index_str = get_property(endpoint, 'PAGE_INDEX')
        page_index = 0 if page_index_str == '' else int(page_index_str)
        logging.info("Start ingest endpoint {} with page_index {}".format(endpoint, page_index))
        if page_index == 0:
            service = ep.SEARCH
            soap_service_xml = create_soap_search_service(self.generate_passport(), endpoint,
                                                          sync_start_time=sync_start_time,
                                                          sync_end_time=sync_end_time)
        else:
            search_id = get_property(endpoint, 'SEARCH_ID')
            soap_service_xml = search_more_with_id(searchId=search_id, pageIndex=page_index, _soapheaders={
                "passport": self.generate_passport()})
            service = ep.SEARCH_MORE_WITH_ID

        headers = {"Content-Type": "application/soap+xml; charset=UTF-8",
                   "Content-Length": str(len(soap_service_xml)),
                   "SOAPAction": service}

        response = requests.post(url='https://tstdrv2177818.suitetalk.api.netsuite.com/services/NetSuitePort_2019_2',
                                 headers=headers,
                                 data=soap_service_xml)
        logging.info("Save xml file for endpoint {} with page index {}".format(endpoint, page_index))
        save_to_file(response.text, endpoint, self.batch_id, page_index)

        force_list = {'record', 'customField'}
        response_dict = xmltodict.parse(response.text, process_namespaces=True, namespaces=ep.ESCAPE_NAMESPACES,
                                        attr_prefix='', cdata_key='', force_list=force_list)
        response_dict.pop('customFieldList', None)
        response_json = json.loads(json.dumps(response_dict))

        search_id, total_pages, records = self.get_sync_info(response_json, service)
        save_to_json(records, endpoint, self.batch_id, page_index)

        if page_index == 0:
            self.udpate_sync_info(endpoint, search_id, total_pages, '1')

        if page_index + 1 == int(total_pages):
            endpoint_index = endpoint_index + 1
            property_list = [{'config_type': 'parameters',
                              'name': 'ENDPOINT_INDEX',
                              'value': str(endpoint_index)},
                             {'config_type': endpoint,
                              'name': 'PAGE_INDEX',
                              'value': '0'}]
            update_property_list(property_list)

        else:
            set_property(endpoint, 'page_index', str(page_index + 1))

        if endpoint_index == len(ep.ENDPOINTS):
            set_property('parameters', 'ENDPOINT_INDEX', '0')
            return False
        else:
            return True

    def update_batch_id(self):
        self.batch_id += str(int(self.batch_id) + 1)
        set_property('parameters', 'BATCH_ID', self.batch_id)

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

        return response_json['Envelope']['Body'][response_field][result_field]['searchId'], \
               int(response_json['Envelope']['Body'][response_field][result_field]['totalPages']), \
               response_json['Envelope']['Body'][response_field][result_field]['recordList']['record']

    def regenerate_json_file(self, endpoint, batch_id, page_index):

        xml_string = read_xml_file(endpoint, batch_id, page_index)

        force_list = {'record', 'customField'}
        response_dict = xmltodict.parse(xml_string, process_namespaces=True, namespaces=ep.ESCAPE_NAMESPACES,
                                        attr_prefix='', cdata_key='', force_list=force_list)
        response_dict.pop('customFieldList', None)
        response_json = json.loads(json.dumps(response_dict))

        if endpoint == ep.CURRENCY:
            service = ep.GET_ALL
        elif page_index == 0:
            service = ep.SEARCH
        else:
            service = ep.SEARCH_MORE_WITH_ID

        response_field = ep.SERVICE_FIELD[service][ep.RESPONSE_FIELD]
        result_field = ep.SERVICE_FIELD[service][ep.RESULT_FIELD]

        records = response_json['Envelope']['Body'][response_field][result_field]['recordList']['record']

        save_to_json(records, endpoint, self.batch_id, page_index)

    def generate_passport(self):
        email = get_user_name()
        password = get_password()
        realm = get_realm()
        return passport(email, password, realm)


if __name__ == '__main__':
    netsuite_client = NetsuiteClient()
    # if batch_id == 0:
    #     netsuite_client.generate_currency()
    result = True
    while result:
        result = netsuite_client.generate_xml()

    netsuite_client.update_batch_id()
    # netsuite_client.regenerate_json_file(ep.CURRENCY, 0, 0)
    # netsuite_client.regenerate_json_file(ep.CUSTOMER, 1, 0)
    # netsuite_client.regenerate_json_file(ep.CUSTOMER, 1, 1)