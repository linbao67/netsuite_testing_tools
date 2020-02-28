import requests

import main.netsuite.endpoints as ep
from main.netsuite.netsuite_utils import get_all, passport

# input the email address
email = ''
password = 'Admin12345'
realm = 'TSTDRV2177818'


class NetSuite(object):

    @staticmethod
    def generate_currency():
        endpoint = 'currency'
        soap_service_xml = get_all(ep.ENDPOINTS_FIELDS[endpoint][ep.RECORD_TYPE],
                                   _soapheaders={"tokenPassport": passport(email, password, realm)})
        service = ep.ENDPOINTS_FIELDS[endpoint][ep.SERVICE]

        headers = {"Content-Type": "application/soap+xml; charset=UTF-8",
                   "Content-Length": str(len(soap_service_xml)),
                   "SOAPAction": service}
        print(soap_service_xml)
        response = requests.post(url='https://tstdrv2177818.suitetalk.api.netsuite.com/services/NetSuitePort_2019_2',
                                 headers=headers,
                                 data=soap_service_xml)

        print response.text

        print(response)


if __name__ == '__main__':
    NetSuite.generate_currency()
