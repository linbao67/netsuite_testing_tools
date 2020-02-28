from main.netsuite.netsuite_utils import get_all, passport
import main.netsuite.endpoints as ep
import requests


class NetSuite(object):

    @staticmethod
    def generate_currency():

        endpoint = 'currency'
        soap_service_xml = get_all(ep.ENDPOINTS_FIELDS[endpoint][ep.RECORD_TYPE],
                                   _soapheaders={"tokenPassport": passport()})
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


