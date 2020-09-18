# -*- coding: UTF-8 -*-
import logging

import main.netsuite.endpoints as ep

# if page_size < 3, NetSuite will return invalid page size error
PAGE_SIZE = 100


def get_search_preferences(page_size=PAGE_SIZE, body_fields_only=True):
    """
    Returns the xml part string of <searchPreference>
    Args:
        page_size (str): The parameter page_size pass to SOAP service.
        body_fields_only (str): The parameter bodyFieldsOnly in SOAP xml, whether to return body fields or embedded
        xml tags.
    Returns:
        (str) The xml part string
    """
    body_fields_only_str = 'true' if body_fields_only else 'false'

    search_preference_xml = '<urn:searchPreferences><urn:bodyFieldsOnly>{}' \
                            '</urn:bodyFieldsOnly><urn:pageSize>{}</urn:pageSize></urn:searchPreferences>'.format(
        body_fields_only_str, page_size)
    return search_preference_xml


def get_token_passport(token_passport):
    """
        Refresh the nonce and timestamp and returns the xml part string of <tokenPassport>
        Args:
            token_passport : The app.clients.netsuite_client.TokenPassportProperty object.
        Returns:
            (str) The xml part string
    """
    token_passport.update()
    token_passport_xml = '<urn:tokenPassport><urn1:account>{}</urn1:account>' \
                         '<urn1:consumerKey>{}</urn1:consumerKey><urn1:token>{}</urn1:token><urn1:nonce>{}</urn1:nonce>' \
                         '<urn1:timestamp>{}</urn1:timestamp><urn1:signature algorithm="{}">{}</urn1:signature>' \
                         '</urn:tokenPassport>'.format(token_passport.account, token_passport.consumer_key,
                                                       token_passport.token_id, token_passport.nonce,
                                                       token_passport.timestamp, token_passport.algorithm,
                                                       token_passport.signature)
    return token_passport_xml


def passport(email, password, realm):
    passport_xml = '<urn:applicationInfo>' \
                   '<urn:applicationId>BB3A8015-9FB5-4E6D-B3DE-8C54E336388D</urn:applicationId>' \
                   '</urn:applicationInfo>' \
                   '<urn:passport>' \
                   '<urn1:email>{}</urn1:email>' \
                   '<urn1:password>{}</urn1:password>' \
                   '<urn1:account>{}</urn1:account>'\
                   '<urn1:role internalId = "3"/>'\
                   '</urn:passport>'.format(email, password, realm.upper())
    return passport_xml


def search(searchRecord, _soapheaders):
    """
        Returns the xml part string of soap request xml of search service.
        Args:
            searchRecord (str): The xml <searchRecord> string.
            _soapheaders(str): The xml string to form <header></header> xml part
            xml tags.
        Returns:
            (str) The xml part string
    """
    head_xml = _soapheaders.get('searchPreferences', '')
    head_xml += _soapheaders.get('tokenPassport', '')
    head_xml += _soapheaders.get('passport', '')

    search_xml = '<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" ' \
                 'xmlns:urn="urn:messages_2019_2.platform.webservices.netsuite.com" ' \
                 'xmlns:urn1="urn:core_2019_2.platform.webservices.netsuite.com" ' \
                 'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">' \
                 '<soapenv:Header>{}</soapenv:Header>' \
                 '<soapenv:Body><urn:search>{}</urn:search></soapenv:Body></soapenv:Envelope>'.format(head_xml,
                                                                                                      searchRecord)

    return search_xml


def search_more_with_id(searchId, pageIndex, _soapheaders):
    """
        Returns the xml part string of soap request xml of searchMoreWithId service.
        Args:
            searchId (str): The parameter searchId returned by search service.
            pageIndex (int) : The page number of data to be fetched for the search id.
            _soapheaders(str): The xml string to form <header></header> xml part
            xml tags.
        Returns:
            (str) The xml part string
    """
    head_xml = _soapheaders.get('tokenPassport', '')
    head_xml += _soapheaders.get('passport', '')

    search_xml = '<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" ' \
                 'xmlns:urn="urn:messages_2019_2.platform.webservices.netsuite.com" ' \
                 'xmlns:urn1="urn:core_2019_2.platform.webservices.netsuite.com" ' \
                 'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">' \
                 '<soapenv:Header>{}</soapenv:Header>' \
                 '<soapenv:Body><urn:searchMoreWithId><urn:searchId>{}</urn:searchId><urn:pageIndex>{}</urn:pageIndex>' \
                 '</urn:searchMoreWithId></soapenv:Body></soapenv:Envelope>'.format(head_xml, searchId, pageIndex)

    return search_xml


def get_all(recordType, _soapheaders):
    """
        Returns the xml part string of soap request xml of getAll service.
        Args:
            recordType (str): The parameter recordType to fetch, e.g. 'currency'.
            _soapheaders(str): The xml string to form <header></header> xml part
            xml tags.
        Returns:
            (str) The xml part string
    """
    head_xml = _soapheaders.get('tokenPassport', '')
    head_xml += _soapheaders.get('passport', '')

    get_xml = '<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" ' \
              'xmlns:urn="urn:messages_2019_2.platform.webservices.netsuite.com" ' \
              'xmlns:urn1="urn:core_2019_2.platform.webservices.netsuite.com" ' \
              'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">' \
              '<soapenv:Header>{}</soapenv:Header>' \
              '<soapenv:Body><urn:getAll><urn:record recordType="{}"/>' \
              '</urn:getAll ></soapenv:Body></soapenv:Envelope>'.format(head_xml, recordType)

    return get_xml


def get_deleted(sync_start_date, sync_end_date, page_index, _soapheaders):
    """
        Returns the xml part string of soap request xml of getAll service.
        Args:
            sync_start_date (str): The parameter sync_start.
            _soapheaders(str): The xml string to form <header></header> xml part
            xml tags.
        Returns:
            (str) The xml part string
    """
    head_xml = _soapheaders.get('tokenPassport', '')
    head_xml += _soapheaders.get('passport', '')

    get_xml = '<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" ' \
              'xmlns:urn="urn:messages_2019_2.platform.webservices.netsuite.com" ' \
              'xmlns:urn1="urn:core_2019_2.platform.webservices.netsuite.com" ' \
              'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">' \
              '<soapenv:Header>{}</soapenv:Header>' \
              '<soapenv:Body>' \
              '<urn:getDeleted>' \
              '<urn:getDeletedFilter>' \
              '<urn1:deletedDate operator="within">' \
              '<urn1:searchValue>{}</urn1:searchValue>' \
              '<urn1:searchValue2>{}</urn1:searchValue2>' \
              '</urn1:deletedDate>' \
              '<urn1:type operator="anyOf">' \
              '<urn1:searchValue>creditMemo</urn1:searchValue>' \
              '<urn1:searchValue>invoice</urn1:searchValue>' \
              '<urn1:searchValue>customer</urn1:searchValue>' \
              '<urn1:searchValue>assemblyItem</urn1:searchValue>' \
              '<urn1:searchValue>descriptionItem</urn1:searchValue>' \
              '<urn1:searchValue>discountItem</urn1:searchValue>' \
              '<urn1:searchValue>downloadItem</urn1:searchValue>' \
              '<urn1:searchValue>giftCertificateItem</urn1:searchValue>' \
              '<urn1:searchValue>inventoryItem</urn1:searchValue>' \
              '<urn1:searchValue>kitItem</urn1:searchValue>' \
              '<urn1:searchValue>lotNumberedAssemblyItem</urn1:searchValue>' \
              '<urn1:searchValue>lotNumberedInventoryItem</urn1:searchValue>' \
              '<urn1:searchValue>markupItem</urn1:searchValue>' \
              '<urn1:searchValue>nonInventoryPurchaseItem</urn1:searchValue>' \
              '<urn1:searchValue>nonInventoryResaleItem</urn1:searchValue>' \
              '<urn1:searchValue>nonInventorySaleItem</urn1:searchValue>' \
              '<urn1:searchValue>otherChargePurchaseItem</urn1:searchValue>' \
              '<urn1:searchValue>otherChargeResaleItem</urn1:searchValue>' \
              '<urn1:searchValue>otherChargeSaleItem</urn1:searchValue>' \
              '<urn1:searchValue>paymentItem</urn1:searchValue>' \
              '<urn1:searchValue>serializedAssemblyItem</urn1:searchValue>' \
              '<urn1:searchValue>serializedInventoryItem</urn1:searchValue>' \
              '<urn1:searchValue>serializedPurchasedItem</urn1:searchValue>' \
              '<urn1:searchValue>servicePurchaseItem</urn1:searchValue>' \
              '<urn1:searchValue>serviceResaleItem</urn1:searchValue>' \
              '<urn1:searchValue>serviceSaleItem</urn1:searchValue>' \
              '<urn1:searchValue>subtotalItem</urn1:searchValue>' \
              '<urn1:searchValue>itemGroup</urn1:searchValue>' \
              '</urn1:type></urn:getDeletedFilter>' \
              '<urn:pageIndex>{}</urn:pageIndex>' \
              '</urn:getDeleted></soapenv:Body></soapenv:Envelope>'.format(head_xml, sync_start_date, sync_end_date,
                                                                           page_index)
    return get_xml


def get(internalId, recordType, _soapheaders):
    """
        Returns the xml part string of soap request xml of getAll service.
        Args:
            :param internalId: internalId of the missing item
            :param recordType : The parameter recordType to fetch, e.g. 'currency'.
            :param _soapheaders : The xml string to form <header></header> xml part
            xml tags.
        Returns:
            (str) The xml part string

    """
    head_xml = _soapheaders.get('tokenPassport', '')
    head_xml += _soapheaders.get('passport', '')

    get_xml = '<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" ' \
              'xmlns:urn="urn:messages_2019_2.platform.webservices.netsuite.com" ' \
              'xmlns:urn1="urn:core_2019_2.platform.webservices.netsuite.com" ' \
              'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">' \
              '<soapenv:Header>{}</soapenv:Header>' \
              '<soapenv:Body><urn:get><urn:baseRef internalId="{}" type="{}" ' \
              'xsi:type="urn1:RecordRef"/></urn:get></soapenv:Body></soapenv:Envelope>'.format(head_xml,
                                                                                               internalId,
                                                                                               recordType)

    return get_xml


def get_list(itemList, _soapheaders):
    """
        Returns the xml part string of soap request xml of getAll service.
        Args:
            :param internalId: internalId of the missing item
            :param recordType : The parameter recordType to fetch, e.g. 'currency'.
            :param _soapheaders : The xml string to form <header></header> xml part
            xml tags.
        Returns:
            (str) The xml part string

    """
    head_xml = _soapheaders.get('tokenPassport', '')
    head_xml += _soapheaders.get('passport', '')

    record_list = ''

    for item in itemList:
        record_list += '<urn:baseRef internalId="{}" type="{}" xsi:type="urn1:RecordRef"/>'.format(
            item.get("id"),
            item.get("type"))

    get_xml = '<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" ' \
              'xmlns:urn="urn:messages_2019_2.platform.webservices.netsuite.com" ' \
              'xmlns:urn1="urn:core_2019_2.platform.webservices.netsuite.com" ' \
              'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">' \
              '<soapenv:Header>{}</soapenv:Header>' \
              '<soapenv:Body><urn:getList>{}</urn:getList>' \
              '</soapenv:Body></soapenv:Envelope>'.format(head_xml, record_list)

    return get_xml


def search_record_part(search_endpoint, search_schema, basic_search_criteria):
    """
        Returns the xml part string of <searchRecord>
        Args:
            :param basic_search_criteria: xml string <Basic>
            :param search_schema:
            :param search_endpoint:
        Returns:
            (str) The xml part string

    """
    body_xml = '<searchRecord xsi:type = "ns1:{}" xmlns:ns1 = "{}">{}</searchRecord>'.format(search_endpoint,
                                                                                             search_schema,
                                                                                             basic_search_criteria)
    return body_xml


def basic_search(basic_search_type, _basic_search_criteria):
    """
        Returns the xml part string of <basic>
        Args:
            :param _basic_search_criteria:
            :param basic_search_type:
        Returns:
            (str) The xml part string
    """
    basic_search_criteria = _basic_search_criteria.get('lastModifiedDate', '')
    basic_search_criteria += _basic_search_criteria.get('type', '')
    basic_search_criteria += _basic_search_criteria.get('internalId', '')

    basic_search_xml = '<ns1:basic xsi:type = "ns2:{}" ' \
                       'xmlns:ns2 = "urn:common_2019_2.platform.webservices.netsuite.com">{}</ns1:basic>'.format(
        basic_search_type, basic_search_criteria)
    return basic_search_xml


def search_date_criteria(searchValue, searchValue2, operator, field_name):
    """
        Returns the xml part string of <lastModifiedDate>...</lastModifiedDate>
        Args:
            :param field_name: the search name of lastModifiedDate, a certain endpoint of lastModifiedDate is
            lastModified.
            :param operator: e.g. within, greater than
            :param searchValue: the begin date when the operator is "within"
            :param searchValue2: the end date when the operator is "within"

        Returns:
            (str) The xml part string <lastModifiedDate>...</lastModifiedDate>
    """
    if searchValue2 is not None:
        search_value2_str = '<urn1:searchValue2 xsi:type = "xsd:dateTime">{}</urn1:searchValue2>'.format(searchValue2)
    else:
        search_value2_str = ''

    date_xml = '<ns2:{} operator = "{}" xsi:type = "urn1:SearchDateField">' \
               '<urn1:searchValue xsi:type = "xsd:dateTime">{}</urn1:searchValue>{}' \
               '</ns2:{}>'.format(field_name, operator, searchValue, search_value2_str, field_name)
    return date_xml


def search_type_criteria(searchValue, operator):
    """
        Returns the xml part string of <type></type>
        Args:
            :param operator: operator to search the type within the BascSearch
            :param searchValue: type to search
        Returns:
            (str) The xml part string
    """
    type_xml = '<ns2:type operator = "{}" xsi:type = "urn1:SearchEnumMultiSelectField">' \
               '<urn1:searchValue xsi:type = "xsd:string">{}</urn1:searchValue></ns2:type>'.format(operator,
                                                                                                   searchValue)
    return type_xml


def search_internal_id_criteria(searchValue, operator):
    """
        Returns the xml part string of <type></type>
        Args:
            :param operator: operator to search the internalId within the <SearchBasic>
            :param searchValue: internalId to search
        Returns:
            (str) The xml part string
    """
    internal_id_xml = '<ns2:internalId operator="{}" xsi:type="urn1:SearchMultiSelectField"><urn1:searchValue ' \
                      'ns2:internalId="{}" xsi:type="ns2:RecordRef"/></ns2:internalId>'.format(operator, searchValue)
    return internal_id_xml

def create_advanced_search(passport,start_invoice_id,end_invoice_id=None,endpoint='invoice'):
    search_preference = get_search_preferences(page_size=100,
                                               body_fields_only=True)
    search_body_xml = '<searchRecord xsi:type="ns1:TransactionSearchAdvanced" ' \
                      'xmlns:ns1="urn:sales_2019_2.transactions.webservices.netsuite.com">' \
                      '<ns1:criteria><ns1:basic xmlns:ns2="urn:common_2019_2.platform.webservices.netsuite.com">' \
                      '<ns2:internalIdNumber operator="between" xsi:type="urn1:SearchLongField">' \
                      '<urn1:searchValue xsi:type="xsd:long">{}</urn1:searchValue>' \
                      '<urn1:searchValue2 xsi:type="xsd:long">{}</urn1:searchValue2>' \
                      '</ns2:internalIdNumber>' \
                      '<ns2:type operator="anyOf" xsi:type="urn1:SearchEnumMultiSelectField">' \
                      '<urn1:searchValue xsi:type="xsd:string">invoice</urn1:searchValue>' \
                      '</ns2:type>' \
                      '</ns1:basic>' \
                      '</ns1:criteria>' \
                      '<ns1:columns>' \
                      '<ns1:basic xmlns:ns2="urn:common_2019_2.platform.webservices.netsuite.com" ' \
                      'xmlns:ns3="urn:core_2019_2.platform.webservices.netsuite.com">' \
                      '<ns2:internalId/></ns1:basic> </ns1:columns>' \
                      '</searchRecord>'.format(start_invoice_id,end_invoice_id)

    soap_xml = search(search_body_xml, _soapheaders={"passport": passport,
                                                   "searchPreferences": search_preference})

    return soap_xml

def delete_invoice(deleted_list,_soapheaders):
    head_xml = _soapheaders.get('tokenPassport', '')
    head_xml += _soapheaders.get('passport', '')
    record_list = ''
    for n in deleted_list:
        record_list += '<urn:baseRef internalId="{}" type="invoice" xsi:type="urn1:RecordRef"/>'.format(n)
    delete_xml = '<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" ' \
              'xmlns:urn="urn:messages_2019_2.platform.webservices.netsuite.com" ' \
              'xmlns:urn1="urn:core_2019_2.platform.webservices.netsuite.com" ' \
              'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">' \
              '<soapenv:Header>{}</soapenv:Header>' \
              '<soapenv:Body><urn:deleteList>{}</urn:deleteList>' \
              '</soapenv:Body></soapenv:Envelope>'.format(head_xml, record_list)
    return delete_xml

def create_soap_search_service(passport, endpoint, sync_start_time=None, sync_end_time=None,
                               internal_id=None):
    """
            Returns the xml part string of <type></type>
            Args:
                :param passport:
                :param sync_end_time:
                :param sync_start_time:
                :param endpoint:
                :param internal_id:
            Returns:
                (str) The xml part string
        """
    search_preference = get_search_preferences(page_size=PAGE_SIZE,
                                               body_fields_only=ep.ENDPOINTS_FIELDS[endpoint][ep.BODY_FIELDS_ONLY])

    _basic_search_criteria = {}

    if internal_id is not None:
        internal_id_search_criteria = search_internal_id_criteria(searchValue=internal_id, operator='anyOf')
        _basic_search_criteria['internalId'] = internal_id_search_criteria
    else:
        date_search_criteria = search_date_criteria(searchValue=sync_start_time,
                                                    searchValue2=sync_end_time, operator="within", field_name=
                                                    ep.ENDPOINTS_FIELDS[endpoint][ep.LAST_MODIFIED_DATE])
        _basic_search_criteria['lastModifiedDate'] = date_search_criteria

    if ep.ENDPOINTS_FIELDS[endpoint][ep.REQUIRES_SEARCH_TYPE]:
        type_search_criteria = search_type_criteria(searchValue=ep.ENDPOINTS_FIELDS[endpoint][ep.TRANSACTION_TYPE],
                                                    operator="anyOf")
        _basic_search_criteria['type'] = type_search_criteria

    # TransactionSearchBasic has an argument type, but the other endpoint may not have e.g. customer
    basic_search_criteria = basic_search(ep.ENDPOINTS_FIELDS[endpoint][ep.BASIC_SEARCH],
                                         _basic_search_criteria)

    search_record = search_record_part(ep.ENDPOINTS_FIELDS[endpoint][ep.SEARCH],
                                       ep.ENDPOINTS_FIELDS[endpoint][ep.SEARCH_NAMESPACE],
                                       basic_search_criteria)
    soap_xml = search(search_record, _soapheaders={"passport": passport,
                                                   "searchPreferences": search_preference})

    return soap_xml


def process_error_response(endpoint, error_node):
    """
        Process error response, log the error code and message and raise the exception.
        Args:
            endpoint(str): endpoint of current call
            error_node(json obj): error node from response
    """
    error_code = error_node['status']['statusDetail']['code']
    error_message = error_node['status']['statusDetail']['message']

    logging.info("Endpoint {} encounter an error with code:{} message: {}".format(
        endpoint, error_code, error_message))

    if error_code == 'SSS_RECORD_TYPE_MISMATCH':
        start_position = error_message.index(':') + 2
        end_position = error_message.index(' ', start_position)
        type_str = error_message[start_position:end_position].lower()
        correct_type = ep.TYPE_LIST.get(type_str)
        return correct_type

    elif error_code == 'INSUFFICIENT_PERMISSION':
        raise ValueError("Insufficient permission with code {} and error message {}".format(
            error_code, error_message))
    elif error_code == 'INVALID_SEARCH_PAGE_INDEX':
        raise ValueError("invalid page index with code {} and error message {}".format(
            error_code, error_message))

    else:
        raise ValueError("api call failed with code {} and error message {}".format(
            error_code, error_message))
