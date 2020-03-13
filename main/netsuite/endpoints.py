CUSTOMER = 'customer'
ITEM = 'item'
INVOICE = 'invoice'
CREDIT_MEMO = 'creditMemo'
CURRENCY = 'currency'
TIME_BILL = 'timeBill'
CUSTOMER_PAYMENT = 'customerPayment'
DELETED_DATA = 'deletedData'
VENDOR_BILL = 'vendorBill'
ACCOUNTING_PERIOD = 'accountingPeriod'

SERVICE = 'service'
SEARCH = 'Search'
SEARCH_MORE_WITH_ID = 'SearchMoreWithId'
GET_ALL = 'GetAll'
GET_LIST = 'GetList'
GET = 'Get'
GET_DELETED = 'getDeleted'
BASIC_SEARCH = 'BasicSearch'
BODY_FIELDS_ONLY = 'BodyFieldsOnly'
REQUIRES_SEARCH_TYPE = 'RequiresSearchType'
TRANSACTION_TYPE = 'TransactionType'
SEARCH_NAMESPACE = 'SearchNamespace'
RECORD_TYPE = 'RecordType'
RESPONSE_FIELD = 'ResponseField'
RESULT_FIELD = 'ResultField'
RECORD_LIST = 'RecordList'
RECORD = 'Record'
LAST_MODIFIED_DATE = 'lastModifiedDate'

SERVICE_FIELD = {
    SEARCH: {
        RESPONSE_FIELD: 'searchResponse',
        RESULT_FIELD: 'searchResult',
        RECORD_LIST: 'recordList',
        RECORD: 'record'
    },
    SEARCH_MORE_WITH_ID: {
        RESPONSE_FIELD: 'searchMoreWithIdResponse',
        RESULT_FIELD: 'searchResult',
        RECORD_LIST: 'recordList',
        RECORD: 'record'
    },
    GET_ALL: {
        RESPONSE_FIELD: 'getAllResponse',
        RESULT_FIELD: 'getAllResult',
        RECORD_LIST: 'recordList',
        RECORD: 'record'
    },
    GET: {
        RESPONSE_FIELD: 'getResponse',
        RESULT_FIELD: 'readResponse'
    },

    GET_DELETED: {
        RESPONSE_FIELD: 'getDeletedResponse',
        RESULT_FIELD: 'getDeletedResult',
        RECORD_LIST: 'deletedRecordList',
        RECORD: 'deletedRecord'
    }

}

# Invoice includes invoice, invoice item
# Credit Memo includes credit memo, credit memo items, credit memo apply (credit note allocation)
ENDPOINTS = [
    CUSTOMER, ITEM, ACCOUNTING_PERIOD, TIME_BILL, CUSTOMER_PAYMENT, VENDOR_BILL, CREDIT_MEMO,
    INVOICE
]

ENDPOINTS_FIELDS = {
    CUSTOMER: {
        SERVICE: SEARCH,
        SEARCH: 'CustomerSearch',
        SEARCH_NAMESPACE: 'urn:relationships_2019_2.lists.webservices.netsuite.com',
        BASIC_SEARCH: 'CustomerSearchBasic',
        LAST_MODIFIED_DATE: 'lastModifiedDate',
        BODY_FIELDS_ONLY: False,
        REQUIRES_SEARCH_TYPE: False

    },
    ITEM: {
        SERVICE: SEARCH,
        SEARCH: 'ItemSearch',
        SEARCH_NAMESPACE: 'urn:accounting_2019_2.lists.webservices.netsuite.com',
        BASIC_SEARCH: 'ItemSearchBasic',
        LAST_MODIFIED_DATE: 'lastModifiedDate',
        BODY_FIELDS_ONLY: True,
        REQUIRES_SEARCH_TYPE: False
    },
    ACCOUNTING_PERIOD: {
        SERVICE: SEARCH,
        SEARCH: 'AccountingPeriodSearch',
        SEARCH_NAMESPACE: 'urn:accounting_2019_2.lists.webservices.netsuite.com',
        BASIC_SEARCH: 'AccountingPeriodSearchBasic',
        LAST_MODIFIED_DATE: 'startDate',
        BODY_FIELDS_ONLY: True,
        REQUIRES_SEARCH_TYPE: False
    },
    INVOICE: {
        SERVICE: SEARCH,
        SEARCH: 'TransactionSearch',
        SEARCH_NAMESPACE: 'urn:sales_2019_2.transactions.webservices.netsuite.com',
        BASIC_SEARCH: 'TransactionSearchBasic',
        LAST_MODIFIED_DATE: 'lastModifiedDate',
        BODY_FIELDS_ONLY: False,
        REQUIRES_SEARCH_TYPE: True,
        TRANSACTION_TYPE: INVOICE
    },
    CREDIT_MEMO: {
        SERVICE: SEARCH,
        SEARCH: 'TransactionSearch',
        SEARCH_NAMESPACE: 'urn:sales_2019_2.transactions.webservices.netsuite.com',
        BASIC_SEARCH: 'TransactionSearchBasic',
        LAST_MODIFIED_DATE: 'lastModifiedDate',
        BODY_FIELDS_ONLY: False,
        REQUIRES_SEARCH_TYPE: True,
        TRANSACTION_TYPE: CREDIT_MEMO
    },

    TIME_BILL: {
        SERVICE: SEARCH,
        SEARCH: 'TimeBillSearch',
        SEARCH_NAMESPACE: 'urn:employees_2019_2.transactions.webservices.netsuite.com',
        BASIC_SEARCH: 'TimeBillSearchBasic',
        LAST_MODIFIED_DATE: 'lastModified',
        BODY_FIELDS_ONLY: True,
        REQUIRES_SEARCH_TYPE: False
    },
    CUSTOMER_PAYMENT: {
        SERVICE: SEARCH,
        SEARCH: 'TransactionSearch',
        SEARCH_NAMESPACE: 'urn:sales_2019_2.transactions.webservices.netsuite.com',
        BASIC_SEARCH: 'TransactionSearchBasic',
        LAST_MODIFIED_DATE: 'lastModifiedDate',
        BODY_FIELDS_ONLY: False,
        REQUIRES_SEARCH_TYPE: True,
        TRANSACTION_TYPE: CUSTOMER_PAYMENT
    },
    VENDOR_BILL: {
        SERVICE: SEARCH,
        SEARCH: 'TransactionSearch',
        SEARCH_NAMESPACE: 'urn:sales_2019_2.transactions.webservices.netsuite.com',
        BASIC_SEARCH: 'TransactionSearchBasic',
        LAST_MODIFIED_DATE: 'lastModifiedDate',
        BODY_FIELDS_ONLY: False,  # Need to return ItemList
        REQUIRES_SEARCH_TYPE: True,
        TRANSACTION_TYPE: VENDOR_BILL
    },

    CURRENCY: {
        SERVICE: GET_ALL,
        RECORD_TYPE: 'currency',
        BODY_FIELDS_ONLY: True
    },

    DELETED_DATA: {
        SERVICE: GET_DELETED

    }
}

TYPE_LIST = {
    'assemblyitem': 'assemblyItem',
    'descriptionitem': 'descriptionItem',
    'discountitem': 'discountItem',
    'downloaditem': 'downloadItem',
    'giftcertificateitem': 'giftCertificateItem',
    'inventoryitem': 'inventoryItem',
    'kititem': 'kitItem',
    'lotnumberedassemblyitem': 'lotNumberedAssemblyItem',
    'lotnumberedinventoryitem': 'lotNumberedInventoryItem',
    'markupitem': 'markupItem',
    'noninventorypurchaseitem': 'nonInventoryPurchaseItem',
    'noninventoryresaleitem': 'nonInventoryResaleItem',
    'noninventorysaleitem': 'nonInventorySaleItem',
    'otherchargepurchaseitem': 'otherChargePurchaseItem',
    'otherchargeresaleitem': 'otherChargeResaleItem',
    'otherchargesaleitem': 'otherChargeSaleItem',
    'paymentitem': 'paymentItem',
    'serializedassemblyitem': 'serializedAssemblyItem',
    'servicepurchaseitem': 'servicePurchaseItem',
    'serviceresaleitem': 'serviceResaleItem',
    'servicesaleitem': 'serviceSaleItem',
    'subtotalitem': 'subtotalItem',
    'itemgroup': 'itemGroup'
}

ESCAPE_NAMESPACES = {
    'http://schemas.xmlsoap.org/soap/envelope/': None,
    'http://www.w3.org/2001/XMLSchema': None,
    'http://www.w3.org/2001/XMLSchema-instance': None,
    'urn:messages_2019_2.platform.webservices.netsuite.com': None,
    'urn:core_2019_2.platform.webservices.netsuite.com': None,
    'urn:relationships_2019_2.lists.webservices.netsuite.com': None,
    'urn:sales_2019_2.transactions.webservices.netsuite.com': None,
    'urn:accounting_2019_2.lists.webservices.netsuite.com': None,
    'urn:customers_2019_2.transactions.webservices.netsuite.com': None,
    'urn:common_2019_2.platform.webservices.netsuite.com': None,
    'urn:employees_2019_2.transactions.webservices.netsuite.com': None,
    'urn:purchases_2019_2.transactions.webservices.netsuite.com': None
}
