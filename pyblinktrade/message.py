__author__ = 'rodrigo'
import json

class InvalidMessageException(Exception):
  def __init__(self, raw_message, json_message=None, tag=None, value=None):
    super(InvalidMessageException, self).__init__()
    self.raw_message = raw_message
    self.json_message = json_message
    self.tag = tag
    self.value = value
  def __str__(self):
    return 'Invalid Message'

class InvalidMessageLengthException(InvalidMessageException):
  def __str__(self):
    return 'Invalid message length'

class InvalidMessageTypeException(InvalidMessageException):
  def __str__(self):
    return 'Invalid Message Type (%s)' % str(self.tag)

class InvalidMessageMissingTagException(InvalidMessageException):
  def __str__(self):
    return 'Missing tag %s' % str(self.tag)

class InvalidMessageFieldException(InvalidMessageException):
  def __str__(self):
    return 'Invalid value tag(%s)=%s'%(self.tag, self.value)

class BaseMessage(object):
  MAX_MESSAGE_LENGTH = 10024*1000
  def __init__(self, raw_message):
    self.raw_message = raw_message

  def has(self, attr):
    raise  NotImplementedError()

  def get(self, attr, default):
    raise  NotImplementedError()

  def set(self, attr, value):
    raise NotImplementedError()

  def is_valid(self):
    raise  NotImplementedError()


class JsonMessage(BaseMessage):
  MAX_MESSAGE_LENGTH = 10024*1000
  def raise_exception_if_required_tag_is_missing(self, tag):
    if tag not in self.message:
      raise InvalidMessageMissingTagException(self.raw_message, self.message, tag)

  def raise_exception_if_not_a_integer(self, tag):
    val = self.get(tag)
    if not type(val) == int:
      raise InvalidMessageFieldException(self.raw_message, self.message, tag, val)

  def raise_exception_if_not_a_number(self, tag):
    val = self.get(tag)
    if not( type(val) == float or type(val) == int):
      raise InvalidMessageFieldException(self.raw_message, self.message, tag, val)

  def raise_exception_if_empty(self, tag):
    val = self.get(tag)
    if not val :
      raise InvalidMessageFieldException(self.raw_message, self.message, tag, val)

  def raise_exception_if_not_string(self, tag):
    val = self.get(tag)
    if not( type(val) == str or type(val) == unicode):
      raise InvalidMessageFieldException(self.raw_message, self.message, tag, val)

  def raise_exception_if_not_greater_than_zero(self, tag):
    self.raise_exception_if_not_a_number(tag)
    val = self.get(tag)
    if not val > 0:
      raise InvalidMessageFieldException(self.raw_message, self.message, tag, val)

  def raise_exception_if_optional_field_is_a_negative_number(self, tag):
    val = self.message.get(tag)
    if val:
      if not( type(val) == float or type(val) == int) or val < 0:
        raise InvalidMessageFieldException(self.raw_message, self.message, tag, val)

  def raise_exception_if_not_in(self, tag, list):
    val = self.get(tag)
    if val not in list :
      raise InvalidMessageFieldException(self.raw_message, self.message, tag, val)

  def raise_exception_if_length_is_greater_than(self, tag, length):
    val = self.get(tag)
    if len(val) > length:
      raise InvalidMessageFieldException(self.raw_message, self.message, tag, val)

  def raise_exception_if_length_is_less_than(self, tag, length):
    val = self.get(tag)
    if len(val) < length:
      raise InvalidMessageFieldException(self.raw_message, self.message, tag, val)
      
  def __str__(self):
    return str(self.message)

  def toJSON(self):
    return self.message

  def __init__(self, raw_message):
    super(JsonMessage, self).__init__(raw_message)
    self.valid = False

    # make sure a malicious users didn't send us more than 4096 bytes
    if len(raw_message) > self.MAX_MESSAGE_LENGTH:
      raise InvalidMessageLengthException(raw_message)


    # parse the message
    self.message = json.loads(raw_message)

    if 'MsgType' not in self.message:
      raise InvalidMessageTypeException(raw_message, self.message)

    self.type = self.message['MsgType']
    del self.message['MsgType']

    self.valid_message_types = {
      # User messages based on the Fix Protocol
      '0':   'Heartbeat',
      '1':   'TestRequest',
      'B':   'News',
      'C':   'Email',
      'V':   'MarketDataRequest',
      'W':   'MarketDataFullRefresh',
      'X':   'MarketDataIncrementalRefresh',
      'Y':   'MarketDataRequestReject',
      'BE':  'UserRequest',
      'BF':  'UserResponse',
      'D':   'NewOrderSingle',
      'F':   'OrderCancelRequest',
      '8':   'ExecutionReport',
      '9':   'OrderCancelReject',
      'x':   'SecurityListRequest',
      'y':   'SecurityList',
      'e':   'SecurityStatusRequest',
      'f':   'SecurityStatus',

      # User  messages
      'U0':  'Signup',
      'U2':  'UserBalanceRequest',
      'U3':  'UserBalanceResponse',
      'U4':  'OrdersListRequest',
      'U5':  'OrdersListResponse',
      'U6':  'WithdrawRequest',
      'U7':  'WithdrawResponse',
      'U9':  'WithdrawRefresh',

      'U10': 'CreatePasswordResetRequest',
      'U11': 'CreatePasswordResetResponse',
      'U12': 'ProcessPasswordResetRequest',
      'U13': 'ProcessPasswordResetResponse',
      'U16': 'EnableDisableTwoFactorAuthenticationRequest',
      'U17': 'EnableDisableTwoFactorAuthenticationResponse',

      'U18': 'DepositRequest',
      'U19': 'DepositResponse',
      'U23': 'DepositRefresh',

      'U20': 'DepositMethodsRequest',
      'U21': 'DepositMethodsResponse',


      'U24': 'WithdrawConfirmationRequest',
      'U25': 'WithdrawConfirmationResponse',
      'U26': 'WithdrawListRequest',
      'U27': 'WithdrawListResponse',
      'U28': 'BrokerListRequest',
      'U29': 'BrokerListResponse',

      'U30': 'DepositListRequest',
      'U31': 'DepositListResponse',

      'U32': 'TradeHistoryRequest',
      'U33': 'TradeHistoryResponse',

      'U34': 'LedgerListRequest',
      'U35': 'LedgerListResponse',

      'U36': 'TradersRankRequest',
      'U37': 'TradersRankResponse',

      'U38': 'UpdateProfile',
      'U39': 'UpdateProfileResponse',
      'U40': 'ProfileRefresh',

      'U42': 'PositionRequest' ,
      'U43': 'PositionResponse' ,

      'U44': 'ConfirmTrustedAddressRequest' ,
      'U45': 'ConfirmTrustedAddressResponse' ,
      'U46': 'SuggestTrustedAddressPublish' ,

      'U48': 'DepositMethodRequest',
      'U49': 'DepositMethodResponse',

      'U50': 'APIKeyListRequest',
      'U51': 'APIKeyListResponse',
      'U52': 'APIKeyCreateRequest',
      'U53': 'APIKeyCreateResponse',
      'U54': 'APIKeyRevokeRequest',
      'U55': 'APIKeyRevokeResponse',

      'U56': 'GetCreditLineOfCreditRequest',
      'U57': 'GetCreditLineOfCreditResponse',
      'U58': 'PayCreditLineOfCreditRequest',
      'U59': 'PayCreditLineOfCreditResponse',
      'U60': 'LineOfCreditListRequest',
      'U61': 'LineOfCreditListResponse',
      'U62': 'EnableCreditLineOfCreditRequest',
      'U63': 'EnableCreditLineOfCreditResponse',
      'U65': 'LineOfCreditRefresh',
      
      'U70': 'CancelWithdrawalRequest',
      'U71': 'CancelWithdrawalResponse',

      'U72': 'CardListRequest',
      'U73': 'CardListResponse',
      'U74': 'CardCreateRequest',
      'U75': 'CardCreateResponse',
      'U76': 'CardDisableRequest',
      'U77': 'CardDisableResponse',
      'U78': 'WithdrawCommentRequest',
      'U79': 'WithdrawCommentResponse',

      # Broker messages
      'B0':  'ProcessDeposit',
      'B1':  'ProcessDepositResponse',
      'B2':  'CustomerListRequest',
      'B3':  'CustomerListResponse',
      'B4':  'CustomerDetailRequest',
      'B5':  'CustomerDetailResponse',
      'B6':  'ProcessWithdraw',
      'B7':  'ProcessWithdrawResponse',
      'B8':  'VerifyCustomerRequest',
      'B9':  'VerifyCustomerResponse',
      'B11': 'VerifyCustomerRefresh',
      'B12': 'ClearingHistoryRequest',
      'B13': 'ClearingHistoryResponse',
      'B14': 'ProcessClearingRequest',
      'B15': 'ProcessClearingResponse',
      'B17': 'ProcessClearingRefresh',
      

      # System messages
      'S0':  'AccessLog',

      'S2':  'AwayMarketTickerRequest',
      'S3':  'AwayMarketTickerResponse',
      'S4':  'AwayMarketTickerPublish',

      'S6':  'RestAPIRequest',
      'S7':  'RestAPIResponse',
      
      'S8':  'SetInstrumentDefinitionRequest',
      'S9':  'SetInstrumentDefinitionResponse',

      'S10': 'DocumentPublish',

      'S12': 'DocumentListRequest',
      'S13': 'DocumentListResponse',
        
      'S14' : 'CryptoWithdrawNetworkFeeTransferRequest',
      'S15' : 'CryptoWithdrawNetworkFeeTransferResponse',
      
      'S16' : 'UserLogonReport',
      'S17' : 'UserLogonReportAck',

      'S20': 'BrokerCreateRequest',
      'S21': 'BrokerCreateResponse',
      'S22': 'BrokersListRequest',
      'S23': 'BrokersListResponse',
      'S24': 'BrokerAccountsListRequest',
      'S25': 'BrokerAccountsListResponse',
      'S26': 'BrokerDeleteRequest',
      'S27': 'BrokerDeleteResponse',

      'S30': 'AccountCreateRequest',
      'S31': 'AccountCreateResponse',
      'S32': 'AccountDeleteRequest',
      'S33': 'AccountDeleteResponse',

      # Administrative messages
      'A0':  'DbQueryRequest',
      'A1':  'DbQueryResponse',

      'I0': 'UpdateBalanceRequest',
      'I1': 'UpdateBalanceResponse',
      'I2': 'FundTransferReport',

      'ERROR': 'ErrorMessage',
    }


    def make_helper_is_message_type( tag):
      def _method(self):
        return self.type == tag
      return _method

    for k,v in self.valid_message_types.iteritems():
      _method = make_helper_is_message_type(k)
      setattr(JsonMessage, 'is' + v, _method)


    #validate Type
    if self.type not in self.valid_message_types:
      raise InvalidMessageTypeException(raw_message, self.message, self.type)

    # validate all fields
    if self.type == '0':  #Heartbeat
      self.raise_exception_if_required_tag_is_missing('TestReqID')

    elif self.type == '1':  # TestRequest
      self.raise_exception_if_required_tag_is_missing('TestReqID')

    elif self.type == 'V':  #MarketData Request
      self.raise_exception_if_required_tag_is_missing('MDReqID')
      self.raise_exception_if_required_tag_is_missing('SubscriptionRequestType')
      self.raise_exception_if_required_tag_is_missing('MarketDepth')

      subscriptionRequestType = self.message.get('SubscriptionRequestType')
      if subscriptionRequestType == '1':
        self.raise_exception_if_required_tag_is_missing('MDUpdateType')


      #TODO: Validate all fields of MarketData Request Message

    elif self.type == 'Y':
      self.raise_exception_if_required_tag_is_missing('MDReqID')
      #TODO: Validate all fields of MarketData Request Cancel Message

    elif self.type == 'BE':  #logon
      self.raise_exception_if_required_tag_is_missing('BrokerID')
      self.raise_exception_if_required_tag_is_missing('UserReqID')
      self.raise_exception_if_required_tag_is_missing('Username')
      self.raise_exception_if_not_string('Username')
      self.raise_exception_if_required_tag_is_missing('UserReqTyp')

      reqId = self.message.get('UserReqTyp')
      if reqId in ('1', '3'):
        self.raise_exception_if_required_tag_is_missing('Password')

      if reqId == '3':
        self.raise_exception_if_required_tag_is_missing('NewPassword')

      #TODO: Validate all fields of Logon Message

    elif self.type == 'U0':  #Signup
      # Username must be between 3 bytes and 10 bytes
      self.raise_exception_if_required_tag_is_missing('Username')
      self.raise_exception_if_not_string('Username')
      self.raise_exception_if_length_is_less_than('Username', 3)
      self.raise_exception_if_length_is_greater_than('Username', 15)
      
      # password is greater than 8 bytes
      self.raise_exception_if_required_tag_is_missing('Password')
      self.raise_exception_if_not_string('Password')
      
      # check the Email
      self.raise_exception_if_required_tag_is_missing('Email')
      self.raise_exception_if_empty('Email')
      #TODO: create a function to verify the email is valid
      
      # check the BrokerID
      self.raise_exception_if_required_tag_is_missing('BrokerID')
      self.raise_exception_if_not_a_integer('BrokerID')

      # Disabling Invalid Brokers
      if self.message.get('BrokerID') not in (-1,8999999,1,3,4,5,8,9,11):
        raise InvalidMessageFieldException(self.raw_message, self.message, "Broker", "FOXBIT")

    elif self.type == 'U10':  # Create Password Reset Request
      self.raise_exception_if_required_tag_is_missing('BrokerID')
      self.raise_exception_if_required_tag_is_missing('Email')

    elif self.type == 'U12':  # Process Password Reset Request
      self.raise_exception_if_required_tag_is_missing('Token')
      self.raise_exception_if_required_tag_is_missing('NewPassword')

    elif self.type == 'U16':  #Enable Disable Two Factor Authentication
      self.raise_exception_if_required_tag_is_missing('Enable')

    elif self.type == 'U18': # Deposit Request
      self.raise_exception_if_required_tag_is_missing('DepositReqID')
      self.raise_exception_if_optional_field_is_a_negative_number('Value')

      if "DepositMethodID" not in self.message and "DepositID" not in self.message  and 'Currency' not in self.message:
        raise InvalidMessageMissingTagException(self.raw_message, self.message, "DepositID,DepositMethodID,Currency")

    elif self.type == 'U19': # Deposit Response
      self.raise_exception_if_required_tag_is_missing('DepositReqID')
      self.raise_exception_if_required_tag_is_missing('DepositID')


    elif self.type == 'U20': # Request Deposit Methods
      self.raise_exception_if_required_tag_is_missing('DepositMethodReqID')

    elif self.type == 'U48': # Deposit Method Request
      self.raise_exception_if_required_tag_is_missing('DepositMethodReqID')
      self.raise_exception_if_required_tag_is_missing('DepositMethodID')


    elif self.type == 'D':  #New Order Single
      self.raise_exception_if_required_tag_is_missing('ClOrdID')
      self.raise_exception_if_not_string('ClOrdID')

      self.raise_exception_if_required_tag_is_missing('Symbol')
      self.raise_exception_if_empty('Symbol')

      self.raise_exception_if_required_tag_is_missing('Side')
      self.raise_exception_if_not_in('Side', ( '1', '2' )) # Only BUY and SELL sides

      self.raise_exception_if_required_tag_is_missing('OrdType')

      self.raise_exception_if_not_in('OrdType', ( '1', '2', '3', '4' )) # market, limited, stop market and stop limited

      if self.get('OrdType') == '2':
        self.raise_exception_if_required_tag_is_missing('Price')  # price is required for limited orders
        self.raise_exception_if_not_a_integer('Price')
        self.raise_exception_if_not_greater_than_zero('Price')
      elif self.get('OrdType') == '3':
        self.raise_exception_if_required_tag_is_missing('StopPx')  # stop price is required for stop orders
        self.raise_exception_if_not_a_integer('StopPx')
        self.raise_exception_if_not_greater_than_zero('StopPx')
      elif self.get('OrdType') == '4':
        self.raise_exception_if_required_tag_is_missing('StopPx')  # stop price is required for stop orders
        self.raise_exception_if_not_a_integer('StopPx')
        self.raise_exception_if_not_greater_than_zero('StopPx')
        self.raise_exception_if_required_tag_is_missing('Price')   # price is required for stop limit orders
        self.raise_exception_if_not_a_integer('Price')
        self.raise_exception_if_not_greater_than_zero('Price')

      self.raise_exception_if_required_tag_is_missing('OrderQty')
      self.raise_exception_if_not_a_integer('OrderQty')
      self.raise_exception_if_not_greater_than_zero('OrderQty')

      #TODO: Validate all fields of New Order Single Message

    elif self.type == 'B': # News
      self.raise_exception_if_required_tag_is_missing('Headline')
      self.raise_exception_if_required_tag_is_missing('LinesOfText')
      self.raise_exception_if_required_tag_is_missing('Text')

      self.raise_exception_if_empty('Headline')
      self.raise_exception_if_not_a_integer('LinesOfText')
      self.raise_exception_if_not_greater_than_zero('LinesOfText')
      self.raise_exception_if_empty('Text')

    elif self.type == 'C': # Email
      self.raise_exception_if_required_tag_is_missing('EmailThreadID')
      self.raise_exception_if_required_tag_is_missing('Subject')
      self.raise_exception_if_required_tag_is_missing('EmailType')

    elif self.type == 'x': # Security List Request
      self.raise_exception_if_required_tag_is_missing('SecurityReqID')
      self.raise_exception_if_required_tag_is_missing('SecurityListRequestType')
      self.raise_exception_if_not_a_integer('SecurityListRequestType')
      self.raise_exception_if_not_in('SecurityListRequestType', (0,1,2,3,4))

    elif self.type == 'y': # Security List
      self.raise_exception_if_required_tag_is_missing('SecurityReqID')
      self.raise_exception_if_required_tag_is_missing('SecurityResponseID')
      self.raise_exception_if_required_tag_is_missing('SecurityRequestResult')

    elif self.type == 'F':  #Order Cancel Request
      has_cl_ord_id = "ClOrdID" in self.message
      has_order_id = "OrderID" in self.message

      if not has_cl_ord_id and not has_order_id:
        self.raise_exception_if_required_tag_is_missing('Side')
        self.raise_exception_if_not_in('Side', ('1', '2'))

      if has_cl_ord_id:
        self.raise_exception_if_not_string('ClOrdID')

    elif self.type == 'U2' :  # User Balance
      self.raise_exception_if_required_tag_is_missing('BalanceReqID')

      self.raise_exception_if_not_a_integer('BalanceReqID')
      self.raise_exception_if_not_greater_than_zero('BalanceReqID')

      #TODO: Validate all fields of Request For Balance Message
    elif self.type == 'U4': #  Orders List
      self.raise_exception_if_required_tag_is_missing('OrdersReqID')
      self.raise_exception_if_empty('OrdersReqID')

    elif self.type == 'U6': # Withdraw Request
      self.raise_exception_if_required_tag_is_missing('WithdrawReqID')
      self.raise_exception_if_required_tag_is_missing('Amount')
      self.raise_exception_if_required_tag_is_missing('Currency')
      self.raise_exception_if_required_tag_is_missing('Method')

      self.raise_exception_if_not_a_integer('WithdrawReqID')
      self.raise_exception_if_not_greater_than_zero('WithdrawReqID')

      self.raise_exception_if_not_a_number('Amount')
      self.raise_exception_if_not_greater_than_zero('Amount')

      self.raise_exception_if_empty('Method')

      if self.get('Type') == 'CRY':
        self.raise_exception_if_required_tag_is_missing('Wallet')
        self.raise_exception_if_empty('Wallet')
      elif self.get('Type') == 'BBT':
        self.raise_exception_if_required_tag_is_missing('Amount')
        self.raise_exception_if_required_tag_is_missing('BankNumber')
        self.raise_exception_if_required_tag_is_missing('BankName')
        self.raise_exception_if_required_tag_is_missing('AccountName')
        self.raise_exception_if_required_tag_is_missing('AccountNumber')
        self.raise_exception_if_required_tag_is_missing('AccountBranch')
        self.raise_exception_if_required_tag_is_missing('CPFCNPJ')

        self.raise_exception_if_empty('BankNumber')
        self.raise_exception_if_empty('BankName')
        self.raise_exception_if_empty('AccountName')
        self.raise_exception_if_empty('AccountNumber')
        self.raise_exception_if_empty('AccountBranch')
        self.raise_exception_if_empty('CPFCNPJ')
    elif self.type == 'U7': # WithdrawResponse
      self.raise_exception_if_required_tag_is_missing('WithdrawReqID')
      self.raise_exception_if_not_a_integer('WithdrawReqID')
      self.raise_exception_if_not_greater_than_zero('WithdrawReqID')

      self.raise_exception_if_required_tag_is_missing('WithdrawID')
      self.raise_exception_if_not_a_integer('WithdrawID')
    elif self.type == 'U8': #WithdrawRefresh
      self.raise_exception_if_required_tag_is_missing('WithdrawID')
      self.raise_exception_if_not_a_integer('WithdrawID')


    elif self.type == 'U24': # WithdrawConfirmationRequest
      self.raise_exception_if_required_tag_is_missing('WithdrawReqID')
      self.raise_exception_if_not_a_integer('WithdrawReqID')
      self.raise_exception_if_not_greater_than_zero('WithdrawReqID')

    elif self.type == 'U25': # WithdrawConfirmationResponse
      self.raise_exception_if_required_tag_is_missing('WithdrawReqID')

    elif self.type == 'U26': # Withdraw List Request
      self.raise_exception_if_required_tag_is_missing('WithdrawListReqID')
      self.raise_exception_if_empty('WithdrawListReqID')
    elif self.type == 'U27': # Withdraw List Response
      self.raise_exception_if_required_tag_is_missing('WithdrawListReqID')
      self.raise_exception_if_empty('WithdrawListReqID')

    elif self.type == 'U28': # Broker List Request
      self.raise_exception_if_required_tag_is_missing('BrokerListReqID')
      self.raise_exception_if_empty('BrokerListReqID')
    elif self.type == 'U29': # Broker List Response
      self.raise_exception_if_required_tag_is_missing('BrokerListReqID')
      self.raise_exception_if_empty('BrokerListReqID')

    elif self.type == 'U30': # DepositList Request
      self.raise_exception_if_required_tag_is_missing('DepositListReqID')
      self.raise_exception_if_empty('DepositListReqID')
    elif self.type == 'U31': # DepositList Response
      self.raise_exception_if_required_tag_is_missing('DepositListReqID')
      self.raise_exception_if_empty('DepositListReqID')

    elif self.type == 'U32': # Trade History Request
      self.raise_exception_if_required_tag_is_missing('TradeHistoryReqID')
      self.raise_exception_if_empty('TradeHistoryReqID')
    elif self.type == 'U33': # Trade History Response
      self.raise_exception_if_required_tag_is_missing('TradeHistoryReqID')
      self.raise_exception_if_empty('TradeHistoryReqID')

    elif self.type == 'U34': # LedgerList Request
      self.raise_exception_if_required_tag_is_missing('LedgerListReqID')
      self.raise_exception_if_empty('LedgerListReqID')
    elif self.type == 'U35': # LedgerList Response
      self.raise_exception_if_required_tag_is_missing('LedgerListReqID')
      self.raise_exception_if_empty('LedgerListReqID')


    elif self.type == 'U38': # Update User Profile Request
      self.raise_exception_if_required_tag_is_missing('UpdateReqID')
      self.raise_exception_if_empty('UpdateReqID')
    elif self.type == 'U39': # Update User Profile Response
      self.raise_exception_if_required_tag_is_missing('UpdateReqID')
      self.raise_exception_if_empty('UpdateReqID')

      self.raise_exception_if_required_tag_is_missing('Profile')
      self.raise_exception_if_empty('Profile')

    elif self.type == 'U40': # Profile Refresh
      self.raise_exception_if_required_tag_is_missing('Profile')
      self.raise_exception_if_empty('Profile')


    elif self.type == 'U42': # Position Request
      self.raise_exception_if_required_tag_is_missing('PositionReqID')
      self.raise_exception_if_empty('PositionReqID')


    elif self.type == 'U44': # Confirm Trusted Address Request
      self.raise_exception_if_required_tag_is_missing('ConfirmTrustedAddressReqID')
      self.raise_exception_if_empty('ConfirmTrustedAddressReqID')
    elif self.type == 'U45': # Confirm Trusted Address Response
      self.raise_exception_if_required_tag_is_missing('ConfirmTrustedAddressReqID')
      self.raise_exception_if_empty('ConfirmTrustedAddressReqID')
    elif self.type == 'U46': # Suggest Trusted Address Publish
      self.raise_exception_if_required_tag_is_missing('SuggestTrustedAddressReqID')
      self.raise_exception_if_empty('SuggestTrustedAddressReqID')


    elif self.type == 'U50': # APIKey List Request
      self.raise_exception_if_required_tag_is_missing('APIKeyListReqID')
      self.raise_exception_if_empty('APIKeyListReqID')
    elif self.type == 'U51': # APIKey List Response
      self.raise_exception_if_required_tag_is_missing('APIKeyListReqID')
      self.raise_exception_if_empty('APIKeyListReqID')

    elif self.type == 'U52': # APIKey Create Request
      self.raise_exception_if_required_tag_is_missing('APIKeyCreateReqID')
      self.raise_exception_if_required_tag_is_missing('Label')
      self.raise_exception_if_required_tag_is_missing('PermissionList')
      self.raise_exception_if_required_tag_is_missing('IPWhiteList')
      self.raise_exception_if_empty('APIKeyCreateReqID')
      self.raise_exception_if_empty('Label')
      self.raise_exception_if_empty('PermissionList')
    elif self.type == 'U53': # APIKey Create Response
      self.raise_exception_if_required_tag_is_missing('APIKeyCreateReqID')
      self.raise_exception_if_required_tag_is_missing('APIKey')
      self.raise_exception_if_required_tag_is_missing('APISecret')
      self.raise_exception_if_required_tag_is_missing('APIPassword')
      self.raise_exception_if_empty('APIKey')
      self.raise_exception_if_empty('APISecret')
      self.raise_exception_if_empty('APIPassword')


    elif self.type == 'U54': # APIKey Revoke Request
      self.raise_exception_if_required_tag_is_missing('APIKeyRevokeReqID')
      self.raise_exception_if_required_tag_is_missing('APIKey')
      self.raise_exception_if_empty('APIKeyRevokeReqID')
      self.raise_exception_if_empty('APIKey')
    elif self.type == 'U55': # APIKey Revoke Response
      self.raise_exception_if_required_tag_is_missing('APIKeyRevokeReqID')
      self.raise_exception_if_empty('APIKeyRevokeReqID')


    elif self.type == 'U70': # Cancel Withdrawal Request
      self.raise_exception_if_required_tag_is_missing('WithdrawCancelReqID')
      self.raise_exception_if_required_tag_is_missing('WithdrawID')

      self.raise_exception_if_empty('WithdrawCancelReqID')
      self.raise_exception_if_not_a_integer('WithdrawID')

    elif self.type == 'U72': # Card List Request
      self.raise_exception_if_required_tag_is_missing('CardListReqID')
      self.raise_exception_if_empty('CardListReqID')

    elif self.type == 'U74': # Card Create Request
      self.raise_exception_if_required_tag_is_missing('CardCreateReqID')
      self.raise_exception_if_required_tag_is_missing('Instructions')
      self.raise_exception_if_empty('CardCreateReqID')

      self.raise_exception_if_empty('Instructions')

    elif self.type == 'U76': # Card Disable Response
      self.raise_exception_if_required_tag_is_missing('CardDisableReqID')
      self.raise_exception_if_required_tag_is_missing('CardID')
      self.raise_exception_if_empty('CardDisableReqID')
      self.raise_exception_if_empty('CardID')

    elif self.type == 'U78': # WithdrawCommentRequest WithdrawReqID
      self.raise_exception_if_required_tag_is_missing('WithdrawReqID')
      self.raise_exception_if_not_a_integer('WithdrawReqID')
      self.raise_exception_if_not_greater_than_zero('WithdrawReqID')
      self.raise_exception_if_required_tag_is_missing('WithdrawID')
      self.raise_exception_if_required_tag_is_missing('Message')

    elif self.type == 'U79': # WithdrawCommentResponse
      self.raise_exception_if_required_tag_is_missing('WithdrawReqID')
      self.raise_exception_if_required_tag_is_missing('WithdrawID')



    elif self.type == 'B0': # Deposit Payment Confirmation
      self.raise_exception_if_required_tag_is_missing('ProcessDepositReqID')
      self.raise_exception_if_empty('ProcessDepositReqID')

      self.raise_exception_if_required_tag_is_missing('Action')
      self.raise_exception_if_not_in('Action', ['CONFIRM', 'CANCEL', 'PROGRESS', 'COMPLETE'])


    elif self.type == 'B2': # Customer List Request
      self.raise_exception_if_required_tag_is_missing('CustomerListReqID')
      self.raise_exception_if_empty('CustomerListReqID')

      pass
    elif self.type == 'B3': # Customer List Response
      pass

    elif self.type == 'B4': # Customer Detail Request
      pass
    elif self.type == 'B5': # Customer Detail Response
      pass

    elif self.type == 'B6': # Process Withdraw
      self.raise_exception_if_required_tag_is_missing('ProcessWithdrawReqID')
      self.raise_exception_if_not_a_integer('ProcessWithdrawReqID')
      self.raise_exception_if_not_greater_than_zero('ProcessWithdrawReqID')

      self.raise_exception_if_required_tag_is_missing('WithdrawID')
      self.raise_exception_if_not_a_integer('WithdrawID')
      self.raise_exception_if_not_greater_than_zero('WithdrawID')

      self.raise_exception_if_required_tag_is_missing('Action')
      self.raise_exception_if_not_in('Action', ['CANCEL', 'PROGRESS', 'COMPLETE'])


    elif self.type == 'B7': # Process Withdraw
      self.raise_exception_if_required_tag_is_missing('ProcessWithdrawReqID')
      self.raise_exception_if_not_a_integer('ProcessWithdrawReqID')
      self.raise_exception_if_not_greater_than_zero('ProcessWithdrawReqID')

      self.raise_exception_if_required_tag_is_missing('WithdrawID')
      self.raise_exception_if_not_a_integer('WithdrawID')
      self.raise_exception_if_not_greater_than_zero('WithdrawID')

      self.raise_exception_if_required_tag_is_missing('Status')

    elif self.type == 'B8': # Verify Customer Request
      self.raise_exception_if_required_tag_is_missing('VerifyCustomerReqID')
      self.raise_exception_if_required_tag_is_missing('ClientID')
      self.raise_exception_if_required_tag_is_missing('Verify')

      self.raise_exception_if_not_a_integer('VerifyCustomerReqID')
      self.raise_exception_if_not_greater_than_zero('VerifyCustomerReqID')

      self.raise_exception_if_not_a_integer('Verify')
      self.raise_exception_if_not_in('Verify', [0,1,2,3,4,5])

      self.raise_exception_if_not_a_integer('ClientID')
      self.raise_exception_if_not_greater_than_zero('ClientID')

      if 'VerificationData' in self.message:
        self.raise_exception_if_empty('VerificationData')

    elif self.type == 'B9': # Verify Customer Response
      self.raise_exception_if_required_tag_is_missing('VerifyCustomerReqID')
      
    elif self.type == 'B12': # Clearing History Request
      self.raise_exception_if_required_tag_is_missing('ClearingHistoryReqID')
      self.raise_exception_if_required_tag_is_missing('Page')
      self.raise_exception_if_required_tag_is_missing('PageSize')
      self.raise_exception_if_not_a_integer('ClearingHistoryReqID')
      self.raise_exception_if_not_a_integer('Page')
      self.raise_exception_if_not_a_integer('PageSize')
    
    elif self.type == 'B13': # Clearing History Response
      self.raise_exception_if_required_tag_is_missing('ClearingHistoryReqID')
    
    elif self.type == 'B14': # Process Clearing Request
      self.raise_exception_if_required_tag_is_missing('ProcessClearingReqID')
      self.raise_exception_if_required_tag_is_missing('Action')
    
    elif self.type == 'B15': # Process Clearing Response
      self.raise_exception_if_required_tag_is_missing('ProcessClearingReqID')
      self.raise_exception_if_required_tag_is_missing('ClearingProcessID')
      self.raise_exception_if_required_tag_is_missing('ClearingStatus')
      self.raise_exception_if_required_tag_is_missing('PartyBrokerID')
      self.raise_exception_if_required_tag_is_missing('CounterPartyBrokerID')
      self.raise_exception_if_required_tag_is_missing('PartyBrokerSettlementAccount')
      self.raise_exception_if_required_tag_is_missing('CounterPartyBrokerSettlementAccount')
      
    
    elif self.type == 'B17': # Process Clearing Refresh
      self.raise_exception_if_required_tag_is_missing('ClearingProcessID')
      self.raise_exception_if_required_tag_is_missing('ClearingStatus')
      self.raise_exception_if_required_tag_is_missing('PartyBrokerID')
      self.raise_exception_if_required_tag_is_missing('CounterPartyBrokerID')
      self.raise_exception_if_required_tag_is_missing('PartyBrokerSettlementAccount')
      self.raise_exception_if_required_tag_is_missing('CounterPartyBrokerSettlementAccount')

    elif self.type == 'S2': # Away Market Ticker Request
      self.raise_exception_if_required_tag_is_missing('AwayMarketTickerReqID')
      self.raise_exception_if_required_tag_is_missing('Market')
      self.raise_exception_if_required_tag_is_missing('Symbol')
      self.raise_exception_if_required_tag_is_missing('BestBid')
      self.raise_exception_if_required_tag_is_missing('BestAsk')
      self.raise_exception_if_required_tag_is_missing('LastPx')
      self.raise_exception_if_required_tag_is_missing('HighPx')
      self.raise_exception_if_required_tag_is_missing('LowPx')
      self.raise_exception_if_required_tag_is_missing('Volume')
      self.raise_exception_if_required_tag_is_missing('VWAP')

      self.raise_exception_if_not_a_integer('AwayMarketTickerReqID')
      self.raise_exception_if_empty('Market')
      self.raise_exception_if_empty('Symbol')
      self.raise_exception_if_not_a_integer('BestBid')
      self.raise_exception_if_not_a_integer('BestBid')
      self.raise_exception_if_not_a_integer('BestAsk')
      self.raise_exception_if_not_a_integer('LastPx')
      self.raise_exception_if_not_a_integer('HighPx')
      self.raise_exception_if_not_a_integer('LowPx')
      self.raise_exception_if_not_a_integer('Volume')
      self.raise_exception_if_not_a_integer('VWAP')


    elif self.type == 'S6': #Rest Api Request
      self.raise_exception_if_required_tag_is_missing('RestAPIReqID')
      self.raise_exception_if_required_tag_is_missing('APIKey')
      self.raise_exception_if_required_tag_is_missing('Signature')
      self.raise_exception_if_required_tag_is_missing('Payload')
      self.raise_exception_if_required_tag_is_missing('DigestMod')
      self.raise_exception_if_required_tag_is_missing('Nonce')
      self.raise_exception_if_required_tag_is_missing('Message')
      self.raise_exception_if_required_tag_is_missing('RemoteIP')

      self.raise_exception_if_not_a_integer('RestAPIReqID')
      self.raise_exception_if_empty('APIKey')
      self.raise_exception_if_empty('Signature')
      self.raise_exception_if_empty('Payload')
      self.raise_exception_if_empty('DigestMod')
      self.raise_exception_if_not_a_integer('Nonce')
      self.raise_exception_if_not_greater_than_zero('Nonce')
      self.raise_exception_if_empty('Message')
      self.raise_exception_if_empty('RemoteIP')

    elif self.type == 'S8': #Set/Update Instrument definition
      self.raise_exception_if_required_tag_is_missing('UpdateReqID')
      self.raise_exception_if_required_tag_is_missing('Symbol')
      self.raise_exception_if_required_tag_is_missing('MinPrice')
      self.raise_exception_if_required_tag_is_missing('MaxPrice')
      
      self.raise_exception_if_not_a_integer('UpdateReqID')
      self.raise_exception_if_not_a_integer('MinPrice')
      self.raise_exception_if_not_a_integer('MaxPrice')
   
    elif self.type == 'S9': #Instrument definition
      self.raise_exception_if_required_tag_is_missing('UpdateReqID')
      self.raise_exception_if_required_tag_is_missing('Symbol')

    elif self.type == 'S12': #Document List Request
      self.raise_exception_if_required_tag_is_missing('DocumentListReqID')
      self.raise_exception_if_required_tag_is_missing('Page')
      self.raise_exception_if_required_tag_is_missing('PageSize')
      self.raise_exception_if_required_tag_is_missing('DocumentName')
      self.raise_exception_if_required_tag_is_missing('Since')
      self.raise_exception_if_not_a_integer('DocumentListReqID')
      self.raise_exception_if_not_a_integer('Page')
      self.raise_exception_if_not_a_integer('PageSize')
      self.raise_exception_if_empty('DocumentName')
      self.raise_exception_if_not_a_integer('Since')
      
    elif self.type == 'S14': # Crypto Network Fee Charge Request
      self.raise_exception_if_required_tag_is_missing('CryptoNetworkFeeChargeReqID')
      self.raise_exception_if_required_tag_is_missing('ClientID')
      self.raise_exception_if_required_tag_is_missing('Currency')
      self.raise_exception_if_required_tag_is_missing('Amount')
      
      self.raise_exception_if_not_a_integer('Amount')
      self.raise_exception_if_not_greater_than_zero('Amount')
    
    elif self.type == 'S16':  # User Logon Report
      self.raise_exception_if_required_tag_is_missing('LogonRptReqID')
      self.raise_exception_if_required_tag_is_missing('UserReqID')
      self.raise_exception_if_required_tag_is_missing('BrokerID')
      self.raise_exception_if_required_tag_is_missing('ClientID')
      self.raise_exception_if_required_tag_is_missing('IsApiKey')
      #self.raise_exception_if_required_tag_is_missing('UserReqType')
      #self.raise_exception_if_required_tag_is_missing('CancelOnDisconnect')
      #self.raise_exception_if_required_tag_is_missing('PermissionList')


  def __contains__(self, value):
    return value in self.message

  def __getitem__(self, key):
    return self.message[key]

  def __setitem__(self, key, val):
    return self.set(key, val)

  def has(self, attr):
    return attr in self.message

  def get(self, attr , default=None):
    if attr not in self.message:
      return  default
    return self.message[attr]

  def set(self, attr, value):
    self.message[attr] = value
    self.raw_message = json.dumps(  dict(self.message.items()  +  {'MsgType' : self.type}.items() ) )
    return self
