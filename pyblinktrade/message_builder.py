import time
import random

class MessageBuilder(object):
  @staticmethod
  def testRequestMessage(request_id=None):
    if request_id:
      return {'MsgType': '1', 'TestReqID': request_id }
    else:
      return {'MsgType': '1', 'TestReqID': int(time.time()*1000)}

  @staticmethod
  def login(broker_id, user, password, second_factor=None):
    if not user or not password:
      raise ValueError('Invalid parameters')

    loginMsg = {
      'UserReqID': 'initial',
      'MsgType' : 'BE',
      'BrokerID': broker_id,
      'Username': user,
      'Password': password,
      'UserReqTyp': '1'
    }
    if second_factor:
      loginMsg['SecondFactor'] = second_factor

    return loginMsg

  @staticmethod
  def getDepositList(status_list, opt_filter=None, client_id=None, page=0, page_size=100,opt_request_id=None):
    if not opt_request_id:
      opt_request_id = random.randint(1,10000000)

    msg = {
      "MsgType":"U30",
      "DepositListReqID":opt_request_id,
      "Page":page,
      "PageSize":page_size,
      "StatusList":status_list
    }
    if client_id:
      msg["ClientID"] = client_id

    if opt_filter:
      msg["Filter"] = opt_filter
    return msg

  @staticmethod
  def updateProfile(update_dict, opt_user_id=None,opt_request_id=None):
    if not opt_request_id:
      opt_request_id = random.randint(1,10000000)

    msg = {
      "MsgType":"U38",
      "UpdateReqID":opt_request_id,
      "Fields": update_dict
    }
    if opt_user_id:
      msg["UserID"] = opt_user_id
    return msg

  @staticmethod
  def getWithdrawList(status_list, opt_filter=None, client_id=None, page=0, page_size=100,opt_request_id=None):
    if not opt_request_id:
      opt_request_id = random.randint(1,10000000)

    msg = {
      "MsgType":"U26",
      "WithdrawListReqID":opt_request_id,
      "Page":page,
      "PageSize":page_size,
      "StatusList":status_list,
    }
    if client_id:
      msg["ClientID"] = client_id
    if opt_filter:
      msg["Filter"] = opt_filter

    return msg

  @staticmethod
  def getBrokerList(status_list, country=None, page=None, page_size=100, opt_request_id=None):
    if not opt_request_id:
      opt_request_id = random.randint(1,10000000)

    msg = {
      'MsgType' : 'U28',
      'BrokerListReqID': opt_request_id
    }
    if page:
      msg['Page'] = page

    if page_size:
      msg['PageSize'] = page_size

    if status_list:
      msg['StatusList'] = status_list

    if country:
      msg['Country'] = country

    return  msg

  @staticmethod
  def verifyCustomer(broker_id, client_id, verify, verification_data, opt_request_id=None):
    if not opt_request_id:
      opt_request_id = random.randint(1,10000000)

    return {
      'MsgType': 'B8',
      'VerifyCustomerReqID': opt_request_id,
      'BrokerID': broker_id,
      'ClientID': client_id,
      'Verify':  verify,
      'VerificationData': verification_data
    }

  @staticmethod
  def processDeposit(action,opt_request_id = None,opt_secret=None,opt_depositId=None,opt_reasonId=None,
                     opt_reason=None,opt_amount=None,opt_percent_fee=None,opt_fixed_fee=None):

    if not opt_request_id:
      opt_request_id = random.randint(1,10000000)

    msg = {
      'MsgType': 'B0',
      'ProcessDepositReqID': opt_request_id,
      'Action': action
    }

    if opt_secret:
      msg['Secret'] = opt_secret

    if opt_depositId:
      msg['DepositID'] = opt_depositId

    if opt_reasonId:
      msg['ReasonID'] = opt_reasonId

    if opt_reason:
      msg['Reason'] = opt_reason

    if opt_amount:
      msg['Amount'] = opt_amount

    if opt_percent_fee:
      msg['PercentFee'] = opt_percent_fee

    if opt_fixed_fee:
      msg['FixedFee'] = opt_fixed_fee

    return msg

  @staticmethod
  def requestBalances(request_id = None, client_id = None):
    if not request_id:
      request_id = random.randint(1,10000000)
    msg = {
      'MsgType': 'U2',
      'BalanceReqID': request_id
    }
    if client_id:
      msg['ClientID'] = client_id
    return msg

  @staticmethod
  def requestPositions(request_id = None, client_id = None):
    if not request_id:
      request_id = random.randint(1,10000000)
    msg = {
      'MsgType': 'U42',
      'PositionReqID': request_id
    }
    if client_id:
      msg['ClientID'] = client_id
    return msg


  @staticmethod
  def requestMarketData(request_id,  symbols, entry_types, subscription_type='1', market_depth=0 ,update_type = '1'):
    if not symbols or not entry_types:
      raise ValueError('Invalid parameters')

    return {
      'MsgType' : 'V',
      'MDReqID': request_id,
      'SubscriptionRequestType': subscription_type,
      'MarketDepth': market_depth,
      'MDUpdateType': update_type,  #
      'MDEntryTypes': entry_types,  # bid , offer, trade
      'Instruments': symbols
    }

  @staticmethod
  def processWithdraw(action, withdrawId, request_id=None, reasonId=None, reason=None, data=None,percent_fee=None,fixed_fee=None):
    if not request_id:
      request_id = random.randint(1,10000000)

    msg = {
      'MsgType': 'B6',
      'ProcessWithdrawReqID': request_id,
      'WithdrawID': withdrawId,
      'Action': action
    }

    if reasonId:
      msg['ReasonID'] = reasonId

    if reason:
      msg['Reason'] = reason

    if data:
      msg['Data'] = data

    if percent_fee:
      msg['PercentFee'] = percent_fee

    if fixed_fee:
      msg['FixedFee']  = fixed_fee

    return msg

  @staticmethod
  def sendLimitedBuyOrder(symbol, qty, price, clientOrderId ):
    if not symbol or not qty or  not qty or not price or not clientOrderId:
      raise ValueError('Invalid parameters')

    if qty <= 0 or price <= 0:
      raise ValueError('Invalid qty or price')

    return {
      'MsgType': 'D',
      'ClOrdID': str(clientOrderId),
      'Symbol': symbol,
      'Side': '1',
      'OrdType': '2',
      'Price': price,
      'OrderQty': qty
    }

  @staticmethod
  def sendLimitedSellOrder(symbol, qty, price, clientOrderId ):
    if not symbol or not qty or  not qty or not price or not clientOrderId:
      raise ValueError('Invalid parameters')

    if qty <= 0 or price <= 0:
      raise ValueError('Invalid qty or price')

    return {
      'MsgType': 'D',
      'ClOrdID': str(clientOrderId),
      'Symbol': symbol,
      'Side': '2',
      'OrdType': '2',
      'Price': price,
      'OrderQty': qty
    }

