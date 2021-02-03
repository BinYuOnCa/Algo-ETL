# send sms
def send_sms(table, msg):
    account_sid = sms_id
    auth_token = sms_token
    client = Client(account_sid, auth_token)
    message = client.messages.create(
                         body=msg,
                         from_=sms_from,
                         to=sms_to
                     )
    print(message.sid)
