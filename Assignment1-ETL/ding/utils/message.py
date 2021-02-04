import configparser
from mailer import Mailer
from twilio.rest import Client

# get parameters
config = configparser.ConfigParser()
config.read('./settings.ini')

# send email alert
def send_email(email_txt, attach,subject=None):
    mail = Mailer(email=config['email']['sender'],
                  password=config['email']['senderpwd'])

    mail.send(receiver=config['email']['receiver'],
              bcc=config['email']['bcc'],
              subject=config['email']['subject'] if subject is None else subject,
              message=email_txt,
              file=attach)

# send sms alert
def send_sms(sms_txt):
    account_sid = config['sms']['account_sid']
    auth_token = config['sms']['auth_token']
    client = Client(account_sid, auth_token)
    message = client.messages.create(
        body=sms_txt,
        from_=config['sms']['from'],
        to=config['sms']['to']
    )
    return message.sid
