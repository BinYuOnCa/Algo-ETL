import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from twilio.rest import Client

import config.config_parser as conf

def send_sms(sms_body):
    # Your Account Sid and Auth Token from twilio.com/console
    # and set the environment variables. See http://twil.io/secure
    account_sid = conf.sms()["SID"]
    auth_token = conf.sms()["auth_token"]
    from_number = conf.sms()["from_phone"]
    to_number = conf.sms()["to_phone"]
    client = Client(account_sid, auth_token)

    message = client.messages \
        .create(
        body=sms_body,
        from_=from_number,
        to=to_number
    )

def send_email(subject, mail_body):
    #The mail addresses and password
    sender_address = conf.email()["sender"]
    sender_pass = conf.email()["sender_pwd"]
    receiver_address = conf.email()["receiver"]
    #Setup the MIME
    message = MIMEMultipart()
    message['From'] = sender_address
    message['To'] = receiver_address
    message['Subject'] = subject  # The subject line
    mail_content = mail_body
    # The body and the attachments for the mail
    message.attach(MIMEText(mail_content, 'plain'))
    # Create SMTP session for sending the mail
    session = smtplib.SMTP('smtp.gmail.com', 587)  # use gmail with port
    session.starttls()  # enable security
    session.login(sender_address, sender_pass)  # login with mail_id and password
    text = message.as_string()
    session.sendmail(sender_address, receiver_address, text)
    session.quit()
