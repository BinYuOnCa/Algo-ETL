import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from twilio.rest import Client
from project_utils.config import TWILIO_CONFIG
from project_utils.config import GMAIL_ACCT_CONFIG


def send_sms(sms_body):
    # Your Account Sid and Auth Token from twilio.com/console
    # and set the environment variables. See http://twil.io/secure
    account_sid = TWILIO_CONFIG["TWILIO_ACCOUNT_SID"]
    auth_token = TWILIO_CONFIG["TWILIO_AUTH_TOKEN"]
    from_number = TWILIO_CONFIG["TWILIO_FROM_PHONE"]
    to_number = TWILIO_CONFIG["TWILIO_TO_PHONE"]
    client = Client(account_sid, auth_token)

    message = client.messages.create(
        body=sms_body,
        from_=from_number,
        to=to_number
    )


# send_sms('Hello, this is a test')


def send_email(subject, mail_body):
    # The mail addresses and password
    sender_address = GMAIL_ACCT_CONFIG["GMAIL_SENDER"]
    sender_pass = GMAIL_ACCT_CONFIG["GMAIL_SENDER_PWD"]
    receiver_address = GMAIL_ACCT_CONFIG["GMAIL_RECEIVER"]

    # Setup the MIME
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


# send_email('this is a test email for stonehobby', 'I love you too')
