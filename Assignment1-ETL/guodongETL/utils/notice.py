import smtplib
from email.message import EmailMessage

from twilio.rest import Client

from utils.settings import TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, EMAIL_PASSWORD, EMAIL_ACCOUNT, TWILIO_SMS_NUMBER, \
    RECEIVER_NUMBER


def send_sms(msg: str):
    client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
    client.messages.create(
        body=msg,
        from_=TWILIO_SMS_NUMBER,
        to=RECEIVER_NUMBER
    )


def send_email(date, to):
    mail = smtplib.SMTP('smtp.gmail.com', 587)
    mail.ehlo()
    mail.starttls()
    mail.login(EMAIL_ACCOUNT, EMAIL_PASSWORD)
    msg = EmailMessage()

    msg.set_content(f'ETL job finish for {date}')
    msg['Subject'] = f'ETL job finish for {date}'
    msg['From'] = EMAIL_ACCOUNT
    msg['To'] = to

    mail.send_message(msg)

    mail.quit()
