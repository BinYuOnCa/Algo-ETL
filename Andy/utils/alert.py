from .config import cred_info, msg_info, log
import smtplib
from twilio.rest import Client


def send_email(subject, msg):
    '''
    sends email
    :param subject: (Str)
    :param msg: (Str)
    :return:
    '''
    server = smtplib.SMTP('smtp.gmail.com:587')
    server.ehlo()
    server.starttls()

    try:
        server.login(cred_info['email_addr'], cred_info['email_code'])
        server.sendmail(from_addr=cred_info['email_addr'],
                        to_addrs=cred_info['email_addr'],
                        msg='Subject: ' + msg_info['tmp'].format(subject, msg))
        server.quit()
    except Exception as e:
        log.error('Cannot Log in the email, the message is\n {} - {}'.format(
            msg, e))


def send_sms(subject, msg):
    '''
    sends sms
    :param subject: (Str)
    :param msg: (Str)
    :return: 
    '''
    try:
        client = Client(cred_info['sid'], cred_info['token'])
    except Exception as e:
        log.warning(
            'Error in account sid or token, the message is\n {} - {}'.format(
                msg, e))
    try:
        client.messages.create(body='------------------' +
                               msg_info['tmp'].format(subject, msg),
                               from_=cred_info['free_tel'],
                               to=cred_info['personal_tel'])
    except Exception as e:
        log.warning('Error in phone number, the message is\n {} - {}'.format(
            msg, e))


def notification(subject, msg, sms=True, email=True):
    '''
    sends notification via sms or email or both
    :param subject: (Str)
    :param msg: (Str)
    :param sms: (Bool)
    :param email: (Bool)
    '''
    if sms:
        send_sms(subject, msg)
    if email:
        send_email(subject, msg)
