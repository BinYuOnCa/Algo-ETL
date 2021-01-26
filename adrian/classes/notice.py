import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from utils.config import email_config


class Notice():
    '''
    Message class which is used to send various types of messages.
    '''
    def __init__(self, content, content_type='ETL Executed'):
        self.content = content
        self.content_type = content_type


    def __str__(self):
        return self.content + '\n' + 'Type:' + self.content_type


    def email_msg(self, logfile=None):
        content_time = datetime.datetime.now()
        mailtext = content_time.strftime(
            '%Y-%m-%d %H:%M:%S') + '    ' + self.content
        msg = MIMEMultipart()
        msg.attach(MIMEText(mailtext, 'plain', 'utf-8'))
        if logfile is not None:
            annex = MIMEApplication(open(logfile, 'rb').read())
            annex.add_header('content-disposition', 'attachment', filename='error.log')
            msg.attach(annex)
        msg['From'] = email_config['sender']
        msg['To'] = email_config['receiver']
        msg['Subject'] = self.content_type
        return msg


    def send_email(self, msg):
        smtpobj = smtplib.SMTP_SSL(
            email_config['smtp_server'], email_config['smtp_port'])
        smtpobj.login(email_config['account'], email_config['password'])
        try:
            smtpobj.sendmail(
                email_config['sender'], email_config['receiver'], msg.as_string())
        except smtplib.SMTPException as e:
            with open('./smtp_err.log', 'a') as f:
                f.write(e)
        smtpobj.quit()

    '''
    This part of code is just for Twilio demo:

    def send_sms(self):
        import os
        from twilio.rest import Client

        account_sid = os.environ['TWILIO_ACCOUNT_SID']
        auth_token = os.environ['TWILIO_AUTH_TOKEN']
        client = Client(account_sid, auth_token)

        message = client.messages \
        .create(
            body = self.content,
            from_='+14160123456',
            to='+16478888888'
        )
        print(message.sid)
    '''


if __name__ == '__main__':
    test = Notice('This is a test message', 'TEST TYPE')
    test.send_email()
