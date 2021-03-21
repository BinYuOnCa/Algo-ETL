import os
from os.path import basename
import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate

# Requires settings

def send_email(subject="", body="", files=None, additional_recipients=[]):
    # Fetch env variables
    send_from = os.getenv("SMTP_SENDER")
    send_to = [os.getenv("SMTP_RECIPIENT")]
    account_password = os.getenv("SMTP_SENDER_PASSWORD")
 
    send_to.extend(additional_recipients)

    # Config MIME
    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = COMMASPACE.join(send_to)
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject

    msg.attach(MIMEText(body))

    for f in files or []:
        with open(f, "rb") as fil:
            part = MIMEApplication(
                fil.read(),
                Name=basename(f)
            )
        # After the file is closed
        part['Content-Disposition'] = 'attachment; filename="%s"' % basename(f)
        msg.attach(part)
    
    # SMTP
    smtp = smtplib.SMTP_SSL("smtp.gmail.com", 465)
    smtp.login(send_from, account_password)

    smtp.sendmail(send_from, send_to, msg.as_string())

    smtp.close()
