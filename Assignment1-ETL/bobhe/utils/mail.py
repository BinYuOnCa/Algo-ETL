import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from email.mime.text import MIMEText
import base64
from apiclient import errors
import utils.param as param
from utils.log import init_log
from utils.log import output_log


def set_up_gmail_api_credential():
    """Set up gmail api credential
      Args:
      Returns:
        An object containing credential
      """
    creds = None
    scopes = param.GMAIL_SCOPES
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', scopes)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    return creds


def set_up_gmail_api_service(credential):
    """Set up gmail api service
      Args:
        credential: gmail api credential
      Returns:
        An object containing gmail api service
      """
    gmail_service = build('gmail', 'v1', credentials=credential)
    return gmail_service


def create_email_message(sender, to, subject, message_text):
    """Create a message for an email.
      Args:
        sender: Email address of the sender.
        to: Email address of the receiver.
        subject: The subject of the email message.
        message_text: The text of the email message.
      Returns:
        An object containing a base64url encoded email object.
      """
    message = MIMEText(message_text)
    message['to'] = to
    message['from'] = sender
    message['subject'] = subject
    return {'raw': base64.urlsafe_b64encode(
        message.as_string().encode()).decode()}


def send_email_message(service, user_id, message):
    """Send an email message.
    Args:
      service: Authorized Gmail API service instance.
      user_id: User's email address. The special value "me"
      can be used to indicate the authenticated user.
      message: Message to be sent.
    Returns:
      Sent Message.
    """
    try:
        message = (service.users().messages().send(userId=user_id,
                                                   body=message).execute())
        output_log('Mail Message Id: %s ' % message['id'])
        return message
    except errors.HttpError as error:
        output_log('An error occured: %s' % error)


def send_gmail_message(subject,
                       message_text,
                       sender_email=param.FROM_EMAIL_ADDR,
                       to_email=param.TO_EMAIL_ADDR):

    gmail_api_credential = set_up_gmail_api_credential()

    gmail_api_service = set_up_gmail_api_service(gmail_api_credential)

    message_body = create_email_message(sender_email,
                                        to_email,
                                        subject,
                                        message_text)

    return send_email_message(gmail_api_service, 'me', message_body)


if __name__ == "__main__":
    init_log()
    output_log(dir())
