from twilio.rest import Client
import utils.param as param
from utils.log import init_log
from utils.log import output_log


def send_twilio_message(sms_message):
    # setup twilio client
    twilio_client = Client(param.TWILIO_ACCOUNT_SID, param.TWILIO_AUTH_TOKEN)

    message = twilio_client.messages.create(
                body=sms_message,
                from_=param.FROM_PHONE_NO,
                to=param.TO_PHONE_NO)

    output_log('SMS Message Id: %s ' % message.sid)
    return message


if __name__ == "__main__":
    init_log()
    output_log(dir())
    send_twilio_message('testing')
