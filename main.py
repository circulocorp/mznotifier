from PydoNovosoft.utils import Utils
from PydoNovosoft.scope import MZone
from time import sleep
import json_logging
import logging
import os
import pika
import json


json_logging.ENABLE_JSON_LOGGING = True
json_logging.init()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
log_path = os.path.join('logs', 'mznotifier.log')
logger.addHandler(logging.FileHandler(filename=log_path, mode='w'))
config = Utils.read_config("package.json")
env_cfg = config[os.environ["environment"]]
rabbitmq = env_cfg["RABBITMQ_URL"]

if env_cfg["secrets"]:
    mzone_user = Utils.get_secret("mzone_user")
    mzone_pass = Utils.get_secret("mzone_pass")
    mzone_secret = Utils.get_secret("mzone_secret")
    rabbit_user = Utils.get_secret("rabbitmq_user")
    rabbit_pass = Utils.get_secret("rabbitmq_passw")
else:
    mzone_user = env_cfg["mzone_user"]
    mzone_pass = env_cfg["mzone_pass"]
    mzone_secret = env_cfg["mzone_secret"]
    rabbit_user = env_cfg["rabbitmq_user"]
    rabbit_pass = env_cfg["rabbitmq_passw"]

m = MZone(mzone_user, mzone_pass, mzone_secret, "mz-a3tek", "https://live.mzoneweb.net/mzone61.api/")


def get_subscriptions(template):
    subs = m.get_subscriptions(extra="id eq "+template)["value"]
    addresses = []
    for subj in subs:
        if subj["subscriber"]["phoneMobile"] not in addresses:
            addresses.append(subj["subscriber"]["phoneMobile"])
    return addresses


def address_helper(addresses, token):
    for address in addresses:
        if address["template"] == token:
            return address


def send_to_rabbit(envelop):
    credentials = pika.PlainCredentials(rabbit_user, rabbit_pass)
    parameters = pika.ConnectionParameters(rabbitmq, 5672, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.exchange_declare(exchange='circulocorp', exchange_type='direct', durable=True)
    channel.basic_publish(exchange='circulocorp', routing_key='notificaciones',
                          body=json.dumps(envelop))


def mark_read(messages):
    notifications = []
    for i in messages:
        notifications.append(i["id"])
    status = m.set_notifications_read(notifications)
    if status.status_code == 200 or status.status_code == 204:
        logger.info("Notifications set read mark", extra={'props': {"notifications": messages,
                                                                    "app": config["name"], "label": config["name"]}})
    else:
        logger.error("Problem setting read mark", extra={'props': {"notifications": messages,
                                                                    "app": config["name"], "label": config["name"]}})


def build_message(messages, addresses):
    mq = dict()
    envelops = []
    for message in messages:
        for phone in address_helper(addresses, message["template"])["phones"]:
            enve = dict()
            enve["message"] = message["text"]
            enve["address"] = phone
            envelops.append(enve)

    mq["data"] = envelops
    logger.info("Posting message to RabbitMQ", extra={'props': {"message": mq,
                                                          "app": config["name"], "label": config["name"]}})
    send_to_rabbit(mq)
    mark_read(messages)


def start():
    notifis = m.get_notifications(extra="readUtcTimestamp eq null")["value"]
    logger.info("Reading notifications", extra={'props': {"notifications": notifis,
                                                          "app": config["name"], "label": config["name"]}})
    templates = []
    messages = []
    addresses = []
    for noti in notifis:
        if noti["notificationTemplate_Id"] not in templates:
            templates.append(noti["notificationTemplate_Id"])
        message = dict()
        message["template"] = noti["notificationTemplate_Id"]
        message["text"] = noti["message"]
        message["id"] = noti["id"]
        messages.append(message)

    for temple in templates:
        address = dict()
        address["phones"] = get_subscriptions(temple)
        address["template"] = temple
        addresses.append(address)

    build_message(messages, addresses)


def main():
    print(Utils.print_title("package.json"))
    while True:
        start()
        sleep(300)


if __name__ == '__main__':
    main()
