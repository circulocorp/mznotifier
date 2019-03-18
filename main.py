from PydoNovosoft.utils import Utils
from PydoNovosoft.scope import MZone
from datetime import datetime, timedelta
from threading import Thread
from time import sleep
from requests.auth import HTTPBasicAuth
import requests
import sys
import json_logging
import logging
import os
import pika
import json


json_logging.ENABLE_JSON_LOGGING = True
json_logging.init()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler(sys.stdout))
config = Utils.read_config("package.json")

if os.environ is None or "environment" not in os.environ:
    env_cfg = config["dev"]
else:
    env_cfg = config[os.environ["environment"]]

url = env_cfg["API_URL"]
rabbitmq = env_cfg["RABBITMQ_URL"]

if env_cfg["secrets"]:
    mzone_user = Utils.get_secret("mzone_user")
    mzone_pass = Utils.get_secret("mzone_pass")
    mzone_secret = Utils.get_secret("mzone_secret")
    rabbit_user = Utils.get_secret("rabbitmq_user")
    rabbit_pass = Utils.get_secret("rabbitmq_passw")
    api_pass = Utils.get_secret("token_key")
else:
    mzone_user = env_cfg["mzone_user"]
    mzone_pass = env_cfg["mzone_pass"]
    mzone_secret = env_cfg["mzone_secret"]
    rabbit_user = env_cfg["rabbitmq_user"]
    rabbit_pass = env_cfg["rabbitmq_passw"]
    api_pass = env_cfg["token_key"]


def get_subscriptions(template, mz=None):
    subs = mz.get_subscriptions(extra="id eq "+template)["value"]
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
    logger.info("Posting data to RabbitMQ", extra={'props': {"app": config["name"], "label": config["name"]}})
    credentials = pika.PlainCredentials(rabbit_user, rabbit_pass)
    parameters = pika.ConnectionParameters(rabbitmq, 5672, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.exchange_declare(exchange='circulocorp', exchange_type='direct', durable=True)
    channel.basic_publish(exchange='circulocorp', routing_key='notificaciones',
                          body=json.dumps(envelop))


def mark_read(messages, mz):
    notifications = []
    for i in messages:
        notifications.append(i["id"])
    status = mz.set_notifications_read(notifications)
    if status.status_code == 200 or status.status_code == 204:
        logger.info("Notifications set read mark", extra={'props': {"notifications": messages,
                                                                    "app": config["name"], "label": config["name"]}})
    else:
        logger.error("Problem setting read mark", extra={'props': {"notifications": messages,
                                                                    "app": config["name"], "label": config["name"]}})


def build_message(messages, addresses, mz=None, extras=""):
    mq = dict()
    envelops = []
    extra_subscribers = []
    if extras:
        extra_subscribers = extras.split(",")
    for message in messages:
        for phone in address_helper(addresses, message["template"])["phones"]:
            enve = dict()
            enve["message"] = message["text"]
            enve["address"] = phone
            envelops.append(enve)
        if len(extra_subscribers) > 0:
            for extra_s in extra_subscribers:
                enve = dict()
                enve["message"] = message["text"]
                enve["address"] = extra_s
                envelops.append(enve)

    if len(envelops) > 0:
        mq["data"] = envelops
        logger.info("Posting message to RabbitMQ", extra={'props': {"message": json.dumps(mq), "app": config["name"],
                                                                    "label": config["name"]}})
        send_to_rabbit(mq)
        mark_read(messages, mz)
    else:
        logger.info("There is nothing to send to RabbitMQ", extra={'props': {"app": config["name"],
                                                                             "label": config["name"]}})


def start(account):
    logger.info("Searching notifications for "+account["user"], extra={'props': {"app": config["name"],
                                                                                 "label": config["name"]}})
    yesterday = Utils.format_date(datetime.now() - timedelta(hours=12), "%Y-%m-%dT%H:%M:%S")
    m = MZone(account["user"], account["password"], mzone_secret, "mz-a3tek", "https://live.mzoneweb.net/mzone61.api/")
    notifis = m.get_notifications(extra="readUtcTimestamp eq null and utcTimestamp gt "+yesterday+"Z")["value"]
    if len(notifis) > 0:
        logger.info("Reading notifications", extra={'props': {"notifications": notifis,
                                                              "app": config["name"], "label": config["name"]}})
    else:
        logger.info("No notifications found for "+account["user"],
                    extra={'props': {"app": config["name"], "label": config["name"]}})
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
        address["phones"] = get_subscriptions(temple, m)
        address["template"] = temple
        addresses.append(address)

    build_message(messages, addresses, m, account["extraSubscribers"])


# def get_accounts():
#     accounts = []
#     i = 0
#     for user in env_cfg["mzone_users"]:
#         account = dict()
#         account["user"] = user
#         account["pass"] = env_cfg["mzone_passwords"][i]
#         accounts.append(account)
#         i = i + 1
#     return accounts


def get_accounts():
    response = requests.get(url+"/api/notificationadmins", auth=HTTPBasicAuth('circulocorp', api_pass))
    if response.status_code == 200:
        data = response.json()
    else:
        data = None
    return data


def main():
    accounts = get_accounts()
    while True:
        for account in accounts:
            thread = Thread(target=start, args=(account,))
            thread.start()
        sleep(300)


if __name__ == '__main__':
    main()
