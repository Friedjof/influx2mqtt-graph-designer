# Friedjof Noweck
# 2022-05-06
import sys
import logging
import configparser
from datetime import datetime, date

from paho.mqtt import client as mqtt_client
from influxdb import InfluxDBClient


class Model:
    def __init__(
            self, configuration: configparser.SectionProxy,
            logger: logging.Logger, logging_inf: dict = None
    ):
        if logging_inf is None:
            logging_inf: dict = {"system": "Model"}

        self.configuration: configparser.SectionProxy = configuration
        self.logging_inf: dict = logging_inf
        self.logger: logging.Logger = logger

        self.database: InfluxDBClient = InfluxDBClient()

    def connect(self):
        self.database: InfluxDBClient = InfluxDBClient(
            host=self.configuration["host"],
            port=self.configuration.getint("port"),
            username=self.configuration["username"],
            password=self.configuration["password"],
            database=self.configuration["database"],
            ssl=False
        )
        self.database.switch_database(self.configuration["database"])

        if self.is_connected():
            self.logger.info(f"Model is connected", extra=self.logging_inf)
        else:
            self.logger.warning("Model is not connected", extra=self.logging_inf)

    def is_connected(self) -> bool:
        return len(self.database.get_list_database()) > 0

    def get_data(self) -> dict:
        return self.database.query(
            self.configuration["query"].format(measurement=self.configuration["measurement"])
        ).raw

    def __del__(self):
        self.database.close()


class API:
    def __init__(
            self, configuration: configparser.SectionProxy,
            logger: logging.Logger, logging_inf: dict = None
    ):
        if logging_inf is None:
            logging_inf: dict = {"system": "API"}

        self.configuration: configparser.SectionProxy = configuration
        self.logging_inf: dict = logging_inf
        self.logger: logging.Logger = logger

        self.api: mqtt_client.Client = mqtt_client.Client(
            self.configuration["userid"]
        )
        self.trigger_func: any = None

    def __on_connect(self, client, userdata, flags, rc) -> None:

        if rc == 0:
            self.logger.info("API is connected", extra=self.logging_inf)
        else:
            self.logger.warning("API is not connected", extra=self.logging_inf)

        self.api.subscribe(topic=self.configuration["trigger"])

    def __on_message(self, client, userdata, msg: mqtt_client.MQTTMessage):
        self.logger.debug(f"msg: {msg.topic} = '{msg.payload.decode('UTF-8')}'", extra=self.logging_inf)

        if msg.topic == self.configuration["trigger"]:
            self.send(topic=self.configuration["post"], data=f"{self.trigger_func()}")

    def connect(self) -> None:
        self.api.on_connect = self.__on_connect
        self.api.on_message = self.__on_message

        try:
            self.api.connect(
                host=self.configuration["host"],
                port=self.configuration.getint("port")
            )
        except KeyboardInterrupt:
            self.logger.info(">> END <<", extra=self.logging_inf)

        self.api.username_pw_set(
            username=self.configuration["username"],
            password=self.configuration["password"]
        )

    def send(self, topic: str, data: str) -> None:
        self.api.publish(topic=topic, payload=data)

    def sub_loop(self) -> None:
        self.connect()

        self.api.loop_forever()

    def __del__(self):
        self.api.disconnect()


class GraphDesigner:
    def __init__(
            self, model: Model, api: API, configuration: configparser.ConfigParser,
            logger: logging.Logger, logging_inf: dict = None
    ):
        if logging_inf is None:
            logging_inf: dict = {"system": "GraphDesigner"}

        self.configuration: configparser.ConfigParser = configuration
        self.logging_inf: dict = logging_inf
        self.logger: logging.Logger = logger

        self.model: Model = model
        self.api: API = api

        self.api.trigger_func = self.make

    def start(self) -> None:
        self.logger.info("Start Graph Designer", extra=self.logging_inf)
        self.model.connect()
        self.api.connect()

        self.api.sub_loop()

    def make(self) -> dict:
        self.logger.info("Trigger via API", extra=self.logging_inf)
        today: date = date.today()
        with_today: bool = False

        data: dict = self.model.get_data()
        data: list = data["series"][0]["values"]

        result: dict = {}

        for nr, d in enumerate(data):
            self.logger.debug(d, extra=self.logging_inf)
            result[nr] = d[1]

        return result


if __name__ == "__main__":
    l: logging.Logger = logging.getLogger('GraphDesigner')

    handler: logging.Handler = logging.StreamHandler(stream=sys.stdout)

    handler.setFormatter(logging.Formatter(fmt='%(asctime)-24s- %(system)-14s- %(levelname)-8s- %(message)s'))
    l.level = logging.INFO

    l.addHandler(handler)

    config: configparser.ConfigParser = configparser.ConfigParser()
    config.read('influx2mqtt-graph-designer.ini')

    m: Model = Model(configuration=config["influx"], logger=l)
    a: API = API(configuration=config["mqtt"], logger=l)

    g: GraphDesigner = GraphDesigner(model=m, api=a, configuration=config, logger=l)
    g.start()
