#!/usr/bin/env python3
# Friedjof Noweck
# 2022-05-06

import sys
import logging
import configparser

from paho.mqtt import client as mqtt_client
from influxdb_client import InfluxDBClient, QueryApi
from influxdb_client.client.query_api import FluxTable, FluxRecord


# InfluxDB Connector Class
class Model:
    def __init__(
            self, configuration: configparser.SectionProxy,
            logger: logging.Logger, logging_inf: dict = None
    ):
        """
        :param configuration: Configuration form the ini File
        :param logger: The Log Object
        :param logging_inf: More Information about Logs
        """
        if logging_inf is None:
            logging_inf: dict = {"system": "Model"}

        self.configuration: configparser.SectionProxy = configuration
        self.logging_inf: dict = logging_inf
        self.logger: logging.Logger = logger

        self.database: InfluxDBClient = None
        self.api: QueryApi = None

    # Connect Model to InfluxDB
    def connect(self):
        self.database: InfluxDBClient = InfluxDBClient(
            url=self.configuration["url"],
            token=self.configuration["token"],
            org=self.configuration["organisation"],
            bucket=self.configuration["bucket"]
        )

        self.api: QueryApi = self.database.query_api()

        if self.is_connected():
            self.logger.info(f"Model is connected", extra=self.logging_inf)
        else:
            self.logger.warning("Model is not connected", extra=self.logging_inf)

    # True if Model is connected to the InfluxDB
    def is_connected(self) -> bool:
        return self.database.ping()

    # Read Influxql query from file
    def get_query(self) -> str:
        with open(self.configuration["query"], "r") as query_file:
            query: str = query_file.read()

        return query

    # Exec InfluxQL Query
    def get_data(self) -> iter:
        query: str = self.get_query()

        for start, stop in (
                ('-1d', '0d'), ('-2d', '-1d'), ('-3d', '-2d'),
                ('-4d', '-3d'), ('-5d', '-4d'), ('-6d', '-5d'), ('-7d', '-6d')
        ):
            q = query.format(start=start, stop=stop)

            tables: list[FluxTable] = self.api.query(q)

            if len(tables) > 0:
                for table in tables:
                    record: FluxRecord
                    for record in table.records:

                        yield int(record.get_value())
            else:
                yield 0

    # Disconnect from InfluxDB
    def __del__(self):
        self.database.close()


# MQTT Class as API
class API:
    def __init__(
            self, configuration: configparser.SectionProxy,
            logger: logging.Logger, logging_inf: dict = None
    ):
        """
        :param configuration: Configuration form the ini File
        :param logger: The Log Object
        :param logging_inf: More Information about Logs
        """
        if logging_inf is None:
            logging_inf: dict = {"system": "API"}

        self.configuration: configparser.SectionProxy = configuration
        self.logging_inf: dict = logging_inf
        self.logger: logging.Logger = logger

        self.api: mqtt_client.Client = mqtt_client.Client(
            self.configuration["userid"]
        )
        self.trigger_func: any = None

    # On Connection
    def __on_connect(self, client, userdata, flags, rc) -> None:
        if rc == 0:
            self.logger.info("API is connected", extra=self.logging_inf)
            self.api.subscribe(topic=self.configuration["trigger"])
            return

        self.logger.warning("API is not connected", extra=self.logging_inf)
        self.api.subscribe(topic=self.configuration["trigger"])

    # On Message
    def __on_message(self, client, userdata, msg: mqtt_client.MQTTMessage):
        self.logger.debug(f"msg: {msg.topic} = '{msg.payload.decode('UTF-8')}'", extra=self.logging_inf)

        if msg.topic == self.configuration["trigger"]:
            self.send(topic=self.configuration["post"], data=f"{self.trigger_func()}")

    # Connect API to MQTT
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

    # Send Data to Topic
    def send(self, topic: str, data: str) -> None:
        self.api.publish(topic=topic, payload=data)

    # Main Subscribe Loop
    def sub_loop(self) -> None:
        self.connect()

        try:
            self.api.loop_forever()
        except KeyboardInterrupt:
            self.logger.info(">> End <<", extra=self.logging_inf)

    # Disconnect api
    def __del__(self):
        self.api.disconnect()


# The GraphDesigner class
class GraphDesigner:
    def __init__(
            self, model: Model, api: API, configuration: configparser.ConfigParser,
            logger: logging.Logger, logging_inf: dict = None
    ):
        """
        :param model: Database Controller
        :param api: MQTT Controller
        :param configuration: Configuration form the ini File
        :param logger: The Log Object
        :param logging_inf: More Information about Logs
        """
        if logging_inf is None:
            logging_inf: dict = {"system": "GraphDesigner"}

        self.configuration: configparser.ConfigParser = configuration
        self.logging_inf: dict = logging_inf
        self.logger: logging.Logger = logger

        self.model: Model = model
        self.api: API = api

        self.api.trigger_func = self.make

    # Start Main Loop
    def start(self) -> None:
        self.logger.info("Start Graph Designer", extra=self.logging_inf)
        self.model.connect()
        self.api.connect()

        self.api.sub_loop()

    # Select Data from InfluxDB
    def make(self) -> dict:
        self.logger.info("Trigger via API", extra=self.logging_inf)

        data: tuple = tuple(self.model.get_data())

        result: dict = {}

        for nr, d in enumerate(data):
            self.logger.debug(d, extra=self.logging_inf)
            result[f'{nr}'] = d

        return result


if __name__ == "__main__":
    # Logging configuration
    l: logging.Logger = logging.getLogger('GraphDesigner')
    handler: logging.Handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(logging.Formatter(fmt='%(asctime)-24s- %(system)-14s- %(levelname)-8s- %(message)s'))
    l.level = logging.INFO
    l.addHandler(handler)

    # Read Configuration
    config: configparser.ConfigParser = configparser.ConfigParser()
    config.read('influx2mqtt-graph-designer.ini')

    # Init Model (InfluxDB)
    m: Model = Model(configuration=config["influx"], logger=l)
    # Init API (MQTT)
    a: API = API(configuration=config["mqtt"], logger=l)

    # Init Graph Designer
    g: GraphDesigner = GraphDesigner(model=m, api=a, configuration=config, logger=l)
    # Start Designer
    g.start()
