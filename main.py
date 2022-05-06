# Friedjof Noweck
# 2022-05-06
import configparser


class GraphMaker:
    def __init__(self):
        pass


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read('graph_maker.ini')
    g = GraphMaker()
