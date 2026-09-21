import os
import logging
import bisect
import signal
import sys

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"])
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, OUTPUT_QUEUE)
        self.fruit_top = {}
        self.eof = {}

    def _process_data(self, fruit, amount, userId):
        logging.info("Processing data message")
        
        if userId not in self.fruit_top:
            self.fruit_top[userId] = []

        for i in range(len(self.fruit_top[userId])):
            if self.fruit_top[userId][i].fruit == fruit:
                current = self.fruit_top[userId].pop(i)
                current = current + fruit_item.FruitItem(fruit, amount)
                bisect.insort(self.fruit_top[userId], current)
                return
            
        bisect.insort(self.fruit_top[userId], fruit_item.FruitItem(fruit, amount))

    def _process_eof(self, userId):
        logging.info("Received EOF")

        if userId not in self.eof:
            self.eof[userId] = 1
        else:
            self.eof[userId] += 1

        if self.eof[userId] == SUM_AMOUNT:
            fruit_chunk = list(self.fruit_top[userId][-TOP_SIZE:])
            fruit_chunk.reverse()
            fruit_top = list(map(lambda fruit_item: (fruit_item.fruit, fruit_item.amount), fruit_chunk))

            fruit_top.append(userId)

            self.output_queue.send(message_protocol.internal.serialize(fruit_top))
            
            self.fruit_top[userId] = []

    def process_messsage(self, message, ack, nack):
        logging.info("Process message")
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            self._process_data(*fields)
        else:
            self._process_eof(*fields)
        ack()

    def handle_sigterm(self):
        self.input_exchange.close()
        self.output_queue.close()
        sys.exit(0)

    def start(self):
        self.input_exchange.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    signal.signal(signal.SIGTERM, lambda signum, frame: aggregation_filter.handle_sigterm())
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()
