import os
import logging
import signal
import sys
import bisect

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, INPUT_QUEUE)
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, OUTPUT_QUEUE)
        self.top = {}
        self.eof = {}

    def process_messsage(self, message, ack, nack):
        try:
            logging.info("Received top")
            fruit_top = message_protocol.internal.deserialize(message)

            userId = fruit_top.pop()

            if userId not in self.top:
                self.top[userId] = []
                self.eof[userId] = 1

            for f in fruit_top:
                fruit = fruit_item.FruitItem(f[0], f[1])
                bisect.insort(self.top[userId], fruit)

            if self.eof[userId] == AGGREGATION_AMOUNT:
                fruit_chunk = list(self.top[userId][-TOP_SIZE:])
                fruit_chunk.reverse()
                top = list(map(lambda fruit_item: (fruit_item.fruit, fruit_item.amount), fruit_chunk))

                top.append(userId)

                self.output_queue.send(message_protocol.internal.serialize(top))
                
                del self.top[userId]
                del self.eof[userId]
            else:
                self.eof[userId] += 1

            ack()
        except Exception as e:
            logging.error(f"{e}")

    def handle_sigterm(self):
        self.input_queue.close()
        self.output_queue.close()
        sys.exit(0)

    def start(self):
        self.input_queue.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    signal.signal(signal.SIGTERM, lambda signum, frame: join_filter.handle_sigterm())
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()
