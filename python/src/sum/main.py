import os
import logging
import signal
import sys

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, INPUT_QUEUE)
        self.amount_by_user = {}        
        self.sum_control = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, INPUT_QUEUE)
        self.eof = {}

        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"])
            self.data_output_exchanges.append(data_output_exchange)


    def _process_data(self, fruit, amount, userId):
        if userId not in self.amount_by_user:
            self.amount_by_user[userId] = {}
        
        self.amount_by_user[userId][fruit] = self.amount_by_user[userId].get(fruit, fruit_item.FruitItem(fruit, 0)) + fruit_item.FruitItem(fruit, int(amount))
        

    def _process_eof(self, userId):
        if userId in self.eof:
            if self.eof[userId] != SUM_AMOUNT-1:
                self.sum_control.send(message_protocol.internal.serialize([userId]))
                self.eof[userId] += 1
            return
        
        self.eof[userId] = 1

        node = int(userId.replace("-", ""), 16) % AGGREGATION_AMOUNT    

        for final_fruit_item in self.amount_by_user[userId].values():
            self.data_output_exchanges[node].send(message_protocol.internal.serialize([final_fruit_item.fruit, final_fruit_item.amount, userId]))
        
        self.data_output_exchanges[node].send(message_protocol.internal.serialize([userId]))

        self.amount_by_user[userId] = {}

        for i in range(SUM_AMOUNT):
            self.sum_control.send(message_protocol.internal.serialize([userId]))

    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            self._process_data(*fields)
        else:
            logging.info(f"Me llego {fields}")
            self._process_eof(*fields)

        ack()

    def handle_sigterm(self):
        self.input_queue.close()
        self.sum_control.close()
        for queue in self.data_output_exchanges:
            queue.close()

        sys.exit(0)

    def start(self):
        self.input_queue.start_consuming(self.process_data_messsage)

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    signal.signal(signal.SIGTERM, lambda signum, frame: sum_filter.handle_sigterm())
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
