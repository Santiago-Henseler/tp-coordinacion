import uuid
from common import message_protocol

class MessageHandler:

    def __init__(self):
        self.userId = str(uuid.uuid4())
        self.totalMsg = 0
    
    def serialize_data_message(self, message):
        [fruit, amount] = message
        self.totalMsg += 1
        return message_protocol.internal.serialize([fruit, amount, self.userId]) # TODO: podria hacerse de otra manera mas prolijo

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize([self.userId])

    def deserialize_result_message(self, message):
        fields = message_protocol.internal.deserialize(message)
        return fields
