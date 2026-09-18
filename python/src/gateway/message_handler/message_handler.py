import uuid
from common import message_protocol

class MessageHandler:

    def __init__(self):
        self.id = uuid.uuid1()
    
    def serialize_data_message(self, message):
        [fruit, amount] = message
        return message_protocol.internal.serialize([fruit, amount, str(self.id)]) # TODO: podria hacerse de otra manera mas prolijo

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize([str(self.id)])

    def deserialize_result_message(self, message):
        fields = message_protocol.internal.deserialize(message)
        return fields
