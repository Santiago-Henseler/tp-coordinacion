import uuid
from common import message_protocol

class MessageHandler:

    def __init__(self):
        self.userId = str(uuid.uuid4())
    
    def serialize_data_message(self, message):
        [fruit, amount] = message
        return message_protocol.internal.serialize([fruit, amount, self.userId])

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize([self.userId])

    def deserialize_result_message(self, message):
        fields = message_protocol.internal.deserialize(message)

        if fields.pop() != self.userId:
            return None

        return fields
