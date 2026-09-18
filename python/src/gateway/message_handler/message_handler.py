from common import message_protocol


class MessageHandler:

    def __init__(self):
        pass
    
    def serialize_data_message(self, message, userId):
        [fruit, amount] = message
        return message_protocol.internal.serialize([fruit, amount, userId])

    def serialize_eof_message(self, userId):
        return message_protocol.internal.serialize([userId])

    def deserialize_result_message(self, message):
        fields = message_protocol.internal.deserialize(message)
        return fields
