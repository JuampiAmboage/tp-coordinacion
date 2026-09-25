import json


class MsgType:
    DATA = "DATA"
    EOF = "EOF"


def serialize(client_id, msg_type, payload):
    # now we can serialize the message as a JSON string and encode it to bytes
    envelope = {"client_id": client_id, "type": msg_type, "payload": payload}
    return json.dumps(envelope).encode("utf-8")


def deserialize(message):
    envelope = json.loads(message.decode("utf-8"))
    return envelope["client_id"], envelope["type"], envelope["payload"]
