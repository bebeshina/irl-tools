import json
from definitions import RESOURCE_DIR


def get_inverted_fields_dict() -> dict:
    """
        getting the ids and description of the system available ticket fields
        :return: dict of fields {"id": "value", "title": "value"}
    """
    field_list = json.load(open(f"{RESOURCE_DIR}/fields.json", "r"))["ticket_fields"]
    field_dict = {d["id"]: d["title"] for d in field_list}
    return field_dict


fds = get_inverted_fields_dict()


def get_fields_dict(fields):
    dic = dict()
    for field in fields:
        k = field.get("field").get("zendeskId")
        v = field.get("value")
        if v:
            dic[fds.get(int(k))] = v
    return dic