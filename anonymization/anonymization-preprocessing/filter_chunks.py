import json
import re
import pandas as pd

from definitions import INPUT_DIR
from resources import dialog_acts


def get_inverted_fields_dict() -> dict:
    """
        getting the ids and description of the system available ticket fields
        :return: dict of fields {"id": "value", "title": "value"}
    """
    field_list = json.load(open("../resources/fields.json", "r"))["ticket_fields"]
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


def apply_filter(chunk):
    for mark in dialog_acts.formulas.keys():
        if  re.search(mark, chunk):
            return 1
    return 0


def filter_chunks(chat):
    chat_history = json.loads(json.loads(chat[0].get("history")))
    chunks = [elem.get("msg").replace("\n", " ").strip() for elem in chat_history if "msg" in elem.keys()]
    filtered_chunks = [chunk for chunk in chunks if apply_filter(chunk) == 0]
    return filtered_chunks


def load_chunks(fpath=f"{INPUT_DIR}records.json"):
    df = pd.read_json(fpath)
    df["filtered_chunks"] = df["chat"].apply(filter_chunks)
    return df

