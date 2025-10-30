import re
import filter_chunks
from gql_backend import getters


def gpe(chunk, fields):
    response = dict()
    if len(chunk) == 5:
        if re.match("[0-9]{5}", chunk):
            response[chunk] = "GPE"
    else:
        if "Ville" in fields.keys():
            ville = fields.get("Ville").casefold()
            print(ville, " >>> ", chunk)
            if re.search(ville, chunk.casefold()):
                response[chunk] = "GPE"
            elif m:=re.search(ville, chunk.replace(" ", "-").replace("st", "saint").casefold()):
                response[m.group(0).replace("-", " ").replace("saint", "st")] = "GPE"
        if m:=re.search("(?<![0-9])[0-9]{5}\\s?(?!\s?km)", chunk.casefold()):
            response[m.group(0).strip()] = "GPE"
    return response


def person(chunk, fields):
    response = dict()
    if "Email" in fields.keys():
        mail = fields.get("Email").casefold()
        if re.search(mail, chunk.casefold()):
            response[mail] = "PERSON"
    if "Nom" in fields.keys():
        nom =  fields.get("Nom").casefold()
        if re.search(nom, chunk.casefold()):
            response[nom] = "PERSON"
    if "Prénom" in fields.keys():
        prenom = fields.get("Prénom").casefold()
        if re.search(prenom, chunk.casefold()):
            response[prenom] = "PERSON"

    return response


def date(chunk, fields):
    response = dict()
    if "Date de naissance" in fields.keys():
        date = fields.get("Date de naissance").casefold()
        if m:= re.search(date, chunk.casefold()):
            response[m.group(0)] = "DATE"
    if m := re.search(r'[le ]{3}?[0-9]{2}[/ ][0-9]{2}[/ ][0-9]{2,4}(?=!\d+)', chunk):
        p = m.group(0)
        response[p] = "DATE"
    return response


def product(chunk, fields) -> dict:
    response = dict()
    if "Immatriculation de la reprise" in fields.keys():
        immatriculation = fields.get("Immatriculation de la reprise").casefold()

        if re.search(immatriculation, chunk.casefold()):
            response[immatriculation] = "PRODUCT"
    if "Numéro de série (en Majuscule, « E » sur la carte grise)" in fields.keys():
        serie = fields.get("Numéro de série (en Majuscule, « E » sur la carte grise)").casefold()
        if m:=re.search(serie, chunk.casefold()):
            response[m.group(0)]= "PRODUCT"
    if m:=re.search(r'[A-Za-z]{2}[ -]?[0-9]{3}[ -]?[A-Za-z]{2}', chunk):
        response[m.group(0)] = "PRODUCT"
    return response


def cardinal(chunk, fields) -> dict:
    # "Téléphone à rappeler", "Numéro de contrat du distributeur "
    response = dict()
    telephone = fields.get("Téléphone à rappeler").casefold()
    if re.search(telephone, chunk):
        response[telephone] = "CARDINAL"
    elif m:= re.search("([+]33)?\\s?[0-9 ]{10,14}", chunk):
        p = m.group(0)
        response[p] = "CARDINAL"
    elif m := re.search(r'[0-9]{2}[/ .][0-9]{2}[/ .][0-9]{2,4}', chunk):
        p = m.group(0)
        response[p] = "DATE"
    return response


def get_entities(chunk, fields) -> dict:
    entities = {}
    for func in [cardinal, product, gpe, person, date]:
        entities.update(func(chunk, fields))
    print(chunk, entities)
    return entities


def list_entities(chunks: list, fields) -> list:
    return [get_entities(chunk, fields) for chunk in chunks]


# def get_tokens(chunks, entities):
#     # isoler les entités
#     #


def casefold_chunks(chunks: list, entities: list) -> list:
    # Note : tout est casefold dans la première version
    casefold = []
    if chunks:
        while chunks:
            chunk = chunks.pop()
            if chunk:
                cf = chunk.casefold()
                ents = entities.pop()
                for e in ents.keys():
                    if e in cf:
                        e1 = e.strip() + ">" + ents.get(e)
                        cf = cf.replace(e, e1)
                casefold.append(cf)
    tokens = [cfc.split(" ") for cfc in casefold]
    return tokens


def label_chunks(fpath="../data/records.json"):
    df = filter_chunks.load_chunks(fpath)
    df["fields"] = df["ticketField"].apply(getters.get_fields_dict)
    df["entities"] = df.apply(lambda x: list_entities(x["filtered_chunks"], x["fields"]), axis=1)
    # df["casefold_tokens"] = df.apply(lambda x: casefold_chunks(x["filtered_chunks"], x["entities"]), axis=1)
    # df["tokens"] = df.apply(lambda x: prepare_tokens(x["filtred_chunks"], x["entities"]), axis=1)
    # df["examples"]

