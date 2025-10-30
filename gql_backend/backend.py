import asyncio
import os
import dotenv
import pandas as pd
from gql_backend import getters

dotenv.load_dotenv()
import queries
from gql import Client
from gql.transport import exceptions
from gql.transport.aiohttp import AIOHTTPTransport
from graphql.error import graphql_error


async def request_ticket_chat(tid):
    query = queries.prepare_ticket_query(tid)
    trans = AIOHTTPTransport(url="https://data.meetdeal.fr/graphql",
                             headers={"Authorization": "Bearer %s" % os.getenv("token")})
    """
        Using `async with` on the client will start a connection on the transport
        and provide a `session` variable to execute queries on this connection
    """
    async with Client(
            transport=trans,
            fetch_schema_from_transport=True,
            execute_timeout=3000
    ) as session:
        try:
            result = await session.execute(query)
            ticket_data = result.get("ticket")
            df = pd.DataFrame.from_dict(ticket_data, orient="index").T
            df["field_dict"] = df["ticketField"].apply(getters.get_fields_dict)
            df["Email"] = df["field_dict"].apply(lambda x: x.get("Email"))
            df["nom"] = df["field_dict"].apply(lambda x: x.get("Nom"))
            df["Civilité"] = df["field_dict"].apply(lambda x: x.get("Civilité"))
            df["Tel"] = df["field_dict"].apply(lambda x: x.get("Téléphone à rappeler"))
            df["CP"] = df["field_dict"].apply(lambda x: x.get("Code postal"))
            df.rename(columns={"zendeskId": "ID du ticket"},  inplace=True)
            res = df[["ID du ticket", "Email", "Nom", "Tel", "CP"]]
            return res.iloc[0].values.tolist()

        except graphql_error.GraphQLError as e:
            print("GQL error! ")
            print(e)
            pass
        except exceptions.TransportQueryError as t:
            print(t)
            print("%s" % str(tid))


def run_request_ticket_chat(tid: int) -> list:
    res = asyncio.run(request_ticket_chat(tid))
    return res