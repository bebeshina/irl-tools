from gql import gql
from graphql import GraphQLError


def prepare_ticket_query(tid: int):
    query_str = """
                    query {
                            ticket(id: "%s") 
                            {
                                zendeskId
                                ticketField 
                                {
                                    field 
                                    {
                                        zendeskId
                                    }
                                value
                                }  
                            }
                    }""" % str(tid)
    query = gql(query_str)
    return query


def prepare_tickets_query(status="hold", offset=0):
    """
    :param status: Any of 'hold', 'open', 'closed', 'pending'
    :param offset: offset while iterating over the dataset
    :return: query
    """
    query_str = """
                    query {
                            tickets(
                                hasChat: true
                                status: %s
                                offset: %s) 
                                {edges 
                                    {node
                                        {
                                            zendeskId
                                            fields
                                            {
                                                ticketField
                                                {
                                                    {
                                                        fieldName
                                                    },
                                                    value
                                                }
                                            }
                                            tags
                                            {
                                                zendeskTagName
        
                                            }
                                            chat
                                            {
                                                history

                                            }
                                        }
                                    }
                                }
                    }""" % (status, str(offset))
    try:
        query = gql(query_str)
    except GraphQLError as e:
        print(e)
        return None
    return query