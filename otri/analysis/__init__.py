
from typing import Any, Sequence
from sqlalchemy.orm.session import Session
from sqlalchemy.orm.query import Query
from ..filtering.stream import Stream


def db_share_query(session: Session, atoms_table: str, ticker: str, provider: str) -> Query:
    '''
    Rettrieve a query for all atoms for a certain ticker and provider.

    Parameters:
        session : Session
            Database session for the query.\n
        atoms_table : str
            The table containing the atoms. Must have a structure like: (int, json/jsonb), with the
            json field being called "data_json". Can also be directly a table mapped class.
        ticker : str
            The ticker for which to retrieve the atoms.\n
    Returns:
        An sqlalchemy query as described above.
    '''
    t = atoms_table
    return session.query(t.data_json).filter(
        t.data_json['ticker'].astext == ticker
    ).filter(
        t.data_json['provider'].astext == provider
    ).order_by(t.data_json['datetime'].astext).distinct()


class Analysis:
    '''
    Superclass for analysis.
    '''

    def execute(self, in_streams: Sequence[Stream]) -> Any:
        '''
        Starts data analyis.\n

        Parameters:\n
            in_streams : Stream
                Any kind of stream required by the analysis.\n
        Returns:
            Results, depends on the type of analysis.\n
        '''
        raise NotImplementedError("This is an abstract class")
