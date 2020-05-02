from datetime import date, datetime, timedelta
from collections import OrderedDict
from .timeseries_downloader import TimeseriesDownloader, Union, METADATA_KEY, META_INTERVAL_KEY, META_PROVIDER_KEY, META_TICKER_KEY, ATOMS_KEY, Union
from ..utils import key_handler
import json
import yfinance as yf
import requests
import xmltodict

META_REQ_TYPE_KEY = "request"
META_INTERVAL_VALUE = "1d"
META_PROVIDER_VALUE = "gme"

TRANSLATE_ALIASES = {
    "market" : "mercato",
    "limite": "limit",
    "coefficiente": "coefficient",
    "da": "from",
    "a": "to",
    "totale":"total",
    "liquidita" : "liquid assets",
    "limiteimport": "import limit",
    "limiteexport": "export limit",
    "flussoimport": "import flow",
    "flussoexport": "export flow",
    "zona": "zone",
    "transitomwh": "mwh transit"
}

class GMEDownloader:

    def download_between_dates(self, category: str, req_type: str, start: date, end: date, debug: bool = False) -> Union[dict, bool]:
        '''
        Downloads quote data for a single ticker given the start date and end date.

        Parameters:
            category : str
                The GME market where to source data from.
            req_type : str
                The category type of data (see gme_data_dictionary.json for a list of possible values)
            start : datetime
                Must be before end.
            end : datetime
                Must be after or the same day from start.
        Returns:
            False if there has been an error,
            a dict containing "metadata" and "atoms" otherwise.

            metadata is a dict containing at least:
                - request
                - interval
                - provider
                - other data that the atomizer could want to apply to every atom

            atoms is a list of dicts containing:
                - datetime (format Y-m-d H:m:s.ms)
                - other financial values
        '''
        merged_data = None
        for day_diff in range((end - start).days + 1):
            # Decide which day to download
            day = start + timedelta(days=day_diff)
            # Actaully retrieve data
            xml_data = GMEDownloader.__get_data(
                category=category, req_type=req_type, day=day)
            if(xml_data == False):
                return False
            # Parse data into a dict
            try:
                dict_data = xmltodict.parse(xml_data)
            except TypeError as error:
                print("Error while parsing data {} {}: {}".format(
                    category, req_type, error))
                return False
            # Format data properly
            formatted_data = GMEDownloader.__prepare_data(
                dict_data=dict_data)
            if(formatted_data == False):
                print(json.dumps(dict_data, indent=4))
            # Merge data from multiple days
            merged_data = GMEDownloader.__merge_data(
                merged_data, formatted_data)

        return merged_data

    @staticmethod
    def __prepare_data(dict_data: dict) -> dict:
        '''
        Formats raw dict data into a dictionary made of {"metadata" : dict, "atoms" : list}

        Parameters:
            dict_data : dict
                Raw data retrieved from GME and parsed with xmltodict.parse()
        Returns:
            dict formatted properly in {"metadata" : dict, "atoms" : list}
        '''
        formatted_dict = dict()
        try:
            xs_element = dict_data['NewDataSet']['xs:schema']['xs:element']['xs:complexType']['xs:choice']['xs:element']
            if(type(xs_element) == dict or type(xs_element) == OrderedDict):
                req_types = [xs_element['@name']]
            elif(type(xs_element) == list):
                req_types = [dict['@name'] for dict in xs_element]
            else:
                print("req_type is non of the above: ", type(xs_element))
                return False
        except TypeError as error:
            print("Unable to retrieve req_type, ",error)
            return False
        # Extract atoms in an OrderedDict
        ordered_atoms = dict_data['NewDataSet'][req_types[0]]
        # Convert OrderedDict to normal dict
        atoms = json.loads(json.dumps(ordered_atoms))
        # Fix the datetime
        atoms = GMEDownloader.__fix_atoms_datetime(atoms)
        # Lower all keys
        atoms = key_handler.lower_all_keys_deep(atoms)
        # Translate keys
        atoms = key_handler.rename_deep(atoms, TRANSLATE_ALIASES)
        # Append metadata
        formatted_dict[METADATA_KEY] = {
            META_REQ_TYPE_KEY: req_types[0], META_INTERVAL_KEY: META_INTERVAL_VALUE, META_PROVIDER_KEY: META_PROVIDER_VALUE}
        # Append atoms
        formatted_dict[ATOMS_KEY] = atoms
        return formatted_dict

    @staticmethod
    def __get_data(category: str, req_type: str, day: date) -> Union[str, bool]:
        '''
        Prepares request with necessary cookies and post data to bypass required conditions.

        Parameters:
            category : str
                Could be "MGP" or "MI1" to "MI7".
            req_type : str
                Depends on the category, could be "Prezzi" or "Quantita" or "Fabbisogno" and more.
            day : date
                Any date between 2009 circa and today, depends on category and date.
        Returns:
            Retrieved XML file as a string.
            False if there has been errors.
        '''
        session = requests.Session()
        post_data = {
            '__VIEWSTATE': '/wEPDwULLTIwNTEyNDQzNzQPZBYCZg9kFgICAw9kFgJmD2QWBAIMD2QWAmYPZBYCZg9kFgICCQ8PZBYCHgpvbmtleXByZXNzBRxyZXR1cm4gaW52aWFQV0QodGhpcyxldmVudCk7ZAIVD2QWAgIBDw8WAh4NT25DbGllbnRDbGljawUmamF2YXNjcmlwdDp3aW5kb3cub3BlbignP3N0YW1wYT10cnVlJylkZBgBBR5fX0NvbnRyb2xzUmVxdWlyZVBvc3RCYWNrS2V5X18WBQUMY3RsMDAkSW1hZ2UxBRJjdGwwMCRJbWFnZUJ1dHRvbjEFIGN0bDAwJENvbnRlbnRQbGFjZUhvbGRlcjEkc3RhbXBhBSRjdGwwMCRDb250ZW50UGxhY2VIb2xkZXIxJENCQWNjZXR0bzEFJGN0bDAwJENvbnRlbnRQbGFjZUhvbGRlcjEkQ0JBY2NldHRvMvV5e94ExnpHUcybAr1bPdOOHxYDHpQG7fgAyUlbfpUy',
            # '__VIEWSTATEGENERATOR' : 'BD5243C0',
            # '__PREVIOUSPAGE' : 'cZ9asoMdEhcsdMTrKLddyuDgUqrpgV44mkItwJfPMdC5pTBV2YSxs8G-heXd_cSe0LgJT2dUbmEwn5EAxW2CKqwwsuEEvSwkj_TDS8XqtiFWyG906u2-XjhdXsqvVULm0',
            '__EVENTVALIDATION': '/wEdABN5QfIZ0Z09c70NXWGRJiGpcS/s8I39AyxLz4tn+AkBiEW+okpiqwYG+B4aTa9o+s43drX32rKpFiwqoHxZnWEOD4zZrxX92uOlyIx1SyGTQmV8haT0EfVomfKCKov4HgnZl/Xwcz7QqxVnz+OmFVuWzNBM98trssXld5dD73vgQX4H/0z/058uP3NmytG8PXozrkfQ7SmiPGgdsZPdEEV8g/gu4+zhSeI0ttI2ADLh/wU7Nz/6FKjnm2sSszw4FMr8VEDvc+zuMc1oKpjHdCosjDu35o5CUn6umW4JNpE1p4raaQaFnXKaLuO1sKRm4e9ZUwtJIYRkZxZmb4HmgHR6ltkgVwReXnm+EHOYvXjKP0Sd1PBpsO2hEyKj10xH8juA+rwVNruExpEBEKBupGsoUlq8qqob2Hte6ABdfJHWar0vp/uG8tjo+1et9YAPjLg=',
            # 'ctl00$tbTitolo': 'cerca nel sito',
            # 'ctl00$UserName': '',
            # 'ctl00$Password': '',
            'ctl00$ContentPlaceHolder1$CBAccetto1': 'on',
            'ctl00$ContentPlaceHolder1$CBAccetto2': 'on',
            'ctl00$ContentPlaceHolder1$Button1': 'Accetto',
        }
        formatted_date = day.strftime("%Y%m%d")
        url = "https://www.mercatoelettrico.org/It/Tools/Accessodati.aspx?ReturnUrl=/It/WebServerDataStore/{}_{}/{}{}{}.xml".format(
            category, req_type, formatted_date, category, req_type)
        response = session.post(url, data=post_data)
        if(response.status_code == 200):
            return response.text
        return False

    @staticmethod
    def __merge_data(data1: dict, data2: dict) -> dict:
        '''
        Merges two data in one single formatted dict.
        '''
        if(data1 == None):
            return data2
        if(data2 == None):
            return data1
        # Copy all missing metadata values in dict1
        for key in data2[METADATA_KEY]:
            if not data1.get(key) is None:
                data1[key] = data2[key]

        data1[ATOMS_KEY].extend(data2.get(ATOMS_KEY, list()))
        return data1

    @staticmethod
    def __fix_atoms_datetime(atoms: list) -> dict:
        '''
        Converts atom['Data'] and atom['Ora'] in one single datetime format (yyyy-mm-dd HH:MM:ss.sss)

        Returns:
            the given atom with the correct datetime.
        '''
        for atom in atoms:
            if atom['Ora'] == "24":
                time_string = "{}/{}/{}".format(atom['Data']
                                                [6:8], atom['Data'][4:6], atom['Data'][:4])
                atom_timestamp = datetime.strptime(time_string, "%d/%m/%Y")
                atom_timestamp += timedelta(days=1)
            else:
                time_string = "{}/{}/{} {}".format(
                    atom['Data'][6:8], atom['Data'][4:6], atom['Data'][:4], atom['Ora'])
                atom_timestamp = datetime.strptime(time_string, "%d/%m/%Y %H")
            atom['datetime'] = atom_timestamp.strftime(
                "%Y-%m-%d %H:%M:%S.%f")[:-3]
            del atom['Ora']
            del atom['Data']
        return atoms
