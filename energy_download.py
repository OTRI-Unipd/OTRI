from downloader.gme_downloader import GMEDownloader
from pathlib import Path
from datetime import date, datetime
import timeseries_download
import json

DATA_FOLDER = Path("data")
CATEGORY_LISTS_FOLDER = Path("downloader/extra")

DOWNLOADERS = {
    "GME": GMEDownloader()
}

def retrieve_categories_list(categories_list_path: Path):
    doc = json.load(categories_list_path.open("r"))
    return [category_dict for category_dict in doc['categories']]


def ask_date(date_name: str) -> date:
    '''
    Asks for a date until the user inputs it correctly
    '''
    while(1):
        chosen_date = input("Enter {}: ".format(date_name))
        try:
            parsed_datetime = datetime.strptime(chosen_date, "%d-%m-%Y")
            break
        except:
            try:
                parsed_datetime = datetime.strptime(chosen_date, "%Y-%m-%d")
                break
            except:
                print("Unable to parse given date")
    return date(parsed_datetime.year, parsed_datetime.month, parsed_datetime.day)


def get_filename(category: str, req_type: str, start_date: date, end_date: date) -> str:
    return "{}_{}_from_{}-{}-{}_to_{}-{}-{}.json".format(
        category,
        req_type,
        start_date.day,
        start_date.month,
        start_date.year,
        end_date.day,
        end_date.month,
        end_date.year
    )


if __name__ == "__main__":
    # First, let's check if DATA_FOLDER is created
    timeseries_download.check_and_create_folder(DATA_FOLDER)
    downloader_name = timeseries_download.choose_downloader(DOWNLOADERS)
    downloader = DOWNLOADERS[downloader_name]
    service_data_folder = Path(DATA_FOLDER, downloader_name)
    timeseries_download.check_and_create_folder(service_data_folder)

    # Choose caregories list file
    categories_list_path = timeseries_download.choose_tickers_file(
        CATEGORY_LISTS_FOLDER)

    # Create a subfolder named like the chosen file
    categories_list_data_folder = Path(
        service_data_folder, categories_list_path.name.replace('.json', ''))
    timeseries_download.check_and_create_folder(categories_list_data_folder)

    # Retrieve the ticker list from the chosen file
    categories = retrieve_categories_list(categories_list_path)
    start_date = ask_date("start date")
    end_date = ask_date("end date")

    # Create a folder inside data/service_name/categories_list_filename/ with a proper name
    datafolder = Path(categories_list_data_folder, timeseries_download.get_datafolder_name(
        "1m", start_date=start_date, end_date=end_date))
    timeseries_download.check_and_create_folder(datafolder)

    for category in categories:
        for req_type in category['types']:
            print("Working on {} {}".format(category['name'], req_type))
            # Prepare the filename
            filename = get_filename(
                category=category['name'], req_type=req_type, start_date=start_date, end_date=end_date)
            # Actually download data
            downloaded_data = downloader.download_between_dates(
                category=category['name'], req_type=req_type, start=start_date, end=end_date)
            if(downloaded_data == False):
                print("Unable to download {} {}".format(category['name'], req_type))
                continue
            # Write data in the chosen file
            timeseries_download.write_in_file(Path(datafolder, filename), downloaded_data)
            print("OK {} {}".format(category['name'], req_type))
