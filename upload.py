import argparse
from sources import owid, gh5050
import logging
import requests

loglevels = [
    logging.WARNING,
    logging.INFO,
    logging.DEBUG,
]

def main():
    parser = argparse.ArgumentParser(description='Upload data to Africa Data Hub wazimap')
    parser.add_argument("--wazimap-endpoint", type=str, required=True)
    parser.add_argument("--wazimap-token", type=str, required=True)
    parser.add_argument("--dont-upload", action="store_true")
    parser.add_argument("-v", "--verbose", action="count", default=0)

    group = parser.add_argument_group('Datasets')
    group.add_argument("--owid-vaccines-distributed-to-date", metavar="DATASET_ID")
    group.add_argument("--owid-total-vaccinations", metavar="DATASET_ID")
    group.add_argument("--owid-total-tests", metavar="DATASET_ID")
    group.add_argument("--owid-total-cases", metavar="DATASET_ID")
    group.add_argument("--owid-total-deaths", metavar="DATASET_ID")
    group.add_argument("--gh5050-gender-data", metavar="DATASET_ID")

    args = parser.parse_args()

    logging.basicConfig(level=loglevels[args.verbose])

    # VACCINES DISTRIBUTED TO DATE
    if args.owid_vaccines_distributed_to_date:
        logging.info("Downloading OWID file")
        owid.download_file()

        logging.info("Processing vaccines_distributed_to_date")
        file_path = owid.write_vaccines_distributed_to_date()
        logging.info(f"File written to {file_path}")

        if args.dont_upload:
            logging.info(f"Not uploading {file_path} due to --dont-upload")
        else:
            logging.info(f"Uploading {file_path} to dataset id {args.owid_vaccines_distributed_to_date}")
            args.wazimap_endpoint = ''
            args.wazimap_token = ''
            upload(
                args.wazimap_endpoint,
                args.wazimap_token,
                args.owid_vaccines_distributed_to_date,
                file_path
            )

    # TOTAL VACCINATIONS
    if args.owid_total_vaccinations:
        logging.info("Downloading OWID file")
        owid.download_file()

        logging.info("Processing total number of vaccinations administered")
        file_path = owid.write_total_vaccinations()
        logging.info(f"File written to {file_path}")

        if args.dont_upload:
            logging.info(f"Not uploading {file_path} due to --dont-upload")
        else:
            logging.info(f"Uploading {file_path} to dataset id {args.owid_total_vaccinations}")
            args.wazimap_endpoint = ''
            args.wazimap_token = ''
            upload(
                args.wazimap_endpoint,
                args.wazimap_token,
                args.write_total_vaccinations,
                file_path
            )

    # TOTAL TESTS
    if args.owid_total_tests:
        logging.info("Downloading OWID file")
        owid.download_file()

        logging.info("Processing total tests conducted")
        file_path = owid.write_total_tests()
        logging.info(f"File written to {file_path}")

        if args.dont_upload:
            logging.info(f"Not uploading {file_path} due to --dont-upload")
        else:
            logging.info(f"Uploading {file_path} to dataset id {args.owid_total_tests}")
            upload(
                args.wazimap_endpoint,
                args.wazimap_token,
                args.owid_total_tests,
                file_path
            )

    # TOTAL CASES
    if args.owid_total_cases:
        logging.info("Downloading OWID file")
        owid.download_file()

        logging.info("Processing total cases")
        file_path = owid.write_total_cases()
        logging.info(f"File written to {file_path}")

        if args.dont_upload:
            logging.info(f"Not uploading {file_path} due to --dont-upload")
        else:
            logging.info(f"Uploading {file_path} to dataset id {args.owid_total_cases}")
            upload(
                args.wazimap_endpoint,
                args.wazimap_token,
                args.owid_total_cases,
                file_path
            )
    
    # TOTAL DEATHS
    if args.owid_total_deaths:
        logging.info("Downloading OWID file")
        owid.download_file()

        logging.info("Processing total deaths")
        file_path = owid.write_total_deaths()
        logging.info(f"File written to {file_path}")

        if args.dont_upload:
            logging.info(f"Not uploading {file_path} due to --dont-upload")
        else:
            logging.info(f"Uploading {file_path} to dataset id {args.owid_total_deaths}")
            upload(
                args.wazimap_endpoint,
                args.wazimap_token,
                args.owid_total_deaths,
                file_path
            )

    # GENDER DATA
    if args.gh5050_gender_data:
        logging.info("Downloading GH5050 file")
        gh5050.download_file()

        logging.info("Processing gender data")
        file_path = gh5050.write_gender_data()
        logging.info(f"File written to {file_path}")

        if args.dont_upload:
            logging.info(f"Not uploading {file_path} due to --dont-upload")
        else:
            logging.info(f"Uploading {file_path} to dataset id {args.gh5050_gender_data}")
            upload(
                args.wazimap_endpoint,
                args.wazimap_token,
                args.gh5050_gender_data,
                file_path
            )


def upload(wazimap_endpoint, wazimap_token, dataset_id, file_path):
    url = f"{wazimap_endpoint}/api/v1/datasets/{dataset_id}/upload/"

    # headers = {'authorization': f"Token {wazimap_token}"}
    # files = {'file': open(file_path, 'rb')}
    # payload = {'update': True, 'overwrite': True}

    # r = requests.post(url, headers=headers, data=payload, files=files)
    # r.raise_for_status()


if __name__ == "__main__":
    main()
