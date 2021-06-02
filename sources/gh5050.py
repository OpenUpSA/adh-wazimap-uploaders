import requests
import pandas as pd
import numpy as np
import logging

OWID_FILE_URL = "https://globalhealth5050.org/?_covid-data=datasettable&_extype=csv"
# OWID_FILE_PATH = "/tmp/global5050-covid-data.csv"
OWID_FILE_PATH = "global5050-covid-data.csv"

USE_COLS_POPULATION = ["Country code","Country","Case & death data by sex?","Cases date","Cases where sex-disaggregated data is available","Cases (% male)","Cases (% female)","Deaths date","Deaths where sex-disaggregated data is available","Deaths (% male)","Deaths (% female)","Deaths in confirmed cases date","Proportion of deaths in confirmed cases (male)","Proportion of deaths in confirmed cases (female)","Proportion of deaths in confirmed cases (Male:female ratio)","Source"]


def download_file():
    gh5050_r = requests.get(OWID_FILE_URL, allow_redirects=True)
    gh5050_r.raise_for_status()
    with open(OWID_FILE_PATH, 'wb') as fd:
        fd.write(gh5050_r.content)


def load_file():
    # Load the Citizen CSV as a pandas dataframe, but only selected columns
    gh5050 = pd.read_csv(OWID_FILE_PATH, delimiter=",", usecols=USE_COLS_POPULATION)

    gh5050['Cases date'] =  pd.to_datetime(gh5050['Cases date'], format='%Y-%m-%d')
    gh5050 = gh5050.rename(columns={'Country code': 'Geography', 'Cases date': 'Date'})

    return gh5050

# GENDER DATA
def write_gender_data():
    """Latest value for each area"""
    gh5050 = load_file()

    # CASES % MALE

    gh1 = gh5050[["Geography", "Date", "Cases (% male)"]]

    gh1 = gh1.pivot(index='Geography', columns='Date')
    gh1.columns = gh1.columns.droplevel(0)
    gh1['LastValue'] = gh1.iloc[:, 1:].ffill(axis=1).iloc[:, -1]
    gh1.drop(gh1.columns.difference(['Geography','LastValue']), 1, inplace=True)
    gh1['Indicator'] = "Male"
    gh1.reset_index(level=0, inplace=True)
    gh1 = gh1.astype(object).replace(np.nan, 'Null')
    gh1 = gh1.rename(columns={'LastValue': 'Count'})

    # CASES % FEMALE

    # gh2 = gh5050[["Geography", "Date", "Cases (% female)"]]

    # gh2 = gh2.pivot(index='Geography', columns='Date')
    # gh2.columns = gh2.columns.droplevel(0)
    # gh2['LastValue'] = gh2.iloc[:, 1:].ffill(axis=1).iloc[:, -1]
    # gh2.drop(gh2.columns.difference(['Geography','LastValue']), 1, inplace=True)
    # gh2['Indicator'] = "Female"
    # gh2.reset_index(level=0, inplace=True)
    # gh2 = gh2.astype(object).replace(np.nan, 'Null')
    # gh2 = gh2.rename(columns={'LastValue': 'Count'})

    tmp = [gh1]
    gender_totals = pd.concat(tmp)

    gender_totals = gender_totals[gender_totals['Count'].notna()]
    file_path = "gh5050_CasesByGenderPercentage.csv"
    gender_totals.to_csv(file_path, index = False, sep=',')
    return file_path