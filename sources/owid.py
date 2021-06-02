import requests
import pandas as pd
import numpy as np
import logging

OWID_FILE_URL = "https://github.com/owid/covid-19-data/raw/master/public/data/owid-covid-data.csv"
# OWID_FILE_PATH = "/tmp/owid-covid-data.csv"
OWID_FILE_PATH = "owid-covid-data.csv"


USE_COLS_POPULATION = ['iso_code', 'continent', 'date', 'stringency_index', 'population', 'population_density', 'median_age', 'aged_65_older', 'aged_70_older', 'gdp_per_capita',
                       'extreme_poverty', 'cardiovasc_death_rate', 'diabetes_prevalence', 'female_smokers', 'male_smokers', 'handwashing_facilities', 'hospital_beds_per_thousand',
                       'life_expectancy', 'human_development_index', 'new_tests', 'total_tests', 'positive_rate', 'total_vaccinations', 'people_vaccinated', 'people_fully_vaccinated',
                      'new_vaccinations', 'new_vaccinations_smoothed', 'total_vaccinations_per_hundred', 'people_vaccinated_per_hundred', 'people_fully_vaccinated_per_hundred',
                      'total_tests_per_thousand', 'new_tests_per_thousand', 'new_tests_smoothed_per_thousand', 'total_cases_per_million', 'total_deaths_per_million', 'positive_rate']


def download_file():
    owid_r = requests.get(OWID_FILE_URL, allow_redirects=True)
    owid_r.raise_for_status()
    with open(OWID_FILE_PATH, 'wb') as fd:
        fd.write(owid_r.content)


def load_file():
    # Load the Citizen CSV as a pandas dataframe, but only selected columns
    owid = pd.read_csv(OWID_FILE_PATH, delimiter=",", usecols=USE_COLS_POPULATION)

    owid['date'] =  pd.to_datetime(owid['date'], format='%Y-%m-%d')
    owid = owid.rename(columns={'iso_code': 'Geography', 'date': 'Date'})

    owid = owid.replace(to_replace ="OWID_AFR",
                     value ="Africa")
    owid = owid.replace(to_replace ="OWID_ASI",
                     value ="Asia")
    owid = owid.replace(to_replace ="OWID_EUR",
                     value ="Europe")
    owid = owid.replace(to_replace ="OWID_NAM",
                     value ="North America")
    owid = owid.replace(to_replace ="OWID_SAM",
                     value ="South America")
    owid = owid.replace(to_replace ="OWID_OCE",
                     value ="Oceania")
    return owid

# VACCINES DISTRIBUTED TO DATE
def write_vaccines_distributed_to_date():
    """Latest value for each area"""
    owid = load_file()
    # Initial transformation and extraction of vaccinations administered
    vac1 = owid[["Geography", "Date", "people_vaccinated"]]

    vac1 = vac1.pivot(index='Geography', columns='Date')
    vac1.columns = vac1.columns.droplevel(0)
    vac1['LastValue'] = vac1.iloc[:, 1:].ffill(axis=1).iloc[:, -1]
    vac1.drop(vac1.columns.difference(['Geography','LastValue']), 1, inplace=True)
    vac1['Indicator'] = "At least one vaccine dose"
    vac1.reset_index(level=0, inplace=True)
    vac1 = vac1.astype(object).replace(np.nan, 'Null')
    vac1 = vac1.rename(columns={'LastValue': 'Count'})

    vac2 = owid[["Geography", "Date", "people_fully_vaccinated"]]

    vac2 = vac2.pivot(index='Geography', columns='Date')
    vac2.columns = vac2.columns.droplevel(0)
    vac2['LastValue'] = vac2.iloc[:, 1:].ffill(axis=1).iloc[:, -1]
    vac2.drop(vac2.columns.difference(['Geography','LastValue']), 1, inplace=True)
    vac2['Indicator'] = "All doses prescribed by the vaccination protocol"
    vac2.reset_index(level=0, inplace=True)
    vac2 = vac2.astype(object).replace(np.nan, 'Null')
    vac2 = vac2.rename(columns={'LastValue': 'Count'})

    tmp = [vac1, vac2]
    vaccinations_totals = pd.concat(tmp)

    vaccinations_totals = vaccinations_totals[vaccinations_totals['Count'].notna()]
    file_path = "/tmp/owid_Vaccinations_DosesReceived.csv"
    vaccinations_totals.to_csv(file_path, index = False, sep=',')
    return file_path

# TOTAL VACCINATIONS ADMINISTERED
def write_total_vaccinations():

    owid = load_file()

    # OWID: Vaccines per 100 people
    # total_vaccinations_per_hundred
    # Total number of COVID-19 vaccination doses administered per 100 people in the total population
    vac1 = owid[["Geography", "Date", "total_vaccinations_per_hundred"]]
    vac1 = vac1.pivot(index='Geography', columns='Date')
    vac1.columns = vac1.columns.droplevel(0)
    vac1['LastValue'] = vac1.iloc[:, 1:].ffill(axis=1).iloc[:, -1]
    vac1.drop(vac1.columns.difference(['Geography','LastValue']), 1, inplace=True)
    vac1['Indicator'] = "per 100 people"
    vac1.reset_index(level=0, inplace=True)
    vac1 = vac1.astype(object).replace(np.nan, 'Null')
    vac1 = vac1.rename(columns={'LastValue': 'Count'})

    # OWID COVID-19 - Total tests conducted

    # people_vaccinated_per_hundred
    # Total number of people who received at least one vaccine dose per 100 people in the total population
    # vac2 = owid[["Geography", "Date", "people_vaccinated_per_hundred"]]
    # vac2 = vac2.pivot(index='Geography', columns='Date')
    # vac2.columns = vac2.columns.droplevel(0)
    # vac2['LastValue'] = vac2.iloc[:, 1:].ffill(axis=1).iloc[:, -1]
    # vac2.drop(vac2.columns.difference(['Geography','LastValue']), 1, inplace=True)
    # vac2['Indicator'] = "Total number of people who received at least one vaccine dose per 100 people in the total population"
    # vac2.reset_index(level=0, inplace=True)
    # vac2 = vac2.astype(object).replace(np.nan, 'Null')
    # vac2 = vac2.rename(columns={'LastValue': 'Count'})

    # people_fully_vaccinated_per_hundred
    # Total number of people who received at least one vaccine dose per 100 people in the total population
    # vac3 = owid[["Geography", "Date", "people_fully_vaccinated_per_hundred"]]
    # vac3 = vac3.pivot(index='Geography', columns='Date')
    # vac3.columns = vac3.columns.droplevel(0)
    # vac3['LastValue'] = vac3.iloc[:, 1:].ffill(axis=1).iloc[:, -1]
    # vac3.drop(vac3.columns.difference(['Geography','LastValue']), 1, inplace=True)
    # vac3['Indicator'] = "Total number of people who received all doses prescribed by the vaccination protocol per 100 people in the total population"
    # vac3.reset_index(level=0, inplace=True)
    # vac3 = vac3.astype(object).replace(np.nan, 'Null')
    # vac3 = vac3.rename(columns={'LastValue': 'Count'})

    tmp = [vac1]
    vaccinations_totals = pd.concat(tmp)

    vaccinations_totals = vaccinations_totals[vaccinations_totals['Count'].notna()]
    file_path = "owid_Vaccines_Per100People.csv"
    vaccinations_totals.to_csv(file_path, index = False, sep=',')
    return file_path

# TESTS
def write_total_tests():

    owid = load_file()

    # OWID: Total Tests per 1000
    # total_tests_per_thousand
    # Total number of COVID-19 vaccination doses administered per 100 people in the total population
    vac1 = owid[["Geography", "Date", "total_tests_per_thousand"]]
    vac1 = vac1.pivot(index='Geography', columns='Date')
    vac1.columns = vac1.columns.droplevel(0)
    vac1['LastValue'] = vac1.iloc[:, 1:].ffill(axis=1).iloc[:, -1]
    vac1.drop(vac1.columns.difference(['Geography','LastValue']), 1, inplace=True)
    vac1['Indicator'] = "per 1,000 people"
    vac1.reset_index(level=0, inplace=True)
    vac1 = vac1.astype(object).replace(np.nan, 'Null')
    vac1 = vac1.rename(columns={'LastValue': 'Count'})

    tmp = [vac1]
    tests_totals = pd.concat(tmp)

    tests_totals = tests_totals[tests_totals['Count'].notna()]
    file_path = "owid_TotalTests_Per1000People.csv"
    tests_totals.to_csv(file_path, index = False, sep=',')
    return file_path

# CASES 
def write_total_cases():

    owid = load_file()

    # OWID: Total Cases per Million
    # total_cases_per_million
    # Total confirmed cases of COVID-19 per 1,000,000 people
    vac1 = owid[["Geography", "Date", "total_cases_per_million"]]
    vac1 = vac1.pivot(index='Geography', columns='Date')
    vac1.columns = vac1.columns.droplevel(0)
    vac1['LastValue'] = vac1.iloc[:, 1:].ffill(axis=1).iloc[:, -1]
    vac1.drop(vac1.columns.difference(['Geography','LastValue']), 1, inplace=True)
    vac1['Indicator'] = "per 1,000,000 people"
    vac1.reset_index(level=0, inplace=True)
    vac1 = vac1.astype(object).replace(np.nan, 'Null')
    vac1 = vac1.rename(columns={'LastValue': 'Count'})

    tmp = [vac1]
    cases_totals = pd.concat(tmp)

    cases_totals = cases_totals[cases_totals['Count'].notna()]
    file_path = "owid_TotalCases_PerMillionPeople.csv"
    cases_totals.to_csv(file_path, index = False, sep=',')
    return file_path

# DEATHS
def write_total_deaths():

    owid = load_file()

    # OWID: Total Deaths per Million
    # total_deaths_per_million
    # Total deaths attributed to COVID-19 per 1,000,000 people
    vac1 = owid[["Geography", "Date", "total_deaths_per_million"]]
    vac1 = vac1.pivot(index='Geography', columns='Date')
    vac1.columns = vac1.columns.droplevel(0)
    vac1['LastValue'] = vac1.iloc[:, 1:].ffill(axis=1).iloc[:, -1]
    vac1.drop(vac1.columns.difference(['Geography','LastValue']), 1, inplace=True)
    vac1['Indicator'] = "per 1,000,000 people"
    vac1.reset_index(level=0, inplace=True)
    vac1 = vac1.astype(object).replace(np.nan, 'Null')
    vac1 = vac1.rename(columns={'LastValue': 'Count'})

    tmp = [vac1]
    deaths_totals = pd.concat(tmp)

    deaths_totals = deaths_totals[deaths_totals['Count'].notna()]
    file_path = "owid_TotalDeaths_PerMillionPeople.csv"
    deaths_totals.to_csv(file_path, index = False, sep=',')
    return file_path


