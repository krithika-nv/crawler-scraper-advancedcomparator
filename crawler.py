from selenium import webdriver
from selenium.webdriver.common.by import By
import chromedriver_autoinstaller
import multiprocessing 
import multiprocessing.pool
import time
import datetime
import re
import pandas as pd
import csv
import sys
import os.path
import logging
from fuzzywuzzy import fuzz

### create condition for wrong number of input cols
### env var with list value

OUTPUT_COLUMN_NAMES = ['Name', 'Developer ID', 'Scraped Playstore App Name', 'Scraped Playstore App Link', 'Scraped Google Developer ID']
INPUT_FILE_COLUMN_NAMES = ["Name", "Developer ID"]

input_file_path = 'sample_input.csv'
search_url_prefix = "https://play.google.com/store/search?q="
search_url_suffix = "&c=apps"
output_file_name_prefix = "./Results_" 
apps_to_compare_list_xpath = "//div[contains(@class,'fUEl2e')]//a"
apps_to_compare_direct_page_xpath = "//div[contains(@class,'ipRz4')]/a"
direct_page_app_name_xpath = "//div[contains(@class,'vWM94c')]"
direct_page_google_developer_id_xpath = "//div[contains(@class,'LbQbAe')]"
search_list_app_name_xpath = "//span[contains(@class,'DdYX5')]"
search_list_google_developer_id_xpath = "//span[contains(@class,'wMUdtb')]"
num_process = 10

output_file_name = output_file_name_prefix + str(datetime.datetime.now().strftime("%d%b%y_%H%M%S")) + ".csv"


def advanced_comparator(title, app):
    """
    Compares input title from input file and scraped app name. 
    Returns average compare ratio.
    """
    title = str(title).lower()
    title=re.sub(r'[:;,\'\]?/]','',title)
    app=re.sub(r'[:;,\'\]?/]','',app.lower())
    average_title_app = (fuzz.partial_ratio(title, app) + fuzz.token_set_ratio( title, app) + fuzz.ratio(title, app) + fuzz.token_sort_ratio(title, app)) / 4
    average_app_title = (fuzz.partial_ratio(app, title) + fuzz.token_set_ratio( app, title) + fuzz.ratio(app, title) + fuzz.token_sort_ratio(app, title)) / 4
    return (average_title_app + average_app_title) / 4
    
def extract_data_from_input_file():
    """
    Extracts input file data into DataFrame.
    Returns individual columns as separate lists.
    """
    try:
        input_file_df = pd.read_csv(input_file_path,encoding='iso-8859-1')
        if len(input_file_df.Name)==0 :
            logging.error('Input file empty')
        else :
            input_titles = input_file_df.loc[:,INPUT_FILE_COLUMN_NAMES[0]].tolist()
            developer_ids = input_file_df.loc[:,INPUT_FILE_COLUMN_NAMES[1]].tolist()
            return input_titles, developer_ids      
    except Exception as e:
        logging.error('Exception in fetching listings from file: {}'.format(e))

def url_construction(partial_input):
    """
    Constructs and returns search URL.
    Allowed values for partial_input are the app names to be searched.
    """
    return search_url_prefix + partial_input.replace(' ','+') + search_url_suffix

def create_chromedriver():
    """
    Creates and returns Chrome webdriver.
    """
    opt = webdriver.ChromeOptions()
    opt.add_argument("--headless")
    return webdriver.Chrome(options=opt)

def write_to_file(output_file_csv_writer, results):
    """
    Writes crawled, scraped and compared results to output csv file.
    Takes csv_writer object and starmap_async's results object as input param.
    """
    output_file_csv_writer.writerows(results)

def crawl_and_scrape_wrapper(data_from_file, output_file_csv_writer):
    """
    Wrapper for worker process target.
    takes input file DataFrame, csv_writer object and Chrome webdriver as input param.
    Creates worker process pool and starts them.
    """
    try:
        crawl_and_scrape_process_pool = multiprocessing.Pool(num_process)
        results = crawl_and_scrape_process_pool.starmap_async(crawl_and_scrape, [(title, developer_id) for title,developer_id in zip(data_from_file[0],data_from_file[1])])
        crawl_and_scrape_process_pool.close()
        crawl_and_scrape_process_pool.join()
    except Exception as e:
        logging.error('Exception in crawl_and_scrape_wrapper process pool creation/exec: {}'.format(e))
    write_to_file(output_file_csv_writer,results.get())

def crawl_and_scrape(title, developer_id):
    """
    Worker process definition.
    Takes input app title, input developer ID and Chrome webdriver as param.
    Returns input app title, input developer_id, matched app_name, app_link, google_developer_id as list.
    """
    try:
        driver = create_chromedriver()
        driver.get(url_construction(title))
        apps_to_compare_list=driver.find_elements(By.XPATH,apps_to_compare_list_xpath)
        apps_to_compare_direct_page = driver.find_elements(By.XPATH,apps_to_compare_direct_page_xpath)
        if len(apps_to_compare_list) == 0 and len(apps_to_compare_direct_page) == 1:
           app_name = apps_to_compare_direct_page[0].find_element(By.XPATH,direct_page_app_name_xpath).text
           app_link = apps_to_compare_direct_page[0].get_attribute("href")
           google_developer_id = apps_to_compare_direct_page[0].find_element(By.XPATH,direct_page_google_developer_id_xpath).text
        else:
            if len(apps_to_compare_list) >= 5:
                number_of_apps_to_be_compared = 6
            else:
                number_of_apps_to_be_compared = len(apps_to_compare_list)+1
            most_likely_match = 0.0
            app_name = "-"
            app_link = "-"
            google_developer_id = "-"
            current_app_name = "-"
            current_google_developer_id = "-"
            for iter_number,app in enumerate(apps_to_compare_list[0:number_of_apps_to_be_compared]):
                current_app_name = app.find_elements(By.XPATH,search_list_app_name_xpath)[iter_number].text
                current_google_developer_id = app.find_elements(By.XPATH,search_list_google_developer_id_xpath)[iter_number].text
                if advanced_comparator(developer_id, current_google_developer_id) > 35:
                    app_link = app.get_attribute("href")
                    app_name = current_app_name 
                    google_developer_id = current_google_developer_id
                    break
                else:
                    current_title_match = advanced_comparator(title,current_app_name)
                    if current_title_match > most_likely_match:
                        most_likely_match = current_title_match
                        app_link = app.get_attribute("href")
                        app_name = current_app_name
                        google_developer_id = current_google_developer_id
        driver.close()
        result_row = [title,developer_id,app_name,app_link,google_developer_id]
        return result_row
    except Exception as e :
        logging.error('Exception in crawl and scrape worker: {}'.format(e))

def starter():
    """
    Starter function for prerequisite object instantiations and trigger crawls and scrapes. 
    """
    try:
        output_file = open(output_file_name, 'a+')
        output_file_csv_writer = csv.writer(output_file)
        output_file_csv_writer.writerow(OUTPUT_COLUMN_NAMES)
    except Exception as e :
        logging.error('Exception in creating output file/writer: {}'.format(e))
    try:
        chromedriver_autoinstaller.install()
    except Exception as e :
        logging.error('Exception installing Chromedriver: {}'.format(e))
    if os.path.isfile(input_file_path):
        data_from_file = extract_data_from_input_file()
        logging.info('{} input file exists. Extracted input file data. Starting crawl_and_scrape.'.format(input_file_path))
        crawl_and_scrape_wrapper(data_from_file, output_file_csv_writer)
    else:
        print(input_file_path + " file does not exist in the given path.")

if __name__ == '__main__':
    # Protecting __main__ caller from multiprocessing. 
    logging.info('Started crawl and scrape at: {}.'.format(datetime.datetime.now()))
    starter()
    logging.info('Published finished results at {}.'.format(datetime.datetime.now()))
