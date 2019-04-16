import json
import boto3
import os
import re
import requests
from bs4 import BeautifulSoup
from datetime import datetime

#Create Date
dt = datetime.now()
date = datetime.strftime(dt,'%y_%m_%d')

#set up S3 resource
s3 = boto3.resource("s3")

#Define input and output buckets and file names
input_bucket = os.getenv("S3_INPUT_BUCKET")
input_folder_path = os.getenv("S3_INPUT_FOLDER_PATH")
neighborhoods_file = os.getenv("NEIGHBORHOODS_FILENAME")
input_obj_path = os.path.join(input_folder_path,neighborhoods_file)

output_bucket = os.getenv("S3_OUTPUT_BUCKET")
output_folder_path = os.getenv("S3_OUTPUT_FOLDER_PATH")
filename = "rent_data_{}.json".format(date)
output_obj_path = os.path.join(output_folder_path, filename)

default_url = "https://www.funda.nl/koop/{}/150000-200000/+{}km/sorteer-prijs-op/"
# e.g. https://www.funda.nl/koop/amsterdam/150000-200000/+5km/sorteer-prijs-op/
url = os.getenv("FUNDA_URL",default_url)

#Define regex pattern to extract numeric information
#pattern = '([0-9]+)(ft2|br)'
#regex = re.compile(pattern)

#Get list of all topics from s3

def build_housing_list():
    """ Pulls file from s3 containing the names of housing lists to be scraped

    Returns:
        Returns a list of topics to scrape.
    """

    obj = s3.Object(input_bucket,input_obj_path)
    contents = obj.get()['Body'].read().decode('utf-8')
    topical_list = contents.split("\n")

    return topical_list

#Creates dictionary with url links
def build_url_dic(housing_list):
    """ build dictionary with topical list as key and funda url as value which is passed
    into get_rental_data() function

    :param housing_list: List of houses to scrape
    :return: Dictionary of topics and url links
    """
    housing = {}
    for house in housing_list:
        fmt = house.split() #e.g. [amsterdam','1']
        link = url.format(fmt) #not sure if this is going to work
        housing[house] = link
    return housing

#Create function to run scrape on all topics
def get_housing_data(housings):
    """ this function loops through all the items with similar topic,
    scrapes craiglist for date for that topics, append to list,
    and uplods a json to s3

    :param housings: is a dictionary containing the names of the topics
    as keys and craiglist URLs as values
    :return:
    """
    #create list to hold all scraped data
    housings_data=[]

    #Loop through housings dict
    for housing,url in housings.items():

        #Retrieve page with the requests module
        response = requests.get(url)

        #Create BeautifulSoup object (library for web parse) parse with 'lxml'
        soup = BeautifulSoup(response.text,'lxml')

        #results are returned as an iterable list
        results = soup.find_all('ol', class_="search-results")

        #Loop through returned results
        for result in results:
            #Error handling
            try:
                #Identify and return the title of the housings
                title = result.find('a',class_="search-result-title").text

                #Identify and return location
                location = result.find('h3',class_="search-result-title").text

                #postcode and city of location
                address = result.find('small',class_="search-result-subtitle").text

                #Identify and return link to housings
                link = result.a['href']

                data = {
                    "title":title,
                    "location": location,
                    "address":address,
                    "link":link
                }
                #append data to list
                housings_data.append(data)
            except:
                continue

        #Load rental data to s3
        obj = s3.Object(output_bucket,output_obj_path)
        obj.put(Body=json.dumps(housings_data,separators=(',',':')))

#Handler Function
def main(event,context):
    """Lambda handler function that runs all the functions to scrape data

    :param event: An AWS Lambda type that is passed to the handler
    :param context: AWS runtime information passed to the handler
    :return:
    """
    housing_list =   build_housing_list()
    housings = build_url_dic(housing_list)
    get_housing_data(housings)

    if __name__ == '__main__':

        #for testing locally
        print("scrapping funda")
        main("", "")

        print("Uploaded:",os.path.join(output_bucket,output_obj_path))
    


