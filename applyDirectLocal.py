
from loguru import logger
from time import sleep
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import pandas as pd
import requests
import json
import config
import xlsxwriter


api_key = config.api_key


def placesQuery(query, api_key):

    #https://www.developers.google.com/maps/documentation/places/web-service/search-text#maps_http_places_textsearch-py
    url = "https://maps.googleapis.com/maps/api/place/textsearch/json?"
    
    r = requests.get(url + 'query=' + query + '&key=' + api_key)
    logger.debug("Getting first page.")
    x = r.json()

    #Up to 60 results to the text query per the API docs, so just try it three times. 
    #Could make a function and make this recursive later. 
    results = []
    firstResults = x['results']
    logger.debug("First query length: " + str(len(firstResults)))
    addResults(results, firstResults)
    if x.get('next_page_token'):
        sleep(2)
        secondResults = runTokenRequest(url, x.get('next_page_token'), api_key)
        if type(secondResults) == tuple:
            pageToken = secondResults[1]
            secondResults = secondResults[0]
        logger.debug("Second query length: " + str(len(secondResults)))
        addResults(results, secondResults)
        try:
            if pageToken:
                sleep(2)
                thirdResults = runTokenRequest(url, pageToken, api_key)
                logger.debug("Third query length: " + str(len(thirdResults)))
                addResults(results, thirdResults)
            else:
                logger.debug("There was no token for a third page.")
        except:
            logger.debug("There was no token for a third page.")
    else:
        logger.debug("There was no token for a second page.")


    return results
    
  
   
def addResults(results, resultSet):

    for rs in resultSet:
        results.append(rs)

    return



def runTokenRequest(url, pageToken, api_key):

    s = requests.get(url + "pagetoken=" + pageToken + "&key=" + api_key)
    logger.debug("Getting next page.")
    y = s.json()
    results = (y['results'])
    if y.get('next_page_token'):
        pageToken = y.get('next_page_token')
        return results, pageToken

    return results



def getPlaceDetails(id, api_key):

    url = "https://maps.googleapis.com/maps/api/place/details/json?place_id=" + id + "&fields=website%2Cformatted_phone_number&key=" + api_key

    payload={}
    headers = {}

    response = requests.get(url, headers=headers, data=payload)
    x = response.json()

    return x



def cleanData(y):

    cleanData = []
    logger.debug("Getting all place details and enriching records...")
    for i in range(len(y)):
        cleanRecord = {}
        details = getPlaceDetails(y[i].get('place_id'), api_key)
        cleanRecord['place_id'] = y[i].get('place_id')
        cleanRecord['name'] = y[i].get('name')
        cleanRecord['status'] = y[i].get('business_status')
        cleanRecord['address'] = y[i].get('formatted_address')
        cleanRecord['rating'] = y[i].get('rating')
        cleanRecord['user_ratings_total'] = y[i].get('user_ratings_total')
        cleanRecord['phone_number'] = details.get('result').get('formatted_phone_number')
        cleanRecord['website'] = details.get('result').get('website')
        cleanData.append(cleanRecord)

    return cleanData



#This isn't getting ALL of the career links for some reason
#Case study - h1ac.com (San Jose HVAC Company)
def scrapeForCareers(cd):

    careerUrls = []
    if cd.get('website'):
        #Clean up the URL for a higher chance of getting the careers page. 
        #This gets rid of area specific URLs/Analytic Landing Pages
        domain = urlparse(cd.get('website')).netloc
        prefix = urlparse(cd.get('website')).scheme
        website = prefix + "://" + domain
        try:
            tempList = []
            logger.debug("Scraping site: " + website)
            page = requests.get(website)
            soup = BeautifulSoup(page.content, "html.parser")
            string = "career"
            for a_href in soup.find_all("a", href=True):
                if string in a_href["href"]:
                    tempList.append(a_href["href"])
            deduped = list(pd.unique(tempList))
            for d in deduped:
                careerUrls.append(d)
        except:
            logger.debug("Couldn't successfully get the web contents from site.")
    else:
        logger.debug("No website for this record.")

    if careerUrls:
        if len(careerUrls) == 1:
            csUrl = urlHandling(careerUrls[0], website)
            return csUrl
        else:
            logger.debug("More than one unique careers URL present in results.")

    return None



def urlHandling(url, website):

    if url.startswith("/"):
        if website.endswith("/"):
            website = website[:-1]
            finalUrl = website + url
        else:
            finalUrl = website + url
    else:
        finalUrl = url

    return finalUrl



def main():

    query = "hvac washington dc"
    y = placesQuery(query, api_key)
    cleaned = cleanData(y)
    for cd in cleaned:
        careersUrl = scrapeForCareers(cd)
        if careersUrl:
            cd['careersPage'] = careersUrl
    
    for cd in cleaned:
        if cd.get('careersPage'):
            cd['tier'] = "Tier 1"
        elif cd.get('phone_number'):
            cd['tier'] = "Tier 2"
        else:
            cd['tier'] = "Tier 3"
        print(json.dumps(cd, indent=4))

    df = pd.DataFrame(data=cleaned)
    df.to_excel("example.xlsx", index=False)

    return


if __name__ == "__main__":
    main()
