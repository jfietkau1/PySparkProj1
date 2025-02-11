import requests
from bs4 import BeautifulSoup

def scrape_espn():
    url = "https://www.espn.com/nba/"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")

    # Find top headlines
    headlines = []
    for meta in soup.find_all("meta", property="og:title", limit=15):  # Modify for correct tag/attribute
        headlines.append(meta["content"].strip())
    
    return headlines

print(scrape_espn())


