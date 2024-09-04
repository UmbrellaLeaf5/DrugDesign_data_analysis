import requests
from bs4 import BeautifulSoup
import lxml

url_all = "https://www.ebi.ac.uk/chembl/g/#search_results/all"

# ответ с сайта
response = requests.get(url_all)

soup = BeautifulSoup(response.text, "lxml")
print(soup)

# ошибка из-за того, что элементы подгружаются динамически
# data = soup.find("div", class_="col s12 m6 l4")

# name = data.find("p", class_="p-oneline")
# print(name)
