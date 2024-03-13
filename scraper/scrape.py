import csv
import time
import datetime
import requests
from bs4 import BeautifulSoup
list_of_rows = []
# for the current year, use disciplinary
# for any previous year, use disciplinary_{year}
    url = 'https://ota.dc.gov/page/scheduled-evictions'

    response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0'})
    html = response.content
    soup = BeautifulSoup(html, features="html.parser")
    table = soup.find('div class="field-items"')
        for row in table.find_all('a'):
            list_of_cells = []
            for cell in row.find_all('td'):
                if cell.find('a'):
                    list_of_cells.append("https://www.mbp.state.md.us" + cell.find('a')['href'])
                text = cell.text.strip()
                list_of_cells.append(text)
            list_of_cells.append(year)
            list_of_rows.append(list_of_cells)
    else:
        for row in table.find_all('tr'):
            list_of_cells = []
            for cell in row.find_all('td'):
                if cell.find('a'):
                    link = "https://www.mbp.state.md.us" + cell.find('a')['href']
                    type, name = cell.text.rsplit(' - ', 1)
                    list_of_cells = [link, name, type]
                else:
                    list_of_cells.append(cell.text)
                    list_of_cells.append(year)
            list_of_rows.append(list_of_cells)
outfile = open("alerts.csv", "w")
writer = csv.writer(outfile)
# i am writing a header row
writer.writerow(["url", "name", "type", "date","year"])
writer.writerows(list_of_rows)