from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import pandas as pd
from datetime import date

month = date.today()
month = month.strftime("%B").lower()

def get_four_factors_recent():
    url = 'https://www.nba.com/stats/teams/four-factors/?sort=W_PCT&dir=-1&Season=2020-21&SeasonType=Regular%20Season&LastNGames=5'
    chrome_options = Options()
    driver = webdriver.Chrome(options=chrome_options)
    driver.get(url)
    html = driver.page_source
    table = BeautifulSoup(html, 'html.parser').find(class_="table")

    df_rows = []

    for row in table.find('tbody').find_all('tr'):
        df_cells = []
        for cell in row.children:
            try:
                df_cells.append(cell.get_text())
            except AttributeError:
                pass
        df_rows.append(df_cells)

    return pd.DataFrame(df_rows)