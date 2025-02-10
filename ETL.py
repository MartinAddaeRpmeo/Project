''' Python data Extract'''

import requests
import pandas as pd 
from sqlalchemy import create_engine

def extract():

    Api="http://universities.hipolabs.com/search?country=United+States"
    data = requests.get(Api).json()
    return data

def transform(data):
    df=pd.DataFrame(data)
    df_cali=df[df["name"].str.contains("California")]
    
    print(f"the number of universities in California is {len(df_cali)}")
    df["domains"]=[",".join(map(str,i)) for i in df['domains']]
    df["web_page"]=[",".join(map(str,i)) for i in df['web_page']]
    return df[["domains","country", "webpages", 'name']]


def load(df):
    disk_engine=create_engine('sqlite://my_lite_store.db')
    df.to_sql("cal_uni", disk_engine, if_exist="replace")




data=extract()
df=transform(data)
load(df)