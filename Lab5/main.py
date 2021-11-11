import numpy as np
import pickle

import psycopg2 as pg
import pandas.io.sql as psql
import pandas as pd

from typing import Union, List, Tuple

connection = pg.connect(host='pgsql-196447.vipserv.org', port=5432, dbname='wbauer_adb', user='wbauer_adb', password='adb2020');

def film_in_category(category:Union[int,str])->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o tytuł filmu, język, oraz kategorię dla zadanego:
        - id: jeżeli categry jest int
        - name: jeżeli category jest str, dokładnie taki jak podana wartość
    Przykład wynikowej tabeli:
    |   |title          |languge    |category|
    |0	|Amadeus Holy	|English	|Action|
    
    Tabela wynikowa ma być posortowana po tylule filmu i języku.
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
    
    Parameters:
    category (int,str): wartość kategorii po id (jeżeli typ int) lub nazwie (jeżeli typ str)  dla którego wykonujemy zapytanie
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if isinstance(category,int):
        sql_code = f"""select f.title, l.name as languge, c.name as category
                    from film f
                    join language l on l.language_id = f.language_id
                    join film_category fc on fc.film_id = f.film_id
                    join category c on c.category_id = fc.category_id
                    where c.category_id = {category}
                    order by f.title, languge"""
        df = pd.read_sql(sql_code, con=connection)
        return df

    if isinstance(category,str):
        sql_code = f"""select f.title, l.name as languge, c.name as category
                    from film f
                    join language l on l.language_id = f.language_id
                    join film_category fc on fc.film_id = f.film_id
                    join category c on c.category_id = fc.category_id
                    where c.name like '{category}'
                    order by f.title, languge"""
        df = pd.read_sql(sql_code, con=connection)
        return df
    else: 
        return None
    
def film_in_category_case_insensitive(category:Union[int,str])->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o tytuł filmu, język, oraz kategorię dla zadanego:
        - id: jeżeli categry jest int
        - name: jeżeli category jest str
    Przykład wynikowej tabeli:
    |   |title          |languge    |category|
    |0	|Amadeus Holy	|English	|Action|
    
    Tabela wynikowa ma być posortowana po tylule filmu i języku.
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
    
    Parameters:
    category (int,str): wartość kategorii po id (jeżeli typ int) lub nazwie (jeżeli typ str)  dla którego wykonujemy zapytanie
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if isinstance(category,int):
        sql_code = f"""select f.title, l.name as languge, c.name as category
                    from film f
                    join language l on l.language_id = f.language_id
                    join film_category fc on fc.film_id = f.film_id
                    join category c on c.category_id = fc.category_id
                    where c.category_id = {category}
                    order by f.title, languge"""
        df = pd.read_sql(sql_code, con=connection)
        return df

    if isinstance(category,str):
        sql_code = f"""select f.title, l.name as languge, c.name as category
                    from film f
                    join language l on l.language_id = f.language_id
                    join film_category fc on fc.film_id = f.film_id
                    join category c on c.category_id = fc.category_id
                    where c.name ilike '{category}'
                    order by f.title, languge"""
        df = pd.read_sql(sql_code, con=connection)
        return df
    else: 
        return None
    
    
def film_cast(title:str)->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o obsadę filmu o dokładnie zadanym tytule.
    Przykład wynikowej tabeli:
    |   |first_name |last_name  |
    |0	|Greg       |Chaplin    | 
    
    Tabela wynikowa ma być posortowana po nazwisku i imieniu klienta.
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    title (int): wartość id kategorii dla którego wykonujemy zapytanie
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if isinstance(title,str):
        sql_code = f"""select a.first_name, a.last_name
                    from actor a
                    join film_actor fa on fa.actor_id = a.actor_id
                    join film f on f.film_id = fa.film_id
                    where f.title like '{title}'
                    order by a.last_name, a.first_name"""
        df = pd.read_sql(sql_code, con=connection)
        return df
    else:
        return None
    

def film_title_case_insensitive(words:list) :
    ''' Funkcja zwracająca wynik zapytania do bazy o tytuły filmów zawierających conajmniej jedno z podanych słów z listy words.
    Przykład wynikowej tabeli:
    |   |title              |
    |0	|Crystal Breaking 	| 
    
    Tabela wynikowa ma być posortowana po nazwisku i imieniu klienta.

    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    words(list): wartość minimalnej długości filmu
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    
    if isinstance(words,list):
        words_ = '|'.join(map(str, words))
        
        sql_code = f"""select title
                    from film
                    where title ~* '(?:^| )({words_})""" + """{1,}(?:$| )'
                    order by title"""
        df = pd.read_sql(sql_code, con=connection)
        return df
    else:
        return None
    
    
    
    