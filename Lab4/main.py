import numpy as np
import pickle

import psycopg2 as pg
import pandas.io.sql as psql
import pandas as pd

from typing import Union, List, Tuple

connection = pg.connect(host='pgsql-196447.vipserv.org', port=5432, dbname='wbauer_adb', user='wbauer_adb', password='adb2020');

def film_in_category(category_id:int)->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o tytuł filmu, język, oraz kategorię dla zadanego id kategorii.
    Przykład wynikowej tabeli:
    |   |title          |languge    |category|
    |0	|Amadeus Holy	|English	|Action|
    
    Tabela wynikowa ma być posortowana po tylule filmu i języku.
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
    
    Parameters:
    category_id (int): wartość id kategorii dla którego wykonujemy zapytanie
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    
    if type(category_id) == int:
        sql_code = '''SELECT film.title title, 
    language.name languge, 
    category.name category
    FROM film
    INNER JOIN language USING(language_id)
    INNER JOIN film_category USING(film_id)
    INNER JOIN category USING(category_id)
    WHERE category.category_id IN({id})
    ORDER BY film.title, language.name ASC;
    '''.format(id = category_id)
        df = pd.read_sql(sql_code, con=connection)
        return df
    else: 
        return None
    
def number_films_in_category(category_id:int)->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o ilość filmów w zadanej kategori przez id kategorii.
    Przykład wynikowej tabeli:
    |   |category   |count|
    |0	|Action 	|64	  | 
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    category_id (int): wartość id kategorii dla którego wykonujemy zapytanie
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    
    if type(category_id) == int:
        sql_code = '''SELECT category.name category,
                COUNT(film_category.film_id) count
              FROM category
              INNER JOIN film_category USING(category_id)
              GROUP BY category.category_id 
              HAVING category.category_id IN({category_id});
              '''.format(category_id = category_id)
        df = pd.read_sql(sql_code, con=connection)
        return df
    else: 
        return None

def number_film_by_length(min_length: Union[int,float] = 0, max_length: Union[int,float] = 1e6 ) :
    ''' Funkcja zwracająca wynik zapytania do bazy o ilość filmów o dla poszczegulnych długości pomiędzy wartościami min_length a max_length.
    Przykład wynikowej tabeli:
    |   |length     |count|
    |0	|46 	    |64	  | 
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    min_length (int,float): wartość minimalnej długości filmu
    max_length (int,float): wartość maksymalnej długości filmu
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    
    if (isinstance(min_length, int) or isinstance(min_length, float)) and (isinstance(max_length, int) or isinstance(max_length, float)) and min_length < max_length:
        sql_code = '''SELECT film.length length,
                    COUNT(film.film_id) count
                  FROM film
                  GROUP BY film.length
                  HAVING film.length >= {min_length} AND film.length <= {max_length};
                  '''.format(min_length = min_length, max_length = max_length)
        df = pd.read_sql(sql_code, con=connection)
        return df
    else: 
        return None

def client_from_city(city:str)->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o listę klientów z zadanego miasta przez wartość city.
    Przykład wynikowej tabeli:
    |   |city	    |first_name	|last_name
    |0	|Athenai	|Linda	    |Williams
    
    Tabela wynikowa ma być posortowana po nazwisku i imieniu klienta.
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    city (str): nazwa miaste dla którego mamy sporządzić listę klientów
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    
    if type(city) == str:
        sql_code = '''SELECT ct.city city,
                    cust.first_name first_name,
                    cust.last_name last_name
                  FROM city ct
                  INNER JOIN address addr USING (city_id)
                  INNER JOIN customer cust USING (address_id)
                  WHERE ct.city = '{city}'
                  ORDER BY cust.last_name, cust.first_name
                  '''.format(city = city)
        df = pd.read_sql(sql_code, con=connection)
        return df
    else: 
        return None

def avg_amount_by_length(length:Union[int,float])->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o średnią wartość wypożyczenia filmów dla zadanej długości length.
    Przykład wynikowej tabeli:
    |   |length |avg
    |0	|48	    |4.295389
    
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    length (int,float): długość filmu dla którego mamy policzyć średnią wartość wypożyczonych filmów
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    
    if type(length) == int or type(length) == float:
        sql_code = '''SELECT film.length length,
                    AVG(pay.amount) avg
                  FROM film
                  INNER JOIN inventory USING (film_id)
                  INNER JOIN rental USING (inventory_id)
                  INNER JOIN payment pay USING (rental_id)
                  GROUP BY film.length
                  HAVING film.length = {length};
                  '''.format(length = length)
        df = pd.read_sql(sql_code, con=connection)
        return df
    else: 
        return None

def client_by_sum_length(sum_min:Union[int,float])->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o sumaryczny czas wypożyczonych filmów przez klientów powyżej zadanej wartości .
    Przykład wynikowej tabeli:
    |   |first_name |last_name  |sum
    |0  |Brian	    |Wyman  	|1265
    
    Tabela wynikowa powinna być posortowane według sumy, imienia i nazwiska klienta.
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    sum_min (int,float): minimalna wartość sumy długości wypożyczonych filmów którą musi spełniać klient
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    
    if type(sum_min) == int or type(sum_min) == float:
        sql_code = '''SELECT cust.first_name first_name,
                    cust.last_name last_name,
                    SUM(film.length) sum
                  FROM customer cust
                  INNER JOIN rental USING (customer_id)
                  INNER JOIN inventory USING (inventory_id)
                  INNER JOIN film USING (film_id)
                  GROUP BY cust.first_name, cust.last_name
                  HAVING SUM(film.length) >= {sum_min}
                  ORDER BY SUM(film.length), cust.last_name, cust.first_name;
                  '''.format(sum_min = sum_min)
        df = pd.read_sql(sql_code, con=connection)
        return df
    else:
        return None 

def category_statistic_length(name:str)->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o statystykę długości filmów w kategorii o zadanej nazwie.
    Przykład wynikowej tabeli:
    |   |category   |avg    |sum    |min    |max
    |0	|Action 	|111.60 |7143   |47 	|185
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    name (str): Nazwa kategorii dla której ma zostać wypisana statystyka
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    
    if type(name) == str:
        sql_code = '''SELECT category.name category,
                    AVG(film.length) avg,
                    SUM(film.length) sum,
                    MIN(film.length) min,
                    MAX(film.length) max
                FROM category
                INNER JOIN film_category USING(category_id)
                INNER JOIN film USING(film_id)
                GROUP BY category.name
                HAVING category.name = '{name}';
                '''.format(name = name)
        df = pd.read_sql(sql_code, con=connection)
        return df
    else: 
        return None





