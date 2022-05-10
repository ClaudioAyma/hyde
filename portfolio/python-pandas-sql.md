---
layout: portfolio
title: SQL Analysis
permalink: /python-pandas-sql-analysis/
---
### Import Libraries


```python
import os
import psycopg2 as ps
import pandas as pd
from sqlalchemy import create_engine

```

### Connect to Data Base


```python
db = {
    'host': '127.0.0.1',
    'port': '5432',
    'dbname': 'CovidProject',
    'user': 'postgres',
    'password': 'admin'
}

```


```python
db_ = f"postgresql://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['dbname']}"
engine = create_engine(db_)
```

### Get data from covid_deaths Table

```sql
SELECT *
FROM covid_deaths
WHERE continent is not null 
ORDER BY 3,4
```


```python
query = """
SELECT *
FROM covid_deaths
WHERE continent is not null 
ORDER BY 3,4
"""
df = pd.read_sql_query(query, con=engine)
df.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>iso_code</th>
      <th>continent</th>
      <th>location</th>
      <th>date</th>
      <th>population</th>
      <th>total_cases</th>
      <th>new_cases</th>
      <th>new_cases_smoothed</th>
      <th>total_deaths</th>
      <th>...</th>
      <th>new_deaths_smoothed_per_million</th>
      <th>reproduction_rate</th>
      <th>icu_patients</th>
      <th>icu_patients_per_million</th>
      <th>hosp_patients</th>
      <th>hosp_patients_per_million</th>
      <th>weekly_icu_admissions</th>
      <th>weekly_icu_admissions_per_million</th>
      <th>weekly_hosp_admissions</th>
      <th>weekly_hosp_admissions_per_million</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>3159</td>
      <td>DZA</td>
      <td>Africa</td>
      <td>Algeria</td>
      <td>2022-03-09</td>
      <td>44616626.0</td>
      <td>265346.0</td>
      <td>23.0</td>
      <td>38.143</td>
      <td>6860.0</td>
      <td>...</td>
      <td>0.054</td>
      <td>0.28</td>
      <td>13.0</td>
      <td>0.291</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1</th>
      <td>3158</td>
      <td>DZA</td>
      <td>Africa</td>
      <td>Algeria</td>
      <td>2022-03-08</td>
      <td>44616626.0</td>
      <td>265323.0</td>
      <td>26.0</td>
      <td>44.714</td>
      <td>6858.0</td>
      <td>...</td>
      <td>0.058</td>
      <td>0.29</td>
      <td>14.0</td>
      <td>0.314</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3157</td>
      <td>DZA</td>
      <td>Africa</td>
      <td>Algeria</td>
      <td>2022-03-07</td>
      <td>44616626.0</td>
      <td>265297.0</td>
      <td>32.0</td>
      <td>51.571</td>
      <td>6857.0</td>
      <td>...</td>
      <td>0.070</td>
      <td>0.31</td>
      <td>11.0</td>
      <td>0.247</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>3</th>
      <td>3156</td>
      <td>DZA</td>
      <td>Africa</td>
      <td>Algeria</td>
      <td>2022-03-06</td>
      <td>44616626.0</td>
      <td>265265.0</td>
      <td>38.0</td>
      <td>58.571</td>
      <td>6855.0</td>
      <td>...</td>
      <td>0.077</td>
      <td>0.32</td>
      <td>13.0</td>
      <td>0.291</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>4</th>
      <td>3155</td>
      <td>DZA</td>
      <td>Africa</td>
      <td>Algeria</td>
      <td>2022-03-05</td>
      <td>44616626.0</td>
      <td>265227.0</td>
      <td>41.0</td>
      <td>64.143</td>
      <td>6853.0</td>
      <td>...</td>
      <td>0.080</td>
      <td>0.32</td>
      <td>14.0</td>
      <td>0.314</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
  </tbody>
</table>
<p>5 rows × 27 columns</p>
</div>



### Get data from covid_deaths Table

```sql
SELECT *
FROM covid_vaccinations
WHERE continent is not null 
ORDER BY 3,4
```


```python
query = """
SELECT *
FROM covid_vaccinations
WHERE continent is not null 
ORDER BY 3,4
"""
df = pd.read_sql_query(query, con=engine)
df.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>iso_code</th>
      <th>continent</th>
      <th>location</th>
      <th>date</th>
      <th>total_tests</th>
      <th>new_tests</th>
      <th>total_tests_per_thousand</th>
      <th>new_tests_per_thousand</th>
      <th>new_tests_smoothed</th>
      <th>...</th>
      <th>female_smokers</th>
      <th>male_smokers</th>
      <th>handwashing_facilities</th>
      <th>hospital_beds_per_thousand</th>
      <th>life_expectancy</th>
      <th>human_development_index</th>
      <th>excess_mortality_cumulative_absolute</th>
      <th>excess_mortality_cumulative</th>
      <th>excess_mortality</th>
      <th>excess_mortality_cumulative_per_million</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2441</td>
      <td>DZA</td>
      <td>Africa</td>
      <td>Algeria</td>
      <td>2020-03-21</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>...</td>
      <td>0.7</td>
      <td>30.4</td>
      <td>83.741</td>
      <td>1.9</td>
      <td>76.88</td>
      <td>0.748</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1</th>
      <td>3136</td>
      <td>DZA</td>
      <td>Africa</td>
      <td>Algeria</td>
      <td>2022-02-14</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>...</td>
      <td>0.7</td>
      <td>30.4</td>
      <td>83.741</td>
      <td>1.9</td>
      <td>76.88</td>
      <td>0.748</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3137</td>
      <td>DZA</td>
      <td>Africa</td>
      <td>Algeria</td>
      <td>2022-02-15</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>...</td>
      <td>0.7</td>
      <td>30.4</td>
      <td>83.741</td>
      <td>1.9</td>
      <td>76.88</td>
      <td>0.748</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>3</th>
      <td>3138</td>
      <td>DZA</td>
      <td>Africa</td>
      <td>Algeria</td>
      <td>2022-02-16</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>...</td>
      <td>0.7</td>
      <td>30.4</td>
      <td>83.741</td>
      <td>1.9</td>
      <td>76.88</td>
      <td>0.748</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>4</th>
      <td>3134</td>
      <td>DZA</td>
      <td>Africa</td>
      <td>Algeria</td>
      <td>2022-02-12</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>...</td>
      <td>0.7</td>
      <td>30.4</td>
      <td>83.741</td>
      <td>1.9</td>
      <td>76.88</td>
      <td>0.748</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
  </tbody>
</table>
<p>5 rows × 46 columns</p>
</div>



#### Select Data that we are going to be starting with


```python
query = """
SELECT location, date, total_cases, new_cases, total_deaths, population
FROM covid_deaths
WHERE continent IS NOT NULL 
ORDER BY 1,2
"""
df = pd.read_sql_query(query, con=engine)
df
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>location</th>
      <th>date</th>
      <th>total_cases</th>
      <th>new_cases</th>
      <th>total_deaths</th>
      <th>population</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Afghanistan</td>
      <td>2020-02-24</td>
      <td>5.0</td>
      <td>5.0</td>
      <td>NaN</td>
      <td>39835428.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Afghanistan</td>
      <td>2020-02-25</td>
      <td>5.0</td>
      <td>0.0</td>
      <td>NaN</td>
      <td>39835428.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Afghanistan</td>
      <td>2020-02-26</td>
      <td>5.0</td>
      <td>0.0</td>
      <td>NaN</td>
      <td>39835428.0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Afghanistan</td>
      <td>2020-02-27</td>
      <td>5.0</td>
      <td>0.0</td>
      <td>NaN</td>
      <td>39835428.0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Afghanistan</td>
      <td>2020-02-28</td>
      <td>5.0</td>
      <td>0.0</td>
      <td>NaN</td>
      <td>39835428.0</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>173300</th>
      <td>Zimbabwe</td>
      <td>2022-05-01</td>
      <td>247911.0</td>
      <td>36.0</td>
      <td>5469.0</td>
      <td>15092171.0</td>
    </tr>
    <tr>
      <th>173301</th>
      <td>Zimbabwe</td>
      <td>2022-05-02</td>
      <td>247935.0</td>
      <td>24.0</td>
      <td>5470.0</td>
      <td>15092171.0</td>
    </tr>
    <tr>
      <th>173302</th>
      <td>Zimbabwe</td>
      <td>2022-05-03</td>
      <td>247990.0</td>
      <td>55.0</td>
      <td>5470.0</td>
      <td>15092171.0</td>
    </tr>
    <tr>
      <th>173303</th>
      <td>Zimbabwe</td>
      <td>2022-05-04</td>
      <td>248050.0</td>
      <td>60.0</td>
      <td>5471.0</td>
      <td>15092171.0</td>
    </tr>
    <tr>
      <th>173304</th>
      <td>Zimbabwe</td>
      <td>2022-05-05</td>
      <td>248050.0</td>
      <td>0.0</td>
      <td>5471.0</td>
      <td>15092171.0</td>
    </tr>
  </tbody>
</table>
<p>173305 rows × 6 columns</p>
</div>



### Total cases vs Total population
#### Shows the probability of contracting covid in all continents

```sql
-- ODERED BY PERCENTAGE OF INFECTED CONTINENT 
WITH CONTINENT_CASES ("Continent", "Total Cases", "Population")
AS (
	SELECT
		continent AS "Continent",
		SUM(total_cases) AS "Total Cases",
		SUM(population) AS "Population"	
	FROM covid_deaths
	WHERE
        date = (SELECT MAX(DATE) FROM COVID_DEATHS) AND
        population IS NOT NULL AND
        total_cases IS NOT NULL AND
        continent IS NOT NULL
	GROUP BY continent
	ORDER BY continent DESC
)
SELECT *, ROUND(("Total Cases"/"Population") * 100, 2) AS "Infected Population"
FROM CONTINENT_CASES
ORDER BY "Infected Population" DESC
```


```python
query = """
WITH CONTINENT_CASES ("Continent", "Total Cases", "Population")
AS (
	SELECT
		continent AS "Continent",
		SUM(total_cases) AS "Total Cases",
		SUM(population) AS "Population"	
	FROM covid_deaths
	WHERE
        date = (SELECT MAX(DATE) FROM COVID_DEATHS) AND
        population IS NOT NULL AND
        total_cases IS NOT NULL AND
        continent IS NOT NULL
	GROUP BY continent
	ORDER BY continent DESC
)
SELECT *, ROUND(("Total Cases"/"Population") * 100, 2) AS "Infected Population"
FROM CONTINENT_CASES
ORDER BY "Infected Population" DESC
"""
df = pd.read_sql_query(query, con=engine)
df
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Continent</th>
      <th>Total Cases</th>
      <th>Population</th>
      <th>Infected Population</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Europe</td>
      <td>193944851.0</td>
      <td>7.505074e+08</td>
      <td>25.84</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Oceania</td>
      <td>7449535.0</td>
      <td>4.317664e+07</td>
      <td>17.25</td>
    </tr>
    <tr>
      <th>2</th>
      <td>North America</td>
      <td>96705347.0</td>
      <td>5.927914e+08</td>
      <td>16.31</td>
    </tr>
    <tr>
      <th>3</th>
      <td>South America</td>
      <td>56830311.0</td>
      <td>4.339537e+08</td>
      <td>13.10</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Asia</td>
      <td>149534730.0</td>
      <td>4.646498e+09</td>
      <td>3.22</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Africa</td>
      <td>11706055.0</td>
      <td>1.371693e+09</td>
      <td>0.85</td>
    </tr>
  </tbody>
</table>
</div>



#### Shows the probability of contracting covid in all countries

```sql
-- ODERTED BY PERCENTAGE OF INFECTED POPULATION 
SELECT
	DISTINCT location AS "Country",
	total_cases AS "Total Cases",
	population AS "Population",
	ROUND((total_cases/ population) * 100, 2) AS "Infected Population"
FROM covid_deaths
WHERE date = '2022-05-05' AND population IS NOT NULL AND total_cases IS NOT NULL 
ORDER BY "Infected Population" DESC
```


```python
query = """
SELECT
    DISTINCT location AS "Country",
    total_cases AS "Total Cases",
    population AS "Population",
    ROUND((total_cases/ population) * 100, 2) AS "Infected Population"
FROM covid_deaths
WHERE date = '2022-05-05' AND population IS NOT NULL AND total_cases IS NOT NULL 
ORDER BY "Infected Population" DESC
"""
df = pd.read_sql_query(query, con=engine)
df
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Country</th>
      <th>Total Cases</th>
      <th>Population</th>
      <th>Infected Population</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Faeroe Islands</td>
      <td>34658.0</td>
      <td>49053.0</td>
      <td>70.65</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Andorra</td>
      <td>41717.0</td>
      <td>77354.0</td>
      <td>53.93</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Denmark</td>
      <td>3120933.0</td>
      <td>5813302.0</td>
      <td>53.69</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Cyprus</td>
      <td>480220.0</td>
      <td>896005.0</td>
      <td>53.60</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Gibraltar</td>
      <td>17996.0</td>
      <td>33691.0</td>
      <td>53.41</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>222</th>
      <td>Niger</td>
      <td>8957.0</td>
      <td>25130810.0</td>
      <td>0.04</td>
    </tr>
    <tr>
      <th>223</th>
      <td>Yemen</td>
      <td>11819.0</td>
      <td>30490639.0</td>
      <td>0.04</td>
    </tr>
    <tr>
      <th>224</th>
      <td>Marshall Islands</td>
      <td>17.0</td>
      <td>59618.0</td>
      <td>0.03</td>
    </tr>
    <tr>
      <th>225</th>
      <td>Macao</td>
      <td>82.0</td>
      <td>658391.0</td>
      <td>0.01</td>
    </tr>
    <tr>
      <th>226</th>
      <td>Micronesia (country)</td>
      <td>7.0</td>
      <td>116255.0</td>
      <td>0.01</td>
    </tr>
  </tbody>
</table>
<p>227 rows × 4 columns</p>
</div>



### Total cases vs Total deaths
#### Shows the probability of dying if you have covid in all continents

```sql
WITH CONTINENT_CASES ("Continent", "Total Cases", "Total Deaths")
AS (
	SELECT
		continent AS "Continent",
		SUM(total_cases) AS "Total Cases",
		SUM(total_deaths) AS "Total Deaths"	
	FROM covid_deaths
	WHERE
        date = (SELECT MAX(DATE) FROM COVID_DEATHS) AND
        total_deaths IS NOT NULL AND
        total_cases IS NOT NULL AND
        continent IS NOT NULL
	GROUP BY continent
	ORDER BY continent DESC
)
SELECT
    *,
    ROUND(("Total Deaths"/"Total Cases") * 100, 2) AS "Death Percentage"
FROM CONTINENT_CASES
ORDER BY "Death Percentage" DESC
```


```python
query = """
WITH CONTINENT_CASES ("Continent", "Total Cases", "Total Deaths")
AS (
	SELECT
		continent AS "Continent",
		SUM(total_cases) AS "Total Cases",
		SUM(total_deaths) AS "Total Deaths"	
	FROM covid_deaths
	WHERE date = '2022-05-05' AND total_deaths IS NOT NULL AND total_cases IS NOT NULL AND continent IS NOT NULL
	GROUP BY continent
	ORDER BY continent DESC
)
SELECT *, ROUND(("Total Deaths"/"Total Cases") * 100, 2) AS "Death Percentage"
FROM CONTINENT_CASES
ORDER BY "Death Percentage" DESC
"""
df = pd.read_sql_query(query, con=engine)
df
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Continent</th>
      <th>Total Cases</th>
      <th>Total Deaths</th>
      <th>Death Percentage</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>South America</td>
      <td>56830114.0</td>
      <td>1294963.0</td>
      <td>2.28</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Africa</td>
      <td>11706051.0</td>
      <td>253047.0</td>
      <td>2.16</td>
    </tr>
    <tr>
      <th>2</th>
      <td>North America</td>
      <td>96705347.0</td>
      <td>1434678.0</td>
      <td>1.48</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Asia</td>
      <td>149534648.0</td>
      <td>1430289.0</td>
      <td>0.96</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Europe</td>
      <td>193944822.0</td>
      <td>1823589.0</td>
      <td>0.94</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Oceania</td>
      <td>7449511.0</td>
      <td>10939.0</td>
      <td>0.15</td>
    </tr>
  </tbody>
</table>
</div>



#### Shows the probability of dying if you have covid in all countries

```sql
WITH DEATH_VS_CASES ("Country", "Total Cases", "Total Deaths") 
AS (
    SELECT
        location as "Country",
        MAX(total_cases) AS "Total Cases",
        MAX(total_deaths) AS "Total Deaths"
    FROM covid_deaths
    GROUP BY location
    ORDER BY "Total Cases" DESC
)
SELECT
    *,
    ROUND(("Total Deaths"/"Total Cases")*100, 2) AS "Death Percentage"
FROM DEATH_VS_CASES
WHERE
    "Total Cases" IS NOT NULL AND
    "Total Deaths" IS NOT NULL
ORDER BY "Death Percentage" DESC
```


```python
query = """
WITH DEATH_VS_CASES ("Country", "Total Cases", "Total Deaths") 
AS (
    SELECT location as "Country", MAX(total_cases) AS "Total Cases", MAX(total_deaths) AS "Total Deaths"
    FROM covid_deaths
    GROUP BY location
    ORDER BY "Total Cases" DESC
)
SELECT *,ROUND(("Total Deaths"/"Total Cases")*100, 2) AS "Death Percentage" FROM DEATH_VS_CASES
WHERE "Total Cases" IS NOT NULL AND "Total Deaths" IS NOT NULL
ORDER BY "Death Percentage" DESC
"""
df = pd.read_sql_query(query, con=engine)
df
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Country</th>
      <th>Total Cases</th>
      <th>Total Deaths</th>
      <th>Death Percentage</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Yemen</td>
      <td>11819.0</td>
      <td>2149.0</td>
      <td>18.18</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Sudan</td>
      <td>62135.0</td>
      <td>4933.0</td>
      <td>7.94</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Peru</td>
      <td>3568692.0</td>
      <td>212906.0</td>
      <td>5.97</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Mexico</td>
      <td>5740080.0</td>
      <td>324350.0</td>
      <td>5.65</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Syria</td>
      <td>55837.0</td>
      <td>3150.0</td>
      <td>5.64</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>217</th>
      <td>New Zealand</td>
      <td>977380.0</td>
      <td>797.0</td>
      <td>0.08</td>
    </tr>
    <tr>
      <th>218</th>
      <td>Iceland</td>
      <td>185795.0</td>
      <td>119.0</td>
      <td>0.06</td>
    </tr>
    <tr>
      <th>219</th>
      <td>Saint Pierre and Miquelon</td>
      <td>2727.0</td>
      <td>1.0</td>
      <td>0.04</td>
    </tr>
    <tr>
      <th>220</th>
      <td>Bhutan</td>
      <td>59422.0</td>
      <td>21.0</td>
      <td>0.04</td>
    </tr>
    <tr>
      <th>221</th>
      <td>Cook Islands</td>
      <td>5246.0</td>
      <td>1.0</td>
      <td>0.02</td>
    </tr>
  </tbody>
</table>
<p>222 rows × 4 columns</p>
</div>



### Countries with Highest Death Count per country

```sql
SELECT
	DISTINCT location AS "Country",
	total_deaths AS "Total Deaths"
FROM covid_deaths
WHERE
    date = (SELECT MAX(DATE) FROM COVID_DEATHS) AND
    continent IS NOT NULL AND
    location IS NOT NULL AND
    total_deaths IS NOT NULL
ORDER BY "Total Deaths" DESC
```


```python
query = """
SELECT
	DISTINCT location AS "Country",
	total_deaths AS "Total Deaths"
FROM covid_deaths
WHERE
    date = (SELECT MAX(DATE) FROM COVID_DEATHS) AND
    continent IS NOT NULL AND
    location IS NOT NULL AND
    total_deaths IS NOT NULL
ORDER BY "Total Deaths" DESC
"""
df = pd.read_sql_query(query, con=engine)
df
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Country</th>
      <th>Total Deaths</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>United States</td>
      <td>996964.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Brazil</td>
      <td>664131.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>India</td>
      <td>524002.0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Russia</td>
      <td>368974.0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Mexico</td>
      <td>324334.0</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>204</th>
      <td>Wallis and Futuna</td>
      <td>7.0</td>
    </tr>
    <tr>
      <th>205</th>
      <td>Palau</td>
      <td>6.0</td>
    </tr>
    <tr>
      <th>206</th>
      <td>Montserrat</td>
      <td>2.0</td>
    </tr>
    <tr>
      <th>207</th>
      <td>Cook Islands</td>
      <td>1.0</td>
    </tr>
    <tr>
      <th>208</th>
      <td>Saint Pierre and Miquelon</td>
      <td>1.0</td>
    </tr>
  </tbody>
</table>
<p>209 rows × 2 columns</p>
</div>



### GLOBAL NUMBERS 

```sql
SELECT SUM(new_cases) AS "Total Cases", SUM(CAST(new_deaths AS int)) AS "Total Deaths", SUM(CAST(new_deaths AS int))/SUM(New_Cases)*100 as "Death Percentage"
FROM covid_deaths
WHERE continent IS NOT NULL
ORDER BY 1,2
```


```python
query = """
SELECT SUM(new_cases) AS "Total Cases", SUM(CAST(new_deaths AS int)) AS "Total Deaths", SUM(CAST(new_deaths AS int))/SUM(New_Cases)*100 as "Death Percentage"
FROM covid_deaths
WHERE continent IS NOT NULL
ORDER BY 1,2
"""
df = pd.read_sql_query(query, con=engine)
df
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Total Cases</th>
      <th>Total Deaths</th>
      <th>Death Percentage</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>515027097.0</td>
      <td>6204121</td>
      <td>1.20462</td>
    </tr>
  </tbody>
</table>
</div>



### Total Population vs Vaccinations
#### Shows Percentage of Population that has recieved Covid Vaccine


```sql
SELECT
	TOTAL_VAC."Country",
	TOTAL_VAC."Vaccinated People",
	POP_COUNTRY."Population",
	ROUND((TOTAL_VAC."Vaccinated People" / POP_COUNTRY."Population") * 100, 2) AS "Percentage Vaccinated People"
	
FROM
(
	SELECT
		location AS "Country",
		MAX(people_vaccinated) AS "Vaccinated People"
	FROM COVID_VACCINATIONS
	WHERE location IS NOT NULL AND continent IS NOT NULL AND new_vaccinations IS NOT NULL
	GROUP BY location
) AS TOTAL_VAC
INNER JOIN
(
	SELECT
		DISTINCT location AS "Country",
		iso_code,
		population AS "Population"
	FROM covid_deaths
	WHERE date = '2022-05-05'

) AS POP_COUNTRY
ON TOTAL_VAC."Country" = POP_COUNTRY."Country"
ORDER BY "Percentage Vaccinated People" DESC
```


```python
query = """
SELECT
	TOTAL_VAC."Country",
	TOTAL_VAC."Vaccinated People",
	POP_COUNTRY."Population",
	ROUND((TOTAL_VAC."Vaccinated People" / POP_COUNTRY."Population") * 100, 2) AS "Percentage Vaccinated People"
	
FROM
(
	SELECT
		location AS "Country",
		MAX(people_vaccinated) AS "Vaccinated People"
	FROM COVID_VACCINATIONS
	WHERE location IS NOT NULL AND continent IS NOT NULL AND new_vaccinations IS NOT NULL
	GROUP BY location
) AS TOTAL_VAC
INNER JOIN
(
	SELECT
		DISTINCT location AS "Country",
		iso_code,
		population AS "Population"
	FROM covid_deaths
	WHERE date = '2022-05-05'

) AS POP_COUNTRY
ON TOTAL_VAC."Country" = POP_COUNTRY."Country"
ORDER BY "Percentage Vaccinated People" DESC
"""
df = pd.read_sql_query(query, con=engine)
df
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Country</th>
      <th>Vaccinated People</th>
      <th>Population</th>
      <th>Percentage Vaccinated People</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Gibraltar</td>
      <td>42046.0</td>
      <td>33691.0</td>
      <td>124.80</td>
    </tr>
    <tr>
      <th>1</th>
      <td>United Arab Emirates</td>
      <td>9881456.0</td>
      <td>9991083.0</td>
      <td>98.90</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Portugal</td>
      <td>9663542.0</td>
      <td>10167923.0</td>
      <td>95.04</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Cuba</td>
      <td>10658408.0</td>
      <td>11317498.0</td>
      <td>94.18</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Chile</td>
      <td>17921746.0</td>
      <td>19212362.0</td>
      <td>93.28</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>164</th>
      <td>Madagascar</td>
      <td>150329.0</td>
      <td>28427333.0</td>
      <td>0.53</td>
    </tr>
    <tr>
      <th>165</th>
      <td>Sierra Leone</td>
      <td>42566.0</td>
      <td>8141343.0</td>
      <td>0.52</td>
    </tr>
    <tr>
      <th>166</th>
      <td>Democratic Republic of Congo</td>
      <td>257013.0</td>
      <td>92377986.0</td>
      <td>0.28</td>
    </tr>
    <tr>
      <th>167</th>
      <td>Burundi</td>
      <td>1592.0</td>
      <td>12255429.0</td>
      <td>0.01</td>
    </tr>
    <tr>
      <th>168</th>
      <td>Myanmar</td>
      <td>3800.0</td>
      <td>54806014.0</td>
      <td>0.01</td>
    </tr>
  </tbody>
</table>
<p>169 rows × 4 columns</p>
</div>
