#################################### IMPORTING LIBRARIES #####################################
import pandas as pd
import numpy as np
import os
import requests
from S3_Uploader import Trigger_Uploader
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

#################################### CONFIGURATIONS ##########################################

TMBD_API_KEY = os.environ.get('TMBD_API_KEY')
file_path = 'PreparedData/'
file_name = "UpdatedDataSet.csv"
list_of_months = {'1': 'January', '2': 'February', '3': 'March',
                  '4': 'April', '5': 'May', '6': 'June', '7': 'July',
                  '8': 'August', '9': 'September', '10': 'October',
                  '11': 'November', '12': 'December'}

#################################### Generating Dataset ######################################


def lambda_handler(event, context):
    print("AWS! base function to trigger checking")
    print("event = {}".format(event))
    return {
        'statusCode': 200,
    }


def get_genre(x):
    try:
        """
            1. Here Searching the Movie Titles what we have got from Wikipedia
            2. Collecting Genres from TMDB 
        """
        # print(x)
        genres = []
        result = requests.get(
            f"https://api.themoviedb.org/3/search/movie?api_key={TMBD_API_KEY}&query={x}")
        movie_json = result.json()
        if movie_json['results'][0]['id']:
            movie_id = movie_json['results'][0]['id']
            response = requests.get(
                'https://api.themoviedb.org/3/movie/{}?api_key={}'.format(movie_id, TMBD_API_KEY))
            data_json = response.json()
            # print(movie_id)
            if data_json['genres']:
                genre_str = " "
                for i in range(0, len(data_json['genres'])):
                    genres.append(data_json['genres'][i]['name'])
                return genre_str.join(genres)
        else:
            np.NaN
    except Exception as e:
        return str(e)


def get_director(x):
    if " (director)" in x:
        return x.split(" (director)")[0]
    elif " (directors)" in x:
        return x.split(" (directors)")[0]
    else:
        return x.split(" (director/screenplay)")[0]


def get_actor_one(x):
    return ((x.split("screenplay); ")[-1]).split(", ")[0])


def get_actor_two(x):
    if len((x.split("screenplay); ")[-1]).split(", ")) < 2:
        return np.NaN
    else:
        return ((x.split("screenplay); ")[-1]).split(", ")[1])


def get_actor_three(x):
    if len((x.split("screenplay); ")[-1]).split(", ")) < 3:
        return np.NaN
    else:
        return ((x.split("screenplay); ")[-1]).split(", ")[2])


def s3_updated_dataset():
    data = pd.read_csv(
        'https://movie-dataset-live.s3.ap-south-1.amazonaws.com/PreparedData/UpdatedDataSet.csv')
    return data


def preprocess_data(df):
    """
        1. Here Creating Feature List that we have used to make our old Dataset.
        2. Renaming the Dataframe Fields
        3. Replacing Nan Values with Unknown to avoid Conflict
        4. Lower casing the Title of the movies
        5. Combining all the data's
        6. Dropping values contain Nan
        7. Taking Previously updated Dataset From S3 bucket
        8. Merging Old and New Dataset
        9. Dropping Duplicate Movie Details
        10. Saving Dataset
        11. Uploading to S3 bucket (which will replace the old one with new one)
    """
    try:
        df_new = df[['Title', 'Cast and crew', 'genres']]
        df_new['director_name'] = df_new['Cast and crew'].map(
            lambda x: get_director(str(x)))
        df_new['actor_1_name'] = df_new['Cast and crew'].map(
            lambda x: get_actor_one(str(x)))
        df_new['actor_2_name'] = df_new['Cast and crew'].map(
            lambda x: get_actor_two(str(x)))
        df_new['actor_3_name'] = df_new['Cast and crew'].map(
            lambda x: get_actor_three(str(x)))
        df_new = df_new.rename(columns={'Title': 'movie_title'})
        new_df = df_new.loc[:, ['director_name', 'actor_1_name',
                                'actor_2_name', 'actor_3_name', 'genres', 'movie_title']]
        new_df['actor_2_name'] = new_df['actor_2_name'].replace(
            np.nan, 'unknown')
        new_df['actor_3_name'] = new_df['actor_3_name'].replace(
            np.nan, 'unknown')
        new_df['movie_title'] = new_df['movie_title'].str.lower()
        new_df['comb'] = new_df['actor_1_name'] + ' ' + new_df['actor_2_name'] + ' ' + \
            new_df['actor_3_name'] + ' ' + \
            new_df['director_name'] + ' ' + new_df['genres']
        new_df = new_df.dropna(how='any')
        updated_data = s3_updated_dataset()
        if updated_data is not None:
            new_data = pd.concat([updated_data, new_df])
            new_data = new_data.dropna(how='any')
            new_data.drop_duplicates(
                subset="movie_title", keep='last', inplace=True)
            new_data.to_csv(file_path+file_name, index=False)
            uploaded = Trigger_Uploader(
                file_path=file_path, file_name=file_name)
            if uploaded == "Uploaded to S3 bucket":
                s3_updated_dataset()
        else:
            raise Exception("UpdateData not getting from S3")
    except Exception as e:
        print(e)


def wikipedia_data_scrapper(country, year):
    """
        1. Here Collection Movie Details From Wikipedia based on the year and country that provided by user.
        2. Merging all the tables that we have extracted from wikipedia
    """
    link = f"https://en.wikipedia.org/wiki/List_of_{country.capitalize()}_films_of_{year}"
    df1 = pd.read_html(link, header=0)[2]
    df2 = pd.read_html(link, header=0)[3]
    df3 = pd.read_html(link, header=0)[4]
    df4 = pd.read_html(link, header=0)[5]
    df4 = pd.read_html(link, header=0)[6]
    df = df1.append(df2.append(df3.append(df4, ignore_index=True),
                    ignore_index=True), ignore_index=True)
    return df


try:
    if TMBD_API_KEY is None:
        raise Exception("TMBD_API_KEY is not Getting")

    def make_new_dataset(country, year):
        """
            1. Scrapping Movies Title from Wikipedia
            2. Collecting Genres from TMDB Using API
            3. Calling Preprocess Function to Make a new Dataset What we have created before and update to it.
            4. Same process happening on else part too but putting some condition to stop taking movies title after the current month
        """
        current_year = datetime.today().year
        if current_year != year:
            df = wikipedia_data_scrapper(country, year)
            df['genres'] = df['Title'].map(lambda x: get_genre(str(x)))
            preprocess_data(df)
        else:
            current_month = " ".join(
                list_of_months[str(datetime.today().month)].upper())
            df = wikipedia_data_scrapper(country, year)
            df['new_data'] = df['Opening'].map(lambda x: str(x))
            # print(f"df['new_data'] : {df['new_data']} length : {len(df['new_data'])}")
            for i in range(len(df['new_data'])):
                month = df['new_data'][i]
                print(month)
                if month == current_month:
                    if current_month != df['new_data'][i + 1]:
                        print(f"testing break{df['new_data'][i + 1]}")
                        break
                else:
                    pass
                    # df['genres'] = df['Title'].map(lambda x: get_genre(str(x)))
            # preprocess_data(df)
except Exception as e:
    print(e)


country = "American"
print("file Executing..!")
year = datetime.today().year  # to do cronjob just changed the year to current_year
make_new_dataset(country=country, year=year)
print(f"done :{year} dataset updated")
