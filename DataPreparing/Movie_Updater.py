#################################### IMPORTING LIBRARIES #####################################
import pandas as pd
import numpy as np
import json
import os
import requests
import boto3
from botocore.exceptions import NoCredentialsError
from tmdbv3api import TMDb
from tmdbv3api import Movie
from S3_Uploader import Trigger_Uploader #S3 Trigger function calling.
from dotenv import load_dotenv
load_dotenv()

#################################### CONFIGURATIONS ##########################################

API_KEY = os.environ.get('TMBD_API_KEY')
tmdb = TMDb()
tmdb_movie = Movie()
tmdb.api_key = API_KEY
file_path = 'PreparedData/'
file_name = "UpdatedDataSet.csv"

if API_KEY is None:
    print("API_KEY is not Getting")
    


#################################### Generating Dataset ######################################

def tmdb_hit_issue():
    try:
        movie_id = 785084
        response = requests.get('https://api.themoviedb.org/3/movie/{}?api_key={}'.format(movie_id,tmdb.api_key))
        response.raise_for_status()
        data_json = response.json()
        if data_json['genres']:
            print('just hitting to avoid tmdb issue')
    except Exception as e:
        print(e)

def get_genre(x):
    try:
        genres = []
        result = tmdb_movie.search(x)
        movie_id = result[0].id
        response = requests.get('https://api.themoviedb.org/3/movie/{}?api_key={}'.format(movie_id,tmdb.api_key))
        response.raise_for_status()
        data_json = response.json()
        if data_json['genres']:
            genre_str = " " 
            for i in range(0,len(data_json['genres'])):
                genres.append(data_json['genres'][i]['name'])
            return genre_str.join(genres)
        else:
            np.NaN
    except:
        raise Exception("TMDB is having some issues!")
        
def get_director(x):
    if " (director)" in x:
        # this will split director
        return x.split(" (director)")[0]
    elif " (directors)" in x:
        # this will split directors
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
    data = pd.read_csv('https://movie-dataset-live.s3.ap-south-1.amazonaws.com/PreparedData/UpdatedDataSet.csv')
    return data

try:
    def make_new_dataset(country,year):
        link = f"https://en.wikipedia.org/wiki/List_of_{country.capitalize()}_films_of_{year}"
        # print(link)
        if link:
            df1 = pd.read_html(link, header=0)[2]
            df2 = pd.read_html(link, header=0)[3]
            df3 = pd.read_html(link, header=0)[4]
            df4 = pd.read_html(link, header=0)[5]
            # Merging all the data's
            df = df1.append(df2.append(df3.append(df4,ignore_index=True),ignore_index=True),ignore_index=True)
            # To avoid tmdb issue
            n = 0
            while n<= 5:
                 tmdb_hit_issue()
                 n += 1
            print("before Calling TMDB Function")
            # here collecting the Genres of the Movies using the Title that we have got from the Wikipedia
            df['genres'] = df['Title'].map(lambda x: get_genre(str(x)))
            print("After Done TMDB Function")
            # Extracting the Features 
            df_new = df[['Title','Cast and crew','genres']]
            # here creating new director name column to store the splitted data
            df_new['director_name'] = df_new['Cast and crew'].map(lambda x: get_director(x))
            # here taking the actors name [1st index values]
            df_new['actor_1_name'] = df_new['Cast and crew'].map(lambda x: get_actor_one(x))
            # here taking the actors name [2nd index values]
            df_new['actor_2_name'] = df_new['Cast and crew'].map(lambda x: get_actor_two(x))
            # here taking the actors name [2nd index values]
            df_new['actor_3_name'] = df_new['Cast and crew'].map(lambda x: get_actor_three(x))
            # here renaming the Title Field Column name to movie_title
            df_new = df_new.rename(columns={'Title':'movie_title'})
            # extract the feature that we need to train our model
            new_df = df_new.loc[:,['director_name','actor_1_name','actor_2_name','actor_3_name','genres','movie_title']]
            # Replacing the nan values to ('unknown')
            new_df['actor_2_name'] = new_df['actor_2_name'].replace(np.nan, 'unknown')
            new_df['actor_3_name'] = new_df['actor_3_name'].replace(np.nan, 'unknown')
            # lowercasing the movie_title
            new_df['movie_title'] = new_df['movie_title'].str.lower()
            # combining data
            new_df['comb'] = new_df['actor_1_name'] + ' ' + new_df['actor_2_name'] + ' '+ new_df['actor_3_name'] + ' '+ new_df['director_name'] +' ' + new_df['genres']
            # dropping the null data
            # If any NA values are present, drop that row or column
            new_df = new_df.dropna(how='any')
            # Our S3 Updated Data
            updated_data = s3_updated_dataset()
            print(f"Before Updating : {updated_data.shape}")
            # print('outside :')
            if updated_data is not None:
                print('inside : checking new updates working or not')
                # Appending old Update data with new Dataset Generated 
                new_data = pd.concat([updated_data,new_df])
                # If any NA values are present, drop that row or column
                new_data = new_data.dropna(how='any')
                # dropping the duplicate values
                new_data.drop_duplicates(subset ="movie_title", keep = 'last', inplace = True)
                print('after dropping items')
                # before update S3 dataset
                new_data.to_csv(file_path+file_name,index=False) # keep the data in local storage
                uploaded = Trigger_Uploader(file_path=file_path,file_name=file_name)
                print('after s3 triggered')
                if uploaded == "Uploaded to S3 bucket":
                    print(uploaded)
                    after_updated_data = s3_updated_dataset()
                    print(f"After Updating : {after_updated_data.shape}")
            else:
                raise Exception("UpdateData not getting from S3")
        else:
            raise Exception("Wikipedia Link is getting Error")
except Exception as e:
    print(e)
    

# if __name__ == "__main__":
#     make_new_dataset(country=country,year=year)
#     print("done")
    