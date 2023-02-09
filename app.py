###################################### IMPORTING LIBRARIES ############################################
from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
import pickle
import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from dotenv import load_dotenv
import json
import os
import requests
load_dotenv()

###################################### Configurations #############################################

app = FastAPI()
origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

TMBD_API_KEY = os.environ.get('TMBD_API_KEY')

###################################### LOADING THE MODELS #############################################

file_path = "model/"
trained_model = "trained_model.pkl"
transformed_model = "transformed.pkl"
classifier = pickle.load(open(file_path+trained_model, 'rb'))
vectorizer = pickle.load(open(file_path+transformed_model, 'rb'))

###################################### Working with apis #############################################


def s3_updated_dataset():
    data = pd.read_csv(
        'https://movie-dataset-live.s3.ap-south-1.amazonaws.com/PreparedData/UpdatedDataSet.csv')
    return data


def get_details(movie_title):
    try:
        data = {}
        url = f"https://api.themoviedb.org/3/search/movie?api_key={TMBD_API_KEY}&query={movie_title}"
        response = requests.get(url)
        movie_details = response.json()
        data['id'] = movie_details['results'][0]['id']
        data['movie_title'] = movie_details['results'][0]['original_title']
        return data
    except Exception as e:
        return (e)


def create_similarity():
    # taking the latest data and training each time.
    data = s3_updated_dataset()
    # Convert a collection of text documents to a matrix of token counts
    cv = CountVectorizer()
    # taking the field of combined data's
    count_matrix = cv.fit_transform(data['comb'])
    # creating a similarity score matrix
    similarity = cosine_similarity(count_matrix)
    return data, similarity


def get_recommended_movies(movie_name,number_of_recommended):
    movie_name = movie_name.lower()
    try:
        # train the model and get latest similarities 
        data, similarity = create_similarity()
    except:
        raise Exception("Similarity Creating Got Failed")
    if movie_name not in data['movie_title'].unique():
        return ('Sorry! The movie you requested is not in our database. Please check the spelling or try with some other movies')
    else:
        # to get the exact movie
        i = data.loc[data['movie_title'] == movie_name].index[0]
        similarity_score = list(enumerate(similarity[i]))  # similarity taking
        sorted_similarity_movies = sorted(
            similarity_score, key=lambda x: x[1], reverse=True)
        # excluding first item since it is the requested movie itself
        # second range will give you the number of movies.
        sorted_similarity_movies = sorted_similarity_movies[1:number_of_recommended]
        titles = []
        for i in range(len(sorted_similarity_movies)):
            a = sorted_similarity_movies[i][0]
            titles.append(data['movie_title'][a])
        return titles

@app.post("/api/movie")
async def get_movie_name(movie_details: Request):
    try:
        collected_data = await movie_details.json()
        movie_title = collected_data['movie_title']
        movie_exist = get_details(movie_title)
        return {
            "status": status.HTTP_200_OK,
            "results": {
                "searched_movie": movie_exist,
            }
        }
    except Exception as e:
        return {
            "status": status.HTTP_404_NOT_FOUND,
            'message': str(e)
        }

def get_runtime(runtime):
    if runtime % 60 == 0:
        runtime = str(round(runtime/60))+" hour(s)"
        return runtime
    else:
        runtime = str(round(runtime/60))+" hour(s) "+str(runtime%60)+" min(s)"
        return runtime

def get_movie_cast(movie_id):
    try:
        cast_info = []
        url = f"https://api.themoviedb.org/3/movie/{movie_id}/credits?api_key={TMBD_API_KEY}"
        response = requests.get(url)
        cast_details = response.json()
        for cast in cast_details['cast']:
            if cast['known_for_department'] == "Acting":
                if cast['popularity'] >= 5:
                    cast_info.append({
                            "cast_id" : cast['id'],
                            "name" : cast['name'],
                            "character" : cast['character'],
                            "profile_url" : f"https://image.tmdb.org/t/p/original{cast['profile_path']}"
                        })
        return cast_info
    except Exception as e:
        return str(e)
    

@app.get("/api/movie/{movie_id}")
def get_movie_details(movie_id: int):
    try:
        url = f"https://api.themoviedb.org/3/movie/{movie_id}?api_key={TMBD_API_KEY}"
        response = requests.get(url)
        movie_details = response.json()
        poster_url = f"https://image.tmdb.org/t/p/original{movie_details['poster_path']}"
        genres_list = ", ".join([data['name'] for data in movie_details['genres']])
        runtime = get_runtime(movie_details['runtime'])
        cast_details = get_movie_cast(movie_id)
        number_of_recommended_movies = 20
        recommended_movies = get_recommended_movies(movie_details['original_title'],number_of_recommended_movies)
        return {
            "status": status.HTTP_200_OK,
            "results": {
                "movie_id" : movie_id,
                "movie_title" : movie_details['original_title'],
                "imdb_id" : movie_details['imdb_id'],
                "poster" : poster_url,
                "overview" : movie_details['overview'],
                "genres" :  genres_list,
                "rating" : movie_details['vote_average'],
                "vote_count" : movie_details['vote_count'],
                "release_date" : movie_details['release_date'],
                "runtime" : runtime,
                "status" : movie_details['status'],
                "cast_details" : cast_details,
                "recommended_movies" : recommended_movies
            }
        }
    except Exception as e:
        return {
            "status": status.HTTP_404_NOT_FOUND,
            'message': str(e)
        }

def get_individual_cast(cast_id):
    try:
        cast_info = {}
        url = f"https://api.themoviedb.org/3/person/{cast_id}?api_key={TMBD_API_KEY}"
        response = requests.get(url)
        cast_details = response.json()
        cast_info['name'] = cast_details['name']
        cast_info['biography'] = cast_details['biography']
        cast_info['birthday'] = cast_details['birthday']
        cast_info['known_for_department'] = cast_details['known_for_department']
        cast_info['place_of_birth'] = cast_details['place_of_birth']
        cast_info['profile_url'] = f"https://image.tmdb.org/t/p/original{cast_details['profile_path']}"
        return cast_info
    except Exception as e:
        return str(e)

@app.get("/api/cast/{cast_id}")
def get_cast_details(cast_id: int):
    try:
        cast_info = get_individual_cast(cast_id)
        return {
            "status": status.HTTP_200_OK,
            "results": cast_info
        }
    except Exception as e:
        return {
            "status": status.HTTP_404_NOT_FOUND,
            'message': str(e)
        }