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
    data = s3_updated_dataset()
    # Convert a collection of text documents to a matrix of token counts
    cv = CountVectorizer()
    # taking the field of combined data's
    count_matrix = cv.fit_transform(data['comb'])
    # creating a similarity score matrix
    similarity = cosine_similarity(count_matrix)
    return data, similarity


def get_recommended_movies(movie_name):
    movie_name = movie_name.lower()
    try:
        data.head()
        similarity.shape
    except:
        data, similarity = create_similarity()
    if movie_name not in data['movie_title'].unique():
        return ('Sorry! The movie you requested is not in our database. Please check the spelling or try with some other movies')
    else:
        i = data.loc[data['movie_title'] == movie_name].index[0] # to get the exact movie
        similarity_score = list(enumerate(similarity[i])) # similarity taking
        sorted_similarity_movies = sorted(similarity_score, key=lambda x: x[1], reverse=True)
        # excluding first item since it is the requested movie itself
        sorted_similarity_movies = sorted_similarity_movies[1:11]
        titles = []
        for i in range(len(sorted_similarity_movies)):
            a = sorted_similarity_movies[i][0]
            titles.append(data['movie_title'][a])
        return titles


def similar_movies(title):
    similar_movies = get_recommended_movies(title)
    return similar_movies


@app.post("/api/movieDetails")
async def getMovieDetails(movie_details: Request):
    try:
        collected_data = await movie_details.json()
        movie_title = collected_data['movie_title']
        movie_details = get_details(movie_title)
        similar_mv = similar_movies(movie_details['movie_title'])
        return {
            "status": status.HTTP_200_OK,
            "results": {
                "searched_movie": movie_details,
                "similar_movies" : similar_mv
            }
        }
    except Exception as e:
        return {
            "status": status.HTTP_409_CONFLICT,
            'message': str(e)
        }
