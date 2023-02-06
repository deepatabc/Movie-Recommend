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
vectorizer = pickle.load(open(file_path+transformed_model,'rb'))

###################################### Working with apis #############################################

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
    
@app.post("/api/movieDetails")
async def getMovieDetails(movie_details : Request):
  try:
      collected_data = await movie_details.json()
      movie_title = collected_data['movie_title']
      movie_details = get_details(movie_title)
      return {
        "status" : status.HTTP_200_OK,
        "results" : {
          "details" : movie_details
        }
      }
  except Exception as e:
      return {
          "status"  : status.HTTP_409_CONFLICT,
          'message' : str(e)
      }