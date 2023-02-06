###################################### IMPORTING LIBRARIES ############################################
from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
import json
import random
import requests
import pickle
import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity

###################################### FASTAPI SETTING UP #############################################

app = FastAPI()
origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

###################################### LOADING THE MODELS #############################################

file_path = "model/"
trained_model = "trained_model.pkl"
transformed_model = "transformed.pkl"
classifier = pickle.load(open(file_path+trained_model, 'rb'))
vectorizer = pickle.load(open(file_path+transformed_model,'rb'))

###################################### Working with apis #############################################
        
    
@app.post("/api/movieDetails")
async def getMovieDetails(movie_details : Request):
    collected_data = await movie_details.json()
    movie_title = collected_data['movie_title']
    return {
      "status" : status.HTTP_200_OK,
      "results" : {
        "move_title" : movie_title
      }
    }