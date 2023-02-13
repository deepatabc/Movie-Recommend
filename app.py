###################################### IMPORTING LIBRARIES ############################################
from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
import pickle
import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from dotenv import load_dotenv
import os
import requests
import bs4 as bs
import urllib.request
import numpy as np
import re
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
        'https://movie-dataset-keeper.s3.us-east-2.amazonaws.com/PreparedData/UpdatedDataSet.csv')
    return data


def get_title(movie_title):
    """
        1. Here Taking Movie Title and passing it to the tmdb url
        2. collecting information
        3. return dict of data contain movie_id and title.
    """
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
    """
        1. Calling S3 updated Dataset
        2. Convert a collection of text documents to a matrix of token counts,taking text from movie combined column 
        3. Creating a similarity score matrix
        4. returning latest dataset, similarity score.
    """
    data = s3_updated_dataset()
    cv = CountVectorizer()
    count_matrix = cv.fit_transform(data['comb'])
    similarity = cosine_similarity(count_matrix)
    return data, similarity


def get_recommended_movies(movie_name, number_of_recommended):
    """
        1. Taking Movie Title, Number of recommended Movies count.
        2. Collecting Latest dataset, newly trained similarity score. 
        3. Checking name of the movie that we have passed and the dataset contain same movie or not.
        4. Given Movie Similar Score taking.
        5. Sorting The Similar Movies.
        6. excluding first Movie Name since it is the requested movie itself.
        7. Similar Movie Name's collecting from the dataset and returning.
    """
    movie_name = movie_name.lower()
    try:
        data, similarity = create_similarity()
    except:
        raise Exception("Similarity Creating Got Failed")
    if movie_name not in data['movie_title'].unique():
        return ('Sorry! Requested Movie Not in Our Database, Please check spelling.')
    else:
        i = data.loc[data['movie_title'] == movie_name].index[0]
        similarity_score = list(enumerate(similarity[i]))
        sorted_similar_movies = sorted(
            similarity_score, key=lambda x: x[1], reverse=True)
        similar_movies = sorted_similar_movies[1:int(
            number_of_recommended)+1]
        titles = []
        for i in range(len(similar_movies)):
            a = similar_movies[i][0]
            titles.append(data['movie_title'][a])
        return titles


def get_movie_reviews(imdb_id):
    """
        1. Scrapping Movie Reviews from IMDB.
        2. Extracted reviews regex removing (to avoid invalid symbols in the string)
        3. Converting Review text to Numpy Array For Training (you should use 2d Array)
        4. Transforming the np array to vectorizer (get some distinct features out of the text for the model to train)
        5. Using NLP(sentimental analysis) to predict the result using the trained model.
        6. Status will give you based on the predict result (0 represent Average, 1 represent Great)
        7. Function will return combined data (review,status)
    """
    scrapped_reviews = urllib.request.urlopen(
        f"https://www.imdb.com/title/{imdb_id}/reviews?ref_=tt_ov_rt")
    extracted_reviews = bs.BeautifulSoup(scrapped_reviews, 'lxml')
    all_reviews = extracted_reviews.find_all(
        "div", {"class": "text show-more__control"})
    reviews = []
    status = []
    for review in all_reviews:
        if review.text:
            reviews.append(re.sub(r'[^\w]', ' ', review.text))
            numpy_array = np.array([review.text])
            vectorized = vectorizer.transform(numpy_array)
            result = classifier.predict(vectorized)
            status.append('Great' if result else 'Average')
    combined = [{"review": review, "status": status}
                for review, status in zip(reviews, status)]
    return combined


def get_individual_cast(cast_id):
    """
        1. Taking Cast_ID and pass to TMDB api to collect info (Individual Cast Details).
        2. return required Fields (name,biography,birthday,known_for_department,place_of_birth,profile_pic url)
    """
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


def get_runtime(runtime):
    if runtime % 60 == 0:
        runtime = str(round(runtime/60))+" hour(s)"
        return runtime
    else:
        runtime = str(round(runtime/60))+" hour(s) " + \
            str(runtime % 60)+" min(s)"
        return runtime


def get_movie_cast(movie_id):
    """
        1. This Function is Taking movie_id and passing to TMDB api.
        2. Collecting Information.
        3. return Only Actor's who have score more than 5. 
    """
    try:
        cast_info = []
        url = f"https://api.themoviedb.org/3/movie/{movie_id}/credits?api_key={TMBD_API_KEY}"
        response = requests.get(url)
        cast_details = response.json()
        for cast in cast_details['cast']:
            if cast['known_for_department'] == "Acting":
                if cast['popularity'] >= 5:
                    cast_info.append({
                        "cast_id": cast['id'],
                        "name": cast['name'],
                        "character": cast['character'],
                        "profile_url": f"https://image.tmdb.org/t/p/original{cast['profile_path']}"
                    })
        return cast_info
    except Exception as e:
        return str(e)


def get_movies(movie_id):
    """
        1. Taking Movie_id and passing it to TMDB api.
        2. collecting Info.
        3. Calculating Movie Runtime into Hours format.
        4. returning all the required fields as a dict.
    """
    try:
        url = f"https://api.themoviedb.org/3/movie/{movie_id}?api_key={TMBD_API_KEY}"
        response = requests.get(url)
        movie_details = response.json()
        poster_url = f"https://image.tmdb.org/t/p/original{movie_details['poster_path']}"
        genres_list = ", ".join([data['name']
                                for data in movie_details['genres']])
        runtime = get_runtime(movie_details['runtime'])
        return {
            "movie_id": movie_id,
            "movie_title": movie_details['original_title'],
            "imdb_id": movie_details['imdb_id'],
            "poster": poster_url,
            "overview": movie_details['overview'],
            "genres":  genres_list,
            "rating": movie_details['vote_average'],
            "vote_count": movie_details['vote_count'],
            "release_date": movie_details['release_date'],
            "runtime": runtime,
            "status": movie_details['status'],
        }
    except Exception as e:
        return str(e)


@app.post("/api/title")
async def get_movie_name(movie_details: Request):
    try:
        collected_data = await movie_details.json()
        movie_title = collected_data['movie_title']
        title = get_title(movie_title)
        return {
            "status": status.HTTP_200_OK,
            "results": {
                "searched_movie": title,
            }
        }
    except Exception as e:
        return {
            "status": status.HTTP_404_NOT_FOUND,
            'message': str(e)
        }


@app.get("/api/movie/")
def get_movie_details(movie_id: int, recommend_count: int):
    try:
        movie_details = get_movies(movie_id)
        reviews = get_movie_reviews(movie_details['imdb_id'])
        cast_details = get_movie_cast(movie_id)
        number_of_recommended_movies = recommend_count
        recommended_movies = get_recommended_movies(
            movie_details['movie_title'], number_of_recommended_movies)
        recommended_movies = [get_title(name) for name in recommended_movies]
        recommended_movies = [get_movies(movie_id['id'])
                              for movie_id in recommended_movies]
        return {
            "status": status.HTTP_200_OK,
            "results": {
                "movie_id": movie_id,
                "movie_title": movie_details['movie_title'],
                "imdb_id": movie_details['imdb_id'],
                "poster": movie_details['poster'],
                "overview": movie_details['overview'],
                "genres":  movie_details['genres'],
                "rating": movie_details['rating'],
                "vote_count": movie_details['vote_count'],
                "release_date": movie_details['release_date'],
                "runtime": movie_details['runtime'],
                "status": movie_details['status'],
                "reviews": reviews,
                "cast_details": cast_details,
                "recommended_movies": recommended_movies
            }
        }
    except Exception as e:
        return {
            "status": status.HTTP_404_NOT_FOUND,
            'message': str(e)
        }


@app.get("/api/cast/")
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

