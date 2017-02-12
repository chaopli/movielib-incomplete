from flask import Flask, request, jsonify
import sys
from elasticsearch_dsl import Search, A, Q
from elasticsearch import Elasticsearch
app = Flask(__name__)
from flask import render_template
import logging

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)

es = Elasticsearch(['http://elastic:changeme@localhost:9200/'], use_ssl=False)

PAGESIZE = 8


@app.route('/')
@app.route('/movies')
def index():
    page = 1
    sort = request.args.get('sort')
    search = request.args.get('search')
    logger.debug(request.args)
    s = Search(using=es)
    s = s.index('imdb')
    s = s.source(includes=['title', 'poster', '_id'])
    s = s[(page-1)*PAGESIZE:page*PAGESIZE]
    if search:
        s = s.query(Q('multi_match', query=search, fields=['title', 'summary', 'casts'])).extra(size=8)
    if sort:
        s = s.sort(sort)
    ret = s.execute()
    logger.debug(ret.hits)
    movies = get_movies(ret.hits)
    genres = get_genre_agg()
    return render_template('review.html', movies=movies, genres=genres)


@app.route('/movie/<string:mid>')
def movie_page(mid):
    s = Search(using=es)
    s = s.index('imdb')
    s = s.filter('term', _id=mid)
    ret = s.execute()
    return render_template('single.html', movie=get_movie_detail(ret.hits[0].to_dict()))


def get_genre_agg():
    s = Search(using=es)
    s = s.index('imdb')
    s.aggs.bucket('genres', A('terms', field='genres'))
    ret = s.execute()
    return [x['key'] for x in ret.to_dict()['aggregations']['genres']['buckets']]


def get_movie_detail(movie):
    movie['genres'] = '/'.join(movie['genres'])
    movie['creators'] = ', '.join(movie['creators'])
    movie['casts'] = ', '.join(movie['casts'])
    return movie


def get_movies(hits):
    for r in hits:
        r._d_['id'] = r.meta.id
    return [x.to_dict() for x in hits]


@app.route('/suggest/<string:input>')
def get_suggest(input):
    if not input:
        return None
    s = Search(using=es)
    s = s.index('imdb')
    s = s.suggest('suggestion', input, completion={'field': 'suggest'})
    s = s.source(False)
    ret = s.execute()
    results = [x['text'] for x in ret.suggest.suggestion[0]['options']]
    return jsonify(result=results)
