from flask import Flask, abort, request, jsonify
from elasticsearch import Elasticsearch


app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False

elastic_response_fields = [
    'id',
    'title',
    'description',
    'imdb_rating',
    'writers',
    'actors',
    'genre',
    'director',
]


def validate_args(args):
    try:
        assert int(args.get('limit', 1)) > 0
        assert int(args.get('page', 1)) > 0
        assert args.get('sort', 'id') in ['id', 'title']
        assert args.get('sort_order', 'asc') in ['asc', 'desc']

    except Exception:
        return False

    return True


def _transform(result_data):
    if not result_data:
        return result_data

    # Строгий порядок ключей
    transform_data = {key: result_data[key] for key in elastic_response_fields}
    # Сценаристы упорядочены по фамилии
    transform_data['writers'] = sorted(transform_data['writers'], key=lambda x: x['name'].split(' ')[-1])
    # Актеры упорядочены по ID
    transform_data['actors'] = sorted(transform_data['actors'], key=lambda x: x['id'])

    return transform_data


@app.route('/')
def index():
    return 'worked'


@app.route('/api/movies/')
def movie_list():
    if not validate_args(request.args):
        return abort(422)

    defaults = {
        'limit': 50,
        'page': 1,
        'sort': 'id',
        'sort_order': 'asc'
    }

    # Тут уже валидно все
    for param in request.args.keys():
        defaults[param] = request.args.get(param)

    # Уходит в тело запроса. Если запрос не пустой - мультисерч, если пустой - выдает все фильмы
    body = {
        "query": {
            "multi_match": {
                "query": defaults['search'],
                "fields": ['title', 'description', 'genre', 'actors_names', 'writers_names', 'director']
            },
        }
    } if defaults.get('search', False) else {}

    params = {
        'from': int(defaults['limit']) * (int(defaults['page']) - 1),
        'size': defaults['limit'],
        'sort': [
            {
                defaults["sort"]: defaults["sort_order"]
            }
        ]
    }

    es_client = Elasticsearch([{'host': 'localhost', 'port': 9200}], )
    search_res = es_client.search(
        body=body,
        index='movies',
        params=params,
        _source_includes=elastic_response_fields,
    )
    es_client.close()

    return jsonify([_transform(doc['_source']) for doc in search_res['hits']['hits']])


@app.route('/api/movies/<string:movie_id>')
def get_movie(movie_id):
    es_client = Elasticsearch([{'host': 'localhost', 'port': 9200}], )

    if not es_client.ping():
        print('oh(')

    search_result = es_client.get(index='movies', id=movie_id, ignore=404, _source_includes=elastic_response_fields)

    es_client.close()

    if search_result['found']:
        return jsonify(_transform(search_result['_source']))

    return abort(404)


if __name__ == "__main__":
    app.run(host='localhost', port=8000)
