import sqlite3
import json

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk


def extract():
    """
    extract data from sql-db
    """
    connection = sqlite3.connect("db.sqlite")
    cursor = connection.cursor()

    # Получаем все поля для индекса, кроме списка сценаристов
    cursor.execute("""
        SELECT movies.id, movies.title, movies.genre, movies.plot, movies.imdb_rating, movies.director,
               movies.writer, movies.writers, GROUP_CONCAT(actors.id), GROUP_CONCAT(actors.name)
        FROM movies JOIN movie_actors ON movies.id = movie_actors.movie_id
                    JOIN actors ON movie_actors.actor_id = actors.id
        GROUP BY movies.id
    """)

    raw_data = cursor.fetchall()

    # Получаем всех сценаристов
    writers_dict = {row[0]: row[1] for row in cursor.execute('SELECT id, name FROM writers WHERE name != "N/A"')}

    connection.close()

    return _transform(raw_data, writers_dict)


def _transform(raw_data, writers_dict):
    """

    :param __actors:
    :param __writers:
    :param __raw_data:
    :return:
    """
    documents_list = []
    for movie_info in raw_data:
        # Разыменование списка
        movie_id, title, genre, plot, imdb_rating, director, writer, writers, actors_ids, actors_names = movie_info

        if writers and writers[0] == '[':
            parsed = json.loads(writers)
            new_writers = [writer_row['id'] for writer_row in parsed]
        else:
            new_writers = [writer]

        writers_list = [(writer_id, writers_dict.get(writer_id)) for writer_id in new_writers]
        actors_list = zip(actors_ids.split(','), actors_names.split(','))

        document = {
            "_index": "movies",
            "_id": movie_id,
            "id": movie_id,
            "imdb_rating": float(imdb_rating) if imdb_rating != 'N/A' else None,
            "genre": genre.split(', '),
            "title": title,
            "description": plot if plot != 'N/A' else None,
            "director": [director] if director != 'N/A' else None,
            "actors": [
                {
                    "id": int(actor[0]),
                    "name": actor[1]
                }
                for actor in set(actors_list) if actor[1] != 'N/A'
            ],
            "writers": [
                {
                    "id": writer[0],
                    "name": writer[1]
                }
                for writer in set(writers_list) if writer[1]
            ]
        }

        document['actors_names'] = [actor['name'] for actor in document['actors']]
        document['writers_names'] = [writer['name'] for writer in document['writers']]

        documents_list.append(document)

    return documents_list


def load(acts):
    """

    :param acts:
    :return:
    """
    es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    bulk(es, acts)

    es.close()

    return True


if __name__ == '__main__':
    load(extract())
