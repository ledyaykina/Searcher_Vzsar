import time
import json

from elasticsearch import Elasticsearch
from wiki_ru_wordnet import WikiWordnet


class Searcher:
    # Конструктор
    def __init__(self):
        # Переменная, которая инициализирует Elasticsearch
        self.elastic_search = Elasticsearch()
        # Переменная, которая инициализирует WikiWordnet (готовая библиотека с синонимами)
        self.wiki_wordnet = WikiWordnet()
        # Путь к файлу с распаршенными данными
        self.json_path = 'C:\\Users\\ledya\\PycharmProjects\\ir-vzsar\\vzsar\\output.json'
        # Название поискового индекса
        self.index_name = 'news'

    # Метод создания индекса. Индекс содержит документы. У каждого документа свой уникальный ID
    # Документ - это каждая новая строка json файле с парсингом странички.
    # В нашем случае, документ имеет структуру [Заголовок, Тело, Дата, Адрес]
    def create_index(self):
        # Создание индекса с помощью медота create из Elasticsearch
        self.elastic_search.indices.create(
            index=self.index_name,
            body={
                'settings': {
                    'number_of_shards': 1,
                    'number_of_replicas': 0,
                    'analysis': {
                        'filter': {
                            'ru_stop': {
                                'type': 'stop',
                                'stopwords': '_russian_'
                            },
                            'ru_stemming': {
                                'type': 'snowball',
                                'language': 'Russian',
                            }
                        },
                        # Здесь указаны правила, которые будут применяться к построению токенов
                        'analyzer': {
                            'default': {
                                'char_filter': ['html_strip'],
                                'tokenizer': 'standard',
                                'filter': ['lowercase', 'ru_stop', 'ru_stemming']
                            },
                            # Кастомный нужны для поиска синонимом
                            'custom': {
                                'char_filter': ['html_strip'],
                                'tokenizer': 'standard',
                                'filter': ['lowercase', 'ru_stop']
                            }
                        }
                    }
                }
            },
            ignore=400
        )

    # Метод добавления документов в индекс
    def append_doc_to_index(self):
        # Открывает на чтение файл с распаршенными данными (с документами)
        with open(self.json_path, 'r', encoding='utf-8') as input_stream:
            # Загружаем все документы в переменную data
            data = json.loads(input_stream.read())
            # Инициализируем ID для документа
            k = 1
            # Для каждого документа
            for doc in data:
                # Добавляем документ в индекс с его ID = k
                self.elastic_search.index(index=self.index_name, id=k, body=doc)
                # Немного ждем (т.к. Elasticsearch не успевает добавлять документы слишком быстро)
                time.sleep(0.2)
                # Увеличиваем ID -> переходим к следующему документу
                k += 1

    # Метод добавления синонимов
    def add_synonyms(self, query):
        # Временный список с синонимами
        tmp_list = list()
        # Формирование токенов из поискового запроса, по которым будут добавляться синонимы
        all_tokens = self.elastic_search.indices.analyze(index=self.index_name, body={
            'analyzer': 'custom',
            'text': [query]
        })
        # Для каждого токена из запроса добавляем синонимы
        for token in all_tokens['tokens']:
            tmp_list.append(token['token'])
            synonym_sets = self.wiki_wordnet.get_synsets(token['token'])
            if synonym_sets:
                for synonym in synonym_sets[0].get_words():
                    word = synonym.lemma()
                    if tmp_list.count(word) == 0:
                        tmp_list.append(word)
        # Прогоняем синонимы через стеммер
        new_text = ' '.join(tmp_list)
        tmp_list = list()
        all_tokens = self.elastic_search.indices.analyze(index=self.index_name, body={
            'analyzer': 'default',
            'text': [new_text]
        })
        for token in all_tokens['tokens']:
            tmp_list.append(token['token'])
        # Возвращаем список синонимов
        return ' '.join(tmp_list)

    # Метод непосредственно поиска
    def search(self, find_option, query):
        # Прописываем логику выбора полей документов, по которым будем искать
        fields_list = ['title']
        if find_option == '2':
            fields_list = ['body']
        elif find_option == '3':
            fields_list = ['title', 'body']

        # Формируем поисковой запрос
        query_body = {
            'query': {
                'bool': {
                    'should': [
                        {
                            'multi_match': {
                                'query': query,
                                'analyzer': 'default',
                                'fields': fields_list,
                            }
                        },
                    ],
                }
            }
        }
        # Возвращаем результат поскивого запроса
        return self.elastic_search.search(index='news', body=query_body)

    # Метод удаление индекса
    def delete_indices(self):
        # Удаляем все ранее созданные индексы
        for key in self.elastic_search.indices.get_alias().keys():
            self.elastic_search.indices.delete(index=key)


# Интерфейс и вывод результатов на экран
if __name__ == '__main__':
    my_searcher = Searcher()
    my_searcher.create_index()
    my_searcher.append_doc_to_index()

    menu = '-1'
    while menu != '0':
        menu = input('\nВыберите, по чему произвести поиск: '
                       '\n1 - по заголовку'
                       '\n2 - по тексту статьи'
                       '\n3 - по заголовку и тексту статьи'
                       '\n0 - завершить работу'
                       '\nВаш выбор: ')
        if menu == '0':
            my_searcher.delete_indices()
            exit(0)
        elif menu in ['1', '2', '3']:
            # Формируем запрос, добавляя к нему наши синонимы
            new_query = my_searcher.add_synonyms(input('\nВведите запрос: '))
            print(f'\nСинонимы: {new_query}')
            # Получаем результат поиска
            result = my_searcher.search(menu, new_query)

            hits_len = len(result['hits']['hits'])
            print('Всего найдено совпадений:', hits_len)

            print('№', 'Score', 'URL')
            if hits_len > 0:
                for i in range(hits_len):
                   print(i + 1, result['hits']['hits'][i]['_score'], result['hits']['hits'][i]['_source']['url'])
        else:
            print('\nНеверно введенный символ! Попробуйте снова. ')