import psycopg2

question_one = 'What are the most popular three articles of all time?'
query_one = """
select title, count(*) as views from articles inner join
log on concat('/article/', articles.slug) = log.path
where log.status like '%200%'
group by log.path, articles.title order by views desc limit 3;
"""

question_two = 'Who are the most popular article authors of all time?'
query_two = """
select authors.name, count(*) as views from articles inner join
authors on articles.author = authors.id inner join
log on concat('/article/', articles.slug) = log.path where
log.status like '%200%' group by authors.name order by views desc
"""

question_three = 'On which days did more than 1% of requests lead to errors?'
query_three = """
select * from (
    select a.day,
    round(cast((100*b.hits) as numeric) / cast(a.hits as numeric), 2)
    as errorpec from
        (select date(time) as day, count(*) as hits from log group by day) as a
        inner join
        (select date(time) as day, count(*) as hits from log where status
        like '%404%' group by day) as b
    on a.day = b.day)
as t where errorpec > 1.0;
"""


class Database:
    def __init__(self):
        try:
            self.db = psycopg2.connect('dbname=news')
            self.cursor = self.db.cursor()
        except Exception as e:
            print (e)

    def execute_query(self, query_to_execute):
        self.cursor.execute(query_to_execute)
        return self.cursor.fetchall()

    def get_results(self, question, query_to_execute, subjoin='views'):
        query_to_execute = query_to_execute.replace('\n', ' ')
        result = self.execute_query(query_to_execute)
        print(question)
        for i in range(len(result)):
            print (i + 1, '.', result[i][0], '--', result[i][1], subjoin)
        # blank line
        print('')

    def close_db(self):
        self.db.close()


if __name__ == '__main__':
    db = Database()
    db.get_results(question_one, query_one)
    db.get_results(question_two, query_two)
    db.get_results(question_three, query_three, '% error')
    db.close_db()