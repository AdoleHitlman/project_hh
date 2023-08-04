import psycopg2
import requests


class DBManager:
    def __init__(self, host, port, dbname, user, password):
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password

    def connect_to_db(self):
        conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=self.dbname,
            user=self.user,
            password=self.password
        )
        return conn

    def create_tables(self):
        conn = self.connect_to_db()
        cursor = conn.cursor()

        # Создание таблицы с работодателями
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS employers (
                id SERIAL PRIMARY KEY,
                name varchar(255),
                domain varchar(255)
            );
        ''')

        # Создание таблицы с вакансиями
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS vacancies (
                id SERIAL PRIMARY KEY,
                title varchar(255),
                salary float,
                description text,
                employer_id int,
                FOREIGN KEY (employer_id) REFERENCES employers(id)
            );
        ''')

        conn.commit()
        conn.close()

    def get_employers(self):
        url = 'https://api.hh.ru/employers'
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers)
        employers = response.json()
        return employers

    def get_vacancies(self, employer_id):
        url = f'https://api.hh.ru/vacancies?employer_id={employer_id}'
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers)
        vacancies = response.json()
        return vacancies

    def populate_tables(self):
        conn = self.connect_to_db()
        cursor = conn.cursor()

        employers = self.get_employers()
        for employer in employers['items']:
            cursor.execute('INSERT INTO employers (name, domain) VALUES (%s, %s) RETURNING id',
                           (employer['name'], employer['alternate_url']))

            employer_id = cursor.fetchone()[0]
            vacancies = self.get_vacancies(employer['id'])
            if vacancies['found'] != 0:
                for vacancy in vacancies['items']:
                    for v in vacancy:
                        if v == "salary":
                            other_currency = ""
                            if vacancy[v] != None and vacancy[v]['currency'] == 'KZT':
                                other_currency = vacancy[v]['currency']
                            if vacancy[v] == None:
                                if v == "salary":
                                    vacancy[v] = 0
                                else:
                                    vacancy[v] = ''
                            if v == 'salary' and vacancy[v] != 0:
                                if vacancy[v]['from'] == None:
                                    vacancy[v]['from'] = 0
                                if vacancy[v]['to'] == None:
                                    vacancy[v]['to'] = 0
                                if vacancy[v]['from'] > vacancy[v]['to']:
                                    vacancy[v] = vacancy[v]['from']
                                elif vacancy[v]['from'] < vacancy[v]['to']:
                                    vacancy[v] = vacancy[v]['to']
                                elif vacancy[v]['from'] == vacancy[v]['to'] and vacancy[v]['to'] != 0:
                                    vacancy[v] = vacancy[v]['to']
                                if other_currency == "KZT":
                                    vacancy[v] = int(round(vacancy[v] / 4.6,1))
                    cursor.execute(
                        'INSERT INTO vacancies (title, salary, description, employer_id) VALUES (%s, %s, %s, %s)',
                        (vacancy['name'], vacancy['salary'],
                         vacancy['address']['description'], employer_id))

        conn.commit()
        conn.close()

    def get_companies_and_vacancies_count(self):
        conn = self.connect_to_db()
        cursor = conn.cursor()

        cursor.execute('''
            SELECT DISTINCT employers.name, COUNT(vacancies.id) 
            FROM employers 
            LEFT JOIN vacancies ON employers.id = vacancies.employer_id 
            GROUP BY employers.id, employers.name;
        ''')
        companies = cursor.fetchall()

        conn.close()

        return companies

    def get_all_vacancies(self):
        conn = self.connect_to_db()
        cursor = conn.cursor()

        cursor.execute('''
            SELECT employers.name, vacancies.title, vacancies.salary, vacancies.description 
            FROM employers 
            JOIN vacancies ON employers.id = vacancies.employer_id;
        ''')
        vacancies = cursor.fetchall()

        conn.close()

        return vacancies

    def get_avg_salary(self):
        conn = self.connect_to_db()
        cursor = conn.cursor()

        cursor.execute('SELECT AVG(salary) FROM vacancies;')
        avg_salary = cursor.fetchone()[0]

        conn.close()

        return avg_salary

    def get_vacancies_with_higher_salary(self):
        conn = self.connect_to_db()
        cursor = conn.cursor()

        cursor.execute(
            'SELECT employers.name, vacancies.title, vacancies.salary FROM employers JOIN vacancies ON employers.id = vacancies.employer_id WHERE vacancies.salary > (SELECT AVG(salary) FROM vacancies);')
        vacancies = cursor.fetchall()

        conn.close()

        return vacancies

    def get_vacancies_with_keyword(self, keyword):
        conn = self.connect_to_db()
        cursor = conn.cursor()

        cursor.execute(
            'SELECT employers.name, vacancies.title, vacancies.salary FROM employers JOIN vacancies ON employers.id = vacancies.employer_id WHERE vacancies.title ILIKE %s;',
            ('%' + keyword + '%',))
        vacancies = cursor.fetchall()

        conn.close()

        return vacancies
