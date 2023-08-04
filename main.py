from Dbmn import DBManager

db_manager = DBManager(host='localhost', port='5432', dbname='hh', user='postgres', password='1')
db_manager.create_tables()
db_manager.populate_tables()

q = 0
answers = [1, 2, 3, 4, 5, 0]
while q == 0:
    user_input = input(
        "Выберете пункт меню:\n1.Вывести список всех компаний и количество вакансий у каждой компании\n2.Вывести список всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на вакансию.\n3.Вывести среднюю зарплату по вакансиям.\n4.Вывести список всех вакансий, у которых зарплата выше средней по всем вакансиям.\n5.Вывести список всех вакансий с ключевым словом\n0.Выход\nВведите ответ----->")
    if type(user_input) == str:
        try:
            user_input = int(user_input)
        except ValueError:
            print("\n\n\nНет такого варианта ответа\n\n\n")
    if user_input == 1:
        companies_and_vacancies = db_manager.get_companies_and_vacancies_count()
        print("\n",companies_and_vacancies)
    if user_input == 2:
        all_vacancies = db_manager.get_all_vacancies()
        print("\n",all_vacancies)
    if user_input == 3:
        avg_salary = db_manager.get_avg_salary()
        print("\n",avg_salary)
    if user_input == 4:
        higher_salary_vacancies = db_manager.get_vacancies_with_higher_salary()
        print("\n",higher_salary_vacancies)
    if user_input == 5:
        keyword = input("\nВведите ключевое слово--->").lower()
        keyword_vacancies = db_manager.get_vacancies_with_keyword(keyword)
        print(keyword_vacancies)
    if user_input == 0:
        q = 1
    if user_input not in answers:
        print("\n\n\nНет такого варианта ответа\n\n\n")
