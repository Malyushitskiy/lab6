import mysql.connector
import pandas as pd
from mysql.connector import Error


def create_connection(host_name, user_name, user_password, database_name=None):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            password=user_password,
            database=database_name,
        )
        print("Подключение к базе данных MySQL прошло успешно")
    except Error as e:
        print(f"Произошла ошибка: {e}")
    return connection


def create_database(connection):
    try:
        cursor = connection.cursor()
        database_name = "Cinema_lab6"
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
        print(f"База данных '{database_name}' создана успешно")
    except Error as e:
        print(f"Произошла ошибка: {e}")


def create_tables(connection, ):
    try:
        database_name = "Cinema_lab6"
        cursor = connection.cursor()
        cursor.execute(f"USE {database_name}")

        # Створення таблиці Movie
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Movies (
                MovieCode SERIAL PRIMARY KEY,
                Title VARCHAR(100) NOT NULL,
                Genre VARCHAR(20) CHECK (Genre IN ('мелодрама', 'комедія', 'бойовик')),
                Duration INT CHECK (Duration > 0),
                Rating NUMERIC(3, 2)
            );
        ''')

        # Створення таблиці Cinemas
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Cinemas (
            CinemaCode SERIAL PRIMARY KEY,
            Name VARCHAR(100) NOT NULL,
            TicketPrice NUMERIC(10, 2),
            SeatCount INT CHECK (SeatCount > 0),
            Address TEXT,
            Phone CHAR(15)
        );
        ''')

        # Создание таблицы "MovieScreenings"
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS MovieScreenings (
            ScreeningCode SERIAL PRIMARY KEY,
            MovieID INT REFERENCES Movies (MovieCode) ON DELETE CASCADE,
            CinemaID INT REFERENCES Cinemas (CinemaCode) ON DELETE CASCADE,
            StartDate DATE,
            DisplayDuration INT CHECK (DisplayDuration > 0)
        );
            
                ''')

        connection.commit()
        print("Таблицы созданы успешно")
    except Error as e:
        print(f"Произошла ошибка: {e}")


def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Запрос выполнен успешно.")
    except Error as e:
        print(f"Произошла ошибка при выполнении запроса: {e}")


def execute_query_print(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        result = cursor.fetchall()

        # Преобразовать результат в объект DataFrame
        df = pd.DataFrame(result, columns=[i[0] for i in cursor.description])

        # Вывести DataFrame
        print(df)

        print("Запит виконано успішно.")
    except Error as e:
        print(f"Произошла ошибка при выполнении запроса: {e}")
    finally:
        cursor.close()


def display(connect):
    connect = connect
    print("MovieScreenings")
    query = ("""SELECT * FROM MovieScreenings """)
    execute_query_print(connect, query)
    print("Cinemas")
    query = ("""SELECT * FROM Cinemas""")
    execute_query_print(connect, query)
    print("Movies")
    query = ("""SELECT * FROM Movies""")
    execute_query_print(connect, query)
    print("Відобразити всі комедії і відсортувати фільми по рейтингу:")
    query = ("""
    SELECT *
        FROM Movies
        WHERE Genre = 'комедія'
        ORDER BY Rating DESC;
    """)
    execute_query_print(connect, query)

    print('Порахувати останню дату показу фільму для кожного транслювання:')
    query = ("""
SELECT
    MS.MovieID,
    MS.CinemaID,
    MS.StartDate,
    MS.DisplayDuration,
    DATE_ADD(MS.StartDate, INTERVAL MS.DisplayDuration DAY) AS Кінцева_дата_показу
FROM MovieScreenings AS MS;

        """)
    execute_query_print(connect, query)
    print('Порахувати суму максимального прибутку для кожного кінотеатру від одного показу:')
    
    query = ("""
    SELECT 
    MS.CinemaID, 
    C.Name AS Назва_кінотеатру, 
    MAX(MS.DisplayDuration * C.TicketPrice) AS Максимальний_прибуток
    FROM MovieScreenings AS MS
    INNER JOIN Cinemas AS C ON MS.CinemaID = C.CinemaCode
    GROUP BY MS.CinemaID, C.Name;

    """)
    execute_query_print(connect, query)

    print('Відобразити всі фільми заданого жанру (запит з параметром, наприклад, для жанру "бойовик"):')
    query = ("""
       SELECT *
    FROM Movies
    WHERE Genre = 'бойовик';
    """)
    execute_query_print(connect, query)

    print('Порахувати кількість фільмів кожного жанру (підсумковий запит):')
    query = ("""
    SELECT Genre, COUNT(*) AS Кількість_фільмів
    FROM Movies
    GROUP BY Genre;
    """)
    execute_query_print(connect, query)

    print("Порахувати кількість мелодрам, комедій, бойовиків, які транслюються в кожному кінотеатрі (перехресний запит):")

    query = ("""
    SELECT 
        C.Name AS Назва_кінотеатру, 
        COUNT(CASE WHEN M.Genre = 'мелодрама' THEN 1 ELSE NULL END) AS Кількість_мелодрам,
        COUNT(CASE WHEN M.Genre = 'комедія' THEN 1 ELSE NULL END) AS Кількість_комедій,
        COUNT(CASE WHEN M.Genre = 'бойовик' THEN 1 ELSE NULL END) AS Кількість_бойовиків
    FROM Cinemas AS C
    LEFT JOIN MovieScreenings AS MS ON C.CinemaCode = MS.CinemaID
    LEFT JOIN Movies AS M ON MS.MovieID = M.MovieCode
    GROUP BY C.CinemaCode, C.Name;
    """)
    execute_query_print(connect, query)


def insert_tables(conn):
    conn = conn
    query = (f"""
    INSERT INTO Cinemas (Name, TicketPrice, SeatCount, Address, Phone)
    VALUES
    ('Кінотеатр 1', 10.50, 200, 'Адреса 1', '123-456-7890'),
    ('Кінотеатр 2', 12.00, 150, 'Адреса 2', '987-654-3210'),
    ('Кінотеатр 3', 9.99, 180, 'Адреса 3', '567-890-1234');

        """)
    execute_query(conn, query)

    query = ("""
    INSERT INTO Movies (Title, Genre, Duration, Rating)
    VALUES
        ('Фильм 1', 'комедія', 120, 8.5),
        ('Фильм 2', 'мелодрама', 110, 7.8),
        ('Фильм 3', 'бойовик', 140, 9.2);
        """)
    execute_query(conn, query)
    query = (f"""
    INSERT INTO MovieScreenings (MovieID, CinemaID, StartDate, DisplayDuration)
    VALUES
    (1, 1, '2023-10-26', 6),
    (2, 2, '2023-10-27', 5),
    (3, 3, '2023-10-28', 7),
    (4, 1, '2023-10-26', 6),
    (5, 2, '2023-10-27', 5),
    (6, 3, '2023-10-28', 7),
    (7, 1, '2023-10-26', 6),
    (8, 2, '2023-10-27', 5),
    (9, 3, '2023-10-28', 7),
    (10, 1, '2023-10-26', 6),
    (11, 2, '2023-10-27', 5),
    (12, 3, '2023-10-28', 7),
    (13, 1, '2023-10-26', 6),
    (14, 2, '2023-10-27', 5),
    (15, 3, '2023-10-28', 7);
            """)
    execute_query(conn, query)
    conn.close()


if __name__ == "__main__":
    config = {
        'host_name': '127.0.0.1',
        'user_name': 'root',
        'user_password': 'root',
    }
    config1 = {
        'host_name': '127.0.0.1',
        'user_name': 'root',
        'user_password': 'root',
        'database_name': 'Cinema_lab6',
    }
    # Подключение к серверу MySQL
    conn = create_connection(**config)

    # Создание базы данных
    create_database(conn)

    # Создание таблиц
    create_tables(conn)
    # Insert Tables
    conn = create_connection(**config1)
    insert_tables(conn)


    conn = create_connection(**config1)
    display(conn)

    conn.close()

    print("База данных и таблицы созданы успешно.")
