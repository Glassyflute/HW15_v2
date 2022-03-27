# `'age_upon_outcome'` — возраст животного на момент прибытия в приют.
# `'animal_id'` — идентификатор животного.
# `'animal_type'` — тип животного.
# `'name'` — кличка.
# `'breed'` — порода.
# `'color1', 'color2'` — цвет или сочетание цветов.
# `'date_of_birth'` — дата рождения.
# `'outcome_subtype'` — программа, в которой участвует животное. (В Америке есть разные варианты программ для бездомных
# животных. Например, про SCRP из нашей таблицы можно прочесть [здесь]
# (https://www.maddiesfund.org/austin-animal-services-stray-cat-return-program.htm).)
# `'outcome_type'` — что сейчас с животным.
# `'outcome_month'` — месяц прибытия.
# `'outcome_year'` — год прибытия.

import sqlite3


def main():
    connection = sqlite3.connect("animal.db")
    cursor = connection.cursor()

    sqlite_query = ("""
    --проверка на повторы - нормализация по НФ 3 нужна
--по name, date_of_birth нелогично считать повторы. есть повторы в animal_id, но мало. animal_id не равно идентификатор (id) в колонке index.
--В текущей версии считаю animal_id ближе к уникальному значению (как кличка, дата рождения), чем к нормализуемым значениям;
--либо просто опечатки при внесении данных; либо животное несколько раз попадало в приют и по нему заводили новую запись (animal_id оставался, а
--возраст попадания в приют и далее действия менялись.)

-- Создаем пустые таблицы в нужном формате и определяем на этом этапе типы данных и связи

-- сейчас тип животного - Cat (единственный), но далее может меняться.
CREATE TABLE IF NOT EXISTS animal_type_data (
id integer PRIMARY KEY AUTOINCREMENT,
type_name varchar(30) NOT NULL
);

CREATE TABLE IF NOT EXISTS breed (
id integer PRIMARY KEY AUTOINCREMENT,
breed_name varchar(30) NOT NULL
);

CREATE TABLE IF NOT EXISTS color1_data (
id integer PRIMARY KEY AUTOINCREMENT,
color1_name varchar(30) NOT NULL
);

CREATE TABLE IF NOT EXISTS color2_data (
id integer PRIMARY KEY AUTOINCREMENT,
color2_name varchar(30)
);

CREATE TABLE IF NOT EXISTS outcome_type_data (
id integer PRIMARY KEY AUTOINCREMENT,
outcome_type_name varchar(30)
);

CREATE TABLE IF NOT EXISTS outcome_subtype_data (
id integer PRIMARY KEY AUTOINCREMENT,
outcome_subtype_name varchar(30)
);

CREATE TABLE IF NOT EXISTS outcome_year_data (
id integer PRIMARY KEY AUTOINCREMENT,
outcome_year_value integer NOT NULL
);

CREATE TABLE IF NOT EXISTS outcome_month_data (
id integer PRIMARY KEY AUTOINCREMENT,
outcome_month_value integer NOT NULL
);

-- пока относительно мало вариаций значений, есть много (тысяч) повторов на некоторые значения. выделила в таблицу.
CREATE TABLE IF NOT EXISTS age_upon_outcome_data (
id integer PRIMARY KEY AUTOINCREMENT,
age_upon_outcome_value varchar(30) NOT NULL
);



CREATE TABLE IF NOT EXISTS animals_normalized (
        id integer PRIMARY KEY AUTOINCREMENT,
        animal_id varchar(20) NOT NULL,
        name varchar(30),
        date_of_birth date,
        animal_type_id integer,
        breed_id integer,
        color1_id integer,
        color2_id integer,
        outcome_type_id integer,
        outcome_subtype_id integer,
        outcome_year_id integer,
        outcome_month_id integer,
        age_upon_outcome_id integer,
        FOREIGN KEY (animal_type_id) REFERENCES animal_type_data(id),
        FOREIGN KEY (breed_id) REFERENCES breed(id),
        FOREIGN KEY (outcome_type_id) REFERENCES outcome_type_data(id),
        FOREIGN KEY (outcome_subtype_id) REFERENCES outcome_subtype_data(id),
        FOREIGN KEY (outcome_year_id) REFERENCES outcome_year_data(id),
        FOREIGN KEY (outcome_month_id) REFERENCES outcome_month_data(id),
        FOREIGN KEY (age_upon_outcome_id) REFERENCES age_upon_outcome_data(id),
        FOREIGN KEY (color1_id) REFERENCES color1_data(id),
        FOREIGN KEY (color2_id) REFERENCES color2_data(id)
);

-- далее наполняем созданные связанные таблицы данными из оригинальной таблицы animals
INSERT INTO breed (breed_name)
SELECT breed
FROM animals
GROUP BY breed;

INSERT INTO color2_data (color2_name)
SELECT color2
FROM animals
WHERE color2 != '' or color2 != NULL
GROUP BY color2;

INSERT INTO color1_data (color1_name)
SELECT color1
FROM animals
WHERE color1 != '' or color1 != NULL
GROUP BY color1;

INSERT INTO animal_type_data (type_name)
SELECT animal_type
FROM animals
WHERE animal_type != '' or animal_type != NULL
GROUP BY animal_type;

INSERT INTO age_upon_outcome_data (age_upon_outcome_value)
SELECT age_upon_outcome
FROM animals
WHERE age_upon_outcome != '' or age_upon_outcome != NULL
GROUP BY age_upon_outcome;

INSERT INTO outcome_month_data (outcome_month_value)
SELECT outcome_month
FROM animals
WHERE outcome_month != '' or outcome_month != NULL
GROUP BY outcome_month;

INSERT INTO outcome_year_data (outcome_year_value)
SELECT outcome_year
FROM animals
WHERE outcome_year != '' or outcome_year != NULL
GROUP BY outcome_year;

INSERT INTO outcome_type_data (outcome_type_name)
SELECT outcome_type
FROM animals
WHERE outcome_type != '' or outcome_type != NULL
GROUP BY outcome_type;

INSERT INTO outcome_subtype_data (outcome_subtype_name)
SELECT outcome_subtype
FROM animals
WHERE outcome_subtype != '' or outcome_subtype != NULL
GROUP BY outcome_subtype;



-- переносим данные в новую таблицу animals_normalized из оригинальной таблицы animals и связанных таблиц

INSERT INTO animals_normalized (
        animal_id,
        name,
        date_of_birth,
        animal_type_id,
        breed_id,
        color1_id,
        color2_id,
        outcome_type_id,
        outcome_subtype_id,
        outcome_year_id,
        outcome_month_id,
        age_upon_outcome_id
    )
SELECT
        animal_id,
        name,
        date_of_birth,
        animal_type_data.id as animal_type_id,
        breed.id as breed_id,
        color1_data.id as color1_id,
        color2_data.id as color2_id,
        outcome_type_data.id as outcome_type_id,
        outcome_subtype_data.id as outcome_subtype_id,
        outcome_year_data.id as outcome_year_id,
        outcome_month_data.id as outcome_month_id,
        age_upon_outcome_data.id as age_upon_outcome_id
FROM animals
LEFT JOIN animal_type_data
        ON animal_type_data.type_name = animals.animal_type
LEFT JOIN breed
        ON breed.breed_name = animals.breed
LEFT JOIN color1_data
        ON color1_data.color1_name = animals.color1
LEFT JOIN color2_data
        ON color2_data.color2_name = animals.color2
LEFT JOIN outcome_type_data
        ON outcome_type_data.outcome_type_name = animals.outcome_type
LEFT JOIN outcome_subtype_data
        ON outcome_subtype_data.outcome_subtype_name = animals.outcome_subtype
LEFT JOIN outcome_year_data
        ON outcome_year_data.outcome_year_value = animals.outcome_year
LEFT JOIN outcome_month_data
        ON outcome_month_data.outcome_month_value = animals.outcome_month
LEFT JOIN age_upon_outcome_data
        ON age_upon_outcome_data.age_upon_outcome_value = animals.age_upon_outcome;
    """)

    cursor.executescript(sqlite_query)
    cursor.close()
    connection.close()


if __name__ == '__main__':
    main()
