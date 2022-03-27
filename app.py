from flask import Flask, jsonify
import sqlite3

# ниже выбранная краткая информация о животном для отображения при запросе.
# выбрала animal_id как некий уникальный идентификатор животного, тк альтернативный вариант по индексу
# выглядит малоинформативным сам по себе.
query = """
SELECT 
		animals_normalized.id,
		animal_id,
		name,
		breed.breed_name as breed_id,
		outcome_type_data.outcome_type_name as outcome_type_id,
		outcome_subtype_data.outcome_subtype_name as outcome_subtype_id,
		outcome_year_data.outcome_year_value as outcome_year_id
FROM animals_normalized
LEFT JOIN breed
		ON breed.id = animals_normalized.breed_id 
LEFT JOIN outcome_type_data 
		ON outcome_type_data.id = animals_normalized.outcome_type_id 
LEFT JOIN outcome_subtype_data 
		ON outcome_subtype_data.id = animals_normalized.outcome_subtype_id
LEFT JOIN outcome_year_data 
		ON outcome_year_data.id = animals_normalized.outcome_year_id
WHERE animals_normalized.animal_id = :1; 
"""

app = Flask(__name__)


@app.route("/<itemid>")
def show_animal_by_id(itemid):
    """
    показывает краткую информацию о животном по запросу на идентификатор
    :param itemid:
    :return:
    """
    con = app.config['db']
    cursor = con.cursor()

    cursor.execute(query, (itemid, ))
    row = cursor.fetchone()
    cursor.close()

    return jsonify(dict(row))


if __name__ == '__main__':
    connection = sqlite3.connect("animal.db", check_same_thread=False)
    connection.row_factory = sqlite3.Row
    app.config['db'] = connection
    app.run()

