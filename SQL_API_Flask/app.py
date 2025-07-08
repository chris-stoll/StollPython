from flask import Flask, request, jsonify
from db import DBConfig, UserDataAccess

app = Flask(__name__)

# Initialize DB access
db_config = DBConfig(
    server='your_server_name',
    database='your_database_name',
    username='your_username',
    password='your_password'
)
user_dao = UserDataAccess(db_config)


@app.route('/get-user', methods=['GET'])
def get_user():
    user_id = request.args.get('id', type=int)
    if user_id is None:
        return jsonify({"error": "Missing 'id' parameter"}), 400

    try:
        result = user_dao.get_user_by_id(user_id)
        if result:
            return jsonify(result)
        return jsonify({"message": "User not found"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/update-user', methods=['POST'])
def update_user():
    data = request.get_json()
    user_id = data.get('id')
    first_name = data.get('first_name')

    if not user_id or not first_name:
        return jsonify({"error": "Missing 'id' or 'first_name'"}), 400

    try:
        user_dao.update_user_first_name(user_id, first_name)
        return jsonify({"message": "User first name updated successfully"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True)
