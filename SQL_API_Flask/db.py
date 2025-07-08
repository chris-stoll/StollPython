from dataclasses import dataclass
import pyodbc


@dataclass
class DBConfig:
    server: str
    database: str
    username: str
    password: str
    driver: str = '{ODBC Driver 17 for SQL Server}'


class UserDataAccess:
    def __init__(self, config: DBConfig):
        self.config = config

    def get_connection(self):
        conn_str = (
            f"DRIVER={self.config.driver};"
            f"SERVER={self.config.server};"
            f"DATABASE={self.config.database};"
            f"UID={self.config.username};"
            f"PWD={self.config.password}"
        )
        return pyodbc.connect(conn_str)

    def get_user_by_id(self, user_id: int):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("EXEC GetUserById @UserId = ?", user_id)
            row = cursor.fetchone()
            if row:
                columns = [col[0] for col in cursor.description]
                return dict(zip(columns, row))
            return None

    def update_user_first_name(self, user_id: int, first_name: str):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("EXEC UpdateUserFirstName @UserId = ?, @FirstName = ?", user_id, first_name)
            conn.commit()
