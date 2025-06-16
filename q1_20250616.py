"""
人の名前を保存するDB操作のためのSQL,NoSQLのDB、計6種の簡易操作の共通IFを作成。
ORMのDialectとengineの初歩的な実装。
各DBのクラス化とpersonsテーブル作成、コネクション・カーソル設定、CRUD操作の関数化のみ実装し、実際に操作。
"""



import sqlite3
import mysql.connector
from pymongo import MongoClient
import dbm
import pickle
import memcache


"""
SQLite
"""
class SQLiteHandler:
    def __init__(self):
        self.conn = sqlite3.connect(':memory:')
        self.curs = self.conn.cursor()
        self.curs.execute('CREATE TABLE persons (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)')
        self.conn.commit()

    def insert(self, key, value):  # key は使わず name だけでOK
        self.curs.execute('INSERT INTO persons (name) VALUES (?)', (value,))
        self.conn.commit()

    def update(self, key, value):
        self.curs.execute('UPDATE persons SET name = ? WHERE name = ?', (value, key))
        self.conn.commit()

    def delete(self, key):
        self.curs.execute('DELETE FROM persons WHERE name = ?', (key,))
        self.conn.commit()

    def select(self, key):
        self.curs.execute('SELECT * FROM persons WHERE name = ?', (key,))
        return self.curs.fetchall()

    def close(self):
        self.curs.close()
        self.conn.close()

"""
MySQL
"""
class MySQLHandler:
    def __init__(self):
        self._create_database_if_not_exists()
        self.conn = mysql.connector.connect(
            host='127.0.0.1',
            user='root',
            password='',
            database='handler_mysql_db'
        )
        self.curs = self.conn.cursor()
        self.curs.execute('CREATE TABLE IF NOT EXISTS persons '
                          '(id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255))')
        self.conn.commit()

    def _create_database_if_not_exists(self):
        conn = mysql.connector.connect(
            host='127.0.0.1',
            user='root',
            password=''
        )
        curs = conn.cursor()
        curs.execute('CREATE DATABASE IF NOT EXISTS handler_mysql_db')
        conn.commit()
        curs.close()
        conn.close()

    def insert(self, key, value):  # key は使わず name だけでOK
        self.curs.execute('INSERT INTO persons (name) VALUES (%s)', (value,))
        self.conn.commit()

    def update(self, key, value):
        self.curs.execute('UPDATE persons SET name = %s WHERE name = %s', (value, key))
        self.conn.commit()

    def delete(self, key):
        self.curs.execute('DELETE FROM persons WHERE name = %s', (key,))
        self.conn.commit()

    def select(self, key):
        self.curs.execute('SELECT * FROM persons WHERE name = %s', (key,))
        return self.curs.fetchall()

    def close(self):
        self.curs.close()
        self.conn.close()

"""
MongoDB
"""
class MongoHandler:
    def __init__(self):
        self.client = MongoClient('mongodb://localhost:27017/')
        self.db = self.client['handler_mongo_db']
        self.collection = self.db.stacks
        self.collection.delete_many({})  # 初期化

    def insert(self, key, value):
        self.collection.insert_one({'key': key, 'name': value})

    def update(self, key, value):
        self.collection.update_one({'key': key}, {'$set': {'name': value}})

    def delete(self, key):
        self.collection.delete_one({'key': key})

    def select(self, key):
        return list(self.collection.find({'key': key}))

"""
DBM
"""
class DBMHandler:
    def insert(self, key, value):
        with dbm.open('cache', 'c') as db:
            db[key] = value.encode()

    def update(self, key, value):
        self.insert(key, value)

    def delete(self, key):
        with dbm.open('cache', 'c') as db:
            if key in db:
                del db[key]

    def select(self, key):
        with dbm.open('cache', 'r') as db:
            return db.get(key).decode() if key in db else None

"""
Pickle
"""
class PickleHandler:
    def __init__(self):
        self.data = {}

    def insert(self, key, value):
        self.data[key] = value
        self._save()

    def update(self, key, value):
        self.insert(key, value)

    def delete(self, key):
        if key in self.data:
            del self.data[key]
        self._save()

    def select(self, key):
        self._load()
        return self.data.get(key)

    def _save(self):
        with open('data.pickle', 'wb') as f:
            pickle.dump(self.data, f)

    def _load(self):
        try:
            with open('data.pickle', 'rb') as f:
                self.data = pickle.load(f)
        except FileNotFoundError:
            self.data = {}

"""
memcache
"""
class MemcachedHandler:
    def __init__(self):
        self.client = memcache.Client(['127.0.0.1:11211'])

    def insert(self, key, value):
        self.client.set(key, value, time=60)

    def update(self, key, value):
        self.insert(key, value)

    def delete(self, key):
        self.client.delete(key)

    def select(self, key):
        return self.client.get(key)

"""
起動が必要なDBサーバについて、通知する関数
"""
def check_requirements():
    print("■注意：以下のDBはローカルで起動しておいてください：")
    print("MySQL:    　port 3306  (brew services start mysql)")
    print("MongoDB:  　port 27017 (brew services start mongodb-community@7.0)")
    print("Memcached:　port 11211 (brew services start memcached or memcached -vv &)")

"""
DBセットオブジェクトをSQL,NOSQLでそれぞれ作成し
CRUD操作を行う
"""
def main():
    check_requirements()

    sql_dbs = [
        SQLiteHandler(),
        MySQLHandler()
    ]

    nosql_dbs = [
        MongoHandler(),
        DBMHandler(),
        PickleHandler(),
        MemcachedHandler()
    ]

    for db in sql_dbs:
        print(f"\n{db.__class__.__name__}")
        db.insert("Mike", "Mike")
        print("insert:", db.select("Mike"))
        db.update("Mike", "Micheal")
        print("update:", db.select("Micheal"))
        db.delete("Micheal")
        print("delete:", db.select("Micheal"))

    for db in nosql_dbs:
        print(f"\n{db.__class__.__name__}")
        db.insert("user1", "Mike")
        print("insert:", db.select("user1"))
        db.update("user1", "Micheal")
        print("update:", db.select("user1"))
        db.delete("user1")
        print("delete:", db.select("user1"))

if __name__ == '__main__':
    main()