import sqlite3
from threading import Lock

CREATE_DEALS = """
CREATE TABLE `deals` (
	`time`	INTEGER,
	`sellerId`	TEXT,
	`sellerCastle`	INTEGER,
	`sellerName`	TEXT,
	`buyerId`	TEXT,
	`buyerCastle`	INTEGER,
	`buyerName`	TEXT,
	`item`	TEXT,
	`qty`	INTEGER,
	`price`	INTEGER
);"""

CREATE_OFFERS = """
CREATE TABLE `offers` (
	`time`	INTEGER,
	`sellerId`	TEXT,
	`sellerCastle`	INTEGER,
	`sellerName`	TEXT,
	`item`	TEXT,
	`qty`	INTEGER,
	`price`	INTEGER
);"""

def sync(locker):
    assert isinstance(locker, type(Lock())), 'ERROR: Lock class is required'
    def real_decorator(f):
        def synced_func(*args):
            locker.acquire()
            f(*args)
            locker.release()
        return synced_func
    return real_decorator

db_lock = Lock()

@sync(db_lock)
def deals_push(values):
    c.execute('''INSERT INTO deals VALUES
    (:time, :sellerId, :sellerCastle, :sellerName, :buyerId, :buyerCastle,
    :buyerName, :item, :qty, :price)''', values)

@sync(db_lock)
def offers_push(values):
    c.execute('''INSERT INTO offers VALUES
    (:time, :sellerId, :sellerCastle, :sellerName,
    :item, :qty, :price)''', values)

@sync(db_lock)
def database_commit():
    database.commit()

def database_commit_close():
    database.commit()
    database.close()

def check_and_create_tables():
    c.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [table[0] for table in c.fetchall()]

    if 'deals' not in tables:
        c.execute(CREATE_DEALS)
    if 'offers' not in tables:
        c.execute(CREATE_OFFERS)

    database.commit()

database = sqlite3.connect('cwapi.db', check_same_thread = False)
c = database.cursor()
check_and_create_tables()
