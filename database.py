import sqlite3
from threading import Lock

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

database = sqlite3.connect('cwapi.db', check_same_thread = False)
c = database.cursor()
