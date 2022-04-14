import redis
CONN = None


def get_conn(CONN=None):
    if CONN is not None:
        return CONN
    CONN = redis.Redis(db=15)
    return CONN
