import json
import threading
import time
import urllib.parse
import uuid
from conn import get_conn


def to_bytes(x):
    return x.encode() if isinstance(x, str) else x


def to_str(x):
    return x.decode() if isinstance(x, bytes) else x


# <start id="_1311_14471_8266"/>
def check_token(conn, token):
    return conn.hget('login:', token)  # A


# <end id="_1311_14471_8266"/>
# A Fetch and return the given user, if available
# END

# <start id="_1311_14471_8265"/>
def update_token(conn, token, user, item=None):
    timestamp = time.time()  # A
    conn.hset('login:', token, user)  # B
    conn.zadd('recent:', {token: timestamp})  # C
    if item:
        conn.zadd('viewed:' + token, {item: timestamp})  # D
        conn.zremrangebyrank('viewed:' + token, 0, -26)  # E


# <end id="_1311_14471_8265"/>
# A Get the timestamp
# B Keep a mapping from the token to the logged-in user
# C Record when the token was last seen
# D Record that the user viewed the item
# E Remove old items, keeping the most recent 25
# END

# <start id="_1311_14471_8270"/>
QUIT = False
LIMIT = 10000000


def clean_sessions(conn):
    while not QUIT:
        size = conn.zcard('recent:')  # A
        if size <= LIMIT:  # B
            time.sleep(1)  # B
            continue

        end_index = min(size - LIMIT, 100)  # C
        tokens = conn.zrange('recent:', 0, end_index - 1)  # C

        session_keys = []  # D
        for token in tokens:  # D
            token = to_str(token)
            session_keys.append('viewed:' + token)  # D

        # TIP 删除 viewed:tokenxx 这些 zset
        conn.delete(*session_keys)  # E
        # TIP 删除 hash login: 中的一些值
        conn.hdel('login:', *tokens)  # E
        # TIP 删除 recent: 中的一些值
        conn.zrem('recent:', *tokens)  # E


# <end id="_1311_14471_8270"/>
# A Find out how many tokens are known
# B We are still under our limit, sleep and try again
# C Fetch the token ids that should be removed
# D Prepare the key names for the tokens to delete
# E Remove the oldest tokens
# END

# <start id="_1311_14471_8279"/>
def add_to_cart(conn, session, item, count):
    if count <= 0:
        conn.hrem('cart:' + session, item)  # A
    else:
        conn.hset('cart:' + session, item, count)  # B


# <end id="_1311_14471_8279"/>
# A Remove the item from the cart
# B Add the item to the cart
# END

# <start id="_1311_14471_8271"/>
def clean_full_sessions(conn):
    while not QUIT:
        size = conn.zcard('recent:')
        if size <= LIMIT:
            time.sleep(1)
            continue

        end_index = min(size - LIMIT, 100)
        sessions = conn.zrange('recent:', 0, end_index - 1)

        session_keys = []
        for sess in sessions:
            sess = to_str(sess)
            session_keys.append('viewed:' + sess)
            # TIP 这里新增了一个清理购物车的操作
            session_keys.append('cart:' + sess)  # A

        conn.delete(*session_keys)
        conn.hdel('login:', *sessions)
        conn.zrem('recent:', *sessions)


# <end id="_1311_14471_8271"/>
# A The required added line to delete the shopping cart for old sessions
# END

# <start id="_1311_14471_8291"/>
def cache_request(conn, request, callback):
    if not can_cache(conn, request):  # A
        return callback(request)  # A

    page_key = 'cache:' + hash_request(request)  # B
    content = conn.get(page_key)  # C

    if not content:
        content = callback(request)  # D
        conn.setex(page_key, 300, content)  # E

    return content  # F


# <end id="_1311_14471_8291"/>
# A If we cannot cache the request, immediately call the callback
# B Convert the request into a simple string key for later lookups
# C Fetch the cached content if we can, and it is available
# D Generate the content if we can't cache the page, or if it wasn't cached
# E Cache the newly generated content if we can cache it
# F Return the content
# END

# <start id="_1311_14471_8287"/>
def schedule_row_cache(conn, row_id, delay):
    conn.zadd('delay:', {row_id: delay})  # A
    conn.zadd('schedule:', {row_id: time.time()})  # B


# <end id="_1311_14471_8287"/>
# A Set the delay for the item first
# B Schedule the item to be cached now
# END


# <start id="_1311_14471_8292"/>
def cache_rows(conn):
    while not QUIT:
        next = conn.zrange('schedule:', 0, 0, withscores=True)  # A
        now = time.time()
        if not next or next[0][1] > now:
            time.sleep(.05)  # B
            continue

        row_id = next[0][0]
        row_id = to_str(row_id)
        delay = conn.zscore('delay:', row_id)  # C
        if delay <= 0:
            conn.zrem('delay:', row_id)  # D
            conn.zrem('schedule:', row_id)  # D
            conn.delete('inv:' + row_id)  # D
            continue

        row = Inventory.get(row_id)  # E
        conn.zadd('schedule:', {row_id: now + delay})  # F
        row = {to_str(k): to_str(v) for k, v in row.to_dict().items()}
        conn.set('inv:' + row_id, json.dumps(row))  # F


# <end id="_1311_14471_8292"/>
# A Find the next row that should be cached (if any), including the timestamp, as a list of tuples with zero or one items
# B No rows can be cached now, so wait 50 milliseconds and try again
# C Get the delay before the next schedule
# D The item shouldn't be cached anymore, remove it from the cache
# E Get the database row
# F Update the schedule and set the cache value
# END

# <start id="_1311_14471_8298"/>
def update_token(conn, token, user, item=None):
    timestamp = time.time()
    # TIP hash 记录 token 与已登录用户之间的映射
    conn.hset('login:', token, user)
    # TIP zset 记录最近登录的用户
    conn.zadd('recent:', {token: timestamp})
    if item:
        # TIP zset 记录该用户最近浏览的商品，且只保留 25 个
        conn.zadd('viewed:' + token, {item: timestamp})
        # TIP viewd 里面只保存 25 个元素，分数低的就删掉了（删除第 0 到第倒数 26 个）
        #    相当于先将时间戳比较早的那些浏览记录给删了
        conn.zremrangebyrank('viewed:' + token, 0, -26)
        # TIP 给 item 这个元素减一分
        conn.zincrby('viewed:', -1, item)  # A


# <end id="_1311_14471_8298"/>
# A The line we need to add to update_token()
# END

# <start id="_1311_14471_8288"/>
def rescale_viewed(conn):
    while not QUIT:
        conn.zremrangebyrank('viewed:', 20000, -1)  # A
        conn.zinterstore('viewed:', {'viewed:': .5})  # B
        time.sleep(300)  # C


# <end id="_1311_14471_8288"/>
# A Remove any item not in the top 20,000 viewed items
# B Rescale all counts to be 1/2 of what they were before
# C Do it again in 5 minutes
# END

# <start id="_1311_14471_8289"/>
def can_cache(conn, request):
    item_id = extract_item_id(request)  # A
    if not item_id or is_dynamic(request):  # B
        return False
    rank = conn.zrank('viewed:', item_id)  # C
    return rank is not None and rank < 10000  # D


# <end id="_1311_14471_8289"/>
# A Get the item id for the page, if any
# B Check whether the page can be statically cached, and whether this is an item page
# C Get the rank of the item
# D Return whether the item has a high enough view count to be cached
# END


# --------------- Below this line are helpers to test the code ----------------

def extract_item_id(request):
    parsed = urllib.parse.urlparse(request)
    query = urllib.parse.parse_qs(parsed.query)
    return (query.get('item') or [None])[0]


def is_dynamic(request):
    parsed = urllib.parse.urlparse(request)
    query = urllib.parse.parse_qs(parsed.query)
    return '_' in query


def hash_request(request):
    return str(hash(request))


class Inventory(object):
    def __init__(self, id):
        self.id = id

    @classmethod
    def get(cls, id):
        return Inventory(id)

    def to_dict(self):
        return {'id': self.id, 'data': 'data to cache...', 'cached': time.time()}

    # class TestCh02(unittest.TestCase):
    #     def setUp(self):
    #         import redis
    #         self.conn = redis.Redis(db=15)
    #
    #     def tearDown(self):
    #         conn = self.conn
    #         to_del = (
    #             conn.keys('login:*') + conn.keys('recent:*') + conn.keys('viewed:*') +
    #             conn.keys('cart:*') + conn.keys('cache:*') + conn.keys('delay:*') +
    #             conn.keys('schedule:*') + conn.keys('inv:*'))
    #         if to_del:
    #             self.conn.delete(*to_del)
    #         del self.conn
    #         global QUIT, LIMIT
    #         QUIT = False
    #         LIMIT = 10000000
    #         print()
    #         print()
    #
    def test_login_cookies(self):
        conn = self.conn
        global LIMIT, QUIT
        token = str(uuid.uuid4())

        update_token(conn, token, 'username', 'itemX')
        print("We just logged-in/updated token:", token)
        print("For user:", 'username')
        print()

        print("What username do we get when we look-up that token?")
        r = check_token(conn, token)
        print(r)
        print()
        self.assertTrue(r)

        print("Let's drop the maximum number of cookies to 0 to clean them out")
        print("We will start a thread to do the cleaning, while we stop it later")

        LIMIT = 0
        t = threading.Thread(target=clean_sessions, args=(conn,))
        t.setDaemon(1)  # to make sure it dies if we ctrl+C quit
        t.start()
        time.sleep(1)
        QUIT = True
        time.sleep(2)
        if t.isAlive():
            raise Exception("The clean sessions thread is still alive?!?")

        s = conn.hlen('login:')
        print("The current number of sessions still available is:", s)
        self.assertFalse(s)



    def test_shopping_cart_cookies(self):
        conn = self.conn
        global LIMIT, QUIT
        token = str(uuid.uuid4())

        print("We'll refresh our session...")
        update_token(conn, token, 'username', 'itemX')
        print("And add an item to the shopping cart")
        add_to_cart(conn, token, "itemY", 3)
        r = conn.hgetall('cart:' + token)
        print("Our shopping cart currently has:", r)
        print()

        self.assertTrue(len(r) >= 1)

        print("Let's clean out our sessions and carts")
        LIMIT = 0
        t = threading.Thread(target=clean_full_sessions, args=(conn,))
        t.setDaemon(1) # to make sure it dies if we ctrl+C quit
        t.start()
        time.sleep(1)
        QUIT = True
        time.sleep(2)
        if t.isAlive():
            raise Exception("The clean sessions thread is still alive?!?")

        r = conn.hgetall('cart:' + token)
        print("Our shopping cart now contains:", r)

        self.assertFalse(r)
#
#     def test_cache_request(self):
#         conn = self.conn
#         token = str(uuid.uuid4())
#
#         def callback(request):
#             return "content for " + request
#
#         update_token(conn, token, 'username', 'itemX')
#         url = 'http://test.com/?item=itemX'
#         print("We are going to cache a simple request against", url)
#         result = cache_request(conn, url, callback)
#         print("We got initial content:", repr(result))
#         print()
#
#         self.assertTrue(result)
#
#         print("To test that we've cached the request, we'll pass a bad callback")
#         result2 = cache_request(conn, url, None)
#         print("We ended up getting the same response!", repr(result2))
#
#         self.assertEqual(to_bytes(result), to_bytes(result2))
#
#         self.assertFalse(can_cache(conn, 'http://test.com/'))
#         self.assertFalse(can_cache(conn, 'http://test.com/?item=itemX&_=1234536'))
#
#     def test_cache_rows(self):
#         import pprint
#         conn = self.conn
#         global QUIT
#
#         print("First, let's schedule caching of itemX every 5 seconds")
#         schedule_row_cache(conn, 'itemX', 5)
#         print("Our schedule looks like:")
#         s = conn.zrange('schedule:', 0, -1, withscores=True)
#         pprint.pprint(s)
#         self.assertTrue(s)
#
#         print("We'll start a caching thread that will cache the data...")
#         t = threading.Thread(target=cache_rows, args=(conn,))
#         t.setDaemon(1)
#         t.start()
#
#         time.sleep(1)
#         print("Our cached data looks like:")
#         r = conn.get('inv:itemX')
#         print(repr(r))
#         self.assertTrue(r)
#         print()
#         print("We'll check again in 5 seconds...")
#         time.sleep(5)
#         print("Notice that the data has changed...")
#         r2 = conn.get('inv:itemX')
#         print(repr(r2))
#         print()
#         self.assertTrue(r2)
#         self.assertTrue(r != r2)
#
#         print("Let's force un-caching")
#         schedule_row_cache(conn, 'itemX', -1)
#         time.sleep(1)
#         r = conn.get('inv:itemX')
#         print("The cache was cleared?", not r)
#         print()
#         self.assertFalse(r)
#
#         QUIT = True
#         time.sleep(2)
#         if t.isAlive():
#             raise Exception("The database caching thread is still alive?!?")
#
#     # We aren't going to bother with the top 10k requests are cached, as
#     # we already tested it as part of the cached requests test.

if __name__ == '__main__':
    # unittest.main()
    # TIP 实现登录令牌 cookie 的能力，需要
    #   hash 记录已登录用户和 token 之间的映射 login:
    #   zset 保存最近登录用户：token + 时间戳 recent:
    #   zset 保存某个用户最近浏览的商品 viewed:tokenxx
    conn = get_conn(None)
    token = str(uuid.uuid4())

    # 更新 token
    # update_token(conn, token, 'username', 'itemX')
    # print("We just logged-in/updated token:", token)
    # print("For user:", 'username')
    # print()

    # 检查 token 是否登录
    # 上一步给的 token 是:
    # token = '0df69ec5-8905-48c7-b7d9-d52dd31936f8'
    # print("What username do we get when we look-up that token?")
    # r = check_token(conn, token)
    # print(r)
    # print()

    # TIP 清理 session
    # TIP 设定限制 LIMIT，超过 limit 的部分清理掉
    # print("Let's drop the maximum number of cookies to 0 to clean them out")
    # print("We will start a thread to do the cleaning, while we stop it later")
    # LIMIT = 0
    # # TIP 起一个线程去解决清理 session 的问题
    # t = threading.Thread(target=clean_sessions, args=(conn,))
    # t.setDaemon(1)  # to make sure it dies if we ctrl+C quit
    # t.start()
    # time.sleep(1)
    # QUIT = True
    # time.sleep(2)
    # if t.isAlive():
    #     raise Exception("The clean sessions thread is still alive?!?")
    #
    # s = conn.hlen('login:')
    # print("The current number of sessions still available is:", s)

    # TIP 实现购物车，需要
    #   hash 记录购物车信息包含商品 ID 与商品订购数量之间的映射
    # token = str(uuid.uuid4())
    #
    # # 新登录的用户 token 为 cbffb5c1-cf6a-45cf-82d9-ac9fe432d3bc
    # print("We'll refresh our session...")
    # update_token(conn, token, 'username', 'itemX')
    # print("And add an item to the shopping cart")
    # add_to_cart(conn, token, "itemY", 3)
    # r = conn.hgetall('cart:' + token)
    # print("Our shopping cart currently has:", r)
    # print()

    # TIP 清理 session，包括清理购物车中的内容
    # print("Let's clean out our sessions and carts")
    # LIMIT = 0
    # t = threading.Thread(target=clean_full_sessions, args=(conn,))
    # t.setDaemon(1)  # to make sure it dies if we ctrl+C quit
    # t.start()
    # time.sleep(1)
    # QUIT = True
    # time.sleep(2)
    # if t.isAlive():
    #     raise Exception("The clean sessions thread is still alive?!?")
    #
    # r = conn.hgetall('cart:' + token)
    # print("Our shopping cart now contains:", r)
    pass
