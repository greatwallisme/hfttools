import psycopg2 as pg
import random
import time

# make some data
N = 10000
ids = range(0,N)
chars = [str(random.randint(1,10)) for i in range(0,N)]
nums = [random.randint(1,10) for i in range(0,N)]
X = [(ids[i], chars[i], nums[i]) for i in range(0,N)]

# write it to the database
conn = pg.connect(host='localhost', user='colinswaney')
with conn.cursor() as cur:
    cur.execute("create table test (id serial primary key, char varchar, num integer)")
    start = time.time()
    for i in range(0,len(X)):
        # cur.execute("insert into test values(%s, %s, %s);", X[i])
        cur.execute("insert into test values%s;", [X[i]])
conn.commit()  # !
conn.close()
stop = time.time()
print('elapsed time: {}'.format(stop-start))

# make a database for messages
conn = pg.connect(host='localhost', user='colinswaney')
SQL = """create table messages (id serial primary key,
                                date date,
                                sec integer,
                                nano integer,
                                type varchar,
                                event varchar,
                                name varchar,
                                buysell varchar,
                                price integer,
                                shares integer,
                                refno integer,
                                newrefno integer)"""
with conn.cursor() as cur:
    cur.execute(SQL)
conn.commit()
conn.close()

# make a database for order books
levels = 10
conn = pg.connect(host='localhost', user='colinswaney')
db = 'orderbooks'
x = ['date date', 'sec integer', 'nano integer']
x.extend(['bid_prc_' + str(i) + ' integer' for i in range(1,levels)])
x.extend(['ask_prc_' + str(i) + ' integer' for i in range(1,levels)])
x.extend(['bid_vol_' + str(i) + ' integer' for i in range(1,levels)])
x.extend(['ask_vol_' + str(i) + ' integer' for i in range(1,levels)])
varlist = ', '.join(x)
SQL = 'create table ' + db + '(' + varlist + ')'
with conn.cursor() as cur:
    cur.execute(SQL)
conn.commit()
conn.close()


# psycopg2 error handling -- this will handle generic errors
try:
    conn = pg.connect(host='localhost', user='colinswaney')
    with conn.cursor() as cur:
        try:
            cur.execute('create table orderbooks (id serial primary key);')
            conn.commit()
            conn.close()
        except pg.Error as e:
            print(e.pgerror)
except pg.Error as e:
    print('ERROR: unable to connect to database.')
