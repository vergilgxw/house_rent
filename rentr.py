# -*- coding: utf-8 -*-
"""
    Flaskr
    ~~~~~~

    A microblog example application written as Flask tutorial with
    Flask and sqlite3.

    :copyright: (c) 2015 by Armin Ronacher.
    :license: BSD, see LICENSE for more details.
"""

from collections import defaultdict
import difflib
import os
from sqlite3 import dbapi2 as sqlite3
from flask import Flask, request, session, g, redirect, render_template



# create our little application :)
app = Flask(__name__)

# Load default config and override config from an environment variable
app.config.update(dict(
    DATABASE=os.path.join(app.root_path, 'data.db'),
    DEBUG=True,
    SECRET_KEY='development key',
))
app.config.from_envvar('FLASKR_SETTINGS', silent=True)




def connect_db():
    """Connects to the specific database."""
    rv = sqlite3.connect(app.config['DATABASE'])
    rv.row_factory = sqlite3.Row
    return rv


def get_db():
    """Opens a new database connection if there is none yet for the
    current application context.
    """
    if not hasattr(g, 'sqlite_db'):
        g.sqlite_db = connect_db()
    return g.sqlite_db


def init_db():
    db = get_db()

    db.execute(
        "create table if not exists params"
        "(name text unique, value text)")

    db.execute(
        "create table if not exists sp_params("
        "name text unique, value text)")


    db.execute(
        "create table if not exists items("
        "id integer primary key autoincrement, "
        "time timestamp, "
        "title text, "
        "link text unique, "
        "status text default 'unread', "
        "city text)")
    db.execute("create index if not exists idx on items (time)")

    db.execute(
        "create table if not exists sp_param("
        "name text unique, value text)")

    db.commit()



def title_repeat(x, y):
    s = difflib.SequenceMatcher(None, x['title'], y['title'])
    _, _, overlapped_len = s.find_longest_match(0, len(x['title']), 0, len(y['title']))
    return overlapped_len/float(len(x['title'])) > 0.5

def folding(items, info):
    """
    folding the overlapped titles
    """

    nitems = int(info['nitems'])
    fold_list = len(items) * [False]

    id_set = defaultdict(list)
    ret = []
    for i, x in enumerate(items):
        if  fold_list[i]:
            continue

        t = {key: x[key] for key in x.keys()}
        t['fold_num'] = 1
        ret.append(t)
        tid = int(t['id'])
        id_set[tid].append(tid)

        for j, y in enumerate(items[i+1:]):
            if title_repeat(x, y):
                t['fold_num'] += 1
                id_set[tid].append(int(y['id']))
                fold_list[i+j+1] = True

        if len(ret) == nitems:
            break

    return ret, dict(id_set)

def filter_info(items, info):
    """
    filter information
    """
    def sel(x, in_set, out_set):
        result = False
        if len(in_set) == 0:
            result = True
        else:
            for l in in_set:
                if l in x:
                    result = True
                    break

        for l in out_set:
            if l in x:
                return False
        return result

    in_words = [x.strip() for x in info['in_words'].split()]
    out_words = [x.strip() for x in info['out_words'].split()]
    items = [x for x in items if sel(x['title'], in_words, out_words)]

    return items



def display(db, info, status):


    if info['city'] == 'all':
        city_str = ""
    else:
        city_str = "and city='{}' ".format(info['city'])


    query = ("select * from items where datetime(time) > datetime('now', ?) "
            "and status = ? ") + city_str + "order by datetime(time) desc"

    global items

    items = db.execute(query, ['-{} days'.format(int(info['display_days'])), status]).fetchall()

    items = filter_info(items, info)
    items, id_set = folding(items, info)
    session['last_id_set'] = id_set

    return items




def get_filter_info(db):

    db.commit()

    cur = db.execute("select value from params where name='in_words'")
    in_words = cur.fetchone()
    if in_words is not None:
        in_words = in_words[0]
    else:
        in_words = ''

    cur = db.execute("select value from params where name='out_words'")
    out_words = cur.fetchone()
    if out_words is not None:
        out_words = out_words[0]
    else:
        out_words = ''

    cur = db.execute("select value from params where name='display_days'")
    days = cur.fetchone()
    if days is None:
        days = 5
    else:
        days = int(days[0])

    cur = db.execute("select value from params where name='city'")
    city = cur.fetchone()
    if city is None:
        city = 'all'
    else:
        city = city[0]


    cur = db.execute("select value from params where name='nitems'")
    nitems = cur.fetchone()
    if nitems is None:
        nitems = -1
    else:
        nitems = int(nitems[0])

    return {"in_words":in_words, "out_words":out_words, "display_days": days, "nitems": nitems, 'city': city}

def set_filter_info(db, info):

    insert_info = [(k, v) for k, v in info.items()]
    db.executemany("insert or replace into params (name,  value) values (?, ?)", insert_info)
    db.commit()

def set_status(db, status, ids):
    params = [(status, int(x)) for x in ids]
    db.executemany("update items set status=? where id = ?", params)
    db.commit()



@app.cli.command('initdb')
def init_db_command():
    init_db()
    print("Initialized the database.")


@app.teardown_appcontext
def close_db(error):
    """Closes the database again at the end of the request."""
    if hasattr(g, 'sqlite_db'):
        g.sqlite_db.close()



def get_disp_info(status):
    db = get_db()

    info = get_filter_info(db)

    g.items = display(db, info, status)

    city_map = {'all': u'所有', 'beijing': u'北京', 'shenzhen': u'深圳'}
    info['city'] = city_map[info['city']]

    # return id_set
    return {'entries': g.items, 'filter_info': info}

def get_sp_info():

    db = get_db()

    cur = db.execute("select value from sp_params where name='status'")
    status = cur.fetchone()
    if status is None:
        status = 0;
    else:
        status = status[0]

    cur = db.execute("select value from sp_params where name='keywords'")
    kws_str = cur.fetchone()
    if kws_str is None:
        kws = '';
    else:
        kws = kws_str[0]

    cur = db.execute("select value from sp_params where name='ndays'")
    ndays = cur.fetchone()
    if ndays is None:
        ndays = 0;
    else:
        ndays = max(int(ndays[0]), 0)

    info = {'key_words': kws, 'ndays': str(ndays), 'status': str(status)}

    return info



@app.route('/')
def show_unread():
    # return str(get_disp_info('unread'))
    # return (str(session['last_id_set']))
    return render_template('show_unread.html', **get_disp_info('unread'))

@app.route('/read')
def show_read():
    return render_template('show_read.html', **get_disp_info('read'))

@app.route('/collection')
def show_collection():
    return render_template('show_collection.html', **get_disp_info('collection'))

@app.route('/sp')
def show_sp():
    return render_template('show_sp.html', sp_info=get_sp_info())


@app.route('/set_sp', methods=['POST'])
def set_sp():
    status_dict = {u'抓取': 1, u'循环抓取': 2, u'停止': 0}
    status = status_dict[request.form['submit']]
    keywords = ' '.join([x.strip() for x in request.form['sp_keywords'].split()])

    ndays = request.form['sp_ndays']
    try:
        ndays = max(int(ndays), 0)
    except:
        ndays = 0


    db = get_db()
    db.execute("insert or replace into sp_params (name, value) values (?, ?)", ('status',status))
    db.execute("insert or replace into sp_params (name, value) values (?, ?)", ('keywords',keywords))
    db.execute("insert or replace into sp_params (name, value) values (?, ?)", ('ndays', ndays))
    db.commit()
    return redirect(request.referrer)



@app.route('/submit_filter', methods=['POST'])
def submit_filter():
    db = get_db()
    info = {}
    info['in_words']  = ' '.join([x.strip() for x in request.form['in_words'].split()])
    info['out_words'] = ' '.join([x.strip() for x in request.form['out_words'].split()])
    info['display_days']      = request.form['days']
    info['nitems']      = request.form['nitems']
    if request.form['city'] != 'default':
        info['city']      = request.form['city']

    set_filter_info(db, info)

    return redirect(request.referrer)

def augment_id(ids):
    ret_ids = []
    id_set = session['last_id_set']
    for x in ids:
        if x not in id_set:
            ret_ids.append(x)
        else:
            ret_ids.extend(id_set[x])

    return ret_ids



@app.route('/set_type', methods=['POST'])
def set_type():
    status_dict = {u'+未读': 'unread', u'+已读': 'read', u'+收藏': 'collection'}
    ids = request.form.getlist('select')
    ids = augment_id(ids)
    # return str(ids) + '\n' + str(session['last_id_set'])
    status = status_dict[request.form['submit']]
    db = get_db()
    set_status(db, status, ids)
    return redirect(request.referrer)
