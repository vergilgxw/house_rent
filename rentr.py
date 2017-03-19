# -*- coding: utf-8 -*-
"""
    Flaskr
    ~~~~~~

    A microblog example application written as Flask tutorial with
    Flask and sqlite3.

    :copyright: (c) 2015 by Armin Ronacher.
    :license: BSD, see LICENSE for more details.
"""

import os
from sqlite3 import dbapi2 as sqlite3
from flask import Flask, request, session, g, redirect, url_for, abort, \
     render_template, flash

import difflib
import numpy as np


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
        "create table if not exists in_words"
        "(word text unique)")
    db.execute(
        "create table if not exists out_words"
        "(word text unique)")
    db.execute(
        "create table if not exists display_days"
        "(day int)")

    db.execute(
        "create table if not exists agent_authors"
        "(author_link text)")

    db.commit()



def title_repeat(x, y):
    s = difflib.SequenceMatcher(None, x['title'], y['title'])  
    _, _, overlapped_len = s.find_longest_match(0, len(x['title']), 0, len(y['title'])) 
    return overlapped_len/float(len(x['title'])) > 0.5

def folding(items):
    """
    folding the overlapped titles
    """

    fold_list = len(items) * [False]

    ret = []
    for i, x in enumerate(items):
        if  fold_list[i]:
            continue

        t = {key: x[key] for key in x.keys()}
        t['fold_num'] = 1
        t['fold_ids'] = []
        ret.append(t)

        for j, y in enumerate(items[i+1:]):
            if x['author_link'] != y['author_link']:
                continue
            if title_repeat(x, y): 
                t['fold_num'] += 1
                t['fold_ids'].append(y['id'])
                fold_list[i+j+1] = True


    return ret 

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

    items = [x for x in items if sel(x['title'], info['in_words'], info['out_words'])]
            # and x['author_link'] not in self.ban_authors]

    return items

def set_ban_author(db):
    db.execute("delete from agent_authors")
    db.execute("insert or ignore into agent_authors (author_link) "
            "select author_link from items where status='agent'")
    db.commit()



def display(db, info, status):


    set_ban_author(db)
    if status == 'agent':
        agent_str1 = agent_str2 = ""
    else:
        agent_str1 = ("left join agent_authors "
                    "on items.author_link == agent_authors.author_link ")
        agent_str2 =  "agent_authors.author_link is null and "

    query = ("select * "
            "from items ") + agent_str1 + "where " + agent_str2 + ("datetime(time) > datetime('now', ?) "
            "and status = ? "
            "order by datetime(time) desc")

    global items

    items = db.execute(query, ['-{} days'.format(int(info['days'])), status]).fetchall()

    items = filter_info(items, info)
    items = folding(items)

    return items




def get_filter_info(db):
    
    db.commit()

    cur = db.execute("select * from in_words")
    in_words = [x[0] for x in cur.fetchall()]

    cur = db.execute("select * from out_words")
    out_words = [x[0] for x in cur.fetchall()]

    cur = db.execute("select * from display_days")
    days = cur.fetchone()
    if days is not None:
        days = days[0]

    return {"in_words":in_words, "out_words":out_words, "days": days}

def set_filter_info(db, info):

    db.execute("delete from in_words")
    db.execute("delete from out_words")
    db.execute("delete from display_days")
    db.executemany("insert or ignore into in_words values (?)", [(x,) for x in info['in_words']])
    db.executemany("insert or ignore into out_words values (?)", [(x,) for x in info['out_words']])
    db.execute("insert into display_days values (?)", (info['days'],))
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
    if info['days'] is None:
        info['days'] = 5

    g.items = display(db, info, status)

    info['in_words'] =  ' '.join(info['in_words'])
    info['out_words'] = ' '.join(info['out_words'])
    return {'entries': g.items, 'filter_info': info}



@app.route('/')
def show_unread():
    return render_template('show_unread.html', **get_disp_info('unread'))


@app.route('/read')
def show_read():
    return render_template('show_read.html', **get_disp_info('read'))

@app.route('/agent')
def show_agent():
    return render_template('show_agent.html', **get_disp_info('agent'))

@app.route('/collection')
def show_collection():
    return render_template('show_collection.html', **get_disp_info('collection'))


@app.route('/submit_filter', methods=['POST'])
def submit_filter():
    db = get_db()
    info = {}
    info['in_words']  = [x.strip() for x in request.form['in_words'].split()]
    info['out_words'] = [x.strip() for x in request.form['out_words'].split()]
    info['days']      = request.form['days']
    
    set_filter_info(db, info)

    return redirect(request.referrer)

def augment_id(ids):
    
    db = get_db()
    ids = [int(x) for x in ids]
    authors = []
    anchor_items = []
    ret_ids = []
    for x in ids:
        anchor_item = db.execute("select title, author_link from items where id = ?", [x]).fetchone()
        author_link = anchor_item['author_link'] 
        items = db.execute("select id, title, author_link "
        "from items where author_link = ?", (author_link,)).fetchall()
        for y in items:
            if title_repeat(anchor_item, y):
                ret_ids.append(y['id'])
    
    return ret_ids
    


@app.route('/set_type', methods=['POST'])
def set_type():
    status_dict = {u'未读': 'unread', u'已读': 'read', u'收藏': 'collection', u'中介': 'agent'}
    ids = request.form.getlist('select') 
    ids = augment_id(ids)
    status = status_dict[request.form['submit']]
    db = get_db()
    set_status(db, status, ids)
    return redirect(request.referrer)
        

