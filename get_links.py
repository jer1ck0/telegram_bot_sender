import requests
import sqlite3
from conf import *
import time
import telebot
import random
import logging

logging.basicConfig(filename = LOG, filemode = 'a', level = logging.DEBUG, format = u'%(filename)s[LINE:%(lineno)d]# %(levelname)-8s [%(asctime)s]  %(message)s')

class db_connect: 
    def __init__(self, dbname):
        self.dbname = dbname

    def __enter__(self):
        self.conn = sqlite3.connect(self.dbname)
        logging.debug("Succesfull connect to DB")
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.commit()
        logging.debug("Base was changed")
        self.conn.close()

def post_wall_grabber(db):
    wallcontent = requests.get('https://api.vk.com/method/wall.get',  # parse request with parametres
                               params={
                                   'access_token': API_KEY,
                                   'v': VERSION,
                                   'domain': GROUP_DOMAIN,
                                   'count': COUNT,
                                   'offset': OFFSET
                               }
                               )
    data = wallcontent.json()['response']['items']
    with db_connect(db) as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("""CREATE TABLE items (id, filename, rate, posted, link)""")
            logging.debug('Table was created')
        except sqlite3.OperationalError:
            pass
        for each in data:
            postrate = each['likes']['count'] / each['views']['count'] * 100
            if postrate > 2.5:
                postid = each['id']
                postname = "img/" + str(each['id']) + '.gif'
                posturl = each['attachments'][0]['doc']['url']
                cursor_data = cursor.execute("SELECT * FROM items WHERE (id IS ?)", (postid,)).fetchall()
                if cursor_data == [] or cursor_data[0][0] != postid:
                    cursor.execute("INSERT INTO items VALUES (?,?,?,?,?)", (postid, postname, postrate, 0, posturl))
                    logging.debug ("Post {} added to database".format(postid))
                else:
                    logging.debug ("{} no added".format(postid))
                    pass
        logging.debug("All new post added")

def posting_to_chat():
    with db_connect(DATABASE) as connct:
        cursor = connct.cursor()
        a = cursor.execute('SELECT id, link, filename FROM items where posted = 0').fetchone()
        if a != None:
            id = a[0]
            link = a[1]
            bot = telebot.TeleBot(TG_TOKEN)
            try:
                bot.send_animation(-1001457583348,link)
                logging.debug ("{} posted to chat".format(id))
            except telebot.apihelper.ApiTelegramException:
                logging.debug ("Houston we have a problem")
                pass
            cursor.execute('UPDATE items SET posted = 1 WHERE id = (?)', (id,))
        else:
            pass

if __name__ == "__main__":
    temp_count = 0   
    post_wall_grabber(DATABASE)
    while True:
        if temp_count != 30:
            posting_to_chat()
            time.sleep(3600)
            temp_count += 1
        else:
            logging.debug ("Need fresh posts")
            post_wall_grabber(DATABASE)
            temp_count = 0
