import tweepy
import json
import sqlite3
import time
import datetime
import threading

class StreamThread(threading.Thread):
    def __init__(self, listener_class, db, api):
        super().__init__()
        listener = listener_class(db)
        self.stream = tweepy.Stream(auth=api.auth, listener = listener)

    def run(self, filters):
        self.stream.filter(track=filters)

# Simple example StreamListener
class MyStreamListener(tweepy.StreamListener):

    def __init__(self, db):
        super().__init__()
        self.conn = sqlite3.connect(db, detect_types=sqlite3.PARSE_DECLTYPES)
        cursor = self.conn.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS tweet_metadata (
        id INTEGER PRIMARY KEY,
        longitude NUMERIC,
        latitude NUMERIC,
        created_at TIMESTAMP,
        country TEXT
        );''')
        cursor.execute('''CREATE VIRTUAL TABLE IF NOT EXISTS tweet_text
        USING fts4(text);''')

        cursor.execute('''CREATE VIEW IF NOT EXISTS tweet AS
        SELECT id, longitude, latitude, created_at, country, text
        FROM tweet_metadata JOIN tweet_text ON tweet_text.rowid = id;''')

        cursor.execute('''CREATE TRIGGER IF NOT EXISTS metadata_DELETE AFTER DELETE ON tweet_metadata
        BEGIN
          DELETE FROM tweet_text WHERE rowid = OLD.id;
        END;
        ''')
        cursor.execute('''CREATE TRIGGER IF NOT EXISTS tweet_INSERT INSTEAD OF INSERT ON tweet
        BEGIN
            INSERT INTO tweet_metadata(id,longitude,latitude,created_at,country)
                   VALUES (NEW.id, NEW.longitude, NEW.latitude, NEW.created_at, NEW.country);
            INSERT INTO tweet_text(rowid, text) VALUES (NEW.id, NEW.text);
        END;
        ''')
        cursor.execute('''CREATE TRIGGER IF NOT EXISTS tweet_DELETE INSTEAD OF DELETE ON tweet
        BEGIN
            DELETE FROM tweet_metadata WHERE rowid = OLD.rowid;
            DELETE FROM tweet_text WHERE rowid = OLD.rowid;
        END;
        ''')
        self.conn.commit()
        print('Successfully created StreamListener')

    def on_status(self, status):
        # if status.place:
        #     print(status.place.country)
        # else:
        #     print('Unknown Country')
        cursor = self.conn.cursor()
        if status.coordinates and status.coordinates:
            longitude = status.coordinates["coordinates"][0]
            latitude = status.coordinates["coordinates"][1]
            print('latitude: {}, longitude: {}'.format(latitude, longitude))
        else:
            longitude = None
            latitude = None
        if status.place:
            country = status.place.country
        else:
            country = 'Unknown'
        cursor.execute('''INSERT INTO tweet (longitude,latitude,created_at,country,text)
        VALUES (?,?,?,?,?)''', (longitude,latitude,status.created_at,country,status.text))
        self.conn.commit()

def main():

    with open('keys.json') as k:
        secrets = json.load(k)

    auth = tweepy.OAuthHandler(secrets['key'], secrets['secret'])
    auth.set_access_token(secrets['access_key'], secrets['access_secret'])
    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

    streamThread = StreamThread(MyStreamListener, 'candidates.db', api)
    streamThread.run(['hillary', 'clinton', 'donald', 'trump', 'debate', 'shillary', 'drumpf'])

if __name__ == '__main__':
    main()
    while True:
        time.sleep(60)
        # Need to create a new cursor here
        cursor = myStream.conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM tweet')
        print('{} Total tweets collected: {}'.format(datetime.datetime.now(),
                                                     myStream.cursor.fetchone()))
