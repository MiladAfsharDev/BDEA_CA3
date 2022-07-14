import string
from cassandra.cluster import Cluster
import csv
import pandas as pd
from pandasql import sqldf
from datetime import datetime

session1 = None


def insertDataIntoTwitterOrignal(author, content, country, datetime1, id, count, language, latitude, longitude, numL, numS, userid, dateval):
    session1.execute(
        """
        INSERT INTO tweets_orignal (author, content,country,datetime,id,idkey,language,latitude,longitude,number_of_likes,number_of_shares,userid,century) values
        (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
        """,
        (author, content, country, datetime1, id, count, language,
         latitude, longitude, int(numL), int(numS), userid, dateval)
    )


def insertDataintoTwitterConnects(following, followed):

    session1.execute(
        """
        INSERT INTO twitterConnections (following,followed) values
        (%s,%s);
        """,
        (following, followed)
    )


def insertDataintoFollowedCount(followed, count):
    session1.execute(
        """
        INSERT INTO followedcount (relation,followedaccount,followcount) values
        ('followed',%s,%s);
        """,
        (followed, count)
    )


def insertDataIntoFollowedCount2(followed, count):
    session1.execute(
        """
        INSERT INTO followedcount (relation,followedaccount,followcount) values
        ('follower',%s,%s);
        """,
        (followed, count)
    )


if __name__ == "__main__":
    cluster = Cluster(['127.0.0.1'], port=9042)
    session = cluster.connect('twitter_dataset', wait_for_all_pools=True)
    session.execute('USE twitter_dataset')
    session1 = session
    session.execute(""" 
    CREATE table if not exists tweets_orignal (
    author varchar,
    content varchar,
    country varchar,
    datetime varchar,
    id varchar,
    idkey int,
    language varchar,
    latitude float,
    longitude float,
    number_of_likes int,
    number_of_shares int,
    userid varchar,
    century varchar,
    Primary key (century,number_of_likes,number_of_shares,content,idkey)
    ) with clustering order by (number_of_likes desc,number_of_shares desc)
    """)
    session.execute("""
    CREATE CUSTOM INDEX contentIndex ON tweets_orignal (content) USING 'org.apache.cassandra.index.sasi.SASIIndex' WITH OPTIONS = {'mode': 'CONTAINS', 'analyzer_class': 'org.apache.cassandra.index.sasi.analyzer.StandardAnalyzer', 'case_sensitive': 'false'};
    """)
    with open('tweetsUpdated.csv', 'r', encoding='utf8') as file:
        reader = csv.reader(file)
        count = 0
        for row in reader:
            author = str(row[0])
            content = str(row[1])
            country = str(row[2])
            datetime1 = str(row[3])
            id = str(row[4])
            langugage = str(row[5])
            latitude = str(row[6])
            longitude = str(row[7])
            numL = str(row[8])
            numS = str(row[9])
            userid = str(row[10])
            if(count == 0):
                count = count+1
                continue
            else:
                count = count + 1
                if(latitude == ''):
                    latitude = '0'
                    longitude = '0'
                latitude = float(latitude)
                longitude = float(longitude)
                timee = datetime.strptime(datetime1, '%d/%m/%Y %H:%M')
                dateval = ''
                if timee > datetime.strptime('01/01/2000 01:01', '%d/%m/%Y %H:%M'):
                    dateval = 'Current'
                else:
                    dateval = 'Old'
                insertDataIntoTwitterOrignal(author, content, country, datetime1, id,
                                             count, langugage, latitude, longitude, numL, numS, userid, dateval)

    session.execute("""
    create table twitterConnections(
    following bigint,
    followed bigint,
    primary key (followed,following))
    """)
    Accounts = open("twitter_combined.txt", "r")
    for x in Accounts:
        txt = x.split()
        insertDataintoTwitterConnects(txt[0], txt[1])

    session.execute("""
    create table followedcount(
    relation varchar,
    followedAccount bigint,
    followcount bigint,
    primary key (relation,followcount,followedaccount))
    with clustering order by (followcount desc)
    """)
    rows = session.execute(
        'select followed,count(followed) as countfollowed from twitterConnections group by followed')
    for r in rows:
        insertDataintoFollowedCount(id, r[0], r[1])

    set1 = session.execute('select following,followed from twitterconnections')
    df = pd.DataFrame(set1, columns=['following', 'followed'])
    set2 = session.execute(
        """select followedaccount from followedcount where relation = 'followed'  order by followcount DESC limit 100 ALLOW FILTERING """)
    df1 = pd.DataFrame(set2, columns=['followedaccount'])
    query = """ select following,count(following) as followcount from df where followed in (select followedaccount from df1) group by following order by count(following) desc  """
    df_out = sqldf(query)
    df2 = pd.DataFrame(df_out, columns=['following', 'followcount'])
    df2 = df2.reset_index()
    for index, row in df2.iterrows():
        insertDataIntoFollowedCount2(row['following'], row['followcount'])

    print('All Data Populated')
