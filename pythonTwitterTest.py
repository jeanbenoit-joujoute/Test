import twitter
from pymongo import MongoClient
import time
from time import gmtime,strftime
import sys
CONSUMER_KEY =
CONSUMER_SECRET =
ACCESS_TOKEN =
ACCESS_TOKEN_SECRET = 
MAX_COUNT_USERTIMELINE_ENDPOINT = 200
MAX_COUNT_SEARCH_ENDPOINT = 100
CALL_TIME_LIMIT_USERTIMELINE_ENDPOINT = 900
CALL_NUMBER_LIMIT_USERTIMELINE_ENDPOINT = 800
CALL_TIME_LIMIT_SEARCH_ENDPOINT = 900
CALL_NUMBER_LIMIT_SEARCH_ENDPOINT = 800
UT_TYPE = "ut"
SH_TYPE = "sh"

def auth():
    """
        Method that connect to twitter api
    """
    my_consumer_key = CONSUMER_KEY
    my_consumer_secret = CONSUMER_SECRET
    my_access_token = ACCESS_TOKEN
    my_access_token_secret = ACCESS_TOKEN_SECRET
    my_api = twitter.Api(consumer_key=my_consumer_key, consumer_secret=my_consumer_secret,
                         access_token_key=my_access_token, access_token_secret=my_access_token_secret)
    return my_api

def storeToMongo(client, doc):
    client = MongoClient(client)
    db = client.marcTest
    db.statuses.insert_one(doc)

def pull_ut (api, screen, collect_rts, exclude_replies, new_last, client,sleep):
    try:
        return api.GetUserTimeline(screen_name=screen, include_rts=collect_rts, exclude_replies=exclude_replies,
                                   max_id=new_last, count=MAX_COUNT_USERTIMELINE_ENDPOINT)
    except twitter.error.TwitterError as err:
        if err.message[0]["code"] == 131 | err.message[0]["code"] == 130:
            print(strftime("%Y%m%d%H%M%S ", gmtime())," - [UserFeed] -- Twitter error ",err.message[0]["code"],
                  " restart pulling with a little sleep and a lowest freq...")
            return restart_pulling(api=api,type=UT_TYPE, screen=screen, last=new_last, client=client,
                                   my_sleep=sleep["short"], freq=10, collect_rts=collect_rts,
                                   exclude_replies=exclude_replies)
        elif err.message[0]["code"] >= 420 & err.message[0]["code"] >= 504:
            print(strftime("%Y%m%d%H%M%S ", gmtime()), " - [UserFeed] -- Twitter error ", err.message[0]["code"],
                  " restart pulling with a medium sleep and a lowest freq...")
            return restart_pulling(api=api,type=UT_TYPE, screen=screen, last=new_last, client=client,
                                      my_sleep=sleep["medium"], freq=15, collect_rts=collect_rts,
                                      exclude_replies=exclude_replies)

def pull_sh (api, query, new_last, client,sleep):
    try:
        return api.GetSearch(raw_query=query, since_id=1, max_id=new_last, count=MAX_COUNT_SEARCH_ENDPOINT)
    except twitter.error.TwitterError as err:
        if err.message[0]["code"] == 131 | err.message[0]["code"] == 130:
            print(strftime("%Y%m%d%H%M%S ", gmtime())," - [UserFeed] -- Twitter error ",err.message[0]["code"],
                  " restart pulling with a little sleep and a lowest freq...")
            return restart_pulling(api=api,type=SH_TYPE, query=query, last=new_last, client=client,
                                   my_sleep=sleep["short"], freq=10)
        elif err.message[0]["code"] >= 420 & err.message[0]["code"] >= 504:
            print(strftime("%Y%m%d%H%M%S ", gmtime()), " - [UserFeed] -- Twitter error ", err.message[0]["code"],
                  " restart pulling with a medium sleep and a lowest freq...")
            return restart_pulling(api=api,type=SH_TYPE, query=query, last=new_last, client=client,
                                   my_sleep=sleep["medium"], freq=15)



def restart_pulling(api,type,last,client, my_sleep=0, freq=10,query="",screen="", collect_rts = False,
                    exclude_replies=True):
    """
        Method that pull all twitter statuses for a given screen_name and store it in mongoDB
        @type  api: object
        @param api: The slope of the line.
        @type  screen: text
        @param screen: The screen_name of user to target
        @type  last: number
        @param last: ID of the newest tweet where the method have to stop pulling
        @type  exclude_replies: boolean
        @param exclude_replies: Tell if the method have to collect replies of the status
        @type  client: text
        @param client: Host of the mongoDB
        @type  my_sleep: number
        @param my_sleep: number of seconds before the method start
        @type  freq: number
        @param freq: number of seconds between each twitter api calls
    """
    if (type != UT_TYPE) & (type != UT_TYPE):
        return "Type Error", 2
    if (query == "") & (screen == ""):
        return "Screen and query are empty", 3

    my_cpt = 0
    time.sleep(my_sleep)
    call_time_limit = CALL_TIME_LIMIT_USERTIMELINE_ENDPOINT
    call_number_limit = CALL_NUMBER_LIMIT_USERTIMELINE_ENDPOINT
    sleep = {"short": int(call_time_limit*0.10), "medium": int(call_time_limit*0.30),
            "long": int(call_time_limit)}
    start_time = time.time()
    new_last = last
    print(strftime("%Y%m%d%H%M%S", gmtime()), " - [UserFeed] -- Collection of statuses for", screen, " starting from ")
    mongoError= " Raw query : " + query
    if type == UT_TYPE:
        mongoError = " Screen-name : " + screen
        new_statuses = pull_ut(api=api, screen=screen, collect_rts=collect_rts, exclude_replies=exclude_replies,
                               sleep=my_sleep, new_last=new_last, client=client)
    else:
        new_statuses = pull_sh(api=api, query=query, new_last=new_last, client=client)
    while new_statuses.__len__() > 1:
        for s in new_statuses:
            storeToMongo(client, s._json)

            #print (strftime("%Y%m%d%H%M%S ", gmtime()), " - [UserFeed] -- Error while inserting data in "
            #                                                         "mongo -- ",mongoError," to : ", last)
        new_last = new_statuses[new_statuses.__len__()-1].id + 1
        my_cpt = my_cpt + 1
        time.sleep(freq)
        if (time.time() - start_time) > call_time_limit & my_cpt > call_number_limit:
            return restart_pulling(api=api, screen=screen, last=new_last, client=client,
                                      my_sleep=sleep["long"], freq=10, collect_rts=collect_rts,
                                      exclude_replies=exclude_replies)
        if type == UT_TYPE:
            new_statuses = pull_ut(api=api, screen=screen, collect_rts=collect_rts, exclude_replies=exclude_replies,
                                   sleep=my_sleep,new_last=new_last, client=client)
        else:
            new_statuses = pull_sh(api=api, query=query, new_since=1, new_last=new_last, client=client)
    print(strftime("%Y%m%d%H%M%S", gmtime()), " - [UserFeed] -- End of collection of statuses for", screen,
          " starting last tweet", new_last)

def error_file():
    ok = 0

def UserTimeline(my_screen_name):
    """
        Method that get all twitter statuses for a given screen_name
        @type  api: number
        @param api: The slope of the line.
        @type  screen: text
        @param screen: The screen_name of user to target
        @type  since: number
        @param since: ID of the oldest tweet where the method starting to pull
        @type  last: number
        @param last: ID of the newest tweet where the method have to stop pulling
    """
    api =auth()
    statuses = api.GetUserTimeline(screen_name=my_screen_name, since_id=1, count=200)
    last_tweet = statuses[0].id
    since_tweet = statuses[statuses.__len__()-1].id
    my_client = "mongodb://localhost:27017"
    restart_pulling(api=api,type=UT_TYPE, screen=my_screen_name, last=last_tweet, client=my_client,
                       my_sleep=0, freq=0, exclude_replies=False, collect_rts=True)

def write_log(path, message):
    my_path=path
    #try:
    myFile = open(my_path, 'w')
    myFile.write(message)
    myFile.close()
    #except


def Search (query):
    api = auth()
    statuses = api.GetSearch(raw_query=query, count=100)
    for s in statuses:
        print(s._json)
    last_tweet = statuses[0].id
    since_tweet = statuses[statuses.__len__() - 1].id
    my_client = "mongodb://localhost:27017"
    restart_pulling(api=api, query=query, since=since_tweet, last=last_tweet, client=my_client, freq=0, mysleep=0)

# TODO: Query maker for search
#def query_marker():

def main ():
    #if ()
    UserTimeline("@artcanorg")
    #Search('q=trump%20macron')

#if __name__ == '__main__':
#    main(sys.argv)

main()
