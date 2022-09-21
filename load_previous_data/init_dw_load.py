import os
import sys
import requests
from time import sleep
from dotenv import load_dotenv
from pymongo import MongoClient


if __name__ == "__main__":
    load_dotenv()
    DB_HOST = os.getenv('DB_HOST')
    DB_PNUM = int(os.getenv('DB_PNUM'))
    DB_USER = os.getenv('DB_USER')
    DB_PASS = os.getenv('DB_PASS')

    client = MongoClient("mongodb://"+DB_USER+":"+DB_PASS+"@"+DB_HOST, DB_PNUM)
    db_general = client["player_traditionalgeneral"]
    db_info = client["player_information"]


    # loading past 'traditional genaral' and 'information' data
    # 1996 - 2021 seasons
    headers = {
        'Connection': 'keep-alive',
        'Accept': 'application/json, text/plain, */*',
        'x-nba-stats-token': 'true',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36',
        'x-nba-stats-origin': 'stats',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-Mode': 'cors',
        'Referer': 'https://stats.nba.com/',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.9',
    }

    per_mode = 'Totals'
    season_list = [
        '1996-97',
        '1997-98',
        '1998-99',
        '1999-00',
        '2000-01',
        '2001-02',
        '2002-03',
        '2003-04',
        '2004-05',
        '2005-06',
        '2006-07',
        '2007-08',
        '2008-09',
        '2009-10',
        '2010-11',
        '2011-12',
        '2012-13',
        '2013-14',
        '2014-15',
        '2015-16',
        '2016-17',
        '2017-18',
        '2018-19',
        '2019-20',
        '2020-21',
        '2021-22'
    ]

    for season_id in season_list:
        url_general = 'https://stats.nba.com/stats/leaguedashplayerstats?College=&Conference=&Country=&DateFrom=&DateTo=&Division=&DraftPick=&DraftYear=&GameScope=&GameSegment=&Height=&LastNGames=0&LeagueID=00&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode='+per_mode+'&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season='+season_id+'&SeasonSegment=&SeasonType=Regular+Season&ShotClockRange=&StarterBench=&TeamID=0&TwoWay=0&VsConference=&VsDivision=&Weight='
        url_info = "https://stats.nba.com/stats/playerindex?College=&Country=&DraftPick=&DraftRound=&DraftYear=&Height=&Historical=1&LeagueID=00&Season="+season_id+"&SeasonType=Regular%20Season&TeamID=0&Weight="

        response_general = requests.get(url=url_general, headers=headers).json()
        response_info = requests.get(url=url_info, headers=headers).json()

        db_general.posts.insert_one(response_general)
        db_info.posts.insert_one(response_info)
        print(f".....year {season_id} completed.....")

    print("!!!#!!INSERTION  COMPLETED!!!#!!")

    sleep(10)
    sys.exit(0)
