# -*- coding: utf-8 -*-
"""
Fantasy Premier League 2017-2018 scraper

@author: Theodoros Panagiotakos
"""

#The link with the FPL json data
#https://fantasy.premierleague.com/drf/leagues-classic-standings/42407

import get_data as gd
import manage_sqllite as sq
import params

###### Values used for testing
# player_id = 355 # Id of Jermain Defoe
# alefantos_id = 129099

##### Dictionary for different people leagues
USER_LEAGUE = params.USER_LEAGUE


#PLAYER_HISTORY = "element-summary/{0}".format(player_id)

#FPL_LEAGUE_ID = 42407
START_PAGE = 1
leagueStandingUrl = gd.FPL_URL + gd.LEAGUE_CLASSIC_STANDING_SUBURL

rebuild_value = True

'''
# Database file
database = "fpl.db"
'''
# Views file
views_file = "DB_Views.sql"


# Table for users
user_table_name = "users"
# Table for tokens history
token_table_name = "tokens"
# Table for team's history
history_table_name = "userTeamHistory"
# Table for team's transfers
transfer_table_name = "userTransferHistory"
'''
# Table for gameweek deadlines
gameweek_deadline_table = "gameweekDeadline"
# Table for gameweek performance
gameweek_performance_table = "gameweekPerformance"
# Table for gameweek player picks
gameweek_picks_table = "gameweekPicks"
# Table for gameweek subs
gameweek_subs_table = "gameweekSubs"
'''
# List of performance tables
performance_table_list = ["gameweekDeadline", "gameweekPicks", "gameweekSubs", "gameweekPerformance"]
# List of lookup tables
lookup_table_list = ["playerPosition_LK","premierTeams_LK","statNames_LK","playerInfo_LK"]


def users_data(dbase, table_name, fpl_league_id, league_standing_url, start_page = 1, rebuild = False):
    try:
        player_ids = gd.getUserEntryIds(fpl_league_id, start_page, league_standing_url)
        connection, cursor = sq.connect(dbase)
        if rebuild:
            sq.delete_table(connection,table_name)
            sq.drop_table(connection, table_name)
        sq.users_table(connection, player_ids, table_name)
        sq.close(connection)
        print ("Users data loaded in table {0}".format(table_name))
    except BaseException as e:
        print ("Issue with loading user's data")
        print(str(e))

def user_history_data(dbase, user_table, tokens_table, history_table, rebuild = False):
    try:
        connection, cursor = sq.connect(dbase)
        cursor.execute("PRAGMA busy_timeout = 30000")
        with connection:
            user_list = connection.execute("SELECT DISTINCT entry FROM {0}".format(user_table)).fetchall()        
        if rebuild:
            sq.delete_table(connection,tokens_table)
            sq.drop_table(connection, tokens_table)
            sq.delete_table(connection,history_table)
            sq.drop_table(connection, history_table)   
        for user_id in user_list:
            tokens, hist = gd.getUserTeamHistory(user_id[0])                    
            print(user_id, tokens)
            if tokens:
                sq.team_tokens_table(connection, user_id[0], tokens, tokens_table)
            print("Tokens for user {0} successfully loaded".format(user_id[0]))
            if hist:
                sq.team_history_table(connection, user_id[0], hist, history_table)
            print("Team history for user {0} successfully loaded".format(user_id[0]))
        sq.close(connection)  
    except BaseException as e:
        print ("Issue with loading user:{0} tokens data".format(user_id))
        print(str(e))
        
def transfer_history_data(dbase, user_table, transfer_table, rebuild = False):
    try:
        connection, cursor = sq.connect(dbase)
        cursor.execute("PRAGMA busy_timeout = 30000")
        with connection:
            user_list = connection.execute("SELECT DISTINCT entry FROM {0}".format(user_table)).fetchall()
        if rebuild:
            sq.delete_table(connection,transfer_table)
            sq.drop_table(connection, transfer_table)
        for user_id in user_list:
            transfers = gd.getUserTransferHistory(user_id[0])                      
            if transfers:
                sq.transfer_history_table(connection, user_id[0], transfers, transfer_table)
            print("Transfer history for user {0} successfully loaded".format(user_id[0]))
        sq.close(connection)          
    except BaseException as e:
        print ("Issue with loading user:{0} transfer history".format(user_id[0]))
        print(str(e))

def build_lookup_tables(dbase, lookup_tables, rebuild = False):
    try:
        connection, cursor = sq.connect(dbase)
        cursor.execute("PRAGMA busy_timeout = 30000")
        if rebuild:
            for table in lookup_tables:
                sq.delete_table(connection,table)
                sq.drop_table(connection, table)
        gameweek, element_types, teams, stat_types = gd.getGameData() 
        sq.player_position_lookup_table(connection, element_types, lookup_tables[0])
        sq.teams_lookup_table(connection, teams, lookup_tables[1])
        sq.stats_lookup_table(connection, stat_types, lookup_tables[2])
        player_info = gd.getPlayerData()
        sq.player_lookup_table(connection, player_info, lookup_tables[3])
        sq.close(connection)
        print ("Lookup tables completed succesfully")
        return gameweek
    except BaseException as e:
        print ("Issue with creating lookup tables")                    
        print (str(e))
    
        
def gameweek_data(dbase, history_table, performance_table_list, player_lookup_table, rebuild = False):
    try:
        connection, cursor = sq.connect(dbase)
        cursor.execute("PRAGMA busy_timeout = 30000")
        with connection:
            user_weeks = connection.execute("SELECT entry_id, MAX(gameweek), MIN(gameweek) FROM {0} GROUP BY entry_id".format(history_table)).fetchall()
        print ("Userlist and corresponding gameweek range retrieval step completed")
        if rebuild:
            for table in performance_table_list:
                sq.delete_table(connection,table)
                sq.drop_table(connection, table)
        deadline_table, picks_table, subs_table, player_table = performance_table_list
        #sq.delete_table(connection,deadline_table)
        #sq.drop_table(connection, deadline_table)
        #sq.delete_table(connection,performance_table)
        #sq.drop_table(connection, performance_table)
        for user_id, max_week, min_week in user_weeks:
            for week in range(min_week,max_week+1):
                deadline, picks, subs = gd.getUserGameweekPicks(user_id , week)
                sq.gameweek_deadlines_table(connection, week, deadline, deadline_table)
                sq.user_gameweek_picks_table(connection, week, user_id, picks, picks_table)
                if len(subs) > 0:
                    sq.user_gameweek_auto_subs_table(connection, week, user_id, subs, subs_table)
                #sq.gameweek_performance_table(connection, week, user_id, week_perf, performance_table)
        
        with connection:
            max_player_id = connection.execute("SELECT MAX(id) FROM {0}".format(player_lookup_table)).fetchone()
        print("Max player id : {0}".format(str(max_player_id)))
        for i in range(1,int(max_player_id[0]+1)):
            stats = gd.getPlayerStats(i)
            if stats:
                sq.player_performance_table(connection, stats, player_table)
            
        sq.close(connection)
        print ("Gameweek user data completed succesfully")
    except BaseException as e:
        print ("Issue with loading gameweek user data for user:{0}".format(user_id))            
        print (e.args)

def views_creation(dbase, views_script):
    try:
        connection, cursor = sq.connect(dbase)
        sq.create_views(connection, views_script)
        sq.close(connection)
        print("Views created")
    except:
        print ("Issue with creating the views")          

if __name__ == "__main__":
    # the main process will be here
    
    '''
    Scrap the data from the website
    '''
    
    '''
    #gd.getPlayersInfo()    
    # Get the users
    player_ids = gd.getUserEntryIds(FPL_LEAGUE_ID, START_PAGE, leagueStandingUrl)
    # Get each team's history
    tokens, hist = gd.getUserTeamHistory(alefantos_id)
    # Get each team's transfers
    transfers = gd.getUserTransferHistory(alefantos_id)
    # Get each team's gameweek performance
    deadline, week_perf = gd.getUserGameweekData(alefantos_id , 1)   
    '''

    '''
    Get data and store them in DB tables
    '''
    
    for key in USER_LEAGUE:
        for fpl_league_id in USER_LEAGUE[key]:
            database = "fpl_{0}.db".format(fpl_league_id)
            # Create and populate the users table in DB
            users_data(database, user_table_name, fpl_league_id, leagueStandingUrl, rebuild_value)
            
            # Create and populate lookup tables
            current_gameweek = build_lookup_tables(database, lookup_table_list, rebuild_value)

            # Create and populate the table in DB with information about users tokens and team history
            user_history_data(database, user_table_name, token_table_name, history_table_name, rebuild_value)
            
            # Create and populate the table in DB with information about users trasfer history
            transfer_history_data(database, user_table_name, transfer_table_name, rebuild_value)
                    
            # Create and populate the table in DB witn information about users gameweek performance and deadlines
            gameweek_data(database, history_table_name, performance_table_list, lookup_table_list[3], rebuild_value )
            
            # Create the database views
            views_creation(database, views_file)
