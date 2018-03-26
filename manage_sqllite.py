# -*- coding: utf-8 -*-
"""
Fantasy Premier League 2017-2018 scraper

@author: Theodoros Panagiotakos
"""

import sqlite3

def connect(sqlite_file):
    ''' 
    Make connection to an SQLite database file 
    '''
    connection = sqlite3.connect(sqlite_file)
    c = connection.cursor()
    return connection, c

def close(connection):
    ''' 
    Commit changes and close connection to the database 
    '''
    # conn.commit()
    connection.close()

def drop_table(connection, table_name):
    try:
        with connection:
            connection.execute("DROP TABLE IF EXISTS {0}".format(table_name))
        print ("Table {0} dropped successfully".format(table_name))
    except sqlite3.Error as e:
        print ("Error info:", e.args[0]) 

def delete_table(connection, table_name):
    '''
    Deletes a table in our DB
    
    Input: connection = the connection with the DB
           table_name = the table to be emptied
    '''
    
    try:
        with connection:
            connection.execute("DELETE FROM {0}".format(table_name))
        print ("Table {0} deleted successfully".format(table_name))
    except sqlite3.Error as e:
        print ("Error info:", e.args[0]) 


def users_table(connection, entries, table_name):
    '''
    Creates and populates an sql3 table with the users in FPL league
    
    Input: connection = the connection object with the DB
           entries = a dictionary with the info about the users (key=entry_id, value=[player_name, total])
           table_name = the name of the corresponding DB table
    '''    
    try:
        with connection:
            connection.execute("CREATE TABLE IF NOT EXISTS {0} (entry INTEGER PRIMARY KEY, player_name TEXT, total INTEGER)".format(table_name))
        print ("Users' table creation step completed")    
        data = [(entry,entry_info[0],entry_info[1]) for entry, entry_info in entries.items()]
        with connection:
            connection.executemany("INSERT OR REPLACE INTO {0} (entry, player_name, total) VALUES (?, ?, ?)".format(table_name), data)
        print ("Users' table population step completed")
    except sqlite3.Error as e:
        print ("Error info:", e.args[0])    

def team_tokens_table(connection, entry_id, tokens, table_name):
    '''
    Creates and populates an sql3 table with tokens used by users in FPL league
    
    Input: connection = the connection object with the DB
           entry_id = the id of the user whose information were retrieved
           tokens = dictionary with the info about user's tokens (key=token_name, value=gameweek in which it was used)
           table_name = the name of the corresponding DB table
    '''
    try:
        with connection:
            connection.execute("CREATE TABLE IF NOT EXISTS {0} (entry_id INTEGER, gameweek INTEGER, token TEXT)".format(table_name))
        with connection:
            connection.execute("CREATE UNIQUE INDEX IF NOT EXISTS token_index ON {0} (entry_id , gameweek)".format(table_name))
        print("Tokens table creation step completed")
        data = [(entry_id, week, token) for chip_id, (token, week) in tokens.items()]
        with connection:
            connection.executemany("INSERT OR REPLACE INTO {0} (entry_id, gameweek, token) VALUES (?, ?, ?)".format(table_name), data)
        print("Insertion for user:{0} finished".format(entry_id))
    except sqlite3.Error as e:
        print ("Error info:", e.args[0]) 
        
def team_history_table(connection, entry_id, hist, table_name):
    '''
    Creates and populates an sql3 table with each team's performance history
    
    Input: connection = the connection object with the DB
           entry_id = the id of the user whose information were retrieve
           hist = dictionary with the info about user's history (key=gameweek, value=dictionary with keys [points, points_on_bench, total_points
                                                      gameweek_rank, overall_rank, week_transfers, week_transfers_cost, team_value, money_in_bank])
           table_name = the name of the table to store the data
    '''
    column_names = ["points", "points_on_bench", "total_points", "gameweek_rank", "overall_rank", "week_transfers", "week_transfers_cost", "team_value", "money_in_bank"]
    try:
        # create the table
        with connection:
            connection.execute("CREATE TABLE IF NOT EXISTS {0} (entry_id INTEGER, gameweek INTEGER, {1}, UNIQUE (entry_id, gameweek))".format(table_name, ", ".join(name+" INTEGER" for name in column_names)))
        print("Team history table creation step completed")
        # data preparation
        data = [(entry_id, gameweek, *[hist[gameweek][col] for col in column_names]) for gameweek in hist]
        with connection:
            connection.executemany("INSERT OR REPLACE INTO {0} (entry_id, gameweek, {1}) VALUES ({2})".format(table_name, ", ".join(name for name in column_names), ",".join(list("?"*(len(column_names)+2)))), data)
        print("Insertion of team history for user:{0} finished".format(entry_id))    
    except sqlite3.Error as e:
        print ("Error info:", e.args[0])
        
def transfer_history_table(connection, entry_id, transfers, table_name):
    '''
    Creates and populates an sql3 table with each team's transfer history
    
    Input: connection = the connection object with the DB
           entry_id = the id of the user whose information were retrieve
           transfer = dictionary with the info about user's transfer history (key=gameweek, value=list of dictionaries with keys [player_in, player_out, cost_in, cost_out, date])
           table_name = the name of the table to store the data
    '''
    column_names = [("player_in","INTEGER"), ("player_out","INTEGER"), ("cost_in","INTEGER"), ("cost_out","INTEGER"), ("date","TEXT")]
    try:
        # create the table
        with connection:
            connection.execute("CREATE TABLE IF NOT EXISTS {0} (entry_id INTEGER, gameweek INTEGER, {1}, UNIQUE (entry_id, gameweek, {2}))".format(table_name, ", ".join(name[0]+" "+ name[1] for name in column_names), ", ".join(name[0]+" " for name in column_names)))
        print("Transfer history table creation step completed")
        # data preparation
        data = [(entry_id, gameweek, *[transfer_line[col[0]] for col in column_names]) for gameweek, week_transf in transfers.items() for transfer_line in week_transf]
        with connection:
            connection.executemany("INSERT OR REPLACE INTO {0} (entry_id, gameweek, {1}) VALUES ({2})".format(table_name, ", ".join(name[0] for name in column_names), ",".join(list("?"*(len(column_names)+2)))), data)
        print("Insertion of team transfer history for user:{0} finished".format(entry_id))    
    except sqlite3.Error as e:
        print ("Error info:", e.args[0])
        
def gameweek_deadlines_table(connection, gameweek, deadline, table_name):
    '''
    Creates and populates an sql3 table with the deadlines of each gameweek in FPL league
    
    Input: connection = the connection object with the DB
           gameweek = the gameweek to record
           deadline =  the deadline for that gameweek           
           table_name = the name of the corresponding DB table
    '''    
    try:
        with connection:
            connection.execute("CREATE TABLE IF NOT EXISTS {0} (gameweek INTEGER PRIMARY KEY, deadline TEXT)".format(table_name))
        print("Deadline table creation step completed")
        data = [(gameweek, deadline)]
        with connection:
            connection.executemany("INSERT OR IGNORE INTO {0} (gameweek, deadline) VALUES (?, ?)".format(table_name), data)
        print("Deadline record step completed")
    except sqlite3.Error as e:
        print ("Error info:", e.args[0])          

def player_performance_table(connection, stats_dict, table_name):
    '''
    Creates and populates an sql3 table with the performance of each gameweek in FPL league
    
    Input: connection = the connection object with the DB
           stats_dict = the dictionary with the player performance
           table_name = the name of the corresponding DB table
    '''
    column_names = [("id","INTEGER"),("team_h_score","INTEGER"),("team_a_score","INTEGER"),("was_home","INTEGER"),("round","INTEGER"),("total_points","INTEGER"),
                   ("value","INTEGER"),("transfers_balance","INTEGER"),("selected","INTEGER"),("transfers_in","INTEGER"),("transfers_out","INTEGER"),("loaned_in","INTEGER"),
                   ("loaned_out","INTEGER"),("minutes","INTEGER"),("goals_scored","INTEGER"),("assists","INTEGER"),("clean_sheets","INTEGER"),("goals_conceded","INTEGER"),
                   ("own_goals","INTEGER"),("penalties_saved","INTEGER"),("penalties_missed","INTEGER"),("yellow_cards","INTEGER"),("red_cards","INTEGER"),("saves","INTEGER"),
                   ("bonus","INTEGER"),("bps","INTEGER"),("influence","REAL"),("creativity","REAL"),("threat","REAL"),("ict_index","REAL"),("ea_index","INTEGER"),("open_play_crosses","INTEGER"),
                   ("big_chances_created","INTEGER"),("clearances_blocks_interceptions","INTEGER"),("recoveries","INTEGER"),("key_passes","INTEGER"),("tackles","INTEGER"),
                   ("winning_goals","INTEGER"),("attempted_passes","INTEGER"),("completed_passes","INTEGER"),("penalties_conceded","INTEGER"),("big_chances_missed","INTEGER"),
                   ("errors_leading_to_goal","INTEGER"),("errors_leading_to_goal_attempt","INTEGER"),("tackled","INTEGER"),("offside","INTEGER"),("target_missed","INTEGER"),
                   ("fouls","INTEGER"),("dribbles","INTEGER"),("element","INTEGER"),("fixture","INTEGER"),("opponent_team","INTEGER")]        
    try:
        with connection:
            # we add in the primary key the ict_index to overcome potential double gameweek issues with the table. needs to be revised in case of issues
            connection.execute("CREATE TABLE IF NOT EXISTS {0} ({1}, UNIQUE(id) )".format(table_name, ", ".join(name[0]+" "+ name[1] for name in column_names)))
        print("Performance table creation step completed")
        data = [tuple(info[name[0]] for name in column_names) for info in stats_dict]
        with connection:
            connection.executemany("INSERT OR IGNORE INTO {0} ({1}) VALUES ({2})".format(table_name, ", ".join(name[0] for name in column_names), ",".join(list("?"*(len(column_names))))), data)
        print("Performance for player {0} added successfully".format(stats_dict[0]["element"]))
    except sqlite3.Error as e:
        print ("Error info:", e.args[0])      

def player_position_lookup_table(connection, player_positions, table_name):
    '''
    Creates and populates an sql3 table with lookup information regarding player positions
    
    Input: connection = the connection object with the DB
           player_positions = a list of dictionaries for the available position types
           table_name = the name of the table to store the data
    '''
    json_titles = sorted(player_positions[0].keys())
    column_types = ["INTEGER"] + ["TEXT" for i in range(len(json_titles) - 1)]
    column_names = list(zip(json_titles, column_types))
    try:
        # create the table
        with connection:
            connection.execute("CREATE TABLE IF NOT EXISTS {0} ({1}, UNIQUE (id))".format(table_name, ", ".join(name[0]+" "+ name[1] for name in column_names)))
        print("Player position lookup table creation step completed")
        # data preparation
        data = [tuple(info[title] for title in json_titles) for info in player_positions]
        with connection:
            connection.executemany("INSERT OR REPLACE INTO {0} ({1}) VALUES ({2})".format(table_name, ", ".join(name[0] for name in column_names), ",".join(list("?"*(len(column_names))))), data)
        print("Insertion of player positions finished")    
    except sqlite3.Error as e:
        print ("Error info:", e.args[0])

def teams_lookup_table(connection, teams, table_name):
    '''
    Creates and populates an sql3 table with lookup information regarding premier league teams
    
    Input: connection = the connection object with the DB
           teams = a list of dictionaries for the available teams
           table_name = the name of the table to store the data
    '''
    json_titles = ["id","name","short_name"]
    column_types = ["INTEGER"] + ["TEXT" for i in range(len(json_titles) - 1)]
    column_names = list(zip(json_titles, column_types))
    try:
        # create the table
        with connection:
            connection.execute("CREATE TABLE IF NOT EXISTS {0} ({1}, UNIQUE (id))".format(table_name, ", ".join(name[0]+" "+ name[1] for name in column_names)))
        print("Team lookup table creation step completed")
        # data preparation
        data = [tuple(info[title] for title in json_titles) for info in teams]
        with connection:
            connection.executemany("INSERT OR REPLACE INTO {0} ({1}) VALUES ({2})".format(table_name, ", ".join(name[0] for name in column_names), ",".join(list("?"*(len(column_names))))), data)
        print("Insertion of teams finished")    
    except sqlite3.Error as e:
        print ("Error info:", e.args[0])

def stats_lookup_table(connection, stats_lookup, table_name):
    '''
    Creates and populates an sql3 table with lookup information regarding stats descriptions
    
    Input: connection = the connection object with the DB
           stats_lookup = a list of dictionaries for the available stats
           table_name = the name of the table to store the data
    '''
    json_titles = ["stat_name","full_name"]
    column_types = ["TEXT" for i in range(len(json_titles))]
    column_names = list(zip(json_titles, column_types))
    try:
        # create the table
        with connection:
            connection.execute("CREATE TABLE IF NOT EXISTS {0} ({1}, UNIQUE (stat_name))".format(table_name, ", ".join(name[0]+" "+ name[1] for name in column_names)))
        print("Stats lookup table creation step completed")
        # data preparation
        data = [(info["key"],info["name"]) for info in stats_lookup]
        with connection:
            connection.executemany("INSERT OR REPLACE INTO {0} ({1}) VALUES ({2})".format(table_name, ", ".join(name[0] for name in column_names), ",".join(list("?"*(len(column_names))))), data)
        print("Insertion of stats finished")    
    except sqlite3.Error as e:
        print ("Error info:", e.args[0])

def player_lookup_table(connection, player_lookup, table_name):
    '''
    Creates and populates an sql3 table with lookup information regarding players
    
    Input: connection = the connection object with the DB
           player_lookup = a list of dictionaries for the available players
           table_name = the name of the table to store the data
    '''
    json_titles = ["id","element_type","web_name"]
    column_types = ["INTEGER","INTEGER","TEXT"]
    column_names = list(zip(json_titles, column_types))
    try:
        # create the table
        with connection:
            connection.execute("CREATE TABLE IF NOT EXISTS {0} ({1}, UNIQUE (id))".format(table_name, ", ".join(name[0]+" "+ name[1] for name in column_names)))
        print("Player lookup table creation step completed")
        # data preparation
        data = [tuple(info[title] for title in json_titles) for info in player_lookup]
        with connection:
            connection.executemany("INSERT OR REPLACE INTO {0} ({1}) VALUES ({2})".format(table_name, ", ".join(name[0] for name in column_names), ",".join(list("?"*(len(column_names))))), data)
        print("Insertion of players info finished")    
    except sqlite3.Error as e:
        print ("Error info:", e.args[0])

def user_gameweek_picks_table(connection, gameweek, entry_id, picks, table_name):
    '''
    Creates and populates an sql3 table with information about the gameweek picks for each user
    
    Input: connection = the connection object with the DB
           gameweek = the current game week
           entry_id = the user's fpl entry id
           picks = a list of dictionaries with the picks for gameweek
           table_name = the name of the table to store the data
    '''
    json_titles = sorted(picks[0].keys())
    column_types = ["INTEGER" for i in range(len(json_titles))]
    column_names = list(zip(json_titles, column_types))
    try:
        # create the table
        with connection:
            connection.execute("CREATE TABLE IF NOT EXISTS {0} (entry_id INTEGER, gameweek INTEGER, {1}, PRIMARY KEY(entry_id, gameweek, position))".format(table_name, ", ".join(name[0]+" "+ name[1] for name in column_names)))
        print("User picks table creation step completed")
        # data preparation
        data = [(entry_id, gameweek, *[info[title] for title in json_titles]) for info in picks]
        with connection:
            connection.executemany("INSERT OR REPLACE INTO {0} (entry_id, gameweek, {1}) VALUES ({2})".format(table_name, ", ".join(name[0] for name in column_names), ",".join(list("?"*(len(column_names) + 2)))), data)
        print("Insertion of picks for gameweek {0} for player {1} finished successfully".format(gameweek, entry_id))
    except sqlite3.Error as e:
        print ("Error info:", e.args[0])

def user_gameweek_auto_subs_table(connection, gameweek, entry_id, subs, table_name):
    '''
    Creates and populates an sql3 table with information about the gameweek automatic subs for each user
    
    Input: connection = the connection object with the DB
           gameweek = the current game week
           entry_id = the user's fpl entry id
           picks = a list of dictionaries with the auto subs that occured
           table_name = the name of the table to store the data
    '''
    json_titles = sorted(subs[0].keys())
    column_types = ["INTEGER" for i in range(len(json_titles))]
    column_names = list(zip(json_titles, column_types))
    try:
        # create the table
        with connection:
            connection.execute("CREATE TABLE IF NOT EXISTS {0} (entry_id INTEGER, gameweek INTEGER, {1}, UNIQUE(id))".format(table_name, ", ".join(name[0]+" "+ name[1] for name in column_names)))
        print("User auto subs table creation step completed")
        # data preparation
        data = [(entry_id, gameweek,*[info[title] for title in json_titles]) for info in subs]
        with connection:
            connection.executemany("INSERT OR IGNORE INTO {0} (entry_id, gameweek, {1}) VALUES ({2})".format(table_name, ", ".join(name[0] for name in column_names), ",".join(list("?"*(len(column_names) + 2)))), data)
        print("Insertion of auto subs for gameweek {0} for player {1} finished successfully".format(gameweek, entry_id))
    except sqlite3.Error as e:
        print ("Error info:", e.args[0])


def create_views(connection, file):
    '''
    Reads the file at Desktop-Games-FPL-fpl crawler and creates the corresponding Views
    '''
    try:
        with open(file, 'r') as f:
            qry = f.read().strip()
        with connection:
            connection.executescript(qry)
        print("Views created")
    except sqlite3.Error as e:
        print ("Error info:", e.args[0])    

#c.close()
#conn.close()