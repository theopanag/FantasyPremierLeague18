# -*- coding: utf-8 -*-
"""
Fantasy Premier League 2017-2018 scraper

@author: Theodoros Panagiotakos
"""

#import fpl_info
import requests
import json
import params


FPL_URL = params.FPL_URL 
USER_SUMMARY_SUBURL = params.USER_SUMMARY_SUBURL
LEAGUE_CLASSIC_STANDING_SUBURL = params.LEAGUE_CLASSIC_STANDING_SUBURL
LEAGUE_H2H_STANDING_SUBURL = params.LEAGUE_H2H_STANDING_SUBURL
TEAM_ENTRY_SUBURL = params.TEAM_ENTRY_SUBURL
PLAYERS_INFO_SUBURL = params.PLAYERS_INFO_SUBURL
PLAYERS_INFO_FILENAME = params.PLAYERS_INFO_FILENAME

USER_SUMMARY_URL = FPL_URL + USER_SUMMARY_SUBURL
PLAYERS_INFO_URL = FPL_URL + PLAYERS_INFO_SUBURL

# Dictionary with the position values. Used in getUserGameweekData()
ELEMENT_TYPE = params.ELEMENT_TYPE #{1:"Goalkeeper", 2:"Defender", 3:"Midfielder", 4:"Attaker"}


# Download all player data: https://fantasy.premierleague.com/drf/bootstrap-static
def getPlayersInfo():
    ''' Creates a json file in local machine with all the data in the bootstrap_static page.'''
    r = requests.get(PLAYERS_INFO_URL)
    jsonResponse = r.json()
    with open(PLAYERS_INFO_FILENAME, 'w') as outfile:
        json.dump(jsonResponse, outfile)
        

# Get users in league: https://fantasy.premierleague.com/drf/leagues-classic-standings/336217?phase=1&le-page=1&ls-page=5
# In our case we try https://fantasy.premierleague.com/drf/leagues-classic-standings/42407
def getUserEntryIds(league_id, ls_page, league_Standing_Url):
    ''' 
    Returns a dictionary with key= player_entry_code and value=[player_name,total_score]
    
    Input: league_id = league id
           ls_page = standings page number
           league_Standing_Url = combined url showing classical or h2h league
    '''
    league_url = league_Standing_Url + str(league_id) + "?phase=1&le-page=1&ls-page=" + str(ls_page)
    try:
        r = requests.get(league_url)
        jsonResponse = r.json()
        standings = jsonResponse["standings"]["results"]
        # future thought to add the gameweek this league started scoring
        if not standings:
            print("no more standings found!")
            return None
    
        entries = {}
    
        for player in standings:
            entries[player["entry"]] = [player["entry_name"],player["total"]]
    
        return entries
    except BaseException as e:
        print("There was a problem with loading league_id:{0} for page:{1}".format(league_id, ls_page))
        print(str(e))
        return None

# team picked by user. example: https://fantasy.premierleague.com/drf/entry/2677936/event/1/picks
# with 2677936 being entry_id of the player
def getUserGameweekData(entry_id, GWNumber):
    '''
    Returns a dictionary "players{}" with keys = the players' element id and 
    value = several info regarding player's performance at that gameweek
    
    Input:  entry_id = team's entry id
            GWNumber = the gameweek to review
            
    Output: players{} = dictionary{element_id : info about player's performance that week}
            deadline = the transfer deadline for that gameweek

    '''
    eventSubUrl = "event/" + str(GWNumber)
    playerTeamUrlForSpecificGW = FPL_URL + TEAM_ENTRY_SUBURL + str(entry_id) + "/" + eventSubUrl
    try:
        r = requests.get(playerTeamUrlForSpecificGW)
        jsonResponse = r.json()
        
        # Get the gameweek deadline
        deadline = jsonResponse["fixtures"][0]["deadline_time_formatted"]
    
        # Get the team's gameweek players   
        players = {}
        picks = jsonResponse["picks"]        
        for pick in picks:
            selectionData = {}

            selectionData["position"] = ELEMENT_TYPE[int(pick["element_type"])] # The position out of th
            selectionData["points"] = pick["points"]
            selectionData["is_sub"] = pick["is_sub"]
            selectionData["is_captain"] = pick["is_captain"]
            selectionData["is_vice_captain"] = pick["is_vice_captain"]
            selectionData["multiplier"] = pick["multiplier"] # 2 if he's captain, 3 if triple captain chip played
            
            selectionData["stats"] = pick["stats"] # the stats of the gameweek
            
            # Add the player to the dictionary with element_id as key
            players[pick["element"]] = selectionData
    
        return deadline, players
    except BaseException as e:
        print("There was a problem with the JSON file for team:{0} and gameweek:{1} / link: {3}".format(
                entry_id,
                GWNumber,
                playerTeamUrlForSpecificGW))
        print(str(e))
        return None
    

def getUserTeamHistory(entry_id):
    '''  
    Input:  entry_id = team's entry id
    
    Output: tokens = a dictionary with the tokens played { key=token_name ,
                                                          value=gameweek_played}
            history = a dictionary with the team's history {key=gameweek ,
                     value= a dictionary with various info about 
                     (points, team_rank, transfers_made and team_money) }
    '''
    
    histTeamUrl = FPL_URL + TEAM_ENTRY_SUBURL + str(entry_id) + "/history"
    try:
        r = requests.get(histTeamUrl)
        jsonResponse = r.json()
        
        # Get when the chips where played
        tokens = {}
        chips = jsonResponse["chips"]
        for chip in chips:
            tokens[chip["chip"]] = (chip["name"], chip["event"])
            
        # Get the team's season history
        history = {}
        hist = jsonResponse["history"]
        for gameweek in hist:
            gameweekHist = {}
            gameweekHist["points"] = gameweek["points"]
            gameweekHist["points_on_bench"] = gameweek["points_on_bench"]
            gameweekHist["total_points"] = gameweek["total_points"]
            gameweekHist["gameweek_rank"] = gameweek["rank"]
            gameweekHist["overall_rank"] = gameweek["overall_rank"]
            gameweekHist["week_transfers"] = gameweek["event_transfers"]
            gameweekHist["week_transfers_cost"] = gameweek["event_transfers_cost"]
            gameweekHist["team_value"] = gameweek["value"]
            gameweekHist["money_in_bank"] = gameweek["bank"]
            
            # Add gameweek history to the history dictionary with key = gameweek_no            
            history[gameweek["event"]] = gameweekHist
        return tokens, history
        
    except BaseException as e:
        print("There was a problem with the JSON file for history of team:{0}".format(entry_id))
        print(str(e))
        return None        


def getUserTransferHistory(entry_id):
    '''  
    Input:  entry_id = team's entry id
    
    Output: transfers = a dictionary with the team's transfer history {key=gameweek ,
                        value= a dictionary with various info about 
                     (player_in, cost_in, player_out, cost_out and date) }
    '''
    histTeamUrl = FPL_URL + TEAM_ENTRY_SUBURL + str(entry_id) + "/transfers"
    try:
        r = requests.get(histTeamUrl)
        jsonResponse = r.json()
                  
        # Get the team's transfer history
        transfers = {}
        transf = jsonResponse["history"]
        for gameweek in transf:
            gameweekTransf = {}
            gameweekTransf["player_in"] = gameweek["element_in"]
            gameweekTransf["cost_in"] = gameweek["element_in_cost"]
            gameweekTransf["player_out"] = gameweek["element_out"]
            gameweekTransf["cost_out"] = gameweek["element_out_cost"]
            gameweekTransf["date"] = gameweek["time_formatted"]
            
            # Add gameweek history to the history dictionary with key = gameweek_no
            if gameweek["event"] in transfers:
                transfers[gameweek["event"]].append(gameweekTransf)
            else:
                transfers[gameweek["event"]] = [gameweekTransf]
        return transfers
        
    except BaseException as e:
        print("There was a problem with the transfer history of team:{0}".format(entry_id))
        print(str(e))
        return None

def getUserGameweekPicks(entry_id, GWNumber):
    '''
    Returns a dictionary "picks{}" with keys = the players' element id and
    value = several info regarding manager's selection at that gameweek
    
    Input:  entry_id = team's entry id
            GWNumber = the gameweek to review
            
    Output: deadline = the transfer deadline for that gameweek
            picks = list of dictionaries with gameweek picks for entry_id team
            subs = list of dictionaries with gameweek automatic substitutions            
    '''
    eventSubUrl = "event/" + str(GWNumber) + "/picks"
    playerTeamUrlForSpecificGW = FPL_URL + TEAM_ENTRY_SUBURL + str(entry_id) + "/" + eventSubUrl
    try:
        r = requests.get(playerTeamUrlForSpecificGW)
        jsonResponse = r.json()
        
        # Get the gameweek deadline
        deadline = jsonResponse["event"]["deadline_time_formatted"]
    
        # Get the team's gameweek players   
        picks = jsonResponse["picks"]

        # Get the team's gameweek automatic subs
        subs = jsonResponse["automatic_subs"]
        
        return deadline, picks, subs
    except BaseException as e:
        print("There was a problem with the JSON file for team:{0} and gameweek:{1} / link: {2}".format(
                entry_id, GWNumber,playerTeamUrlForSpecificGW))
        print(str(e))
        return None

def getPlayerStats(element_id):
    '''
    Returns a dictionary "player_stats" with keys = the players' element id and
    value = several info regarding player performance
    
    Input:  element_id = the player's id to collect the stats for
            
    Output: stats_dict =  a dictionary of format { stat_title : stat_value }
            where "element" is the key for the element_id
    '''
    eventSubUrl = "element-summary/" + str(element_id)
    url = FPL_URL + eventSubUrl
    try:
        r = requests.get(url)
        jsonResponse = r.json()
            
        # Get the gameweek stats for all players
        stats_dict = jsonResponse["history"]
        
        return stats_dict
    except BaseException as e:
        print("There was a problem with the JSON file for player:{0} / link: {1}".format(element_id,url))
        print(str(e))
        return None

def getGameData():
    '''
    Returns several information regarding the game like: lookup tables, player positions, etc
    
    Output: gameweek = the current gameweek
            element_types = a list of dictionaries for the available element types
            teams = a list of dictionaries for the teams
            stats_lookup = a list of dictionaries for the stats
    '''
    eventSubUrl = "bootstrap-static"
    url = FPL_URL + eventSubUrl
    try:
        r = requests.get(url)
        jsonResponse = r.json()
            
        # Get the gameweek stats for all players
        gameweek = jsonResponse["current-event"] # an integer specifying finished gameweek
        element_types = jsonResponse["element_types"] # a list of dictionaries
        teams = jsonResponse["teams"] # a list of dictionaries
        stats_lookup = jsonResponse["stats_options"] # a list of dictionaries
        
        return gameweek, element_types, teams, stats_lookup
    except BaseException as e:
        print("There was a problem with the JSON file for link: {0}".format(url))
        print(str(e))
        return None

def getPlayerData():
    '''
    Returns several information regarding the players like: player_name, position, etc
    
    Output: player_info = a dictionary with key the element_id and values a dictionary with 
            element_type(code for player_position) and web_name (the player's name)
    '''
    eventSubUrl = "elements"
    url = FPL_URL + eventSubUrl
    try:
        r = requests.get(url)
        jsonResponse = r.json()
            
        # Get the lookup info for all players
        info_to_get = ["id","element_type","web_name"]
        player_info = [{ attr:item[attr] for attr in info_to_get} for item in jsonResponse ]
        
        return player_info
    except BaseException as e:
        print("There was a problem with the JSON file for link: {0}".format(url))
        print(str(e))
        return None
