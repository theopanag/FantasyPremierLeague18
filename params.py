# -*- coding: utf-8 -*-
"""
Fantasy Premier League 2017-2018 scraper

@author: Theodoros Panagiotakos
"""

##################### PARAMETERS ########################

# testing id for my team
# alefantos_id = 129099

##### Dictionary for different people leagues
# Change this to the league_id that you want to analyze
USER_LEAGUE = {"alefantos":[80757]} #"foufoulas":[348673]}

# Web links (leave them as is)
FPL_URL = "https://fantasy.premierleague.com/drf/"
USER_SUMMARY_SUBURL = "element-summary/"
LEAGUE_CLASSIC_STANDING_SUBURL = "leagues-classic-standings/"
LEAGUE_H2H_STANDING_SUBURL = "leagues-h2h-standings/"
TEAM_ENTRY_SUBURL = "entry/"
PLAYERS_INFO_SUBURL = "bootstrap-static"
PLAYERS_INFO_FILENAME = "allPlayersInfo.json"

USER_SUMMARY_URL = FPL_URL + USER_SUMMARY_SUBURL
PLAYERS_INFO_URL = FPL_URL + PLAYERS_INFO_SUBURL

# Dictionary with the position values. Used in getUserGameweekData()
ELEMENT_TYPE = {1:"Goalkeeper", 2:"Defender", 3:"Midfielder", 4:"Attaker"}