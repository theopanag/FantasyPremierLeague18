DROP VIEW IF EXISTS "UserTokens_V";
CREATE VIEW "UserTokens_V" AS SELECT "player_name","gameweek","token" FROM tokens INNER JOIN users ON tokens."entry_id" = users."entry";

DROP VIEW IF EXISTS "UserTeamHistory_V";
CREATE VIEW "UserTeamHistory_V" AS SELECT "player_name","gameweek","points","points_on_bench","total_points","gameweek_rank","overall_rank","week_transfers","week_transfers_cost","team_value"/10.0 as "team_value","money_in_bank"/10.0 as "money_in_bank" FROM userTeamHistory INNER JOIN users ON userTeamHistory."entry_id" = users."entry";

DROP VIEW IF EXISTS "UserTransferHistory_V";
CREATE VIEW "UserTransferHistory_V"
AS SELECT
b."player_name",
a."gameweek",
e.singular_name_short,
a."player_in",
c.web_name AS web_name_in,
a."player_out",
d.web_name AS web_name_out,
a."cost_in",
a."cost_out",
a."date" 
FROM userTransferHistory AS a
INNER JOIN users AS b
ON a."entry_id" = b."entry"
INNER JOIN playerInfo_LK AS c
ON a.player_in = c.id
INNER JOIN playerPosition_LK AS e
ON c.element_type = e.id
INNER JOIN playerInfo_LK AS d
ON a.player_out = d.id;

DROP VIEW IF EXISTS "UserGameweekPerformance_V";
CREATE VIEW "UserGameweekPerformance_V"  AS
SELECT
f.player_name
,a.gameweek
,a.element
,d.singular_name_short
,c.web_name
,a.is_captain
,a.is_vice_captain
,a.multiplier
,a.position
,CASE WHEN a.position > 11 THEN 1 ELSE 0 END AS is_sub
,b.team_h_score
,b.team_a_score
,b.was_home
,b.total_points
,b.value
,b.transfers_balance
,b.selected
,b.transfers_in
,b.transfers_out
,b.minutes
,b.goals_scored
,b.assists
,b.clean_sheets
,b.goals_conceded
,b.own_goals
,b.penalties_saved
,b.penalties_missed
,b.yellow_cards
,b.red_cards
,b.saves
,b.bonus
,b.bps
,b.influence
,b.creativity
,b.threat
,b.ict_index
,b.ea_index
,b.open_play_crosses
,b.big_chances_created
,b.clearances_blocks_interceptions
,b.recoveries
,b.key_passes
,b.tackles
,b.winning_goals
,b.attempted_passes
,b.completed_passes
,b.penalties_conceded
,b.big_chances_missed
,b.errors_leading_to_goal
,b.errors_leading_to_goal_attempt
,b.tackled
,b.offside
,b.target_missed
,b.fouls
,b.dribbles
,e.name AS opponent_team_name
FROM
gameweekPicks AS a
INNER JOIN
gameweekPerformance AS b
ON a.element = b.element
AND a.gameweek = b.round
INNER JOIN
playerInfo_LK AS c
ON b.element = c.id
INNER JOIN
playerPosition_LK AS d
ON c.element_type = d.id
INNER JOIN
premierTeams_LK AS e
ON b.opponent_team = e.id
INNER JOIN users AS f
ON a.entry_id = f.entry;