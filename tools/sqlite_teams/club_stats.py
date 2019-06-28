import logging

from tools.constants import TABLE_NAME_ALL_TEAMS
from tools.utils import sqlite_table_to_df

log = logging.getLogger(__name__)

"""
# leauge
# "eng": {
#   1: "premier_league",
#   2: "championship",
#   3: "league_one",
#   4: "league_two"},

# cup
# "eng": {
#   1: "fa_cup",
#   2: "league_cup",
#   3: "community_shield",
"""


class ManageTeamStats:
    def __init__(self):
        self.teams = df_all_games = sqlite_table_to_df(table_name=TABLE_NAME_ALL_TEAMS)

    def run(self):
        # TODO: stopped here..
        for index, row in self.teams.iterrows():
            team_id = row["id"]
            team_name = row["team"]
            country = row["country"]

            team_stats = TeamStats(team_id, team_name, country)
            team_stats.run()

            if index == 10:
                break


class TeamStats:
    def __init__(self, team_id, team_name, country):
        self.team_id = team_id
        self.team_name = team_name
        self.country = country

    def get_competition_per_season(self):
        """
        :return: {
        2000: premier_league,
        2001: championship,
        2002: premier_league,
        } """
        print("hoi")

    def get_cups_per_season(self):
        """
        :return: {
        2000: ['fa_cup', 'league_cup', 'community_shield', 'champions_league_qual', etc
        2001: ['fa_cup', 'league_cup', 'community_shield', 'champions_league_qual', etc
        }
        """
        pass

    def get_score_per_season_per_cup(self):
        """
        :return:
        2000: { 'fa_cup': 50,
                'league_cup': 60,
                'community_shield': 10,
                'champions_league_qual: 100,
                'champions_league': 62},
        2000: { 'fa_cup': 50,
                'league_cup': 60,
                'community_shield': 10,
                'champions_league_qual: 100,
                'champions_league': 62,
                'playoff_championship': 100}
        """

    def run(self):
        self.get_competition_per_season()
