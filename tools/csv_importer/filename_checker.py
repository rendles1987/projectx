from tools.constants import (
    CUP_COUNTRY_GAMENAMES_MAPPING,
    LEAGUE_COUNTRY_GAMENAMES_MAPPING,
)


class LeagueFilenameChecker:
    """ handles league csv. Retrieves three things
    - str: country (e.g. "ger"),
    - str: game_name (e.g. "eredivisie")
    - int: season (if playseason is "2008/2009" then int(2008))
    """

    def __init__(self, csv_file_full_path):
        self.csv_file_full_path = csv_file_full_path
        self._country = None
        self._game_name = None
        self._season = None
        self.countries = LEAGUE_COUNTRY_GAMENAMES_MAPPING.keys()
        self.country_gamename_mapping = LEAGUE_COUNTRY_GAMENAMES_MAPPING

    @property
    def csv_file_name_with_extension(self):
        return self.csv_file_full_path.split("/")[-1]  # 'xx.csv'

    @property
    def csv_file_name_without_extension(self):
        return (self.csv_file_full_path.split("/")[-1]).split(".")[0]  # 'xx'

    @property
    def csv_file_name_without_prefix(self):
        prefix = "all_games_"
        if self.csv_file_name_without_extension.startswith(prefix):
            csvfilename = self.csv_file_name_without_extension.split(prefix)[1]
        else:
            csvfilename = self.csv_file_name_without_extension
        return csvfilename

    @property
    def country(self):
        if self._country:
            return self._country
        csv_file_name_without_prefix = self.csv_file_name_without_prefix
        self._country = self._get_country(csv_file_name_without_prefix)
        return self._country

    @property
    def game_name(self):
        if self._game_name:
            return self._game_name
        self._game_name = self._get_game_name()
        return self._game_name

    @property
    def season(self):
        if self._season:
            return self._season
        self._season = self._get_season()
        return self._season

    def _get_country(self, csv_file_name_without_prefix):
        """ get it, but first two checks.
        by the way: filename prefix 'all_games_' is already deleted """
        # check if 4th char csvfilename is underscore
        assert csv_file_name_without_prefix[3] == "_", (
            csv_file_name_without_prefix + " 4th char of csv filename is not underscore"
        )

        country = csv_file_name_without_prefix[0:3]
        # check if country in whitelist
        assert country in self.countries, (
            self.csv_file_full_path
            + ": country "
            + country
            + " not in "
            + str(self.countries)
        )
        return country

    def _get_game_name(self):
        """ get it, but also check it """
        expected_names = self.country_gamename_mapping.get(self.country)
        # sort list by string length (longest first)
        expected_names.sort(key=len, reverse=True)
        for gamename in expected_names:
            if gamename in self.csv_file_name_without_extension:
                return gamename
        # if not found gamename
        raise Exception(self.csv_file_full_path + ": could not find gamename")

    def _get_season(self):
        """ get it, but also check it """
        """ >>> str = "h3110 23 cat 444.4 rabbit 11 2 dog"
            >>> [int(s) for s in str.split() if s.isdigit()]
            [23, 11, 2] """

        # first delete season digits from filename string
        csv_filename_without_gamename = self.csv_file_name_without_extension.replace(
            self.game_name, ""
        )
        numbers = [
            int(s) for s in csv_filename_without_gamename.split("_") if s.isdigit()
        ]

        # now check season digits
        season = min(numbers)
        assert 2000 <= season <= 2019, (
            self.csv_file_full_path + ": season " + str(season) + " not logic"
        )
        return season

    def check_all(self):
        self.country
        self.game_name
        self.season


class CupFilenameChecker(LeagueFilenameChecker):
    def __init__(self, csv_file_full_path):
        LeagueFilenameChecker.__init__(self, csv_file_full_path)
        self.csv_file_full_path = csv_file_full_path
        self._country = None
        self._game_name = None
        self.countries = CUP_COUNTRY_GAMENAMES_MAPPING.keys()
        self.country_gamename_mapping = CUP_COUNTRY_GAMENAMES_MAPPING

    @property
    def season(self):
        raise AssertionError("only league has season in filename")

    @property
    def country(self):
        if self._country:
            return self._country
        csv_file_name_without_prefix = self.csv_file_name_without_prefix
        self._country = self._get_country(csv_file_name_without_prefix)
        return self._country

    @property
    def game_name(self):
        if self._game_name:
            return self._game_name
        self._game_name = self._get_game_name()
        return self._game_name

    def check_all(self):
        self.country
        self.game_name


class PlayerFilenameChecker:
    pass
