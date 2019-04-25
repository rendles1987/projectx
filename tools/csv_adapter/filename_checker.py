from tools.constants import COUNTRY_CUP_NAMES, COUNTRY_LEAGUE_NAMES


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

    @property
    def csv_file_name_with_extension(self):
        return self.csv_file_full_path.split("/")[-1]  # 'xx.csv'

    @property
    def csv_file_name_without_extension(self):
        return (self.csv_file_full_path.split("/")[-1]).split(".")[0]  # 'xx'

    @property
    def country(self):
        if self._country:
            return self._country
        self._country = self.csv_file_name_with_extension[0:3]
        self.__check_country()
        return self._country

    @property
    def game_name(self):
        if self._game_name:
            return self._game_name
        self._game_name = self.__get_game_name()
        return self._game_name

    @property
    def season(self):
        if self._season:
            return self._season
        self._season = self.__get_season()
        return self._season

    def __check_country(self):
        # check if 4th char csvfilename is underscore
        assert self.csv_file_name_with_extension[3] == "_", (
            "4th char of csv file",
            self.csv_file_name_with_extension,
            "name is not underscore",
        )
        # check if country in whitelist
        assert self.country in COUNTRY_LEAGUE_NAMES.keys(), (
            "country",
            self.country,
            "not in ",
            COUNTRY_LEAGUE_NAMES.keys(),
        )

    def __get_game_name(self):
        country = self.country
        expected_names = COUNTRY_LEAGUE_NAMES.get(country).values()
        for gamename in expected_names:
            if gamename in self.csv_file_name_without_extension:
                return gamename
        # if not found gamename
        raise Exception(
            "could not find gamename for " + self.csv_file_name_without_extension
        )

    def __get_season(self):
        """
        >>> str = "h3110 23 cat 444.4 rabbit 11 2 dog"
        >>> [int(s) for s in str.split() if s.isdigit()]
        [23, 11, 2]
        """
        numbers = [
            int(s)
            for s in self.csv_file_name_without_extension.split("_")
            if s.isdigit()
        ]
        season = min(numbers)
        assert 2000 <= season <= 2019
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
        # self._season = None

    @property
    def season(self):
        raise AssertionError("only league has season in filename")

    @property
    def country(self):
        if self._country:
            return self._country
        self._country = self.csv_file_name_with_extension[0:3]
        self.__check_country()
        return self._country

    def __check_country(self):
        # check if 4th char csvfilename is underscore
        assert self.csv_file_name_with_extension[3] == "_", (
            "4th char of csv file",
            self.csv_file_name_with_extension,
            "name is not underscore",
        )
        # check if country in whitelist
        assert self.country in COUNTRY_CUP_NAMES.keys(), (
            "country",
            self.country,
            "not in ",
            COUNTRY_CUP_NAMES.keys(),
        )

    @property
    def game_name(self):
        if self._game_name:
            return self._game_name
        self._game_name = self.__get_game_name()
        return self._game_name

    def __get_game_name(self):
        country = self.country
        expected_names = COUNTRY_CUP_NAMES.get(country).values()
        for gamename in expected_names:
            if gamename in self.csv_file_name_without_extension:
                return gamename
        # if not found gamename
        raise Exception(
            "could not find gamename for " + self.csv_file_name_without_extension
        )

    def check_all(self):
        self.country
        self.game_name


class PlayerFilenameChecker:
    pass
