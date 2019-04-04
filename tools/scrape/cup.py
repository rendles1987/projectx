class CupScraper:
    def __init__(self, nation="", season=0):
        self.name = self.__str__()
        self.nation = nation
        self.scraper_type = "cup"
        self.season = season

    def check_season_in_class(self):
        if self.season not in self.name:
            raise AssertionError("season not in class name")

    def scrape(self):
        print("start scraping")

    def save_to_csv(self):
        print("export to csv")


NedCup2018 = CupScraper(nation="ned", season=2018)
NedCup2017 = CupScraper(nation="ned", season=2017)
GerCup2018 = CupScraper(nation="ger", season=2018)
