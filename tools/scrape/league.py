
class LeagueScraper:
    def __init__(self, nation='', start=0, end=1):
        self.name = self.__str__()
        self.nation = nation
        self.scraper_type = 'competition'
        self.start = start
        self.end = end

    def check_season_in_class(self):
        if self.season not in self.name:
            raise AssertionError('season not in class name')

    def scrape(self):
        print('start scraping')

    def save_to_csv(self):
        print('export to csv')


NedLeague = LeagueScraper(nation='ned', start=2018, end=2019)
GerLeague = LeagueScraper(nation='ger', start=2018, end=2019)
