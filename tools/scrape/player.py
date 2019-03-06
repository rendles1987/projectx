

class PlayerScraper:
    def __init__(self, season):
        self.name = self.__str__()
        self.scraper_type = 'player'
        self.season = season

    def scrape(self):
        pass

    def save_to_csv(self):
        pass


Players2015 = PlayerScraper(2015)
Players2014 = PlayerScraper(2014)
