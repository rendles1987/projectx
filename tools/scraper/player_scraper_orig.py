#!/usr/bin/python3
# -*- coding: utf-8 -*-

from bs4 import BeautifulSoup
import requests
import pandas as pd
import re
import time
import os.path
from datetime import datetime


def namestr(obj, namespace):
    return [name for name in namespace if namespace[name] is obj]


def check_content_int(attribute, content):
    try:
        try_this = int(content)
        return content
    except Exception as e:
        # raise AssertionError(attribute + ' has no int' + e)
        return None


def get_01_01_yyyy(date_str):
    # typical date_str = Jan. 1 2014
    # in fifa12 the contract joining date is always 1 Jan.
    year = date_str[-4:]
    dd_mm_yyyy = "01/01/" + year
    return dd_mm_yyyy


def get_slashes_date(s_date):

    s_date = str(s_date)
    # Sept. is not recognized by datetime
    s_date = s_date.replace("Sept.", "Sep.")

    date_patterns = [
        "%B %d, %Y",  # <-- with comma
        "%b. %d, %Y",  # <-- with comma
        "%B %d %Y",
        "%b. %d %Y",
    ]

    # %b = maand afkorting (zonder punt!)   # Jan, Feb, …, Dec (Sept bestaat niet!!)
    # %B = maand uitgeschreven              # January, February, …, December
    # %d = zero-padded decimal number
    # %Y = year 4 digits
    for pattern in date_patterns:
        try:
            dd_mm_yyyy = datetime.strptime(s_date, pattern).date()
            date_slashes = dd_mm_yyyy.strftime("%d/%m/%Y")  # '24/06/1987'
            return date_slashes
        except Exception as e:
            pass
    raise AssertionError("Date %s is not in expected format" % s_date)


nr_secondes_sleep = 3

folder_path = (
    "/home/renier.kramer/Documents/werk/Machine_learning/soccer_predition/data/players/"
)
# csv_file = 'players_url_2018_22aug2017'
csv_file = "players_url_2017_25apr2016"
# csv_file = 'players_url_2016_21sep2015'
# csv_file = 'players_url_2015_29aug2014'
# csv_file = 'players_url_2014_30aug2013'
# csv_file = 'players_url_2013_31aug2012'
# csv_file = 'players_url_2012_1sep2011'
# csv_file = 'players_url_2011_1sep2010'
# csv_file = 'players_url_2010_1sep2009'
# csv_file = 'players_url_2009_1sep2008'
# csv_file = 'players_url_2008_25aug2007'
# csv_file = 'players_url_2007_28aug2006'
# csv_file = 'players_url_2006_24aug2005'
# csv_file = 'players_url_2005_08oct2004'

df = pd.read_csv(folder_path + csv_file + ".csv", sep="\t")
print("Read complete")
url_default = "https://www.fifaindex.com"

total_nr_players = 19000
window = 1000
index_1 = 10000
index_2 = index_1 + window

# xxx = sys.modules[__name__]
# for n in dir():
#     if n[0] != '_' and n != xxx:
#         print n
#         delattr(xxx, n)

while True:

    if csv_file in ["players_url_2017_25apr2016", "players_url_2018_22aug2017"]:
        Composure = []  # <-- vanaf 2017 en later

    Url = []
    Player_id = []
    Name = []
    Nationality = []
    Rating = []
    Height = []
    Weight = []
    Preffered_foot = []
    Birth_date = []
    Age = []
    Preffered_position = []
    Work_rate = []
    Weak_foot = []
    Skill_moves = []
    Teams = []
    Teams_id = []
    National_position = []
    National_kit = []
    Position = []
    Kit = []
    Joining = []
    Contract = []
    Ball_Control = []
    Dribbling = []
    Marking = []
    Sliding = []
    Standing = []
    Aggression = []
    Reaction = []
    Attack = []
    Interceptions = []
    Vision = []
    Crossing = []
    Short_pass = []
    Long_pass = []
    Acceleration = []
    Stamina = []
    Strength = []
    Balance = []
    Sprint = []
    Agility = []
    Jumping = []
    Heading = []
    Shot_power = []
    Finishing = []
    Long_shots = []
    Curve = []
    Freekick = []
    Penalties = []
    Volleys = []
    GK_Posi = []
    GK_Diving = []
    GK_Kick = []
    GK_Handling = []
    GK_Reflexes = []

    for index in range(index_1, index_2):
        part = "_" + str(index_1) + "_" + str(index_2)
        try:
            Name.append(df["Name"][index])
        except:
            index = index - 1
            print("no more players? " + df["Name"][index] + " is the last one?")
            break

        # messi (fifa 12 (1 sept 2011))
        # https://www.fifaindex.com/player/158023/lionel-messi/fifa12_8/

        url_temp = url_default + df["url"][index]

        Url.append(url_temp)
        clip_string = url_temp[33:40]
        index_first_slash = clip_string.find("/")
        Player_id.append(int(clip_string[:index_first_slash]))

        while True:
            # if i%10 == 0:

            print(
                "Getting page for "
                + df["Name"][index]
                + " index "
                + str(index)
                + " of total index "
                + str(index_2)
            )

            try:
                page = requests.get(url_temp)
                time.sleep(nr_secondes_sleep)
            except requests.exceptions.RequestException as e:  # This is the correct syntax
                print(e)
                continue
            break
        html = page.content
        soup = BeautifulSoup(html, "lxml")

        # 2 divs bovenin hebben beide class="col-sm-6"
        Both = soup.findAll("div", class_="col-sm-6")

        """
        linksboven div
        (met img, naam en nationaliteit) <-- deze div nodig voor nationaliteit
        """
        nationality_div = Both[0].find("a", class_="link-nation")
        nationality = nationality_div.find(text=True)
        if not nationality:
            nationality = nationality_div.get("title")
        if not nationality:
            raise AssertionError("no natiality available ??")
        Nationality.append(nationality)

        """
        rechtsboven div
        in de rechtsboven div zit een
        1) header (met daarin rating)
        2) onder header zit een card-body (met naam, height, weight, etc)
        """

        # 1) header (met daarin rating)
        header = Both[1].find("div", class_="card mb-5")

        # blijkbaar schuift dit alle kanten op. We hebben en current en potential.
        # Ik wil alleen current. Echter, soms pakt ie potential in r2 en current in r3
        # dus ik ga alles pakken en dan de laagste.
        # Aanname: potential is altijd hoger dan current
        r1_rating_span = header.findAll("span", class_="badge badge-dark rating r1")
        r2_rating_span = header.findAll("span", class_="badge badge-dark rating r2")
        r3_rating_span = header.findAll("span", class_="badge badge-dark rating r3")
        r4_rating_span = header.findAll("span", class_="badge badge-dark rating r4")
        r5_rating_span = header.findAll("span", class_="badge badge-dark rating r5")

        all_ratings = []
        for rating_span in [
            r1_rating_span,
            r2_rating_span,
            r3_rating_span,
            r4_rating_span,
            r5_rating_span,
        ]:
            try:
                rating = rating_span[0].find(text=True)
                all_ratings.append(rating)
            except:
                pass

        if not all_ratings:
            raise AssertionError("no rating found")
        else:
            Rating.append(min(all_ratings))

        # 2) onder header zit een card-body (met height, weight, etc)
        Card_body = Both[1].find("div", class_="card-body")
        Pars = Card_body.findAll("p")

        # het aantal elements in de Pars scheelt per fifa versie:
        #    - In e.g fifa12 zijn er 9 elementen,
        #    - In e.g fifa08 zijn er 7 elementen (geen Player Work Rate, Skill Moves)
        # daarom lopen we over de elementen in Pars

        for item in Pars:
            attribute = item.find(text=True)
            # e.g. u'Height '
            # e.g. u'Birth Date '
            content = item.find(text=False).find(text=True)
            # e.g. u'169 cm'
            # e.g. u'June 24, 1987'
            if "Height" in attribute:
                Height.append(content)
            elif "Weight" in attribute:
                Weight.append(content)
            elif "Preferred Foot" in attribute:
                Preffered_foot.append(content)
            elif "Birth Date" in attribute:
                date_slashes = get_slashes_date(content)
                Birth_date.append(date_slashes)
            elif "Age" in attribute:
                Age.append(content)
            elif "Preferred Positions" in attribute:
                # all_positions_without_space = content.text
                player_positions = []
                # there can be multiple elements! so findAll
                content = item.findAll(text=True)
                for pos in content[1:]:
                    player_positions.append(pos)
                    # ['CF', 'ST', 'RW']
                # only 1 element allowed ??
                all_pos = "/".join(player_positions)
                # 'CF/ST/RW'
                Preffered_position.append(all_pos)
            elif "Player Work Rate" in attribute:
                # select everything in between "> and </
                if csv_file != "players_url_2011_1sep2010":
                    Work_rate.append(content)
            elif "Weak Foot" in attribute:
                content = item.findAll("span", class_="star")
                # [<span class="star"><i class="fas fa-star fa-lg"></i><i class="fas fa-star fa-lg"></i><i class="fas fa-star fa-lg"></i><i class="far fa-star fa-lg"></i><i class="far fa-star fa-lg"></i></span>]
                stars = content[0].findAll("i", class_="fas fa-star fa-lg")
                Weak_foot.append(len(stars))
            elif "Skill Moves" in attribute:
                contents = item.findAll("span", class_="star")
                stars = contents[0].findAll("i", class_="fas fa-star fa-lg")
                Skill_moves.append(len(stars))

        """
        linksboven:
        Nationality['Argentina']  
        # ------------------
        rechtsboven:
        Url['https://www.fifaindex.com/player/158023/lionel-messi/fifa12_8/']
        Player_id[158023]
        Name['Lionel Messi']
        Rating[u'94']
        Height['169 cm']
        Weight['67 kg']
        Preffered_foot['Left']
        Birth_date['24/06/1987']
        Age['24']
        Preffered_position['CF', 'ST', 'RW']
        Work_rate['High / Medium']
        Weak_foot[3]
        Skill_moves[4]
        """

        """
        midden div
        bestaat uit 2 divs :
        - linksmidden (club info)
        - rechtsmidden (national team info)
        """
        Team_info = soup.findAll("div", class_="col-12 col-sm-6 col-lg-6 team")

        """
        Club_info (linkmidden div)
        (Team_info[0]) waarin teaminfo staat 
        (position, kit number, joined club, etc) <-- van de club !!
        """

        # in header zit 'Barcelona'

        # soms is club links en nation rechts, maar soms is club rechts en nation links...
        # enige verschil is dat 'Contract_lengt' alleen bij club zit (vanaf FIFA 2006 !!)
        # dus, zoeken naar Contract Length --> dat is club

        # scenario 1

        club_ele = None
        nation_ele = None
        nr_ele = len(Team_info)
        plays_in_clubteam = None
        plays_in_clubteam = None
        plays_in_free_agent = None

        if csv_file == "players_url_2005_08oct2004":
            # voor fifa2005 is het net ff KAK, want 'contract length' is niet
            # opgenomen in club div. Dus we kunnen niet zo handig onderscheid maken tussen club en nationaal voetbal.
            # daarom pragmatische oplossing:
            # als er 1 div is, neem ik aan dat dat clubvoetbal is.
            # als er 2 divs zijn, dan speelt speler in zowel club als nationaal
            # ik neem aan dat links club is
            # ik neem aan dat rechts nationaal is
            if nr_ele == 0:
                # https://www.fifaindex.com/player/193834/john-smith/fifa12_8/
                plays_in_free_agent = True
                plays_in_clubteam = False
                plays_in_national_team = False

            if nr_ele == 1:
                plays_in_national_team = False
                plays_in_free_agent = False
                plays_in_clubteam = True
                club_ele = 0

            if nr_ele == 2:
                plays_in_national_team = True
                plays_in_free_agent = False
                plays_in_clubteam = True
                club_ele = 0
                nation_ele = 1

        else:

            if nr_ele == 0:
                # https://www.fifaindex.com/player/193834/john-smith/fifa12_8/
                plays_in_free_agent = True
                plays_in_clubteam = False
                plays_in_national_team = False

            if nr_ele == 1:
                # club
                # or
                # nationaal elftal
                # uitvinden wat het is!
                # speler kan blijkbaar ook bij nationale team zitten en niet bij een club
                # zie https://www.fifaindex.com/player/157191/abdulkader-keita/fifa12_8/
                div_nr = 0
                Pars = Team_info[div_nr].findAll("p")
                for item in Pars:
                    attribute = item.find(text=True)
                    content = item.find(text=False).find(text=True)
                    if "Contract Length" in attribute:
                        plays_in_clubteam = True
                        club_ele = div_nr
                        plays_in_national_team = False
                        plays_in_free_agent = False
                if not plays_in_clubteam:
                    plays_in_national_team = True
                    plays_in_free_agent = False
                    nation_ele = div_nr

            elif nr_ele == 2:
                # club
                # or
                # nationaal elftal
                # uitvinden welke is welke?
                for div_nr in [0, 1]:
                    Pars = Team_info[div_nr].findAll("p")
                    for item in Pars:
                        attribute = item.find(text=True)
                        content = item.find(text=False).find(text=True)
                        if "Contract Length" in attribute:
                            if div_nr == 0:
                                club_ele = div_nr
                                nation_ele = div_nr + 1
                            elif div_nr == 1:
                                club_ele = div_nr
                                nation_ele = div_nr - 1
                            plays_in_clubteam = True
                            plays_in_national_team = True

        if not any([plays_in_clubteam, plays_in_national_team, plays_in_free_agent]):
            raise AssertionError("not free-agent, does not play at club and nation??")

        if not plays_in_clubteam or plays_in_free_agent:
            Teams_id.append(" ")
            Teams.append("Free agent")
            Position.append(" ")
            Kit.append(" ")
            if csv_file != "players_url_2011_1sep2010":
                Joining.append(" ")
            Contract.append(" ")

        if plays_in_clubteam:
            Club_info = Team_info[club_ele]
            header = Club_info.find("div", class_="card mb-5")
            club_link = header.find("a", href=re.compile(r"[/]([a-z]|[A-Z])\w+")).attrs[
                "href"
            ]
            # /team/241/fc-barcelona/fifa12_8/'

            href_split = club_link.split("/")
            # 2 types of href_split
            # ['', 'team', '101014', 'fifa09-31010140', 'fifa09_5', '']
            # ['', 'team', '1952', 'hull-city', 'fifa09_5', '']
            # 3rd element is team_id
            # 4th element is team_name
            team_id = href_split[2]
            team_name = href_split[3]

            if not team_id or not team_name:
                raise AssertionError("no team_id, team_name ?")

            Teams_id.append(team_id)
            Teams.append(team_name)
            # what to do with "Free agent" ??

            """
            nog steeds clubinfo (position, kit etc)
            """
            Pars = Club_info.findAll("p")
            for item in Pars:
                attribute = item.find(text=True)
                content = item.find(text=False).find(text=True)
                if "Position" in attribute:
                    player_positions = []
                    # there can be multiple elements! so findAll
                    content = item.findAll(text=True)
                    for pos in content[1:]:
                        player_positions.append(pos)
                        # ['CF', 'ST', 'RW']
                    for pos in player_positions:
                        Position.append(pos)
                elif "Kit Number" in attribute:
                    Kit.append(content)
                elif "Joined Club" in attribute:
                    date_slashes = get_slashes_date(content)
                    Joining.append(date_slashes)
                elif "Contract Length" in attribute:
                    Contract.append(content)

        if plays_in_national_team:
            National_team_info = Team_info[nation_ele]
            Pars = National_team_info.findAll("p")
            for item in Pars:
                attribute = item.find(text=True)
                content = item.find(text=False).find(text=True)
                if "Position" in attribute:
                    player_positions = []
                    # there can be multiple elements! so findAll
                    content = item.findAll(text=True)
                    for pos in content[1:]:
                        player_positions.append(pos)
                        # ['CF', 'ST', 'RW']
                    for pos in player_positions:
                        National_position.append(pos)
                elif "Kit Number" in attribute:
                    National_kit.append(content)

        if not plays_in_national_team:
            National_position.append(" ")
            National_kit.append(" ")

        """
        onderaan div bestaat uit vele divs, bevat oa:
        - 
        midden div
        - Ball Skills
        - Defence
        - Mental
        -Passing
        """

        specs = soup.findAll("div", class_="col-12 col-md-4 item")
        for i in specs:
            para = i.find_all("p")
            for para_ele in para:
                attribute = para_ele.find(text=True)
                all_text = (
                    para_ele.text
                )  # u'Ball Control 1 97' <-- messi has ball_control 97 and the 1
                # stands for +1 (edited that season?? anyway, shows +1 on website)
                # so we split the string (by space) and get the last element
                content = all_text.split(" ")[-1]
                if attribute == "Ball Control" or attribute == "Ball Control ":
                    content = check_content_int(attribute, content)
                    if content:
                        Ball_Control.append(content)
                elif attribute == "Dribbling" or attribute == "Dribbling ":
                    content = check_content_int(attribute, content)
                    if content:
                        Dribbling.append(content)
                elif attribute == "Marking" or attribute == "Marking ":
                    content = check_content_int(attribute, content)
                    if content:
                        Marking.append(content)
                elif attribute == "Slide Tackle" or attribute == "Slide Tackle ":
                    content = check_content_int(attribute, content)
                    if content:
                        Sliding.append(content)
                elif attribute == "Stand Tackle" or attribute == "Stand Tackle ":
                    content = check_content_int(attribute, content)
                    if content:
                        Standing.append(content)
                elif attribute == "Tackling" or attribute == "Tackling ":
                    content = check_content_int(attribute, content)
                    if content:
                        Standing.append(content)
                elif attribute == "Aggression" or attribute == "Aggression ":
                    content = check_content_int(attribute, content)
                    if content:
                        Aggression.append(content)
                elif attribute == "Reactions" or attribute == "Reactions ":
                    content = check_content_int(attribute, content)
                    if content:
                        Reaction.append(content)
                elif attribute == "Att. Position" or attribute == "Att. Position ":
                    content = check_content_int(attribute, content)
                    if content:
                        Attack.append(content)
                elif attribute == "Interceptions" or attribute == "Interceptions ":
                    content = check_content_int(attribute, content)
                    if content:
                        Interceptions.append(content)
                elif attribute == "Vision" or attribute == "Vision ":
                    content = check_content_int(attribute, content)
                    if content:
                        Vision.append(content)
                elif attribute == "Crossing" or attribute == "Crossing ":
                    content = check_content_int(attribute, content)
                    if content:
                        Crossing.append(content)
                elif attribute == "Short Pass" or attribute == "Short Pass ":
                    content = check_content_int(attribute, content)
                    if content:
                        Short_pass.append(content)
                elif attribute == "Long Pass" or attribute == "Long Pass ":
                    content = check_content_int(attribute, content)
                    if content:
                        Long_pass.append(content)
                elif attribute == "Acceleration" or attribute == "Acceleration ":
                    content = check_content_int(attribute, content)
                    if content:
                        Acceleration.append(content)
                elif attribute == "Sprint Speed" or attribute == "Sprint Speed ":
                    content = check_content_int(attribute, content)
                    if content:
                        Sprint.append(content)
                elif attribute == "Stamina" or attribute == "Stamina ":
                    content = check_content_int(attribute, content)
                    if content:
                        Stamina.append(content)
                elif attribute == "Strength" or attribute == "Strength ":
                    content = check_content_int(attribute, content)
                    if content:
                        Strength.append(content)
                elif attribute == "Balance" or attribute == "Balance ":
                    content = check_content_int(attribute, content)
                    if content:
                        Balance.append(content)
                elif attribute == "Agility" or attribute == "Agility ":
                    content = check_content_int(attribute, content)
                    if content:
                        Agility.append(content)
                elif attribute == "Jumping" or attribute == "Jumping ":
                    content = check_content_int(attribute, content)
                    if content:
                        Jumping.append(content)
                elif attribute == "Heading" or attribute == "Heading ":
                    content = check_content_int(attribute, content)
                    if content:
                        Heading.append(content)
                elif attribute == "Shot Power" or attribute == "Shot Power ":
                    content = check_content_int(attribute, content)
                    if content:
                        Shot_power.append(content)
                elif attribute == "Finishing" or attribute == "Finishing ":
                    content = check_content_int(attribute, content)
                    if content:
                        Finishing.append(content)
                elif attribute == "Shot Power" or attribute == "Shot Power ":
                    content = check_content_int(attribute, content)
                    if content:
                        Shot_power.append(content)
                elif attribute == "Long Shots" or attribute == "Long Shots ":
                    content = check_content_int(attribute, content)
                    if content:
                        Long_shots.append(content)
                elif attribute == "Curve" or attribute == "Curve ":
                    content = check_content_int(attribute, content)
                    if content:
                        Curve.append(content)
                elif attribute == "FK Acc." or attribute == "FK Acc. ":
                    content = check_content_int(attribute, content)
                    if content:
                        Freekick.append(content)
                elif attribute == "Penalties" or attribute == "Penalties ":
                    content = check_content_int(attribute, content)
                    if content:
                        Penalties.append(content)
                elif attribute == "Volleys" or attribute == "Volleys ":
                    content = check_content_int(attribute, content)
                    if content:
                        Volleys.append(content)
                elif attribute == "GK Positioning" or attribute == "GK Positioning ":
                    content = check_content_int(attribute, content)
                    if content:
                        GK_Posi.append(content)
                elif attribute == "GK Diving" or attribute == "GK Diving ":
                    content = check_content_int(attribute, content)
                    if content:
                        GK_Diving.append(content)

                elif (
                    attribute == "GK Handling"
                    or attribute == "GK Handling "
                    or attribute == "Handling"
                    or attribute == "Handling "
                ):
                    content = check_content_int(attribute, content)
                    if content:
                        GK_Handling.append(content)

                elif attribute == "GK Kicking" or attribute == "GK Kicking ":
                    content = check_content_int(attribute, content)
                    if content:
                        GK_Kick.append(content)

                elif (
                    attribute == "GK Reflexes"
                    or attribute == "GK Reflexes "
                    or attribute == "Reflexes"
                    or attribute == "Reflexes "
                ):
                    content = check_content_int(attribute, content)
                    if content:
                        GK_Reflexes.append(content)

                # in fifa06
                elif attribute == "Anticipation" or attribute == "Anticipation ":
                    content = check_content_int(attribute, content)
                    if content:
                        Reaction.append(content)

                # in fifa06
                elif attribute == "Passing" or attribute == "Passing ":
                    content = check_content_int(attribute, content)
                    if content:
                        Short_pass.append(content)

                # in fifa06
                elif attribute == "Passing" or attribute == "Passing ":
                    content = check_content_int(attribute, content)
                    if content:
                        Short_pass.append(content)

                # in fifa06
                elif attribute == "Long Balls" or attribute == "Long Balls ":
                    content = check_content_int(attribute, content)
                    if content:
                        Long_pass.append(content)

                # in fifa06
                elif attribute == "Pace" or attribute == "Pace ":
                    content = check_content_int(attribute, content)
                    if content:
                        Sprint.append(content)

                # in fifa17 and later
                if csv_file in [
                    "players_url_2017_25apr2016",
                    "players_url_2018_22aug2017",
                ]:
                    if attribute == "Composure" or attribute == "Composure ":
                        content = check_content_int(attribute, content)
                        if content:
                            Composure.append(content)

        if csv_file == "players_url_2011_1sep2010":
            # deze fifa versie heeft geen work_rate en joining
            nr = len(Skill_moves)
            Work_rate = []
            Joining = []
            temp = range(0, nr, 1)
            for i in temp:
                Work_rate.append("")
                Joining.append("")

        if csv_file == "players_url_2010_1sep2009":
            # deze fifa versie heeft geen work_rate en joining, skill_moves, Sliding,
            nr = len(Weak_foot)
            Work_rate = []
            Joining = []
            Skill_moves = []
            Sliding = []
            # Standing = [] deze vullen we met 'Tackling'
            Attack = []
            Interceptions = []
            Vision = []
            Balance = []
            Agility = []
            Jumping = []
            Curve = []
            Penalties = []
            Volleys = []
            GK_Kick = []

            temp = range(0, nr, 1)
            for i in temp:
                Work_rate.append("")
                Joining.append("")
                Skill_moves.append("")
                Sliding.append("")
                # Standing.append('')
                Attack.append("")  # Att. Position
                Interceptions.append("")
                Vision.append("")
                Balance.append("")
                Agility.append("")
                Jumping.append("")
                Curve.append("")
                Penalties.append("")
                Volleys.append("")
                GK_Kick.append("")
                # GK_Handling wordt op de fifaindex site nu 'Handling' genoemd. Ik append nog steeds naar GK_Handling
                # GK_Reflexes wordt op de fifaindex site nu 'Reflexes' genoemd. Ik append nog steeds naar GK_Reflexes

        if csv_file == "players_url_2009_1sep2008":
            # deze fifa versie heeft geen work_rate en joining, skill_moves, Sliding,
            nr = len(Weak_foot)
            Work_rate = []
            Joining = []
            Skill_moves = []
            Sliding = []
            # Standing = [] :deze vullen we met 'Tackling'
            Attack = []
            Interceptions = []
            Vision = []
            Balance = []
            Agility = []
            Jumping = []
            Curve = []
            Penalties = []
            Volleys = []
            GK_Kick = []

            temp = range(0, nr, 1)
            for i in temp:
                Work_rate.append("")
                Joining.append("")
                Skill_moves.append("")
                Sliding.append("")
                # Standing.append('')
                Attack.append("")  # Att. Position
                Interceptions.append("")
                Vision.append("")
                Balance.append("")
                Agility.append("")
                Jumping.append("")
                Curve.append("")
                Penalties.append("")
                Volleys.append("")
                GK_Kick.append("")
                # GK_Handling wordt op de fifaindex site nu 'Handling' genoemd. Ik append nog steeds naar GK_Handling
                # GK_Reflexes wordt op de fifaindex site nu 'Reflexes' genoemd. Ik append nog steeds naar GK_Reflexes

        if csv_file == "players_url_2008_25aug2007":
            # deze fifa versie heeft geen work_rate en joining, skill_moves, Sliding,
            nr = len(Weak_foot)
            Work_rate = []
            Joining = []
            Skill_moves = []
            Sliding = []
            # Standing = [] :deze vullen we met 'Tackling'
            Attack = []
            Interceptions = []
            Vision = []
            Balance = []
            Agility = []
            Jumping = []
            Curve = []
            Penalties = []
            Volleys = []
            GK_Kick = []

            temp = range(0, nr, 1)
            for i in temp:
                Work_rate.append("")
                Joining.append("")
                Skill_moves.append("")
                Sliding.append("")
                # Standing.append('')
                Attack.append("")  # Att. Position
                Interceptions.append("")
                Vision.append("")
                Balance.append("")
                Agility.append("")
                Jumping.append("")
                Curve.append("")
                Penalties.append("")
                Volleys.append("")
                GK_Kick.append("")
                # GK_Handling wordt op de fifaindex site nu 'Handling' genoemd. Ik append nog steeds naar GK_Handling
                # GK_Reflexes wordt op de fifaindex site nu 'Reflexes' genoemd. Ik append nog steeds naar GK_Reflexes

        if csv_file == "players_url_2007_28aug2006":
            # deze fifa versie heeft geen work_rate en joining, skill_moves, Sliding,
            nr = len(Weak_foot)
            Work_rate = []
            Joining = []
            Skill_moves = []
            Sliding = []
            # Standing = []
            Attack = []
            Interceptions = []
            Vision = []
            Balance = []
            Agility = []
            Jumping = []
            Curve = []
            Penalties = []
            Volleys = []
            GK_Kick = []

            temp = range(0, nr, 1)
            for i in temp:
                Work_rate.append("")
                Joining.append("")
                Skill_moves.append("")
                Sliding.append("")
                # Standing.append('') :deze vullen we met 'Tackling'
                Attack.append("")  # Att. Position
                Interceptions.append("")
                Vision.append("")
                Balance.append("")
                Agility.append("")
                Jumping.append("")
                Curve.append("")
                Penalties.append("")
                Volleys.append("")
                GK_Kick.append("")
                # GK_Handling wordt op de fifaindex site nu 'Handling' genoemd. Ik append nog steeds naar GK_Handling
                # GK_Reflexes wordt op de fifaindex site nu 'Reflexes' genoemd. Ik append nog steeds naar GK_Reflexes

        if csv_file == "players_url_2006_24aug2005":
            # deze fifa versie heeft geen work_rate en joining, skill_moves, Sliding,
            nr = len(Crossing)
            Weak_foot = []
            Work_rate = []
            Joining = []
            Skill_moves = []
            Sliding = []
            # Standing = [] :deze vullen we met 'Tackling'
            Attack = []
            Interceptions = []
            Vision = []
            Balance = []
            Agility = []
            Jumping = []
            Curve = []
            Penalties = []
            Volleys = []
            GK_Kick = []
            # Reaction :deze vullen we met 'Anticipation'
            # Short_pass :deze vullen we met 'Passing'
            # Long_pass :deze vullen we met 'Long Balls'
            Finishing = []
            Freekick = []
            GK_Diving = []

            temp = range(0, nr, 1)
            for i in temp:
                Weak_foot.append("")
                Work_rate.append("")
                Joining.append("")
                Skill_moves.append("")
                Sliding.append("")
                # Standing.append('')
                Attack.append("")  # Att. Position
                Interceptions.append("")
                Vision.append("")
                Balance.append("")
                Agility.append("")
                Jumping.append("")
                Curve.append("")
                Penalties.append("")
                Volleys.append("")
                GK_Kick.append("")
                # GK_Handling wordt op de fifaindex site nu 'Handling' genoemd. Ik append nog steeds naar GK_Handling
                # GK_Reflexes wordt op de fifaindex site nu 'Reflexes' genoemd. Ik append nog steeds naar GK_Reflexes
                Finishing.append("")
                Freekick.append("")
                GK_Diving.append("")

        if csv_file == "players_url_2005_08oct2004":
            # deze fifa versie heeft geen work_rate en joining, skill_moves, Sliding,
            nr = len(Crossing)
            Weak_foot = []
            Work_rate = []
            Joining = []
            Skill_moves = []
            Sliding = []
            # Standing = [] :deze vullen we met 'Tackling'
            Attack = []
            Interceptions = []
            Vision = []
            Balance = []
            Agility = []
            Jumping = []
            Curve = []
            Penalties = []
            Volleys = []
            GK_Kick = []
            # Reaction :deze vullen we met 'Anticipation'
            # Short_pass :deze vullen we met 'Passing'
            # Long_pass :deze vullen we met 'Long Balls'
            Finishing = []
            Freekick = []
            GK_Diving = []
            Contract = []  # dit zit niet in 2005, maar wel in 2006 en hoger
            Long_shots = []  # dit zit niet in 2005, maar wel in 2006 en hoger

            temp = range(0, nr, 1)
            for i in temp:
                Weak_foot.append("")
                Work_rate.append("")
                Joining.append("")
                Skill_moves.append("")
                Sliding.append("")
                # Standing.append('')
                Attack.append("")  # Att. Position
                Interceptions.append("")
                Vision.append("")
                Balance.append("")
                Agility.append("")
                Jumping.append("")
                Curve.append("")
                Penalties.append("")
                Volleys.append("")
                GK_Kick.append("")
                # GK_Handling wordt op de fifaindex site nu 'Handling' genoemd. Ik append nog steeds naar GK_Handling
                # GK_Reflexes wordt op de fifaindex site nu 'Reflexes' genoemd. Ik append nog steeds naar GK_Reflexes
                Finishing.append("")
                Freekick.append("")
                GK_Diving.append("")
                Contract.append("")  # dit zit niet in 2005, maar wel in 2006 en hoger
                Long_shots.append("")  # dit zit niet in 2005, maar wel in 2006 en hoger

        check_list = [
            Url,  # 1
            Player_id,  # 2
            Name,  # 3
            Nationality,  # 4
            Rating,  # 5
            Height,  # 6
            Weight,  # 7
            Preffered_foot,  # 8
            Birth_date,  # 9
            Age,  # 10
            Preffered_position,  #
            Work_rate,  # 12
            Weak_foot,  #
            Skill_moves,  # 14
            Teams,  #
            National_position,  # 16
            National_kit,  #
            Position,  # 18
            Kit,  #
            Joining,  # 20
            Contract,  #
            Ball_Control,  # 22
            Dribbling,  #
            Marking,  # 24
            Sliding,  #
            Standing,  # 26
            Aggression,  #
            Reaction,  # 28
            Attack,  #
            Interceptions,  # 30
            Vision,  #
            Crossing,  # 32
            Short_pass,  #
            Long_pass,  # 34
            Acceleration,  #
            Stamina,  # 36
            Strength,  #
            Balance,  # 38
            Sprint,  #
            Agility,  # 40
            Jumping,  #
            Heading,  # 42
            Shot_power,  #
            Finishing,  # 44
            Long_shots,  #
            Curve,  # 46
            Freekick,  #
            Penalties,  # 48
            Volleys,  #
            GK_Posi,  # 50
            GK_Diving,  #
            GK_Kick,  # 52
            GK_Handling,  #
            GK_Reflexes,  # 54
            Teams_id,  # 55
        ]
        if csv_file in ["players_url_2017_25apr2016", "players_url_2018_22aug2017"]:
            check_list.append(Composure)  # 56

        lengte = None
        for idx, i in enumerate(check_list):
            if not lengte:
                lengte = len(i)
            else:
                if not len(i) == lengte:
                    msg = "%d, %s, %s " % (
                        idx + 1,
                        i,
                        namestr(check_list[idx], globals()),
                    )
                    raise AssertionError("not same length = %s" % msg)

    if csv_file in ["players_url_2017_25apr2016", "players_url_2018_22aug2017"]:
        try:
            df_result = pd.DataFrame(
                {
                    "Url": Url,
                    "Player_id": Player_id,
                    "Name": Name,
                    "Nationality": Nationality,
                    "National_Position": National_position,
                    "National_Kit": National_kit,
                    "Club": Teams,
                    "Club_id": Teams_id,
                    "Club_Position": Position,
                    "Club_Kit": Kit,
                    "Club_Joining": Joining,
                    "Contract_Expiry": Contract,
                    "Rating": Rating,
                    "Height": Height,
                    "Weight": Weight,
                    "Preffered_Foot": Preffered_foot,
                    "Birth_Date": Birth_date,
                    "Age": Age,
                    "Preffered_Position": Preffered_position,
                    "Work_Rate": Work_rate,
                    "Weak_foot": Weak_foot,
                    "Skill_Moves": Skill_moves,
                    "Ball_Control": Ball_Control,
                    "Dribbling": Dribbling,
                    "Marking": Marking,
                    "Sliding_Tackle": Sliding,
                    "Standing_Tackle": Standing,
                    "Aggression": Aggression,
                    "Reactions": Reaction,
                    "Attacking_Position": Attack,
                    "Interceptions": Interceptions,
                    "Vision": Vision,
                    "Composure": Composure,
                    "Crossing": Crossing,
                    "Short_Pass": Short_pass,
                    "Long_Pass": Long_pass,
                    "Acceleration": Acceleration,
                    "Speed": Sprint,
                    "Stamina": Stamina,
                    "Strength": Strength,
                    "Balance": Balance,
                    "Agility": Agility,
                    "Jumping": Jumping,
                    "Heading": Heading,
                    "Shot_Power": Shot_power,
                    "Finishing": Finishing,
                    "Long_Shots": Long_shots,
                    "Curve": Curve,
                    "Freekick_Accuracy": Freekick,
                    "Penalties": Penalties,
                    "Volleys": Volleys,
                    "GK_Positioning": GK_Posi,
                    "GK_Diving": GK_Diving,
                    "GK_Kicking": GK_Kick,
                    "GK_Handling": GK_Handling,
                    "GK_Reflexes": GK_Reflexes,
                }
            )

            cols = [
                "Url",
                "Player_id",
                "Name",
                "Nationality",
                "National_Position",
                "National_Kit",
                "Club",
                "Club_id",
                "Club_Position",
                "Club_Kit",
                "Club_Joining",
                "Contract_Expiry",
                "Rating",
                "Height",
                "Weight",
                "Preffered_Foot",
                "Birth_Date",
                "Age",
                "Preffered_Position",
                "Work_Rate",
                "Weak_foot",
                "Skill_Moves",
                "Ball_Control",
                "Dribbling",
                "Marking",
                "Sliding_Tackle",
                "Standing_Tackle",
                "Aggression",
                "Reactions",
                "Attacking_Position",
                "Interceptions",
                "Vision",
                "Composure",
                "Crossing",
                "Short_Pass",
                "Long_Pass",
                "Acceleration",
                "Speed",
                "Stamina",
                "Strength",
                "Balance",
                "Agility",
                "Jumping",
                "Heading",
                "Shot_Power",
                "Finishing",
                "Long_Shots",
                "Curve",
                "Freekick_Accuracy",
                "Penalties",
                "Volleys",
                "GK_Positioning",
                "GK_Diving",
                "GK_Kicking",
                "GK_Handling",
                "GK_Reflexes",
            ]

            df_result = df_result[cols]

        except Exception as e:
            raise AssertionError("could not fill df_result 1")

    else:
        try:
            df_result = pd.DataFrame(
                {
                    "Url": Url,
                    "Player_id": Player_id,
                    "Name": Name,
                    "Nationality": Nationality,
                    "National_Position": National_position,
                    "National_Kit": National_kit,
                    "Club": Teams,
                    "Club_id": Teams_id,
                    "Club_Position": Position,
                    "Club_Kit": Kit,
                    "Club_Joining": Joining,
                    "Contract_Expiry": Contract,
                    "Rating": Rating,
                    "Height": Height,
                    "Weight": Weight,
                    "Preffered_Foot": Preffered_foot,
                    "Birth_Date": Birth_date,
                    "Age": Age,
                    "Preffered_Position": Preffered_position,
                    "Work_Rate": Work_rate,
                    "Weak_foot": Weak_foot,
                    "Skill_Moves": Skill_moves,
                    "Ball_Control": Ball_Control,
                    "Dribbling": Dribbling,
                    "Marking": Marking,
                    "Sliding_Tackle": Sliding,
                    "Standing_Tackle": Standing,
                    "Aggression": Aggression,
                    "Reactions": Reaction,
                    "Attacking_Position": Attack,
                    "Interceptions": Interceptions,
                    "Vision": Vision,  # 'Composure': Composure,
                    "Crossing": Crossing,
                    "Short_Pass": Short_pass,
                    "Long_Pass": Long_pass,
                    "Acceleration": Acceleration,
                    "Speed": Sprint,
                    "Stamina": Stamina,
                    "Strength": Strength,
                    "Balance": Balance,
                    "Agility": Agility,
                    "Jumping": Jumping,
                    "Heading": Heading,
                    "Shot_Power": Shot_power,
                    "Finishing": Finishing,
                    "Long_Shots": Long_shots,
                    "Curve": Curve,
                    "Freekick_Accuracy": Freekick,
                    "Penalties": Penalties,
                    "Volleys": Volleys,
                    "GK_Positioning": GK_Posi,
                    "GK_Diving": GK_Diving,
                    "GK_Kicking": GK_Kick,
                    "GK_Handling": GK_Handling,
                    "GK_Reflexes": GK_Reflexes,
                }
            )

            cols = [
                "Url",
                "Player_id",
                "Name",
                "Nationality",
                "National_Position",
                "National_Kit",
                "Club",
                "Club_id",
                "Club_Position",
                "Club_Kit",
                "Club_Joining",
                "Contract_Expiry",
                "Rating",
                "Height",
                "Weight",
                "Preffered_Foot",
                "Birth_Date",
                "Age",
                "Preffered_Position",
                "Work_Rate",
                "Weak_foot",
                "Skill_Moves",
                "Ball_Control",
                "Dribbling",
                "Marking",
                "Sliding_Tackle",
                "Standing_Tackle",
                "Aggression",
                "Reactions",
                "Attacking_Position",
                "Interceptions",
                "Vision",  # 'Composure',
                "Crossing",
                "Short_Pass",
                "Long_Pass",
                "Acceleration",
                "Speed",
                "Stamina",
                "Strength",
                "Balance",
                "Agility",
                "Jumping",
                "Heading",
                "Shot_Power",
                "Finishing",
                "Long_Shots",
                "Curve",
                "Freekick_Accuracy",
                "Penalties",
                "Volleys",
                "GK_Positioning",
                "GK_Diving",
                "GK_Kicking",
                "GK_Handling",
                "GK_Reflexes",
            ]

            df_result = df_result[cols]

        except Exception as e:
            raise AssertionError("could not fill df_result 2")

    # write dataframe to a csv
    # csv_file_name en folder_path staan helemaal bovenaan
    path = folder_path + csv_file + part + ".csv"
    # check if file already exists
    if os.path.isfile(path) == False:
        df_result.to_csv(path, sep="\t", encoding="utf-8")
        print("export to csv file " + csv_file + part)
    else:
        print("csv file " + csv_file + part + " already exists")

    index_1 = index_1 + window
    index_2 = index_2 + window
    df_result = None

    # this = sys.modules[__name__]
    # for n in dir():
    #     print n
    #     if n[0] != '_' or n not in ['this', 'nr_secondes_sleep', 'check_list', 'folder_path', 'csv_file', 'df',
    #         'url_default', 'total_nr_players', 'window', 'index_1', 'index_2', 'df_result']:
    #         delattr(this, n)

    Url = []
    Player_id = []
    Name = []
    Nationality = []
    Rating = []
    Height = []
    Weight = []
    Preffered_foot = []
    Birth_date = []
    Age = []
    Preffered_position = []
    Work_rate = []
    Weak_foot = []
    Skill_moves = []
    Teams = []
    Teams_id = []
    National_position = []
    National_kit = []
    Position = []
    Kit = []
    Joining = []
    Contract = []
    Ball_Control = []
    Dribbling = []
    Marking = []
    Sliding = []
    Standing = []
    Aggression = []
    Reaction = []
    Attack = []
    Interceptions = []
    Vision = []
    Crossing = []
    Short_pass = []
    Long_pass = []
    Acceleration = []
    Stamina = []
    Strength = []
    Balance = []
    Sprint = []
    Agility = []
    Jumping = []
    Heading = []
    Shot_power = []
    Finishing = []
    Long_shots = []
    Curve = []
    Freekick = []
    Composure = []
    Penalties = []
    Volleys = []
    GK_Posi = []
    GK_Diving = []
    GK_Kick = []
    GK_Handling = []
    GK_Reflexes = []

    if index_1 >= total_nr_players:
        break
