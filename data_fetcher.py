import requests
import pandas as pd
from collections import Counter
import asyncio
import concurrent.futures
import functools
import nest_asyncio


class Data_Fetcher:
    nest_asyncio.apply()

    GAMEWEEK = "14"
    PAGES = 100
    OVERALL_LEAGUE_ID = "314"
    RETRY = 5
    DIVISOR = (PAGES * 50) / 100
    MISSED_PAGE_COUNTER = 0
    MISSED_ENTRY_COUNTER = 0
    TOTAL_ENTRIES_PROCESSED = 0
    BASE_URL = "https://fantasy.premierleague.com/api/"
    STANDINGS_ENDPOINT = "/standings/?page_standings="
    PLAYERS_ENDPOINT = "bootstrap-static/"

    def get_page_urls_to_query_as_list(self):
        """Gets the urls for each page of entries in the overall league"""
        i = 1
        url_list = []
        print("Creating Page Url List")
        while i < self.PAGES:
            url = (
                self.BASE_URL
                + "leagues-classic/"
                + self.OVERALL_LEAGUE_ID
                + self.STANDINGS_ENDPOINT
                + str(i)
            )
            url_list.append(url)
            i += 1
        return url_list

    def get_top_entry_ids(self):
        """Gets the entry ids for the top entries in the overall league"""
        page_urls = self.get_page_urls_to_query_as_list()
        entry_ids = []
        loop = asyncio.get_event_loop()
        print("Querying for Entries in Pages")
        results = loop.run_until_complete(self.get_all_top_entries(page_urls))
        print("Returning Entry Urls")
        for result in results:
            entry_ids.append(result["entry"])
        return entry_ids

    async def get_all_top_entries(self, page_urls):
        """ Gets all top entries in the given page urls"""
        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=40) as executor:
            loop = asyncio.get_event_loop()
            futures = [
                loop.run_in_executor(
                    executor,
                    functools.partial(self.get_page_entries, url),
                )
                for url in page_urls
            ]
            for response in await asyncio.gather(*futures):
                try:
                    response = response.json()
                    results.extend(response["standings"]["results"])
                except:
                    self.MISSED_PAGE_COUNTER += 1
        print(f"Pages Missed: {self.MISSED_PAGE_COUNTER}")
        return results

    def get_page_entries(self, page_url):
        """Gets all entries in a given page. With some retry logic"""
        try:
            response = requests.get(page_url)
            temp = response.json()
        except:
            i = 0
            temp = None
            while i < self.RETRY and temp == None:
                response = requests.get(page_url)
                try:
                    temp = response.json()
                except:
                    response = None
                i += 1
        return response

    def get_entry_urls_to_query_as_list(self, entries):
        """Gets the urls for each top entry in the overall league"""
        url_list = []
        for entry in entries:
            url = (
                self.BASE_URL
                + "entry/"
                + str(entry)
                + "/event/"
                + self.GAMEWEEK
                + "/picks/"
            )
            url_list.append(url)
        return url_list

    def get_top_players(self, entries):
        """Gets the list of top picked players by the top entries"""
        entry_urls = self.get_entry_urls_to_query_as_list(entries)
        loop = asyncio.get_event_loop()
        print("Querying for All Picked players")
        players = loop.run_until_complete(self.get_players_from_all_entries(entry_urls))
        print("Returning All Picked players")
        return players

    async def get_players_from_all_entries(self, url_list):
        """Gets all picked players for the entries provided"""
        players = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=40) as executor:
            loop = asyncio.get_event_loop()
            futures = [
                loop.run_in_executor(
                    executor,
                    functools.partial(self.get_entry_picks, url),
                )
                for url in url_list
            ]
            for response in await asyncio.gather(*futures):
                try:
                    response = response.json()
                    picks = response["picks"]
                    for pick in picks:
                        players.append(pick["element"])
                except:
                    self.MISSED_ENTRY_COUNTER += 1
        print(f"Missed Entries: {self.MISSED_ENTRY_COUNTER}")
        return players

    def get_entry_picks(self, url):
        """Gets the picked players for a given entry"""
        try:
            response = requests.get(url)
            temp = response.json()
        except:
            i = 0
            temp = None
            while i < RETRY and temp == None:
                response = requests.get(url)
                try:
                    temp = response.json()
                except:
                    print(url)
                    response = None
                i += 1
        self.TOTAL_ENTRIES_PROCESSED += 1
        if self.TOTAL_ENTRIES_PROCESSED % 1000 == 0:
            print(f"Processed {self.TOTAL_ENTRIES_PROCESSED} of {self.PAGES * 50}")
        return response

    def get_players_data(self):
        """Get data for all league players"""
        url = "https://fantasy.premierleague.com/api/" + self.PLAYERS_ENDPOINT

        response = requests.get(url)

        response_json = response.json()

        players = pd.DataFrame(response_json["elements"])
        positions = pd.DataFrame(response_json["element_types"])
        teams = pd.DataFrame(response_json["teams"])

        pd.set_option("mode.chained_assignment", None)

        players = players[
            [
                "id",
                "second_name",
                "team",
                "element_type",
                "now_cost",
                "value_season",
                "total_points",
            ]
        ]

        players["value"] = players.value_season.astype(float)
        players = players.drop("value_season", axis=1)

        players["points"] = players.total_points.astype(float)
        players = players.drop("total_points", axis=1)

        players["cost"] = players["now_cost"] / 10
        players = players.drop("now_cost", axis=1)

        # Set Position
        players["position"] = players.element_type.map(
            positions.set_index("id").singular_name
        )
        players = players.drop("element_type", axis=1)

        # Set Team
        players["team"] = players.team.map(teams.set_index("id").name)

        return players

    def get_most_picked_players(self):
        """Gets the most picked players from the top entries"""
        print("Getting Top Picked Players")
        entries = self.get_top_entry_ids()
        picks = self.get_top_players(entries)
        picks_count = dict(Counter(picks))
        picks_count = pd.DataFrame(picks_count.items(), columns=["id", "confidence"])
        picks_count["confidence"] = picks_count.confidence.astype(float)
        self.DIVISOR = (
            (self.DIVISOR)
            - (self.MISSED_PAGE_COUNTER * 0.5)
            - (self.MISSED_ENTRY_COUNTER * 0.01)
        )
        self.DIVISOR = round(self.DIVISOR)
        picks_count["confidence"] = picks_count["confidence"] / self.DIVISOR

        print("Getting player data")
        players = self.get_players_data()

        picked_players = picks_count.merge(players, on="id")
        picked_players = picked_players.drop("id", axis=1)
        picked_players = picked_players[picked_players["confidence"] > 5]
        picked_players = picked_players.sort_values("confidence", ascending=False)
        fwds = picked_players.loc[picked_players.position == "Forward"]
        fwds = fwds.sort_values("confidence", ascending=False)
        mids = picked_players.loc[picked_players.position == "Midfielder"]
        mids = mids.sort_values("confidence", ascending=False)
        defs = picked_players.loc[picked_players.position == "Defender"]
        defs = defs.sort_values("confidence", ascending=False)
        keeps = picked_players.loc[picked_players.position == "Goalkeeper"]
        keeps = keeps.sort_values("confidence", ascending=False)

        return fwds, mids, defs, keeps