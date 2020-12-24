import requests
import pandas as pd
import numpy as np
import json
import warnings
from data_fetcher import Data_Fetcher


data_fetcher = Data_Fetcher()

fwds, mids, defs, keeps = data_fetcher.get_most_picked_players()

fwds.to_csv("fwds.csv", index=False)
mids.to_csv("mids.csv", index=False)
defs.to_csv("defs.csv", index=False)
keeps.to_csv("keeps.csv", index=False)


def pickTeam(keeps=keeps, defs=defs, fwds=fwds, mids=mids, budget=104):
    warnings.simplefilter(action="ignore", category=FutureWarning)
    team = pd.DataFrame()
    players_left_to_add = 15
    keeps_left_to_add = 2
    defs_left_to_add = 5
    mids_left_to_add = 5
    fwds_left_to_add = 3
    i = 0
    while i < 10 and keeps_left_to_add > 0:
        player_budget = budget / players_left_to_add
        keeper = keeps.iloc[i]
        if keeper.cost <= player_budget and keeper["second_name"] not in team.values:
            team = team.append(keeper)
            budget -= keeper.cost
            keeps_left_to_add -= 1
            players_left_to_add -= 1
        i += 1
    i = 0
    while i < 10 and defs_left_to_add > 0:
        player_budget = budget / players_left_to_add
        defender = defs.iloc[i]
        if (
            defender.cost <= player_budget
            and defender["second_name"] not in team.values
        ):
            team = team.append(defender)
            budget -= defender.cost
            defs_left_to_add -= 1
            players_left_to_add -= 1
        i += 1
    i = 0
    while i < 10 and fwds_left_to_add > 0:
        player_budget = budget / players_left_to_add
        forward = fwds.iloc[i]
        if forward.cost <= player_budget and forward["second_name"] not in team.values:
            team = team.append(forward)
            budget -= forward.cost
            fwds_left_to_add -= 1
            players_left_to_add -= 1
        i += 1
    i = 0
    while i < 10 and mids_left_to_add > 0:
        player_budget = budget / players_left_to_add
        mid = mids.iloc[i]
        if mid.cost <= player_budget and mid["second_name"] not in team.values:
            team = team.append(mid)
            budget -= mid.cost
            mids_left_to_add -= 1
            players_left_to_add -= 1
        i += 1
    i = 0
    return team


team = pickTeam()

print(team)
print(f"Points: {team['points'].sum()}")
print(f"Cost: {team['cost'].sum()}")
