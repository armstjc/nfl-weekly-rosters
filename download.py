import json
import time
from os.path import isfile

import pandas as pd
import requests
from tqdm import tqdm


def convert_height(height_str: str) -> int:
    """ """
    # "6'02"""
    if height_str == None or height_str == "":
        return 0
    height_str = height_str.replace('"', "")
    height_ft, height_in = height_str.split("'")
    height_ft = int(height_ft)
    height_in = int(height_in)
    return (height_ft * 12) + height_in


def get_team_weekly_rosters(team_id: str, season: int) -> pd.DataFrame:
    """ """
    team_id = team_id.lower()
    columns = [
        "season",
        "team",
        "position",
        "depth_chart_position",
        "jersey_number",
        "status",
        "full_name",
        "first_name",
        "last_name",
        "birth_date",
        "height",
        "weight",
        "college",
        "gsis_id",
        "espn_id",
        "sportradar_id",
        "yahoo_id",
        "rotowire_id",
        "pff_id",
        "pfr_id",
        "fantasy_data_id",
        "sleeper_id",
        "years_exp",
        "headshot_url",
        "ngs_position",
        "week",
        "game_type",
        "status_description_abbr",
        "football_name",
        "esb_id",
        "gsis_it_id",
        "smart_id",
        "entry_year",
        "rookie_year",
        "draft_club",
        "draft_number",
    ]
    weeks_arr = []
    roster_df = pd.DataFrame()
    roster_df_arr = []
    temp_df = pd.DataFrame()

    username = "firefox"  # Replace this
    password = "firefox"  # Replace this

    if season == 1982:
        # 1982: Strike year
        weeks_arr = [x for x in range(1, 17)]
    elif season == 1987:
        # 1987: Strike year
        weeks_arr = [x for x in range(1, 17)]
    elif season == 1993 or season == 2001:
        # 1999: Two bye weeks season
        # 2001: Significant rescheduling due to 9/11
        weeks_arr = [x for x in range(1, 19)]

    elif season < 1978:
        weeks_arr = [x for x in range(1, 15)]
    elif season >= 1978 and season < 1990:
        weeks_arr = [x for x in range(1, 17)]
    elif season >= 1990 and season < 2021:
        weeks_arr = [x for x in range(1, 18)]
    elif season >= 2021:
        weeks_arr = [x for x in range(1, 19)]

    for week in weeks_arr:
        url = (
            "https://www.nfl.info/nfldataexchange/dataexchange.asmx"
            + f"/getRosterAllJSON?lseason={season}"
            + f"&lweek={week}&lseasontype=reg"
            + f"&lclub={team_id}&showall=true"
        )
        if isfile(f"raw_json/{season}_{week:02d}_{team_id}.json"):
            with open(f"raw_json/{season}_{week:02d}_{team_id}.json", "r") as f:
                json_data = json.loads(f.read())
            temp_df = pd.json_normalize(json_data)
            roster_df_arr.append(temp_df)
        else:
            response = requests.get(url=url, auth=(username, password))
            json_data = json.loads(response.text)
            temp_df = pd.json_normalize(json_data)
            roster_df_arr.append(temp_df)

            if len(json_data) > 0:
                with open(
                    f"raw_json/{season}_{week:02d}_{team_id}.json", "w+"
                ) as f:
                    f.write(json.dumps(json_data, indent=4))

            time.sleep(4)

    roster_df = pd.concat(roster_df_arr, ignore_index=True)
    roster_df.rename(
        columns={
            "Season": "season",
            "CurrentClub": "team",
            "Position": "position",
            "JerseyNumber": "jersey_number",
            "StatusShortDescription": "status",
            # "":"full_name",
            "LastName": "last_name",
            "FirstName": "first_name",
            "FootballName": "football_name",
            "Birthdate": "birth_date",
            "Weight": "weight",
            "college": "college",
            "GsisID": "gsis_id",
            "EliasID": "esb_id",
            "Week": "week",
            "SeasonType": "game_type",
            "StatusDescriptionAbbr": "status_description_abbr",
            "DraftClub": "draft_club",
            "DraftNumber": "draft_number",
            "EntryYear": "entry_year",
            "RookieYear": "rookie_year",
        },
        inplace=True,
    )
    if len(roster_df) > 0 and len(roster_df.columns) > 0:
        roster_df["height"] = roster_df["Height"].map(convert_height)

        roster_df["full_name"] = (
            roster_df["football_name"] + " " + roster_df["last_name"]
        )
        roster_df = roster_df.reindex(columns=columns)
    return roster_df


def get_all_weekly_rosters(season: int) -> pd.DataFrame:
    """ """
    season_rosters_df = pd.read_csv(
        "https://github.com/nflverse/nflverse-data/releases/download"
        + f"/rosters/roster_{season}.csv"
    )
    temp_df = pd.DataFrame()
    rosters_df = pd.DataFrame()
    rosters_df_arr = []

    teams_arr = season_rosters_df["team"].to_numpy()
    teams_arr = set(teams_arr)

    # print(teams_arr)
    for team_id in tqdm(teams_arr):
        temp_df = get_team_weekly_rosters(team_id=team_id, season=season)
        rosters_df_arr.append(temp_df)
    rosters_df = pd.concat(rosters_df_arr, ignore_index=True)
    rosters_df.to_csv(f"csv/{season}_nfl_weekly_rosters.csv", index=False)


if __name__ == "__main__":
    for i in range(1971, 2002):
        print(f"Getting Rosters for the {i} season.")
        get_all_weekly_rosters(season=i)
