import pathlib

import pandas as pd


def load_reviews_dataset(folder_with_csvs_path="../data/raw/steam-reviews-dataset/"):
    # steam reviews dataset
    chunk_paths = [path for path in pathlib.Path(folder_with_csvs_path).glob("*.csv")]
    dfs = []
    for i in chunk_paths:
        dfs.append(pd.read_csv(i))
    reviews = (
        pd.concat(dfs)
        .rename(columns={"appid": "app_id", "steamid": "user_id"})
        .assign()
    )
    reviews["unix_timestamp_created"] = pd.to_datetime(reviews["unix_timestamp_created"], unit="s")
    reviews["unix_timestamp_updated"] = pd.to_datetime(reviews["unix_timestamp_updated"], unit="s")
    return reviews


def load_game_recommendations_on_steam_games_dataset(csv_path="../data/raw/game-recommendations-on-steam/games.csv"):
    return (
        pd.read_csv(csv_path)
        .rename(columns={
            "date_release": "release_date",
            "price_final": "final_price",
            "price_original": "original_price",
            "win": "win_support",
            "mac": "mac_support",
            "linux": "linux_support",
            "steam_deck": "steam_deck_support",
        }).drop("discount", axis=1)
    )


def load_steam_games_complete_dataset(csv_path="../data/raw/steam-games-complete-dataset/steam_games.csv"):
    def convert_price_to_float(x):
        if isinstance(x, float):
            return x
        elif x.startswith("$"):
            return float(x.lstrip("$"))
        elif x == "Free":
            return 0
        else:
            return None

    # int(x.lstrip("$") if x.startswith("$") else (if )

    df_games_complete = (
        pd.read_csv(csv_path)
        .query("types == 'app' | types == 'bundle'")
        .rename(columns={"name": "title", "achievements": "n_game_achievements"})
        .assign(refined_price=lambda df: df["original_price"].apply(convert_price_to_float))
    )
    df_games_complete["app_id"] = (
        df_games_complete.url
        .str.removeprefix("https://store.steampowered.com/")
        .str.split("/")
        .apply(lambda x: x[1]).astype(int)
    )
    df_games_complete["app_id"] = df_games_complete["app_id"].astype(int)
    return df_games_complete


def load_dataset(
    steam_reviews_dataframe,
    game_rec_steam_dataframe,
    steam_games_complete_dataframe,
):
    # @TODO: remove duplicates
    duplicates = (("name", "title"), ("release_date", "date_release"), ("rating", "positive_ratio", "recent_reviews"))
    games_joined = (
        steam_games_complete_dataframe.set_index("app_id")
        .join(game_rec_steam_dataframe.set_index("app_id"), lsuffix="_sgcd", rsuffix="_grps")
        .drop([
            "url",
            "minimum_requirements", "recommended_requirements",  # can be turned into features
            # can be turned into features
            "discount_price",
            "user_reviews", "rating", "positive_ratio",  # can result in data leakage
            "final_price", "original_price_grps",
        ], axis=1)
        .assign(
            final_title=lambda df: df.apply(
                lambda row: (row.title_grps if not pd.isna(row.title_grps) else row.title_sgcd),
                axis=1),
        )
    )
    return (
        steam_reviews_dataframe.sort_values(by="app_id")
        .join(games_joined.sort_values(by="app_id"), on="app_id", lsuffix="srd")
    )


def impute_missing_values(dataframe, timestamp_col):
    return dataframe

if __name__ == "__main__":
    reviews = load_reviews_dataset()
    df_games_rec = load_game_recommendations_on_steam_games_dataset()
    games_complete = load_steam_games_complete_dataset()
    ready_dataset = load_dataset(reviews, df_games_rec, games_complete)
    ready_dataset.to_csv("../data/interim/joined_dataset.csv")




