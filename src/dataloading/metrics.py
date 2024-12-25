import numpy as np
import pandas as pd


def ndcg_at_k_score(df_true, df_pred, *, user_col, item_col, k=10):
    return df_true.set_index([user_col, item_col]).join(
        df_pred.set_index([user_col, item_col]),
        how="left",
        lsuffix="_true",
        rsuffix="_predicted"
    )


def mrr_at_k_score(
    df_true, df_pred,
    predicted_col="rank",
    *, user_col, item_col, k=10,
):
    return df_true.set_index([user_col, item_col]).join(
        df_pred.set_index([user_col, item_col]),
        how="left",
        lsuffix="_true",
        rsuffix="_predicted"
    ).sort_values(by=[user_col, predicted_col]).assign(
        reciprocal_rank=lambda x: 1 / x[predicted_col],
    ).groupby(level=user_col).max().fillna(0).mean()


def map_at_k_score(df_true, df_pred, predicted_col="rank", *, user_col="user_id", item_col="item_id", k=10):
    a = df_true.set_index([user_col, item_col]).join(
        df_pred.set_index([user_col, item_col]),
        how="left",
        lsuffix="_true",
        rsuffix="_predicted"
    ).sort_values(by=[user_col, predicted_col]).assign(
        cumulative_rank=lambda x: x.groupby(level=user_col).cumcount() + 1
    ).assign(
        cumulative_rank=lambda x: x["cumulative_rank"] / x["rank"]
    ).assign(
        user_item_count=lambda x: x.groupby(level=user_col)["rank"].transform(np.size)
    )
    users_count = a.index.get_level_values(user_col).nunique()
    return (a["cumulative_rank"] / a["users_item_count"]).sum() / users_count


