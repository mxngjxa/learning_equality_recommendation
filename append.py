res_df = res_df.merge(res_df2, how="outer", on=["topic_id", "content_id"])
res_df = res_df.merge(dup_df, how="outer", on=["topic_id", "content_id"])
res_df = res_df.merge(second_degree_match_df, how="outer", on=["topic_id", "content_id"])
#res_df.shape


topic_df["key"] = topic_df["topic_title"].fillna("")

train_df = topic_df[~topic_df["topic_sub"]]


lookup = train_df.merge(corr_df, on="topic_id").groupby(["key", "content_id"])["topic_channel"].count().reset_index()
lookup.rename(columns={"topic_channel": "tdup_count"}, inplace=True)

dup_df = topic_df[topic_df["topic_sub"]].merge(lookup, on=["key"])[["topic_id", 'content_id', "tdup_count"]]

res_df = res_df.merge(dup_df, how="outer", on=["topic_id", "content_id"])
#res_df.shape

topic_df["key"] = topic_df["topic_parent"].fillna(topic_df["topic_id"])

train_df = topic_df[~topic_df["topic_sub"]]


lookup = train_df.merge(corr_df, on="topic_id").groupby(["key", "content_id"])["topic_channel"].count().reset_index()
lookup.rename(columns={"topic_channel": "pdup_count"}, inplace=True)

dup_df = topic_df[topic_df["topic_sub"]].merge(lookup, on=["key"])[["topic_id", 'content_id', "pdup_count"]]

res_df = res_df.merge(dup_df, how="outer", on=["topic_id", "content_id"])
#res_df.shape


res_df = res_df.merge(topic_df[topic_df["topic_sub"]], on="topic_id", how="left")
res_df = res_df.merge(content_df, on="content_id", how="left")

#res_df.shape



res_df["topic_language"] = res_df["topic_language"].astype("category")
res_df["topic_category"] = res_df["topic_category"].astype("category")
res_df["content_kind"] = res_df["content_kind"].astype("category")
res_df["topic_channel"] = res_df["topic_channel"].astype("category")

res_df["len_topic_title"] = res_df["topic_title"].fillna("").apply(len)
res_df["len_topic_description"] = res_df["topic_description"].fillna("").apply(len)
res_df["len_content_title"] = res_df["content_title"].fillna("").apply(len)
res_df["len_content_description"] = res_df["content_description"].fillna("").apply(len)
res_df["len_content_text"] = res_df["content_text"].fillna("").apply(len)
res_df["match_score_max"] = res_df.groupby("topic_id")["match_score"].transform("max")
res_df["match_score_min"] = res_df.groupby("topic_id")["match_score"].transform("min")

res_df["vec_dist_max"] = res_df.groupby("topic_id")["vec_dist"].transform("max")
res_df["vec_dist_min"] = res_df.groupby("topic_id")["vec_dist"].transform("min")

res_df["dup_count"] = res_df["dup_count"].fillna(0)
res_df["total_count"] = res_df.groupby("topic_id")["content_id"].transform("count")
res_df["dup_count_mean"] = res_df.groupby("topic_id")["dup_count"].transform("mean")

res_df["tdup_count"] = res_df["tdup_count"].fillna(0)
res_df["tdup_count_mean"] = res_df.groupby("topic_id")["tdup_count"].transform("mean")

res_df["pdup_count"] = res_df["pdup_count"].fillna(0)
res_df["pdup_count_mean"] = res_df.groupby("topic_id")["pdup_count"].transform("mean")

res_df["same_chapter"] = res_df["topic_chapter"] == res_df["content_chapter"]
res_df["starts_same"] = res_df["topic_title"].apply(lambda x: x.split(" ", 1)[0]) == res_df["content_title"].apply(lambda x: str(x).split(" ", 1)[0])

res_df["content_is_train"] = res_df["content_is_train"].astype(bool)

res_df.loc[~res_df["content_is_train"], "content_max_train_score"] = None
res_df["second_degree"].fillna(False, inplace=True)
res_df["topic_max_train_score"] = res_df["topic_id"].map(topic_sub_df.set_index("id")["max_train_score"].to_dict())

