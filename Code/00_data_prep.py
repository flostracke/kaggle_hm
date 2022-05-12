import pandas as pd
import glob

# load and prepare the article data
df_articles = pd.read_csv("../Rawdata/articles.csv")
df_articles["article_id"] = df_articles["article_id"].astype(str)
df_articles["article_id"] = "0" + df_articles["article_id"]

# construct a df with the image path and the article id
img_files_raw = glob.glob("../Rawdata/**/*.jpg", recursive=True)
df_paths = pd.DataFrame({"path": img_files_raw})
df_paths["article_id"] = df_paths["path"].str[-14:].str.replace(".jpg", "", regex=False)

# only keep articles with images
df_articles = df_articles.merge(df_paths, on="article_id", how="inner")
df_articles.to_parquet("../Data/processed_articles.parquet", index=False)

df_articles.head()

# load and prepare the transaction data
df_transactions = pd.read_csv("../Rawdata/transactions_train.csv")
df_transactions["article_id"] = df_transactions["article_id"].astype(str)
df_transactions["article_id"] = "0" + df_transactions["article_id"]

# keep only transactions with images
df_transactions = df_transactions.merge(df_articles, on="article_id", how="inner")
df_transactions = df_transactions[["t_dat", "article_id", "price", "sales_channel_id"]]
df_transactions.to_parquet("../Data/processed_transactions.parquet", index=False)
