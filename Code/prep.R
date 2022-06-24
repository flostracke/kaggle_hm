library(tidyverse)
library(fs)
library(arrow)

theme_set(theme_minimal())

tbl_articles <- read_csv("Rawdata/articles.csv") %>% 
  select(
    article_id,
    product_code,
    prod_name,
    product_type_name,
    product_group_name,
    graphical_appearance_name,
    colour_group_name,
    perceived_colour_value_name,
    department_name,
    index_name,
    index_group_name,
    section_name,
    garment_group_name,
    detail_desc
  )


image_paths = fs::dir_ls("Rawdata/images/", glob = "*.jpg", recurse = TRUE) %>% 
  as.character()

tbl_path <- tibble(path = image_paths) %>% 
  mutate(article_id = str_replace(basename(path), ".jpg", ""))


tbl_trans <- read_csv("Rawdata/transactions_train.csv") %>% 
  mutate(value = 1)

tbl_trans_agg <- tbl_trans %>% 
  left_join(tbl_articles, by = "article_id") %>% 
  group_by(product_code) %>% 
  summarise(value = sum(value), .groups = "drop") %>% 
  mutate(value_binned = ntile(value, n = 4) %>% as.character()) %>% 
  select(-value)
  
tbl_articles <- tbl_articles %>% 
  inner_join(tbl_trans_agg, by = "product_code") %>% 
  inner_join(tbl_path, by = "article_id")

tbl_articles %>% 
  write_parquet("Data/processed_articles.parquet")
