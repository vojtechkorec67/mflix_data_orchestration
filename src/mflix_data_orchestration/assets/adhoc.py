from dagster import asset, Config
from dagster_snowflake import SnowflakeResource

import pandas as pd
from matplotlib import pyplot as plt
from sklearn.manifold import TSNE


class AdhocConfig(Config):
    filename: str
    ratings: str


def _parse_embedding(embedding_str):
    cleaned_str = embedding_str.replace(' ', '').replace('\n', '').replace('[', '').replace(']', '')
    return list(eval(cleaned_str))


@asset(
    deps=["dlt_mongodb_embedded_movies"]
)
def movie_embeddings(config: AdhocConfig, snowflake: SnowflakeResource):
    """
    Generate movie embedding plots using t-NSE algorithm, based on movie ratings
    """
    query = f"""
        select
            movies.title,
            movies.imdb__rating,
            movies.imdb__votes
        from embedded_movies movies
        where
            movies.imdb__rating >= {config.ratings}
        group by movies.title, movies.imdb__rating, movies.imdb__votes
    """

    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        df = cursor.fetch_pandas_all()

    # Prepare numeric rating and votes
    df['IMDB__RATING'] = pd.to_numeric(df['IMDB__RATING'], errors='coerce')
    df['IMDB__VOTES'] = pd.to_numeric(df['IMDB__VOTES'], errors='coerce').fillna(0).astype(int)

    # For log-scale plotting, replace zeros with 1
    votes_plot = df['IMDB__VOTES'].replace(0, 1)

    # Scatter plot: Vote count vs IMDb rating
    _, ax = plt.subplots(figsize=(10, 8))
    ax.scatter(df['IMDB__RATING'], votes_plot, alpha=.6)
    ax.set_xlabel('IMDb Rating')
    ax.set_ylabel('Vote Count')
    ax.set_title('Vote Count vs IMDb Rating')
    ax.set_yscale('log')
    for idx, title in enumerate(df['TITLE']):
        ax.annotate(title, (df['IMDB__RATING'].iloc[idx], votes_plot.iloc[idx]))
    plt.tight_layout()
    plt.savefig('data/votes_vs_rating.png')
