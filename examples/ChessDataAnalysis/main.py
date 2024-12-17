import datetime
from dateutil.relativedelta import relativedelta
import io
import typing

import chess.pgn
import pandas as pd
from pyper import task
import requests


def generate_urls_by_month(player: str, num_months: int):
    """Define a series of pgn game resource urls for a player, for num_months recent months."""
    today = datetime.date.today()
    for i in range(num_months):
        d = today - relativedelta(months=i)
        yield f"https://api.chess.com/pub/player/{player}/games/{d.year}/{d.month:02}/pgn"


def fetch_text_data(url: str, session: requests.Session):
    """Fetch text data from a url."""
    r = session.get(url, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})
    return r.text


def _clean_opening_name(eco_url: str):
    """Get a rough opening name from the chess.com ECO url."""
    name = eco_url.removeprefix("https://www.chess.com/openings/")
    return " ".join(name.split("-")[:2])


def read_game_data(pgn_text: str, player: str):
    """Read PGN data and generate game details (each PGN contains details for multiple games)."""
    pgn = io.StringIO(pgn_text)
    while (headers := chess.pgn.read_headers(pgn)) is not None:
        color = 'W' if headers["White"].lower() == player else 'B'
        
        if headers["Result"] == "1/2-1/2":
            score = 0.5
        elif (color == 'W' and headers["Result"] == "1-0") or (color == 'B' and headers["Result"] == "0-1"):
            score = 1
        else:
            score = 0
        
        yield {
            "color": color,
            "score": score,
            "opening": _clean_opening_name(headers["ECOUrl"])
        }


def build_df(data: typing.Iterable[dict]) -> pd.DataFrame:
    df = pd.DataFrame(data)
    df = df[df["color"] == 'W']
    df = df.groupby("opening").agg(total_games=("score", "count"), average_score=("score", "mean"))
    df = df.sort_values(by="total_games", ascending=False)
    return df


def main():
    player = "hikaru"
    num_months = 6  # Keep this number low, or add sleeps for etiquette

    with requests.Session() as session:
        run = (
            task(generate_urls_by_month, branch=True)
            | task(
                fetch_text_data,
                workers=3,
                bind=task.bind(session=session))
            | task(
                read_game_data,
                branch=True,
                bind=task.bind(player=player))
            > build_df
        )
        df = run(player, num_months)
        print(df.head(10))


if __name__ == "__main__":
    main()
