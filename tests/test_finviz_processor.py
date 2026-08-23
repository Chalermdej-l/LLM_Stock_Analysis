import datetime

from stock_analysis.helper.finviz_processor import FinvizScraper

EXPECTED_COLUMNS = [
    "ticker",
    "company",
    "sector",
    "industry",
    "country",
    "market_cap",
    "pe",
    "volume",
    "price",
    "change",
    "date_insert",
]


def _row(ticker):
    return [
        "1",
        ticker,
        "Apple Inc.",
        "Technology",
        "Semiconductors",
        "USA",
        "$10.1B",
        "25.1",
        "51.7M",
        "$232.16",
        "-1.5%",
    ]


def test_create_dataframe_reshapes_cells_and_drops_index():
    scraper = FinvizScraper("http://example.com/screener")
    scraper.data = _row("AAPL") + _row("MSFT")
    scraper.create_dataFrame()

    assert list(scraper.df.columns) == EXPECTED_COLUMNS
    assert "index" not in scraper.df.columns
    assert scraper.df["ticker"].tolist() == ["AAPL", "MSFT"]
    assert scraper.df["company"].tolist() == ["Apple Inc.", "Apple Inc."]
    assert (scraper.df["date_insert"] == datetime.date.today().strftime("%Y-%m-%d")).all()
