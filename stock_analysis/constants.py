# Database table names
SEC_13F_TABLE = "sec_13f"
DATAROMA_INSIDER_BUY_TABLE = "dataroma_insider_buy"
DATAROMA_SCREEN_INSIDER_TABLE = "dataroma_screen_insider"
DATAROMA_BIGBETS_TABLE = "dataroma_bigbets"
DATAROMA_LOW_TABLE = "dataroma_low"
DATAROMA_INSIDER_SUPER_TABLE = "dataroma_insider_super"
FINVIZ_SCREEN_TABLE = "finviz_screen"
MAGIC_SCREEN_TABLE = "magic_screen"

# Scraper endpoints
FINVIZ_SCREENER_URL = (
    "https://finviz.com/screener.ashx?v=151&f=cap_microover,fa_curratio_o2,"
    "fa_eps5years_o5,fa_opermargin_o10,fa_roe_pos,fa_sales5years_o5,geo_usa,"
    "sh_insiderown_o10,sh_insidertrans_neg,sh_outstanding_o1,sh_price_o4,"
    "ta_highlow52w_b30h&ft=4&o=change"
)
DATAROMA_INSIDER_BUY_PATH = "/m/ins/ins.php?t=w&po=1&am=10000&sym=&o=fd&d=d&L=1"
