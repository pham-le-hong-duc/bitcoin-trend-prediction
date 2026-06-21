from pathlib import Path

from base import RedditZstFilter


DATA_DIR = Path(__file__).resolve().parent / "reddit" / "submissions"
if __name__ == "__main__":
    stats = RedditZstFilter.run(
        input_files=[
            str(DATA_DIR / "RS_2026-01.zst"),
            str(DATA_DIR / "RS_2026-02.zst"),
            str(DATA_DIR / "RS_2026-03.zst"),
            str(DATA_DIR / "RS_2026-04.zst"),
            ],
        subreddits = ["Bitcoin", "InBitcoinWeTrust", "btc", "Buttcoin", 
                    "BitcoinBeginners", "BitcoinMarkets", "BitcoinMining", 
                    "BitcoinDE", "BitcoinBrasil", "BitcoinCA", "BitcoinUK", 
                    "BitcoinEU", "Bitcoincash", "BitcoinIndia", "BitcoinAUS", 
                    "bitcoincashSV", "Daytrading", "CryptoCurrency", 
                    "CryptoMarkets", "Trading", "BitMartExchange", 
                    "XGramatikInsights", "CryptoChartWatch", "CryptoIndia", 
                    "CryptoTax", "DubaiCrypto", "CryptoCurrencyClassic", 
                    "cryptocurrencymemes", "CryptoHelp", "CryptoExchange", 
                    "binance", "CryptoReality", "AllCryptoBets", "CryptoNews", 
                    "CryptoTechnology", "nanotrade", "WallStreetBetsCrypto", 
                    "Crypto_com", "BinanceCrypto", "Crypto_Currency_News", 
                    "CryptoStock", "altcoin", "CryptoMarsShots", "Crypto_General", 
                    "CryptoTradingFloor", "CryptoMars", "CryptoInvesting", 
                    "CryptoMoon", "CryptoMoonInvestors", "CryptoCurrencyTrading", 
                    "HodlyCrypto", "CryptoNews2day"],
        fields=["id", "author", "created_utc", "subreddit", "link_flair_text", "title", "selftext"],
        parallel=True  
    )
'''
r/Bitcoin 792K weekly visitors 12K weekly contributions
r/InBitcoinWeTrust 449K weekly visitors 8.5K weekly contributions
r/btc 378K weekly visitors 6.2K weekly contributions
r/Buttcoin 115K weekly visitors 2.4K weekly contributions
r/BitcoinBeginners 82K weekly visitors 1.1K weekly contributions
r/BitcoinMarkets 8.4K weekly visitors 795 weekly contributions
r/BitcoinMining 41K weekly visitors 449 weekly contributions
r/BitcoinDE 19K weekly visitors 363 weekly contributions
r/BitcoinBrasil 8.7K weekly visitors 254 weekly contributions
r/BitcoinCA 28K weekly visitors 215 weekly contributions
r/BitcoinUK 20K weekly visitors 146 weekly contributions
r/BitcoinEU 3.8K weekly visitors 114 weekly contributions
r/Bitcoincash 3K weekly visitors 100 weekly contributions
r/BitcoinIndia 7.1K weekly visitors 99 weekly contributions
r/BitcoinAUS 8.5K weekly visitors 70 weekly contributions
r/bitcoincashSV 601 weekly visitors 33 weekly contributions
 
r/Daytrading 497K weekly visitors 13K weekly contributions
r/CryptoCurrency 913K weekly visitors 10K weekly contributions
r/CryptoMarkets 203K weekly visitors 4.9K weekly contributions
r/Trading 121K weekly visitors 4.4K weekly contributions
r/BitMartExchange 1.2K weekly visitors 1.9K weekly contributions
r/XGramatikInsights 19K weekly visitors 1.7K weekly contributions
r/CryptoChartWatch 24K weekly visitors 1.7K weekly contributions
r/CryptoIndia 33K weekly visitors 1.4K weekly contributions
r/CryptoTax 18K weekly visitors 912 weekly contributions
r/DubaiCrypto 5.2K weekly visitors 888 weekly contributions
r/CryptoCurrencyClassic 1.2K weekly visitors 837 weekly contributions
r/cryptocurrencymemes 50K weekly visitors 695 weekly contributions
r/CryptoHelp 26K weekly visitors 661 weekly contributions
r/CryptoExchange 3.2K weekly visitors 639 weekly contributions
r/binance 21K weekly visitors 382 weekly contributions
r/CryptoReality 14K weekly visitors 364 weekly contributions
r/AllCryptoBets 1.4K weekly visitors 259 weekly contributions
r/CryptoNews 3.2K weekly visitors 256 weekly contributions
r/CryptoTechnology 9.7K weekly visitors 235 weekly contributions
r/nanotrade 1.3K weekly visitors 228 weekly contributions
r/WallStreetBetsCrypto 13K weekly visitors 225 weekly contributions
r/Crypto_com 24K weekly visitors 212 weekly contributions
r/BinanceCrypto 743 weekly visitors 209 weekly contributions
r/Crypto_Currency_News 1.9K weekly visitors 180 weekly contributions
r/CryptoStock 11K weekly visitors 172 weekly contributions
r/altcoin 2.7K weekly visitors 163 weekly contributions
r/CryptoMarsShots 610 weekly visitors 154 weekly contributions
r/Crypto_General 1.5K weekly visitors 138 weekly contributions
r/CryptoTradingFloor 698 weekly visitors 131 weekly contributions
r/CryptoMars 1.1K weekly visitors 124 weekly contributions
r/CryptoInvesting 1.9K weekly visitors 123 weekly contributions
r/CryptoMoon 642 weekly visitors 108 weekly contributions
r/CryptoMoonInvestors 139 weekly visitors 104 weekly contributions
r/CryptoCurrencyTrading 1.3K weekly visitors 91 weekly contributions
r/HodlyCrypto 3.2K weekly visitors 72 weekly contributions
r/CryptoNews2day 171 weekly visitors 64 weekly contributions
'''

