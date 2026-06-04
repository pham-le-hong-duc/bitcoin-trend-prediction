from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import re
import unicodedata

import polars as pl
try:
    from nltk.corpus import stopwords as nltk_stopwords
except ImportError:
    nltk_stopwords = None

from src.batch.timescaledb.base import HistoricalSource, HistoricalTimescaleBatch, INTERVAL_TO_MS


URL_PATTERN = re.compile(r"https?://\S+|www\.\S+")
CUSTOM_STOPWORDS = {
    "https",
    "http",
    "utm",
    "x200b",
    "xpost",
    "rbitcoin",
    "im",
    "i'm",
    "ive",
    "i've",
    "dont",
    "don't",
    "isnt",
    "isn't",
    "thats",
    "that's",
    "theres",
    "there's",
    "youre",
    "you're",
}
SHORT_TOKEN_ALLOWLIST = {"ai", "us", "uk", "eu"}


class SentimentBatch(HistoricalTimescaleBatch):
    def __init__(self) -> None:
        super().__init__(
            schema_name="dashboard",
            time_column="create_time",
            intervals=["1h", "4h", "1d"],
            historical_sources=[
                HistoricalSource(
                    name="submission",
                    prefix="submissions",
                    file_pattern="monthly",
                    file_prefix="RS",
                ),
                HistoricalSource(
                    name="comment",
                    prefix="comments",
                    file_pattern="monthly",
                    file_prefix="RC",
                ),
            ],
            base_start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
            minio_bucket="reddit",
        )
        try:
            if nltk_stopwords is None:
                raise ImportError
            self.stopwords = set(nltk_stopwords.words("english"))
        except (ImportError, LookupError):
            # Keep the batch runnable even if the NLTK corpus has not been downloaded yet.
            self.stopwords = {
                "a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "from",
                "if", "in", "into", "is", "it", "no", "not", "of", "on", "or", "such",
                "that", "the", "their", "then", "there", "these", "they", "this", "to",
                "was", "will", "with",
            }

    def table_name(self, interval: str) -> str:
        return f"sentiment_{interval}"

    def _normalize_submission(self, df: pl.DataFrame) -> pl.DataFrame:
        if df.is_empty():
            return df
        return (
            df.filter(
                pl.col("sentiment").is_not_null()
                & (pl.col("sentiment").cast(pl.Utf8) != "")
                & pl.col("created_utc").is_not_null()
            )
            .with_columns(
                pl.lit("submission").alias("source"),
                (pl.col("created_utc").cast(pl.Int64) * 1000).alias("event_ts_ms"),
                pl.col("sentiment").cast(pl.Int64),
                (
                    pl.coalesce([pl.col("title").cast(pl.Utf8), pl.lit("")])
                    + pl.lit(" ")
                    + pl.coalesce([pl.col("selftext").cast(pl.Utf8), pl.lit("")])
                ).alias("text"),
            )
            .select(["source", "event_ts_ms", "sentiment", "text"])
        )

    def _normalize_comment(self, df: pl.DataFrame) -> pl.DataFrame:
        if df.is_empty():
            return df
        return (
            df.filter(
                pl.col("sentiment").is_not_null()
                & (pl.col("sentiment").cast(pl.Utf8) != "")
                & pl.col("created_utc").is_not_null()
            )
            .with_columns(
                pl.lit("comment").alias("source"),
                (pl.col("created_utc").cast(pl.Int64) * 1000).alias("event_ts_ms"),
                pl.col("sentiment").cast(pl.Int64),
                pl.coalesce([pl.col("body").cast(pl.Utf8), pl.lit("")]).alias("text"),
            )
            .select(["source", "event_ts_ms", "sentiment", "text"])
        )

    def _tokenize(self, text: str) -> list[str]:
        lowered = (
            URL_PATTERN.sub(" ", text.lower())
            .replace("’", "'")
            .replace("‘", "'")
            .replace("\u200b", " ")
        )
        cleaned_chars = []
        for char in lowered:
            if char != "'" and unicodedata.category(char).startswith("P"):
                cleaned_chars.append(" ")
            else:
                cleaned_chars.append(char)

        tokens = []
        for token in "".join(cleaned_chars).split():
            if token in self.stopwords:
                continue
            if token in CUSTOM_STOPWORDS:
                continue
            if token.isdigit():
                continue
            if len(token) == 1:
                continue
            if len(token) == 2 and token not in SHORT_TOKEN_ALLOWLIST:
                continue
            tokens.append(token)
        return tokens

    def aggregate_timestamps(
        self,
        interval: str,
        timestamps: list[int],
        historical_frames: dict[str, pl.DataFrame],
    ) -> pl.DataFrame | None:
        frames = []
        submission_df = historical_frames.get("submission")
        comment_df = historical_frames.get("comment")

        if submission_df is not None and not submission_df.is_empty():
            frames.append(self._normalize_submission(submission_df))
        if comment_df is not None and not comment_df.is_empty():
            frames.append(self._normalize_comment(comment_df))

        if not frames:
            return None

        df = pl.concat(frames, how="vertical_relaxed")
        if df.is_empty():
            return None

        interval_ms = INTERVAL_TO_MS[interval]
        rows = []

        for boundary_ts_ms in timestamps:
            window_start = boundary_ts_ms - interval_ms
            window_df = df.filter(
                (pl.col("event_ts_ms") >= window_start)
                & (pl.col("event_ts_ms") < boundary_ts_ms)
            )

            if window_df.is_empty():
                continue

            sentiments = window_df["sentiment"].to_list()
            total = len(sentiments)
            positive = sum(1 for value in sentiments if value == 2)
            neutral = sum(1 for value in sentiments if value == 1)
            negative = sum(1 for value in sentiments if value == 0)

            frequency = Counter()
            for text in window_df["text"].to_list():
                frequency.update(self._tokenize(text or ""))

            top_words = dict(
                sorted(
                    ((word, count) for word, count in frequency.items() if count >= 2),
                    key=lambda item: (-item[1], item[0]),
                )[:100]
            )

            rows.append(
                {
                    "create_time": datetime.fromtimestamp(boundary_ts_ms / 1000, tz=timezone.utc),
                    "word_frequency": top_words,
                    "count": total,
                    "score": (positive - negative) / total,
                    "confidence": (positive + negative) / total,
                    "pct_negative": negative / total,
                    "pct_positive": positive / total,
                    "pct_neutral": neutral / total,
                }
            )

        return pl.DataFrame(rows) if rows else None


def main() -> None:
    batch = SentimentBatch()
    try:
        batch.detect_all_gaps_and_propagate()
        batch.fill_gaps()
    finally:
        batch.close()


if __name__ == "__main__":
    main()
