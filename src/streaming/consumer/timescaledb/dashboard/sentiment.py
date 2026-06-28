"""
Realtime TimescaleDB consumer for Reddit sentiment dashboard aggregates.

Flow:
- Read reddit submissions, comments, and reddit-status from Redpanda
- Load recent submissions/comments history from MinIO
- Only process boundaries that have a matching reddit-status ping
- Aggregate sentiment on 1h / 4h / 1d windows
"""

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

from .base import Consumer


URL_PATTERN = re.compile(r"https?://\S+|www\.\S+")
CUSTOM_STOPWORDS = {
    "able",
    "actual",
    "already",
    "also",
    "another",
    "anyone",
    "anything",
    "around",
    "could",
    "don",
    "even",
    "every",
    "get",
    "got",
    "https",
    "http",
    "many",
    "much",
    "now",
    "one",
    "really",
    "see",
    "since",
    "someone",
    "something",
    "thing",
    "things",
    "use",
    "utm",
    "way",
    "will",
    "would",
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


class SentimentConsumer(Consumer):
    """Realtime dashboard consumer for Reddit sentiment aggregates."""

    def __init__(self, **kwargs):
        self.pending_status_boundaries = set()
        try:
            if nltk_stopwords is None:
                raise ImportError
            self.stopwords = set(nltk_stopwords.words("english"))
        except (ImportError, LookupError):
            # Keep the realtime consumer runnable even if the NLTK corpus has not been downloaded yet.
            self.stopwords = {
                "a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "from",
                "if", "in", "into", "is", "it", "no", "not", "of", "on", "or", "such",
                "that", "the", "their", "then", "there", "these", "they", "this", "to",
                "was", "will", "with",
            }
        super().__init__(
            topics=["reddit-submissions", "reddit-comments", "reddit-status"],
            group_id="timescaledb-dashboard-reddit-sentiment",
            data_type="reddit-sentiment",
            timestamp_field="event_ts_ms",
            intervals=["1h", "4h", "1d"],
            boundary_interval="10m",
            dedupe_columns=["source", "record_id"],
            warmup_messages=200,
            minio_bucket="reddit",
            historical_sources=[
                ("submissions", "submission"),
                ("comments", "comment"),
            ],
            schema_name="dashboard",
            key_column="create_time",
            **kwargs,
        )

    def transform_record(self, record, topic):
        if topic == "reddit-status":
            timestamp_utc = record.get("timestamp_utc")
            if timestamp_utc is None:
                return None

            timestamp_ms = int(timestamp_utc) * 1000
            boundary_ts_ms = (
                (timestamp_ms + self.base_boundary_ms - 1)
                // self.base_boundary_ms
            ) * self.base_boundary_ms
            self.pending_status_boundaries.add(boundary_ts_ms)
            return None

        if topic == "reddit-submissions":
            return self._normalize_submission_record(record)

        if topic == "reddit-comments":
            return self._normalize_comment_record(record)

        return None

    def transform_historical_df(self, df, source_name):
        if source_name == "submission":
            records = []
            for row in df.to_dicts():
                normalized = self._normalize_submission_record(row)
                if normalized is not None:
                    records.append(normalized)
            return pl.DataFrame(records) if records else pl.DataFrame()

        if source_name == "comment":
            records = []
            for row in df.to_dicts():
                normalized = self._normalize_comment_record(row)
                if normalized is not None:
                    records.append(normalized)
            return pl.DataFrame(records) if records else pl.DataFrame()

        return pl.DataFrame()

    def should_evaluate_boundaries_without_new_records(self):
        return bool(self.pending_status_boundaries)

    def _boundary_requires_status(self, boundary_ts_ms):
        return any(
            self._should_aggregate_interval(boundary_ts_ms, interval)
            for interval in self.intervals
        )

    def can_process_boundary(self, boundary_ts_ms, max_ts):
        if not self._boundary_requires_status(boundary_ts_ms):
            return True
        return boundary_ts_ms in self.pending_status_boundaries

    def boundary_ready_ts(self, max_ts):
        if not self.pending_status_boundaries:
            return max_ts
        return max(max_ts, max(self.pending_status_boundaries))

    def on_boundary_processed(self, boundary_ts_ms):
        self.pending_status_boundaries.discard(boundary_ts_ms)

    def aggregate_window(self, df_window, window_ts, interval):
        total = len(df_window)
        if total == 0:
            return None

        sentiments = df_window["sentiment"].to_list()
        negative = sum(1 for value in sentiments if value == 0)
        neutral = sum(1 for value in sentiments if value == 1)
        positive = sum(1 for value in sentiments if value == 2)

        score = (positive - negative) / total
        confidence = (positive + negative) / total

        texts = df_window["text"].to_list()
        freq_counter = Counter()
        for text in texts:
            freq_counter.update(self._tokenize_text(text))

        filtered = sorted(
            {word: count for word, count in freq_counter.items() if count >= 2}.items(),
            key=lambda item: (-item[1], item[0]),
        )[:100]

        word_frequency = {word: count for word, count in filtered}

        row = {
            "create_time": datetime.fromtimestamp(window_ts / 1000, tz=timezone.utc),
            "word_frequency": word_frequency,
            "count": total,
            "score": score,
            "confidence": confidence,
            "pct_negative": negative / total,
            "pct_positive": positive / total,
            "pct_neutral": neutral / total,
        }
        return pl.DataFrame([row])

    def resolve_table_target(self, interval):
        return ("dashboard", f"sentiment_{interval}")

    def _normalize_submission_record(self, record):
        sentiment = record.get("sentiment")
        if sentiment is None or sentiment == "":
            return None

        created_utc = record.get("created_utc")
        if created_utc is None:
            return None

        title = record.get("title") or ""
        selftext = record.get("selftext") or ""
        text = f"{title} {selftext}".strip()

        return {
            "record_id": record.get("id"),
            "source": "submission",
            "event_ts_ms": int(created_utc) * 1000,
            "sentiment": int(sentiment),
            "text": text,
        }

    def _normalize_comment_record(self, record):
        sentiment = record.get("sentiment")
        if sentiment is None or sentiment == "":
            return None

        created_utc = record.get("created_utc")
        if created_utc is None:
            return None

        return {
            "record_id": record.get("id"),
            "source": "comment",
            "event_ts_ms": int(created_utc) * 1000,
            "sentiment": int(sentiment),
            "text": (record.get("body") or "").strip(),
        }

    def _tokenize_text(self, text):
        if not text:
            return []

        normalized = (
            URL_PATTERN.sub(" ", text.lower())
            .replace("’", "'")
            .replace("‘", "'")
            .replace("\u200b", " ")
        )
        cleaned_chars = []
        for char in normalized:
            category = unicodedata.category(char)
            if char != "'" and (category.startswith("P") or category.startswith("S")):
                cleaned_chars.append(" ")
            else:
                cleaned_chars.append(char)

        cleaned_tokens = []
        for token in "".join(cleaned_chars).split():
            token = token.strip("'")
            if not token:
                continue
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
            cleaned_tokens.append(token)
        return cleaned_tokens


def main():
    consumer = SentimentConsumer()
    consumer.consume()


if __name__ == "__main__":
    main()
