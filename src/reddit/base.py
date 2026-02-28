import zstandard
import os
import json
import csv
from datetime import datetime
from typing import List, Dict
import logging


class RedditZstFilter:
    """Filter Reddit .zst files by subreddit and export to CSV."""
    
    def __init__(self, input_file: str, output_file: str = None, 
                 subreddits: List[str] = None, fields: List[str] = None,
                 progress_interval: int = 100000,
                 verbose: bool = True):
        self.input_file = input_file
        
        if output_file is None:
            if input_file.endswith('.zst'):
                self.output_file = input_file[:-4] + '.csv'
            else:
                self.output_file = input_file + '.csv'
        else:
            self.output_file = output_file
        
        self.subreddits = [s.lower() for s in subreddits] if subreddits else []
        self.fields = fields if fields else []
        self.progress_interval = progress_interval
        self.verbose = verbose
        
        self.total_lines = 0
        self.matched_lines = 0
        self.bad_lines = 0
        
        self.log = logging.getLogger(f"RedditZstFilter-{id(self)}")
        self.log.setLevel(logging.INFO if verbose else logging.WARNING)
        if not self.log.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s: %(message)s'))
            self.log.addHandler(handler)
        
        self.is_submission = "submission" in input_file.lower() or "_rs_" in input_file.lower()
    
    @staticmethod
    def _read_and_decode(reader, chunk_size, max_window_size, previous_chunk=None, bytes_read=0):
        chunk = reader.read(chunk_size)
        bytes_read += chunk_size
        if previous_chunk is not None:
            chunk = previous_chunk + chunk
        try:
            return chunk.decode()
        except UnicodeDecodeError:
            if bytes_read > max_window_size:
                raise UnicodeError(f"Unable to decode frame after reading {bytes_read:,} bytes")
            return RedditZstFilter._read_and_decode(reader, chunk_size, max_window_size, chunk, bytes_read)
    
    def _read_lines_zst(self):
        with open(self.input_file, 'rb') as file_handle:
            buffer = ''
            reader = zstandard.ZstdDecompressor(max_window_size=2**31).stream_reader(file_handle)
            while True:
                chunk = self._read_and_decode(reader, 2**27, (2**29) * 2)
                if not chunk:
                    break
                lines = (buffer + chunk).split("\n")
                for line in lines[:-1]:
                    yield line.strip(), file_handle.tell()
                buffer = lines[-1]
            reader.close()
    
    def _get_field_value(self, obj: dict, field: str) -> str:
        if field == "created":
            return datetime.fromtimestamp(int(obj['created_utc'])).strftime("%Y-%m-%d %H:%M:%S")
        elif field == "created_date":
            return datetime.fromtimestamp(int(obj['created_utc'])).strftime("%Y-%m-%d")
        elif field in ("link", "permalink_full"):
            if 'permalink' in obj:
                return f"https://www.reddit.com{obj['permalink']}"
            else:
                return f"https://www.reddit.com/r/{obj['subreddit']}/comments/{obj['link_id'][3:]}/_/{obj['id']}"
        elif field == "author_prefixed":
            return f"u/{obj.get('author', '[deleted]')}"
        elif field == "text":
            return obj.get('selftext' if self.is_submission else 'body', '')
        elif field in obj:
            return obj[field]
        else:
            return ""
    
    def process(self) -> dict:
        if not os.path.exists(self.input_file):
            raise FileNotFoundError(f"Input file not found: {self.input_file}")
        
        file_size = os.stat(self.input_file).st_size
        self.log.info(f"Processing: {self.input_file} ({file_size / (1024**3):.2f} GB)")
        self.log.info(f"Output: {self.output_file}")
        self.log.info(f"Subreddits: {', '.join(self.subreddits)}")
        self.log.info(f"Fields: {', '.join(self.fields)}")
        
        with open(self.output_file, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(self.fields)
            
            self.total_lines = 0
            self.matched_lines = 0
            self.bad_lines = 0
            created = None
            
            for line, file_bytes_processed in self._read_lines_zst():
                self.total_lines += 1
                
                if self.total_lines % self.progress_interval == 0:
                    progress = (file_bytes_processed / file_size) * 100
                    time_str = created.strftime('%Y-%m-%d %H:%M:%S') if created else 'N/A'
                    self.log.info(f"{time_str} | Lines: {self.total_lines:,} | Matched: {self.matched_lines:,} | Progress: {progress:.1f}%")
                
                try:
                    obj = json.loads(line)
                    
                    if 'created_utc' in obj:
                        created = datetime.utcfromtimestamp(int(obj['created_utc']))
                    
                    if obj.get('subreddit', '').lower() not in self.subreddits:
                        continue
                    
                    row = []
                    for field in self.fields:
                        try:
                            value = self._get_field_value(obj, field)
                            row.append(str(value).encode("utf-8", errors='replace').decode())
                        except:
                            row.append("")
                    
                    writer.writerow(row)
                    self.matched_lines += 1
                    
                except (json.JSONDecodeError, KeyError):
                    self.bad_lines += 1
        
        self.log.info("=" * 80)
        self.log.info(f"Complete! Total: {self.total_lines:,} | Matched: {self.matched_lines:,} | Bad: {self.bad_lines:,}")
        if self.total_lines > 0:
            self.log.info(f"Match rate: {(self.matched_lines / self.total_lines * 100):.2f}%")
        
        return {
            'total_lines': self.total_lines,
            'matched_lines': self.matched_lines,
            'bad_lines': self.bad_lines,
            'match_rate': (self.matched_lines / self.total_lines * 100) if self.total_lines > 0 else 0
        }
    
    @classmethod
    def run(cls, input_file: str, output_file: str = None, 
            subreddits: List[str] = None, fields: List[str] = None,
            progress_interval: int = 100000,
            verbose: bool = True) -> dict:
        return cls(input_file, output_file, subreddits, fields, progress_interval, verbose).process()
