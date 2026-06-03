import zstandard
import os
import json
import csv
from datetime import datetime
from typing import List, Dict, Union
import logging
from multiprocessing import Pool, cpu_count


def _process_file_worker(input_file: str, output_file: str, subreddits: List[str], 
                         fields: List[str], progress_interval: int, verbose: bool) -> dict:
    """Worker function for parallel processing. Must be at module level for pickling."""
    filter_instance = RedditZstFilter(
        input_files=input_file,
        output_file=output_file,
        subreddits=subreddits,
        fields=fields,
        progress_interval=progress_interval,
        verbose=verbose,
        parallel=False  # Disable parallel in worker to avoid nested parallelism
    )
    # Process single file
    result = filter_instance._process_single_file(input_file, output_file)
    return result


class RedditZstFilter:
    """Filter Reddit .zst files by subreddit and export to CSV."""
    
    def __init__(self, input_files: Union[str, List[str]], output_file: str = None, 
                 subreddits: List[str] = None, fields: List[str] = None,
                 progress_interval: int = 100000,
                 verbose: bool = True,
                 parallel: bool = False,
                 num_workers: int = None):
        # Accept both single file and list of files
        if isinstance(input_files, str):
            self.input_files = [input_files]
        else:
            self.input_files = input_files
        
        self.output_file = output_file
        self.subreddits = [s.lower() for s in subreddits] if subreddits else []
        self.fields = fields if fields else []
        self.progress_interval = progress_interval
        self.verbose = verbose
        self.parallel = parallel
        self.num_workers = num_workers if num_workers else min(cpu_count(), len(self.input_files))
        
        self.total_lines = 0
        self.matched_lines = 0
        self.bad_lines = 0
        
        self.log = logging.getLogger(f"RedditZstFilter-{id(self)}")
        self.log.setLevel(logging.INFO if verbose else logging.WARNING)
        if not self.log.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s: %(message)s'))
            self.log.addHandler(handler)
    
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
    
    def _read_lines_zst(self, input_file):
        with open(input_file, 'rb') as file_handle:
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
    
    def _get_field_value(self, obj: dict, field: str, is_submission: bool) -> str:
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
            return obj.get('selftext' if is_submission else 'body', '')
        elif field in obj:
            return obj[field]
        else:
            return ""
    
    def _process_single_file(self, input_file: str, output_file: str) -> dict:
        """Process a single input file and write to output file."""
        if not os.path.exists(input_file):
            raise FileNotFoundError(f"Input file not found: {input_file}")
        
        is_submission = "submission" in input_file.lower() or "_rs_" in input_file.lower()
        file_size = os.stat(input_file).st_size
        
        self.log.info(f"Processing: {input_file} ({file_size / (1024**3):.2f} GB)")
        self.log.info(f"Output: {output_file}")
        self.log.info(f"Subreddits: {', '.join(self.subreddits)}")
        self.log.info(f"Fields: {', '.join(self.fields)}")
        
        total_lines = 0
        matched_lines = 0
        bad_lines = 0
        created = None
        
        with open(output_file, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(self.fields)
            
            for line, file_bytes_processed in self._read_lines_zst(input_file):
                total_lines += 1
                
                if total_lines % self.progress_interval == 0:
                    progress = (file_bytes_processed / file_size) * 100
                    time_str = created.strftime('%Y-%m-%d %H:%M:%S') if created else 'N/A'
                    self.log.info(f"{time_str} | Lines: {total_lines:,} | Matched: {matched_lines:,} | Progress: {progress:.1f}%")
                
                try:
                    obj = json.loads(line)
                    
                    if 'created_utc' in obj:
                        created = datetime.utcfromtimestamp(int(obj['created_utc']))
                    
                    if obj.get('subreddit', '').lower() not in self.subreddits:
                        continue
                    
                    row = []
                    for field in self.fields:
                        try:
                            value = self._get_field_value(obj, field, is_submission)
                            row.append(str(value).encode("utf-8", errors='replace').decode())
                        except:
                            row.append("")
                    
                    writer.writerow(row)
                    matched_lines += 1
                    
                except (json.JSONDecodeError, KeyError):
                    bad_lines += 1
        
        self.log.info("=" * 80)
        self.log.info(f"Complete! Total: {total_lines:,} | Matched: {matched_lines:,} | Bad: {bad_lines:,}")
        if total_lines > 0:
            self.log.info(f"Match rate: {(matched_lines / total_lines * 100):.2f}%")
        
        return {
            'input_file': input_file,
            'output_file': output_file,
            'total_lines': total_lines,
            'matched_lines': matched_lines,
            'bad_lines': bad_lines,
            'match_rate': (matched_lines / total_lines * 100) if total_lines > 0 else 0
        }
    
    def _generate_output_filename(self, input_file: str, index: int) -> str:
        """Generate output filename for a given input file."""
        if self.output_file is None:
            if input_file.endswith('.zst'):
                return input_file[:-4] + '.csv'
            else:
                return input_file + '.csv'
        else:
            # If output_file is specified and we have multiple inputs, append index
            if len(self.input_files) > 1:
                base, ext = os.path.splitext(self.output_file)
                return f"{base}_{index}{ext}"
            else:
                return self.output_file
    
    def process(self) -> List[dict]:
        """Process all input files."""
        results = []
        
        self.log.info(f"Processing {len(self.input_files)} file(s)...")
        
        if self.parallel and len(self.input_files) > 1:
            # Parallel processing
            self.log.info(f"Using parallel processing with {self.num_workers} workers")
            
            # Prepare arguments for each worker
            tasks = []
            for idx, input_file in enumerate(self.input_files):
                output_file = self._generate_output_filename(input_file, idx)
                tasks.append((input_file, output_file, self.subreddits, self.fields, 
                             self.progress_interval, self.verbose))
            
            # Run in parallel
            with Pool(processes=self.num_workers) as pool:
                results = pool.starmap(_process_file_worker, tasks)
            
        else:
            # Sequential processing
            for idx, input_file in enumerate(self.input_files):
                output_file = self._generate_output_filename(input_file, idx)
                result = self._process_single_file(input_file, output_file)
                results.append(result)
        
        # Calculate totals
        for result in results:
            self.total_lines += result['total_lines']
            self.matched_lines += result['matched_lines']
            self.bad_lines += result['bad_lines']
        
        # Print summary
        self.log.info("\n" + "=" * 80)
        self.log.info("SUMMARY FOR ALL FILES:")
        self.log.info(f"Total files processed: {len(results)}")
        self.log.info(f"Total lines: {self.total_lines:,}")
        self.log.info(f"Total matched: {self.matched_lines:,}")
        self.log.info(f"Total bad: {self.bad_lines:,}")
        if self.total_lines > 0:
            self.log.info(f"Overall match rate: {(self.matched_lines / self.total_lines * 100):.2f}%")
        self.log.info("=" * 80)
        
        return results
    
    @classmethod
    def run(cls, input_files: Union[str, List[str]], output_file: str = None, 
            subreddits: List[str] = None, fields: List[str] = None,
            progress_interval: int = 100000,
            verbose: bool = True,
            parallel: bool = False,
            num_workers: int = None) -> List[dict]:
        return cls(input_files, output_file, subreddits, fields, progress_interval, 
                   verbose, parallel, num_workers).process()
