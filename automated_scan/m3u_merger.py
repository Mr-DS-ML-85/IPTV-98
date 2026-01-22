#!/usr/bin/env python3
"""
M3U Playlist Merger - Merge M3U files with URL-based deduplication
Includes comprehensive URL validation and auditing
"""

import sys
import re
from pathlib import Path
from typing import List, Dict, Tuple, Optional
from urllib.parse import urlparse, unquote, quote
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import time


class URLValidator:
    """Validate and audit M3U URLs"""
    
    VALID_SCHEMES = {'http', 'https', 'rtmp', 'rtmps', 'rtsp', 'udp', 'rtp', 'mms', 'mmsh'}
    VALID_EXTENSIONS = {'.m3u8', '.ts', '.m3u', '.mp4', '.mkv', '.avi', '.flv', '.mp3', '.aac'}
    
    @staticmethod
    def validate_url(url: str) -> Tuple[bool, List[str]]:
        """
        Validate URL and return (is_valid, list_of_issues)
        """
        issues = []
        
        if not url or not url.strip():
            return False, ["Empty URL"]
        
        url = url.strip()
        
        # Check for common malformations
        if url.startswith('://'):
            issues.append("Missing protocol scheme")
            return False, issues
        
        # Check for spaces (should be encoded)
        if ' ' in url:
            issues.append("Contains unencoded spaces")
        
        # Check for invalid characters
        if any(char in url for char in ['\n', '\r', '\t']):
            issues.append("Contains newline or tab characters")
            return False, issues
        
        # Parse URL
        try:
            parsed = urlparse(url)
        except Exception as e:
            issues.append(f"URL parsing failed: {str(e)}")
            return False, issues
        
        # Validate scheme
        if not parsed.scheme:
            issues.append("Missing protocol (http://, https://, etc.)")
            return False, issues
        
        if parsed.scheme.lower() not in URLValidator.VALID_SCHEMES:
            issues.append(f"Unsupported protocol: {parsed.scheme}")
        
        # Validate netloc (domain/host)
        if not parsed.netloc and parsed.scheme in {'http', 'https'}:
            issues.append("Missing domain/host")
            return False, issues
        
        # Check for localhost/private IPs without warning (they're valid for local streaming)
        
        # Check for double slashes in path (common error)
        if '//' in parsed.path and not parsed.path.startswith('//'):
            issues.append("Double slashes in URL path")
        
        # Check for suspicious patterns
        if '..' in parsed.path:
            issues.append("Path traversal detected (..)")
        
        # Warn about non-standard ports for HTTP/HTTPS
        if parsed.scheme in {'http', 'https'} and parsed.port:
            if parsed.scheme == 'http' and parsed.port not in {80, 8080, 8000, 8888}:
                issues.append(f"Non-standard HTTP port: {parsed.port}")
            elif parsed.scheme == 'https' and parsed.port not in {443, 8443}:
                issues.append(f"Non-standard HTTPS port: {parsed.port}")
        
        # Check URL length (some players have limits)
        if len(url) > 2048:
            issues.append(f"URL too long ({len(url)} chars, max recommended: 2048)")
        
        return len(issues) == 0, issues
    
    @staticmethod
    def fix_url(url: str) -> str:
        """Attempt to fix common URL issues"""
        if not url:
            return url
        
        url = url.strip()
        
        # Fix unencoded spaces
        url = url.replace(' ', '%20')
        
        # Remove newlines and tabs
        url = url.replace('\n', '').replace('\r', '').replace('\t', '')
        
        # Fix missing http:// for URLs that look like they should have it
        if not url.startswith(('http://', 'https://', 'rtmp://', 'rtsp://', 'udp://', 'rtp://')):
            if '://' not in url and '.' in url:
                url = 'http://' + url
        
        # Fix double slashes in path (but not after protocol)
        parts = url.split('://', 1)
        if len(parts) == 2:
            protocol, rest = parts
            rest = re.sub(r'/+', '/', rest)
            url = f"{protocol}://{rest}"
        
        return url


class M3UEntry:
    """Represents a single entry in an M3U playlist"""
    
    def __init__(self, extinf_line: str = "", url: str = "", metadata: Dict = None):
        self.extinf_line = extinf_line.strip()
        self.url = url.strip()
        self.metadata = metadata or {}
        self.url_issues = []
        self.is_valid = True
        self.parse_extinf()
        self.validate_url()
    
    def parse_extinf(self):
        """Parse EXTINF line to extract metadata"""
        if not self.extinf_line.startswith('#EXTINF:'):
            return
        
        # Extract duration and title
        match = re.match(r'#EXTINF:([^,]*),(.*)', self.extinf_line)
        if match:
            duration_part = match.group(1).strip()
            title = match.group(2).strip()
            
            # Parse duration
            try:
                self.metadata['duration'] = float(duration_part.split()[0])
            except (ValueError, IndexError):
                self.metadata['duration'] = -1
            
            self.metadata['title'] = title
            
            # Extract additional attributes (tvg-id, tvg-name, group-title, etc.)
            attr_pattern = r'(\w+(?:-\w+)*)="([^"]*)"'
            for attr_match in re.finditer(attr_pattern, duration_part):
                key, value = attr_match.groups()
                self.metadata[key] = value
    
    def validate_url(self):
        """Validate the URL"""
        self.is_valid, self.url_issues = URLValidator.validate_url(self.url)
    
    def fix_url(self):
        """Attempt to fix URL issues"""
        original_url = self.url
        self.url = URLValidator.fix_url(self.url)
        if original_url != self.url:
            # Re-validate after fixing
            self.validate_url()
            return True
        return False
    
    def get_unique_key(self) -> str:
        """Generate a unique key for deduplication - URL ONLY"""
        # Normalize URL for comparison (case-insensitive, trimmed)
        normalized_url = self.url.lower().strip()
        
        # Remove common trailing parameters that don't affect the stream
        # but keep the core URL intact
        normalized_url = re.sub(r'[?&](token|auth|session|time|timestamp)=[^&]*', '', normalized_url)
        
        # Remove trailing slashes for consistency
        normalized_url = normalized_url.rstrip('/')
        
        return normalized_url
    
    def __str__(self) -> str:
        """Return M3U format string"""
        if self.extinf_line:
            return f"{self.extinf_line}\n{self.url}"
        return self.url
    
    def __repr__(self) -> str:
        return f"M3UEntry(title={self.metadata.get('title', 'N/A')}, url={self.url[:50]}...)"


class M3UMerger:
    """Merge multiple M3U playlist files intelligently with multi-threading"""
    
    def __init__(self, conflict_strategy: str = "first", auto_fix: bool = True, 
                 remove_invalid: bool = False, max_workers: int = 200):
        """
        Initialize merger with conflict resolution strategy
        
        Args:
            conflict_strategy: How to handle duplicates
                - "first": Keep entry from first file
                - "last": Keep entry from last file
                - "longest": Keep entry with most metadata
            auto_fix: Automatically fix common URL issues
            remove_invalid: Remove entries with invalid URLs
            max_workers: Maximum concurrent threads (default: 200)
        """
        self.conflict_strategy = conflict_strategy
        self.auto_fix = auto_fix
        self.remove_invalid = remove_invalid
        self.max_workers = max_workers
        self.entries: Dict[str, M3UEntry] = {}
        self.header_lines: List[str] = []
        self.invalid_entries: List[M3UEntry] = []
        self.fixed_entries: List[Tuple[str, str]] = []
        self.stats = {
            'total_processed': 0,
            'valid': 0,
            'invalid': 0,
            'fixed': 0,
            'duplicates': 0
        }
        # Thread-safe locks
        self.entries_lock = Lock()
        self.invalid_lock = Lock()
        self.fixed_lock = Lock()
        self.stats_lock = Lock()
    
    def parse_m3u_file(self, filepath: str) -> List[M3UEntry]:
        """Parse an M3U file and return list of entries"""
        entries = []
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                lines = f.readlines()
        except UnicodeDecodeError:
            # Try with different encoding
            try:
                with open(filepath, 'r', encoding='latin-1') as f:
                    lines = f.readlines()
            except Exception as e:
                print(f"Error reading file {filepath}: {e}")
                return entries
        
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            
            # Handle M3U header
            if line.startswith('#EXTM3U'):
                if not self.header_lines and line not in self.header_lines:
                    self.header_lines.append(line)
                i += 1
                continue
            
            # Handle other comments/metadata (but not EXTINF)
            if line.startswith('#') and not line.startswith('#EXTINF:'):
                i += 1
                continue
            
            # Handle EXTINF entries
            if line.startswith('#EXTINF:'):
                extinf_line = line
                # Get the URL from next non-comment line
                i += 1
                while i < len(lines):
                    url_line = lines[i].strip()
                    if url_line and not url_line.startswith('#'):
                        entries.append(M3UEntry(extinf_line, url_line))
                        break
                    i += 1
            elif line and not line.startswith('#'):
                # URL without EXTINF
                entries.append(M3UEntry("", line))
            
            i += 1
        
        return entries
    
    def process_entry(self, entry: M3UEntry) -> bool:
        """Process a single entry with validation and fixing (thread-safe)"""
        with self.stats_lock:
            self.stats['total_processed'] += 1
        
        # Try to fix URL if auto-fix is enabled
        if self.auto_fix and not entry.is_valid:
            original_url = entry.url
            if entry.fix_url():
                with self.fixed_lock:
                    self.fixed_entries.append((original_url, entry.url))
                with self.stats_lock:
                    self.stats['fixed'] += 1
        
        # Check if still invalid after fix attempt
        if not entry.is_valid:
            with self.invalid_lock:
                self.invalid_entries.append(entry)
            with self.stats_lock:
                self.stats['invalid'] += 1
            if self.remove_invalid:
                return False  # Don't add this entry
        else:
            with self.stats_lock:
                self.stats['valid'] += 1
        
        return True
    
    def process_and_add_entry(self, entry: M3UEntry) -> Tuple[bool, Optional[str]]:
        """Process a single entry and return (should_add, unique_key)"""
        if not self.process_entry(entry):
            return False, None  # Skip invalid entries if remove_invalid is True
        
        key = entry.get_unique_key()
        return True, key
    
    def add_entries(self, entries: List[M3UEntry], priority: int = 0):
        """Add entries to merger with conflict resolution based on URL only (multi-threaded)"""
        if not entries:
            return
        
        print(f"      Processing {len(entries)} entries with {self.max_workers} threads...")
        start_time = time.time()
        
        # Process entries in parallel
        processed_entries = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_entry = {
                executor.submit(self.process_and_add_entry, entry): entry 
                for entry in entries
            }
            
            # Collect results with progress indicator
            completed = 0
            total = len(future_to_entry)
            
            for future in as_completed(future_to_entry):
                entry = future_to_entry[future]
                try:
                    should_add, key = future.result()
                    if should_add and key:
                        processed_entries.append((key, entry))
                    
                    completed += 1
                    # Show progress every 10%
                    if completed % max(1, total // 10) == 0:
                        progress = (completed / total) * 100
                        print(f"      Progress: {completed}/{total} ({progress:.1f}%)", end='\r')
                
                except Exception as e:
                    print(f"\n      Warning: Error processing entry: {e}")
        
        print(f"      Progress: {total}/{total} (100.0%) - Complete!     ")
        
        # Now add all processed entries to the main dictionary (single-threaded for consistency)
        for key, entry in processed_entries:
            with self.entries_lock:
                if key not in self.entries:
                    # New entry, add it
                    self.entries[key] = entry
                else:
                    # URL conflict detected (exact same URL), resolve based on strategy
                    with self.stats_lock:
                        self.stats['duplicates'] += 1
                    existing = self.entries[key]
                    
                    if self.conflict_strategy == "last":
                        self.entries[key] = entry
                    elif self.conflict_strategy == "longest":
                        # Keep entry with more metadata
                        if len(entry.metadata) > len(existing.metadata):
                            self.entries[key] = entry
                    # "first" strategy: do nothing, keep existing
        
        elapsed_time = time.time() - start_time
        print(f"      Completed in {elapsed_time:.2f} seconds")
    
    def merge_files(self, file1: str, file2: str) -> List[M3UEntry]:
        """Merge two M3U files with multi-threaded processing"""
        print("=" * 70)
        print("M3U MERGER WITH URL VALIDATION (MULTI-THREADED)")
        print("=" * 70)
        print(f"Max concurrent threads: {self.max_workers}")
        
        print(f"\n[1/4] Parsing {Path(file1).name}...")
        entries1 = self.parse_m3u_file(file1)
        print(f"      Found {len(entries1)} entries")
        
        print(f"\n[2/4] Parsing {Path(file2).name}...")
        entries2 = self.parse_m3u_file(file2)
        print(f"      Found {len(entries2)} entries")
        
        print(f"\n[3/4] Validating and merging with '{self.conflict_strategy}' strategy...")
        if self.auto_fix:
            print("      Auto-fix enabled: attempting to repair malformed URLs")
        if self.remove_invalid:
            print("      Remove invalid: entries with bad URLs will be excluded")
        
        print(f"\n      Processing first file...")
        self.add_entries(entries1, priority=1)
        
        print(f"\n      Processing second file...")
        self.add_entries(entries2, priority=2)
        
        print(f"\n[4/4] Merge complete!")
        print(f"      Total entries processed: {self.stats['total_processed']}")
        print(f"      ✓ Valid URLs: {self.stats['valid']}")
        print(f"      ✗ Invalid URLs: {self.stats['invalid']}")
        print(f"      🔧 Fixed URLs: {self.stats['fixed']}")
        print(f"      ♻ Duplicate URLs removed: {self.stats['duplicates']}")
        print(f"      → Final unique entries: {len(self.entries)}")
        
        return list(self.entries.values())
    
    def write_m3u_file(self, output_path: str, entries: List[M3UEntry]):
        """Write merged entries to M3U file"""
        with open(output_path, 'w', encoding='utf-8') as f:
            # Write header
            if self.header_lines:
                f.write('\n'.join(self.header_lines) + '\n\n')
            else:
                f.write('#EXTM3U\n\n')
            
            # Write entries
            for entry in entries:
                f.write(str(entry) + '\n')
        
        print(f"\n✓ Output written to: {output_path}")
    
    def write_audit_report(self, output_path: str):
        """Write detailed audit report of URL issues"""
        report_path = output_path.replace('.m3u', '_audit_report.txt')
        
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write("=" * 70 + "\n")
            f.write("M3U PLAYLIST AUDIT REPORT\n")
            f.write("=" * 70 + "\n\n")
            
            # Summary statistics
            f.write("SUMMARY\n")
            f.write("-" * 70 + "\n")
            f.write(f"Total entries processed: {self.stats['total_processed']}\n")
            f.write(f"Valid URLs: {self.stats['valid']}\n")
            f.write(f"Invalid URLs: {self.stats['invalid']}\n")
            f.write(f"Fixed URLs: {self.stats['fixed']}\n")
            f.write(f"Duplicate URLs: {self.stats['duplicates']}\n")
            f.write(f"Final unique entries: {len(self.entries)}\n\n")
            
            # Fixed URLs
            if self.fixed_entries:
                f.write("FIXED URLS\n")
                f.write("-" * 70 + "\n")
                for i, (original, fixed) in enumerate(self.fixed_entries, 1):
                    f.write(f"{i}. ORIGINAL: {original}\n")
                    f.write(f"   FIXED:    {fixed}\n\n")
            
            # Invalid entries
            if self.invalid_entries:
                f.write("\nINVALID URLS (ISSUES DETECTED)\n")
                f.write("-" * 70 + "\n")
                for i, entry in enumerate(self.invalid_entries, 1):
                    title = entry.metadata.get('title', 'No Title')
                    f.write(f"{i}. {title}\n")
                    f.write(f"   URL: {entry.url}\n")
                    f.write(f"   Issues:\n")
                    for issue in entry.url_issues:
                        f.write(f"     - {issue}\n")
                    f.write("\n")
            
            # Group titles summary
            f.write("\nCHANNEL GROUPS SUMMARY\n")
            f.write("-" * 70 + "\n")
            groups = defaultdict(int)
            for entry in self.entries.values():
                group = entry.metadata.get('group-title', 'Uncategorized')
                groups[group] += 1
            
            for group, count in sorted(groups.items(), key=lambda x: x[1], reverse=True):
                f.write(f"{group}: {count} channels\n")
        
        print(f"✓ Audit report written to: {report_path}")


def main():
    """Main function"""
    if len(sys.argv) < 3:
        print("=" * 70)
        print("M3U PLAYLIST MERGER WITH URL VALIDATION (MULTI-THREADED)")
        print("=" * 70)
        print("\nUsage:")
        print("  python m3u_merger.py <file1.m3u> <file2.m3u> [options]")
        print("\nRequired Arguments:")
        print("  file1.m3u    : First M3U playlist file")
        print("  file2.m3u    : Second M3U playlist file")
        print("\nOptional Arguments:")
        print("  output.m3u   : Output file (default: merged.m3u)")
        print("  strategy     : Conflict resolution (default: first)")
        print("                 Options: first, last, longest")
        print("  --threads=N  : Max concurrent threads (default: 200, max: 500)")
        print("  --no-fix     : Disable automatic URL fixing")
        print("  --remove-invalid : Remove entries with invalid URLs")
        print("\nExamples:")
        print("  python m3u_merger.py playlist1.m3u playlist2.m3u")
        print("  python m3u_merger.py old.m3u new.m3u merged.m3u last")
        print("  python m3u_merger.py file1.m3u file2.m3u out.m3u --threads=100")
        print("  python m3u_merger.py f1.m3u f2.m3u output.m3u first --remove-invalid")
        print("\nFeatures:")
        print("  ⚡ Multi-threaded processing (up to 200 concurrent threads)")
        print("  ✓ URL validation and auditing")
        print("  ✓ Automatic URL fixing (spaces, protocol, etc.)")
        print("  ✓ Keeps backup URLs (same name, different URLs)")
        print("  ✓ Removes only exact duplicate URLs")
        print("  ✓ Generates detailed audit report")
        print("  ✓ Real-time progress indicator")
        sys.exit(1)
    
    file1 = sys.argv[1]
    file2 = sys.argv[2]
    
    # Parse arguments
    output = "merged.m3u"
    strategy = "first"
    auto_fix = True
    remove_invalid = False
    max_workers = 200
    
    i = 3
    while i < len(sys.argv):
        arg = sys.argv[i]
        if arg == "--no-fix":
            auto_fix = False
        elif arg == "--remove-invalid":
            remove_invalid = True
        elif arg.startswith("--threads="):
            try:
                max_workers = int(arg.split("=")[1])
                if max_workers < 1 or max_workers > 500:
                    print("Warning: threads must be between 1-500, using default 200")
                    max_workers = 200
            except ValueError:
                print("Warning: invalid threads value, using default 200")
        elif arg.endswith('.m3u'):
            output = arg
        elif arg in ["first", "last", "longest"]:
            strategy = arg
        i += 1
    
    # Validate strategy
    if strategy not in ["first", "last", "longest"]:
        print(f"Error: Invalid strategy '{strategy}'")
        print("Valid strategies: first, last, longest")
        sys.exit(1)
    
    # Check if files exist
    if not Path(file1).exists():
        print(f"Error: File not found: {file1}")
        sys.exit(1)
    
    if not Path(file2).exists():
        print(f"Error: File not found: {file2}")
        sys.exit(1)
    
    # Create merger and process files
    merger = M3UMerger(
        conflict_strategy=strategy,
        auto_fix=auto_fix,
        remove_invalid=remove_invalid,
        max_workers=max_workers
    )
    
    entries = merger.merge_files(file1, file2)
    merger.write_m3u_file(output, entries)
    merger.write_audit_report(output)
    
    print("\n" + "=" * 70)
    print("✓ MERGE COMPLETED SUCCESSFULLY!")
    print("=" * 70)
    
    if merger.stats['invalid'] > 0:
        print(f"\n⚠ Warning: {merger.stats['invalid']} entries have URL issues.")
        print(f"  Check the audit report for details.")
        if not remove_invalid:
            print(f"  Use --remove-invalid to exclude them from output.")


if __name__ == "__main__":
    main()
