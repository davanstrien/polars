#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "httpx",
#     "rich",
# ]
# ///
"""
Benchmark script to compare HuggingFace Hub API approaches for file listing.

Tests three approaches:
1. Current: Multiple Tree API calls per directory
2. Siblings: Single call to repo info with expand=siblings
3. Recursive Tree: Single call with ?recursive=true (paginated)

Usage:
    uv run benchmark_hf_api.py
"""

import time
import fnmatch
import re
from collections import defaultdict
from dataclasses import dataclass
from typing import Optional

import httpx
from rich.console import Console
from rich.table import Table

console = Console()

# Test configuration
REPO_TYPE = "datasets"
# finepdfs-edu has structure: data/{lang}_Latn/train/*.parquet
TEST_REPO = "HuggingFaceFW/finepdfs-edu"
GLOB_PATTERN = "data/*_Latn/train/*.parquet"  # Only Latin script languages


@dataclass
class BenchmarkResult:
    approach: str
    api_calls: int
    total_time_ms: float
    files_found: int
    bytes_transferred: int
    error: Optional[str] = None


def glob_to_regex(pattern: str) -> re.Pattern:
    """Convert glob pattern to regex for matching."""
    # Escape special regex chars except * and ?
    escaped = ""
    i = 0
    while i < len(pattern):
        c = pattern[i]
        if c == "*":
            if i + 1 < len(pattern) and pattern[i + 1] == "*":
                # ** matches any path
                escaped += ".*"
                i += 2
                # Skip trailing /
                if i < len(pattern) and pattern[i] == "/":
                    i += 1
                continue
            else:
                # * matches within directory
                escaped += "[^/]*"
        elif c == "?":
            escaped += "[^/]"
        elif c in ".^$+{}[]|()":
            escaped += "\\" + c
        else:
            escaped += c
        i += 1
    return re.compile(f"^{escaped}$")


def benchmark_current_approach(client: httpx.Client, repo: str, glob_pattern: str) -> BenchmarkResult:
    """
    Current Polars approach: Tree API call per directory.
    Simulates stack-based traversal.
    """
    api_calls = 0
    bytes_transferred = 0
    files = []

    # Extract prefix from glob (everything before first wildcard)
    prefix = ""
    for i, c in enumerate(glob_pattern):
        if c in "*?[":
            break
        prefix += c
    prefix = prefix.rstrip("/")

    regex = glob_to_regex(glob_pattern)

    start = time.perf_counter()

    try:
        # Stack-based traversal like current implementation
        stack = [prefix] if prefix else [""]

        while stack:
            path = stack.pop()
            url = f"https://huggingface.co/api/{REPO_TYPE}/{repo}/tree/main/{path}"

            # Handle pagination
            while url:
                api_calls += 1
                resp = client.get(url)
                resp.raise_for_status()
                bytes_transferred += len(resp.content)

                entries = resp.json()

                for entry in entries:
                    entry_path = entry["path"]
                    entry_type = entry["type"]

                    if entry_type == "directory":
                        # Check if directory could contain matches
                        stack.append(entry_path)
                    elif entry_type == "file" and entry.get("size", 0) > 0:
                        if regex.match(entry_path):
                            files.append(entry_path)

                # Check for pagination
                link_header = resp.headers.get("link", "")
                url = None
                if 'rel="next"' in link_header:
                    # Parse next URL from link header
                    for part in link_header.split(","):
                        if 'rel="next"' in part:
                            url = part.split(";")[0].strip().strip("<>")
                            break

        elapsed_ms = (time.perf_counter() - start) * 1000
        return BenchmarkResult(
            approach="Current (Tree per dir)",
            api_calls=api_calls,
            total_time_ms=elapsed_ms,
            files_found=len(files),
            bytes_transferred=bytes_transferred,
        )
    except Exception as e:
        elapsed_ms = (time.perf_counter() - start) * 1000
        return BenchmarkResult(
            approach="Current (Tree per dir)",
            api_calls=api_calls,
            total_time_ms=elapsed_ms,
            files_found=len(files),
            bytes_transferred=bytes_transferred,
            error=str(e),
        )


def benchmark_siblings_approach(client: httpx.Client, repo: str, glob_pattern: str) -> BenchmarkResult:
    """
    Siblings approach: Single API call to get all files.
    """
    api_calls = 0
    bytes_transferred = 0
    files = []

    regex = glob_to_regex(glob_pattern)

    start = time.perf_counter()

    try:
        url = f"https://huggingface.co/api/{REPO_TYPE}/{repo}?expand=siblings"
        api_calls += 1
        resp = client.get(url)
        resp.raise_for_status()
        bytes_transferred += len(resp.content)

        data = resp.json()
        siblings = data.get("siblings", [])

        for sibling in siblings:
            rfilename = sibling.get("rfilename", "")
            if regex.match(rfilename):
                files.append(rfilename)

        elapsed_ms = (time.perf_counter() - start) * 1000
        return BenchmarkResult(
            approach="Siblings API",
            api_calls=api_calls,
            total_time_ms=elapsed_ms,
            files_found=len(files),
            bytes_transferred=bytes_transferred,
        )
    except Exception as e:
        elapsed_ms = (time.perf_counter() - start) * 1000
        return BenchmarkResult(
            approach="Siblings API",
            api_calls=api_calls,
            total_time_ms=elapsed_ms,
            files_found=len(files),
            bytes_transferred=bytes_transferred,
            error=str(e),
        )


def benchmark_recursive_tree_approach(client: httpx.Client, repo: str, glob_pattern: str) -> BenchmarkResult:
    """
    Recursive Tree approach: Single API call with ?recursive=true.
    """
    api_calls = 0
    bytes_transferred = 0
    files = []

    # Extract prefix from glob
    prefix = ""
    for i, c in enumerate(glob_pattern):
        if c in "*?[":
            break
        prefix += c
    prefix = prefix.rstrip("/")

    regex = glob_to_regex(glob_pattern)

    start = time.perf_counter()

    try:
        url = f"https://huggingface.co/api/{REPO_TYPE}/{repo}/tree/main/{prefix}?recursive=true"

        while url:
            api_calls += 1
            resp = client.get(url)
            resp.raise_for_status()
            bytes_transferred += len(resp.content)

            entries = resp.json()

            for entry in entries:
                entry_path = entry["path"]
                entry_type = entry["type"]

                if entry_type == "file" and entry.get("size", 0) > 0:
                    if regex.match(entry_path):
                        files.append(entry_path)

            # Check for pagination
            link_header = resp.headers.get("link", "")
            url = None
            if 'rel="next"' in link_header:
                for part in link_header.split(","):
                    if 'rel="next"' in part:
                        url = part.split(";")[0].strip().strip("<>")
                        break

        elapsed_ms = (time.perf_counter() - start) * 1000
        return BenchmarkResult(
            approach="Recursive Tree API",
            api_calls=api_calls,
            total_time_ms=elapsed_ms,
            files_found=len(files),
            bytes_transferred=bytes_transferred,
        )
    except Exception as e:
        elapsed_ms = (time.perf_counter() - start) * 1000
        return BenchmarkResult(
            approach="Recursive Tree API",
            api_calls=api_calls,
            total_time_ms=elapsed_ms,
            files_found=len(files),
            bytes_transferred=bytes_transferred,
            error=str(e),
        )


def main():
    console.print(f"\n[bold blue]HuggingFace Hub API Benchmark[/bold blue]")
    console.print(f"Repository: [cyan]{TEST_REPO}[/cyan]")
    console.print(f"Glob pattern: [cyan]{GLOB_PATTERN}[/cyan]\n")

    # Use a single client with connection pooling
    with httpx.Client(timeout=60.0) as client:
        results = []

        # Warm up - make one request to establish connection
        console.print("[dim]Warming up connection...[/dim]")
        client.get(f"https://huggingface.co/api/{REPO_TYPE}/{TEST_REPO}")

        # Run benchmarks
        console.print("[dim]Running Siblings API benchmark...[/dim]")
        results.append(benchmark_siblings_approach(client, TEST_REPO, GLOB_PATTERN))

        console.print("[dim]Running Recursive Tree API benchmark...[/dim]")
        results.append(benchmark_recursive_tree_approach(client, TEST_REPO, GLOB_PATTERN))

        console.print("[dim]Running Current (Tree per dir) benchmark...[/dim]")
        results.append(benchmark_current_approach(client, TEST_REPO, GLOB_PATTERN))

    # Display results
    table = Table(title="Benchmark Results")
    table.add_column("Approach", style="cyan")
    table.add_column("API Calls", justify="right")
    table.add_column("Time (ms)", justify="right")
    table.add_column("Files Found", justify="right")
    table.add_column("Bytes", justify="right")
    table.add_column("Status", justify="center")

    for r in results:
        status = "[green]OK[/green]" if not r.error else f"[red]{r.error[:20]}...[/red]"
        table.add_row(
            r.approach,
            str(r.api_calls),
            f"{r.total_time_ms:.1f}",
            str(r.files_found),
            f"{r.bytes_transferred:,}",
            status,
        )

    console.print(table)

    # Summary
    console.print("\n[bold]Summary:[/bold]")

    current = next((r for r in results if "Current" in r.approach), None)
    siblings = next((r for r in results if "Siblings" in r.approach), None)
    recursive = next((r for r in results if "Recursive" in r.approach), None)

    if current and siblings and not siblings.error:
        speedup = current.total_time_ms / siblings.total_time_ms if siblings.total_time_ms > 0 else 0
        call_reduction = current.api_calls / siblings.api_calls if siblings.api_calls > 0 else 0
        console.print(f"  Siblings vs Current: [green]{speedup:.1f}x faster[/green], [green]{call_reduction:.0f}x fewer API calls[/green]")

    if current and recursive and not recursive.error:
        speedup = current.total_time_ms / recursive.total_time_ms if recursive.total_time_ms > 0 else 0
        call_reduction = current.api_calls / recursive.api_calls if recursive.api_calls > 0 else 0
        console.print(f"  Recursive vs Current: [green]{speedup:.1f}x faster[/green], [green]{call_reduction:.0f}x fewer API calls[/green]")

    if siblings and recursive and not siblings.error and not recursive.error:
        console.print(f"\n[bold]Recommendation:[/bold]")
        if siblings.total_time_ms < recursive.total_time_ms:
            console.print(f"  [cyan]Siblings API[/cyan] is faster ({siblings.total_time_ms:.0f}ms vs {recursive.total_time_ms:.0f}ms)")
            console.print(f"  Note: Siblings API does [yellow]not include file sizes[/yellow]")
        else:
            console.print(f"  [cyan]Recursive Tree API[/cyan] is faster ({recursive.total_time_ms:.0f}ms vs {siblings.total_time_ms:.0f}ms)")
            console.print(f"  Bonus: Recursive Tree API [green]includes file sizes[/green]")


if __name__ == "__main__":
    main()
