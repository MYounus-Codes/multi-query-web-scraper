# Multi-Query Web Scraper

A Python-based web scraper that processes multiple queries from a text file and saves the results in JSONL format.

## Features
- Reads queries from `queries.txt`
- Scrapes web data for each query
- Stores results in `results.jsonl` (JSON Lines format)
- Easy to configure and extend

## Requirements
Install dependencies listed in `requirments.txt`:

```bash
pip install -r requirments.txt
```

## Usage
1. Add your queries to `queries.txt`, one per line.
2. Run the scraper:

```bash
python main.py
```
3. Results will be saved in `results.jsonl`.

## File Structure
- `main.py` — Main script for scraping
- `queries.txt` — List of queries to process
- `requirments.txt` — Python dependencies
- `results.jsonl` — Output file with scraped results

## Customization
- Modify `main.py` to change scraping logic or output format.
- Update `queries.txt` with your own queries.

## License
MIT License

## Author
MYounus-Codes
