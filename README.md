# Custom news feed (RSS.app replacement)

Runs free on GitHub Actions every 30 minutes and publishes:

- `feed.xml` - one combined RSS feed of every source (use this URL anywhere you used the RSS.app feed)
- `index.html` - a searchable reader, filterable by tag, that marks what's new since your last visit
- `status.json` - which sources worked on the last run and which failed

## Setup (about 10 minutes)

1. Create a new **public** GitHub repo and upload everything in this folder (including the hidden `.github` and `.state` folders).
2. Repo **Settings > Actions > General > Workflow permissions**: choose **Read and write permissions**, save.
3. Repo **Settings > Pages**: Source = **Deploy from a branch**, Branch = `main`, folder = `/docs`, save.
4. Edit `feeds.yaml`: set `site_url`, put your email in `user_agent` (SEC requires it), and add your sources.
5. **Actions** tab > **Build news feed** > **Run workflow**.

Your reader is then at `https://<username>.github.io/<repo>/` and the feed at `.../feed.xml`.

## Adding sources

See the comments in `feeds.yaml`. Three types:
- `rss` - any feed URL (most news sites, Substacks, SEC EDGAR, many IR pages)
- `google_news` - any Google News search, operators included
- `scrape` - a page with no feed, read with CSS selectors

## Run locally

    pip install -r requirements.txt
    python build_feed.py
    cd docs && python -m http.server   # open http://localhost:8000
