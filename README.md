# Polymarket Market Browser

This project provides a minimal Streamlit app that displays current Polymarket markets using the public Gamma API. It is read-only and lets you filter markets by probability, hours to expiry, and open interest.

## Requirements

- Python 3.11+
- Packages from `requirements.txt`

## Running

Install dependencies and launch the app with Streamlit:

```bash
pip install -r requirements.txt
streamlit run app/main.py
```

The app fetches market data asynchronously and caches results in memory for 60 seconds to avoid excessive API calls.

# User-provided custom instructions
