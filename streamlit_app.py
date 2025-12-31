#!/usr/bin/env python3
"""
Streamlit app: manage SEC filing URLs in Google Sheet.
- Upload service account JSON and GPT API key text.
- View/add/delete URLs in worksheet `ceo/cfo`; changes persist to the sheet.
Run: streamlit run streamlit_app.py
"""

import io
import json
import re
import html
import warnings
import requests
import subprocess
import time
from datetime import datetime
from typing import List, Tuple, Optional

import streamlit as st
import gspread
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
from google.oauth2.service_account import Credentials
from openai import OpenAI

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1pg2Rb-6DbmSdX4Xbu1MQO3z3QW-RI1fPxLw5lUyFFno"
WORKSHEET_NAME = "ceo/cfo"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# ------------------------------
# Helpers
# ------------------------------

def normalize_urls(raw: str) -> List[str]:
    urls = []
    for line in raw.splitlines():
        u = line.strip()
        if not u:
            continue
        if u.startswith("http"):
            urls.append(u)
        elif "sec.gov" in u.lower():
            urls.append(f"https://{u.lstrip('/')}")
    # dedupe, keep order
    seen = set()
    deduped = []
    for u in urls:
        k = u.lower()
        if k not in seen:
            deduped.append(u)
            seen.add(k)
    return deduped


def connect_sheet(sa_info: dict):
    creds = Credentials.from_service_account_info(sa_info, scopes=SCOPES)
    gc = gspread.authorize(creds)
    sh = gc.open_by_url(SPREADSHEET_URL)
    ws = sh.worksheet(WORKSHEET_NAME)
    return ws


def load_urls(ws) -> List[str]:
    values = ws.get_all_values()
    urls = []
    for row in values:
        if not row:
            continue
        url = row[0].strip()
        if url:
            urls.append(url)
    return urls


def rewrite_urls(ws, urls: List[str]) -> bool:
    try:
        ws.clear()
        if urls:
            data = [[u] for u in urls]
            ws.update(range_name="A1", values=data, value_input_option="RAW")
        return True
    except Exception as e:
        st.error(f"Write failed: {e}")
        return False


def normalize_text(txt: str) -> str:
    if not txt:
        return ""
    txt = html.unescape(txt)
    txt = txt.replace("\u00a0", " ")
    txt = txt.replace("\u2007", " ")
    txt = txt.replace("\u2009", " ")
    txt = txt.replace("\u202f", " ")
    txt = txt.replace("\u2002", " ")
    txt = txt.replace("\u2003", " ")
    txt = txt.replace("\u200a", " ")
    txt = re.sub(r"[\t\r]+", " ", txt)
    txt = re.sub(r"\s+", " ", txt)
    return txt.strip()


def fetch_filing(url: str) -> Optional[str]:
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; SEC-Extractor/1.0)",
        "Accept": "text/html,application/xhtml+xml",
    }

    # Normalize ix?doc URLs
    if "ix?doc=" in url:
        match = re.search(r"ix\?doc=(/.+)", url)
        if match:
            url = f"https://www.sec.gov{match.group(1)}"

    try:
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        html_text = resp.text

        soup = BeautifulSoup(html_text, "lxml")

        # If this is a wrapper page, find the real filing
        iframe = soup.find("iframe")
        if iframe and iframe.get("src"):
            real_url = iframe["src"]
            if real_url.startswith("/"):
                real_url = "https://www.sec.gov" + real_url

            resp2 = requests.get(real_url, headers=headers, timeout=30)
            resp2.raise_for_status()
            return resp2.text

        return html_text

    except Exception as e:
        st.error(f"Fetch failed for {url}: {e}")
        return None



def parse_cik_from_url(url: str) -> Optional[str]:
    patterns = [
        r"/data/0*([0-9]{5,10})/",
        r"[?&]CIK=0*([0-9]{5,10})",
        r"[?&]cik=0*([0-9]{5,10})",
        r"CIK%3D0*([0-9]{5,10})",
    ]
    for pat in patterns:
        m = re.search(pat, url)
        if m:
            return m.group(1).zfill(10)
    return None


def has_item_502(html_text: str) -> bool:
    soup = BeautifulSoup(html_text, "lxml")
    for tag in soup(["script", "style"]):
        tag.decompose()
    text = soup.get_text("\n", strip=True)
    text = text.replace("\u00a0", " ")
    text = re.sub(r"\s+", " ", text)
    return bool(re.search(r"\bitem\s*5[\.\s]*0?2\b", text, re.IGNORECASE))



def extract_ticker_from_filing_tables(soup: BeautifulSoup, raw_html: str) -> str:
    tables = soup.find_all("table")
    for table in tables:
        table_html = str(table).lower()
        if any(phrase in table_html for phrase in ["trading symbol", "trading symbols", "symbol(s)", "name of each exchange"]):
            rows = table.find_all("tr")
            for row in rows:
                cells = row.find_all(["td", "th"])
                texts = [c.get_text(strip=True) for c in cells]
                for cell_text in texts:
                    if 1 <= len(cell_text) <= 5 and cell_text.isupper() and cell_text.isalpha():
                        context = " ".join(texts).lower()
                        if any(x in context for x in ["exchange", "nyse", "nasdaq", "otc", "trading"]):
                            return cell_text
    regex_patterns = [
        r"Trading\s+Symbol(?:\(s\))?[^|>]*[|>]\s*([A-Z]{1,5})\b",
        r"Title of each class[^|>]*[|>][^|>]*[|>]\s*([A-Z]{1,5})\b",
        r"Common Stock[^|>]*[|>]\s*([A-Z]{1,5})\b",
    ]
    for pat in regex_patterns:
        hits = re.findall(pat, raw_html, re.IGNORECASE)
        for h in hits:
            if h and h.isalpha() and 1 <= len(h) <= 5:
                return h
    return "UNKN"


def extract_company_info_from_filing(url: str, html_text: Optional[str] = None) -> Tuple[str, str]:
    """Get company name (and ticker) using primarily the URL (CIK-based lookup), with HTML fallback if needed."""
    company_name = None
    ticker = "UNKN"

    cik = parse_cik_from_url(url)

    # Primary: SEC submissions JSON via CIK
    if cik:
        headers = {
            "User-Agent": "Mozilla/5.0 (compatible; SEC-Extractor/1.0)",
            "Accept": "application/json",
        }
        try:
            resp = requests.get(f"https://data.sec.gov/submissions/CIK{cik}.json", headers=headers, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            company_name = data.get("name") or company_name
            tickers = data.get("tickers") or []
            ticker = tickers[0] if tickers else ticker
        except Exception:
            pass

    # Fallback: parse HTML for company name if still missing
    if not company_name and html_text:
        soup = BeautifulSoup(html_text, "lxml")
        xbrl = soup.find(["dei:entityregistrantname", "entityregistrantname"])
        if xbrl:
            company_name = xbrl.get_text(strip=True)
        if not company_name:
            header = soup.find("span", class_="companyName")
            if header:
                company_name = re.split(r"\(|CIK#", header.get_text())[0].strip()
        if ticker == "UNKN":
            ticker = extract_ticker_from_filing_tables(soup, html_text)

    return company_name or "Company Not Found", ticker



def keyword_extract_with_regex(text: str) -> str:
    text = normalize_text(text)
    verb_patterns = [
        r"appointed", r"named", r"promoted", r"demoted", r"elected", r"resigned", r"resigns", r"retired", r"steps down",
        r"assumed the role", r"assumed the roles", r"will become", r"was promoted", r"was demoted",
    ]
    # Require a verb; titles alone are insufficient to trigger extraction
    all_patterns = verb_patterns
    regex = re.compile(r"(" + r"|".join(all_patterns) + r")(?:[\s,:;\.\-])", re.IGNORECASE)
    matches_all = list(regex.finditer(text))
    # Filter out matches that are part of "continue to serve as" (not appointments)
    matches = []
    for m in matches_all:
        phrase = m.group(1).lower()
        before = text[max(0, m.start()-30):m.start()].lower()
        if "serve as" in phrase and "continue" in before:
            continue
        matches.append(m)

    if not matches:
        return ""

    words = text.split()
    char_to_word_index = []
    idx = 0
    for i, w in enumerate(words):
        char_to_word_index.append((idx, i))
        idx += len(w) + 1

    def charpos_to_wordidx(charpos: int) -> int:
        lo, hi = 0, len(char_to_word_index) - 1
        while lo <= hi:
            mid = (lo + hi) // 2
            start_char, word_idx = char_to_word_index[mid]
            if start_char <= charpos:
                if mid == len(char_to_word_index) - 1 or char_to_word_index[mid + 1][0] > charpos:
                    return word_idx
                lo = mid + 1
            else:
                hi = mid - 1
        return 0

    grouped = []
    current = []
    for m in matches:
        word_idx = charpos_to_wordidx(m.start())
        if not current:
            current.append((word_idx, m))
            continue
        last_idx = current[-1][0]
        if word_idx - last_idx <= 200:
            current.append((word_idx, m))
        else:
            grouped.append(current)
            current = [(word_idx, m)]
    if current:
        grouped.append(current)

    sections = []
    for grp in grouped:
        first_word = grp[0][0]
        last_word = grp[-1][0]
        start = max(0, first_word - 150)
        end = min(len(words), last_word + 150)
        window_words = words[start:end]
        window_text = " ".join(window_words)
        for _, match in grp:
            phrase = match.group(1)
            window_text = re.sub(re.escape(phrase), f"【{phrase.upper()}】", window_text, flags=re.IGNORECASE, count=1)
        sections.append(window_text)

    result = "\n\n--- SECTION BREAK ---\n\n".join(sections)
    return result


def truncate_after_signatures(text: str) -> str:
    pattern = re.compile(r"^\s*signatures\s*$", re.IGNORECASE | re.MULTILINE)
    m = pattern.search(text)
    if m:
        return text[:m.start()].strip()
    return text


def extract_executive_sections(html_text: str) -> str:
    soup = BeautifulSoup(html_text, "lxml")
    for tag in soup(["script", "style"]):
        tag.decompose()
    full_text_raw = soup.get_text(separator="\n", strip=True)
    full_text_raw = truncate_after_signatures(full_text_raw)
    full_text = normalize_text(full_text_raw)
    return keyword_extract_with_regex(full_text)


def extract_with_gpt_for_timeline(text: str, client: OpenAI):
    text = normalize_text(text)
    user_prompt = f"""Extract ALL executive appointment or resignation events from this SEC filing text.
Return JSON list sorted by date (earliest first). Extract ONLY if the position is CEO / Chief Executive Officer (including interim/acting/co) or CFO / Chief Financial Officer (including interim/acting/co). Each item must be:
{{"person_name": "Full Name", "position": "Exact Position", "action": "Appointed" or "Resigned", "date": "YYYY-MM-DD or None"}}
If no matching events, return [].

Text:
{text}
"""
    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are an expert SEC filing analyst."},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.0,
            max_tokens=700,
        )
        content = response.choices[0].message.content.strip()
        tokens = response.usage.total_tokens
        m = re.search(r"\[.*\]", content, re.DOTALL)
        if m:
            arr = json.loads(m.group(0))
            return arr, tokens
        return [], tokens
    except Exception as e:
        st.error(f"GPT error: {e}")
        return [], 0


def is_allowed_role(position: str) -> bool:
    if not position:
        return False
    p = position.lower()
    return bool(re.search(r"(chief\s+executive\s+officer|ceo)", p)) or bool(re.search(r"(chief\s+financial\s+officer|cfo)", p))


def format_events_for_row(events: List[dict]) -> List[str]:
    def parse_date(d: str):
        try:
            return datetime.strptime(d, "%Y-%m-%d")
        except Exception:
            return None

    def sort_key(ev):
        dt = parse_date(ev.get("date", ""))
        return dt if dt else datetime.max

    events_sorted = sorted(events, key=sort_key)
    cells = []
    for ev in events_sorted:
        person = ev.get("person_name", "N/A") or "N/A"
        pos = ev.get("position", "N/A") or "N/A"
        action = ev.get("action", "N/A") or "N/A"
        date = ev.get("date", "N/A") or "N/A"
        cell = f"{person}\n{pos}\n{action}\n{date}"
        cells.append(cell)
    return cells if cells else ["No events found"]


def read_urls(input_ws) -> List[dict]:
    try:
        rows = input_ws.get_all_values()
        urls = []
        seen = set()
        for i, row in enumerate(rows, start=1):
            if row and len(row) > 0:
                url = row[0].strip()
                if url and url.startswith("http"):
                    norm = url.lower()
                    if norm not in seen:
                        urls.append({"row": i, "url": url})
                        seen.add(norm)
                elif url and "sec.gov" in url.lower():
                    fixed = f"https://{url.lstrip('/')}"
                    norm = fixed.lower()
                    if norm not in seen:
                        urls.append({"row": i, "url": fixed})
                        seen.add(norm)
        return urls
    except Exception as e:
        st.error(f"Failed to read sheet: {e}")
        return []


def write_timeline_row(output_ws, company: str, ticker: str, url: str, events: List[str], tokens: int):
    headers = ["company name", "ticker", "url", "token count", "events..."]
    try:
        existing = output_ws.get_all_values()
    except Exception:
        existing = []

    # Ensure header exists in first row
    try:
        if not existing or len(existing) == 0:
            output_ws.update(range_name="A1", values=[headers], value_input_option="RAW")
        else:
            first_row = existing[0]
            if first_row[:len(headers)] != headers:
                output_ws.update(range_name="A1", values=[headers], value_input_option="RAW")
    except Exception:
        # Best effort fallback
        output_ws.append_row(headers)

    row = [str(company)[:100], str(ticker)[:10], str(url)[:500], str(tokens)]
    row.extend(events)
    output_ws.append_row(row)


def add_market_cap_and_filter(ws):
    try:
        rows = ws.get_all_values()
    except Exception as e:
        return
    if not rows:
        return
    headers = rows[0]
    # Normalize header names
    def norm(s):
        return (s or '').strip().lower()
    try:
        idx_ticker = [norm(h) for h in headers].index('ticker')
        idx_token = [norm(h) for h in headers].index('token count')
    except ValueError:
        return
    # Insert 'market cap' after token count if not present
    lc = [norm(h) for h in headers]
    if 'market cap' in lc:
        idx_mcap = lc.index('market cap')
    else:
        idx_mcap = idx_token + 1
        headers = headers[:idx_mcap] + ['market cap'] + headers[idx_mcap:]
        new_rows = [headers]
        for r in rows[1:]:
            # pad row to headers length first
            r = r + [''] * (len(headers) - len(r))
            new_rows.append(r[:idx_mcap] + [''] + r[idx_mcap:])
        rows = new_rows
    # Build formulas for each data row
    data = rows[1:]
    for i, r in enumerate(data, start=2):
        ticker = (r[idx_ticker] or '').strip()
        symbol_clean = ticker
        # Keep only first symbol if multiple separated by , or /
        if ',' in symbol_clean:
            symbol_clean = symbol_clean.split(',')[0].strip()
        if '/' in symbol_clean:
            symbol_clean = symbol_clean.split('/')[0].strip()
        # Remove non-word chars
        import re as _re
        symbol_clean = _re.sub(r'[^A-Za-z0-9\.-]', '', symbol_clean)
        if symbol_clean:
            formula = f'=IFERROR(GOOGLEFINANCE("{symbol_clean}", "marketcap"), "N/A")'
        else:
            formula = ''
        # Set formula into market cap column
        if len(r) <= idx_mcap:
            r += [''] * (idx_mcap - len(r) + 1)
        r[idx_mcap] = formula
        data[i-2] = r
    # Write back full sheet with formulas
    values = [headers] + data
    # Normalize to rectangular shape (pad rows to longest length)
    max_len = max(len(row) for row in values)
    for i in range(len(values)):
        if len(values[i]) < max_len:
            values[i] = values[i] + [''] * (max_len - len(values[i]))
    # Compute A1 range from max length
    import string
    def col_letter(n):
        s = ''
        while n:
            n, r = divmod(n-1, 26)
            s = chr(65+r) + s
        return s
    end_col = col_letter(max_len)
    end_row = len(values)
    rng = f'A1:{end_col}{end_row}'
    ws.update(range_name=rng, values=values, value_input_option='USER_ENTERED')
    # Allow formulas to calculate
    time.sleep(3)
    # Read back and filter by market cap 50M-5B
    rows2 = ws.get_all_values()
    if not rows2:
        return
    headers2 = rows2[0]
    try:
        idx_mcap2 = [norm(h) for h in headers2].index('market cap')
    except ValueError:
        return
    kept = [headers2]
    import re as _re2
    for r in rows2[1:]:
        if len(r) <= idx_mcap2:
            continue
        val = r[idx_mcap2]
        if not val or val.upper() == 'N/A':
            continue
        # Remove currency symbols and commas
        num_str = _re2.sub(r'[^0-9\.]', '', val)
        if not num_str:
            continue
        try:
            num = float(num_str)
        except Exception:
            continue
        if 50_000_000 <= num <= 5_000_000_000:
            kept.append(r)
    if len(kept) == 1:
        # No rows matched; clear data but keep header
        ws.clear()
        ws.update(range_name=f'A1:{col_letter(len(headers2))}1', values=[headers2], value_input_option='USER_ENTERED')
        return
    # Rewrite filtered data
    ws.clear()
    # Pad kept rows to rectangular shape
    max_len2 = max(len(row) for row in kept)
    for i in range(len(kept)):
        if len(kept[i]) < max_len2:
            kept[i] = kept[i] + [''] * (max_len2 - len(kept[i]))
    end_col2 = col_letter(max_len2)
    end_row2 = len(kept)
    ws.update(range_name=f'A1:{end_col2}{end_row2}', values=kept, value_input_option='USER_ENTERED')


def run_timeline(sa_info: dict, gpt_key: str):
    if not gpt_key:
        st.error("GPT API key is required to run the extraction.")
        return
    try:
        creds = Credentials.from_service_account_info(sa_info, scopes=SCOPES)
        gc = gspread.authorize(creds)
        spreadsheet = gc.open_by_url(SPREADSHEET_URL)
        input_ws = spreadsheet.worksheet(WORKSHEET_NAME)
        output_ws = spreadsheet.worksheet("Management Changes") if "Management Changes" in [ws.title for ws in spreadsheet.worksheets()] else spreadsheet.add_worksheet(title="Management Changes", rows=100, cols=30)
    except Exception as e:
        st.error(f"Sheet setup failed: {e}")
        return

    client = OpenAI(api_key=gpt_key)
    urls = read_urls(input_ws)
    if not urls:
        st.warning("No URLs found to process.")
        return

    progress = st.progress(0, text="Processing filings...")
    for idx, item in enumerate(urls, start=1):
        url = item["url"]
        progress.progress(idx / len(urls), text=f"{idx}/{len(urls)}: {url[:60]}...")
        html_text = fetch_filing(url)
        if not html_text:
            write_timeline_row(output_ws, "Fetch Failed", "N/A", url, ["Fetch Failed"], 0)
            continue
        if not has_item_502(html_text):
            continue
        company, ticker = extract_company_info_from_filing(url, html_text)
        exec_text = extract_executive_sections(html_text)
        events, tokens = extract_with_gpt_for_timeline(exec_text, client)
        events = [ev for ev in events if is_allowed_role(ev.get("position", ""))]
        cells = format_events_for_row(events)
        write_timeline_row(output_ws, company, ticker, url, cells, tokens)
    progress.empty()
    # Add market cap formulas and filter rows by 50M-5B
    add_market_cap_and_filter(output_ws)
    st.success("Extraction complete.")


# ------------------------------
# UI
# ------------------------------
st.set_page_config(page_title="SEC URL Manager", layout="wide")
st.title("SEC Filing URL Manager")

with st.sidebar:
    st.header("Credentials")
    sa_file = st.file_uploader("Service account JSON", type=["json"])
    gpt_file = st.file_uploader("GPT API key (.txt)", type=["txt"])
    gpt_key = None
    if gpt_file:
        gpt_key = gpt_file.read().decode("utf-8").strip()
        st.success("GPT key loaded")
    sa_info = None
    if sa_file:
        try:
            sa_info = json.load(sa_file)
            st.success("Service account loaded")
        except Exception as e:
            st.error(f"Invalid JSON: {e}")

if not sa_info:
    st.warning("Upload service account JSON to continue")
    st.stop()

try:
    ws = connect_sheet(sa_info)
except Exception as e:
    st.error(f"Failed to connect to sheet: {e}")
    st.stop()

current_urls = load_urls(ws)

# Add URLs
st.markdown("### Add URLs")
with st.form("add_form"):
    new_urls_raw = st.text_area(
        "Enter URLs (one per line)",
        height=140,
        placeholder="https://www.sec.gov/...",
    )
    add_submit = st.form_submit_button("Add URLs")
    if add_submit:
        new_urls = normalize_urls(new_urls_raw)
        if not new_urls:
            st.warning("No valid URLs to add.")
        else:
            merged = current_urls.copy()
            existing_lower = {u.lower() for u in merged}
            added = 0
            for u in new_urls:
                if u.lower() not in existing_lower:
                    merged.append(u)
                    existing_lower.add(u.lower())
                    added += 1
            if rewrite_urls(ws, merged):
                st.success(f"Added {added} new URL(s).")
                st.rerun()

# Delete URLs
st.markdown("### Delete URLs")
if current_urls:
    to_delete = st.multiselect("Select URLs to delete", options=current_urls)
    if st.button("Delete selected"):
        if not to_delete:
            st.warning("Select at least one URL to delete.")
        else:
            remaining = [u for u in current_urls if u not in to_delete]
            if rewrite_urls(ws, remaining):
                st.success(f"Deleted {len(to_delete)} URL(s).")
                st.rerun()
else:
    st.info("Nothing to delete.")

# Data section (refreshed after any changes)
st.subheader("Current URLs")
st.write(f"Total: {len(current_urls)}")
if current_urls:
    st.dataframe({"URL": current_urls}, use_container_width=True)
else:
    st.info("No URLs found.")

st.markdown("---")
st.subheader("Run Extraction")
if st.button("Run SEC Extraction", type="primary"):
    with st.spinner("Running extraction..."):
        run_timeline(sa_info, gpt_key)

st.caption("Changes above write directly to the Google Sheet.")
