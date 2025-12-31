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


# ------------------------------
# Extraction helpers (inline from exec_timeline)
# ------------------------------

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
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate",
    }
    if "ix?doc=" in url:
        match = re.search(r"ix\?doc=(/.+)", url)
        if match:
            url = f"https://www.sec.gov{match.group(1)}"
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        return resp.text
    except Exception as e:
        st.error(f"Fetch failed for {url}: {e}")
        return None


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


def extract_company_info_from_filing(html_text: str) -> Tuple[str, str]:
    if not html_text:
        return "Company Not Found", "UNKN"
    soup = BeautifulSoup(html_text, "html.parser")

    xml_company_patterns = [
        r"<COMPANY-CONFORMED-NAME>([^<]+)</COMPANY-CONFORMED-NAME>",
        r"<conformed-name>([^<]+)</conformed-name>",
        r'"companyName":"([^"]+)",',
        r'"entityName":"([^"]+)",',
    ]
    company_name = None
    for pattern in xml_company_patterns:
        m = re.search(pattern, html_text, re.IGNORECASE)
        if m:
            company_name = re.sub(r"\s+", " ", m.group(1).replace("&amp;", "&")).strip()
            break

    ticker = extract_ticker_from_filing_tables(soup, html_text)
    if company_name and ticker:
        return company_name, ticker
    if company_name:
        return company_name, "UNKN"
    if ticker != "UNKN":
        return f"Company ({ticker})", ticker
    return "Company Not Found", "UNKN"


def keyword_extract_with_regex(text: str) -> str:
    text = normalize_text(text)
    verb_patterns = [
        r"appointed", r"named", r"promoted", r"demoted", r"elected", r"resigned", r"resigns", r"retired", r"steps down",
        r"assumed the role", r"assumed the roles", r"will serve as", r"to serve as", r"will become", r"was promoted", r"was demoted",
    ]
    title_patterns = [
        r"co[- ]?chief executive officer", r"co[- ]?chief financial officer",
        r"chief executive officer", r"chief financial officer",
        r"co[- ]?ceo", r"co[- ]?cfo",
        r"interim chief executive officer", r"interim chief financial officer",
        r"acting chief executive officer", r"acting chief financial officer",
        r"interim ceo", r"interim cfo", r"acting ceo", r"acting cfo",
        r"ceo", r"cfo",
    ]
    all_patterns = verb_patterns + title_patterns
    regex = re.compile(r"(" + r"|".join(all_patterns) + r")(?:[\s,:;\.\-])", re.IGNORECASE)
    matches = list(regex.finditer(text))
    if not matches:
        words = text.split()
        return " ".join(words[:1500])

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


def extract_executive_sections(html_text: str) -> str:
    soup = BeautifulSoup(html_text, "lxml")
    for tag in soup(["script", "style"]):
        tag.decompose()
    full_text_raw = soup.get_text(separator="\n", strip=True)
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
    try:
        existing = output_ws.get_all_values()
        if not existing:
            headers = ["company", "ticker", "url", "tokens", "events..."]
            output_ws.append_row(headers)
    except Exception:
        headers = ["company", "ticker", "url", "tokens", "events..."]
        output_ws.append_row(headers)

    row = [str(company)[:100], str(ticker)[:10], str(url)[:500], str(tokens)]
    row.extend(events)
    output_ws.append_row(row)


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
        company, ticker = extract_company_info_from_filing(html_text)
        exec_text = extract_executive_sections(html_text)
        events, tokens = extract_with_gpt_for_timeline(exec_text, client)
        events = [ev for ev in events if is_allowed_role(ev.get("position", ""))]
        cells = format_events_for_row(events)
        write_timeline_row(output_ws, company, ticker, url, cells, tokens)
    progress.empty()
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
