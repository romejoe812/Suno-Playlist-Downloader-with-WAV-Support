# Suno Downloader (MP3 + WAV) — GUI App

This repo contains a GUI downloader for **Suno playlists / clips** that can:

- Download **MP3** files plus:
  - Artwork
  - Lyrics
  - Genres
  - Prompt text
- Create Excel indexes:
  - Per-playlist index: `<Playlist Name>.xlsx`
  - Master index: `Suno Master Index.xlsx`
- Optionally download **WAV** via Playwright + your local Chrome:
  - Uses a **persistent Chrome profile** stored next to the script/exe (`pw_suno_profile\`)
  - WAV files are saved into the **same playlist folder** as MP3
  - WAV files are tagged to match the MP3 tags (ID3v2.4 + RIFF INFO)

---

## What you need

- Windows 10/11
- Python **3.10+**
- Google Chrome installed (the app can help you locate `chrome.exe`)
- A Suno account (you’ll sign in using Chrome on first run)

Python packages (installed via `requirements.txt`):
- `requests`, `pillow`, `openpyxl`, `mutagen`, `playwright`

Also required (one-time):
- Playwright browser components:

```bat
py -m playwright install
```

---


## Quick start (run from source)

1. Download this repo (Code → Download ZIP) and extract it.
2. Open **Command Prompt** in the extracted folder.
3. Install dependencies:

```bat
py -m pip install --upgrade pip
py -m pip install -r requirements.txt
py -m playwright install
```

4. Run the app:

```bat
py Suno_Downloader_Wav.py
```

---

## First run (Suno login)

On first run, the app will open Chrome for you to sign in:

1. Sign in to Suno in the Chrome window that opens
2. Close **all** Chrome windows completely
3. The downloader relaunches automatically and continues

The login/profile data is kept in a folder next to the app:
- `pw_suno_profile\`

---

## How to use the app

### 1) Paste inputs
In the big input box, paste **one per line**:

- Playlist URLs or IDs
- Clip/song URLs or IDs

The app can extract IDs from inputs that look like:
- `/playlist/<id>` or `/playlists/<id>`
- `/song/<id>`
- `/clip/<id>` or `/clips/<id>`
- `?id=<id>`
- Or just paste the raw ID (22–36 chars)

### 2) Choose an output folder
Pick a base folder where downloads should be saved.

The app creates one folder per playlist:
- `Your Output Folder\<Playlist Name>\`

Inside each playlist folder it creates:
- `Audio\` (MP3 and WAV)
- `Art\` (covers + thumbnails)
- `Lyrics\`
- `Genres\`
- `Prompt\`

If you download individual clips (not playlists), they go under:
- `Your Output Folder\Unsorted\`

### 3) Choose what to download
Common options include:
- MP3 audio
- Artwork
- Lyrics
- Genres
- Prompts
- Playlist index XLSX
- Master index XLSX
- WAV downloads (Playwright)

There is also a “Retag Only” mode that only fills missing ID tags without rewriting everything.

### 4) Start
Click Start and watch progress in the built-in console area.

---

## WAV download notes (important)

- WAV download uses Playwright automation + your Chrome profile.
- Chrome cannot be running with the same profile while WAV download happens.
- If you see a “profile in use / locked” message, close Chrome completely and try again.

---

## Build a single-file EXE (PyInstaller)

1. Install PyInstaller:

```bat
py -m pip install pyinstaller
```

2. Build (copy/paste one-liner):

```bat
py -m PyInstaller --noconfirm --clean --onefile --noconsole --name "Suno_Downloader_Wav" --collect-all playwright Suno_Downloader_Wav.py
```

**Result:**
- `dist\Suno_Downloader_Wav.exe`

**Note:** Because this uses Playwright + Chrome, it’s usually easiest to build and run the EXE on the same machine where Chrome is installed and where you can sign in to Suno.

---

## License

MIT License (see `LICENSE`). Forking/modifying is allowed as long as the license text stays included.

---

## Disclaimer

This is an unofficial tool. Use it only for content you have rights to access, and follow any applicable terms of service.
