import requests
import time
import random
import os

# Create pdfs directory if it doesn't exist
pdfs_dir = os.path.join(os.path.dirname(__file__), 'pdfs')
os.makedirs(pdfs_dir, exist_ok=True)

# Define the year range
start_year = 2007
end_year = time.localtime().tm_year-1 # Dynamically set to the current year - 1. 


# Loop through each year
for year in range(start_year, end_year + 1):
    next_year = year + 1
    url = f"https://www.sccourts.org/media/annualReports/{year}-{next_year}/CATotalsES2.pdf"
    headers = {'User-Agent': 'Mozilla/5.0'} # Mimic a browser user agent
    filename = f"estate_monthly_caseload_{year}_to_{next_year}.pdf"
    filepath = os.path.join(pdfs_dir, filename)

    # Check if file already exists
    if os.path.exists(filepath):
        print(f"⏭️ Skipping: {filename} (already exists)")
        continue

    success = False
    for attempt in range(1, 4):  # Up to 3 attempts
        try:
            print(f"Attempt {attempt} to download: {filename}")
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()

            with open(filepath, "wb") as f:
                f.write(response.content)
            print(f"✅ Downloaded: {filename}")
            success = True
            break  # Exit retry loop if successful

        except requests.exceptions.RequestException as e:
            print(f"⚠️ Attempt {attempt} failed for {filename}: {e}")
            wait_time = random.uniform(1, 5)
            print(f"⏳ Waiting {wait_time:.2f} seconds before retry...")
            time.sleep(wait_time)

    if not success:
        print(f"❌ Failed to download {filename} after 3 attempts.")
    else:
        # Wait before moving to the next file
        delay = random.uniform(1, 5)
        print(f"⏱️ Waiting {delay:.2f} seconds before next file...")
        time.sleep(delay)
