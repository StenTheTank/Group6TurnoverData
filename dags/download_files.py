import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

base_url = "https://avaandmed.ariregister.rik.ee/et/avaandmete-allalaadimine"
save_dir = "~/downloaded_files"
os.makedirs(save_dir, exist_ok=True)

try:
    response = requests.get(base_url)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    header = soup.find("h2", string="Majandusaasta aruande andmed")

    if header:
        # Find the parent or the block containing the files below this header
        parent_div = header.find_next("div")  # Get the next div after the header

        # Extract all <a> tags within this block
        file_links = parent_div.find_all("a", href=True)

        for link in file_links:
            file_url = link["href"]

            # Skip if the href is not a downloadable file type
            if not file_url.endswith(".zip"):
                continue

            # Create the full URL for the file
            full_url = urljoin(base_url, file_url)

            # Extract the file name
            file_name = os.path.basename(file_url)

            # Download the file
            print(f"Downloading {file_name} from {full_url}...")
            file_response = requests.get(full_url)
            file_response.raise_for_status()

            # Save the file locally
            with open(os.path.join(save_dir, file_name), "wb") as file:
                file.write(file_response.content)

            print(f"Saved: {file_name}")
    else:
        print("Specified header not found on the page.")

except requests.exceptions.RequestException as e:
    print(f"Error: {e}")