import requests
import pandas as pd
import re
import concurrent.futures

def get_insee_code_parallel(addresses):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        insee_codes = list(executor.map(get_insee_code, addresses))
    return insee_codes

def get_insee_code(address):
    if pd.isna(address):
        return float("nan")
    
    # First API call
    url = "https://api-adresse.data.gouv.fr/search/"
    params = {'q': address, 'limit': 1}
    response = requests.get(url, params=params)
    
    # Check if the response is valid
    if response.status_code == 200:
        data = response.json()
        if data['features']:
            insee_code = data['features'][0]['properties'].get('citycode', float("nan"))
            if insee_code != float("nan"):
                return insee_code
    
    # If INSEE code is not found, fallback to the second API
    return get_insee_code_fallback(address)

def get_insee_code_fallback(address):
    # Assuming we are using the 'communes' endpoint to fetch INSEE code based on the city name
    # Extract city name from the address to query the second API
    city = extract_city_from_address(address)
    if city:
        url = "https://geo.api.gouv.fr/communes"
        params = {'nom': city, 'fields': 'code', 'format': 'json', 'limit': 1}
        response = requests.get(url, params=params)
        
        # Check if the response is valid
        if response.status_code == 200:
            data = response.json()
            if data:
                return data[0].get('code', float("nan"))
    
    # Return NaN if no INSEE code is found
    return float("nan")

def extract_city_from_address(address):
    # Simple extraction logic assuming the city is the last part after a comma
    if ',' in address:
        return address.split(',')[-1].strip()
    return address.strip()


def remove_non_alphabetic_characters(text):
    cleaned_text = re.sub(r'[^A-Za-z\s]+', '', text)
    return cleaned_text

def clean_name(name):
    return remove_non_alphabetic_characters(name) if not (pd.isna(name)) else name

def format_dob(date):
    # eg. 1939/05/23 -> 23/05/1939
    if pd.isna(date):
        return float("nan")
    else:
        date_pattern = r"(\d{4})/(\d{2})/(\d{2})"
        match = re.match(date_pattern, date)
        if match:
            month = match.group(2)
            day = match.group(3)
            year = match.group(1)
            return f"{day}/{month}/{year}"

def transform_address_case_1(address):
    # eg. MARSEILLE : CH DE GIBBES , N ° 262 -> 262 CH DE GIBBES, MARSEILLE
    address_pattern = r"([A-Z]+)\s*:\s*(.+)\s*,\s*N\s*°\s*(\d+)"
    match = re.match(address_pattern, address)
    if match:
        number = match.group(3)
        street = match.group(2)
        city = match.group(1)
        formatted_address = f"{number} {street}, {city}"
        # Clean unnecessary spaces
        formatted_address = re.sub(r'\s*,\s*', ', ', formatted_address)
        return formatted_address

def transform_address_case_2(address):
    # eg. ALLAUCH : VILLA BEL AIR -> VILLA BEL AIR, ALLAUCH
    second_address_pattern = r"(\w+)\s*:\s*([\w\s]+)"
    match = re.match(second_address_pattern, address)
    if match:
        area = match.group(2)
        city = match.group(1)
        formatted_address = f"{area}, {city}"
        return formatted_address

def transform_address_case_3(address):
    # eg. MARSEILLE : BD FRANCOIS CAMOIN , " IN CHALLAH " ( CHAT . GOMBERT ) ->
    # " IN CHALLAH " ( CHAT . GOMBERT ) BD FRANCOIS CAMOIN, MARSEILLE
    third_address_pattern = r'(\w+)\s*:\s*([\w\s]+)\s*,\s*("[\w\s]+" \([\w\s\.]+\))'
    match = re.match(third_address_pattern, address)
    if match:
        address_string = match.group(3)
        street = match.group(2)
        city = match.group(1)
        formatted_address = f"{address_string} {street}, {city}"
        formatted_address = re.sub(r'\s*,\s*', ', ', formatted_address)
        return formatted_address

def format_place_of_birth(location):
    # eg. FRANCE / MARSEILLE -> MARSEILLE, FRANCE
    country_city_pattern = r"(?i)^\s*(\w+)\s*/\s*(\w+)\s*$"
    match = re.match(country_city_pattern, location)
    if match:
        country = match.group(1).upper()
        city = match.group(2).upper()
        formatted_location = f"{city}, {country}"
        return formatted_location

def clean_additional_info(additional_info):
    if not (pd.isna(additional_info)):
        # Remove bullet points, periods, and hyphens at the start or after punctuation
        additional_info = re.sub(r'^\s*[•.-]+', '', additional_info)  # Start of string clean
        additional_info = re.sub(r'\s*[•.-]+\s*', ' ', additional_info)  # Clean mid-string
        # Remove space before periods, question marks, commas, and exclamation marks
        additional_info = re.sub(r'\s+([.!?,:])', r'\1', additional_info)
        # Ensure exactly one space after periods (if not at end of string)
        additional_info = re.sub(r'\.\s*-\s*', '. ', additional_info)
        # Remove "(...)" followed by a comma, if any
        additional_info = re.sub(r'\s*\(\s*\.\.\.\s*\)\s*,?', '', additional_info)
        # Remove unwanted spaces around commas
        additional_info = re.sub(r'\s*,\s*', ', ', additional_info)
        # Remove unwanted spaces outside parentheses
        additional_info = re.sub(r'\(\s*', '(', additional_info)
        additional_info = re.sub(r'\s*\)', ')', additional_info)
        # Ensure exactly one space around hyphens
        additional_info = re.sub(r'\s*-\s*', ' - ', additional_info)
        # Remove spaces immediately after opening quotes
        additional_info = re.sub(r'"\s*', '"', additional_info)
        # Remove spaces before closing quotes
        additional_info = re.sub(r'\s*"', '"', additional_info)
        # Ensure space before opening quotes if not at the start
        additional_info = re.sub(r'(?<=\S)\s*(")', r' \1', additional_info)
        # Clean up multiple periods or hyphens and ensure proper spacing
        additional_info = re.sub(r'[\.\-]{2,}', ' ', additional_info)  # Replace multiple periods or hyphens
        additional_info = re.sub(r'\s{2,}', ' ', additional_info)  # Replace multiple spaces with a single space
        # Trim leading and trailing spaces
        return additional_info.strip()
    else:
        return float("nan")

def main():
    df = pd.read_excel('13_raw.xlsx')

    output_data = []
    addresses = []

    for index, row in df.iterrows():
        # Collect addresses for INSEE code retrieval later
        address = row['adresse_actuelle']
        if pd.isna(address):
            formatted_address = float("nan")
        elif re.search(r'[°\d]', address):
            formatted_address = transform_address_case_1(address)
        elif not re.search(r',', address):
            formatted_address = transform_address_case_2(address)
        else:
            formatted_address = transform_address_case_3(address)

        addresses.append(formatted_address)

    # Fetch INSEE codes in parallel
    insee_codes = get_insee_code_parallel(addresses)

    # Iterate through rows again to create the output
    for index, row in df.iterrows():
        # Other processing (name, DOB, etc.)
        first_name = clean_name(row['prenoms'])
        last_name = clean_name(row['nom'])
        maiden_name = clean_name(row['nom_de_jeune_fille'])
        formatted_date = format_dob(row['date_de_naissance'])
        birth_country = row['pays_ville_origine']
        formatted_birth_country = format_place_of_birth(birth_country) if pd.notna(birth_country) else float("nan")
        additional_info = clean_additional_info(row['texte'])
        insee_code = insee_codes[index]

        output_data.append({
            'First Name': first_name,
            'Last Name': last_name,
            'Maiden Name': maiden_name,
            'Formatted Date': formatted_date,
            'Formatted Address': addresses[index],
            'Formatted Birth Country': formatted_birth_country,
            'Additional Info': additional_info,
            'INSEE Code': insee_code,
        })

    # Save output
    output_df = pd.DataFrame(output_data)
    output_df.to_excel('formatted_output.xlsx', index=False)
    print('Data written to formatted_output.xlsx successfully.')


if __name__ == "__main__":
    main()