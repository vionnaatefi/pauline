import pandas as pd
import re


def main():
    # Read the Excel file using pandas
    df = pd.read_excel('13_raw.xlsx')

    # Initialize a list to hold the output data
    output_data = []

    # Iterate through rows in the dataframe
    for index, row in df.iterrows():
        # 1) Separate names into First, Last,  Maiden
        first_name = row['prenoms']
        last_name = row['nom']
        maiden_name = row['nom_de_jeune_fille']
        if pd.isna(maiden_name):
            name = f"{first_name}, {last_name}"
        else:
            name = f"{first_name}, {last_name}, {maiden_name}"

        # 2) Format DOB
        eu_date = row['date_de_naissance']  # Expected format: 1939/05/23
        if pd.isna(eu_date):
            us_date = ""
        else:
            eu_date_pattern = r"(\d{4})/(\d{2})/(\d{2})"
            match = re.match(eu_date_pattern, eu_date)
            if match:
                month = match.group(2)
                day = match.group(3)
                year = match.group(1)
                us_date = f"{month}/{day}/{year}"
            else:
                us_date = ""

        # 3a) Format the addresses
        address = row['adresse_actuelle'] # eg. MARSEILLE : CH DE GIBBES , N ° 262
        if pd.isna(address):
            formatted_address = ""
        else:
            address_pattern = r"([A-Z]+)\s*:\s*(.+)\s*,\s*N\s*°\s*(\d+)"
            match = re.match(address_pattern, address)
            if match:
                number = match.group(3).upper()
                street = match.group(2).upper()
                city = match.group(1).upper()
                formatted_address = f"{number} {street}, {city}"
            else:
                formatted_address = ""

        # 3b) Format the places of birth, places of origin
        birth_country = row['pays_ville_origine']
        if pd.isna(birth_country):
            formatted_birth_country = ""
        else:
            country_city_pattern = r"(?i)^\s*(\w+)\s*/\s*(\w+)\s*$"
            match = re.match(country_city_pattern, birth_country)
            if match:
                country = match.group(1).upper()
                city = match.group(2).upper()
                formatted_birth_country = f"{city}, {country}"
            else:
                formatted_birth_country = ""

        output_data.append({
            'Name': name,
            'Formatted Date': us_date,
            'Formatted Address': formatted_address,
            'Formatted Birth Country': formatted_birth_country
        })

        # Create a DataFrame from the output data
        output_df = pd.DataFrame(output_data)

        # Save the DataFrame to an Excel file
        output_df.to_excel('formatted_output.xlsx', index=False)

    print('Data written to formatted_output.xlsx successfully.')

if __name__ == "__main__":
    main()
