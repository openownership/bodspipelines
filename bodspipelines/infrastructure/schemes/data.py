import csv
import json
from pathlib import Path


def load_data():
    data = []
    with open("bodspipelines/infrastructure/schemes/2022-03-23_ra_list_v1.7.csv") as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        for row in csv_reader:
            data.append(row)
    return data

def lookup_scheme(country, structure):
    print("lookup_scheme:", country, structure)
    directory = Path(f"bodspipelines/infrastructure/schemes/org-id-lists/{country.lower()}")
    schemes = directory.glob("*.json")
    for filename in schemes:
        print(filename)
        with open(filename) as json_file:
            data = json.load(json_file)
            #print(data)
            if (data["confirmed"] and country in data['coverage'] and
                data['structure'] and structure in data['structure']):
                return data['code'], data["name"]["en"]
    return None, None

def get_scheme(scheme_id, scheme_data, country_code=None):
    match = [scheme for scheme in scheme_data if scheme[0] == scheme_id]
    print("matches:", match, country_code)
    if match:
        if country_code:
            for m in match:
                print(m[2], country_code)
                if m[2] == country_code:
                    country = m[2]
                    break
            else:
                country = match[0][2]
        else:
            country = match[0][2]
        return lookup_scheme(country, "company")
    return None, None
