import csv
import json
from pathlib import Path

scheme_dir = Path(__file__).parent.resolve()

def load_data():
    data = []
    #with open("bodspipelines/infrastructure/schemes/2022-03-23_ra_list_v1.7.csv") as csv_file:
    with open(scheme_dir / "2022-03-23_ra_list_v1.7.csv") as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        for row in csv_reader:
            data.append(row)
    return data

def lookup_scheme(country, structure, unconfirmed=False, subnational=False):
    #print("lookup_scheme:", country, structure)
    if subnational and not "-" in subnational:
        country = subnational
        subnational = False
    if "-" in country:
        directory = scheme_dir / f"org-id-lists/{country.split('-')[0].lower()}"
        schemes = directory.glob(f"{country.lower()}-*.json")
        for filename in schemes:
            with open(filename) as json_file:
                data = json.load(json_file)
                if (country.split('-')[0] in data['coverage'] and
                    data['structure'] and structure in data['structure']):
                    return data['code'], data["name"]["en"], data['url']
    else:
        #directory = Path(f"bodspipelines/infrastructure/schemes/org-id-lists/{country.lower()}")
        directory = scheme_dir / f"org-id-lists/{country.lower()}"
        schemes = directory.glob("*.json")
    #print(directory)
    unconfirmed_data = []
    for filename in schemes:
        #print(filename)
        with open(filename) as json_file:
            data = json.load(json_file)
            #print(data['coverage'], data['structure'])
            if (data["confirmed"] and country in data['coverage'] and
                data['structure'] and structure in data['structure']):
                if not subnational:
                    if not data["subnationalCoverage"]:
                        return data['code'], data["name"]["en"], data['url']
                else:
                    if "subnationalCoverage" in data and subnational in data["subnationalCoverage"]:
                        return data['code'], data["name"]["en"], data['url']
            elif (country in data['coverage'] and data['structure'] and
                structure in data['structure']):
                if not subnational:
                    if not data["subnationalCoverage"]:
                        unconfirmed_data.append(data)
                else:
                    if "subnationalCoverage" in data and subnational in data["subnationalCoverage"]:
                        unconfirmed_data.append(data)
    if unconfirmed and unconfirmed_data:
        return unconfirmed_data[0]['code'], unconfirmed_data[0]["name"]["en"], unconfirmed_data[0]["url"]
    return None, None, None

def get_scheme(scheme_id, scheme_data, country_code=None):
    match = [scheme for scheme in scheme_data if scheme[0] == scheme_id]
    #print("matches:", match, country_code)
    if match:
        if country_code:
            for m in match:
                #print(m[2], country_code)
                if m[2] == country_code:
                    country = m[2]
                    break
            else:
                country = match[0][2]
        else:
            country = match[0][2]
        return lookup_scheme(country, "company")
    return None, None, None
