import sys
import json
from amadeus import Client, ResponseError

raw_filename = "reponse_raw.json"

def writeRawResponseToFile(response):
    with open("../spark/input_stream"+raw_filename, 'w') as f:
        json.dump(response.data, f)

def downloadData(from_field, to_field, date):
    amadeus = Client(
        client_id='xxx',
        client_secret='xxx'
    )

    try:
        response = amadeus.shopping.flight_offers_search.get(
              originLocationCode=from_field, destinationLocationCode=to_field, departureDate=date, adults=1
        )
        writeRawResponseToFile(response)
    except ResponseError as error:
        raise error

def main():
    from_field = sys.argv[1]
    to_field = sys.argv[2]
    date = sys.argv[3]
    print("Python we are in main!")
    print("Python " + to_field)
    #downloadData(from_field, to_field, date)

main()