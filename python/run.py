import sys
from amadeus import Client, ResponseError

def downloadData(from_field, to_field, date):
    amadeus = Client(
        client_id='xxx',
        client_secret='xxxx'
    )

    try:
        body = {
            "originDestinations": [
                {
                    "id": "1",
                    "originLocationCode": from_field,
                    "destinationLocationCode": to_field,
                    "departureDateTime": {
                        "date": date
                    }
                }
            ],
            "travelers": [
                {
                    "id": "1",
                    "travelerType": "ADULT"
                }
            ],
            "sources": [
                "GDS"
            ]
        }

        response = amadeus.shopping.availability.flight_availabilities.post(body)
        print(response.data[0]['type'])
        #save json
    except ResponseError as error:
        raise error

def main():
    from_field = sys.argv[1]
    to_field = sys.argv[2]
    date = sys.argv[3]
    print("Python we are in main!")
    print("Python " + to_field)
    downloadData(from_field, to_field, date)

main()
