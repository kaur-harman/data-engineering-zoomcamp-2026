import dlt
import requests


@dlt.resource(name="taxi_trips", write_disposition="replace")
def taxi_trips():

    base_url = "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api"

    page = 1

    while True:
        print(f"Fetching page {page}")
        response = requests.get(base_url, params={"page": page})

        data = response.json()

        if not data:
            print("No more data.")
            break

        yield data

        page += 1


def load_taxi_pipeline():
    pipeline = dlt.pipeline(
        pipeline_name="taxi_pipeline",
        destination="duckdb",
        dataset_name="taxi_data"
    )

    load_info = pipeline.run(taxi_trips())
    print(load_info)


if __name__ == "__main__":
    load_taxi_pipeline()