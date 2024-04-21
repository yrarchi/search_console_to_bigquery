import json
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.auth import default
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


class SearchConsoleDataFetcher:
    def __init__(self, site_url):
        self.site_url = site_url
        self.search_console_client = self.initialize_client()

    def initialize_client(self):
        credentials, _ = default()
        client = build(
            "searchconsole",
            "v1",
            credentials=credentials,
            cache_discovery=False
        )
        return client

    def get_search_console_data(self, dimensions, aggregation_type, types, start_date, end_date):
        all_data = []
        for type in types:
            query_request = {
                "startDate": start_date.strftime("%Y-%m-%d"),
                "endDate": end_date.strftime("%Y-%m-%d"),
                "dimensions": dimensions,
                "type": type,
                "aggregationType": aggregation_type,
                "rowLimit": 25000,
            }
            try:
                response = self.search_console_client.searchanalytics().query(
                    siteUrl=self.site_url,
                    body=query_request
                ).execute()
                all_data.append({"type": type, "data": response})
            except HttpError as error:
                print(f"An error occurred for type {type}: {error}")
                continue
        return all_data

    def format_data(self, data, dimensions, name):
        formatted_data = []
        for item in data:
            if "rows" in item["data"]:
                for row in item["data"]["rows"]:
                    keys = row["keys"]
                    dim_values = {}
                    for dim in dimensions:
                        index = dimensions.index(dim)
                        dim_values[dim] = keys[index]
                    formatted_item = {
                        "data_date": dim_values["date"],
                        "site_url": self.site_url,
                        "query": dim_values["query"],
                        "is_anonymized_query": len(dim_values["query"]) == 0,
                        "country": dim_values["country"],
                        "search_type": item["type"],
                        "device": dim_values["device"],
                        "impressions": row["impressions"],
                        "clicks": row["clicks"]
                    }
                    sum_position = round((row["position"] - 1) * row["impressions"])
                    if name == "site":
                        formatted_item["sum_top_position"] = sum_position
                    elif name == "url":
                        formatted_item["url"] = dim_values["page"]
                        formatted_item["sum_position"] = sum_position
                        formatted_item["is_anonymized_discover"] = len(dim_values["page"]) == 0
                    formatted_data.append(formatted_item)
        return formatted_data


class BigQueryClient:
    def __init__(self, dataset_id):
        self.bigquery_client = self.initialize_client()
        self.dataset_id = dataset_id

    def initialize_client(self):
        credentials, project_id = default()
        client = bigquery.Client(credentials=credentials, project=project_id)
        return client

    def insert_data_to_table(self, table_id, data):
        dataset_ref = self.bigquery_client.dataset(self.dataset_id)
        table_ref = dataset_ref.table(table_id)
        table = self.bigquery_client.get_table(table_ref)
        errors = self.bigquery_client.insert_rows(table, data)
        if not errors:
            print(f"New rows have been added to {table_id}.")
        else:
            print(f"Encountered errors while inserting rows to {table_id}:{errors}")

    def get_max_date(self, table_id):
        query = f"""
            SELECT MAX(data_date) AS max_date
            FROM `{self.dataset_id}.{table_id}`
        """
        try:
            query_job = self.bigquery_client.query(query)
            result = list(query_job.result())
            if result:
                return result[0]["max_date"]
            else:
                return None
        except Exception as error:
            print(f"An error occurred while getting the max date from {table_id}: {error}")
            raise


def get_next_day_after_max_date(client, table_id, default_start_date):
    max_date = client.get_max_date(table_id)
    if max_date:
        next_day = max_date + timedelta(days=1)
        return next_day
    else:
        return datetime.strptime(default_start_date, "%Y-%m-%d").date()


def main(event, context):
    with open("config.json", "r") as f:
        config = json.load(f)

    target_start_date = config["target_start_date"]
    target_end_date = config["target_end_date"]
    days = config["days"]
    site_url = config["site_url"]
    dataset_id = config["dataset_id"]
    site_table = config["site_table"]
    url_table = config["url_table"]

    data_dimensions = [
        {
            "name": "site",
            "dimensions": ["query", "country", "device", "date"],
            "aggregation_type": "byProperty",
            "types": ["NEWS", "IMAGE", "VIDEO", "WEB"],
            "table_id": site_table
        },
        {
            "name": "url",
            "dimensions": ["query", "page", "country", "device", "date"],
            "aggregation_type": "byPage",
            "types": ["NEWS", "IMAGE", "VIDEO", "WEB"],
            "table_id": url_table
        }
    ]

    data_fetcher = SearchConsoleDataFetcher(site_url)
    bigquery_client = BigQueryClient(dataset_id)

    for dimension in data_dimensions:
        start_date = get_next_day_after_max_date(
            bigquery_client, dimension["table_id"], target_start_date
        )
        if start_date <= datetime.strptime(target_end_date, "%Y-%m-%d").date():
            end_date = start_date + timedelta(days=days-1)
            data = data_fetcher.get_search_console_data(
                dimension["dimensions"],
                dimension["aggregation_type"],
                dimension["types"],
                start_date,
                end_date
            )
            formatted_data = data_fetcher.format_data(
                data, dimension["dimensions"], dimension["name"]
            )
            bigquery_client.insert_data_to_table(
                dimension["table_id"], formatted_data
            )
        else:
            print(f"Data for the target period already exists in {dimension['table_id']}. "
                  "No further processing needed.")
