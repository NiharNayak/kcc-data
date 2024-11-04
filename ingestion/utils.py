PROJECT_ID = "kcc_project"
DATASET_ID = "example_dataset"

TABLE_MAPPING_PREFIX = f"{PROJECT_ID}.{DATASET_ID}"

table_mapping = {
    "Sales_Data": f"{TABLE_MAPPING_PREFIX}.sales",
    "Store_Data": f"{TABLE_MAPPING_PREFIX}.stores",
    "Product_Data": f"{TABLE_MAPPING_PREFIX}.products"
}


def get_bq_table_id(dataset_name):
    if dataset_name not in table_mapping:
        raise Exception(f"Table mapping doesn't exists for dataset_name:{dataset_name}")
    else:
        return table_mapping[dataset_name]
