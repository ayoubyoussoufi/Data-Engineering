import pandas as pd
import yaml


def get_config():
    with open("configfile.yml", "r", encoding="utf-8") as yamlfile:
        config = yaml.load(yamlfile, Loader=yaml.FullLoader)
        print("Read successful")
    return config



def read_excel_sheets(excel_file, sheet_names):
    result_list = []
    for sheet_name in sheet_names:
        df = pd.read_excel(excel_file, sheet_name=sheet_name, header=5)
        if not df.empty:
            df['brand'] = sheet_name
            result_list.append(df)
    return pd.concat(result_list).sort_index(kind='merge')


def process_dataframe(df):
    config = get_config()
    df.rename(columns=config["new_column_names"], inplace=True)
    df1 = df[config["split_parameters"]["col_product_2023"]].copy()
    df1.columns = config["split_parameters"]["col_to_rename"]
    df1['year'] = 2023

    result_list = [df1]

    if 'quantity_2024' in df.columns and 'value_2024' in df.columns:
        df2 = df[config["split_parameters"]["col_product_2024"]].copy()
        df2.columns = config["split_parameters"]["col_to_rename"]
        df2['year'] = 2024
        result_list.append(df2)

    return pd.concat(result_list)


def assign_months(final_result):
    config = get_config()
    assigned_months = []
    current_month = None
    for index, row in final_result.iterrows():
        if row[config["split_parameters"]["col_to_use"]] in config["split_parameters"]["months"]:
            current_month = row[config["split_parameters"]["col_to_use"]]
        assigned_months.append(current_month)
    final_result['month'] = assigned_months
    return final_result[final_result[config["split_parameters"]["col_to_use"]] != final_result['month']]


def filter_final_result(final_result):
    config = get_config()
    final_result['extracted_product_id'] = final_result['product_id'].str.extract(r'(\d+)')
    final_result = final_result.dropna(subset=['quantity', 'value', 'extracted_product_id'])
    final_result = final_result[final_result['product_id'] != 'Hovedtotal']
    final_result = final_result[~final_result['product_id'].apply(lambda x: x.isdigit())]
    final_result = final_result.drop(columns='extracted_product_id')
    final_result = final_result[config["split_parameters"]["col_to_final_result"]]
    return final_result


def main():
    excel_file = '../recruitement_execise/raw/20241203_BILLUND Luxe et CPD_202404171158.xlsx'
    config = get_config()
    final_result = read_excel_sheets(excel_file, config["split_parameters"]["sheet_names"])
    final_result = process_dataframe(final_result)
    final_result = assign_months(final_result)
    final_result = filter_final_result(final_result)
    final_result.to_csv('final_result.csv', index=False)

    return final_result


if __name__ == "__main__":
    main()
