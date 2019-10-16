import pandas


def pars_xlsx_to_json(xlsx_file, json_file):
    df = pandas.read_excel(xlsx_file)
    df.to_json(path_or_buf=json_file, orient='table')
    return




