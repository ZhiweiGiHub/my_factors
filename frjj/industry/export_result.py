from openpyxl import load_workbook
import pandas as pd


def export_result_(position, changeday, result_path, file_name1):
    book = load_workbook(result_path + file_name1)
    writer = pd.ExcelWriter(result_path + file_name1, engine='openpyxl')
    writer.book = book
    pd.DataFrame(position).to_excel(writer, sheet_name=str(changeday))
    writer.save()
