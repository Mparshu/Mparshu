import pandas as pd
import streamlit as st
from io import BytesIO

def read_excel(file_path):
    excel_data = pd.read_excel(file_path, sheet_name=None)
    return excel_data

def compare_sheets(excel_data1, excel_data2):
    report = {}
    mismatched_records = {}
    for sheet_name in excel_data1.keys():
        match = excel_data1[sheet_name].equals(excel_data2[sheet_name])
        if match:
            report[sheet_name] = ("Match", None)
        else:
            report[sheet_name] = ("Do Not Match", excel_data1[sheet_name].compare(excel_data2[sheet_name]))
            mismatched_records[sheet_name] = excel_data1[sheet_name].compare(excel_data2[sheet_name])
    return report, mismatched_records

def display_report(report):
    st.title("Excel Sheets Comparison Report")
    for sheet_name, result in report.items():
        status, mismatch = result
        color = "red" if status == "Do Not Match" else "green"
        st.markdown(f"Sheet Name: {sheet_name} - Result: {status}", unsafe_allow_html=True)
        if mismatch is not None:
            st.write("Mismatched Records:")
            st.table(mismatch)
st.title('Excel File Comparator')

uploaded_file1 = st.file_uploader("Choose Excel file 1", type=['xlsx'])
uploaded_file2 = st.file_uploader("Choose Excel file 2", type=['xlsx'])

if uploaded_file1 and uploaded_file2:
    bytes_data1 = BytesIO(uploaded_file1.read())
    bytes_data2 = BytesIO(uploaded_file2.read())

    excel_data1 = read_excel(bytes_data1)
    excel_data2 = read_excel(bytes_data2)

    if st.button('Compare Sheets'):
        comparison_report, mismatched_records = compare_sheets(excel_data1, excel_data2)
        display_report(comparison_report)
