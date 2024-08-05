import pandas as pd
import streamlit as st
from io import BytesIO

def read_excel(file_path):
    excel_data = pd.read_excel(file_path, sheet_name=None)
    return excel_data

def compare_sheets(excel_data1, excel_data2):
    report = {}
    for sheet_name in excel_data1.keys():
        match = excel_data1[sheet_name].equals(excel_data2[sheet_name])
        report[sheet_name] = "Match" if match else "Do Not Match"
    return report

def display_report(report):
    st.title("Excel Sheets Comparison Report")
    for sheet_name, result in report.items():
        color = "red" if result == "Do Not Match" else "green"
        st.write(f"Sheet Name: {sheet_name} - Result: <span style='color:{color}'>{result}</span>", unsafe_allow_html=True)

# Streamlit UI
st.title('Excel File Comparator')

uploaded_file1 = st.file_uploader("Choose Excel file 1", type=['xlsx'])
uploaded_file2 = st.file_uploader("Choose Excel file 2", type=['xlsx'])

if uploaded_file1 and uploaded_file2:
    bytes_data1 = BytesIO(uploaded_file1.read())
    bytes_data2 = BytesIO(uploaded_file2.read())

    excel_data1 = read_excel(bytes_data1)
    excel_data2 = read_excel(bytes_data2)

    if st.button('Compare Sheets'):
        comparison_report = compare_sheets(excel_data1, excel_data2)
        display_report(comparison_report)
