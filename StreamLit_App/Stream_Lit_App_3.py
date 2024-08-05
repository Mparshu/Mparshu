import pandas as pd
import streamlit as st

# Load the Excel files
def load_excel(file_path):
    try:
        return pd.read_excel(file_path, sheet_name=None)
    except Exception as e:
        st.error(f"Error loading Excel file: {e}")
        return None

# Compare two dataframes
def compare_dataframes(df1, df2):
    if df1.equals(df2):
        return True
    else:
        return False

# Streamlit app
def main():
    st.title("Excel Sheet Comparison Tool")
    st.sidebar.header("Upload Excel Files")

    # Upload files
    file1 = st.sidebar.file_uploader("Upload Excel File 1", type=["xlsx"])
    file2 = st.sidebar.file_uploader("Upload Excel File 2", type=["xlsx"])

    if file1 and file2:
        st.sidebar.success("Files uploaded successfully!")

        # Load dataframes
        df1_dict = load_excel(file1)
        df2_dict = load_excel(file2)

        if df1_dict and df2_dict:
            sheet_names = list(df1_dict.keys())

            # Compare sheets
            for sheet_name in sheet_names:
                st.subheader(f"Sheet: {sheet_name}")
                df1 = df1_dict[sheet_name]
                df2 = df2_dict[sheet_name]

                if compare_dataframes(df1, df2):
                    st.success(f"{sheet_name}: Sheets match!")
                else:
                    st.error(f"{sheet_name}: Sheets do not match!")

                    # Show differences
                    diff_df = pd.concat([df1, df2]).drop_duplicates(keep=False)
                    st.dataframe(diff_df)

if __name__ == "__main__":
    main()
