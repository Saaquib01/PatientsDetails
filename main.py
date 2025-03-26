import streamlit as st
import requests
import pandas as pd

# Streamlit UI Setup
st.set_page_config(page_title="Patients Data", layout="wide")
st.title("📋 Patients Data Viewer")

# Base API URL (request 100 rows per server page) and Headers
base_api_url = "https://inspiredhealth.neptune.practicehub.io/api/patients?page_size=100"
headers = {
    "x-practicehub-key": "api",  # Replace with your actual API key
    "x-app-details": "mywonderfulapp=help@practicehub.io",
    "User-Agent": "mywonderfulapp=help@practicehub.io",
    "Content-Type": "application/json"
}

@st.cache_data
def get_all_patients_data():
    """
    Fetches *all* pages from the API by following 'next' links.
    Each server page has up to 100 rows (due to page_size=100).
    """
    all_patients = []
    current_url = base_api_url

    while current_url:
        try:
            response = requests.get(current_url, headers=headers)
            if response.status_code == 200:
                data = response.json()
                batch = data.get("data", [])
                all_patients.extend(batch)

                # Follow the 'next' link if available
                links = data.get("links", {})
                current_url = links.get("next")
            else:
                st.error(f"API Error: {response.status_code}")
                break
        except requests.exceptions.RequestException as e:
            st.error(f"Request failed: {str(e)}")
            break

    # Convert the combined list into a DataFrame
    patients = []
    for patient in all_patients:
        patients.append({
            "ID": patient.get("id"),
            "Patient Number": patient.get("patient_number"),
            "First Name": patient.get("first_name"),
            "Last Name": patient.get("last_name"),
            "Email": patient.get("email"),
            "Phone": patient.get("phone"),
            "Sex": patient.get("sex"),
            "DOB": patient.get("dob"),
            "Medical Insurance": patient.get("medical_insurance"),
            "Total Balance": patient.get("total_balance"),
            "Last Appointment": patient.get("last_apt"),
            "Next Appointment": patient.get("next_apt"),
            "Created At": patient.get("created_at"),
            "Updated At": patient.get("updated_at"),
            "Address Num": patient.get("address_num", ""),
            "Address Str": patient.get("address_str", "")
        })

    return pd.DataFrame(patients)

# Load all data from the API by following pagination links
df = get_all_patients_data()

if df is not None and not df.empty:
    # Search bar for filtering
    search_query = st.text_input("🔍 Search by Name or Patient Number:")
    if search_query:
        df = df[df.apply(lambda row: search_query.lower() in str(row).lower(), axis=1)]
    
    # Client-side Pagination setup
    rows_per_page = 100  # Exactly 100 rows per page
    total_rows = len(df)
    total_pages = (total_rows // rows_per_page) + (1 if total_rows % rows_per_page else 0)

    # Initialize session state for page number if not present
    if "page_number" not in st.session_state:
        st.session_state.page_number = 1

    # Make sure page_number is in valid range after filtering
    if st.session_state.page_number > total_pages and total_pages > 0:
        st.session_state.page_number = total_pages

    # Calculate row indices for the current page
    start_row = (st.session_state.page_number - 1) * rows_per_page
    end_row = start_row + rows_per_page

    # Display the paginated DataFrame (100 rows max per page)
    st.dataframe(df.iloc[start_row:end_row], use_container_width=True)

    # Pagination navigation buttons below the table
    col1, col2, col3 = st.columns([1, 2, 1])
    with col1:
        if st.button("⏮️ Previous") and st.session_state.page_number > 1:
            st.session_state.page_number -= 1
    with col3:
        if st.button("Next ⏭️") and st.session_state.page_number < total_pages:
            st.session_state.page_number += 1
    with col2:
        st.markdown(
            f"<div style='text-align: center;'>"
            f"Page **{st.session_state.page_number}** of **{total_pages}**"
            f"</div>", 
            unsafe_allow_html=True
        )

    # Download button for exporting data
    csv = df.to_csv(index=False).encode('utf-8')
    st.download_button("📥 Download CSV", csv, "patients_data.csv", "text/csv")

else:
    st.warning("No data available. Check API credentials or pagination settings.")
