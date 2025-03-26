import requests
import pandas as pd

# Base API URL (request 100 rows per server page) and Headers
base_api_url = "https://inspiredhealth.neptune.practicehub.io/api/patients?page_size=100"
headers = {
    "x-practicehub-key": "h48kMJnITqZ8MDGl32AFGBARf4niSoAc",  # Replace with your actual API key
    "x-app-details": "mywonderfulapp=help@practicehub.io",
    "User-Agent": "mywonderfulapp=help@practicehub.io",
    "Content-Type": "application/json"
}

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
                print(f"API Error: {response.status_code}")
                break
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {str(e)}")
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

# Fetch data and store it in a DataFrame
df = get_all_patients_data()

# Save to CSV
if not df.empty:
    df.to_csv("patients_data.csv", index=False)
    print("Data successfully saved to patients_data.csv")
else:
    print("No data available.")
