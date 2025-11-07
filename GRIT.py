import streamlit as st
import re
import pandas as pd
from millify import millify # shortens values (10_000 ---> 10k)
from streamlit_extras.metric_cards import style_metric_cards # beautify metric card with css
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import altair as alt
import json
import time
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
import io
from mailjet_rest import Client

st.set_page_config(
        page_title="GRIT Dashboard - OCS",
        page_icon="https://github.com/JiaqinWu/GRIT_Website/raw/main/logo1.png", 
        layout="centered"
    ) 


scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets', "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
#creds = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
# Use Streamlit's secrets management
creds_dict = st.secrets["gcp_service_account"]
# Extract individual attributes needed for ServiceAccountCredentials
credentials = {
    "type": creds_dict.type,
    "project_id": creds_dict.project_id,
    "private_key_id": creds_dict.private_key_id,
    "private_key": creds_dict.private_key,
    "client_email": creds_dict.client_email,
    "client_id": creds_dict.client_id,
    "auth_uri": creds_dict.auth_uri,
    "token_uri": creds_dict.token_uri,
    "auth_provider_x509_cert_url": creds_dict.auth_provider_x509_cert_url,
    "client_x509_cert_url": creds_dict.client_x509_cert_url,
}

# Create JSON string for credentials
creds_json = json.dumps(credentials)

# Load credentials and authorize gspread
creds = ServiceAccountCredentials.from_json_keyfile_dict(json.loads(creds_json), scope)
client = gspread.authorize(creds)

# Cache configuration
CACHE_DURATION = 300  

@st.cache_data(ttl=CACHE_DURATION)
def fetch_google_sheets_data():
    """Fetch data from Google Sheets with caching"""
    try:
        spreadsheet1 = client.open('PWC_Referral_GRIT')
        worksheet1 = spreadsheet1.worksheet('GRIT')
        
        # Get all values and handle duplicate headers
        grit_values = worksheet1.get_all_values()
        if grit_values:
            # Clean up duplicate headers by making them unique
            headers = grit_values[0]
            cleaned_headers = []
            header_counts = {}
            for header in headers:
                if header == '' or header in header_counts:
                    header_counts[header] = header_counts.get(header, 0) + 1
                    cleaned_headers.append(f"{header}_{header_counts[header]}" if header else f"empty_{header_counts[header]}")
                else:
                    header_counts[header] = 1
                    cleaned_headers.append(header)
            
            # Create DataFrame with cleaned headers
            grit_data = grit_values[1:]  # Skip header row
            grit_df = pd.DataFrame(grit_data, columns=cleaned_headers)
            grit_df['Date'] = pd.to_datetime(grit_df['Date'], errors='coerce')
            grit_df['Day of Case Note'] = pd.to_datetime(grit_df['Day of Case Note'], errors='coerce')
        else:
            grit_df = pd.DataFrame()
        
        worksheet2 = spreadsheet1.worksheet('IPE')
        
        # Get all values and handle duplicate headers
        ipe_values = worksheet2.get_all_values()
        if ipe_values:
            # Clean up duplicate headers by making them unique
            headers = ipe_values[0]
            cleaned_headers = []
            header_counts = {}
            for header in headers:
                if header == '' or header in header_counts:
                    header_counts[header] = header_counts.get(header, 0) + 1
                    cleaned_headers.append(f"{header}_{header_counts[header]}" if header else f"empty_{header_counts[header]}")
                else:
                    header_counts[header] = 1
                    cleaned_headers.append(header)
            
            # Create DataFrame with cleaned headers
            ipe_data = ipe_values[1:]  # Skip header row
            ipe_df = pd.DataFrame(ipe_data, columns=cleaned_headers)
            ipe_df['Date Received'] = pd.to_datetime(ipe_df['Date Received'], errors='coerce')
            ipe_df['Day of Case Note'] = pd.to_datetime(ipe_df['Day of Case Note'], errors='coerce')
        else:
            ipe_df = pd.DataFrame()
        
        return grit_df, ipe_df, None  # Return success with no error
        
    except Exception as e:
        error_msg = str(e)
        # Check if it's a quota error
        if "429" in error_msg or "Quota exceeded" in error_msg:
            return pd.DataFrame(), pd.DataFrame(), "quota_exceeded"
        else:
            return pd.DataFrame(), pd.DataFrame(), error_msg

# Initialize session state for data
if "data_last_fetched" not in st.session_state:
    st.session_state.data_last_fetched = 0

# Check if we should fetch fresh data
current_time = time.time()
time_since_fetch = current_time - st.session_state.data_last_fetched

# Fetch data with caching and error handling
grit_df, ipe_df, fetch_error = fetch_google_sheets_data()

if fetch_error == "quota_exceeded":
    st.warning("""
    ⚠️ **Google Sheets API Quota Exceeded**
    
    We've temporarily hit the API limit. The dashboard is showing cached data. 
    Please wait a few minutes and refresh the page, or use the refresh button below.
    """)
    
    # Show refresh button
    if st.button("🔄 Refresh Data", key="refresh_data"):
        # Clear cache and refetch
        fetch_google_sheets_data.clear()
        st.session_state.data_last_fetched = 0
        st.rerun()
        
elif fetch_error:
    st.error(f"Error fetching data from Google Sheets: {fetch_error}")
    if st.button("🔄 Retry", key="retry_fetch"):
        fetch_google_sheets_data.clear()
        st.session_state.data_last_fetched = 0
        st.rerun()

# Update last fetch time
st.session_state.data_last_fetched = current_time

# Function to get worksheets for write operations
def get_worksheets():
    """Get worksheet objects for write operations"""
    try:
        spreadsheet1 = client.open('Referral Information')
        worksheet1 = spreadsheet1.worksheet('GRIT')
        worksheet2 = spreadsheet1.worksheet('IPE')
        return worksheet1, worksheet2, None
    except Exception as e:
        return None, None, str(e)

# Get worksheets for write operations
worksheet1, worksheet2, worksheet_error = get_worksheets()
if worksheet_error:
    st.error(f"Error accessing worksheets: {worksheet_error}")

def format_phone(phone_str):
    # Remove non-digit characters
    digits = re.sub(r'\D', '', phone_str)
    
    # Format if it's 10 digits long
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    elif len(digits) == 11 and digits.startswith("1"):
        return f"+1 ({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
    else:
        return phone_str  # Return original if not standard length

# Apply formatting
#df["Phone Number"] = df["Phone Number"].astype(str).apply(format_phone)

def send_email_mailjet(to_email, subject, body):
    api_key = st.secrets["mailjet"]["api_key"]
    api_secret = st.secrets["mailjet"]["api_secret"]
    sender = st.secrets["mailjet"]["sender"]

    mailjet = Client(auth=(api_key, api_secret), version='v3.1')

    data = {
        'Messages': [
            {
                "From": {
                    "Email": sender,
                    "Name": "PWC GRIT System"
                },
                "To": [
                    {
                        "Email": to_email,
                        "Name": to_email.split("@")[0]
                    }
                ],
                "Subject": subject,
                "TextPart": body
            }
        ]
    }

    try:
        result = mailjet.send.create(data=data)
        #if result.status_code == 200:
            #st.success(f"📤 Email sent to {to_email}")
        #else:
            #st.warning(f"❌ Failed to email {to_email}: {result.status_code} - {result.json()}")
    except Exception as e:
        st.error(f"❗ Mailjet error: {e}")


# --- Load user database from Streamlit secrets
def load_users():
    """Load users from Streamlit secrets"""
    try:
        # Access nested secrets structure
        if "users" not in st.secrets:
            st.error("⚠️ User credentials not found in secrets. Please configure secrets.toml file.")
            st.stop()
            return {}
        
        users_secret = st.secrets["users"]
        # Convert secrets structure to the same format as before
        users = {}
        for email, roles in users_secret.items():
            users[email] = {}
            for role, creds in roles.items():
                # Streamlit secrets are dict-like, so we can access directly
                users[email][role] = {
                    "password": str(creds.get("password", "")),
                    "name": str(creds.get("name", ""))
                }
        return users
    except KeyError:
        st.error("⚠️ User credentials not properly configured in secrets. Please check secrets.toml file.")
        st.stop()
        return {}
    except Exception as e:
        st.error(f"❌ Error loading user credentials: {str(e)}")
        st.info("💡 Make sure your .streamlit/secrets.toml file is properly configured.")
        st.stop()
        return {}

USERS = load_users()

# --- Initialize session state
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False
if "role" not in st.session_state:
    st.session_state.role = None
if "user_email" not in st.session_state:
    st.session_state.user_email = ""

# --- Role selection
if st.session_state.role is None:
    #st.image("Georgetown_logo_blueRGB.png",width=200)
    #st.title("Welcome to the GU Technical Assistance Provider System")
    st.markdown(
        """
        <div style='
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            background: #f8f9fa;
            padding: 2em 0 1em 0;
            border-radius: 18px;
            box-shadow: 0 4px 24px rgba(0,0,0,0.07);
            margin-bottom: 2em;
        '>
            <img src='https://github.com/JiaqinWu/GRIT_Website/raw/main/logo1.png' width='200' style='margin-bottom: 1em;'/>
            <h1 style='
                color: #1a237e;
                font-family: "Segoe UI", "Arial", sans-serif;
                font-weight: 700;
                margin: 0;
                font-size: 2.2em;
                text-align: center;
            '>Welcome to the GRIT Monitor System</h1>
        </div>
        """,
        unsafe_allow_html=True
    )

    role = st.selectbox(
        "Select your dashboard",
        ["GRIT", "IPE"],
        index=None,
        placeholder="Select option..."
    )

    if role:
        st.session_state.role = role
        st.rerun()

# --- Show view based on role
else:
    st.sidebar.markdown(
        f"""
        <div style='
            background: #f8f9fa;
            border-radius: 12px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.04);
            padding: 1.2em 1em 1em 1em;
            margin-bottom: 1.5em;
            text-align: center;
            font-family: Arial, "Segoe UI", sans-serif;
        '>
            <span style='
                font-size: 1.15em;
                font-weight: 700;
                color: #1a237e;
                letter-spacing: 0.5px;
            '>
                Dashboard: {st.session_state.role}
            </span>
        </div>
        """,
        unsafe_allow_html=True
    )

    col_switch, col_refresh = st.sidebar.columns(2)
    
    with col_switch:
        st.sidebar.button("🔄 Switch Dashboard", on_click=lambda: st.session_state.update({
            "authenticated": False,
            "role": None,
            "user_email": ""
        }))
    
    with col_refresh:
        if st.sidebar.button("📊 Refresh Data", key="sidebar_refresh"):
            fetch_google_sheets_data.clear()
            st.session_state.data_last_fetched = 0
            st.rerun()
    


    st.markdown("""
        <style>
        .stButton > button {
            width: 100%;
            background-color: #cdb4db;
            color: black;
            font-family: Arial, "Segoe UI", sans-serif;
            font-weight: 600;
            border-radius: 8px;
            padding: 0.6em;
            margin-top: 1em;
            transition: background 0.2s;
        }
        .stButton > button:hover {
            background-color: #b197fc;
            color: #222;
        }
        </style>
    """, unsafe_allow_html=True)

    if not st.session_state.authenticated:
        st.subheader("🔐 Login Required")

        email = st.text_input("Email")
        password = st.text_input("Password", type="password")
        login = st.button("Login")

        if login:
            user_roles = USERS.get(email)
            if user_roles and st.session_state.role in user_roles:
                user = user_roles[st.session_state.role]
                if user["password"] == password:
                    st.session_state.authenticated = True
                    st.session_state.user_email = email
                    st.success("Login successful!")
                    st.rerun()
                else:
                    st.error("Invalid credentials or role mismatch.")
            else:
                st.error("Invalid credentials or role mismatch.")

    else:
        if st.session_state.role == "GRIT":
            user_info = USERS.get(st.session_state.user_email)
            GRIT_name = user_info["GRIT"]["name"]
            st.markdown(
                """
                <div style='
                    display: flex;
                    flex-direction: column;
                    align-items: center;
                    justify-content: center;
                    background: #f8f9fa;
                    padding: 2em 0 1em 0;
                    border-radius: 18px;
                    box-shadow: 0 4px 24px rgba(0,0,0,0.07);
                    margin-bottom: 2em;
                '>
                    <img src='https://github.com/JiaqinWu/GRIT_Website/raw/main/logo1.png' width='200' style='margin-bottom: 1em;'/>
                    <h1 style='
                        color: #1a237e;
                        font-family: "Segoe UI", "Arial", sans-serif;
                        font-weight: 700;
                        margin: 0;
                        font-size: 2.2em;
                        text-align: center;
                    '>📬 GRIT Dashboard</h1>
                </div>
                """,
                unsafe_allow_html=True
            )
            # Personalized greeting
            if user_info and "GRIT" in user_info:
                st.markdown(f"""
                <div style='                      
                background: #f8f9fa;                        
                border-radius: 12px;                        
                box-shadow: 0 2px 8px rgba(0,0,0,0.04);                        
                padding: 1.2em 1em 1em 1em;                        
                margin-bottom: 1.5em;                        
                text-align: center;                        
                font-family: Arial, "Segoe UI", sans-serif;                    
                '>
                    <span style='                           
                    font-size: 1.15em;
                    font-weight: 700;
                    color: #1a237e;
                    letter-spacing: 0.5px;'>
                        👋 Welcome, {GRIT_name}!
                    </span>
                </div>
                """, unsafe_allow_html=True)
            col1, col2, col3 = st.columns(3)
            referral_num_grit = grit_df['Youth Name'].nunique()
            # create column span
            today = datetime.today()
            last_year = today - timedelta(days=365)
            last_month = today - timedelta(days=30)
            pastyear_request = grit_df[grit_df['Date'] >= last_year].shape[0]
            pastmonth_request = grit_df[grit_df['Date'] >= last_month].shape[0]

            col1.metric(label="# of Total Referrals", value= millify(referral_num_grit, precision=2))
            col2.metric(label="# of Referrals from Last One Year", value= millify(pastyear_request, precision=2))
            col3.metric(label="# of Referrals from Last 30 Days", value= millify(pastmonth_request, precision=2))
            style_metric_cards(border_left_color="#DBF227")

            col4, col5 = st.columns(2)
            with col4:
                st.markdown('##### Time-series Plot of New Referrals:')
                
                # Filter data for current calendar year
                current_year = datetime.now().year
                current_year_data = grit_df[grit_df['Date'].dt.year == current_year].copy()
                
                if not current_year_data.empty:
                    # Group by month and count referrals
                    monthly_referrals = current_year_data.groupby(current_year_data['Date'].dt.month).size().reset_index()
                    monthly_referrals.columns = ['Month', 'Count']
                    
                    # Create month names for better display
                    month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                                 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
                    
                    # Get current month and create dataset only up to current month
                    current_month = datetime.now().month
                    monthly_referrals_complete = pd.DataFrame({
                        'Month': range(1, current_month + 1),
                        'Month_Name': month_names[:current_month],
                        'Count': 0
                    })
                    
                    # Update with actual data
                    for _, row in monthly_referrals.iterrows():
                        month_num = row['Month']
                        count = row['Count']
                        monthly_referrals_complete.loc[monthly_referrals_complete['Month'] == month_num, 'Count'] = count
                    
                    monthly_referrals_complete['Count'] = monthly_referrals_complete['Count'].astype(int)
                    
                    
                    # Create the chart with proper month ordering
                    chart = alt.Chart(monthly_referrals_complete).mark_line(point=True, strokeWidth=3).encode(
                        x=alt.X('Month_Name:O', title='Month', 
                               sort=month_names[:current_month],
                               axis=alt.Axis(labelAngle=0)),
                        y=alt.Y('Count:Q', title='Number of Referrals'),
                        tooltip=['Month_Name', 'Count']
                    ).properties(
                        width=400,
                        height=300,
                        title=f'Monthly New Referrals - {current_year}'
                    ).interactive()
                    
                    st.altair_chart(chart, use_container_width=True)
                else:
                    st.info(f"No new referrals found for {current_year}")
            with col5:
                st.markdown('##### Time-series Plot of New Comments:')

                # Filter data for current calendar year
                current_year = datetime.now().year
                
                # Check if 'Day of Case Note' column exists and convert to datetime
                if 'Day of Case Note' in grit_df.columns:
                    # Convert to datetime if not already, specifying format to avoid warnings
                    grit_df['Day of Case Note'] = pd.to_datetime(grit_df['Day of Case Note'], format='%m/%d/%Y', errors='coerce')
                    current_year_data = grit_df[grit_df['Day of Case Note'].dt.year == current_year].copy()
                else:
                    current_year_data = pd.DataFrame()  # Empty dataframe if column doesn't exist
                
                if not current_year_data.empty:
                    # Group by month and count comments
                    monthly_comments = current_year_data.groupby(current_year_data['Day of Case Note'].dt.month).size().reset_index()
                    monthly_comments.columns = ['Month', 'Count']
                    
                    # Create month names for better display
                    month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                                 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
                    
                    # Get current month and create dataset only up to current month
                    current_month = datetime.now().month
                    monthly_comments_complete = pd.DataFrame({
                        'Month': range(1, current_month + 1),
                        'Month_Name': month_names[:current_month],
                        'Count': 0
                    })
                    
                    # Update with actual data
                    for _, row in monthly_comments.iterrows():
                        month_num = row['Month']
                        count = row['Count']
                        monthly_comments_complete.loc[monthly_comments_complete['Month'] == month_num, 'Count'] = count
                    
                    monthly_comments_complete['Count'] = monthly_comments_complete['Count'].astype(int)
                    

                    # Create the chart with proper month ordering
                    chart = alt.Chart(monthly_comments_complete).mark_line(point=True, strokeWidth=3).encode(
                        x=alt.X('Month_Name:O', title='Month',
                               sort=month_names[:current_month],
                               axis=alt.Axis(labelAngle=0)),
                        y=alt.Y('Count:Q', title='Number of Comments'),
                        tooltip=['Month_Name', 'Count']
                    ).properties(
                        width=400,
                        height=300,
                        title=f'Monthly New Comments - {current_year}'
                    ).interactive()
                
                    st.altair_chart(chart, use_container_width=True)
                else:
                    st.info(f"No new comments found for {current_year}")

            with st.expander("📝 **Add New Referral**"):
                st.markdown("### 📝 Add New Referral")
                with st.form(key="add_referral_form"):
                    date_referral = st.date_input("Date of Referral:", value=datetime.today().date())
                    referral_agent = st.text_input("Referring Agent:")
                    agency = st.text_input("Agency:")
                    youth_name = st.text_input("Youth Name:")
                    dob_age = st.text_input("DOB/Age:")
                    day_of_case_note = st.date_input("Day of Case Note:", value=datetime.today().date())
                    case_notes = st.text_area("Enter your note:", height=100, placeholder="Type your note here...")
                    submit_button = st.form_submit_button("➕ Add Referral")
                    
                    if submit_button:
                        if youth_name.strip() and case_notes.strip():
                            try:
                                new_row_data = {
                                    'Youth Name': youth_name,
                                    'Date': date_referral.strftime('%m/%d/%Y'),
                                    'Referring Agent': referral_agent,
                                    'Agency': agency,
                                    'DOB/Age': dob_age,
                                    'Day of Case Note': day_of_case_note.strftime('%m/%d/%Y'),
                                    'Case Notes': case_notes
                                }
                                # Align to actual sheet headers to avoid mismatch with cleaned DataFrame headers
                                sheet_headers = worksheet1.row_values(1)
                                new_row = [new_row_data.get(col, '') for col in sheet_headers]
                                worksheet1.append_row(new_row, value_input_option='USER_ENTERED')
                                
                                # Clear cache to show updated data
                                fetch_google_sheets_data.clear()
                                st.session_state.data_last_fetched = 0
                                
                                st.success("✅ Referral added successfully!")

                                coordinator_emails = ["jkooyoomjian@pwcgov.org"]
                                subject = f"New Referral Submitted to GRIT program: {youth_name}"

                                for email in coordinator_emails:
                                    try:
                                        coordinator_name = USERS[email]["GRIT"]["name"]
                                        personalized_body = f"""Hi {coordinator_name},

                    A new referral has been submitted to GRIT program:

                    Youth Name: {youth_name}
                    Date: {date_referral.strftime('%m/%d/%Y')}
                    Referring Agent: {referral_agent}
                    Agency: {agency}
                    DOB/Age: {dob_age}
                    Day of Case Note: {day_of_case_note.strftime('%m/%d/%Y')}
                    Case Notes: {case_notes}

                    Please review via the PWC GRIT Dashboard: https://gritwebsitepwc.streamlit.app/.
                    Contact JWu@pwcgov.org for questions.

                    Best,
                    PWC GRIT Dashboard
                    """
                                        send_email_mailjet(
                                            to_email=email,
                                            subject=subject,
                                            body=personalized_body,
                                        )
                                    except Exception as e:
                                        st.warning(f"⚠️ Failed to send email to coordinator {email}: {e}")
                                
                                time.sleep(3)
                                st.rerun()
                            except Exception as e:
                                st.error(f"❌ Error adding referral: {str(e)}")
                        else:
                            st.warning("⚠️ Please fill in Youth Name and Case Notes before submitting.")
                        

            with st.expander("📝 **Add/Edit/Delete Note**"):
                # Add client filter in sidebar
                st.markdown("### 🔍 Filter by Youth")
                # Filter out NA, blank, and whitespace-only names
                unique_youths = sorted([name for name in grit_df['Youth Name'].dropna().unique() 
                                       if isinstance(name, str) and name.strip()])
                
                if not unique_youths:
                    st.warning("⚠️ No valid youth names found in the dataset.")
                    selected_youth = None
                else:
                    selected_youth = st.selectbox(
                        "Select a youth:",
                        unique_youths,
                        index=0
                    )
                
                
                # Filter data based on selected client
                if selected_youth is None:
                    st.warning("⚠️ Please select a valid youth name.")
                    filtered_df = pd.DataFrame()
                else:
                    filtered_df = grit_df[grit_df['Youth Name'] == selected_youth].copy()
                
                if not filtered_df.empty and selected_youth is not None:
                    st.markdown("#### 📋 Youth Information")
                    
                    # Display main client information in narrative format
                    main_columns = ['Youth Name', 'Date', 'Referring Agent', 'Agency', 'DOB/Age']
                    
                    # Filter columns that exist in the dataframe
                    available_columns = [col for col in main_columns if col in filtered_df.columns]
                    
                    # Create narrative display
                    narrative_text = ""
                    for col in available_columns:
                        if col == 'Date':
                            # Special formatting for Date - YYYY-MM-DD format
                            value = filtered_df[col].iloc[0] if not filtered_df[col].isna().all() else "Not specified"
                            if pd.notna(value) and value != "Not specified":
                                try:
                                    # Convert to datetime and format as YYYY-MM-DD
                                    if isinstance(value, str):
                                        date_obj = pd.to_datetime(value, errors='coerce')
                                    else:
                                        date_obj = value
                                    if pd.notna(date_obj):
                                        formatted_date = date_obj.strftime('%Y-%m-%d')
                                        narrative_text += f"**{col}:** {formatted_date}\n"
                                    else:
                                        narrative_text += f"**{col}:** {value}\n"
                                except:
                                    narrative_text += f"**{col}:** {value}\n"
                            else:
                                narrative_text += f"**{col}:** Not specified\n"
                        else:
                            value = filtered_df[col].iloc[0] if not filtered_df[col].isna().all() else "Not specified"
                            if pd.isna(value) or value == "":
                                value = "Not specified"
                            narrative_text += f"**{col}:** {value}\n"
                    
                    # Display each line separately with proper spacing
                    lines = narrative_text.strip().split('\n')
                    for line in lines:
                        if line.strip():  # Only display non-empty lines
                            st.markdown(line)
                            st.markdown("")  # Add extra spacing between lines
                    
                    # Display case notes separately
                    st.markdown("#### 📝 Case Notes")
                    case_notes_columns = ['Day of Case Note', 'Case Notes']
                    case_notes_available = [col for col in case_notes_columns if col in filtered_df.columns]
                    
                    if case_notes_available:
                        # Track original sheet row for reliable edit/delete
                        filtered_df['_sheet_row'] = filtered_df.index + 2
                        case_notes_df = filtered_df[case_notes_available + ['_sheet_row']].dropna(subset=case_notes_available, how='all')
                        if not case_notes_df.empty:
                            # Reset index to remove index column from display
                            case_notes_df_display = case_notes_df.reset_index(drop=True)
                            
                            # Format the display dataframe for better date presentation
                            case_notes_df_display_formatted = case_notes_df_display.copy()
                            if 'Day of Case Note' in case_notes_df_display_formatted.columns:
                                case_notes_df_display_formatted['Day of Case Note'] = case_notes_df_display_formatted['Day of Case Note'].apply(
                                    lambda x: pd.to_datetime(x, errors='coerce').strftime('%Y-%m-%d') if pd.notna(pd.to_datetime(x, errors='coerce')) else str(x)
                                )
                            
                            # Sort notes chronologically by Day of Case Note
                            case_notes_df_display_sorted = case_notes_df_display.copy()
                            if 'Day of Case Note' in case_notes_df_display_sorted.columns:
                                # Convert to datetime for proper sorting
                                case_notes_df_display_sorted['Day of Case Note'] = pd.to_datetime(case_notes_df_display_sorted['Day of Case Note'], format='%m/%d/%Y', errors='coerce')
                                # Sort by date (oldest first)
                                case_notes_df_display_sorted = case_notes_df_display_sorted.sort_values('Day of Case Note', na_position='last')
                                # Convert back to string for display
                                case_notes_df_display_sorted['Day of Case Note'] = case_notes_df_display_sorted['Day of Case Note'].apply(
                                    lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else 'No date'
                                )
                            
                            # Add edit functionality
                            st.markdown("**Select a comment to edit or delete:**")
                            comment_options = []
                            option_sheet_rows = []
                            for idx, row in case_notes_df_display_sorted.iterrows():
                                date_note = row.get('Day of Case Note', 'No date')
                                case_note = row.get('Case Notes', 'No note')
                                sheet_row = row.get('_sheet_row', None)
                                
                                # Format date to YYYY-MM-DD if it's a valid date
                                if pd.notna(date_note) and date_note != 'No date' and str(date_note) != 'NaT':
                                    try:
                                        if isinstance(date_note, str):
                                            # Try to parse the date string and format it
                                            parsed_date = pd.to_datetime(date_note, errors='coerce')
                                            if pd.notna(parsed_date):
                                                formatted_date = parsed_date.strftime('%Y-%m-%d')
                                            else:
                                                formatted_date = 'No date'
                                        else:
                                            # If it's already a datetime object
                                            if pd.notna(date_note):
                                                formatted_date = date_note.strftime('%Y-%m-%d')
                                            else:
                                                formatted_date = 'No date'
                                    except:
                                        formatted_date = 'No date'
                                else:
                                    formatted_date = 'No date'
                                
                                # Truncate long notes for display
                                display_note = case_note[:50] + "..." if len(case_note) > 50 else case_note
                                comment_options.append(f"{formatted_date}: {display_note}")
                                option_sheet_rows.append(int(sheet_row) if sheet_row is not None and str(sheet_row).isdigit() else None)
                            
                            if comment_options:
                                # Edit comment section (first line)
                                selected_comment_idx = st.selectbox(
                                    "Choose comment to edit:",
                                    range(len(comment_options)),
                                    format_func=lambda x: comment_options[x],
                                    key=f"edit_comment_{selected_youth}"
                                )
                                if st.button("✏️ Edit Selected Comment", key=f"edit_btn_{selected_youth}", use_container_width=True):
                                    st.session_state[f"editing_comment_{selected_youth}"] = selected_comment_idx
                                    st.session_state[f"edit_note_date_{selected_youth}"] = case_notes_df_display_sorted.iloc[selected_comment_idx]['Day of Case Note']
                                    st.session_state[f"edit_note_text_{selected_youth}"] = case_notes_df_display_sorted.iloc[selected_comment_idx]['Case Notes']
                                    st.rerun()
                                
                                st.markdown("<br>", unsafe_allow_html=True)  # Add spacing
                                
                                # Delete comment section (second line)
                                selected_delete_idx = st.selectbox(
                                    "Choose comment to delete:",
                                    range(len(comment_options)),
                                    format_func=lambda x: comment_options[x],
                                    key=f"delete_comment_{selected_youth}"
                                )
                                if st.button("🗑️ Delete Selected Comment", key=f"delete_btn_{selected_youth}", use_container_width=True):
                                    st.session_state[f"deleting_comment_{selected_youth}"] = selected_delete_idx
                                    st.rerun()
                                
                                # Delete confirmation
                                if f"deleting_comment_{selected_youth}" in st.session_state:
                                    st.warning("⚠️ Are you sure you want to delete this comment? This action cannot be undone.")
                                    col_confirm, col_cancel_del = st.columns(2)
                                    
                                    with col_confirm:
                                        if st.button("✅ Confirm Delete", key=f"confirm_delete_{selected_youth}"):
                                            try:
                                                # Get sheet row directly from tracked mapping
                                                delete_idx = st.session_state[f"deleting_comment_{selected_youth}"]
                                                row_num = option_sheet_rows[delete_idx]
                                                if row_num is None:
                                                    raise ValueError("Unable to resolve sheet row for deletion.")
                                                
                                                # Delete the row by clearing it
                                                worksheet1.batch_clear([f'A{row_num}:Z{row_num}'])
                                                
                                                # Clear cache to show updated data
                                                fetch_google_sheets_data.clear()
                                                
                                                st.success(f"✅ Comment deleted successfully for {selected_youth}")
                                                
                                                # Clear session state
                                                if f"deleting_comment_{selected_youth}" in st.session_state:
                                                    del st.session_state[f"deleting_comment_{selected_youth}"]
                                                
                                                time.sleep(2)
                                                st.rerun()
                                                
                                            except Exception as e:
                                                st.error(f"❌ Error deleting comment: {str(e)}")
                                    
                                    with col_cancel_del:
                                        if st.button("❌ Cancel Delete", key=f"cancel_delete_{selected_youth}"):
                                            if f"deleting_comment_{selected_youth}" in st.session_state:
                                                del st.session_state[f"deleting_comment_{selected_youth}"]
                                            st.rerun()
                            
                            # Edit form
                            if f"editing_comment_{selected_youth}" in st.session_state:
                                st.markdown("#### ✏️ Edit Comment")
                                with st.form(key=f"edit_form_{selected_youth}"):
                                    # Handle NaT and invalid dates properly
                                    original_date = st.session_state[f"edit_note_date_{selected_youth}"]
                                    try:
                                        parsed_date = pd.to_datetime(original_date, errors='coerce')
                                        if pd.notna(parsed_date):
                                            default_date = parsed_date.date()
                                        else:
                                            default_date = datetime.today().date()
                                    except:
                                        default_date = datetime.today().date()
                                    
                                    edit_date = st.date_input(
                                        "Edit Date:",
                                        value=default_date,
                                        key=f"edit_date_{selected_youth}"
                                    )
                                    
                                    edit_note = st.text_area(
                                        "Edit your note:",
                                        value=st.session_state[f"edit_note_text_{selected_youth}"],
                                        height=100,
                                        key=f"edit_note_{selected_youth}"
                                    )
                                    
                                    col_save, col_cancel = st.columns(2)
                                    
                                    with col_save:
                                        save_button = st.form_submit_button("💾 Save Changes")
                                    
                                    with col_cancel:
                                        cancel_button = st.form_submit_button("❌ Cancel")
                                    
                                    if save_button:
                                        if edit_note.strip():
                                            try:
                                                # Resolve edit index and sheet row
                                                edit_idx = st.session_state[f"editing_comment_{selected_youth}"]
                                                row_num = option_sheet_rows[edit_idx]
                                                if row_num is None:
                                                    raise ValueError("Unable to resolve sheet row for update.")
                                                df_row_idx = int(row_num) - 2
                                                # Start from the full existing row to avoid wiping other fields
                                                current_row = grit_df.iloc[df_row_idx].copy() if 0 <= df_row_idx < len(grit_df) else pd.Series(index=grit_df.columns)
                                                current_row = current_row.reindex(grit_df.columns)
                                                current_row['Day of Case Note'] = edit_date.strftime('%m/%d/%Y')
                                                current_row['Case Notes'] = edit_note.strip()
                                                # Convert to list of strings
                                                updated_row = []
                                                for col in grit_df.columns:
                                                    value = current_row.get(col, '')
                                                    try:
                                                        if hasattr(value, 'strftime') and pd.notna(value):
                                                            updated_row.append(value.strftime('%m/%d/%Y'))
                                                        elif pd.isna(value) or str(value) == 'NaT':
                                                            updated_row.append('')
                                                        else:
                                                            updated_row.append(str(value))
                                                    except:
                                                        updated_row.append('')
                                                worksheet1.update(f'A{row_num}:Z{row_num}', [updated_row])
                                                
                                                # Clear cache to show updated data
                                                fetch_google_sheets_data.clear()
                                                
                                                st.success(f"✅ Comment updated successfully for {selected_youth}")
                                                
                                                # Clear session state
                                                if f"editing_comment_{selected_youth}" in st.session_state:
                                                    del st.session_state[f"editing_comment_{selected_youth}"]
                                                if f"edit_note_date_{selected_youth}" in st.session_state:
                                                    del st.session_state[f"edit_note_date_{selected_youth}"]
                                                if f"edit_note_text_{selected_youth}" in st.session_state:
                                                    del st.session_state[f"edit_note_text_{selected_youth}"]
                                                
                                                time.sleep(2)
                                                st.rerun()
                                                
                                            except Exception as e:
                                                st.error(f"❌ Error updating comment: {str(e)}")
                                        else:
                                            st.warning("⚠️ Please enter a note before saving.")
                                    
                                    if cancel_button:
                                        # Clear session state
                                        if f"editing_comment_{selected_youth}" in st.session_state:
                                            del st.session_state[f"editing_comment_{selected_youth}"]
                                        if f"edit_note_date_{selected_youth}" in st.session_state:
                                            del st.session_state[f"edit_note_date_{selected_youth}"]
                                        if f"edit_note_text_{selected_youth}" in st.session_state:
                                            del st.session_state[f"edit_note_text_{selected_youth}"]
                                        st.rerun()
                            
                            # Display the table with formatted dates (chronologically sorted)
                            case_notes_df_display_sorted = case_notes_df_display_sorted[['Day of Case Note', 'Case Notes']].sort_values('Day of Case Note', na_position='last')
                            st.table(case_notes_df_display_sorted)
                        else:
                            st.info("No case notes available for this youth.")
                    else:
                        st.info("Case notes columns not found in the dataset.")
                    
                    # Add new note section
                    st.markdown("#### 📝 Add New Note")
                    
                    with st.form(key=f"note_form_{selected_youth}"):
                         # Date selection for the new note
                        note_date = st.date_input(
                            "Note Date:",
                            value=datetime.today().date(),
                            key=f"note_date_{selected_youth}"
                        )
                        
                        # Text area for new note
                        new_note = st.text_area(
                            "Enter your note:",
                            height=100,
                            placeholder="Type your note here...",
                            key=f"new_note_{selected_youth}"
                        )
                        
                        # Add note button
                        submit_button = st.form_submit_button("➕ Add Note")
                        
                        if submit_button:
                            if not new_note.strip():
                                st.warning("⚠️ Please enter a note before adding.")
                            elif selected_youth is None or not selected_youth or not selected_youth.strip():
                                st.warning("⚠️ Please select a valid youth name before adding a note.")
                            else:
                                try:
                                    # Format the date as string (4-digit year)
                                    note_date_str = note_date.strftime('%m/%d/%Y')
                                    
                                    # Prepare the new row data
                                    new_row_data = {
                                        'Youth Name': selected_youth.strip(),
                                        'Day of Case Note': note_date_str,
                                        'Case Notes': new_note.strip()
                                    }
                                    
                                    # Add empty values for other columns to maintain structure
                                    for col in grit_df.columns:
                                        if col not in new_row_data:
                                            new_row_data[col] = ''
                                    
                                    # Convert to list in the correct order
                                    new_row = [new_row_data.get(col, '') for col in grit_df.columns]
                                    
                                    # Append to Google Sheets
                                    worksheet1.append_row(new_row)
                                    
                                    # Clear cache to show updated data
                                    fetch_google_sheets_data.clear()
                                    
                                    st.success(f"✅ Note added successfully for {selected_youth} on {note_date_str}")
                                    st.rerun()
                                    
                                except Exception as e:
                                    st.error(f"❌ Error adding note: {str(e)}")
                else:
                    st.warning(f"No data found for youth: {selected_youth}")


        elif st.session_state.role == "IPE":
            # Add staff content here
            user_info = USERS.get(st.session_state.user_email)
            ipe_name = user_info["IPE"]["name"] if user_info and "IPE" in user_info else None
            st.markdown(
                """
                <div style='
                    display: flex;
                    flex-direction: column;
                    align-items: center;
                    justify-content: center;
                    background: #f8f9fa;
                    padding: 2em 0 1em 0;
                    border-radius: 18px;
                    box-shadow: 0 4px 24px rgba(0,0,0,0.07);
                    margin-bottom: 2em;
                '>
                    <img src='https://github.com/JiaqinWu/GRIT_Website/raw/main/logo1.png' width='200' style='margin-bottom: 1em;'/>
                    <h1 style='
                        color: #1a237e;
                        font-family: "Segoe UI", "Arial", sans-serif;
                        font-weight: 700;
                        margin: 0;
                        font-size: 2.2em;
                        text-align: center;
                    '>👷 IPE Dashboard</h1>
                </div>
                """,
                unsafe_allow_html=True
            )
            #st.header("👷 Staff Dashboard")
            # Personalized greeting
            if ipe_name:
                st.markdown(f"""
                <div style='                      
                background: #f8f9fa;                        
                border-radius: 12px;                        
                box-shadow: 0 2px 8px rgba(0,0,0,0.04);                        
                padding: 1.2em 1em 1em 1em;                        
                margin-bottom: 1.5em;                        
                text-align: center;                        
                font-family: Arial, "Segoe UI", sans-serif;                    
                '>
                    <span style='                           
                    font-size: 1.15em;
                    font-weight: 700;
                    color: #1a237e;
                    letter-spacing: 0.5px;'>
                        👋 Welcome, {ipe_name}!
                    </span>
                </div>
                """, unsafe_allow_html=True)
        
            
            col1, col2, col3 = st.columns(3)
            has_client_col = 'Name of Client' in ipe_df.columns
            has_date_received_col = 'Date Received' in ipe_df.columns
            referral_num_ipe = ipe_df['Name of Client'].nunique() if has_client_col else 0
            # create column span
            today = datetime.today()
            last_year = today - timedelta(days=365)
            last_month = today - timedelta(days=30)
            if has_date_received_col:
                pastyear_request_ipe = ipe_df[ipe_df['Date Received'] >= last_year].shape[0]
                pastmonth_request_ipe = ipe_df[ipe_df['Date Received'] >= last_month].shape[0]
            else:
                pastyear_request_ipe = 0
                pastmonth_request_ipe = 0


            col1.metric(label="# of Total Referrals", value= millify(referral_num_ipe, precision=2))
            col2.metric(label="# of Referrals from Last One Year", value= millify(pastyear_request_ipe, precision=2))
            col3.metric(label="# of Referrals from Last 30 Days", value= millify(pastmonth_request_ipe, precision=2))
            style_metric_cards(border_left_color="#DBF227")

            col4, col5 = st.columns(2)
            with col4:
                st.markdown('##### Time-series Plot of New Referrals:')
                
                # Filter data for current calendar year
                current_year = datetime.now().year
                if has_date_received_col:
                    current_year_data = ipe_df[ipe_df['Date Received'].dt.year == current_year].copy()
                else:
                    current_year_data = pd.DataFrame()
                
                if not current_year_data.empty:
                    # Group by month and count referrals
                    monthly_referrals = current_year_data.groupby(current_year_data['Date Received'].dt.month).size().reset_index()
                    monthly_referrals.columns = ['Month', 'Count']
                    
                    # Create month names for better display
                    month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                                 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
                    
                    # Get current month and create dataset only up to current month
                    current_month = datetime.now().month
                    monthly_referrals_complete = pd.DataFrame({
                        'Month': range(1, current_month + 1),
                        'Month_Name': month_names[:current_month],
                        'Count': 0
                    })
                    
                    # Update with actual data
                    for _, row in monthly_referrals.iterrows():
                        month_num = row['Month']
                        count = row['Count']
                        monthly_referrals_complete.loc[monthly_referrals_complete['Month'] == month_num, 'Count'] = count
                    
                    monthly_referrals_complete['Count'] = monthly_referrals_complete['Count'].astype(int)
                    
                    
                    # Create the chart with proper month ordering
                    chart = alt.Chart(monthly_referrals_complete).mark_line(point=True, strokeWidth=3).encode(
                        x=alt.X('Month_Name:O', title='Month', 
                               sort=month_names[:current_month],
                               axis=alt.Axis(labelAngle=0)),
                        y=alt.Y('Count:Q', title='Number of Referrals'),
                        tooltip=['Month_Name', 'Count']
                    ).properties(
                        width=400,
                        height=300,
                        title=f'Monthly New Referrals - {current_year}'
                    ).interactive()
                    
                    st.altair_chart(chart, use_container_width=True)
                else:
                    st.info(f"No new referrals found for {current_year}")
            with col5:
                st.markdown('##### Time-series Plot of New Comments:')

                # Filter data for current calendar year
                current_year = datetime.now().year
                
                # Check if 'Day of Case Note' column exists and convert to datetime
                if 'Day of Case Note' in ipe_df.columns:
                    # Convert to datetime if not already, specifying format to avoid warnings
                    ipe_df['Day of Case Note'] = pd.to_datetime(ipe_df['Day of Case Note'], format='%m/%d/%Y', errors='coerce')
                    current_year_data = ipe_df[ipe_df['Day of Case Note'].dt.year == current_year].copy()
                else:
                    current_year_data = pd.DataFrame()  # Empty dataframe if column doesn't exist
                
                if not current_year_data.empty:
                    # Group by month and count comments
                    monthly_comments = current_year_data.groupby(current_year_data['Day of Case Note'].dt.month).size().reset_index()
                    monthly_comments.columns = ['Month', 'Count']
                    
                    # Create month names for better display
                    month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                                 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
                    
                    # Get current month and create dataset only up to current month
                    current_month = datetime.now().month
                    monthly_comments_complete = pd.DataFrame({
                        'Month': range(1, current_month + 1),
                        'Month_Name': month_names[:current_month],
                        'Count': 0
                    })
                    
                    # Update with actual data
                    for _, row in monthly_comments.iterrows():
                        month_num = row['Month']
                        count = row['Count']
                        monthly_comments_complete.loc[monthly_comments_complete['Month'] == month_num, 'Count'] = count
                    
                    monthly_comments_complete['Count'] = monthly_comments_complete['Count'].astype(int)
                    

                    # Create the chart with proper month ordering
                    chart = alt.Chart(monthly_comments_complete).mark_line(point=True, strokeWidth=3).encode(
                        x=alt.X('Month_Name:O', title='Month',
                               sort=month_names[:current_month],
                               axis=alt.Axis(labelAngle=0)),
                        y=alt.Y('Count:Q', title='Number of Comments'),
                        tooltip=['Month_Name', 'Count']
                    ).properties(
                        width=400,
                        height=300,
                        title=f'Monthly New Comments - {current_year}'
                    ).interactive()
                
                    st.altair_chart(chart, use_container_width=True)
                else:
                    st.info(f"No new comments found for {current_year}")

            with st.expander("📝 **Add New Referral**"):
                st.markdown("### 📝 Add New Referral")
                with st.form(key="add_referral_form"):
                    date_referral = st.date_input("Date of Referral:", value=datetime.today().date())
                    name_of_client = st.text_input("Name of Client:")
                    type = st.selectbox("Type of Referral:", options=["Second referral", "Referral to IPE and VPIP", "VPIP"])
                    referral_agent = st.text_input("Referring Agent:")
                    consent_signed_for_grit_nvfs = st.text_input("Consent Signed for GRIT/NVFS:")
                    case_manager = st.text_input("Case Manager:")
                    progress_reports_sent_to_referring_agent_cm = st.text_input("Progress Reports Sent to Referring Agent/CM")
                    day_of_case_note = st.date_input("Day of Case Note:", value=datetime.today().date())
                    case_notes = st.text_area("Enter your note:", height=100, placeholder="Type your note here...")
                    submit_button = st.form_submit_button("➕ Add Referral")
                    

                    if submit_button:
                        if name_of_client.strip() and case_notes.strip():
                            try:
                                new_row_data = {
                                    'Name of Client': name_of_client,
                                    "Type": type,
                                    'Date Received': date_referral.strftime('%m/%d/%Y'),
                                    'Referral Agent': referral_agent,
                                    'Consent Signed for GRIT/NVFS': consent_signed_for_grit_nvfs,
                                    'Case Manager': case_manager,
                                    'Progress Reports Sent to Referring Agent/CM': progress_reports_sent_to_referring_agent_cm,
                                    'Day of Case Note': day_of_case_note.strftime('%m/%d/%Y'),
                                    'Case Notes': case_notes
                                }
                                # Align to actual sheet headers to avoid mismatch with cleaned DataFrame headers
                                sheet_headers = worksheet2.row_values(1)
                                new_row = [new_row_data.get(col, '') for col in sheet_headers]
                                worksheet2.append_row(new_row, value_input_option='USER_ENTERED')
                                
                                # Clear cache to show updated data
                                fetch_google_sheets_data.clear()
                                st.session_state.data_last_fetched = 0
                                
                                st.success("✅ Referral added successfully!")

                                coordinator_emails = ["jkooyoomjian@pwcgov.org"]
                                subject = f"New Referral Submitted to IPE program: {name_of_client}"

                                for email in coordinator_emails:
                                    try:
                                        coordinator_name = USERS[email]["IPE"]["name"]
                                        personalized_body = f"""Hi {coordinator_name},

                    A new referral has been submitted to IPE program:

                    Name of Client: {name_of_client}
                    Type: {type}
                    Date Received: {date_referral.strftime('%m/%d/%Y')}
                    Referral Agent: {referral_agent}
                    Consent Signed for GRIT/NVFS: {consent_signed_for_grit_nvfs}
                    Case Manager: {case_manager}
                    Progress Reports Sent to Referring Agent/CM: {progress_reports_sent_to_referring_agent_cm}
                    Day of Case Note: {day_of_case_note.strftime('%m/%d/%Y')}
                    Case Notes: {case_notes}

                    Please review via the PWC IPE Dashboard: https://gritwebsitepwc.streamlit.app/.
                    Contact JWu@pwcgov.org for questions.

                    Best,
                    PWC GRIT Dashboard
                    """
                                        send_email_mailjet(
                                            to_email=email,
                                            subject=subject,
                                            body=personalized_body,
                                        )
                                    except Exception as e:
                                        st.warning(f"⚠️ Failed to send email to coordinator {email}: {e}")
                                
                                time.sleep(3)
                                st.rerun()
                            except Exception as e:
                                st.error(f"❌ Error adding referral: {str(e)}")
                        else:
                            st.warning("⚠️ Please fill in Name of Client and Case Notes before submitting.")


            # Add client filter in sidebar
            with st.expander("📝 **Add/Edit/Delete Note**"):
                st.markdown("### 🔍 Filter by Client")
                if not has_client_col or ipe_df.empty:
                    st.info("Client name column not found or no data available.")
                    selected_client = None
                else:
                    # Filter out NA, blank, and whitespace-only names
                    unique_clients = sorted([name for name in ipe_df['Name of Client'].dropna().unique() 
                                           if isinstance(name, str) and name.strip()])
                    
                    if not unique_clients:
                        st.warning("⚠️ No valid client names found in the dataset.")
                        selected_client = None
                    else:
                        selected_client = st.selectbox(
                            "Select a client:",
                            unique_clients,
                            index=0
                        )
                
                
                # Filter data based on selected client
                if selected_client is None:
                    st.warning("⚠️ Please select a valid client name.")
                    filtered_df = pd.DataFrame()
                else:
                    filtered_df = ipe_df[ipe_df['Name of Client'] == selected_client].copy()
                
                if not filtered_df.empty and selected_client is not None:
                    st.markdown("#### 📋 Client Information")
                    
                    # Display main client information in narrative format
                    main_columns = ['Name of Client', 'Type', 'Referral Agent', 'Date Received', 
                                'Service End Date', 'Consent Signed for GRIT/NVFS', 'Case Manager', 
                                'Progress Reports Sent to Referring Agent/CM']
                    
                    # Filter columns that exist in the dataframe
                    available_columns = [col for col in main_columns if col in filtered_df.columns]
                    
                    # Create narrative display
                    narrative_text = ""
                    for col in available_columns:
                        if col == 'Progress Reports Sent to Referring Agent/CM':
                            # Special handling for Progress Reports - collect all non-empty values from all rows
                            progress_reports = filtered_df[col].dropna().unique()
                            if len(progress_reports) > 0:
                                narrative_text += f"**{col}:**\n"
                                for report in progress_reports:
                                    if str(report).strip() and str(report).strip() != "nan":
                                        narrative_text += f"• {report}\n"
                            else:
                                narrative_text += f"**{col}:** Not specified\n"
                        elif col == 'Date Received':
                            # Special formatting for Date Received - YYYY-MM-DD format
                            value = filtered_df[col].iloc[0] if not filtered_df[col].isna().all() else "Not specified"
                            if pd.notna(value) and value != "Not specified":
                                try:
                                    # Convert to datetime and format as YYYY-MM-DD
                                    if isinstance(value, str):
                                        date_obj = pd.to_datetime(value, errors='coerce')
                                    else:
                                        date_obj = value
                                    if pd.notna(date_obj):
                                        formatted_date = date_obj.strftime('%Y-%m-%d')
                                        narrative_text += f"**{col}:** {formatted_date}\n"
                                    else:
                                        narrative_text += f"**{col}:** {value}\n"
                                except:
                                    narrative_text += f"**{col}:** {value}\n"
                            else:
                                narrative_text += f"**{col}:** Not specified\n"
                        else:
                            value = filtered_df[col].iloc[0] if not filtered_df[col].isna().all() else "Not specified"
                            if pd.isna(value) or value == "":
                                value = "Not specified"
                            narrative_text += f"**{col}:** {value}\n"
                    
                    # Display each line separately with proper spacing
                    lines = narrative_text.strip().split('\n')
                    for line in lines:
                        if line.strip():  # Only display non-empty lines
                            st.markdown(line)
                            st.markdown("")  # Add extra spacing between lines
                    
                    # Display case notes separately
                    st.markdown("#### 📝 Case Notes")
                    case_notes_columns = ['Day of Case Note', 'Case Notes']
                    case_notes_available = [col for col in case_notes_columns if col in filtered_df.columns]
                    
                    if case_notes_available:
                        # Track original sheet row for reliable edit/delete
                        filtered_df['_sheet_row'] = filtered_df.index + 2
                        case_notes_df = filtered_df[case_notes_available + ['_sheet_row']].dropna(subset=case_notes_available, how='all')
                        if not case_notes_df.empty:
                            # Reset index to remove index column from display
                            case_notes_df_display = case_notes_df.reset_index(drop=True)
                            
                            # Format the display dataframe for better date presentation
                            case_notes_df_display_formatted = case_notes_df_display.copy()
                            if 'Day of Case Note' in case_notes_df_display_formatted.columns:
                                case_notes_df_display_formatted['Day of Case Note'] = case_notes_df_display_formatted['Day of Case Note'].apply(
                                    lambda x: pd.to_datetime(x, errors='coerce').strftime('%Y-%m-%d') if pd.notna(pd.to_datetime(x, errors='coerce')) else str(x)
                                )
                            
                            # Sort notes chronologically by Day of Case Note
                            case_notes_df_display_sorted = case_notes_df_display.copy()
                            if 'Day of Case Note' in case_notes_df_display_sorted.columns:
                                # Convert to datetime for proper sorting
                                case_notes_df_display_sorted['Day of Case Note'] = pd.to_datetime(case_notes_df_display_sorted['Day of Case Note'], format='%m/%d/%Y', errors='coerce')
                                # Sort by date (oldest first)
                                case_notes_df_display_sorted = case_notes_df_display_sorted.sort_values('Day of Case Note', na_position='last')
                                # Convert back to string for display
                                case_notes_df_display_sorted['Day of Case Note'] = case_notes_df_display_sorted['Day of Case Note'].apply(
                                    lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else 'No date'
                                )
                            
                            # Add edit functionality
                            st.markdown("**Select a comment to edit or delete:**")
                            comment_options = []
                            option_sheet_rows = []
                            for idx, row in case_notes_df_display_sorted.iterrows():
                                date_note = row.get('Day of Case Note', 'No date')
                                case_note = row.get('Case Notes', 'No note')
                                sheet_row = row.get('_sheet_row', None)
                                
                                # Format date to YYYY-MM-DD if it's a valid date
                                if pd.notna(date_note) and date_note != 'No date' and str(date_note) != 'NaT':
                                    try:
                                        if isinstance(date_note, str):
                                            # Try to parse the date string and format it
                                            parsed_date = pd.to_datetime(date_note, errors='coerce')
                                            if pd.notna(parsed_date):
                                                formatted_date = parsed_date.strftime('%Y-%m-%d')
                                            else:
                                                formatted_date = 'No date'
                                        else:
                                            # If it's already a datetime object
                                            if pd.notna(date_note):
                                                formatted_date = date_note.strftime('%Y-%m-%d')
                                            else:
                                                formatted_date = 'No date'
                                    except:
                                        formatted_date = 'No date'
                                else:
                                    formatted_date = 'No date'
                                
                                # Truncate long notes for display
                                display_note = case_note[:50] + "..." if len(case_note) > 50 else case_note
                                comment_options.append(f"{formatted_date}: {display_note}")
                                option_sheet_rows.append(int(sheet_row) if sheet_row is not None and str(sheet_row).isdigit() else None)
                            
                            if comment_options:
                                # Edit comment section (first line)
                                selected_comment_idx = st.selectbox(
                                    "Choose comment to edit:",
                                    range(len(comment_options)),
                                    format_func=lambda x: comment_options[x],
                                    key=f"edit_comment_{selected_client}"
                                )
                                if st.button("✏️ Edit Selected Comment", key=f"edit_btn_{selected_client}", use_container_width=True):
                                    st.session_state[f"editing_comment_{selected_client}"] = selected_comment_idx
                                    st.session_state[f"edit_note_date_{selected_client}"] = case_notes_df_display_sorted.iloc[selected_comment_idx]['Day of Case Note']
                                    st.session_state[f"edit_note_text_{selected_client}"] = case_notes_df_display_sorted.iloc[selected_comment_idx]['Case Notes']
                                    st.rerun()
                                
                                st.markdown("<br>", unsafe_allow_html=True)  # Add spacing
                                
                                # Delete comment section (second line)
                                selected_delete_idx = st.selectbox(
                                    "Choose comment to delete:",
                                    range(len(comment_options)),
                                    format_func=lambda x: comment_options[x],
                                    key=f"delete_comment_{selected_client}"
                                )
                                if st.button("🗑️ Delete Selected Comment", key=f"delete_btn_{selected_client}", use_container_width=True):
                                    st.session_state[f"deleting_comment_{selected_client}"] = selected_delete_idx
                                    st.rerun()
                                
                                # Delete confirmation
                                if f"deleting_comment_{selected_client}" in st.session_state:
                                    st.warning("⚠️ Are you sure you want to delete this comment? This action cannot be undone.")
                                    col_confirm, col_cancel_del = st.columns(2)
                                    
                                    with col_confirm:
                                        if st.button("✅ Confirm Delete", key=f"confirm_delete_{selected_client}"):
                                            try:
                                                # Get sheet row directly from tracked mapping
                                                delete_idx = st.session_state[f"deleting_comment_{selected_client}"]
                                                row_num = option_sheet_rows[delete_idx]
                                                if row_num is None:
                                                    raise ValueError("Unable to resolve sheet row for deletion.")
                                                
                                                # Delete the row by clearing it
                                                worksheet2.batch_clear([f'A{row_num}:Z{row_num}'])
                                                
                                                # Clear cache to show updated data
                                                fetch_google_sheets_data.clear()
                                                
                                                st.success(f"✅ Comment deleted successfully for {selected_client}")
                                                
                                                # Clear session state
                                                if f"deleting_comment_{selected_client}" in st.session_state:
                                                    del st.session_state[f"deleting_comment_{selected_client}"]
                                                
                                                time.sleep(2)
                                                st.rerun()
                                                
                                            except Exception as e:
                                                st.error(f"❌ Error deleting comment: {str(e)}")
                                    
                                    with col_cancel_del:
                                        if st.button("❌ Cancel Delete", key=f"cancel_delete_{selected_client}"):
                                            if f"deleting_comment_{selected_client}" in st.session_state:
                                                del st.session_state[f"deleting_comment_{selected_client}"]
                                            st.rerun()
                            
                            # Edit form
                            if f"editing_comment_{selected_client}" in st.session_state:
                                st.markdown("#### ✏️ Edit Comment")
                                with st.form(key=f"edit_form_{selected_client}"):
                                    # Handle NaT and invalid dates properly
                                    original_date = st.session_state[f"edit_note_date_{selected_client}"]
                                    try:
                                        parsed_date = pd.to_datetime(original_date, errors='coerce')
                                        if pd.notna(parsed_date):
                                            default_date = parsed_date.date()
                                        else:
                                            default_date = datetime.today().date()
                                    except:
                                        default_date = datetime.today().date()
                                    
                                    edit_date = st.date_input(
                                        "Edit Date:",
                                        value=default_date,
                                        key=f"edit_date_{selected_client}"
                                    )
                                    
                                    edit_note = st.text_area(
                                        "Edit your note:",
                                        value=st.session_state[f"edit_note_text_{selected_client}"],
                                        height=100,
                                        key=f"edit_note_{selected_client}"
                                    )
                                    
                                    col_save, col_cancel = st.columns(2)
                                    
                                    with col_save:
                                        save_button = st.form_submit_button("💾 Save Changes")
                                    
                                    with col_cancel:
                                        cancel_button = st.form_submit_button("❌ Cancel")
                                    
                                    if save_button:
                                        if edit_note.strip():
                                            try:
                                                # Resolve edit index and sheet row
                                                edit_idx = st.session_state[f"editing_comment_{selected_client}"]
                                                row_num = option_sheet_rows[edit_idx]
                                                if row_num is None:
                                                    raise ValueError("Unable to resolve sheet row for update.")
                                                df_row_idx = int(row_num) - 2
                                                # Start from the full existing row to avoid wiping other fields
                                                current_row = ipe_df.iloc[df_row_idx].copy() if 0 <= df_row_idx < len(ipe_df) else pd.Series(index=ipe_df.columns)
                                                current_row = current_row.reindex(ipe_df.columns)
                                                current_row['Day of Case Note'] = edit_date.strftime('%m/%d/%Y')
                                                current_row['Case Notes'] = edit_note.strip()
                                                # Convert to list of strings
                                                updated_row = []
                                                for col in ipe_df.columns:
                                                    value = current_row.get(col, '')
                                                    try:
                                                        if hasattr(value, 'strftime') and pd.notna(value):
                                                            updated_row.append(value.strftime('%m/%d/%Y'))
                                                        elif pd.isna(value) or str(value) == 'NaT':
                                                            updated_row.append('')
                                                        else:
                                                            updated_row.append(str(value))
                                                    except:
                                                        updated_row.append('')
                                                worksheet2.update(f'A{row_num}:Z{row_num}', [updated_row])
                                                
                                                # Clear cache to show updated data
                                                fetch_google_sheets_data.clear()
                                                
                                                st.success(f"✅ Comment updated successfully for {selected_client}")
                                                
                                                # Clear session state
                                                if f"editing_comment_{selected_client}" in st.session_state:
                                                    del st.session_state[f"editing_comment_{selected_client}"]
                                                if f"edit_note_date_{selected_client}" in st.session_state:
                                                    del st.session_state[f"edit_note_date_{selected_client}"]
                                                if f"edit_note_text_{selected_client}" in st.session_state:
                                                    del st.session_state[f"edit_note_text_{selected_client}"]
                                                
                                                time.sleep(2)
                                                st.rerun()
                                                
                                            except Exception as e:
                                                st.error(f"❌ Error updating comment: {str(e)}")
                                        else:
                                            st.warning("⚠️ Please enter a note before saving.")
                                    
                                    if cancel_button:
                                        # Clear session state
                                        if f"editing_comment_{selected_client}" in st.session_state:
                                            del st.session_state[f"editing_comment_{selected_client}"]
                                        if f"edit_note_date_{selected_client}" in st.session_state:
                                            del st.session_state[f"edit_note_date_{selected_client}"]
                                        if f"edit_note_text_{selected_client}" in st.session_state:
                                            del st.session_state[f"edit_note_text_{selected_client}"]
                                        st.rerun()
                            
                            # Display the table with formatted dates (chronologically sorted)
                            case_notes_df_display_sorted = case_notes_df_display_sorted[['Day of Case Note', 'Case Notes']].sort_values('Day of Case Note', na_position='last')
                            st.table(case_notes_df_display_sorted)
                        else:
                            st.info("No case notes available for this client.")
                    else:
                        st.info("Case notes columns not found in the dataset.")
                    
                    # Add new note section
                    st.markdown("#### 📝 Add New Note")
                    
                    with st.form(key=f"note_form_{selected_client}"):
                        # Date selection for the new note
                        note_date = st.date_input(
                            "Note Date:",
                            value=datetime.today().date(),
                            key=f"note_date_{selected_client}"
                        )
                        
                        # Text area for new note
                        new_note = st.text_area(
                            "Enter your note:",
                            height=100,
                            placeholder="Type your note here...",
                            key=f"new_note_{selected_client}"
                        )
                        
                        # Add note button
                        submit_button = st.form_submit_button("➕ Add Note")
                        
                        if submit_button:
                            if not new_note.strip():
                                st.warning("⚠️ Please enter a note before adding.")
                            elif selected_client is None or not selected_client or not selected_client.strip():
                                st.warning("⚠️ Please select a valid client name before adding a note.")
                            else:
                                try:
                                    # Format the date as string (4-digit year)
                                    note_date_str = note_date.strftime('%m/%d/%Y')
                                    
                                    # Prepare the new row data
                                    new_row_data = {
                                        'Name of Client': selected_client.strip(),
                                        'Day of Case Note': note_date_str,
                                        'Case Notes': new_note.strip()
                                    }
                                    
                                    # Add empty values for other columns to maintain structure
                                    for col in ipe_df.columns:
                                        if col not in new_row_data:
                                            new_row_data[col] = ''
                                    
                                    # Convert to list in the correct order
                                    new_row = [new_row_data.get(col, '') for col in ipe_df.columns]
                                    
                                    # Append to Google Sheets
                                    worksheet2.append_row(new_row)
                                    
                                    # Clear cache to show updated data
                                    fetch_google_sheets_data.clear()
                                    
                                    st.success(f"✅ Note added successfully for {selected_client} on {note_date_str}")
                                    st.rerun()
                                    
                                except Exception as e:
                                    st.error(f"❌ Error adding note: {str(e)}")
                else:
                    st.warning(f"No data found for client: {selected_client}")