import streamlit as st
import pandas as pd
import sqlite3
import os
from pathlib import Path
from tableau_data_app import TableauConnector
from typing import List
from datetime import datetime
from data_analyzer import show_analysis_tab
from dotenv import load_dotenv
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import io
import time
from report_manager import ReportManager

# Load environment variables at the very start
load_dotenv()

# Verify API key is loaded
api_key = os.getenv('OPENAI_API_KEY')
if not api_key or api_key == 'your-api-key-here':
    st.error("OpenAI API key not found or invalid. Please check your .env file.")
    st.stop()

class DatabaseManager:
    def __init__(self):
        # Create data directory if it doesn't exist
        self.data_dir = Path("data")
        self.data_dir.mkdir(exist_ok=True)
        
        # SQLite database path
        self.db_path = self.data_dir / "tableau_data.db"
        self.db_url = f"sqlite:///{self.db_path}"
        
    def ensure_database_running(self):
        """Initialize SQLite database"""
        try:
            # Test database connection
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                # Create a test table to verify connection
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS app_info (
                        key TEXT PRIMARY KEY,
                        value TEXT
                    )
                """)
                conn.commit()
            
            st.success("✅ Database is ready!")
            return self.db_url
            
        except Exception as e:
            st.error(f"""
            Failed to initialize database. Error: {str(e)}
            
            Please ensure:
            1. The application has write permissions to the data directory
            2. Sufficient disk space is available
            3. SQLite is working properly
            """)
            return None
    
    def list_tables(self):
        """List all tables in the database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                tables = cursor.fetchall()
                return [table[0] for table in tables if table[0] != 'app_info']
        except Exception as e:
            st.error(f"Failed to list tables: {str(e)}")
            return []
    
    def get_table_preview(self, table_name):
        """Get preview of table data"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                return pd.read_sql(f"SELECT * FROM '{table_name}' LIMIT 5", conn)
        except Exception as e:
            st.error(f"Failed to preview table: {str(e)}")
            return pd.DataFrame()

    def get_table_row_count(self, table_name):
        """Get the total number of rows in a table"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(f"SELECT COUNT(*) FROM '{table_name}'")
                return cursor.fetchone()[0]
        except Exception as e:
            st.error(f"Failed to get row count: {str(e)}")
            return 0

def generate_table_name(workbook_name: str, view_names: List[str]) -> str:
    """Generate a unique table name based on workbook and views"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    # Clean workbook name to be SQL friendly
    clean_wb_name = "".join(c if c.isalnum() else "_" for c in workbook_name)
    return f"{clean_wb_name}_{timestamp}"

def show_saved_data(db_manager):
    """Show saved data in the sidebar with enhanced display"""
    st.sidebar.markdown("---")
    st.sidebar.subheader("Saved Data")
    
    tables = db_manager.list_tables()
    if tables:
        # Group tables by workbook
        table_groups = {}
        for table in tables:
            workbook_name = table.split('_')[0]  # Get workbook name from table name
            if workbook_name not in table_groups:
                table_groups[workbook_name] = []
            table_groups[workbook_name].append(table)
        
        # Display tables grouped by workbook
        for workbook, workbook_tables in table_groups.items():
            with st.sidebar.expander(f"📊 {workbook}"):
                for table in workbook_tables:
                    st.write(f"📑 {table}")
                    preview = db_manager.get_table_preview(table)
                    if not preview.empty:
                        st.dataframe(preview, use_container_width=True)
                        row_count = db_manager.get_table_row_count(table)
                        st.caption(f"Total rows: {row_count}")
    else:
        st.sidebar.info("No saved data yet")

def save_to_database(df: pd.DataFrame, table_name: str, db_path: str):
    """Save DataFrame to SQLite database"""
    try:
        # Create database connection
        with sqlite3.connect(db_path) as conn:
            # Save data
            df.to_sql(table_name, conn, if_exists='replace', index=False)
            st.success(f"Data successfully saved to table: {table_name}")
    except Exception as e:
        st.error(f"Failed to save to database: {str(e)}")

def show_help():
    st.markdown("""
    # Tableau Data Downloader Help
    
    This application helps you download data from Tableau Server/Online and save it to a database.
    
    ## Getting Started
    
    1. **Server URL**
       - For Tableau Online (US): https://10ay.online.tableau.com
       - For Tableau Online (EU): https://10az.online.tableau.com
       - For Tableau Server: Your server's URL
    
    2. **Authentication**
       - Personal Access Token (Recommended):
         - Works with 2FA
         - More secure
         - Generate from your Tableau account settings
       - Username/Password:
         - Basic authentication
         - Not compatible with 2FA
    
    3. **Site Name**
       - For Tableau Online: Found in your URL after #/site/
       - For Tableau Server: Usually blank for default
    
    ## Using the App
    
    1. Enter your server details and authenticate
    2. Select a workbook from the list
    3. Choose one or more views to download
    4. Preview the data before saving
    5. Save to database if desired
    
    ## Troubleshooting
    
    - **Connection Issues**: Verify your server URL and credentials
    - **Empty Data**: Ensure the views contain data
    - **Database Errors**: Check if database is running (status shown at top)
    
    ## Need Help?
    
    Contact your Tableau administrator or refer to [Tableau's REST API documentation](https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api.htm)
    """)

# Initialize session state variables
if 'connector' not in st.session_state:
    st.session_state.connector = None
if 'authenticated' not in st.session_state:
    st.session_state.authenticated = False
if 'workbooks' not in st.session_state:
    st.session_state.workbooks = None
if 'selected_workbook' not in st.session_state:
    st.session_state.selected_workbook = None
if 'views' not in st.session_state:
    st.session_state.views = None
if 'downloaded_data' not in st.session_state:
    st.session_state.downloaded_data = None
if 'last_saved_table' not in st.session_state:
    st.session_state.last_saved_table = None

def initialize_session_state():
    """Initialize session state variables"""
    if 'admin_authenticated' not in st.session_state:
        st.session_state.admin_authenticated = False
    if 'show_admin_login' not in st.session_state:
        st.session_state.show_admin_login = False

def authenticate(server_url, auth_method, credentials):
    """Handle authentication and store in session state"""
    connector = TableauConnector(server_url)
    
    if auth_method == "Personal Access Token (PAT)":
        authenticated = connector.sign_in_with_pat(
            credentials['pat_name'],
            credentials['pat_secret'],
            credentials['site_name']
        )
    else:
        authenticated = connector.sign_in(
            credentials['username'],
            credentials['password'],
            credentials['site_name']
        )
    
    if authenticated:
        st.session_state.connector = connector
        st.session_state.authenticated = True
        # Get workbooks immediately after authentication
        st.session_state.workbooks = connector.get_workbooks()
        return True
    return False

def load_views(workbook):
    """Load views for selected workbook"""
    if st.session_state.connector:
        workbook_id = workbook.get('@id') or workbook.get('id')
        st.session_state.views = st.session_state.connector.get_views(workbook_id)
        st.session_state.selected_workbook = workbook

def download_and_save_data(view_ids, workbook_name, view_names, db_manager):
    """Download data and automatically save to database"""
    if st.session_state.connector:
        df = st.session_state.connector.download_view_data(view_ids, workbook_name)
        if not df.empty:
            # Add metadata columns
            df['Workbook'] = workbook_name
            df['Download_Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            df['View_Names'] = ', '.join(view_names)
            
            # Generate table name and save
            table_name = generate_table_name(workbook_name, view_names)
            save_to_database(df, table_name, db_manager.db_path)
            
            st.session_state.downloaded_data = df
            st.session_state.last_saved_table = table_name
            return True
    return False

def admin_login():
    """Show admin login popup and verify credentials"""
    with st.form("admin_login_form"):
        st.markdown("### Admin Login")
        username = st.text_input("Username", value="admin")
        password = st.text_input("Password", type="password")
        submit = st.form_submit_button("Login")
        
        if submit:
            if username == "admin" and password == "admin":
                st.session_state.admin_authenticated = True
                st.session_state.show_admin_login = False
                st.success("Login successful!")
                time.sleep(1)  # Give time for success message
                st.rerun()
            else:
                st.error("Invalid credentials")

def show_schedule_page(datasets=None):
    """Show schedule report page"""
    st.title("📊 Schedule Report")

    # Dataset selection at the top
    st.subheader("📁 Select Dataset")
    available_datasets = get_saved_datasets()
    selected_dataset = st.selectbox(
        "Choose a dataset to schedule",
        available_datasets,
        format_func=lambda x: f"{x} ({get_row_count(x)} rows)"
    )

    if selected_dataset:
        df = load_dataset(selected_dataset)
        if df is not None:
            # Create tabs for different sections
            settings_tab, recipients_tab, schedule_tab = st.tabs([
                "Email Settings", "Recipients", "Schedule"
            ])
            
            with settings_tab:
                st.subheader("📧 Email Server Configuration")
                col1, col2 = st.columns(2)
                
                with col1:
                    smtp_server = st.text_input(
                        "SMTP Server",
                        value=os.getenv('SMTP_SERVER', 'smtp.gmail.com'),
                        placeholder="e.g., smtp.gmail.com"
                    )
                    email_from = st.text_input(
                        "Sender Email",
                        value=os.getenv('EMAIL_FROM', ''),
                        placeholder="your-email@gmail.com"
                    )
                
                with col2:
                    smtp_port = st.number_input(
                        "SMTP Port",
                        value=int(os.getenv('SMTP_PORT', 587))
                    )
                    email_password = st.text_input(
                        "Email Password",
                        type="password",
                        help="For Gmail, use App Password"
                    )
                
                with st.expander("Gmail Setup Instructions", expanded=False):
                    st.markdown("""
                    ### Setting up Gmail:
                    1. Enable 2-Factor Authentication in your Google Account
                    2. Generate an App Password:
                        - Go to Google Account Settings
                        - Search for 'App Passwords'
                        - Select 'Mail' and your device
                        - Use the generated password here
                    """)
            
            with recipients_tab:
                st.subheader("👥 Recipients")
                email_list = st.text_area(
                    "Email Addresses",
                    placeholder="Enter email addresses (one per line)",
                    help="These addresses will receive the scheduled reports"
                )
                
                report_format = st.radio(
                    "Report Format",
                    options=["CSV", "PDF"],
                    horizontal=True,
                    help="PDF support coming soon"
                )
            
            with schedule_tab:
                st.subheader("🕒 Schedule Settings")
                schedule_type = st.selectbox(
                    "Frequency",
                    ["One-time", "Daily", "Weekly", "Monthly"],
                    help="How often to send the report"
                )

                col1, col2 = st.columns(2)
                
                with col1:
                    if schedule_type == "Daily":
                        hour = st.number_input("Hour (24-hour format)", 0, 23, 8)
                        minute = st.number_input("Minute", 0, 59, 0)
                        schedule_config = {
                            'type': 'daily',
                            'hour': hour,
                            'minute': minute
                        }
                    
                    elif schedule_type == "Weekly":
                        weekday = st.selectbox("Day of Week", 
                            ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"])
                        hour = st.number_input("Hour (24-hour format)", 0, 23, 8)
                        minute = st.number_input("Minute", 0, 59, 0)
                        schedule_config = {
                            'type': 'weekly',
                            'day': ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"].index(weekday.lower()),
                            'hour': hour,
                            'minute': minute
                        }
                    
                    elif schedule_type == "Monthly":
                        day = st.number_input("Day of Month", 1, 31, 1)
                        hour = st.number_input("Hour (24-hour format)", 0, 23, 8)
                        minute = st.number_input("Minute", 0, 59, 0)
                        schedule_config = {
                            'type': 'monthly',
                            'day': day,
                            'hour': hour,
                            'minute': minute
                        }
                    else:  # One-time
                        schedule_config = {
                            'type': 'one-time'
                        }

                with col2:
                    if schedule_type != "One-time":
                        st.info(f"""
                        Report will be sent:
                        {'Daily' if schedule_type == 'Daily' else ''}
                        {'Every ' + weekday if schedule_type == 'Weekly' else ''}
                        {'On day ' + str(day) + ' of each month' if schedule_type == 'Monthly' else ''}
                        at {hour:02d}:{minute:02d}
                        """)
                        
                        # Show active schedules
                        st.markdown("### Active Schedules")
                        report_manager = ReportManager()
                        active_schedules = report_manager.get_active_schedules()
                        
                        if active_schedules:
                            for job_id, schedule in active_schedules.items():
                                if schedule['dataset_name'] == selected_dataset:
                                    st.warning(
                                        f"Active schedule: {schedule['schedule_config']['type'].title()}\n"
                                        f"Click to remove", icon="🗑️"
                                    )
                                    if st.button("Remove Schedule", key=f"remove_{job_id}"):
                                        report_manager.remove_schedule(job_id)
                                        st.success("Schedule removed!")
                                        st.rerun()

            # Action buttons at the bottom
            st.markdown("---")
            col1, col2, col3 = st.columns([1, 1, 2])
            
            with col1:
                if st.button("← Back", use_container_width=True):
                    st.session_state.show_schedule_page = False
                    st.rerun()
            
            with col2:
                if st.button("Schedule Report", type="primary", use_container_width=True):
                    if not email_list.strip():
                        st.error("Please enter at least one recipient email")
                        return
                    
                    if not all([smtp_server, smtp_port, email_from, email_password]):
                        st.error("Please fill in all email settings")
                        return
                    
                    try:
                        email_config = {
                            'smtp_server': smtp_server,
                            'smtp_port': smtp_port,
                            'sender_email': email_from,
                            'sender_password': email_password,
                            'recipients': [e.strip() for e in email_list.split('\n') if e.strip()],
                            'format': report_format
                        }
                        
                        report_manager = ReportManager()
                        job_id = report_manager.schedule_report(
                            selected_dataset,
                            email_config,
                            schedule_config
                        )
                        
                        st.success(f"""
                        Report scheduled successfully! 🎉
                        Schedule: {schedule_type}
                        Next run: {hour:02d}:{minute:02d}
                        """)
                        
                    except Exception as e:
                        st.error(f"Failed to schedule report: {str(e)}")

def get_row_count(dataset_name):
    """Get row count for a dataset"""
    try:
        with sqlite3.connect(DatabaseManager().db_path) as conn:
            count = pd.read_sql(f"SELECT COUNT(*) FROM '{dataset_name}'", conn).iloc[0, 0]
            return count
    except Exception:
        return 0

def main():
    # Initialize session state at start
    initialize_session_state()
    
    # Add this to track schedule page state
    if 'show_schedule_page' not in st.session_state:
        st.session_state.show_schedule_page = False
    
    # Show either schedule page or main page
    if st.session_state.show_schedule_page:
        show_schedule_page()
        return  # Exit main() after showing schedule page
    
    # Rest of your main page code...
    st.set_page_config(
        page_title="Tableau Data Downloader",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Initialize database
    db_manager = DatabaseManager()
    db_url = db_manager.ensure_database_running()

    # Sidebar setup
    with st.sidebar:
        st.title("Navigation")
        if st.button("Show Help"):
            show_help()
        
        st.markdown("---")
        
        # Show saved datasets in a table grid
        st.subheader("Saved Data")
        datasets = get_saved_datasets()
        
        if not datasets:
            st.info("No datasets found. Please download some data first.")
        else:
            # Create a table grid for datasets
            data_rows = []
            for dataset in datasets:
                # Get row count and last modified time
                try:
                    with sqlite3.connect(DatabaseManager().db_path) as conn:
                        row_count = pd.read_sql(f"SELECT COUNT(*) FROM '{dataset}'", conn).iloc[0, 0]
                        data_rows.append({
                            "Dataset": dataset,
                            "Rows": row_count,
                            "Actions": dataset  # We'll use this for button keys
                        })
                except Exception as e:
                    continue

            # Create DataFrame for display
            if data_rows:
                df_datasets = pd.DataFrame(data_rows)
                
                # Display each dataset with actions
                for idx, row in df_datasets.iterrows():
                    with st.container():
                        col1, col2, col3 = st.columns([2, 1, 1])
                        
                        with col1:
                            st.write(f"**{row['Dataset']}**\n{row['Rows']} rows")
                        
                        with col2:
                            if st.button("Ask", key=f"ask_{row['Actions']}"):
                                df = load_dataset(row['Dataset'])
                                if df is not None:
                                    st.session_state.current_dataset = df
                                    st.session_state.show_qa = True
                                    st.rerun()
                        
                        with col3:
                            if st.button("📥", key=f"download_{row['Actions']}"):
                                df = load_dataset(row['Dataset'])
                                if df is not None:
                                    csv = df.to_csv(index=False)
                                    st.download_button(
                                        label="Save CSV",
                                        data=csv,
                                        file_name=f"{row['Dataset']}.csv",
                                        mime="text/csv",
                                        key=f"csv_{row['Actions']}"
                                    )
                        
                        st.markdown("---")  # Separator between datasets

        st.markdown("---")
        col1, col2 = st.columns([3, 1])
        with col1:
            st.write("Schedule Reports")
        with col2:
            if st.button("🗓️", key="schedule_report_btn"):
                if not st.session_state.admin_authenticated:
                    st.session_state.show_admin_login = True
                    st.rerun()
                else:
                    st.session_state.show_schedule_page = True
                    st.rerun()
        
        if st.session_state.show_admin_login and not st.session_state.admin_authenticated:
            admin_login()

    # Main content area
    if 'show_qa' in st.session_state and st.session_state.show_qa:
        if 'current_dataset' in st.session_state:
            show_analysis_tab(st.session_state.current_dataset)
    else:
        # Authentication section
        if not st.session_state.authenticated:
            server_url = st.text_input(
                "Tableau Server URL",
                help="Example: https://10ay.online.tableau.com for Tableau Online"
            )
            
            auth_method = st.radio(
                "Authentication Method",
                ["Personal Access Token (PAT)", "Username/Password"],
                help="PAT is recommended and works with 2FA"
            )
            
            with st.form("auth_form"):
                credentials = {}
                if auth_method == "Personal Access Token (PAT)":
                    credentials['pat_name'] = st.text_input("Personal Access Token Name")
                    credentials['pat_secret'] = st.text_input("Personal Access Token Secret", type="password")
                else:
                    credentials['username'] = st.text_input("Username")
                    credentials['password'] = st.text_input("Password", type="password")
                
                credentials['site_name'] = st.text_input("Site Name (optional)")
                submit_auth = st.form_submit_button("Connect")
                
                if submit_auth and server_url:
                    if authenticate(server_url, auth_method, credentials):
                        st.success("Successfully connected to Tableau!")
                        st.rerun()
                    else:
                        st.error("Authentication failed")
        
        # Workbook and View Selection
        if st.session_state.authenticated and st.session_state.workbooks:
            # Workbook selection
            workbook_names = [wb.get('@name') or wb.get('name') for wb in st.session_state.workbooks]
            selected_wb_name = st.selectbox(
                "Select Workbook",
                workbook_names,
                key='workbook_selector'
            )
            
            # Find selected workbook
            selected_workbook = next(
                wb for wb in st.session_state.workbooks 
                if (wb.get('@name') or wb.get('name')) == selected_wb_name
            )
            
            # Load views if workbook changed
            if (not st.session_state.selected_workbook or 
                selected_workbook != st.session_state.selected_workbook):
                load_views(selected_workbook)
            
            # View selection
            if st.session_state.views:
                view_names = [view.get('@name') or view.get('name') for view in st.session_state.views]
                selected_views = st.multiselect("Select Views", view_names)
                
                if selected_views:
                    if st.button("Download Data"):
                        view_ids = [
                            view.get('@id') or view.get('id')
                            for view in st.session_state.views
                            if (view.get('@name') or view.get('name')) in selected_views
                        ]
                        
                        with st.spinner('Downloading and saving data...'):
                            if download_and_save_data(view_ids, selected_wb_name, selected_views, db_manager):
                                st.success(f"""
                                Data downloaded and saved successfully!
                                Table name: {st.session_state.last_saved_table}
                                """)
                                # Force sidebar refresh
                                st.rerun()
            
            # Display downloaded data
            if st.session_state.downloaded_data is not None:
                with st.expander("View Downloaded Data", expanded=True):
                    st.dataframe(st.session_state.downloaded_data.head())
                    st.write(f"Total rows: {len(st.session_state.downloaded_data)}")
                    
                    # Add download button for CSV
                    csv = st.session_state.downloaded_data.to_csv(index=False)
                    st.download_button(
                        label="Download as CSV",
                        data=csv,
                        file_name=f"{st.session_state.last_saved_table}.csv",
                        mime="text/csv"
                    )
        
        # Logout button
        if st.session_state.authenticated:
            if st.sidebar.button("Logout"):
                for key in st.session_state.keys():
                    del st.session_state[key]
                st.rerun()

        if st.session_state.authenticated:
            # Add tabs for different functions
            tab1, tab2 = st.tabs(["Data Download", "Analysis"])
            
            with tab1:
                # Existing data download code
                # ...
                pass  # Replace with your existing download code
            
            with tab2:
                if st.session_state.downloaded_data is not None:
                    show_analysis_tab(st.session_state.downloaded_data)
                else:
                    st.info("Download some data first to see the analysis")

def show_saved_datasets():
    """Show list of saved datasets with analysis options"""
    st.subheader("📊 Saved Datasets")
    
    # Get list of saved datasets
    datasets = get_saved_datasets()
    
    if not datasets:
        st.info("No datasets found. Please download some data first.")
        return
    
    # Create columns for each dataset
    for dataset in datasets:
        col1, col2, col3 = st.columns([3, 1, 1])
        
        with col1:
            st.write(f"**{dataset}**")
        
        with col2:
            if st.button("Analyze", key=f"analyze_{dataset}"):
                # Load dataset and show analysis
                df = load_dataset(dataset)
                if df is not None:
                    show_analysis_tab(df)
        
        with col3:
            if st.button("Ask Questions", key=f"ask_{dataset}"):
                # Load dataset and jump to Q&A section
                df = load_dataset(dataset)
                if df is not None:
                    show_analysis_tab(df)
                    # Use JavaScript to scroll to Q&A section
                    st.markdown(
                        """
                        <script>
                            document.querySelector('[name="qa_section"]').scrollIntoView();
                        </script>
                        """,
                        unsafe_allow_html=True
                    )

def load_dataset(table_name):
    """Load dataset from SQLite database"""
    try:
        with sqlite3.connect(DatabaseManager().db_path) as conn:
            df = pd.read_sql(f"SELECT * FROM '{table_name}'", conn)
            return df
    except Exception as e:
        st.error(f"Failed to load dataset: {str(e)}")
        return None

def get_saved_datasets():
    """Get list of saved datasets"""
    return DatabaseManager().list_tables()

if __name__ == "__main__":
    main() 