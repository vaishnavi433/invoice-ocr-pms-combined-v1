import streamlit as st
import pandas as pd
import os
import shutil
import streamlit.components.v1 as components
import backend  # Imports your backend.py logic

# ============================================================================
# 1. PAGE & THEME CONFIGURATION
# ============================================================================
st.set_page_config(page_title="Supy Converter", page_icon="🔮", layout="wide")

st.markdown("""
    <style>
    /* Purple & White Theme */
    .stApp { background-color: #ffffff; }
    [data-testid="stSidebar"] { background-color: #f3e8ff; border-right: 1px solid #d8b4fe; }
    
    /* Headers */
    h1, h2, h3 { color: #4c1d95; font-family: 'Helvetica Neue', sans-serif; }
    
    /* Buttons */
    .stButton>button { 
        background-color: #6b46c1; 
        color: white; 
        border-radius: 8px; 
        border: none; 
        padding: 0.5rem 1rem;
        font-weight: 600;
        width: 100%;
    }
    .stButton>button:hover { background-color: #5b21b6; color: white; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }
    
    /* Success/Info Boxes */
    .stSuccess { background-color: #ECFDF5; border-left: 5px solid #10B981; }
    .stInfo { background-color: #F3E8FF; border-left: 5px solid #6B46C1; }
    
    /* SECRET CAT BUTTON STYLING - Almost Invisible */
    div.stButton > button[kind="secondary"] {
        background-color: transparent;
        color: #f3e8ff; /* Matches sidebar bg so it's barely visible */
        border: none;
        width: auto;
        font-size: 10px; /* Tiny */
    }
    div.stButton > button[kind="secondary"]:hover {
        color: #6b46c1; /* Only visible on hover */
        background-color: transparent;
        box-shadow: none;
    }
    </style>
""", unsafe_allow_html=True)

# ============================================================================
# 2. SESSION STATE
# ============================================================================
if 'raw_data' not in st.session_state: st.session_state.raw_data = pd.DataFrame()
if 'pms_data' not in st.session_state: st.session_state.pms_data = pd.DataFrame()
if 'review_data' not in st.session_state: st.session_state.review_data = pd.DataFrame()
if 'dup_data' not in st.session_state: st.session_state.dup_data = pd.DataFrame()

# ============================================================================
# 3. SIDEBAR SETTINGS & SECRET SHORTCUT
# ============================================================================
with st.sidebar:
    st.image("https://cdn-icons-png.flaticon.com/512/2920/2920349.png", width=60)
    st.title("Supy Vision")
    st.markdown("**Enterprise Edition v4.0**")
    st.divider()
    
    st.subheader("Global Settings")
    
    # Dynamic Country Loader
    country_options = sorted([(k, v['name']) for k, v in backend.COUNTRY_TAX_DB.items()], key=lambda x: x[1])
    selected_country_code = st.selectbox(
        "Target Country",
        options=[code for code, name in country_options],
        format_func=lambda x: next((name for c, name in country_options if c == x), x),
        index=[code for code, name in country_options].index("AE") if "AE" in [c for c, n in country_options] else 0
    )
    
    # API Key Input
    api_input = st.text_input("API Key", value=backend.OPENROUTER_API_KEY, type="password")
    if api_input:
        backend.OPENROUTER_API_KEY = api_input
    
    enable_translation = st.checkbox("Translate to English", value=False)
    
    st.divider()
    st.caption("Developed for Supy Operations")
    
    # --- 🐱 SECRET EASTER EGG SECTION ---
    st.markdown("<br><br>", unsafe_allow_html=True) 
    
    # The Trigger Button
    if st.button("👀", key="secret_trigger", type="secondary", help="?"):
        if os.path.exists("image_2.png"):
            st.image("image_2.png", caption="Your secret is safe with me.")
        else:
            # Fallback if image isn't found
            st.image("https://media.giphy.com/media/GeimqsH0TLDt4tScGw/giphy.gif", width=300)
            st.toast("Add 'image_2.png' to folder for your specific cat!", icon="ℹ️")
        st.balloons()

    # Keyboard Shortcut Injection (Shift + C)
    components.html("""
    <script>
        document.addEventListener('keydown', function(e) {
            if (e.shiftKey && (e.key === 'C' || e.key === 'c')) {
                const buttons = window.parent.document.getElementsByTagName('button');
                for (let i = 0; i < buttons.length; i++) {
                    if (buttons[i].innerText.includes("👀")) {
                        buttons[i].click();
                        break;
                    }
                }
            }
        });
    </script>
    """, height=0, width=0)

# ============================================================================
# 4. MAIN WORKFLOW
# ============================================================================
st.title("🧾 Invoice & PMS Processor")

# --- STEP 1: SELECT MODE ---
st.subheader("⚙️ Step 1: Select Workflow")
mode_selection = st.radio(
    "Choose your processing mode:",
    ["Full Pipeline (Invoice -> PMS Excel)", 
     "Extraction Only (Invoice -> Raw Excel)", 
     "Conversion Only (Raw Excel -> PMS Excel)"],
    horizontal=True
)

# --- STEP 2: UPLOAD ---
st.subheader("📂 Step 2: Upload Files")

uploaded_files = None
if "Conversion Only" in mode_selection:
    uploaded_files = st.file_uploader("Upload Excel File (.xlsx, .csv)", type=['xlsx', 'xls', 'csv'], accept_multiple_files=False)
else:
    uploaded_files = st.file_uploader("Upload Invoices (PDF, Images) or ZIP", type=['pdf', 'png', 'jpg', 'jpeg', 'zip'], accept_multiple_files=True)

# --- STEP 3: PROCESS ---
if uploaded_files:
    st.subheader("🚀 Step 3: Process")
    
    if st.button("Start Processing"):
        # Reset previous data to avoid confusion
        st.session_state.raw_data = pd.DataFrame()
        st.session_state.pms_data = pd.DataFrame()

        # Create temp directory
        temp_dir = "temp_uploads"
        if os.path.exists(temp_dir): shutil.rmtree(temp_dir)
        os.makedirs(temp_dir)
        
        # 1. Handle File Saving
        files_to_process = []
        uploads = [uploaded_files] if not isinstance(uploaded_files, list) else uploaded_files
        
        for f in uploads:
            path = os.path.join(temp_dir, f.name)
            with open(path, "wb") as buffer: buffer.write(f.getbuffer())
            
            # Handle ZIPs using backend logic
            if f.name.lower().endswith(".zip"):
                extracted = backend.recursive_zip_extractor(path, os.path.join(temp_dir, "unzipped"))
                files_to_process.extend(extracted)
            else:
                files_to_process.append(path)

        # 2. EXECUTE LOGIC
        with st.status("Running Engine...", expanded=True) as status:
            config = {"country": selected_country_code, "translate_enabled": enable_translation, "verify_prices": True}
            
            # --- PATH A: EXTRACTION ---
            if "Conversion Only" not in mode_selection:
                st.write(f"Extracting data from {len(files_to_process)} files...")
                try:
                    raw_df = backend.batch_process_invoices(files_to_process, selected_country_code)
                    if not raw_df.empty:
                        st.session_state.raw_data = raw_df
                    else:
                        status.update(label="Extraction Failed: No data returned.", state="error")
                        st.error("No data extracted. Check your API Key or file content.")
                        st.stop()
                except Exception as e:
                    st.error(f"Backend Error: {e}")
                    st.stop()
            
            # --- PATH B: LOAD EXCEL ---
            elif "Conversion Only" in mode_selection:
                st.write("Loading Excel data...")
                try:
                    # files_to_process[0] is the excel file path
                    raw_df = pd.read_excel(files_to_process[0])
                    st.session_state.raw_data = raw_df
                except Exception as e:
                    st.error(f"Excel Load Error: {e}")
                    st.stop()

            # --- PATH C: PMS CONVERSION ---
            if "Extraction Only" not in mode_selection and not st.session_state.raw_data.empty:
                st.write("Standardizing to PMS format...")
                try:
                    pms, review, dups = backend.run_pms_conversion_engine(st.session_state.raw_data, config)
                    st.session_state.pms_data = pms
                    st.session_state.review_data = review
                    st.session_state.dup_data = dups
                except Exception as e:
                    st.error(f"Conversion Error: {e}")
                    st.stop()
            
            status.update(label="Processing Complete!", state="complete")
            st.success("Task Finished Successfully.")

    # --- RESULTS PREVIEW & DOWNLOAD ---
    
    # 1. Extraction Only Result
    if "Extraction Only" in mode_selection and not st.session_state.raw_data.empty:
        st.divider()
        st.subheader("📊 Extraction Results")
        st.dataframe(st.session_state.raw_data.head(), use_container_width=True)
        
        out_file = f"Supy_Raw_{selected_country_code}.xlsx"
        st.session_state.raw_data.to_excel(out_file, index=False)
        with open(out_file, "rb") as f:
            st.download_button("⬇️ Download Raw Data", f, file_name=out_file, mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    # 2. Full Pipeline / Conversion Result
    elif not st.session_state.pms_data.empty:
        st.divider()
        st.subheader("📊 PMS Results")
        
        tab_res1, tab_res2, tab_res3 = st.tabs(["PMS Data", "Review Queue", "Duplicates"])
        with tab_res1: st.dataframe(st.session_state.pms_data, use_container_width=True)
        with tab_res2: st.dataframe(st.session_state.review_data, use_container_width=True)
        with tab_res3: st.dataframe(st.session_state.dup_data, use_container_width=True)
        
        config = {"country": selected_country_code, "translate_enabled": enable_translation}
        try:
            out_file = backend.create_formatted_excel(
                st.session_state.raw_data,
                st.session_state.pms_data,
                st.session_state.review_data,
                st.session_state.dup_data,
                config
            )
            with open(out_file, "rb") as f:
                st.download_button("⬇️ Download Final PMS Report", f, file_name=out_file, mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        except Exception as e:
            st.error(f"Error creating Excel file: {e}")