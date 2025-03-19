# import packages
import streamlit as st
import pandas as pd
from src.simple_aws_assets import AwsAssets

# Streamlit UI
st.title("Simple Number Input App")

# Input fields for two numbers
num1 = st.number_input("Enter first number:", value=0.0)
num2 = st.number_input("Enter second number:", value=0.0)

# # Initialize session state
if "df_state" not in st.session_state:
    st.session_state["df_state"] = None
# st.session_state.setdefault("df_state", None)
# st.session_state.setdefault("df_", None)
# st.session_state["df_state"] = None

# Button to process numbers
if st.button("Run"):
    ins = AwsAssets(num1, num2)  # Instantiate inside button press
    df_ = ins.create_df()
    st.session_state["df_state"] = 'y'
    # st.session_state["df_"] = df_  # Store DataFrame in session state
    st.dataframe(df_)

# Button to upload DataFrame
if st.button("Upload DataFrame"):
    if st.session_state["df_state"] is None:
        st.error("Please run the code first")
    elif st.session_state["df_state"] == 'y':
        st.success("Will be added later, Gator!")
