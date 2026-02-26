import streamlit as st
import pickle
import numpy as np

# Load trained model
model = pickle.load(open("model.pkl", "rb"))

st.set_page_config(page_title="Fraud Detection System", page_icon="🚀")

st.title("🚀 Real-Time Fraud Detection System")
st.write("Enter transaction details to check whether it is Fraud or Not Fraud.")

# Example input fields
amount = st.number_input("Transaction Amount")
time = st.number_input("Transaction Time")
feature1 = st.number_input("Feature 1")
feature2 = st.number_input("Feature 2")

if st.button("Predict"):
    input_data = np.array([[amount, time, feature1, feature2]])
    prediction = model.predict(input_data)

    if prediction[0] == 1:
        st.error("⚠️ Fraudulent Transaction Detected!")
    else:
        st.success("✅ Legitimate Transaction")