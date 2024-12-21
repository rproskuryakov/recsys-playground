"""
# My first app
Here's our first attempt at using data to create a table:
"""

import streamlit as st
import pandas as pd

query = st.text_input("query")

test_search_response = [
  ("Sekiro", 100000),
  ("Elden Ring", 1000)
]

for response in test_search_response:
  with st.container(border=True):
    # col1, col2, col3 = st.columns(3)
    # with col1:
    #   st.write()
    st.write(response[0])# df = pd.DataFrame({
#   'first column': [1, 2, 3, 4],
#   'second column': [10, 20, 30, 40]
# })
#
# df