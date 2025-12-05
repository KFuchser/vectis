# agent_main.py
import os
import streamlit as st # If you need secrets from Streamlit
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.prompts import PromptTemplate
from langchain_core.output_parsers import PydanticOutputParser
from supabase import create_client, Client

# Import your models
from service_models import PermitClassification

# 1. SETUP: Load API Key
# If running locally, use os.getenv. If on Streamlit Cloud, use st.secrets
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY") or st.secrets["GOOGLE_API_KEY"]

# 2. INITIALIZE: The Intelligence Layer
# We use temperature=0 for consistent, factual data extraction
llm = ChatGoogleGenerativeAI(
    model="gemini-2.0-flash", 
    temperature=0,
    google_api_key=GOOGLE_API_KEY
)

# 3. LOGIC: Define the Classifier Function
def run_permit_agent(description: str):
    # Setup the parser to use your Pydantic model
    parser = PydanticOutputParser(pydantic_object=PermitClassification)

    prompt = PromptTemplate(
        template="""
        You are a government data expert. Analyze the following construction permit description.
        
        Description: {description}
        
        Extract the information to match the following format instructions exactly.
        {format_instructions}
        """,
        input_variables=["description"],
        partial_variables={"format_instructions": parser.get_format_instructions()},
    )

    # Create the chain
    chain = prompt | llm | parser

    try:
        # Invoke Gemini
        result = chain.invoke({"description": description})
        return result # Returns a PermitClassification object (clean data!)
    except Exception as e:
        print(f"Error in agent: {e}")
        return None
    
#Initialize Supabase (Load from secrets/env)
# In production, these should be in .streamlit/secrets.toml
SUPABASE_URL = os.getenv("SUPABASE_URL") or st.secrets["SUPABASE_URL"]
SUPABASE_KEY = os.getenv("SUPABASE_KEY") or st.secrets["SUPABASE_KEY"]

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def save_permit_to_db(permit_data: PermitClassification, raw_id: str):
    """
    Takes the Pydantic model from Gemini and pushes it to Supabase.
    """
    # 1. Convert Pydantic model to a standard Python Dict
    data_payload = permit_data.model_dump()
    
    # 2. Add the ID (Primary Key) so we don't create duplicates
    data_payload["permit_id"] = raw_id
    
    # 3. Insert into the 'permits' table
    try:
        data, count = supabase.table("permits").upsert(data_payload).execute()
        return True
    except Exception as e:
        print(f"❌ Database Error: {e}")
        return False