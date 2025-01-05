import re
from flask import Flask, request, Response, jsonify
from werkzeug.security import check_password_hash, generate_password_hash
import jwt
from flask_cors import CORS
import datetime
from functools import wraps
import openai
from openai import OpenAI
from flask_cors import CORS
from langchain_community.agent_toolkits import create_sql_agent
from langchain_openai import ChatOpenAI
from langchain_community.utilities import SQLDatabase
from langchain_core.messages import HumanMessage 
from dotenv import load_dotenv
import os
import requests
from io import BytesIO
from PyPDF2 import PdfReader
from langchain_openai import OpenAIEmbeddings, OpenAI 
from langchain.text_splitter import CharacterTextSplitter
from langchain_community.vectorstores import FAISS
from langchain.chains.question_answering import load_qa_chain
import psycopg2
from psycopg2.extras import RealDictCursor

load_dotenv()

app = Flask(__name__)
CORS(app)

app.config['SECRET_KEY'] = 'jjsbdjkcbs'

users = {
    'admin': {
        'password': generate_password_hash('admin'),
        'role': 'admin'
    },
    'salesemployee': {
        'password': generate_password_hash('salesemployee'),
        'role': 'store'
    },
     'inventorymanager': {
        'password': generate_password_hash('inventorymanager'),
        'role': 'inventory'
    }
}

OPEN_AI_KEY = os.getenv("OPENAI_API_KEY")
DB_URI = os.getenv("DB_URI")
openai.api_key = OPEN_AI_KEY

db = SQLDatabase.from_uri(DB_URI)

llm = ChatOpenAI(
    openai_api_key=OPEN_AI_KEY,
    model="gpt-3.5-turbo", temperature=0
)

agent_executor = create_sql_agent(llm, db=db, agent_type="openai-tools", verbose=True)

docsearch = None
chain = None

def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.headers.get('Authorization')
        if not token:
            return jsonify({'message': 'Token is missing!'}), 401
        try:
            data = jwt.decode(token, app.config['SECRET_KEY'], algorithms=["HS256"])
        except:
            return jsonify({'message': 'Token is invalid!'}), 401
        return f(*args, **kwargs)
    return decorated



def fetch_pdf_url(role):
    try:
        # Establish a connection to the PostgreSQL database with the corrected URI
        connection = psycopg2.connect(DB_URI)
        cursor = connection.cursor(cursor_factory=RealDictCursor)
        
        # Execute the SQL query to fetch the PDF URL based on the user's role
        cursor.execute(f"SELECT Url FROM Manual WHERE Role = '{role}'")
        
        # Fetch the result
        result = cursor.fetchone()
        # Close the cursor and connection
        cursor.close()
        connection.close()
        
        # If a URL is found, return it
        if result and 'url' in result:
            return result['url']
        else:
            print("No URL found for the given role.")
            return None

    except (Exception, psycopg2.DatabaseError) as e:
        print(f"Failed to fetch PDF URL: {str(e)}")
        return None

def fetch_and_process_pdf(pdf_url):
    
    global docsearch, chain
    
    try:
        response = requests.get(pdf_url)
        response.raise_for_status()  
        pdf_file = BytesIO(response.content)

        pdf_reader = PdfReader(pdf_file)
        raw_text = ''
        for page in pdf_reader.pages:
            text = page.extract_text()
            if text:
                raw_text += text
        
        # Split text and create FAISS index
        text_splitter = CharacterTextSplitter(
            separator="\n",
            chunk_size=1000,
            chunk_overlap=200,
            length_function=len,
        )
        texts = text_splitter.split_text(raw_text)
        embeddings = OpenAIEmbeddings()
        docsearch = FAISS.from_texts(texts, embeddings)

        chain = load_qa_chain(OpenAI(), chain_type="stuff")
        
        print("PDF fetched and processed successfully")

    except requests.RequestException as e:
        print(f"Failed to fetch PDF: {str(e)}")

@app.route('/login', methods=['POST'])
def login():
    auth = request.json

    if not auth or not auth.get('username') or not auth.get('password'):
        return jsonify({'message': 'Could not verify'}), 401

    if auth.get('username') not in users:
        return jsonify({'message': 'User not found!'}), 404

    user = users[auth.get('username')]

    if check_password_hash(user['password'], auth.get('password')):
        token = jwt.encode({
            'username': auth.get('username'),
            'role': user['role'],
            'exp': datetime.datetime.utcnow() + datetime.timedelta(hours=24)
        }, app.config['SECRET_KEY'])
        
        # Fetch the PDF URL based on user role and process the PDF
        # pdf_url = fetch_pdf_url(user['role'])
       
        # if pdf_url:
        #     fetch_and_process_pdf(pdf_url)
        
        return jsonify({
            'token': token,
            'role': user['role'],
            'username': auth.get('username')  # Return the username
        })

    return jsonify({'message': 'Could not verify'}), 401




@app.route('/chat', methods=['POST'])
@token_required
def chat():
    data = request.json
    messages = data.get('messages', [])
    user_role = data.get('role')  # Get the user's role from the request

    # Base prompt for the LLM
    base_prompt = """
        We have two tables, bap_table and current_accounts_table, each containing different columns. 
        Both tables share a common column, id, which serves as a primary/foreign key for linking records between the two tables.

    bap_table contains details related to the BAP program, a special initiative designed to help customers who have taken out loans by assisting them with repayment. 
    Here are the names and descriptions of column headers for bap_table:
{
    "id": "The unique identifier for each loan account, associated with a specific customer",
    "createddate": "The date when the BAP offer was created for a loan account",
    "BAP_offer_status": "The status of the BAP offer (e.g., BAP eligible, offer sent, etc.)",
    "offer_send_dt": "The date when the BAP offer was sent",
    "acceptance_date__c": "The date when the BAP offer was accepted by the customer",
    "offer_type": "Offer type, either 65% offer or 50% offer"
}

current_accounts_table holds information about the loan accounts as of the current date. 
Names and descriptions of column headers for current_accounts_table:
{
    "id": "The unique identifier for each loan account, associated with a specific customer",
    "current_dpd": "Days Past Due (DPD), which represents the number of days the loan account is overdue beyond the due date",
    "close_dt": "The date when the customer’s loan account was closed",
    "charge_off_date": "The date when the loan account was written off",
    "gross_co_bal": "The total charged-off balance, including both interest and principal amounts of the loan",
    "curr_status": "The current status of the loan, such as 'Closed - Written Off', 'Active - Good Standing', etc.",
    "curr_pymt_type": "The payment type for the loan, either 'recurring' or 'non-recurring'",
    "curr_prin_bal": "The remaining principal balance that still needs to be repaid on the loan"
}
*Important Notes:*
- Always use double quotes (") around table names and column names in your queries to ensure proper execution.
- Queries should follow SQL syntax strictly.
- If you need to join the tables, use "id" as the linking column.
- Ensure all generated SQL statements are syntactically correct and properly formatted.
"""

    graph_type = None
    query = None
    is_help = False

    if messages:
        latest_message = messages[-1].get('content', '')
        print(f"Latest Message: {latest_message}")

        # Prepend the base prompt to the user's message
        enriched_prompt = f"{base_prompt}\n\n{latest_message}"
        print(f"Enriched Prompt: {enriched_prompt}")

        if "help" in latest_message.lower():
            is_help = True
            help_prompt = f"{enriched_prompt} Kindly ensure the output is well formatted."

        elif "plot" in latest_message.lower():
            graph_type = enriched_prompt
            print(f"Graph Type: {graph_type}")
        else:
            query = enriched_prompt
            print("Query:", query)

    if is_help:
        if docsearch is None or chain is None:
            return jsonify({"error": "Document search or processing chain is not initialized."}), 500

        try:
            docs = docsearch.similarity_search(help_prompt)
            answer = chain.run(input_documents=docs, question=help_prompt)
            print(f"Answer: {answer}")
            return Response(answer, mimetype='text/plain')
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    elif query:
        try:
            result = agent_executor.invoke(query)
            output = result['output']
            return Response(output, mimetype='text/plain')
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    elif graph_type:
        try:
            print("Generating Graph...")
            graph_prompt = "You are a data analyst. Fetch the data according to the user's request and return it as JSON with keys 'type' for the graph type, 'x' for the x-axis data, and 'y' for the y-axis data. The prompt is: " + graph_type
            result = agent_executor.invoke(graph_prompt)
            output = result['output']
            match = re.search(r'\{.*\}', output, re.DOTALL)
            if match:
                json_data = match.group(0)
                graph_data = eval(json_data)
                print(f"Graph Data: {graph_data}")
                return jsonify(graph_data)
            else:
                return jsonify({"error": "No valid JSON found in the response"}), 500
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    return jsonify({"error": "No valid query or graph request found"}), 400


@app.route('/upload_data', methods=['POST'])
@token_required
def upload_data():
    data = request.json
    url = data.get('url')
    role = data.get('role')

    try:
        # Establish a connection to the PostgreSQL database
        connection = psycopg2.connect(DB_URI)
        cursor = connection.cursor()

        # Check if the role already exists
        cursor.execute("SELECT * FROM Manual WHERE Role = %s", (role,))
        existing_entry = cursor.fetchone()

        if existing_entry:
            # If the role exists, update the URL
            cursor.execute("UPDATE Manual SET Url = %s WHERE Role = %s", (url, role))
        else:
            # If the role doesn't exist, insert a new entry
            cursor.execute("INSERT INTO Manual (Role, Url) VALUES (%s, %s)", (role, url))

        # Commit the transaction
        connection.commit()

        # Close the cursor and connection
        cursor.close()
        connection.close()

        return jsonify({"message": "Data uploaded successfully"}), 200

    except Exception as e:
        print(f"Failed to upload data: {str(e)}")
        return jsonify({"message": "Failed to upload data"}), 500




if __name__ == '__main__':
    app.run(debug=True)
