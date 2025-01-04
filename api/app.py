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

    graph_type = None
    query = None
    is_help = False

    if messages:
        latest_message = messages[-1].get('content', '')
        print(f"Latest Message: {latest_message}")

        if "help" in latest_message.lower():
            is_help = True
            help_prompt =  f"{latest_message} Kindly ensure the output is well formatted."

        # Check if the latest message includes graph instructions
        elif "plot" in latest_message.lower():
            graph_type = latest_message
            print(f"Graph Type: {graph_type}")
        else:
            query = latest_message
            print("Query:", query)

    if is_help:
        if docsearch is None or chain is None:
            return jsonify({"error": "Document search or processing chain is not initialized."}), 500

        try:
            # Define your question here or adapt based on the incoming request
            

            # Perform similarity search in the document
            docs = docsearch.similarity_search(help_prompt)

            # Generate answer based on the document content
            answer = chain.run(input_documents=docs, question=help_prompt)
            print(f"Answer: {answer}")

            return Response(answer, mimetype='text/plain')

        except Exception as e:
            return jsonify({"error": str(e)}), 500

    elif query:
        if user_role:
            # Define the table names based on role
            allowed_table = user_role
            restricted_table = "inventory" if user_role == "store" else "store"

            # Check if the query attempts to access the restricted table
            if restricted_table in query.lower():
                output_response= f"You are restricted from accessing the {restricted_table} table."
                return Response(output_response, mimetype='text/plain')
            
            # Modify the query to restrict it to the allowed table
            extended_query = f"Please use only the {allowed_table} table for this query: {query} ..... If the input query requires usage of {restricted_table} return a response message saying you are restricted from accessing {restricted_table}. Kindly return the output as a markdown text."
        try:
            result = agent_executor.invoke(extended_query)
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
            print(output)
            # Extract JSON part from the output using regular expressions
            match = re.search(r'\{.*\}', output, re.DOTALL)
            if match:
                json_data = match.group(0)
                graph_data = eval(json_data)  # Convert string to dictionary
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
