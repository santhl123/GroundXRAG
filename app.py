import streamlit as st
from utils import ChatbotBackend
import time

# Page configuration
st.set_page_config(
    page_title="GroundX Chatbot",
    page_icon="🤖",
    layout="wide"
)

# Custom CSS
st.markdown("""
    <style>
    .stApp {
        max-width: 1200px;
        margin: 0 auto;
    }
    .chat-message {
        padding: 1.5rem;
        border-radius: 0.5rem;
        margin-bottom: 1rem;
        display: flex;
        flex-direction: column;
        word-wrap: break-word;
    }
    .user-message {
        background-color: #0E1117;
        border: 1px solid #262730;
        color: #FAFAFA;
        border-top-right-radius: 0;
    }
    .bot-message {
        background-color: #262730;
        border: 1px solid #0E1117;
        color: #FAFAFA;
        border-top-left-radius: 0;
    }
    .message-content {
        margin-top: 0.5rem;
        white-space: pre-wrap;
    }
    </style>
    """, unsafe_allow_html=True)

# Initialize session state
if 'chat_history' not in st.session_state:
    st.session_state.chat_history = []

# Initialize chatbot only once
if 'chatbot' not in st.session_state:
    try:
        st.session_state.chatbot = ChatbotBackend()
    except Exception as e:
        st.error(f"Error initializing chatbot: {str(e)}")
        st.stop()

# Header
st.title("🤖 GroundX Document Chatbot")
st.markdown("Ask questions about your documents and get intelligent responses!")

# Input area
try:
    # Create a form container
    with st.form(key='chat_form'):
        user_input = st.text_input("Ask your question:", key="user_input", placeholder="Type your question here...")
        col1, col2 = st.columns([1, 5])
        with col1:
            doc_id = st.number_input("Document ID:", value=17068, min_value=1)
        
        # Add a submit button
        submit_button = st.form_submit_button(label='Send')

    # Process input only when form is submitted
    if submit_button and user_input:
        # Add user message to chat history
        st.session_state.chat_history.append({"role": "user", "content": user_input})
        
        # Show typing indicator
        with st.spinner("Thinking..."):
            try:
                response = st.session_state.chatbot.get_response(user_input, doc_id)
                time.sleep(0.5)  # Small delay for better UX
                
                if "error" in response:
                    st.error(f"Error: {response['error']}")
                else:
                    st.session_state.chat_history.append({
                        "role": "assistant",
                        "content": response["response"],
                        "score": response.get("score", 0)
                    })
            except Exception as e:
                st.error(f"Error processing request: {str(e)}")

    # Display chat history
    if st.session_state.chat_history:
        st.markdown("### Conversation")
        for message in st.session_state.chat_history:
            if message["role"] == "user":
                st.markdown(f"""
                    <div class="chat-message user-message">
                        <div><strong>👤 You</strong></div>
                        <div class="message-content">{message["content"]}</div>
                    </div>
                """, unsafe_allow_html=True)
            else:
                st.markdown(f"""
                    <div class="chat-message bot-message">
                        <div><strong>🤖 Assistant</strong> (Confidence: {message.get("score", "N/A"):.2f})</div>
                        <div class="message-content">{message["content"]}</div>
                    </div>
                """, unsafe_allow_html=True)

    # Clear chat button
    if st.button("Clear Chat"):
        st.session_state.chat_history = []
        st.rerun()

except Exception as e:
    st.error(f"An unexpected error occurred: {str(e)}")
