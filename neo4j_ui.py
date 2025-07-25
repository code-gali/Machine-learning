"""
Stable Enhanced Neo4j UI with unlimited graph exploration and Neo4j-like visualization
This version provides a stable chat interface with comprehensive graph visualization
"""

import streamlit as st
import streamlit.components.v1 as components
from pyvis.network import Network
import requests
import json
import os
import tempfile
from datetime import datetime
import uuid
import traceback
import hashlib
import time

# Page configuration for optimal display
st.set_page_config(
    page_title="Neo4j Graph Explorer Pro", 
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'Get Help': None,
        'Report a bug': None,
        'About': "Neo4j Graph Explorer Pro - Unlimited graph exploration with schema awareness"
    }
)

# Enhanced CSS with Neo4j-like styling
st.markdown("""
<style>
    .main .block-container {
        padding-top: 1rem;
        max-width: 100%;
    }
    
    .neo4j-header {
        background: linear-gradient(135deg, #00857C 0%, #00BCD4 50%, #4CAF50 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-size: 2.8rem;
        font-weight: bold;
        text-align: center;
        margin-bottom: 1rem;
        text-shadow: 2px 2px 4px rgba(0,0,0,0.1);
    }
    
    .neo4j-subtitle {
        text-align: center;
        color: #00857C;
        font-size: 1.1rem;
        margin-bottom: 2rem;
        font-weight: 500;
    }
    
    .stButton button {
        width: 100%;
        background: linear-gradient(45deg, #00857C, #00BCD4);
        color: white;
        border: none;
        padding: 0.7rem 1rem;
        border-radius: 10px;
        font-weight: 600;
        margin-bottom: 0.3rem;
        transition: all 0.3s ease;
        box-shadow: 0 2px 6px rgba(0, 133, 124, 0.3);
    }
    
    .stButton button:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 16px rgba(0, 133, 124, 0.4);
        background: linear-gradient(45deg, #00BCD4, #4CAF50);
    }
    
    .suggestion-btn {
        background: linear-gradient(45deg, #E1F5FE, #B2EBF2) !important;
        color: #00695C !important;
        border: 2px solid #00BCD4 !important;
        border-radius: 8px !important;
        font-size: 0.9rem !important;
        padding: 0.6rem 1rem !important;
        margin: 0.2rem !important;
        transition: all 0.3s ease !important;
    }
    
    .suggestion-btn:hover {
        background: linear-gradient(45deg, #B2EBF2, #80DEEA) !important;
        transform: translateY(-1px) !important;
        box-shadow: 0 4px 12px rgba(0, 188, 212, 0.3) !important;
    }
    
    .neo4j-metric {
        background: linear-gradient(135deg, #00857C, #00BCD4);
        color: white;
        padding: 1.2rem;
        border-radius: 12px;
        text-align: center;
        margin: 0.3rem 0;
        box-shadow: 0 4px 12px rgba(0, 133, 124, 0.3);
    }
    
    .success-box {
        background: linear-gradient(135deg, #4CAF50, #8BC34A);
        border: none;
        color: white;
        padding: 1.2rem;
        border-radius: 12px;
        margin: 0.8rem 0;
        box-shadow: 0 4px 12px rgba(76, 175, 80, 0.3);
        font-weight: 500;
    }
    
    .info-box {
        background: linear-gradient(135deg, #00BCD4, #4FC3F7);
        border: none;
        color: white;
        padding: 1.2rem;
        border-radius: 12px;
        margin: 0.8rem 0;
        box-shadow: 0 4px 12px rgba(0, 188, 212, 0.3);
    }
    
    .warning-box {
        background: linear-gradient(135deg, #FF9800, #FFC107);
        border: none;
        color: white;
        padding: 1.2rem;
        border-radius: 12px;
        margin: 0.8rem 0;
        box-shadow: 0 4px 12px rgba(255, 152, 0, 0.3);
    }
    
    .chat-container {
        background: linear-gradient(135deg, #F5F5F5, #FAFAFA);
        padding: 1.5rem;
        border-radius: 15px;
        margin: 1rem 0;
        border: 2px solid #E0E0E0;
        box-shadow: 0 4px 12px rgba(0,0,0,0.1);
    }
    
    .response-container {
        background: linear-gradient(135deg, #00857C, #00BCD4);
        color: white;
        padding: 1.5rem;
        border-radius: 15px;
        margin: 1rem 0;
        box-shadow: 0 6px 20px rgba(0, 133, 124, 0.3);
    }
    
    .schema-box {
        background: linear-gradient(135deg, #9C27B0, #E91E63);
        color: white;
        padding: 1.2rem;
        border-radius: 12px;
        margin: 0.8rem 0;
        box-shadow: 0 4px 12px rgba(156, 39, 176, 0.3);
    }
    
    .graph-controls {
        background: linear-gradient(135deg, #37474F, #546E7A);
        color: white;
        padding: 1rem;
        border-radius: 10px;
        margin: 0.5rem 0;
    }
    
    .cypher-query {
        background: #263238;
        color: #4CAF50;
        padding: 1rem;
        border-radius: 8px;
        font-family: 'Courier New', monospace;
        font-size: 0.95rem;
        margin: 0.8rem 0;
        border-left: 4px solid #00BCD4;
        box-shadow: 0 2px 8px rgba(0,0,0,0.2);
    }
    
    .legend-container {
        background: linear-gradient(135deg, #ECEFF1, #F5F5F5);
        padding: 1.2rem;
        border-radius: 12px;
        margin: 1rem 0;
        border: 2px solid #CFD8DC;
        box-shadow: 0 2px 8px rgba(0,0,0,0.1);
    }
    
    .status-connected {
        color: #4CAF50;
        font-weight: bold;
        background: rgba(76, 175, 80, 0.1);
        padding: 0.5rem 1rem;
        border-radius: 20px;
    }
    
    .status-disconnected {
        color: #F44336;
        font-weight: bold;
        background: rgba(244, 67, 54, 0.1);
        padding: 0.5rem 1rem;
        border-radius: 20px;
    }
    
    .graph-frame {
        border: 3px solid #00BCD4;
        border-radius: 15px;
        overflow: hidden;
        background: white;
        box-shadow: 0 8px 24px rgba(0, 188, 212, 0.2);
        margin: 1rem 0;
    }
    
    .graph-header {
        background: linear-gradient(90deg, #00857C, #00BCD4);
        color: white;
        padding: 1rem 1.5rem;
        font-weight: bold;
        font-size: 1.1rem;
    }
</style>
""", unsafe_allow_html=True)

# Initialize stable session state
def init_stable_session_state():
    """Initialize session state with stable defaults"""
    defaults = {
        "messages": [],
        "graph_data": None,
        "last_response": None,
        "session_id": str(uuid.uuid4()),
        "connection_status": "unknown",
        "current_schema": {},
        "processing": False,
        "selected_suggestion": "",
        "chat_history": [],
        "graph_settings": {
            "node_size": 25,
            "edge_width": 2,
            "physics_enabled": True,
            "show_labels": True
        }
    }
    
    for key, default_value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = default_value

init_stable_session_state()

# Header with Neo4j styling
st.markdown('<h1 class="neo4j-header">🕸️ Neo4j Graph Explorer Pro</h1>', unsafe_allow_html=True)
st.markdown('<p class="neo4j-subtitle">🧠 Schema-Aware • 🚀 Unlimited Exploration • 📊 Real-time Visualization</p>', unsafe_allow_html=True)

def check_api_health():
    """Check API health with detailed status"""
    try:
        response = requests.get("http://localhost:8081/health", timeout=5)
        if response.status_code == 200:
            health_data = response.json()
            return {"status": "connected", "data": health_data}
        else:
            return {"status": "error", "error": f"HTTP {response.status_code}"}
    except requests.exceptions.ConnectionError:
        return {"status": "disconnected", "error": "Agent API not running on port 8081"}
    except Exception as e:
        return {"status": "error", "error": str(e)}

def call_enhanced_agent_api(question: str, unlimited: bool = True) -> dict:
    """Enhanced API call with better error handling and unlimited option"""
    try:
        api_url = "http://localhost:8081/chat"
        
        # Set high node limit for unlimited exploration
        node_limit = 10000 if unlimited else 100
        
        payload = {
            "question": question,
            "session_id": st.session_state.session_id,
            "node_limit": node_limit
        }
        
        st.session_state.processing = True
        
        with st.spinner("🧠 Processing with schema-aware AI agent..."):
            start_time = time.time()
            response = requests.post(api_url, json=payload, timeout=120)  # Increased timeout
            response.raise_for_status()
            result = response.json()
            processing_time = time.time() - start_time
            
            result["processing_time"] = processing_time
            st.session_state.connection_status = "connected"
            st.session_state.processing = False
            
            return result
            
    except requests.exceptions.ConnectionError:
        st.session_state.connection_status = "disconnected"
        st.session_state.processing = False
        st.error("❌ Cannot connect to agent API. Please ensure the FastAPI server is running on port 8081.")
        return None
    except requests.exceptions.Timeout:
        st.session_state.processing = False
        st.error("⏰ Request timed out. The query might be very complex. Try a simpler query or check server status.")
        return None
    except Exception as e:
        st.session_state.processing = False
        st.error(f"❌ API Error: {str(e)}")
        return None

def get_neo4j_colors():
    """Get Neo4j-inspired color palette"""
    return {
        "Person": "#FF6B6B",        # Red
        "Movie": "#4ECDC4",         # Turquoise  
        "Company": "#45B7D1",       # Blue
        "Product": "#96CEB4",       # Green
        "Location": "#FECA57",      # Yellow
        "Event": "#FF9FF3",         # Pink
        "User": "#A55EEA",          # Purple
        "Order": "#26DE81",         # Emerald
        "Category": "#FD79A8",      # Rose
        "Department": "#6C5CE7",    # Indigo
        "Project": "#FDCB6E",       # Orange
        "Actor": "#00CEC9",         # Cyan
        "Director": "#E84393",      # Magenta
        "Producer": "#00B894",      # Teal
        "Organization": "#74B9FF",  # Light Blue
        "Country": "#A29BFE",       # Lavender
        "Technology": "#FD79A8",    # Pink
        "Language": "#FDCB6E",      # Gold
        "default": "#95A5A6"        # Gray
    }

def get_relationship_colors():
    """Get relationship colors"""
    return {
        "KNOWS": "#E74C3C",
        "FRIEND_OF": "#E74C3C", 
        "WORKS_FOR": "#3498DB",
        "MANAGES": "#9B59B6",
        "LOCATED_IN": "#F39C12",
        "BELONGS_TO": "#27AE60",
        "CREATED": "#E91E63",
        "OWNS": "#673AB7",
        "USES": "#009688",
        "ACTED_IN": "#2196F3",
        "DIRECTED": "#FF9800",
        "PRODUCED": "#4CAF50",
        "LOVES": "#E91E63",
        "MARRIED_TO": "#FD79A8",
        "REPORTS_TO": "#795548",
        "MEMBER_OF": "#607D8B",
        "LIVES_IN": "#FF5722",
        "STUDIED_AT": "#3F51B5",
        "default": "#666666"
    }

def extract_display_name(node):
    """Enhanced name extraction with better fallbacks"""
    try:
        props = node.get("properties", {})
        labels = node.get("labels", ["Node"])
        node_id = str(node.get("id", ""))
        
        # Priority order for name extraction
        name_fields = [
            "name", "title", "fullName", "displayName", 
            "username", "label", "firstName", "lastName",
            "email", "id", "identifier"
        ]
        
        for field in name_fields:
            if field in props and props[field]:
                name = str(props[field]).strip()
                if name and len(name) > 0:
                    return name[:40]  # Reasonable length for display
        
        # If no name found, create meaningful identifier
        label = labels[0] if labels else "Node"
        if node_id:
            short_id = node_id.split(":")[-1][-6:] if ":" in node_id else node_id[-6:]
            return f"{label}_{short_id}"
        
        return f"{label}_{hash(str(node)) % 10000}"
        
    except Exception as e:
        return f"Node_{hash(str(node)) % 10000}"

def create_enhanced_legend(nodes, relationships):
    """Create comprehensive legend with Neo4j styling"""
    try:
        node_colors = get_neo4j_colors()
        rel_colors = get_relationship_colors()
        
        # Analyze actual data
        node_types = {}
        rel_types = {}
        
        for node in nodes:
            labels = node.get("labels", ["Unknown"])
            if labels:
                label = labels[0]
                color = node_colors.get(label, node_colors["default"])
                node_types[label] = color
        
        for rel in relationships:
            rel_type = rel.get("type", "CONNECTED")
            color = rel_colors.get(rel_type, rel_colors["default"])
            rel_types[rel_type] = color
        
        legend_html = '<div class="legend-container">'
        legend_html += '<h4 style="margin-top: 0; color: #00857C;">🎨 Network Legend</h4>'
        
        if node_types:
            legend_html += '<p><strong>📊 Node Types:</strong><br>'
            for label, color in sorted(node_types.items()):
                legend_html += f'<span style="background: {color}; color: white; padding: 4px 12px; border-radius: 15px; margin: 3px; font-size: 13px; font-weight: 500;">{label}</span> '
            legend_html += '</p>'
        
        if rel_types:
            legend_html += '<p><strong>🔗 Relationship Types:</strong><br>'
            for rel_type, color in sorted(rel_types.items()):
                legend_html += f'<span style="background: {color}; color: white; padding: 4px 12px; border-radius: 15px; margin: 3px; font-size: 13px; font-weight: 500;">{rel_type}</span> '
            legend_html += '</p>'
        
        legend_html += '</div>'
        return legend_html
        
    except Exception as e:
        return f'<div class="legend-container">Legend error: {str(e)}</div>'

def render_neo4j_graph(graph_data: dict) -> bool:
    """Render graph with Neo4j-like appearance and unlimited nodes (FIXED for ID mapping)."""
    import streamlit as st
    from pyvis.network import Network
    import tempfile
    import traceback

    if not graph_data:
        st.info("🔍 No graph data available for visualization.")
        return False

    try:
        nodes = graph_data.get("nodes", [])
        relationships = graph_data.get("relationships", [])
        if not nodes:
            st.info("📊 No nodes found in the current result set.")
            return False

        # Create Pyvis network
        net = Network(
            height="700px",
            width="100%",
            bgcolor="#F8F9FA",
            font_color="#2C3E50",
        )

        # --- Always use Neo4j node IDs! ---
        added_nodes = set()
        node_colors = get_neo4j_colors()

        for node in nodes:
            node_id = str(node.get("id"))  # Use Neo4j's real node ID!
            display_name = extract_display_name(node)
            labels = node.get("labels", ["Unknown"])
            main_label = labels[0] if labels else "Unknown"
            color = node_colors.get(main_label, node_colors["default"])
            props = node.get("properties", {})
            tooltip_parts = [f"<b>{display_name}</b>", f"Type: {main_label}"]
            important_props = ["id", "name", "title", "description", "email", "age", "role"]
            for prop in important_props:
                if prop in props and props[prop]:
                    tooltip_parts.append(f"{prop}: {props[prop]}")
            tooltip = "<br>".join(tooltip_parts)
            net.add_node(
                node_id,
                label=display_name,
                color=color,
                size=st.session_state.graph_settings["node_size"],
                title=tooltip,
                shape='dot'
            )
            added_nodes.add(node_id)

        rel_colors = get_relationship_colors()
        added_edges = 0

        for rel in relationships:
            start_id = str(rel.get("startNode", rel.get("start", "")))
            end_id = str(rel.get("endNode", rel.get("end", "")))
            rel_type = str(rel.get("type", "CONNECTED"))
            color = rel_colors.get(rel_type, rel_colors["default"])
            rel_props = rel.get("properties", {})
            rel_tooltip = f"<b>{rel_type}</b>"
            if rel_props:
                for key, value in list(rel_props.items())[:3]:
                    rel_tooltip += f"<br>{key}: {value}"
            # Only add edges if both nodes exist
            if start_id in added_nodes and end_id in added_nodes:
                net.add_edge(
                    start_id,
                    end_id,
                    label=rel_type,
                    color=color,
                    width=st.session_state.graph_settings["edge_width"],
                    title=rel_tooltip,
                    arrows='to'
                )
                added_edges += 1

        # Enable/Disable physics
        if st.session_state.graph_settings["physics_enabled"]:
            net.toggle_physics(True)
        else:
            net.toggle_physics(False)

        # Save and display the graph
        with tempfile.NamedTemporaryFile(delete=False, suffix=".html") as tmp_file:
            net.save_graph(tmp_file.name)
            html_content = open(tmp_file.name, "r", encoding="utf-8").read()
            st.components.v1.html(html_content, height=700, scrolling=True)
        return True

    except Exception as e:
        st.error(f"❌ Graph rendering error: {str(e)}")
        st.code(traceback.format_exc())
        return False

# Main layout
col1, col2 = st.columns([1, 2], gap="large")

with col1:
    st.markdown("### 💬 Enhanced Chat Interface")
    
    # API Status with enhanced display
    health = check_api_health()
    if health["status"] == "connected":
        st.markdown('<div class="status-connected">🟢 API Connected & Ready</div>', unsafe_allow_html=True)
        if health.get("data"):
            agent_status = health["data"].get("agent_ready", False)
            st.markdown(f'<div class="info-box">🧠 <strong>Agent Status:</strong> {"Ready" if agent_status else "Initializing"}</div>', unsafe_allow_html=True)
    else:
        st.markdown('<div class="status-disconnected">🔴 API Disconnected</div>', unsafe_allow_html=True)
        st.error(f"❌ {health.get('error', 'Unknown error')}")
    
    # Enhanced prompt suggestions
    st.markdown("#### 💡 Smart Suggestions")
    st.markdown('<div class="info-box">🚀 <strong>Schema-Aware Prompts</strong> - Click to explore your graph without limits!</div>', unsafe_allow_html=True)
    
    # Categorized suggestions
    suggestion_categories = {
        "🔍 Data Exploration": [
            "Show me the complete network structure",
            "Display all nodes and their relationships", 
            "Show me all node types in the database",
            "Find the most connected nodes"
        ],
        "🏗️ Schema & Structure": [
            "Show me the database schema",
            "What node types exist?",
            "What relationship types are available?", 
            "Show me the database structure"
        ],
        "📊 Analysis Queries": [
            "Find communities in the network",
            "Show connection patterns", 
            "Analyze network density",
            "Find central nodes"
        ],
        "✏️ Data Modification": [
            "Create a new Person node",
            "Add relationships between nodes",
            "Update node properties",
            "Create a company and employees"
        ]
    }
    
    for category, suggestions in suggestion_categories.items():
        with st.expander(category, expanded=False):
            for suggestion in suggestions:
                if st.button(f"💭 {suggestion}", key=f"sug_{hash(suggestion)}", use_container_width=True):
                    st.session_state.selected_suggestion = suggestion
                    st.rerun()
    
    st.divider()
    
    # Enhanced question input
    st.markdown("#### ✍️ Ask Your Question")
    
    # Show selected suggestion
    if st.session_state.selected_suggestion:
        col_s1, col_s2 = st.columns([4, 1])
        with col_s1:
            st.success(f"📝 Selected: {st.session_state.selected_suggestion}")
        with col_s2:
            if st.button("🗑️", key="clear_suggestion"):
                st.session_state.selected_suggestion = ""
                st.rerun()
    
    with st.form("enhanced_question_form", clear_on_submit=True):
        user_question = st.text_area(
            "Your question:",
            value=st.session_state.selected_suggestion,
            placeholder="e.g., Show me the complete network structure with all relationships...",
            height=120,
            help="Ask anything about your Neo4j database - no limits on data exploration!"
        )
        
        col_form1, col_form2 = st.columns(2)
        with col_form1:
            unlimited_mode = st.checkbox(
                "🚀 Unlimited Mode", 
                value=True,
                help="Enable unlimited data exploration (recommended)"
            )
        
        with col_form2:
            show_schema = st.checkbox(
                "📋 Include Schema", 
                value=False,
                help="Show database schema in the response"
            )
        
        submit_button = st.form_submit_button(
            "🧠 Execute Query", 
            use_container_width=True,
            disabled=st.session_state.processing
        )
    
    # Clear selected suggestion after form submission
    if submit_button:
        st.session_state.selected_suggestion = ""
    
    if submit_button and user_question.strip() and not st.session_state.processing:
        # Add schema request if enabled
        final_question = user_question.strip()
        if show_schema:
            final_question += " Also show me the database schema."
        
        result = call_enhanced_agent_api(final_question, unlimited_mode)
        
        if result:
            # Store result and graph data
            st.session_state.last_response = result
            if result.get("graph_data"):
                st.session_state.graph_data = result["graph_data"]
            
            # Add to chat history
            chat_entry = {
                "timestamp": datetime.now().isoformat(),
                "question": final_question,
                "result": result,
                "unlimited": unlimited_mode
            }
            st.session_state.chat_history.append(chat_entry)
            
            st.success(f"✅ Query executed in {result.get('processing_time', 0):.2f}s!")
            st.rerun()
    
    st.divider()
    
    # Enhanced test data
    if st.button("🧪 Load Enhanced Test Dataset", use_container_width=True):
        enhanced_test_data = {
            "nodes": [
                {"id": "p1", "labels": ["Person"], "properties": {"name": "Alice Johnson", "age": 30, "role": "Senior Developer", "department": "Engineering", "email": "alice@techcorp.com"}},
                {"id": "p2", "labels": ["Person"], "properties": {"name": "Bob Smith", "age": 25, "role": "Designer", "department": "Marketing", "email": "bob@techcorp.com"}},
                {"id": "p3", "labels": ["Person"], "properties": {"name": "Carol Brown", "age": 35, "role": "Engineering Manager", "department": "Engineering", "email": "carol@techcorp.com"}},
                {"id": "p4", "labels": ["Person"], "properties": {"name": "David Wilson", "age": 28, "role": "Sales Executive", "department": "Sales", "email": "david@techcorp.com"}},
                {"id": "p5", "labels": ["Person"], "properties": {"name": "Emma Davis", "age": 32, "role": "Product Manager", "department": "Product", "email": "emma@techcorp.com"}},
                {"id": "c1", "labels": ["Company"], "properties": {"name": "TechCorp Inc.", "industry": "Technology", "employees": 500, "founded": 2010, "revenue": "50M"}},
                {"id": "c2", "labels": ["Company"], "properties": {"name": "InnovateAI", "industry": "Artificial Intelligence", "employees": 200, "founded": 2018, "revenue": "20M"}},
                {"id": "l1", "labels": ["Location"], "properties": {"name": "New York", "country": "USA", "population": 8000000, "timezone": "EST"}},
                {"id": "l2", "labels": ["Location"], "properties": {"name": "San Francisco", "country": "USA", "population": 900000, "timezone": "PST"}},
                {"id": "pr1", "labels": ["Project"], "properties": {"name": "AI Innovation Platform", "status": "Active", "budget": 2000000, "duration": "12 months"}},
                {"id": "pr2", "labels": ["Project"], "properties": {"name": "Mobile App Redesign", "status": "Completed", "budget": 500000, "duration": "6 months"}},
                {"id": "t1", "labels": ["Technology"], "properties": {"name": "Neo4j", "category": "Database", "type": "Graph Database"}},
                {"id": "t2", "labels": ["Technology"], "properties": {"name": "Python", "category": "Programming Language", "type": "High-level Language"}}
            ],
            "relationships": [
                {"startNode": "p1", "endNode": "p2", "type": "KNOWS", "properties": {"since": "2020", "relationship": "colleague"}},
                {"startNode": "p2", "endNode": "p4", "type": "FRIEND_OF", "properties": {"since": "2019", "closeness": "high"}},
                {"startNode": "p3", "endNode": "p1", "type": "MANAGES", "properties": {"since": "2021", "team": "Backend"}},
                {"startNode": "p3", "endNode": "p5", "type": "COLLABORATES_WITH", "properties": {"project": "AI Innovation"}},
                {"startNode": "p1", "endNode": "c1", "type": "WORKS_FOR", "properties": {"position": "Senior Developer", "salary": 120000}},
                {"startNode": "p2", "endNode": "c1", "type": "WORKS_FOR", "properties": {"position": "Designer", "salary": 85000}},
                {"startNode": "p3", "endNode": "c1", "type": "WORKS_FOR", "properties": {"position": "Engineering Manager", "salary": 150000}},
                {"startNode": "p4", "endNode": "c1", "type": "WORKS_FOR", "properties": {"position": "Sales Executive", "salary": 95000}},
                {"startNode": "p5", "endNode": "c2", "type": "WORKS_FOR", "properties": {"position": "Product Manager", "salary": 110000}},
                {"startNode": "c1", "endNode": "l1", "type": "LOCATED_IN", "properties": {"headquarters": True, "offices": 3}},
                {"startNode": "c2", "endNode": "l2", "type": "LOCATED_IN", "properties": {"headquarters": True, "offices": 1}},
                {"startNode": "p1", "endNode": "pr1", "type": "ASSIGNED_TO", "properties": {"role": "Technical Lead", "allocation": "100%"}},
                {"startNode": "p2", "endNode": "pr2", "type": "ASSIGNED_TO", "properties": {"role": "UI/UX Designer", "allocation": "80%"}},
                {"startNode": "p3", "endNode": "pr1", "type": "MANAGES", "properties": {"responsibility": "Budget and Timeline"}},
                {"startNode": "p5", "endNode": "pr1", "type": "OWNS", "properties": {"responsibility": "Product Vision"}},
                {"startNode": "pr1", "endNode": "t1", "type": "USES", "properties": {"purpose": "Data Storage"}},
                {"startNode": "pr1", "endNode": "t2", "type": "USES", "properties": {"purpose": "Backend Development"}},
                {"startNode": "c1", "endNode": "c2", "type": "PARTNER_WITH", "properties": {"type": "Strategic Alliance", "since": "2022"}}
            ]
        }
        
        st.session_state.graph_data = enhanced_test_data
        st.success("✅ Enhanced test dataset loaded!")
        st.rerun()
    
    # Schema debugging section
    if st.button("🔍 Debug Schema & Relationships", use_container_width=True):
        with st.spinner("🔍 Checking schema and relationship detection..."):
            try:
                # Check schema status
                schema_response = requests.get("http://localhost:8081/schema-status", timeout=10)
                debug_response = requests.get("http://localhost:8081/debug-schema", timeout=15)
                
                if schema_response.status_code == 200 and debug_response.status_code == 200:
                    schema_data = schema_response.json()
                    debug_data = debug_response.json()
                    
                    st.success("✅ Schema debugging completed!")
                    
                    # Display schema information
                    with st.expander("📊 Schema Cache Status", expanded=True):
                        cache = schema_data.get("cache", {})
                        stats = schema_data.get("statistics", {})
                        
                        col_s1, col_s2, col_s3 = st.columns(3)
                        with col_s1:
                            st.metric("Node Labels", stats.get("labels_count", 0))
                        with col_s2:
                            st.metric("Relationship Types", stats.get("relationship_types_count", 0))
                        with col_s3:
                            st.metric("Relationship Patterns", stats.get("relationship_patterns_count", 0))
                        
                        if cache.get("labels"):
                            st.write("**🏷️ Found Node Labels:**")
                            st.write(", ".join(cache["labels"]))
                        
                        if cache.get("relationship_types"):
                            st.write("**🔗 Found Relationship Types:**")
                            st.write(", ".join(cache["relationship_types"]))
                        
                        # Show relationship patterns
                        patterns_summary = schema_data.get("patterns_summary", {})
                        if patterns_summary:
                            st.write("**🔀 Relationship Patterns:**")
                            for rel_type, patterns in patterns_summary.items():
                                st.write(f"• **{rel_type}**: {', '.join(patterns[:3])}{'...' if len(patterns) > 3 else ''}")
                    
                    # Display debug information
                    with st.expander("🔧 Debug Test Results", expanded=False):
                        tests = debug_data.get("tests", {})
                        
                        for test_name, test_result in tests.items():
                            status = test_result.get("status", "unknown")
                            if status == "success":
                                st.success(f"✅ {test_name.replace('_', ' ').title()}")
                            elif status == "failed":
                                st.error(f"❌ {test_name.replace('_', ' ').title()}")
                            else:
                                st.warning(f"⚠️ {test_name.replace('_', ' ').title()}: {test_result.get('error', 'Unknown error')}")
                            
                            # Show additional details
                            if isinstance(test_result, dict):
                                details = {k: v for k, v in test_result.items() if k not in ['status', 'error']}
                                if details:
                                    st.json(details)
                
                else:
                    st.error("❌ Could not retrieve schema debugging information")
                    
            except Exception as e:
                st.error(f"❌ Schema debugging failed: {e}")
    
    # Manual schema refresh
    if st.button("🔄 Refresh Schema Cache", use_container_width=True):
        with st.spinner("🔄 Refreshing schema cache..."):
            try:
                refresh_response = requests.post("http://localhost:8081/refresh-schema", timeout=20)
                
                if refresh_response.status_code == 200:
                    refresh_data = refresh_response.json()
                    st.success(f"✅ Schema refreshed successfully!")
                    
                    summary = refresh_data.get("cache_summary", {})
                    st.write(f"**Found:** {summary.get('labels_count', 0)} labels, {summary.get('relationship_types_count', 0)} relationships")
                    st.write(f"**Refresh time:** {refresh_data.get('refresh_time_ms', 0):.1f}ms")
                else:
                    st.error("❌ Schema refresh failed")
                    
            except Exception as e:
                st.error(f"❌ Schema refresh error: {e}")
    
    # Chat history section
    st.markdown("#### 📚 Recent Queries")
    
    if st.session_state.chat_history:
        # Show recent queries with better formatting
        recent_queries = st.session_state.chat_history[-5:]  # Last 5 queries
        
        for i, entry in enumerate(reversed(recent_queries)):
            with st.expander(f"🔍 Query {len(st.session_state.chat_history) - i}: {entry['question'][:50]}...", expanded=False):
                st.write(f"**⏰ Time:** {entry['timestamp'][:19]}")
                st.write(f"**🚀 Mode:** {'Unlimited' if entry['unlimited'] else 'Limited'}")
                st.write(f"**❓ Question:** {entry['question']}")
                
                result = entry['result']
                if result.get('tool'):
                    st.write(f"**🔧 Tool Used:** {result['tool']}")
                if result.get('query'):
                    st.code(result['query'], language='cypher')
                
                col_h1, col_h2 = st.columns(2)
                with col_h1:
                    if st.button(f"🔄 Repeat", key=f"repeat_{i}_{hash(entry['question'])}"):
                        result = call_enhanced_agent_api(entry['question'], entry['unlimited'])
                        if result:
                            st.session_state.last_response = result
                            if result.get("graph_data"):
                                st.session_state.graph_data = result["graph_data"]
                            st.rerun()
                
                with col_h2:
                    if entry['result'].get('graph_data') and st.button(f"📊 Load Graph", key=f"load_{i}_{hash(entry['question'])}"):
                        st.session_state.graph_data = entry['result']['graph_data']
                        st.success("Graph loaded!")
                        st.rerun()
        
        if st.button("🗑️ Clear History", use_container_width=True):
            st.session_state.chat_history = []
            st.success("History cleared!")
            st.rerun()
    else:
        st.info("💡 No queries yet. Ask a question to start exploring!")

with col2:
    st.markdown("### 🎨 Graph Visualization & Analysis")
    
    # Enhanced response display
    if st.session_state.last_response:
        answer = st.session_state.last_response.get("answer", "")
        tool_used = st.session_state.last_response.get("tool", "")
        query_used = st.session_state.last_response.get("query", "")
        processing_time = st.session_state.last_response.get("processing_time", 0)
        
        if answer:
            st.markdown("#### 🤖 AI Agent Response")
            
            # Response container with enhanced styling
            st.markdown(f'''
            <div class="response-container">
                <h4>🧠 Schema-Aware Analysis</h4>
                <p><strong>⚡ Processing Time:</strong> {processing_time:.2f}s</p>
                <p><strong>🔧 Tool Used:</strong> {tool_used}</p>
                <div style="margin-top: 1rem;">
                    {answer.replace("**", "<strong>").replace("**", "</strong>")}
                </div>
            </div>
            ''', unsafe_allow_html=True)
            
            # Show query if available
            if query_used:
                st.markdown("**🔧 Executed Cypher Query:**")
                st.markdown(f'<div class="cypher-query">{query_used}</div>', unsafe_allow_html=True)
    
    # Graph visualization section
    if st.session_state.graph_data:
        nodes = st.session_state.graph_data.get("nodes", [])
        relationships = st.session_state.graph_data.get("relationships", [])
        
        # Enhanced metrics display
        col2_1, col2_2, col2_3 = st.columns(3)
        with col2_1:
            st.markdown(f'<div class="neo4j-metric"><h2>{len(nodes)}</h2><p>Nodes</p></div>', unsafe_allow_html=True)
        with col2_2:
            st.markdown(f'<div class="neo4j-metric"><h2>{len(relationships)}</h2><p>Relationships</p></div>', unsafe_allow_html=True)
        with col2_3:
            connectivity = len(relationships) / max(len(nodes), 1)
            st.markdown(f'<div class="neo4j-metric"><h2>{connectivity:.1f}</h2><p>Connectivity</p></div>', unsafe_allow_html=True)
        
        # Enhanced legend
        if nodes or relationships:
            legend = create_enhanced_legend(nodes, relationships)
            st.markdown(legend, unsafe_allow_html=True)
        
        # Network insights
        if nodes:
            node_types = {}
            for node in nodes:
                labels = node.get("labels", ["Unknown"])
                if labels:
                    label = labels[0]
                    node_types[label] = node_types.get(label, 0) + 1
            
            if node_types:
                st.markdown(f'''
                <div class="schema-box">
                    <h4>📊 Network Composition</h4>
                    <p><strong>Node Types:</strong> {len(node_types)} different types</p>
                    <p><strong>Distribution:</strong> {", ".join([f"{k}({v})" for k, v in sorted(node_types.items())])}</p>
                </div>
                ''', unsafe_allow_html=True)
        
        # Render the graph
        st.markdown("#### 🕸️ Interactive Neo4j Graph")
        st.markdown('<div class="info-box">🎯 <strong>Neo4j-Style Visualization</strong> - Drag nodes, zoom, and explore relationships!</div>', unsafe_allow_html=True)
        
        success = render_neo4j_graph(st.session_state.graph_data)
        
        if success:
            if len(relationships) > 0:
                st.markdown(f'''
                <div class="success-box">
                    🎉 <strong>Success!</strong> Interactive graph shows {len(nodes)} nodes connected by {len(relationships)} relationships!
                    <br>🔍 <strong>Features:</strong> Click nodes for details, drag to rearrange, scroll to zoom
                </div>
                ''', unsafe_allow_html=True)
            else:
                st.markdown('<div class="warning-box">ℹ️ <strong>Isolated Nodes:</strong> No relationships found in current data</div>', unsafe_allow_html=True)
        else:
            st.error("❌ Graph rendering failed. Check the debug information above.")
    
    else:
        # Enhanced welcome screen
        st.markdown("""
        <div style="
            text-align: center; 
            padding: 3rem; 
            background: linear-gradient(135deg, #00857C 0%, #00BCD4 50%, #4CAF50 100%); 
            color: white; 
            border-radius: 20px; 
            margin: 2rem 0;
            box-shadow: 0 8px 24px rgba(0, 133, 124, 0.3);
        ">
            <h2>🕸️ Neo4j Graph Explorer Pro</h2>
            <p><strong>Schema-Aware • Unlimited Exploration • Real-time Visualization</strong></p>
            <div style="margin-top: 2rem; font-size: 1.1rem;">
                <p>🧠 AI agent reads your Neo4j schema automatically</p>
                <p>🚀 No limits on data exploration</p>
                <p>📊 Interactive Neo4j-style graph visualization</p>
                <p>🔍 Smart suggestions based on your data structure</p>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("""
        <div class="info-box">
            <h3 style="margin-top: 0;">🚀 Getting Started:</h3>
            <ol style="text-align: left; margin: 1rem 0;">
                <li><strong>Use Smart Suggestions</strong> - Click any suggestion to explore your data</li>
                <li><strong>Ask Natural Questions</strong> - "Show me the network structure"</li>
                <li><strong>Enable Unlimited Mode</strong> - Explore your entire graph without restrictions</li>
                <li><strong>Try Schema Queries</strong> - "What types of nodes exist?"</li>
            </ol>
        </div>
        """, unsafe_allow_html=True)

# Enhanced footer
st.markdown("---")
st.markdown("""
<div style="
    text-align: center; 
    padding: 2rem;
    background: linear-gradient(90deg, rgba(0, 133, 124, 0.1), rgba(0, 188, 212, 0.1));
    border-radius: 15px;
    margin-top: 2rem;
">
    <h3 style="
        margin: 0; 
        background: linear-gradient(90deg, #00857C, #00BCD4); 
        -webkit-background-clip: text; 
        -webkit-text-fill-color: transparent;
    ">
        🚀 Neo4j Graph Explorer Pro
    </h3>
    <p style="margin: 1rem 0; color: #00695C; font-weight: 500;">
        🧠 Schema-Aware AI Agent • 🕸️ Unlimited Graph Exploration • 📊 Real-time Visualization • 🔍 Smart Query Suggestions
    </p>
    <p style="margin: 0; color: #00857C; font-size: 0.9rem;">
        Session ID: <code>{st.session_state.session_id[:8]}...</code> | 
        Queries: <strong>{len(st.session_state.chat_history)}</strong> | 
        Status: <strong>{"🟢 Connected" if st.session_state.connection_status == "connected" else "🔴 Disconnected"}</strong>
    </p>
</div>
""", unsafe_allow_html=True)
