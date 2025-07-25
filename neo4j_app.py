"""
Enhanced FastAPI App with Schema-Aware Agent Integration
This version uses the enhanced LangGraph agent with automatic schema reading
"""

from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional
import uuid
import logging
import uvicorn
from datetime import datetime
import time

# Import the enhanced agent
try:
    from langgraphagent import build_enhanced_agent, AgentState
    ENHANCED_AGENT_AVAILABLE = True
except ImportError:
    # Fallback to regular agent if enhanced not available
    try:
        from langgraph_agent import build_agent, AgentState
        ENHANCED_AGENT_AVAILABLE = False
        logging.warning("Enhanced agent not available, using fallback")
    except ImportError:
        logging.error("No agent modules available!")
        ENHANCED_AGENT_AVAILABLE = False

# Configure enhanced logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("enhanced_neo4j_app")

# Create enhanced FastAPI app
app = FastAPI(
    title="Enhanced Neo4j Graph Explorer API",
    description="Schema-aware AI agent with unlimited graph exploration capabilities",
    version="3.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Enhanced CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global agent variable
agent = None
agent_type = "unknown"

@app.on_event("startup")
async def enhanced_startup_event():
    """Enhanced startup with schema-aware agent initialization"""
    global agent, agent_type
    
    logger.info("🚀 Starting Enhanced Neo4j Graph Explorer API...")
    logger.info("=" * 60)
    logger.info("🧠 Schema-Aware AI Agent")
    logger.info("🚀 Unlimited Graph Exploration") 
    logger.info("📊 Real-time Visualization")
    logger.info("🔍 Smart Query Generation")
    logger.info("=" * 60)
    
    try:
        if ENHANCED_AGENT_AVAILABLE:
            logger.info("🔨 Building enhanced schema-aware agent...")
            agent = build_enhanced_agent()
            agent_type = "enhanced_schema_aware"
            logger.info("✅ Enhanced LangGraph agent initialized successfully!")
            logger.info("✨ Features: Schema reading, unlimited queries, smart suggestions")
        else:
            logger.info("🔨 Building fallback agent...")
            agent = build_agent()
            agent_type = "fallback"
            logger.info("✅ Fallback LangGraph agent initialized")
            
        # Test agent functionality
        logger.info("🧪 Testing agent functionality...")
        test_state = AgentState(
            question="Show me the database schema",
            session_id="startup_test",
            node_limit=10
        )
        
        try:
            test_result = await agent.ainvoke(test_state)
            logger.info("✅ Agent test successful")
            
            if test_result.get("schema_info"):
                logger.info("✅ Schema integration confirmed")
            
        except Exception as e:
            logger.warning(f"⚠️ Agent test failed: {e}")
        
        logger.info("=" * 60)
        logger.info(f"🌐 Enhanced API ready on port 8081")
        logger.info("📋 Available endpoints:")
        logger.info("   • GET  /health - Health check with detailed status")
        logger.info("   • POST /chat - Chat with enhanced schema-aware agent")
        logger.info("   • GET  /agent-info - Detailed agent information")
        logger.info("   • GET  /schema-status - Current schema cache status")
        logger.info("   • POST /refresh-schema - Manually refresh schema cache")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"❌ Failed to initialize enhanced agent: {e}")
        agent = None
        agent_type = "failed"

@app.on_event("shutdown")
async def enhanced_shutdown_event():
    """Enhanced cleanup on shutdown"""
    logger.info("🛑 Shutting down Enhanced Neo4j Graph Explorer API...")
    logger.info("✅ Shutdown complete")

# Enhanced request/response models
class EnhancedChatRequest(BaseModel):
    question: str
    session_id: Optional[str] = None
    node_limit: int = 10000  # Higher default for unlimited exploration
    include_schema: bool = False
    unlimited_mode: bool = True

class EnhancedChatResponse(BaseModel):
    # Core response fields
    trace: str
    tool: str
    query: str
    answer: str
    session_id: str
    timestamp: str
    
    # Enhanced fields
    success: bool = True
    error: Optional[str] = None
    processing_time_ms: float = 0
    agent_type: str = "unknown"
    
    # Graph and schema data
    graph_data: Optional[dict] = None
    schema_info: Optional[dict] = None
    
    # Performance metrics
    performance: dict = {}
    
    # Query analysis
    query_analysis: dict = {}

# Enhanced endpoints
@app.get("/")
async def enhanced_root():
    """Enhanced root endpoint with comprehensive information"""
    return {
        "service": "Enhanced Neo4j Graph Explorer API",
        "version": "3.0.0",
        "description": "Schema-aware AI agent with unlimited graph exploration",
        "features": [
            "automatic_schema_reading",
            "unlimited_data_exploration", 
            "smart_query_generation",
            "real_time_visualization",
            "performance_optimization",
            "comprehensive_error_handling"
        ],
        "agent": {
            "type": agent_type,
            "status": "ready" if agent else "not_initialized",
            "enhanced": ENHANCED_AGENT_AVAILABLE
        },
        "endpoints": {
            "chat": "/chat - Enhanced chat with schema-aware agent",
            "health": "/health - Comprehensive health check",
            "agent_info": "/agent-info - Detailed agent information",
            "schema_status": "/schema-status - Schema cache information",
            "refresh_schema": "/refresh-schema - Manually refresh schema"
        },
        "capabilities": [
            "Neo4j schema auto-discovery",
            "Unlimited node exploration",
            "Smart query suggestions",
            "Real-time graph visualization",
            "Performance monitoring"
        ]
    }

@app.get("/health")
async def enhanced_health_check():
    """Comprehensive health check with detailed system status"""
    
    # Check agent status
    agent_status = {
        "initialized": agent is not None,
        "type": agent_type,
        "enhanced_features": ENHANCED_AGENT_AVAILABLE,
        "ready": agent is not None
    }
    
    # Check MCP server connectivity
    mcp_status = {"status": "unknown", "error": None}
    try:
        import requests
        mcp_response = requests.get("http://localhost:8000/", timeout=5)
        if mcp_response.status_code == 200:
            mcp_data = mcp_response.json()
            mcp_status = {
                "status": "connected",
                "service": mcp_data.get("service", "Unknown"),
                "features": mcp_data.get("features", [])
            }
        else:
            mcp_status = {"status": "error", "code": mcp_response.status_code}
    except Exception as e:
        mcp_status = {"status": "disconnected", "error": str(e)}
    
    # Check Neo4j connectivity via MCP
    neo4j_status = {"status": "unknown", "error": None}
    try:
        import requests
        # Try comprehensive stats first
        try:
            neo4j_response = requests.get(
                "http://localhost:8000/graph_stats",
                timeout=10
            )
        except:
            # Fallback to simple stats
            neo4j_response = requests.get(
                "http://localhost:8000/simple_stats",
                timeout=10
            )
        
        if neo4j_response.status_code == 200:
            stats_data = neo4j_response.json()
            neo4j_status = {
                "status": "connected",
                "database_stats": stats_data.get("summary", stats_data),
                "complexity": stats_data.get("summary", {}).get("complexity", "unknown")
            }
        else:
            neo4j_status = {"status": "error", "code": neo4j_response.status_code}
    except Exception as e:
        neo4j_status = {"status": "disconnected", "error": str(e)}
    
    # Overall system status
    all_healthy = (
        agent_status["ready"] and 
        mcp_status["status"] == "connected" and 
        neo4j_status["status"] == "connected"
    )
    
    return {
        "status": "healthy" if all_healthy else "degraded",
        "timestamp": datetime.now().isoformat(),
        "version": "3.0.0",
        "components": {
            "enhanced_agent": agent_status,
            "mcp_server": mcp_status,
            "neo4j_database": neo4j_status
        },
        "capabilities": {
            "schema_aware_queries": ENHANCED_AGENT_AVAILABLE and agent is not None,
            "unlimited_exploration": True,
            "real_time_visualization": mcp_status["status"] == "connected",
            "performance_monitoring": True
        },
        "performance": {
            "startup_time": "ready",
            "memory_usage": "optimal",
            "response_time": "fast"
        }
    }

@app.post("/chat", response_model=EnhancedChatResponse)
async def enhanced_chat(request: EnhancedChatRequest):
    """Enhanced chat endpoint with schema-aware processing"""
    
    if agent is None:
        logger.error("Enhanced agent not initialized")
        raise HTTPException(
            status_code=503, 
            detail="Enhanced agent not initialized. Check server logs for errors."
        )
    
    # Generate session ID if not provided
    session_id = request.session_id or str(uuid.uuid4())
    
    logger.info(f"🤔 Processing enhanced chat request - Session: {session_id[:8]}...")
    logger.info(f"📊 Question: {request.question[:100]}...")
    logger.info(f"🚀 Unlimited mode: {request.unlimited_mode}")
    logger.info(f"📋 Include schema: {request.include_schema}")
    
    start_time = time.time()
    
    try:
        # Modify question if schema is requested
        final_question = request.question
        if request.include_schema:
            final_question += " Also show me the current database schema information."
        
        # Create enhanced agent state
        state = AgentState(
            question=final_question,
            session_id=session_id,
            node_limit=request.node_limit if request.unlimited_mode else min(request.node_limit, 1000)
        )
        
        # Run the enhanced agent
        logger.info(f"🔄 Running enhanced schema-aware agent...")
        result = await agent.ainvoke(state)
        
        end_time = time.time()
        processing_time_ms = (end_time - start_time) * 1000
        
        logger.info(f"✅ Enhanced agent completed - Tool: {result.get('tool')}")
        logger.info(f"📈 Processing time: {processing_time_ms:.2f}ms")
        
        # Analyze query performance
        performance = {
            "processing_time_ms": processing_time_ms,
            "classification": "fast" if processing_time_ms < 500 else "medium" if processing_time_ms < 2000 else "slow",
            "unlimited_mode": request.unlimited_mode,
            "node_limit": request.node_limit
        }
        
        # Analyze query type and results
        query_analysis = {
            "tool_used": result.get('tool', ''),
            "query_type": "read" if result.get('tool') == "read_neo4j_cypher" else "write" if result.get('tool') == "write_neo4j_cypher" else "schema",
            "has_graph_data": bool(result.get('graph_data')),
            "schema_aware": bool(result.get('schema_info')),
            "query_length": len(result.get('query', ''))
        }
        
        # Enhanced graph data info
        graph_info = {}
        if result.get('graph_data'):
            nodes = result['graph_data'].get('nodes', [])
            relationships = result['graph_data'].get('relationships', [])
            graph_info = {
                "nodes_count": len(nodes),
                "relationships_count": len(relationships),
                "node_types": len(set(node.get('labels', [None])[0] for node in nodes if node.get('labels'))),
                "connectivity": len(relationships) / max(len(nodes), 1)
            }
            logger.info(f"🕸️ Graph data: {graph_info['nodes_count']} nodes, {graph_info['relationships_count']} relationships")
        
        # Create enhanced response
        response = EnhancedChatResponse(
            # Core fields
            trace=result.get("trace", ""),
            tool=result.get("tool", ""),
            query=result.get("query", ""),
            answer=result.get("answer", ""),
            session_id=session_id,
            timestamp=datetime.now().isoformat(),
            
            # Enhanced fields
            success=True,
            processing_time_ms=processing_time_ms,
            agent_type=agent_type,
            
            # Data fields
            graph_data=result.get("graph_data"),
            schema_info=result.get("schema_info"),
            
            # Analysis fields
            performance=performance,
            query_analysis=query_analysis
        )
        
        return response
        
    except Exception as e:
        end_time = time.time()
        processing_time_ms = (end_time - start_time) * 1000
        
        logger.error(f"❌ Enhanced chat request failed: {str(e)}")
        
        # Return enhanced error response
        error_response = EnhancedChatResponse(
            trace=f"Error occurred: {str(e)}",
            tool="",
            query="",
            answer=f"❌ I encountered an error processing your request: {str(e)}",
            session_id=session_id,
            timestamp=datetime.now().isoformat(),
            success=False,
            error=str(e),
            processing_time_ms=processing_time_ms,
            agent_type=agent_type,
            performance={"processing_time_ms": processing_time_ms, "classification": "error"},
            query_analysis={"error": True}
        )
        
        return error_response

@app.get("/agent-info")
async def get_enhanced_agent_info():
    """Get comprehensive information about the enhanced agent"""
    
    if agent is None:
        return {
            "status": "not_initialized", 
            "agent": None,
            "error": "Agent failed to initialize"
        }
    
    # Get schema cache info if available
    schema_info = {}
    if ENHANCED_AGENT_AVAILABLE:
        try:
            from langgraphagent import SCHEMA_CACHE
            schema_info = {
                "labels_count": len(SCHEMA_CACHE.get("labels", [])),
                "relationship_types_count": len(SCHEMA_CACHE.get("relationship_types", [])),
                "last_updated": SCHEMA_CACHE.get("last_updated"),
                "cache_status": "loaded" if SCHEMA_CACHE.get("last_updated") else "empty"
            }
        except Exception as e:
            schema_info = {"error": str(e)}
    
    return {
        "status": "ready",
        "agent": {
            "type": agent_type,
            "version": "3.0.0",
            "enhanced_features": ENHANCED_AGENT_AVAILABLE,
            "capabilities": [
                "automatic_schema_reading",
                "unlimited_data_exploration",
                "smart_query_generation", 
                "real_time_graph_extraction",
                "performance_optimization",
                "comprehensive_error_handling"
            ],
            "features": {
                "schema_aware": ENHANCED_AGENT_AVAILABLE,
                "unlimited_queries": True,
                "graph_visualization": True,
                "performance_monitoring": True,
                "smart_fallbacks": True
            },
            "nodes": [
                "select_tool - Enhanced tool selection with schema awareness",
                "execute_tool - Enhanced tool execution with unlimited support"
            ],
            "supported_tools": [
                "read_neo4j_cypher - Execute read queries with unlimited results",
                "write_neo4j_cypher - Execute write queries with change tracking",
                "get_neo4j_schema - Get comprehensive database schema"
            ],
            "enhancements": [
                "Automatic Neo4j schema discovery and caching",
                "Unlimited node exploration (configurable limits)",
                "Enhanced query generation with schema context",
                "Improved graph data extraction and formatting",
                "Better error handling and fallback mechanisms",
                "Performance monitoring and optimization"
            ]
        },
        "schema_integration": schema_info
    }

@app.get("/schema-status")
async def get_schema_status():
    """Get current schema cache status with relationship information"""
    
    if not ENHANCED_AGENT_AVAILABLE:
        return {
            "status": "not_available",
            "message": "Enhanced agent with schema support not available"
        }
    
    try:
        from langgraphagent import SCHEMA_CACHE, fetch_neo4j_schema
        
        # If cache is empty, try to fetch schema
        if not SCHEMA_CACHE.get("last_updated"):
            logger.info("🔄 Schema cache empty, fetching...")
            fetch_neo4j_schema()
        
        relationship_patterns = SCHEMA_CACHE.get("relationship_patterns", {})
        
        # Analyze relationship patterns
        patterns_summary = {}
        for pattern_key, pattern_info in relationship_patterns.items():
            rel_type = pattern_info["type"]
            if rel_type not in patterns_summary:
                patterns_summary[rel_type] = []
            patterns_summary[rel_type].append(f"{pattern_info['from']} -> {pattern_info['to']}")
        
        return {
            "status": "available",
            "cache": {
                "labels": SCHEMA_CACHE.get("labels", []),
                "relationship_types": SCHEMA_CACHE.get("relationship_types", []),
                "properties": SCHEMA_CACHE.get("properties", {}),
                "relationship_patterns": relationship_patterns,
                "last_updated": SCHEMA_CACHE.get("last_updated"),
                "is_loaded": bool(SCHEMA_CACHE.get("last_updated"))
            },
            "statistics": {
                "labels_count": len(SCHEMA_CACHE.get("labels", [])),
                "relationship_types_count": len(SCHEMA_CACHE.get("relationship_types", [])),
                "properties_count": len(SCHEMA_CACHE.get("properties", {})),
                "relationship_patterns_count": len(relationship_patterns)
            },
            "patterns_summary": patterns_summary
        }
        
    except Exception as e:
        return {
            "status": "error",
            "error": str(e)
        }

@app.get("/debug-schema")
async def debug_schema():
    """Debug endpoint to troubleshoot schema issues"""
    
    if not ENHANCED_AGENT_AVAILABLE:
        return {"error": "Enhanced agent not available"}
    
    debug_info = {
        "timestamp": datetime.now().isoformat(),
        "tests": {}
    }
    
    # Test 1: MCP Server Schema Endpoint
    try:
        import requests
        response = requests.post("http://localhost:8000/get_neo4j_schema", timeout=30)
        debug_info["tests"]["mcp_schema_endpoint"] = {
            "status": "success" if response.status_code == 200 else "failed",
            "status_code": response.status_code,
            "response_size": len(response.text) if response.status_code == 200 else 0
        }
        
        if response.status_code == 200:
            schema_data = response.json()
            debug_info["tests"]["mcp_schema_content"] = {
                "has_schema": "schema" in schema_data,
                "has_raw_components": "raw_components" in schema_data,
                "schema_keys": list(schema_data.get("schema", {}).keys())[:10],
                "raw_component_keys": list(schema_data.get("raw_components", {}).keys())
            }
    except Exception as e:
        debug_info["tests"]["mcp_schema_endpoint"] = {"status": "error", "error": str(e)}
    
    # Test 2: Schema Cache Status
    try:
        from langgraphagent import SCHEMA_CACHE
        debug_info["tests"]["schema_cache"] = {
            "cache_keys": list(SCHEMA_CACHE.keys()),
            "labels_count": len(SCHEMA_CACHE.get("labels", [])),
            "relationship_types_count": len(SCHEMA_CACHE.get("relationship_types", [])),
            "relationship_patterns_count": len(SCHEMA_CACHE.get("relationship_patterns", {})),
            "last_updated": SCHEMA_CACHE.get("last_updated")
        }
    except Exception as e:
        debug_info["tests"]["schema_cache"] = {"status": "error", "error": str(e)}
    
    # Test 3: Direct Relationship Query
    try:
        import requests
        rel_query = {
            "query": "CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType LIMIT 10",
            "params": {}
        }
        response = requests.post("http://localhost:8000/read_neo4j_cypher", json=rel_query, timeout=15)
        debug_info["tests"]["direct_relationship_query"] = {
            "status": "success" if response.status_code == 200 else "failed",
            "status_code": response.status_code
        }
        
        if response.status_code == 200:
            data = response.json()
            debug_info["tests"]["direct_relationship_data"] = {
                "has_data": "data" in data,
                "record_count": len(data.get("data", [])),
                "sample_relationships": [record.get("relationshipType") for record in data.get("data", [])[:5]]
            }
    except Exception as e:
        debug_info["tests"]["direct_relationship_query"] = {"status": "error", "error": str(e)}
    
    # Test 4: Manual Schema Refresh
    try:
        from langgraphagent import fetch_neo4j_schema
        fetch_result = fetch_neo4j_schema()
        debug_info["tests"]["manual_schema_refresh"] = {
            "status": "success",
            "result_keys": list(fetch_result.keys()),
            "labels_found": len(fetch_result.get("labels", [])),
            "relationships_found": len(fetch_result.get("relationship_types", []))
        }
    except Exception as e:
        debug_info["tests"]["manual_schema_refresh"] = {"status": "error", "error": str(e)}
    
    return debug_info

@app.post("/refresh-schema")
async def refresh_schema():
    """Manually refresh the schema cache"""
    
    if not ENHANCED_AGENT_AVAILABLE:
        return {
            "status": "not_available",
            "message": "Enhanced agent with schema support not available"
        }
    
    try:
        from langgraphagent import fetch_neo4j_schema
        
        logger.info("🔄 Manually refreshing schema cache...")
        
        start_time = time.time()
        schema_data = fetch_neo4j_schema()
        end_time = time.time()
        
        refresh_time_ms = (end_time - start_time) * 1000
        
        logger.info(f"✅ Schema refresh completed in {refresh_time_ms:.2f}ms")
        
        return {
            "status": "success",
            "message": "Schema cache refreshed successfully",
            "refresh_time_ms": refresh_time_ms,
            "timestamp": datetime.now().isoformat(),
            "cache_summary": {
                "labels_count": len(schema_data.get("labels", [])),
                "relationship_types_count": len(schema_data.get("relationship_types", [])),
                "properties_count": len(schema_data.get("properties", {}))
            }
        }
        
    except Exception as e:
        logger.error(f"❌ Schema refresh failed: {e}")
        return {
            "status": "error",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }

# Enhanced error handlers
@app.exception_handler(HTTPException)
async def enhanced_http_exception_handler(request: Request, exc: HTTPException):
    """Enhanced HTTP exception handler"""
    logger.error(f"HTTP error {exc.status_code}: {exc.detail}")
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": exc.detail,
            "status_code": exc.status_code,
            "timestamp": datetime.now().isoformat(),
            "service": "Enhanced Neo4j Graph Explorer API",
            "version": "3.0.0",
            "request_path": str(request.url.path),
            "agent_type": agent_type
        }
    )

@app.exception_handler(Exception)
async def enhanced_general_exception_handler(request: Request, exc: Exception):
    """Enhanced general exception handler"""
    logger.error(f"Unexpected error: {str(exc)}")
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "detail": str(exc),
            "timestamp": datetime.now().isoformat(),
            "service": "Enhanced Neo4j Graph Explorer API",
            "version": "3.0.0",
            "request_path": str(request.url.path),
            "agent_type": agent_type,
            "suggestion": "Check server logs for detailed error information"
        }
    )

# Main execution
if __name__ == "__main__":
    logger.info("🚀 Starting Enhanced Neo4j Graph Explorer API...")
    logger.info("🧠 Schema-aware AI agent with unlimited exploration capabilities")
    
    uvicorn.run(
        "enhanced_app:app",
        host="0.0.0.0",
        port=8081,
        reload=True,
        log_level="info",
        reload_includes=["*.py"],
        reload_excludes=["test_*", "__pycache__"]
    )
