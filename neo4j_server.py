"""
Enhanced MCP Server with unlimited graph support and better Neo4j integration
This version removes artificial limits and provides comprehensive graph data extraction
"""

import asyncio
import json
import logging
from datetime import datetime
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from neo4j import AsyncGraphDatabase, AsyncDriver, AsyncTransaction
from typing import Dict, Any, List, Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("enhanced_mcp_neo4j")



NEO4J_URI = "xx"
NEO4J_USER = "xx"
NEO4J_PASSWORD = "xx"  
NEO4J_DATABASE = "xx"

# Initialize enhanced Neo4j driver
try:
    driver: AsyncDriver = AsyncGraphDatabase.driver(
        NEO4J_URI, 
        auth=(NEO4J_USER, NEO4J_PASSWORD),
        connection_timeout=30,
        max_connection_lifetime=7200,  # 2 hours
        max_connection_pool_size=100   # Increased pool size
    )
    logger.info("✅ Enhanced Neo4j driver initialized")
except Exception as e:
    logger.error(f"❌ Failed to initialize Neo4j driver: {e}")
    driver = None

app = FastAPI(
    title="Enhanced MCP Neo4j Server - Unlimited Graph Support",
    description="Neo4j MCP server with unlimited data support and enhanced graph extraction",
    version="2.0.0"
)

class CypherRequest(BaseModel):
    query: str
    params: dict = {}
    node_limit: int = 10000  # Higher default limit

def extract_comprehensive_graph_data(records, node_limit=10000):
    """
    Enhanced graph data extraction with unlimited support and better node/relationship handling
    """
    nodes = {}
    relationships = []
    node_count = 0
    
    logger.info(f"🔍 Extracting graph data with node limit: {node_limit}")
    
    try:
        for record in records:
            for key, value in record.items():
                # Handle Neo4j Node objects
                if hasattr(value, 'labels') and hasattr(value, 'element_id'):
                    if node_count < node_limit:
                        node_id = str(value.element_id)
                        if node_id not in nodes:
                            properties = dict(value)
                            
                            # Ensure we have a display name
                            if not any(name_field in properties for name_field in ['name', 'title', 'displayName']):
                                # Create a meaningful name from available properties
                                if 'email' in properties:
                                    properties['name'] = properties['email'].split('@')[0]
                                elif 'id' in properties:
                                    properties['name'] = str(properties['id'])
                                elif value.labels:
                                    properties['name'] = f"{list(value.labels)[0]}_{node_count + 1}"
                                else:
                                    properties['name'] = f"Node_{node_count + 1}"
                            
                            nodes[node_id] = {
                                'id': node_id,
                                'labels': list(value.labels),
                                'properties': properties
                            }
                            node_count += 1
                
                # Handle Neo4j Relationship objects
                elif hasattr(value, 'type') and hasattr(value, 'start_node') and hasattr(value, 'end_node'):
                    try:
                        start_node_id = str(value.start_node.element_id)
                        end_node_id = str(value.end_node.element_id)
                        
                        # Add start node if not present and within limit
                        if start_node_id not in nodes and node_count < node_limit:
                            start_props = dict(value.start_node)
                            if not any(name_field in start_props for name_field in ['name', 'title', 'displayName']):
                                if value.start_node.labels:
                                    start_props['name'] = f"{list(value.start_node.labels)[0]}_{node_count + 1}"
                                else:
                                    start_props['name'] = f"Node_{node_count + 1}"
                            
                            nodes[start_node_id] = {
                                'id': start_node_id,
                                'labels': list(value.start_node.labels),
                                'properties': start_props
                            }
                            node_count += 1
                        
                        # Add end node if not present and within limit
                        if end_node_id not in nodes and node_count < node_limit:
                            end_props = dict(value.end_node)
                            if not any(name_field in end_props for name_field in ['name', 'title', 'displayName']):
                                if value.end_node.labels:
                                    end_props['name'] = f"{list(value.end_node.labels)[0]}_{node_count + 1}"
                                else:
                                    end_props['name'] = f"Node_{node_count + 1}"
                            
                            nodes[end_node_id] = {
                                'id': end_node_id,
                                'labels': list(value.end_node.labels),
                                'properties': end_props
                            }
                            node_count += 1
                        
                        # Add relationship
                        rel = {
                            'id': str(value.element_id),
                            'type': value.type,
                            'startNode': start_node_id,
                            'endNode': end_node_id,
                            'properties': dict(value)
                        }
                        relationships.append(rel)
                        
                    except Exception as e:
                        logger.warning(f"⚠️ Error processing relationship: {e}")
                        continue
                
                # Handle Neo4j Path objects
                elif hasattr(value, 'nodes') and hasattr(value, 'relationships'):
                    try:
                        # Extract nodes from path
                        for path_node in value.nodes:
                            if node_count < node_limit:
                                node_id = str(path_node.element_id)
                                if node_id not in nodes:
                                    props = dict(path_node)
                                    if not any(name_field in props for name_field in ['name', 'title', 'displayName']):
                                        if path_node.labels:
                                            props['name'] = f"{list(path_node.labels)[0]}_{node_count + 1}"
                                        else:
                                            props['name'] = f"Node_{node_count + 1}"
                                    
                                    nodes[node_id] = {
                                        'id': node_id,
                                        'labels': list(path_node.labels),
                                        'properties': props
                                    }
                                    node_count += 1
                        
                        # Extract relationships from path
                        for path_rel in value.relationships:
                            rel = {
                                'id': str(path_rel.element_id),
                                'type': path_rel.type,
                                'startNode': str(path_rel.start_node.element_id),
                                'endNode': str(path_rel.end_node.element_id),
                                'properties': dict(path_rel)
                            }
                            relationships.append(rel)
                            
                    except Exception as e:
                        logger.warning(f"⚠️ Error processing path: {e}")
                        continue
                
                # Handle lists (might contain nodes/relationships/paths)
                elif isinstance(value, list):
                    for item in value:
                        if hasattr(item, 'labels') and node_count < node_limit:
                            node_id = str(item.element_id)
                            if node_id not in nodes:
                                props = dict(item)
                                if not any(name_field in props for name_field in ['name', 'title', 'displayName']):
                                    if item.labels:
                                        props['name'] = f"{list(item.labels)[0]}_{node_count + 1}"
                                    else:
                                        props['name'] = f"Node_{node_count + 1}"
                                
                                nodes[node_id] = {
                                    'id': node_id,
                                    'labels': list(item.labels),
                                    'properties': props
                                }
                                node_count += 1
                        
                        elif hasattr(item, 'type') and hasattr(item, 'start_node'):
                            rel = {
                                'id': str(item.element_id),
                                'type': item.type,
                                'startNode': str(item.start_node.element_id),
                                'endNode': str(item.end_node.element_id),
                                'properties': dict(item)
                            }
                            relationships.append(rel)
        
        # Filter relationships to only include those between visible nodes
        visible_node_ids = set(nodes.keys())
        filtered_relationships = [
            rel for rel in relationships 
            if rel['startNode'] in visible_node_ids and rel['endNode'] in visible_node_ids
        ]
        
        # Enhanced statistics
        total_nodes = len(nodes)
        total_relationships = len(filtered_relationships)
        limited = total_nodes >= node_limit
        
        logger.info(f"✅ Graph extraction complete: {total_nodes} nodes, {total_relationships} relationships")
        
        return {
            'nodes': list(nodes.values()),
            'relationships': filtered_relationships,
            'total_nodes': total_nodes,
            'total_relationships': total_relationships,
            'limited': limited,
            'extraction_stats': {
                'processed_records': len(records),
                'node_limit': node_limit,
                'nodes_extracted': total_nodes,
                'relationships_extracted': total_relationships
            }
        }
        
    except Exception as e:
        logger.error(f"❌ Error in graph extraction: {e}")
        return {
            'nodes': [],
            'relationships': [],
            'total_nodes': 0,
            'total_relationships': 0,
            'limited': False,
            'error': str(e)
        }

def format_detailed_change_summary(counters, query: str, execution_time: float):
    """Enhanced change summary with more detailed information"""
    timestamp = datetime.now().isoformat()
    
    changes = []
    total_changes = 0
    
    # Detailed change tracking
    changes_map = {
        'nodes_created': ('✅', 'node(s) created'),
        'nodes_deleted': ('🗑️', 'node(s) deleted'),
        'relationships_created': ('🔗', 'relationship(s) created'),
        'relationships_deleted': ('💥', 'relationship(s) deleted'),
        'properties_set': ('📝', 'property(ies) updated'),
        'labels_added': ('🏷️', 'label(s) added'),
        'labels_removed': ('🏷️', 'label(s) removed'),
        'indexes_added': ('📊', 'index(es) created'),
        'indexes_removed': ('📊', 'index(es) removed'),
        'constraints_added': ('🔒', 'constraint(s) added'),
        'constraints_removed': ('🔒', 'constraint(s) removed')
    }
    
    for attr, (emoji, description) in changes_map.items():
        count = getattr(counters, attr, 0)
        if count > 0:
            changes.append(f"{emoji} {count} {description}")
            total_changes += count
    
    if not changes:
        changes.append("ℹ️ No changes detected (query may have been read-only or conditional)")
    
    # Performance classification
    if execution_time < 0.1:
        perf_emoji = "⚡"
        perf_desc = "Lightning fast"
    elif execution_time < 0.5:
        perf_emoji = "🚀"
        perf_desc = "Very fast"
    elif execution_time < 2.0:
        perf_emoji = "✅"
        perf_desc = "Good performance"
    else:
        perf_emoji = "⏱️"
        perf_desc = "Completed"
    
    return {
        "timestamp": timestamp,
        "execution_time_ms": round(execution_time * 1000, 2),
        "performance": {"emoji": perf_emoji, "description": perf_desc},
        "query": query,
        "changes": changes,
        "total_changes": total_changes,
        "summary": f"🕐 {timestamp[:19]} | {perf_emoji} {round(execution_time * 1000, 2)}ms | {' | '.join(changes)}",
        "detailed_counters": {
            "nodes_created": getattr(counters, 'nodes_created', 0),
            "nodes_deleted": getattr(counters, 'nodes_deleted', 0),
            "relationships_created": getattr(counters, 'relationships_created', 0),
            "relationships_deleted": getattr(counters, 'relationships_deleted', 0),
            "properties_set": getattr(counters, 'properties_set', 0),
            "labels_added": getattr(counters, 'labels_added', 0),
            "labels_removed": getattr(counters, 'labels_removed', 0),
            "indexes_added": getattr(counters, 'indexes_added', 0),
            "indexes_removed": getattr(counters, 'indexes_removed', 0),
            "constraints_added": getattr(counters, 'constraints_added', 0),
            "constraints_removed": getattr(counters, 'constraints_removed', 0)
        }
    }

@app.get("/")
async def enhanced_health_check():
    """Enhanced health check with detailed system info"""
    return {
        "status": "healthy",
        "service": "Enhanced Neo4j MCP Server", 
        "version": "2.0.0",
        "features": [
            "unlimited_graph_extraction",
            "enhanced_node_naming",
            "comprehensive_relationship_tracking",
            "detailed_change_monitoring",
            "performance_optimization"
        ],
        "limits": {
            "default_node_limit": 10000,
            "max_node_limit": 50000,
            "connection_timeout": 30,
            "max_pool_size": 100
        }
    }

@app.post("/read_neo4j_cypher")
async def enhanced_read_neo4j_cypher(request: CypherRequest):
    """Enhanced read operation with unlimited support and better graph extraction"""
    if driver is None:
        raise HTTPException(status_code=500, detail="Neo4j driver not initialized")
    
    try:
        start_time = datetime.now()
        node_limit = min(request.node_limit, 50000)  # Reasonable maximum
        
        logger.info(f"📖 Executing read query with node limit: {node_limit}")
        logger.info(f"📖 Query: {request.query[:200]}...")
        
        async with driver.session(database=NEO4J_DATABASE) as session:
            # Execute main query for data
            result = await session.execute_read(_enhanced_read, request.query, request.params or {})
            
            # Execute query for graph visualization data
            viz_result = await session.execute_read(_read_for_enhanced_viz, request.query, request.params or {})
            
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        
        # Parse the JSON result
        result_data = json.loads(result)
        
        # Extract enhanced graph data
        graph_data = None
        try:
            graph_data = extract_comprehensive_graph_data(viz_result, node_limit)
        except Exception as e:
            logger.warning(f"⚠️ Could not extract graph data: {e}")
            graph_data = {'nodes': [], 'relationships': [], 'error': str(e)}
        
        # Enhanced response with detailed metadata
        response = {
            "data": result_data,
            "metadata": {
                "timestamp": start_time.isoformat(),
                "execution_time_ms": round(execution_time * 1000, 2),
                "query": request.query,
                "record_count": len(result_data),
                "node_limit": node_limit,
                "performance": {
                    "fast": execution_time < 0.5,
                    "acceptable": execution_time < 2.0,
                    "slow": execution_time >= 2.0
                }
            },
            "graph_data": graph_data
        }
        
        logger.info(f"✅ Read query completed in {execution_time:.3f}s - {len(result_data)} records, {len(graph_data.get('nodes', []))} nodes")
        return response
        
    except Exception as e:
        logger.error(f"❌ Error in enhanced read_neo4j_cypher: {e}")
        raise HTTPException(status_code=500, detail=f"Query execution failed: {str(e)}")

@app.post("/write_neo4j_cypher")
async def enhanced_write_neo4j_cypher(request: CypherRequest):
    """Enhanced write operation with detailed change tracking and graph data"""
    if driver is None:
        raise HTTPException(status_code=500, detail="Neo4j driver not initialized")
    
    try:
        start_time = datetime.now()
        node_limit = min(request.node_limit, 50000)
        
        logger.info(f"✏️ Executing write query with node limit: {node_limit}")
        logger.info(f"✏️ Query: {request.query[:200]}...")
        
        async with driver.session(database=NEO4J_DATABASE) as session:
            result = await session.execute_write(_enhanced_write, request.query, request.params or {})
        
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        
        # Format enhanced change information
        change_info = format_detailed_change_summary(result._summary.counters, request.query, execution_time)
        
        # Enhanced logging
        logger.info(f"✅ Write operation completed: {change_info['summary']}")
        
        response = {
            "result": "SUCCESS",
            "change_info": change_info,
            "metadata": {
                "timestamp": start_time.isoformat(),
                "execution_time_ms": round(execution_time * 1000, 2),
                "total_changes": change_info["total_changes"]
            }
        }
        
        # Try to get updated graph data if the query returns data
        try:
            if "RETURN" in request.query.upper() or change_info["total_changes"] > 0:
                logger.info("🔄 Attempting to fetch updated graph data...")
                
                # Create a query to show the affected/created data
                if "CREATE" in request.query.upper() and "RETURN" in request.query.upper():
                    # Use the original query for visualization
                    viz_query = request.query
                elif "CREATE" in request.query.upper():
                    # Show recently created nodes
                    viz_query = "MATCH (n) WHERE n.created IS NOT NULL OR timestamp() - coalesce(n.created_timestamp, 0) < 60000 OPTIONAL MATCH (n)-[r]-(m) RETURN n, r, m"
                else:
                    # General query to show current state
                    viz_query = "MATCH (n) OPTIONAL MATCH (n)-[r]-(m) RETURN n, r, m LIMIT 100"
                
                async with driver.session(database=NEO4J_DATABASE) as session:
                    viz_result = await session.execute_read(_read_for_enhanced_viz, viz_query, {})
                
                graph_data = extract_comprehensive_graph_data(viz_result, node_limit)
                response["graph_data"] = graph_data
                
                logger.info(f"✅ Graph data updated: {len(graph_data.get('nodes', []))} nodes, {len(graph_data.get('relationships', []))} relationships")
                
        except Exception as e:
            logger.warning(f"⚠️ Could not fetch updated graph data: {e}")
            response["graph_data"] = None
        
        return response
        
    except Exception as e:
        logger.error(f"❌ Error in enhanced write_neo4j_cypher: {e}")
        raise HTTPException(status_code=500, detail=f"Write operation failed: {str(e)}")

@app.post("/get_neo4j_schema")
async def enhanced_get_neo4j_schema():
    """Enhanced schema retrieval with comprehensive relationship information"""
    if driver is None:
        raise HTTPException(status_code=500, detail="Neo4j driver not initialized")
    
    try:
        start_time = datetime.now()
        
        logger.info("🏗️ Fetching comprehensive Neo4j schema with relationships...")
        
        async with driver.session(database=NEO4J_DATABASE) as session:
            schema = {}
            raw_components = {}
            
            try:
                # Try APOC meta schema first (most comprehensive)
                logger.info("🔍 Attempting APOC meta.schema...")
                apoc_result = await session.execute_read(_read, "CALL apoc.meta.schema() YIELD value RETURN value", {})
                apoc_data = json.loads(apoc_result)
                
                if apoc_data and apoc_data[0].get('value'):
                    schema = apoc_data[0]['value']
                    source = "apoc_meta_schema"
                    logger.info("✅ APOC schema retrieved successfully")
                else:
                    raise Exception("APOC returned empty result")
                    
            except Exception as apoc_error:
                logger.warning(f"⚠️ APOC not available: {apoc_error}")
                logger.info("🔄 Building schema from direct queries...")
                
                # Get all basic components
                try:
                    # Get node labels
                    labels_result = await session.execute_read(_read, "CALL db.labels() YIELD label RETURN collect(label) as labels", {})
                    labels_data = json.loads(labels_result)
                    labels = labels_data[0].get("labels", []) if labels_data else []
                    raw_components["labels"] = [{"labels": labels}]
                    
                    # Get relationship types
                    rels_result = await session.execute_read(_read, "CALL db.relationshipTypes() YIELD relationshipType RETURN collect(relationshipType) as types", {})
                    rels_data = json.loads(rels_result)
                    rel_types = rels_data[0].get("types", []) if rels_data else []
                    raw_components["relationship_types"] = [{"types": rel_types}]
                    
                    # Get property keys
                    props_result = await session.execute_read(_read, "CALL db.propertyKeys() YIELD propertyKey RETURN collect(propertyKey) as keys", {})
                    props_data = json.loads(props_result)
                    prop_keys = props_data[0].get("keys", []) if props_data else []
                    raw_components["property_keys"] = [{"keys": prop_keys}]
                    
                    logger.info(f"📊 Found {len(labels)} labels, {len(rel_types)} relationship types")
                    
                except Exception as basic_error:
                    logger.error(f"❌ Basic schema queries failed: {basic_error}")
                    labels, rel_types, prop_keys = [], [], []
                
                # Build enhanced schema with relationship patterns
                schema = {}
                
                for label in labels:
                    try:
                        # Get sample properties for each label
                        prop_query = f"""
                        MATCH (n:{label}) 
                        WITH keys(n) as props 
                        UNWIND props as prop
                        RETURN DISTINCT prop
                        LIMIT 20
                        """
                        prop_result = await session.execute_read(_read, prop_query, {})
                        prop_data = json.loads(prop_result)
                        
                        properties = {}
                        if prop_data:
                            for record in prop_data:
                                prop_name = record.get("prop")
                                if prop_name:
                                    properties[prop_name] = {"type": "string"}  # Default type
                        
                        # Get outgoing relationships for this label
                        outgoing_query = f"""
                        MATCH (n:{label})-[r]->(m)
                        RETURN DISTINCT type(r) as rel_type, labels(m) as target_labels
                        LIMIT 50
                        """
                        out_result = await session.execute_read(_read, outgoing_query, {})
                        out_data = json.loads(out_result)
                        
                        relationships = []
                        if out_data:
                            for record in out_data:
                                rel_type = record.get("rel_type")
                                target_labels = record.get("target_labels", [])
                                if rel_type and target_labels:
                                    relationships.append({
                                        "type": rel_type,
                                        "direction": "outgoing",
                                        "target": target_labels[0] if target_labels else "Unknown"
                                    })
                        
                        # Get incoming relationships for this label
                        incoming_query = f"""
                        MATCH (m)-[r]->(n:{label})
                        RETURN DISTINCT type(r) as rel_type, labels(m) as source_labels
                        LIMIT 50
                        """
                        in_result = await session.execute_read(_read, incoming_query, {})
                        in_data = json.loads(in_result)
                        
                        if in_data:
                            for record in in_data:
                                rel_type = record.get("rel_type")
                                source_labels = record.get("source_labels", [])
                                if rel_type and source_labels:
                                    relationships.append({
                                        "type": rel_type,
                                        "direction": "incoming", 
                                        "source": source_labels[0] if source_labels else "Unknown"
                                    })
                        
                        schema[label] = {
                            "type": "node",
                            "properties": properties,
                            "relationships": relationships
                        }
                        
                        logger.info(f"📋 {label}: {len(properties)} properties, {len(relationships)} relationships")
                        
                    except Exception as label_error:
                        logger.warning(f"⚠️ Could not analyze label {label}: {label_error}")
                        schema[label] = {
                            "type": "node", 
                            "properties": {}, 
                            "relationships": []
                        }
                
                source = "enhanced_direct_queries"
            
            # Get additional relationship statistics
            try:
                logger.info("📊 Gathering relationship statistics...")
                
                rel_stats_query = """
                MATCH ()-[r]->()
                RETURN type(r) as rel_type, count(*) as count
                ORDER BY count DESC
                """
                rel_stats_result = await session.execute_read(_read, rel_stats_query, {})
                rel_stats_data = json.loads(rel_stats_result)
                raw_components["relationship_statistics"] = rel_stats_data
                
            except Exception as stats_error:
                logger.warning(f"⚠️ Could not get relationship statistics: {stats_error}")
        
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        
        # Enhanced schema statistics
        stats = {
            "node_labels": len(schema) if isinstance(schema, dict) else 0,
            "relationship_types": len(raw_components.get("relationship_types", [{}])[0].get("types", [])),
            "property_keys": len(raw_components.get("property_keys", [{}])[0].get("keys", [])),
            "total_relationships": sum([len(info.get("relationships", [])) for info in schema.values()]) if isinstance(schema, dict) else 0
        }
        
        logger.info(f"✅ Enhanced schema retrieval completed in {execution_time:.3f}s")
        logger.info(f"   📊 {stats['node_labels']} node labels")
        logger.info(f"   🔗 {stats['relationship_types']} relationship types") 
        logger.info(f"   📋 {stats['property_keys']} property keys")
        logger.info(f"   🔀 {stats['total_relationships']} relationship patterns")
        
        return {
            "schema": schema,
            "metadata": {
                "timestamp": start_time.isoformat(),
                "execution_time_ms": round(execution_time * 1000, 2),
                "source": source,
                "statistics": stats
            },
            "raw_components": raw_components
        }
        
    except Exception as e:
        logger.error(f"❌ Error in enhanced schema retrieval: {e}")
        raise HTTPException(status_code=500, detail=f"Schema retrieval failed: {str(e)}")

@app.get("/unlimited_sample_graph")
async def get_unlimited_sample_graph(node_limit: int = 1000):
    """Get comprehensive sample of the graph without artificial restrictions"""
    if driver is None:
        raise HTTPException(status_code=500, detail="Neo4j driver not initialized")
    
    try:
        capped_limit = min(node_limit, 50000)  # Reasonable maximum
        
        # Enhanced sampling query that gets diverse data
        query = f"""
        MATCH (n)
        OPTIONAL MATCH (n)-[r]-(m)
        RETURN n, r, m
        LIMIT {capped_limit}
        """
        
        logger.info(f"🎲 Generating unlimited sample with limit: {capped_limit}")
        
        async with driver.session(database=NEO4J_DATABASE) as session:
            result = await session.execute_read(_read_for_enhanced_viz, query, {})
        
        graph_data = extract_comprehensive_graph_data(result, capped_limit)
        
        return {
            "graph_data": graph_data,
            "query": query,
            "timestamp": datetime.now().isoformat(),
            "parameters": {
                "requested_limit": node_limit,
                "applied_limit": capped_limit,
                "unlimited_mode": True
            }
        }
        
    except Exception as e:
        logger.error(f"❌ Error getting unlimited sample graph: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/graph_stats")
async def get_comprehensive_graph_stats():
    """Get detailed graph statistics for analysis"""
    if driver is None:
        raise HTTPException(status_code=500, detail="Neo4j driver not initialized")
    
    try:
        logger.info("📊 Collecting comprehensive graph statistics...")
        
        stats_queries = [
            ("total_nodes", "MATCH (n) RETURN count(n) as count"),
            ("total_relationships", "MATCH ()-[r]->() RETURN count(r) as count"),
            ("node_labels", "CALL db.labels() YIELD label RETURN collect(label) as labels"),
            ("relationship_types", "CALL db.relationshipTypes() YIELD relationshipType RETURN collect(relationshipType) as types"),
            ("node_label_distribution", "MATCH (n) RETURN labels(n)[0] as label, count(*) as count ORDER BY count DESC LIMIT 20"),
            ("relationship_type_distribution", "MATCH ()-[r]->() RETURN type(r) as type, count(*) as count ORDER BY count DESC LIMIT 20"),
            ("degree_distribution", "MATCH (n) RETURN size((n)--()) as degree, count(*) as nodes ORDER BY degree DESC LIMIT 10"),
            ("property_keys", "CALL db.propertyKeys() YIELD propertyKey RETURN collect(propertyKey) as keys")
        ]
        
        stats = {}
        
        async with driver.session(database=NEO4J_DATABASE) as session:
            for stat_name, query in stats_queries:
                try:
                    result = await session.execute_read(_read, query, {})
                    data = json.loads(result)
                    stats[stat_name] = data
                except Exception as e:
                    logger.warning(f"⚠️ Could not get {stat_name}: {e}")
                    stats[stat_name] = []
        
        # Calculate derived statistics
        total_nodes = stats.get("total_nodes", [{}])[0].get("count", 0)
        total_relationships = stats.get("total_relationships", [{}])[0].get("count", 0)
        
        derived_stats = {
            "density": total_relationships / max(total_nodes, 1),
            "avg_degree": (total_relationships * 2) / max(total_nodes, 1),
            "node_types": len(stats.get("node_labels", [{}])[0].get("labels", [])),
            "relationship_types": len(stats.get("relationship_types", [{}])[0].get("types", [])),
            "complexity_score": (total_nodes + total_relationships) / 1000  # Simple complexity metric
        }
        
        logger.info(f"✅ Comprehensive stats collected: {total_nodes} nodes, {total_relationships} relationships")
        
        return {
            "stats": stats,
            "derived_stats": derived_stats,
            "summary": {
                "total_nodes": total_nodes,
                "total_relationships": total_relationships,
                "density": derived_stats["density"],
                "complexity": "high" if derived_stats["complexity_score"] > 10 else "medium" if derived_stats["complexity_score"] > 1 else "low"
            },
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"❌ Error getting comprehensive graph stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Add a simple fallback endpoint for basic stats
@app.get("/simple_stats")
async def get_simple_stats():
    """Get basic graph statistics (fallback)"""
    if driver is None:
        return {"error": "Neo4j driver not initialized", "total_nodes": 0, "total_relationships": 0}
    
    try:
        async with driver.session(database=NEO4J_DATABASE) as session:
            # Simple node count
            nodes_result = await session.execute_read(_read, "MATCH (n) RETURN count(n) as count", {})
            nodes_data = json.loads(nodes_result)
            total_nodes = nodes_data[0].get("count", 0) if nodes_data else 0
            
            # Simple relationship count  
            rels_result = await session.execute_read(_read, "MATCH ()-[r]->() RETURN count(r) as count", {})
            rels_data = json.loads(rels_result)
            total_relationships = rels_data[0].get("count", 0) if rels_data else 0
            
        return {
            "total_nodes": total_nodes,
            "total_relationships": total_relationships,
            "density": total_relationships / max(total_nodes, 1),
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"❌ Error getting simple stats: {e}")
        return {"error": str(e), "total_nodes": 0, "total_relationships": 0}

# Enhanced transaction functions
async def _enhanced_read(tx: AsyncTransaction, query: str, params: dict):
    """Enhanced read transaction with better error handling"""
    res = await tx.run(query, params)
    records = await res.to_eager_result()
    return json.dumps([r.data() for r in records.records], default=str)

async def _read_for_enhanced_viz(tx: AsyncTransaction, query: str, params: dict):
    """Enhanced read transaction specifically for graph visualization"""
    res = await tx.run(query, params)
    records = await res.to_eager_result()
    return records.records  # Return raw records for graph extraction

async def _enhanced_write(tx: AsyncTransaction, query: str, params: dict):
    """Enhanced write transaction with detailed result tracking"""
    return await tx.run(query, params)

async def _read(tx: AsyncTransaction, query: str, params: dict):
    """Standard read transaction for backward compatibility"""
    res = await tx.run(query, params)
    records = await res.to_eager_result()
    return json.dumps([r.data() for r in records.records], default=str)

if __name__ == "__main__":
    import uvicorn
    
    logger.info("🚀 Starting Enhanced Neo4j MCP Server...")
    logger.info("✨ Features: Unlimited graph support, enhanced extraction, comprehensive stats")
    
    uvicorn.run(
        "mcpserver:app", 
        host="0.0.0.0", 
        port=8000, 
        reload=True,
        log_level="info"
    )
