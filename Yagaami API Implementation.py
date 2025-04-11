from fastapi import FastAPI, Query, HTTPException, Depends
from typing import Optional, List, Dict, Any
import elasticsearch
from elasticsearch_dsl import Search, Q
from pydantic import BaseModel
import redis
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv
import logging
from datetime import datetime
import ml_salary_model  # Custom module for salary prediction

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(title="Job Aggregator API", version="1.0.0")

# Database connections
def get_db_connection():
    """Create and return a database connection"""
    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST"),
            database=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            cursor_factory=RealDictCursor
        )
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise HTTPException(status_code=500, detail="Database connection error")

# Redis connection for caching
redis_client = redis.Redis(
    host=os.getenv("REDIS_HOST"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    db=0,
    decode_responses=True
)

# Elasticsearch connection
es_client = elasticsearch.Elasticsearch(
    [os.getenv("ELASTICSEARCH_HOST")],
    http_auth=(os.getenv("ELASTICSEARCH_USER"), os.getenv("ELASTICSEARCH_PASSWORD"))
)

# Models
class JobSearchResult(BaseModel):
    job_id: str
    title: str
    company: str
    location: str
    remote: bool
    job_type: str
    summary: str
    url: str
    posted_date: str
    salary_range: Optional[Dict[str, float]] = None
    experience_level: Optional[str] = None
    source: str

class JobSearchResponse(BaseModel):
    count: int
    page: int
    limit: int
    results: List[JobSearchResult]

class JobDetail(BaseModel):
    job_id: str
    title: str
    company: str
    location: str
    remote: bool
    job_type: str
    description: str
    qualifications: List[str]
    responsibilities: List[str]
    benefits: Optional[List[str]] = None
    salary_range: Optional[Dict[str, float]] = None
    experience_level: Optional[str] = None
    education_requirements: Optional[List[str]] = None
    skills: Optional[List[str]] = None
    company_info: Optional[Dict[str, Any]] = None
    application_url: str
    posted_date: str
    expiry_date: Optional[str] = None
    source: str
    source_url: str
    contact_info: Optional[Dict[str, str]] = None
    metadata: Optional[Dict[str, Any]] = None

class SalaryEstimate(BaseModel):
    job_title: str
    location: str
    estimated_salary_range: Dict[str, float]
    confidence: float
    similar_roles: List[Dict[str, Any]]
    market_data: Dict[str, Any]

# Rate limiting middleware
async def rate_limiter(request):
    """Basic rate limiting based on client IP"""
    client_ip = request.client.host
    key = f"rate_limit:{client_ip}"
    
    current = redis_client.get(key)
    if current and int(current) > 100:  # 100 requests per minute
        raise HTTPException(status_code=429, detail="Rate limit exceeded")
    
    pipe = redis_client.pipeline()
    pipe.incr(key)
    pipe.expire(key, 60)  # 1 minute expiry
    pipe.execute()

# Caching middleware
def get_cache(key):
    """Get cached response"""
    cached = redis_client.get(key)
    if cached:
        return eval(cached)
    return None

def set_cache(key, value, expiry=300):
    """Cache response with expiry in seconds"""
    redis_client.setex(key, expiry, str(value))

# Endpoints
@app.get("/search", response_model=JobSearchResponse, dependencies=[Depends(rate_limiter)])
async def search_jobs(
    query: str = Query(None, description="Search keywords"),
    location: str = Query(None, description="Location filter"),
    remote_only: bool = Query(False, description="Filter for remote jobs only"),
    job_type: str = Query(None, description="Filter by job type (full-time, part-time, contract)"),
    experience_level: str = Query(None, description="Filter by experience level"),
    page: int = Query(1, description="Page number", ge=1),
    limit: int = Query(20, description="Results per page", ge=1, le=100)
):
    """Search for jobs based on various criteria"""
    # Generate cache key
    cache_key = f"search:{query}:{location}:{remote_only}:{job_type}:{experience_level}:{page}:{limit}"
    cached_result = get_cache(cache_key)
    if cached_result:
        return cached_result
    
    try:
        # Build search query for Elasticsearch
        s = Search(using=es_client, index="jobs")
        
        # Add query filters
        if query:
            s = s.query("multi_match", query=query, fields=["title^3", "company^2", "description"])
        
        if location:
            s = s.filter("match", location=location)
            
        if remote_only:
            s = s.filter("term", remote=True)
            
        if job_type:
            s = s.filter("match", job_type=job_type)
            
        if experience_level:
            s = s.filter("match", experience_level=experience_level)
        
        # Add pagination
        s = s[(page-1)*limit:page*limit]
        
        # Execute search
        response = s.execute()
        
        # Format results
        results = []
        for hit in response:
            results.append(JobSearchResult(
                job_id=hit.meta.id,
                title=hit.title,
                company=hit.company,
                location=hit.location,
                remote=hit.remote,
                job_type=hit.job_type,
                summary=hit.summary,
                url=hit.url,
                posted_date=hit.posted_date,
                salary_range=hit.salary_range if hasattr(hit, 'salary_range') else None,
                experience_level=hit.experience_level if hasattr(hit, 'experience_level') else None,
                source=hit.source
            ))
        
        # Create response
        search_response = JobSearchResponse(
            count=response.hits.total.value,
            page=page,
            limit=limit,
            results=results
        )
        
        # Cache the result
        set_cache(cache_key, search_response.dict())
        
        return search_response
    
    except Exception as e:
        logger.error(f"Search error: {e}")
        raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")

@app.get("/job-details/{job_id}", response_model=JobDetail, dependencies=[Depends(rate_limiter)])
async def get_job_details(job_id: str):
    """Get detailed information about a specific job"""
    # Check cache first
    cache_key = f"job:{job_id}"
    cached_result = get_cache(cache_key)
    if cached_result:
        return cached_result
    
    try:
        # First try Elasticsearch for quick retrieval
        try:
            job = es_client.get(index="jobs", id=job_id)
            job_source = job["_source"]
        except elasticsearch.NotFoundError:
            # Fall back to PostgreSQL for archived jobs
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM jobs WHERE job_id = %s", (job_id,))
            job_source = cursor.fetchone()
            if not job_source:
                raise HTTPException(status_code=404, detail="Job not found")
            conn.close()
        
        # Create job detail object
        job_detail = JobDetail(
            job_id=job_id,
            title=job_source["title"],
            company=job_source["company"],
            location=job_source["location"],
            remote=job_source["remote"],
            job_type=job_source["job_type"],
            description=job_source["description"],
            qualifications=job_source["qualifications"],
            responsibilities=job_source["responsibilities"],
            benefits=job_source.get("benefits"),
            salary_range=job_source.get("salary_range"),
            experience_level=job_source.get("experience_level"),
            education_requirements=job_source.get("education_requirements"),
            skills=job_source.get("skills"),
            company_info=job_source.get("company_info"),
            application_url=job_source["application_url"],
            posted_date=job_source["posted_date"],
            expiry_date=job_source.get("expiry_date"),
            source=job_source["source"],
            source_url=job_source["source_url"],
            contact_info=job_source.get("contact_info"),
            metadata=job_source.get("metadata")
        )
        
        # Cache the result
        set_cache(cache_key, job_detail.dict(), 3600)  # Cache for 1 hour
        
        return job_detail
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Job details error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get job details: {str(e)}")

@app.get("/salary-estimate", response_model=SalaryEstimate, dependencies=[Depends(rate_limiter)])
async def estimate_salary(
    job_title: str = Query(..., description="Job title"),
    location: str = Query(..., description="Job location")
):
    """Estimate salary based on job title and location"""
    # Generate cache key
    cache_key = f"salary:{job_title}:{location}"
    cached_result = get_cache(cache_key)
    if cached_result:
        return cached_result
    
    try:
        # Use internal salary prediction model
        salary_data = ml_salary_model.predict(job_title, location)
        
        # Create response
        estimate = SalaryEstimate(
            job_title=job_title,
            location=location,
            estimated_salary_range={
                "min": salary_data["min_salary"],
                "median": salary_data["median_salary"],
                "max": salary_data["max_salary"]
            },
            confidence=salary_data["confidence"],
            similar_roles=salary_data["similar_roles"],
            market_data=salary_data["market_data"]
        )
        
        # Cache the result
        set_cache(cache_key, estimate.dict(), 86400)  # Cache for 24 hours
        
        return estimate
    
    except Exception as e:
        logger.error(f"Salary estimation error: {e}")
        raise HTTPException(status_code=500, detail=f"Salary estimation failed: {str(e)}")

# Health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    health = {
        "status": "ok",
        "timestamp": datetime.now().isoformat(),
        "version": app.version
    }
    
    # Check database connection
    try:
        conn = get_db_connection()
        conn.close()
        health["database"] = "connected"
    except Exception:
        health["database"] = "disconnected"
        health["status"] = "degraded"
    
    # Check Elasticsearch connection
    try:
        es_status = es_client.ping()
        health["elasticsearch"] = "connected" if es_status else "disconnected"
        if not es_status:
            health["status"] = "degraded"
    except Exception:
        health["elasticsearch"] = "disconnected"
        health["status"] = "degraded"
    
    # Check Redis connection
    try:
        redis_status = redis_client.ping()
        health["redis"] = "connected" if redis_status else "disconnected"
        if not redis_status:
            health["status"] = "degraded"
    except Exception:
        health["redis"] = "disconnected"
        health["status"] = "degraded"
    
    return health

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
