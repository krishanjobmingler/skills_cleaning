#!/usr/bin/env python3

import sys
import json
import asyncio
from datetime import datetime, timezone
from typing import List
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel, Field
from langchain_ollama import ChatOllama
from langchain.agents import create_agent
from langchain.agents.structured_output import ToolStrategy
from google.oauth2 import service_account
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_nvidia_ai_endpoints import ChatNVIDIA

credentials = service_account.Credentials.from_service_account_file(
    "cred.json",
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

PROD_URI = "mongodb+srv://resume_builder:itOceb9dM0wKN3uE@jobminglr-db.6jrmu.mongodb.net/?retryWrites=true&w=majority"
PROD_DB = "jobminglr"
LOCAL_URI = "mongodb://localhost:27017"
LOCAL_DB = "jobminglr"
LOCAL_COLLECTION = "process_skills"
BATCH_SIZE = 500
LLM_CONCURRENCY = 500
LOG_FILE = "/home/krishan/Fee_projects/Job_mingler/code/scripts/cleanup_skills_log.txt"
FAILED_LOG_FILE = "failed_skills_log.txt"
BATCH_CONCURRENCY = 100


def log(msg: str):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {msg}"
    print(line)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")


def log_failed(skill_name: str, error: str, raw_response: str = ""):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(FAILED_LOG_FILE, "a") as f:
        f.write(f"\n{'='*60}\n")
        f.write(f"[{timestamp}] FAILED: {skill_name}\n")
        f.write(f"ERROR: {error}\n")
        if raw_response:
            f.write(f"RAW RESPONSE:\n{raw_response}\n")
        f.write(f"{'='*60}\n")


class SkillCategory(str):
    TECHNICAL = "TECHNICAL"
    NON_TECHNICAL = "NON_TECHNICAL"


class SkillAnalysis(BaseModel):
    is_skill: bool = Field(...)
    is_job_role: bool = Field(...)
    need_name_change: bool = Field(...)
    updated_name: str = Field(default="")
    category: str = Field(...)
    similar_tools_and_skills: List[str] = Field(default_factory=list)
    aliases: List[str] = Field(default_factory=list)


llm = ChatGoogleGenerativeAI(
    model="gemini-2.5-flash",
    temperature=0.0,
    max_tokens=2000,
    credentials=credentials, 
    project="jobminglr-d8b0a", 
)
structured_llm = llm.with_structured_output(SkillAnalysis)

SYSTEM_PROMPT = """You are a skill analyzer. Analyze the given skill name and return structured analysis.

RULES:
1. is_skill: true if it is a learnable professional skill or tool. false for random words, non-English, locations.
2. is_job_role: true if it is a job title like Software Engineer, Data Analyst. false for skills/tools.
3. need_name_change: true if name has typos, redundant company names, or wrong format.
4. updated_name: corrected name if need_name_change is true. Keep it simple and clean.
5. category: TECHNICAL for programming, software, IT, engineering. NON_TECHNICAL for soft skills, business, management.
6. similar_tools_and_skills: list 3-5 alternative or competing tools/skills.
7. aliases: list 2-4 different ways to write or abbreviate this skill name.

NAME CORRECTION EXAMPLES:
- Adobe Systems Adobe Acrobat -> Adobe Acrobat
- Blackbaud The Raiser's Edge -> Raiser's Edge  
- Atlassian JIRA -> Jira
- Cerner ProFile -> Cerner ProFile (no change if already correct)
- ColorSoft AutoMatch -> ColorSoft AutoMatch (no change if already correct)

SIMILAR TOOLS EXAMPLES:
- Jira: Asana, Trello, Monday.com, ClickUp
- Python: R, Julia, Java, Go
- Excel: Google Sheets, LibreOffice Calc, Numbers

ALIASES EXAMPLES:
- Microsoft Excel: Excel, MS Excel, Excel 365
- JavaScript: JS, ECMAScript
- Kubernetes: K8s, Kube

IMPORTANT:
1. NEVER return empty arrays for similar_tools_and_skills or aliases
2. updated_name must be CLEAN text only - no JSON syntax
3. Use your extensive knowledge to generate relevant similar tools and aliases
4. When in doubt about is_skill, return TRUE
5. Job roles ARE skills but mark is_job_role=true"""


def clean_string(value: str) -> str:
    if not value:
        return ""
    cleaned = value.strip()
    cleaned = cleaned.replace('\u2028', '').replace('\u2029', '')
    cleaned = cleaned.rstrip('",\'')
    cleaned = cleaned.lstrip('",\'')
    cleaned = ' '.join(cleaned.split())
    return cleaned


async def analyze_skill(skill_data: dict) -> dict:
    skill_name = skill_data.get("name", "")
    raw_response = ""
    
    try:
        prompt = f"{SYSTEM_PROMPT}\n\nAnalyze this skill: {skill_name}"
        result = await structured_llm.ainvoke(prompt)
        raw_response = str(result)
        print(result)
        cleaned_updated_name = clean_string(result.updated_name)
        
        return {
            "is_skill": result.is_skill,
            "is_job_role": result.is_job_role,
            "need_name_change": result.need_name_change,
            "updated_name": cleaned_updated_name,
            "category": result.category,
            "similar_tools_and_skills": result.similar_tools_and_skills,
            "aliases": result.aliases
        }
    except Exception as e:
        log(f"   Failed for '{skill_name}': {e} - SKIPPING")
        log_failed(skill_name, str(e), raw_response)
        return None


async def process_batch(skills_batch: List[dict]) -> tuple:
    tasks = [analyze_skill(s) for s in skills_batch]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    to_delete = []
    to_update = []
    
    for skill, analysis in zip(skills_batch, results):
        if isinstance(analysis, Exception):
            log(f"   Error processing '{skill['name']}': {analysis}")
            continue
        
        skill_name = skill.get("name", "")
        is_valid = analysis.get("is_skill", True)
        category = analysis.get("category", "NON_TECHNICAL")
        
        if not is_valid:
            to_delete.append(skill)
            log(f"   NOISE: '{skill_name}'")
        else:
            update_data = {
                "_id": skill["_id"],
                "name": skill_name,
                "is_skill": is_valid,
                "category": category
            }
            to_update.append(update_data)
            log(f"   VALID: '{skill_name}' | Category: {category}")
    
    return to_delete, to_update


async def process_and_save_skill(skill: dict, local_col) -> dict:
    skill_name = skill.get("name", "")
    
    log(f"\nProcessing: '{skill_name}'")
    
    analysis = await analyze_skill(skill)
    
    if analysis is None:
        return {"status": "skipped", "name": skill_name, "reason": "parse_failed"}
    
    is_valid = analysis.get("is_skill", True)
    is_job_role = analysis.get("is_job_role", False)
    need_name_change = analysis.get("need_name_change", False)
    updated_name = analysis.get("updated_name", "")
    category = analysis.get("category", "NON_TECHNICAL")
    similar_tools_and_skills = analysis.get("similar_tools_and_skills", [])
    aliases = analysis.get("aliases", [])
    
    local_doc = {
        "originalId": str(skill["_id"]),
        "name": skill_name,
        "needNameChange": need_name_change,
        "updatedName": updated_name if need_name_change else "",
        "isSkill": is_valid,
        "isJobRole": is_job_role,
        "category": category,
        "similarToolsAndSkills": similar_tools_and_skills,
        "aliases": aliases,
        "action": "DELETE" if not is_valid else "UPDATE",
        "processedAt": datetime.now(timezone.utc)
    }
    
    log(f"   IS SKILL: {is_valid}")
    if is_valid:
        log(f"   IS JOB ROLE: {is_job_role}")
        log(f"   NEED NAME CHANGE: {need_name_change}")
        if need_name_change:
            log(f"   UPDATED NAME: '{updated_name}'")
        log(f"   Category: {category}")
        log(f"   Similar Tools & Skills: {similar_tools_and_skills}")
        log(f"   Aliases: {aliases}")
    else:
        log(f"   NOT A VALID SKILL - marked for deletion")
    
    await local_col.update_one(
        {"originalId": str(skill["_id"])},
        {"$set": local_doc},
        upsert=True
    )
    log(f"   Saved to local DB: {LOCAL_DB}.{LOCAL_COLLECTION}")
    
    return {
        "name": skill_name,
        "is_skill": is_valid,
        "is_job_role": is_job_role,
        "need_name_change": need_name_change,
        "updated_name": updated_name,
        "category": category,
        "similar_tools_and_skills": similar_tools_and_skills,
        "aliases": aliases,
        "action": local_doc["action"]
    }


async def cleanup_skills(count: str = "5", dry_run: bool = True):
    start_time = datetime.now()
    
    process_all = count.lower() == "all"
    limit = 0 if process_all else int(count)
    
    log("=" * 60)
    log(f"SKILL ANALYZER - BATCH MODE (100 concurrent)")
    log(f"Start: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    log(f"Count: {'ALL' if process_all else limit}")
    log(f"Local DB: {LOCAL_URI}/{LOCAL_DB}.{LOCAL_COLLECTION}")
    log("=" * 60)
    
    prod_client = AsyncIOMotorClient(PROD_URI)
    prod_db = prod_client[PROD_DB]
    
    collections = await prod_db.list_collection_names()
    log(f"Available collections: {collections}")
    
    skills_col = prod_db["skills"]
    
    local_client = AsyncIOMotorClient(LOCAL_URI)
    local_db = local_client[LOCAL_DB]
    local_col = local_db[LOCAL_COLLECTION]
    
    total_in_db = await skills_col.count_documents({})
    log(f"Total skills in production DB: {total_in_db}")
    
    processed_cursor = local_col.find({}, {"originalId": 1})
    processed_docs = await processed_cursor.to_list(length=None)
    already_processed_ids = {doc["originalId"] for doc in processed_docs if "originalId" in doc}
    log(f"Already processed in local DB: {len(already_processed_ids)}")
    
    cursor = skills_col.find({}, {"_id": 1, "name": 1}).sort("createdAt", -1)
    
    if not process_all and limit > 0:
        cursor = cursor.limit(limit)
    
    all_skills = await cursor.to_list(length=None if process_all else limit)
    
    skills_list = [s for s in all_skills if str(s["_id"]) not in already_processed_ids]
    skipped_count = len(all_skills) - len(skills_list)
    total_skills = len(skills_list)
    
    log(f"Skills fetched from production: {len(all_skills)}")
    log(f"Skipped (already processed): {skipped_count}")
    log(f"Skills to process: {total_skills}")
    
    total_valid = 0
    total_invalid = 0
    technical_count = 0
    non_technical_count = 0
    name_change_count = 0
    job_role_count = 0
    processed_count = 0
    
    for batch_start in range(0, total_skills, BATCH_CONCURRENCY):
        batch_end = min(batch_start + BATCH_CONCURRENCY, total_skills)
        batch = skills_list[batch_start:batch_end]
        batch_size = len(batch)
        
        log(f"\n{'='*60}")
        log(f"Processing batch [{batch_start + 1}-{batch_end}] of {total_skills} ({(batch_end/total_skills)*100:.1f}%)")
        log(f"Running {batch_size} requests concurrently...")
        
        tasks = [process_and_save_skill(skill, local_col) for skill in batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        skipped_in_batch = 0
        for skill, result in zip(batch, results):
            if isinstance(result, Exception):
                log(f"   Error processing '{skill.get('name', '')}': {result}")
                continue
            
            if result.get("status") == "skipped":
                skipped_in_batch += 1
                continue
            
            processed_count += 1
            if result["is_skill"]:
                total_valid += 1
                if result["category"] == "TECHNICAL":
                    technical_count += 1
                else:
                    non_technical_count += 1
                if result["need_name_change"]:
                    name_change_count += 1
                if result["is_job_role"]:
                    job_role_count += 1
            else:
                total_invalid += 1
        
        if skipped_in_batch > 0:
            log(f"Skipped in batch (parse failed): {skipped_in_batch}")
        
        log(f"Batch complete: {batch_size} processed")
        await asyncio.sleep(0.5)
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    log("\n" + "=" * 60)
    log("SUMMARY")
    log("=" * 60)
    log(f"Skills processed: {total_skills}")
    log(f"Valid skills: {total_valid}")
    log(f"Invalid skills: {total_invalid}")
    log(f"Job roles: {job_role_count}")
    log(f"Need name change: {name_change_count}")
    log(f"TECHNICAL: {technical_count}")
    log(f"NON_TECHNICAL: {non_technical_count}")
    log(f"Saved to: {LOCAL_URI}/{LOCAL_DB}.{LOCAL_COLLECTION}")
    log(f"Duration: {duration:.2f}s")
    if total_skills > 0:
        log(f"Avg per skill: {duration/total_skills:.2f}s")
    log("=" * 60)
    
    prod_client.close()
    local_client.close()


if __name__ == "__main__":
    count = sys.argv[1] if len(sys.argv) > 1 else "all"
    
    print(f"Skill Analyzer")
    print(f"   Count: {count.upper() if count.lower() == 'all' else count}")
    print(f"   Source: Production DB (jobminglr.skills)")
    print(f"   Target: Local DB (localhost:27017/jobminglr.process_skills)")
    print()
    
    asyncio.run(cleanup_skills(count=count))
