import asyncio
import sys
from datetime import datetime, timezone
from motor.motor_asyncio import AsyncIOMotorClient
from bson import ObjectId

PROD_URI = "mongodb+srv://resume_builder:itOceb9dM0wKN3uE@jobminglr-db.6jrmu.mongodb.net/?retryWrites=true&w=majority"
PROD_DB = "jobminglr"
PROD_COLLECTION = "skills"

LOCAL_URI = "mongodb://localhost:27017"
LOCAL_DB = "jobminglr"
LOCAL_COLLECTION = "process_skills"

LOG_FILE = "/home/krishan/Fee_projects/Job_mingler/code/clean_skills/sync_skills_log.txt"


def log(msg: str):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {msg}"
    print(line)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")


async def sync_skills(dry_run: bool = False, action_filter: str = "all"):
    start_time = datetime.now()
    
    log("=" * 60)
    log("SYNC SKILLS TO LIVE DB (FAST MODE)")
    log(f"Start: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    log(f"Mode: {'DRY RUN' if dry_run else 'LIVE'}")
    log(f"Action Filter: {action_filter.upper()}")
    log("=" * 60)
    
    local_client = AsyncIOMotorClient(LOCAL_URI)
    local_db = local_client[LOCAL_DB]
    local_col = local_db[LOCAL_COLLECTION]
    
    prod_client = AsyncIOMotorClient(PROD_URI)
    prod_db = prod_client[PROD_DB]
    prod_col = prod_db[PROD_COLLECTION]
    
    log("Fetching processed skills from local DB...")
    cursor = local_col.find({})
    processed_docs = await cursor.to_list(length=None)
    log(f"Total processed skills: {len(processed_docs)}")
    
    log("Fetching existing skills from production...")
    prod_cursor = prod_col.find({}, {"name": 1})
    prod_docs = await prod_cursor.to_list(length=None)
    existing_skill_names = {doc["name"].lower().strip() for doc in prod_docs if doc.get("name")}
    log(f"Existing skills in production: {len(existing_skill_names)}")
    
    log("\nPreparing operations...")
    
    delete_ops = []
    update_ops = []
    create_docs = []
    newly_created_names = set()
    
    for doc in processed_docs:
        original_id = doc.get("originalId", "")
        name = doc.get("name", "")
        is_skill = doc.get("isSkill", True)
        need_name_change = doc.get("needNameChange", False)
        updated_name = doc.get("updatedName", "")
        aliases = doc.get("aliases", [])
        similar_tools = doc.get("similarToolsAndSkills", [])
        category = doc.get("category", "TECHNICAL")
        
        if action_filter in ["all", "delete"] and not is_skill:
            delete_ops.append(original_id)
            continue
        
        if action_filter in ["all", "update"] and is_skill:
            update_fields = {}
            
            if need_name_change and updated_name:
                update_fields["name"] = updated_name
            
            if aliases:
                update_fields["aliases"] = aliases
            
            if update_fields:
                update_fields["type"] = category.lower()
                update_fields["updatedBy"] = "ai"
                update_fields["updatedAt"] = datetime.now(timezone.utc)
                update_ops.append((original_id, update_fields))
        
        if action_filter in ["all", "create"] and similar_tools:
            for tool in similar_tools:
                tool_name = tool.strip()
                tool_lower = tool_name.lower()
                
                if tool_lower in existing_skill_names:
                    continue
                if tool_lower in newly_created_names:
                    continue
                
                create_docs.append({
                    "name": tool_name,
                    "type": category.lower(),
                    "isDeleted": False,
                    "aliases": [],
                    "createdBy": "ai",
                    "updatedBy": "ai",
                    "createdAt": datetime.now(timezone.utc),
                    "updatedAt": datetime.now(timezone.utc)
                })
                newly_created_names.add(tool_lower)
    
    log(f"\nOperations prepared:")
    log(f"  - DELETE: {len(delete_ops)} skills")
    log(f"  - UPDATE: {len(update_ops)} skills")
    log(f"  - CREATE: {len(create_docs)} new skills")
    
    if dry_run:
        log("\nDRY RUN - No changes made")
        log("\nSample DELETE operations:")
        for oid in delete_ops[:5]:
            log(f"  - {oid}")
        log("\nSample UPDATE operations:")
        for oid, fields in update_ops[:5]:
            log(f"  - {oid}: {list(fields.keys())}")
        log("\nSample CREATE operations:")
        for doc in create_docs[:10]:
            log(f"  - {doc['name']} ({doc['type']})")
        if len(create_docs) > 10:
            log(f"  ... and {len(create_docs) - 10} more")
    else:
        log("\nExecuting operations concurrently...")
        
        tasks = []
        
        if delete_ops:
            async def bulk_delete():
                result = await prod_col.update_many(
                    {"_id": {"$in": [ObjectId(oid) for oid in delete_ops]}},
                    {"$set": {
                        "isDeleted": True,
                        "updatedBy": "ai",
                        "updatedAt": datetime.now(timezone.utc)
                    }}
                )
                return ("DELETE", result.modified_count)
            tasks.append(bulk_delete())
        
        if update_ops:
            async def do_update(oid, fields):
                try:
                    await prod_col.update_one(
                        {"_id": ObjectId(oid)},
                        {"$set": fields}
                    )
                    return 1
                except:
                    return 0
            
            async def bulk_update():
                results = await asyncio.gather(*[do_update(oid, f) for oid, f in update_ops])
                return ("UPDATE", sum(results))
            tasks.append(bulk_update())
        
        if create_docs:
            async def bulk_create():
                try:
                    result = await prod_col.insert_many(create_docs, ordered=False)
                    return ("CREATE", len(result.inserted_ids))
                except Exception as e:
                    log(f"  Some creates failed (duplicates): {e}")
                    return ("CREATE", 0)
            tasks.append(bulk_create())
        
        if tasks:
            results = await asyncio.gather(*tasks)
            for op_type, count in results:
                log(f"  {op_type}: {count} completed")
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    log("\n" + "=" * 60)
    log("SUMMARY")
    log("=" * 60)
    log(f"Total processed: {len(processed_docs)}")
    log(f"Delete operations: {len(delete_ops)}")
    log(f"Update operations: {len(update_ops)}")
    log(f"Create operations: {len(create_docs)}")
    log(f"Duration: {duration:.2f}s")
    if dry_run:
        log("\nDRY RUN - No changes were made")
    log("=" * 60)
    
    local_client.close()
    prod_client.close()


if __name__ == "__main__":
    dry_run = "--dry-run" in sys.argv or "-d" in sys.argv
    
    action_filter = "all"
    for arg in sys.argv:
        if arg.startswith("--action="):
            action_filter = arg.split("=")[1].lower()
    
    print("=" * 60)
    print("SYNC SKILLS TO LIVE DB (FAST MODE)")
    print("=" * 60)
    print(f"Source: {LOCAL_URI}/{LOCAL_DB}.{LOCAL_COLLECTION}")
    print(f"Target: PROD/{PROD_DB}.{PROD_COLLECTION}")
    print(f"Mode: {'DRY RUN' if dry_run else 'LIVE'}")
    print(f"Action: {action_filter.upper()}")
    print("=" * 60)
    
    if not dry_run:
        confirm = input("\nThis will MODIFY skills in PRODUCTION. Continue? (yes/no): ")
        if confirm.lower() != "yes":
            print("Aborted.")
            sys.exit(0)
    
    asyncio.run(sync_skills(dry_run=dry_run, action_filter=action_filter))
