import asyncio
import uuid
import logging
from fastapi import APIRouter, Request, HTTPException
from app.core.events.event_bus import EventBus
from app.core.events.enums import EventType, PlatformType
from app.core.events.base import BaseEvent
from app.core.handler.handler_registry import HandlerRegistry
from pydantic import BaseModel
from app.core.config import settings
import httpx

router = APIRouter()

class RepoRequest(BaseModel):
    repo_url: str


logging.basicConfig(level=logging.INFO)
handler_registry = HandlerRegistry()
event_bus = EventBus(handler_registry)

# Sample handler function to process events
async def sample_handler(event: BaseEvent):
    logging.info(f"Handler received event: {event.event_type} with data: {event.raw_data}")

# Register all the event handlers for issues and pull requests
def register_event_handlers():
    # Issue events
    event_bus.register_handler(EventType.ISSUE_CREATED, sample_handler, PlatformType.GITHUB)
    event_bus.register_handler(EventType.ISSUE_CLOSED, sample_handler, PlatformType.GITHUB)
    event_bus.register_handler(EventType.ISSUE_UPDATED, sample_handler, PlatformType.GITHUB)
    event_bus.register_handler(EventType.ISSUE_COMMENTED, sample_handler, PlatformType.GITHUB)
    # Pull request events
    event_bus.register_handler(EventType.PR_CREATED, sample_handler, PlatformType.GITHUB)
    event_bus.register_handler(EventType.PR_UPDATED, sample_handler, PlatformType.GITHUB)
    event_bus.register_handler(EventType.PR_COMMENTED, sample_handler, PlatformType.GITHUB)
    event_bus.register_handler(EventType.PR_MERGED, sample_handler, PlatformType.GITHUB)

@router.post("/repo-stats")
async def get_repo_stats(request: RepoRequest):
    """
    Get repository statistics from GitHub
    """
    try:
        # Parse repo URL to extract owner and repo name
        repo_url = request.repo_url.strip()
        
        # Handle SSH format (git@github.com:owner/repo.git)
        if repo_url.startswith("git@"):
            # Split on ':' to get the path part
            if ":" in repo_url:
                path = repo_url.split(":", 1)[1]
            else:
                raise HTTPException(status_code=400, detail="Invalid GitHub repository URL")
        else:
            # Ensure scheme is present
            if not repo_url.startswith(("http://", "https://")):
                repo_url = "https://" + repo_url
            
            # Extract path from URL
            try:
                # Parse URL to get path component
                if "github.com/" in repo_url:
                    path = repo_url.split("github.com/", 1)[1]
                else:
                    raise HTTPException(status_code=400, detail="Invalid GitHub repository URL")
            except (IndexError, ValueError) as e:
                raise HTTPException(status_code=400, detail="Invalid GitHub repository URL") from e
        
        # Strip leading/trailing slashes and .git suffix
        path = path.strip("/").removesuffix(".git")
        
        # Split into segments and validate
        segments = path.split("/")
        if len(segments) < 2:
            raise HTTPException(status_code=400, detail="Invalid GitHub repository URL")
        
        owner, repo = segments[0], segments[1]
                # Validate GitHub token is configured
        if not settings.github_token:
            raise HTTPException(
                status_code=500,
                detail="GitHub token not configured in environment variables. Please set GITHUB_TOKEN."
            )
                # Fetch repository stats from GitHub API
        headers = {
            "Authorization": f"token {settings.github_token}",
            "Accept": "application/vnd.github.v3+json"
        }
        
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"https://api.github.com/repos/{owner}/{repo}",
                headers=headers,
                timeout=10.0
            )
            
            if response.status_code != 200:
                logging.error(f"GitHub API error: {response.status_code} - {response.text}")
                raise HTTPException(
                    status_code=response.status_code,
                    detail=f"GitHub API returned status {response.status_code}"
                )
            
            repo_data = response.json()
            
            return {
                "name": repo_data.get("name"),
                "full_name": repo_data.get("full_name"),
                "description": repo_data.get("description"),
                "stars": repo_data.get("stargazers_count", 0),
                "forks": repo_data.get("forks_count", 0),
                "open_issues": repo_data.get("open_issues_count", 0),
                "language": repo_data.get("language"),
                "created_at": repo_data.get("created_at"),
                "updated_at": repo_data.get("updated_at"),
                "url": repo_data.get("html_url"),
                "owner": {
                    "login": repo_data.get("owner", {}).get("login"),
                    "avatar_url": repo_data.get("owner", {}).get("avatar_url"),
                }
            }
    except HTTPException:
        raise
    except Exception as e:
        logging.exception("Error fetching repo stats")
        raise HTTPException(status_code=500, detail="Internal server error") from e

@router.post("/github/webhook")
async def github_webhook(request: Request):
    payload = await request.json()
    event_header = request.headers.get("X-GitHub-Event")
    logging.info(f"Received GitHub event: {event_header}")

    event_type = None

    # Handle issue events
    if event_header == "issues":
        action = payload.get("action")
        if action == "opened":
            event_type = EventType.ISSUE_CREATED
        elif action == "closed":
            event_type = EventType.ISSUE_CLOSED
        elif action == "edited":
            event_type = EventType.ISSUE_UPDATED

    # Handle issue comment events
    elif event_header == "issue_comment":
        action = payload.get("action")
        if action == "created":
            event_type = EventType.ISSUE_COMMENTED

    # Handle pull request events
    elif event_header == "pull_request":
        action = payload.get("action")
        if action == "opened":
            event_type = EventType.PR_CREATED
        elif action == "edited":
            event_type = EventType.PR_UPDATED
        elif action == "closed":
            # Determine if the PR was merged or simply closed
            if payload.get("pull_request", {}).get("merged"):
                event_type = EventType.PR_MERGED
            else:
                logging.info("Pull request closed without merge; no event dispatched.")

    # Handle pull request comment events
    elif event_header in ["pull_request_review_comment", "pull_request_comment"]:
        action = payload.get("action")
        if action == "created":
            event_type = EventType.PR_COMMENTED

    # Dispatch the event if we have a matching type
    if event_type:
        event = BaseEvent(
            id=str(uuid.uuid4()),
            actor_id=str(payload.get("sender", {}).get("id", "unknown")),
            event_type=event_type,
            platform=PlatformType.GITHUB,
            raw_data=payload
        )
        await event_bus.dispatch(event)
    else:
        logging.info(f"No matching event type for header: {event_header} with action: {payload.get('action')}")

    return {"status": "ok"}
