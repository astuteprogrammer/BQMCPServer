# reddit_mcp_server.py
import os
import praw
from fastmcp import FastMCP
from dotenv import load_dotenv
from typing import Optional, List, Dict, Any
import json
import requests
import time

# Load environment variables
load_dotenv()

# Initialize FastMCP server
mcp = FastMCP(
    name="Reddit MCP Server",
    instructions="This server provides access to Reddit posts and subreddit information using the Reddit API."
)

# Initialize Reddit API client
def get_reddit_client():
    """Initialize and return a Reddit client instance."""
    return praw.Reddit(
        client_id=os.getenv("REDDIT_CLIENT_ID","[insert your client ID here]"),
        client_secret=os.getenv("REDDIT_CLIENT_SECRET","[insert your client secret here]"),
        user_agent=os.getenv("REDDIT_USER_AGENT", "test script by /u/tester"),
        username=os.getenv("REDDIT_USERNAME"),  # Optional
        password=os.getenv("REDDIT_PASSWORD")   # Optional
    )

@mcp.tool()
def get_subreddit_posts(
    subreddit_name: str,
    sort_type: str = "hot",
    limit: int = 10,
    time_filter: str = "day"
) -> str:
    """
    Fetch posts from a specific subreddit.
    
    Args:
        subreddit_name: Name of the subreddit (without r/)
        sort_type: Sort type - 'hot', 'new', 'top', 'rising'
        limit: Number of posts to fetch (max 100)
        time_filter: Time filter for 'top' sort - 'hour', 'day', 'week', 'month', 'year', 'all'
    
    Returns:
        JSON string containing post information
    """
    try:
        reddit = get_reddit_client()
        subreddit = reddit.subreddit(subreddit_name)
        
        # Get posts based on sort type
        if sort_type == "hot":
            posts = subreddit.hot(limit=limit)
        elif sort_type == "new":
            posts = subreddit.new(limit=limit)
        elif sort_type == "top":
            posts = subreddit.top(time_filter=time_filter, limit=limit)
        elif sort_type == "rising":
            posts = subreddit.rising(limit=limit)
        else:
            return json.dumps({"error": "Invalid sort_type. Use 'hot', 'new', 'top', or 'rising'"})
        
        # Extract post information
        post_data = []
        for post in posts:
            post_info = {
                "id": post.id,
                "title": post.title,
                "author": str(post.author) if post.author else "[deleted]",
                "score": post.score,
                "upvote_ratio": post.upvote_ratio,
                "num_comments": post.num_comments,
                "created_utc": post.created_utc,
                "url": post.url,
                "permalink": f"https://reddit.com{post.permalink}",
                "selftext": post.selftext[:500] + "..." if len(post.selftext) > 500 else post.selftext,
                "is_self": post.is_self,
                "over_18": post.over_18,
                "spoiler": post.spoiler,
                "stickied": post.stickied,
                "flair_text": post.link_flair_text
            }
            post_data.append(post_info)
        
        return json.dumps({
            "subreddit": subreddit_name,
            "sort_type": sort_type,
            "time_filter": time_filter if sort_type == "top" else None,
            "count": len(post_data),
            "posts": post_data
        }, indent=2)
        
    except Exception as e:
        return json.dumps({"error": f"Failed to fetch posts: {str(e)}"})

@mcp.tool()
def get_post_details(post_id: str, include_comments: bool = False, comment_limit: int = 10) -> str:
    """
    Get detailed information about a specific Reddit post.
    
    Args:
        post_id: Reddit post ID
        include_comments: Whether to include top comments
        comment_limit: Number of top comments to include
    
    Returns:
        JSON string containing detailed post information
    """
    try:
        reddit = get_reddit_client()
        submission = reddit.submission(id=post_id)
        
        post_details = {
            "id": submission.id,
            "title": submission.title,
            "author": str(submission.author) if submission.author else "[deleted]",
            "subreddit": str(submission.subreddit),
            "score": submission.score,
            "upvote_ratio": submission.upvote_ratio,
            "num_comments": submission.num_comments,
            "created_utc": submission.created_utc,
            "url": submission.url,
            "permalink": f"https://reddit.com{submission.permalink}",
            "selftext": submission.selftext,
            "is_self": submission.is_self,
            "over_18": submission.over_18,
            "spoiler": submission.spoiler,
            "stickied": submission.stickied,
            "flair_text": submission.link_flair_text,
            "gilded": submission.gilded,
            "distinguished": submission.distinguished
        }
        
        if include_comments:
            submission.comments.replace_more(limit=0)
            comments = []
            for comment in submission.comments[:comment_limit]:
                if hasattr(comment, 'body'):
                    comment_info = {
                        "id": comment.id,
                        "author": str(comment.author) if comment.author else "[deleted]",
                        "body": comment.body[:300] + "..." if len(comment.body) > 300 else comment.body,
                        "score": comment.score,
                        "created_utc": comment.created_utc,
                        "is_submitter": comment.is_submitter,
                        "stickied": comment.stickied,
                        "gilded": comment.gilded
                    }
                    comments.append(comment_info)
            
            post_details["comments"] = comments
        
        return json.dumps(post_details, indent=2)
        
    except Exception as e:
        return json.dumps({"error": f"Failed to fetch post details: {str(e)}"})

@mcp.tool()
def search_reddit(query: str, sort: str = "relevance", time_filter: str = "all", limit: int = 10) -> str:
    """
    Search Reddit for posts matching a query.
    
    Args:
        query: Search query
        sort: Sort results by 'relevance', 'hot', 'top', 'new', 'comments'
        time_filter: Time filter - 'hour', 'day', 'week', 'month', 'year', 'all'
        limit: Number of results to return
    
    Returns:
        JSON string containing search results
    """
    try:
        reddit = get_reddit_client()
        
        search_results = reddit.subreddit("all").search(
            query=query,
            sort=sort,
            time_filter=time_filter,
            limit=limit
        )
        
        results = []
        for post in search_results:
            result_info = {
                "id": post.id,
                "title": post.title,
                "author": str(post.author) if post.author else "[deleted]",
                "subreddit": str(post.subreddit),
                "score": post.score,
                "num_comments": post.num_comments,
                "created_utc": post.created_utc,
                "url": post.url,
                "permalink": f"https://reddit.com{post.permalink}",
                "selftext": post.selftext[:200] + "..." if len(post.selftext) > 200 else post.selftext
            }
            results.append(result_info)
        
        return json.dumps({
            "query": query,
            "sort": sort,
            "time_filter": time_filter,
            "count": len(results),
            "results": results
        }, indent=2)
        
    except Exception as e:
        return json.dumps({"error": f"Search failed: {str(e)}"})

@mcp.tool()
def get_subreddit_info(subreddit_name: str) -> str:
    """
    Get information about a subreddit.
    
    Args:
        subreddit_name: Name of the subreddit (without r/)
    
    Returns:
        JSON string containing subreddit information
    """
    try:
        reddit = get_reddit_client()
        subreddit = reddit.subreddit(subreddit_name)
        
        subreddit_info = {
            "name": subreddit.display_name,
            "title": subreddit.title,
            "description": subreddit.description[:500] + "..." if len(subreddit.description) > 500 else subreddit.description,
            "subscribers": subreddit.subscribers,
            "active_users": subreddit.active_user_count,
            "created_utc": subreddit.created_utc,
            "over18": subreddit.over18,
            "public_description": subreddit.public_description,
            "url": f"https://reddit.com/r/{subreddit_name}",
            "subreddit_type": subreddit.subreddit_type,
            "lang": subreddit.lang
        }
        
        return json.dumps(subreddit_info, indent=2)
        
    except Exception as e:
        return json.dumps({"error": f"Failed to fetch subreddit info: {str(e)}"})



if __name__ == "__main__":

    # Run the server
    mcp.run()
    